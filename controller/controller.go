package controller

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"golang.org/x/xerrors"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/gengo/namer"
	gengotypes "k8s.io/gengo/types"
	"k8s.io/klog"
)

const (
	errorMessage = "standard_init_linux.go:211: exec user process caused \"exec format error\"\n"
	patchJSON    = `
{
  "spec": {
    "template": {
      "spec": {
        "affinity": {
          "nodeAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": {
              "nodeSelectorTerms": [
                {
                  "matchExpressions": [
                    {
                      "key": "beta.kubernetes.io/arch",
                      "operator": "NotIn",
                      "values": [
                        "%s"
                      ]
                    }
                  ]
                }
              ]
            }
          }
        }
      }
    }
  }
}
`
)

// ArchSchedulingController reschedule pods that failed to run with exec format error
// to another architecture node
type ArchSchedulingController struct {
	informerFactory informers.SharedInformerFactory
	podInformer     coreinformers.PodInformer
	podListener     listers.PodLister
	kubeClientset   kubernetes.Interface
	workqueue       workqueue.RateLimitingInterface
}

// NewArchSchedulingController creates a ArchSchedulingController
func NewArchSchedulingController(informerFactory informers.SharedInformerFactory, kubeClientset kubernetes.Interface) *ArchSchedulingController {
	podInformer := informerFactory.Core().V1().Pods()
	c := &ArchSchedulingController{
		informerFactory: informerFactory,
		podInformer:     podInformer,
		podListener:     podInformer.Lister(),
		kubeClientset:   kubeClientset,
		workqueue:       workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "Pods"),
	}

	podInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: c.handleObject,
			UpdateFunc: func(old, new interface{}) {
				newPod := new.(*v1.Pod)
				oldPod := old.(*v1.Pod)
				if newPod.ResourceVersion == oldPod.ResourceVersion {
					return
				}
				c.handleObject(new)
			},
		},
	)
	return c
}

// Run sets up the shared informers and waits for synchronizing
// the shared informer cache.
func (c *ArchSchedulingController) Run(stopCh chan struct{}) error {
	defer runtime.HandleCrash()
	defer c.workqueue.ShutDown()

	c.informerFactory.Start(stopCh)
	klog.Info("Starting Arch scheduling controller")

	klog.Info("Waiting for informer caches to sync")
	if !cache.WaitForCacheSync(stopCh, c.podInformer.Informer().HasSynced) {
		return xerrors.New("Failed to wait for sync to cache")
	}

	for c.processEnqueuedItem() {
	}

	return nil
}

func (c *ArchSchedulingController) processEnqueuedItem() bool {
	obj, shutdown := c.workqueue.Get()
	if shutdown {
		return false
	}

	err := func(obj interface{}) error {
		defer c.workqueue.Done(obj)
		var key string
		var ok bool

		if key, ok = obj.(string); !ok {
			c.workqueue.Forget(obj)
			runtime.HandleError(xerrors.Errorf("Expected the enqueued item is string but got %#v", obj))
			return nil
		}

		if err := c.reSchedulePod(key); err != nil {
			c.workqueue.AddRateLimited(key)
			return xerrors.Errorf("Failed to patch to reschedule '%s' pod : %s, requeuing", key, err.Error())
		}

		c.workqueue.Forget(obj)
		klog.Infof("Successfully patch to reschedule '%s'", key)
		return nil
	}(obj)

	if err != nil {
		runtime.HandleError(err)
		return true
	}

	return true
}

func (c *ArchSchedulingController) reSchedulePod(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(xerrors.Errorf("Invalid resource key: %s", key))
		return nil
	}

	pod, err := c.podListener.Pods(namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			runtime.HandleError(xerrors.Errorf("'%s' pod no longer exists", key))
			return nil
		}

		return err
	}

	cobj, err := c.getOriginControllerObject(pod, pod.Kind)
	if err != nil {
		return err
	}

	node, err := c.kubeClientset.CoreV1().Nodes().Get(context.TODO(), pod.Spec.NodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	patchString := fmt.Sprintf(patchJSON, node.GetLabels()["beta.kubernetes.io/arch"])
	err = c.patchToControllerObject(*cobj, []byte(patchString))
	if err != nil {
		return err
	}

	return nil
}

func (c *ArchSchedulingController) handleObject(obj interface{}) {
	pod := obj.(*v1.Pod)
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Waiting != nil && cs.State.Waiting.Reason == "CrashLoopBackOff" {
			for _, ct := range pod.Spec.Containers {
				req := c.kubeClientset.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &v1.PodLogOptions{Container: ct.Name})
				body, err := req.Stream(context.Background())
				if err != nil {
					klog.Errorf("Failed to get logs: %s", err)
					continue
				}

				var buf bytes.Buffer
				n, err := io.Copy(&buf, body)
				if err != nil {
					klog.Errorf("Failed to read logs: %s", err)
					continue
				}
				if n == 0 {
					klog.V(4).Infoln("Empty log")
				}

				err = body.Close()
				if err != nil {
					klog.Errorf("Failed to close response body: %s", err)
				}

				if buf.String() == errorMessage {
					c.enqueuePod(pod)
					return
				}
			}
		}
	}
}

func (c *ArchSchedulingController) enqueuePod(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}

	c.workqueue.Add(key)
}

func lowercaseAndPluralise(s string) string {
	pluralise := namer.NewAllLowercasePluralNamer(make(map[string]string))
	pluralType := gengotypes.Type{
		Name: gengotypes.Name{Name: s},
	}

	return pluralise.Name(&pluralType)
}

type controllerObject struct {
	kind      string
	name      string
	namespace string
}

func (c *ArchSchedulingController) getOriginControllerObject(obj interface{}, kind string) (*controllerObject, error) {
	object := obj.(metav1.Object)

	if ownerRef := metav1.GetControllerOf(object); ownerRef != nil {
		req := c.kubeClientset.AppsV1().RESTClient().Get().Resource(lowercaseAndPluralise(ownerRef.Kind)).Namespace(object.GetNamespace()).Name(ownerRef.Name)
		res, err := req.Do(context.TODO()).Get()

		if err != nil {
			return nil, err
		}

		return c.getOriginControllerObject(res, ownerRef.Kind)
	}

	return &controllerObject{kind: kind, name: object.GetName(), namespace: object.GetNamespace()}, nil
}

func (c *ArchSchedulingController) patchToControllerObject(obj controllerObject, patch []byte) error {
	req := c.kubeClientset.AppsV1().RESTClient().Patch(types.StrategicMergePatchType).
		Namespace(obj.namespace).
		Resource(lowercaseAndPluralise(obj.kind)).
		Name(obj.name).
		Body(patch)

	_, err := req.Do(context.TODO()).Get()
	if err != nil {
		return err
	}

	return nil
}
