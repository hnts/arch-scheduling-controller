package main

import (
	"flag"
	"path/filepath"
	"time"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"k8s.io/component-base/logs"
	"k8s.io/klog"
)

func main() {
	logs.InitLogs()
	defer logs.FlushLogs()

	var kubeconfig *string

	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"),
			"(optional) kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "kubeconfig file")
	}

	flag.Parse()
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatal(err)
	}

	factory := informers.NewSharedInformerFactory(clientset, time.Second*30)
	controller := NewArchSchedulingController(factory, clientset)
	stop := make(chan struct{})
	defer close(stop)

	err = controller.Run(stop)
	if err != nil {
		klog.Fatal(err)
	}
	select {}
}
