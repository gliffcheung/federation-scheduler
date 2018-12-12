package scheduler

import (
	"flag"
	"os"
	"path/filepath"
	"github.com/golang/glog"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"types"
)

type resource struct {
	allocatedMilliCpu	int64
	allocatedMemory		int64
}

var (
	allocatedResource 	map[string] resource
	clientset			*kubernetes.Clientset
)

func init() {
	var kubeconfig *string
	if home := homeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		glog.Error(err.Error())
	}

	// create the clientset
	clientset, err = kubernetes.NewForConfig(config)
	if err != nil {
		glog.Error(err.Error())
	}

	allocatedResource = make(map[string] resource)
	initializeAllocatedResource()
}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}

func initializeAllocatedResource() {
	pods, err := clientset.CoreV1().Pods("").List(metav1.ListOptions{})
	if err != nil {
		glog.Error(err.Error())
	}
	for _, pod := range(pods.Items) {
		if pod.Status.Phase == v1.PodRunning {
			var requestsMilliCpu, requestsMemory int64
			for _, ctn := range(pod.Spec.Containers) {
				requestsMilliCpu += ctn.Resources.Requests.Cpu().MilliValue()
				requestsMemory += ctn.Resources.Requests.Memory().Value()
			}
			nodeName := pod.Spec.NodeName
			var res resource
			res, ok := allocatedResource[nodeName]
			if ok {
				res.allocatedMilliCpu += requestsMilliCpu
				res.allocatedMemory += requestsMemory
				allocatedResource[nodeName] = res
			} else {
				res.allocatedMilliCpu = requestsMilliCpu
				res.allocatedMemory = requestsMemory
				allocatedResource[nodeName] = res
			}
		}
	}
}

func getUnscheduledPods() []types.Pod {
	pods, err := clientset.CoreV1().Pods("").List(metav1.ListOptions{})
	if err != nil {
		glog.Error(err.Error())
	}
	unscheduledPods := make([]types.Pod, 0)
	for _, pod := range(pods.Items) {
		if pod.Status.Phase == v1.PodPending && pod.Spec.SchedulerName != "default-scheduler" && pod.Spec.NodeName == "" {
			var requestsMilliCpu, requestsMemory int64
			for _, ctn := range(pod.Spec.Containers) {
				requestsMilliCpu += ctn.Resources.Requests.Cpu().MilliValue()
				requestsMemory += ctn.Resources.Requests.Memory().Value()
			}
			newPod := types.Pod{
				Name: pod.Name, 
				Uid: pod.Namespace, 
				RequestMilliCpu: requestsMilliCpu, 
				RequestMemory: requestsMemory,
			}
			unscheduledPods = append(unscheduledPods, newPod)
		}
	}
	return unscheduledPods
}

func getNodes() []types.Node {
	nodes, err := clientset.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		glog.Error(err.Error())
	}
	availabelNodes := make([]types.Node, 0)
	for _, node := range nodes.Items {
		newNode := types.Node{
			Name: node.Name, 
			AllocatableMilliCpu: node.Status.Allocatable.Cpu().MilliValue(), 
			AllocatableMemory: node.Status.Allocatable.Memory().Value(),
		}
		availabelNodes = append(availabelNodes, newNode)
	}
	return availabelNodes
}

func schedulePodToNode(pod types.Pod, node types.Node) {
	binding := v1.Binding{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1", 
			Kind: "Binding",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: pod.Name,
		},
		Target: v1.ObjectReference{
			APIVersion: "v1", 
			Kind: "Node", 
			Name: node.Name,
		},
	}
	err := clientset.CoreV1().Pods("default").Bind(&binding)
	if err != nil {
		glog.Error(err.Error())
	}
	res, _ := allocatedResource[node.Name]
	res.allocatedMilliCpu += pod.RequestMilliCpu
	res.allocatedMemory += pod.RequestMemory
	allocatedResource[node.Name] = res
	glog.Infof("Successfully schedule %s to %s", pod.Name, node.Name)
}
