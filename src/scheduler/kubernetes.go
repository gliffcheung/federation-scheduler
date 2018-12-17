package scheduler

import (
	"flag"
	"os"
	"path/filepath"

	"github.com/golang/glog"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"types"
)

type resource struct {
	allocatedMilliCpu int64
	allocatedMemory   int64
}

var (
	allocatedResource          map[string]resource
	clientset                  *kubernetes.Clientset
	pendingPodCh, deletedPodCh chan types.Pod
)

func init() {
	allocatedResource = make(map[string]resource)
	pendingPodCh = make(chan types.Pod, 10)
	deletedPodCh = make(chan types.Pod, 10)
}

func Init() {
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
	glog.Info("clientset is created successfully.")

	initShare()
	initAllocatedResource()
	go updateAllocatedResource()
}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}

func initAllocatedResource() {
	pods := getRunningPods()
	for _, pod := range pods {
		nodeName := pod.NodeName
		var res resource
		res, ok := allocatedResource[nodeName]
		if ok {
			res.allocatedMilliCpu += pod.RequestMilliCpu
			res.allocatedMemory += pod.RequestMemory
			allocatedResource[nodeName] = res
		} else {
			res.allocatedMilliCpu = pod.RequestMilliCpu
			res.allocatedMemory = pod.RequestMemory
			allocatedResource[nodeName] = res
		}
		glog.Infof("%s is running.\n", pod.Name)
	}
	for k, v := range allocatedResource {
		glog.Info("+++++++++", k, ":", v)
	}
	glog.Info("AllocatedResource initialization is completed.")
}

func getRunningPods() []types.Pod {
	pods, err := clientset.CoreV1().Pods("").List(metav1.ListOptions{})
	if err != nil {
		glog.Error(err.Error())
	}
	runningPods := make([]types.Pod, 0)
	for _, pod := range pods.Items {
		if pod.Status.Phase == v1.PodRunning {
			var requestsMilliCpu, requestsMemory int64
			for _, ctn := range pod.Spec.Containers {
				requestsMilliCpu += ctn.Resources.Requests.Cpu().MilliValue()
				requestsMemory += ctn.Resources.Requests.Memory().Value()
			}
			newPod := types.Pod{
				Name:            pod.Name,
				Uid:             pod.Namespace,
				NodeName:        pod.Spec.NodeName,
				RequestMilliCpu: requestsMilliCpu,
				RequestMemory:   requestsMemory,
			}
			runningPods = append(runningPods, newPod)
		}
	}
	return runningPods
}

func updateAllocatedResource() {
	for pod := range deletedPodCh {
		nodeName := pod.NodeName
		res, _ := allocatedResource[nodeName]
		res.allocatedMilliCpu -= pod.RequestMilliCpu
		res.allocatedMemory -= pod.RequestMemory
		allocatedResource[nodeName] = res
		glog.Info("---------", nodeName, ":", res)
	}
}

func getNodes() []types.Node {
	nodes, err := clientset.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		glog.Error(err.Error())
	}
	availabelNodes := make([]types.Node, 0)
	for _, node := range nodes.Items {
		newNode := types.Node{
			Name:                node.Name,
			AllocatableMilliCpu: node.Status.Allocatable.Cpu().MilliValue(),
			AllocatableMemory:   node.Status.Allocatable.Memory().Value(),
		}
		availabelNodes = append(availabelNodes, newNode)
	}
	return availabelNodes
}

func getNamespaces() []string {
	nss, err := clientset.CoreV1().Namespaces().List(metav1.ListOptions{})
	if err != nil {
		glog.Error(err.Error())
	}
	namespaces := make([]string, 0)
	for _, ns := range nss.Items {
		name := ns.Name
		if name != "default" && name != "kube-public" && name != "kube-system" {
			namespaces = append(namespaces, name)
		}
	}
	return namespaces
}

func schedulePodToNode(pod types.Pod, node types.Node) {
	binding := v1.Binding{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Binding",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: pod.Name,
		},
		Target: v1.ObjectReference{
			APIVersion: "v1",
			Kind:       "Node",
			Name:       node.Name,
		},
	}
	err := clientset.CoreV1().Pods(pod.Uid).Bind(&binding)
	if err != nil {
		glog.Error(err.Error())
	}
	res, _ := allocatedResource[node.Name]
	res.allocatedMilliCpu += pod.RequestMilliCpu
	res.allocatedMemory += pod.RequestMemory
	allocatedResource[node.Name] = res
	glog.Info("+++++++++", node.Name, ":", res)
	glog.Infof("Successfully schedule %s to %s", pod.Name, node.Name)
}

func WatchPods() {
	// In case the eventChan is closed sometime.
	for {
		watchInt, err := clientset.CoreV1().Pods("").Watch(metav1.ListOptions{})
		if err != nil {
			glog.Error(err.Error())
		}
		eventChan := watchInt.ResultChan()
		for event := range eventChan {
			pod := event.Object.(*v1.Pod)
			statusPhase := pod.Status.Phase
			var requestsMilliCpu, requestsMemory int64
			for _, ctn := range pod.Spec.Containers {
				requestsMilliCpu += ctn.Resources.Requests.Cpu().MilliValue()
				requestsMemory += ctn.Resources.Requests.Memory().Value()
			}
			newPod := types.Pod{
				Name:            pod.Name,
				Uid:             pod.Namespace,
				NodeName:        pod.Spec.NodeName,
				RequestMilliCpu: requestsMilliCpu,
				RequestMemory:   requestsMemory,
			}
			switch event.Type {
			case "ADDED":
				if statusPhase == v1.PodPending && pod.Spec.SchedulerName != "default-scheduler" && pod.Spec.NodeName == "" {
					// Need to be scheduled.
					pendingPodCh <- newPod
					glog.Info("pendingPodCh <- ", newPod)
				}
			case "MODIFIED":
				if statusPhase == v1.PodSucceeded {
					// Finished.
					deletedPodCh <- newPod
					glog.Info("deletedPodCh <- ", newPod)
				}
			case "DELETED":
				if statusPhase == v1.PodRunning {
					// Be deleted.
					deletedPodCh <- newPod
					glog.Info("deletedPodCh <- ", newPod)
				}
			}
		}
		glog.Warning("watchPods exit.")
	}
}
