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

var (
	allocatedResource          map[string]types.Resource
	availableNodes             []types.Node
	clientset                  *kubernetes.Clientset
	pendingPodCh, deletedPodCh chan types.Pod
	otherClustersPod           map[string]string
)

func init() {
	allocatedResource = make(map[string]types.Resource)
	availableNodes = make([]types.Node, 0)
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

	initAllocatedResource()
	initNodes()
	initShare()
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
		var res types.Resource
		res, ok := allocatedResource[nodeName]
		if ok {
			res.MilliCpu += pod.RequestMilliCpu
			res.Memory += pod.RequestMemory
			allocatedResource[nodeName] = res
		} else {
			res.MilliCpu = pod.RequestMilliCpu
			res.Memory = pod.RequestMemory
			allocatedResource[nodeName] = res
		}
		glog.Infof("%v is running.\n", pod)
	}
	for k, v := range allocatedResource {
		glog.Infof("%s has used : %v", k, v)
	}
	glog.Info("AllocatedResource initialization is completed.")
}

func initNodes() {
	nodes, err := clientset.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		glog.Error(err.Error())
	}
	for _, node := range nodes.Items {
		newNode := types.Node{
			Name: node.Name,
			Resource: types.Resource{
				MilliCpu: node.Status.Allocatable.Cpu().MilliValue(),
				Memory:   node.Status.Allocatable.Memory().Value() / 1024 / 1024,
			},
		}
		availableNodes = append(availableNodes, newNode)
	}
	for _, node := range availableNodes {
		glog.Infof("%s's total resource : %v", node.Name, node.Resource)
	}
	glog.Info("AvailableNodes initialization is completed.")
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
				requestsMemory += ctn.Resources.Requests.Memory().Value() / 1024 / 1024
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

func getPodByName(podName, namespace string) (v1.Pod, error) {
	pod, err := clientset.CoreV1().Pods(namespace).Get(podName, metav1.GetOptions{})
	if err != nil {
		glog.Error(err)
	} else {
		glog.Info("get pod:", pod)
	}
	return *pod, err
}

func deletePodByName(podName, namespace string) error {
	err := clientset.CoreV1().Pods(namespace).Delete(podName, &metav1.DeleteOptions{})
	if err != nil {
		glog.Error(err)
	} else {
		glog.Info("delete pod:", podName)
	}
	return err
}

func createPod(outsourcePod types.OutsourcePod) error {
	pod := outsourcePod.Pod
	otherClustersPod[pod.Name] = outsourcePod.SourceIP
	containers := make([]v1.Container, 0)
	for _, c := range pod.Spec.Containers {
		container := v1.Container{
			Name:      c.Name,
			Image:     c.Image,
			Command:   c.Command,
			Args:      c.Args,
			Resources: c.Resources,
		}
		containers = append(containers, container)
	}
	newPod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: "other-clusters",
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}
	_, err := clientset.CoreV1().Pods("default").Create(newPod)
	if err != nil {
		glog.Error(err)
	} else {
		glog.Info("create pod:", newPod)
	}
	return err
}

func updateAllocatedResource() {
	for pod := range deletedPodCh {
		nodeName := pod.NodeName
		res, _ := allocatedResource[nodeName]
		res.MilliCpu -= pod.RequestMilliCpu
		res.Memory -= pod.RequestMemory
		allocatedResource[nodeName] = res
		glog.Info("---------", nodeName, ":", res)
	}
}

func getNodes() []types.Node {
	return availableNodes
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
	res.MilliCpu += pod.RequestMilliCpu
	res.Memory += pod.RequestMemory
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
				requestsMemory += ctn.Resources.Requests.Memory().Value() / 1024 / 1024
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
					if pod.Namespace == "OtherClusters" {
						highPriorityCh <- newPod
						glog.Info("highPriorytyCh <- ", newPod)
					} else {
						pendingPodCh <- newPod
						glog.Info("pendingPodCh <- ", newPod)
					}
				}
				if statusPhase == v1.PodPending && pod.Spec.NodeName == "" {
					_, ok := podInfo[pod.Name]
					if !ok && pod.Namespace == "OtherClusters" {
						podInfo[pod.Name] = *pod
					}
				}
			case "MODIFIED":
				if statusPhase == v1.PodSucceeded {
					// Finished.
					deletedPodCh <- newPod
					glog.Info("deletedPodCh <- ", newPod)
					// Return executeResult
					createTime := pod.CreationTimestamp.Size()
					startTime := pod.Status.StartTime.Size()
					executeResult := types.ExecuteResult{
						Pod:        newPod,
						CreateTime: int64(createTime),
						StartTime:  int64(startTime),
						Status:     string(statusPhase),
					}
					_, ok := otherClustersPod[pod.Name]
					if !ok {
						executeResultQ <- executeResult
					} else {
						ReturnExecuteResult(executeResult)
					}
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
