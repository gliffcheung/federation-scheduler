package scheduler

import (
	"container/heap"
	"net/rpc"
	"time"
	"types"

	"github.com/golang/glog"
)

var (
	clustersPriorityQ types.ClustersPriorityQueue
	clustersPresent   map[string]bool
	clustersActiveQ   chan string
	clustersPodsQ     map[string]chan types.InterPod
	clustersIp        map[string]string
)

func init() {
	clustersPresent = make(map[string]bool)
	clustersActiveQ = make(chan string, 10)
	clustersPodsQ = make(map[string]chan types.InterPod)
	clustersIp = make(map[string]string)
}

func RegisterCluster(cluster types.Cluster) {
	clustersShare[cluster.Id] = 0
	clustersIp[cluster.Id] = cluster.Ip
}

func DispatchPods(pendingPodCh chan types.InterPod) {
	for pod := range pendingPodCh {
		value, ok := clustersPodsQ[pod.ClusterId]
		if !ok {
			clustersPodsQ[pod.ClusterId] = make(chan types.InterPod, 20)
			value = clustersPodsQ[pod.ClusterId]
		}
		if len(value) == 0 {
			clustersActiveQ <- pod.ClusterId
		}
		value <- pod
	}
}

func Schedule() {
	for {
		// fix clustersPriorityQ
		clustersActiveQLen := len(clustersActiveQ)
		for i := 0; i < clustersActiveQLen; i++ {
			clusterId := <-clustersActiveQ
			present, ok := clustersPresent[clusterId]
			if ok && present {
				continue
			} else {
				clustersPresent[clusterId] = true
				cluster := &types.Cluster{
					Id:       clusterId,
					Priority: getClusterShare(clusterId),
				}
				heap.Push(&clustersPriorityQ, cluster)
			}
		}

		// schedule pod
		if len(clustersPriorityQ) > 0 {
			topCluster := heap.Pop(&clustersPriorityQ).(*types.Cluster)
			select {
			case firstPod := <-clustersPodsQ[topCluster.Id]:
				glog.Info("=============================")
				glog.Info("Before Schedule()")
				printShare()
				schedulePod(firstPod)
				topCluster.Priority = fixClusterShare(topCluster.Id, firstPod)
				heap.Push(&clustersPriorityQ, topCluster)
				glog.Info("After Schedule()")
				printShare()
				glog.Info("=============================")
			default:
				clustersPresent[topCluster.Id] = false
			}
		} else {
			// cluster is idle, and its resource could be shared.
		}
		time.Sleep(3 * time.Second)
	}
}

func schedulePod(pod types.InterPod) {
	for {
		nodes := getNodes()
		glog.Info(nodes)
		for _, node := range nodes {
			if node.AllocatedMilliCpu+pod.RequestMilliCpu <= node.AllocatableMilliCpu &&
				node.AllocatedMemory+pod.RequestMemory <= node.AllocatableMemory {
				schedulePodToNode(pod, node)
				return
			}
		}
		glog.Info(pod, "cannot be scheduled.")
		time.Sleep(3 * time.Second)
	}
}

func schedulePodToNode(pod types.InterPod, node types.InterNode) {
	for i, n := range clustersIdleNode[node.ClusterId] {
		if n.Name == node.Name {
			clustersIdleNode[node.ClusterId][i].AllocatedMilliCpu += pod.RequestMilliCpu
			clustersIdleNode[node.ClusterId][i].AllocatedMemory += pod.RequestMemory
			break
		}
	}
	uploadResult(pod.Pod, clustersIp[pod.ClusterId], clustersIp[node.ClusterId])
	glog.Infof("Successfully schedule %s to %v", pod.Name, node)
}

func uploadResult(pod types.Pod, sourceIp, destIp string) {
	client, err := rpc.DialHTTP("tcp", sourceIp+":4321")
	if err != nil {
		glog.Info(err)
	}
	result := types.Result{
		Pod:    pod,
		DestIp: destIp,
	}
	var reply int
	err = client.Call("Server.ScheduleResult", result, &reply)
	if err != nil {
		glog.Info(err)
	}
}
