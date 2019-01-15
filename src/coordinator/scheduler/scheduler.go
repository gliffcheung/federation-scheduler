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
	clustersInfo      map[string]types.Cluster
	IdleNodes         map[string]types.InterNode
	totalResource     types.Resource
)

func init() {
	clustersPresent = make(map[string]bool)
	clustersActiveQ = make(chan string, 10)
	clustersPodsQ = make(map[string]chan types.InterPod)
	clustersInfo = make(map[string]types.Cluster)
	IdleNodes = make(map[string]types.InterNode)
}

func RegisterCluster(cluster types.Cluster) {
	clustersShare[cluster.Id] = 0
	clustersInfo[cluster.Id] = cluster
	totalResource.Memory += cluster.TotalResource.Memory
	totalResource.MilliCpu += cluster.TotalResource.MilliCpu
	glog.Info("totalResource:", totalResource)
}

func UpdateCluster(cluster types.Cluster) {
	for _, node := range cluster.IdleNodes {
		nodeName := cluster.Id + node.Name
		idleNode, ok := IdleNodes[nodeName]
		if !ok || idleNode.IdleResource.MilliCpu != node.IdleResource.MilliCpu || idleNode.IdleResource.Memory != node.IdleResource.Memory {
			IdleNodes[nodeName] = node
			glog.Infof("Update %s : %s %v", cluster.Id, node.Name, node.IdleResource)
		}
	}
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
				topCluster.Priority = fixClusterShare(firstPod)
				heap.Push(&clustersPriorityQ, topCluster)
				glog.Info("After Schedule()")
				printShare()
				glog.Info("=============================")
			default:
				clustersPresent[topCluster.Id] = false
			}
		}
		time.Sleep(3 * time.Second)
	}
}

func schedulePod(pod types.InterPod) {
	for {
		for nodeName, node := range IdleNodes {
			if node.IdleResource.Memory >= pod.RequestMemory && node.IdleResource.MilliCpu >= pod.RequestMilliCpu {
				uploadResult(pod.Pod, clustersInfo[pod.ClusterId].Ip, clustersInfo[node.ClusterId].Ip)
				fixContributedResource(pod, node.ClusterId)
				glog.Infof("Successfully schedule %s of %s to %s.", pod.Pod.Name, pod.ClusterId, node.ClusterId)
				node.IdleResource.Memory -= pod.RequestMemory
				node.IdleResource.MilliCpu -= pod.RequestMilliCpu
				IdleNodes[nodeName] = node
				glog.Infof("Update %s : %s %v", node.ClusterId, node.Name, node.IdleResource)
				return
			}
			time.Sleep(3 * time.Second)
		}
	}
}

func uploadResult(pod types.Pod, sourceIp, destIp string) {
	result := &types.ScheduleResult{
		Pod:    pod,
		DestIp: destIp,
	}
	client, err := rpc.DialHTTP("tcp", sourceIp+":4321")
	if err == nil {
		glog.Info("ReturnScheduleResult:", result, " to ", sourceIp)
	} else {
		glog.Info(err)
	}

	var reply int
	err = client.Call("Server.ReturnScheduleResult", result, &reply)
	if err != nil {
		glog.Info(err)
	}
}
