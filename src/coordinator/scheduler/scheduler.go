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
	clustersIdleRes   map[string]types.Resource
	totalResource     types.Resource
	idleResource      types.Resource
)

func init() {
	clustersPresent = make(map[string]bool)
	clustersActiveQ = make(chan string, 10)
	clustersPodsQ = make(map[string]chan types.InterPod)
	clustersInfo = make(map[string]types.Cluster)
	clustersIdleRes = make(map[string]types.Resource)
}

func RegisterCluster(cluster types.Cluster) {
	clustersShare[cluster.Id] = 0
	clustersInfo[cluster.Id] = cluster
	totalResource.Memory += cluster.TotalResource.Memory
	totalResource.MilliCpu += cluster.TotalResource.MilliCpu
	glog.Info("totalResource:", totalResource)
}

func UpdateCluster(cluster types.Cluster) {
	resource, ok := clustersIdleRes[cluster.Id]
	if ok {
		idleResource.Memory += cluster.IdleResource.Memory - resource.Memory
		idleResource.MilliCpu += cluster.IdleResource.MilliCpu - resource.MilliCpu
	} else {
		idleResource.Memory = cluster.IdleResource.Memory
		idleResource.MilliCpu = cluster.IdleResource.MilliCpu
	}
	glog.Info("idleResource:", idleResource)
	clustersIdleRes[cluster.Id] = cluster.IdleResource
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
		for clusterId, res := range clustersIdleRes {
			if res.Memory >= pod.RequestMemory && res.MilliCpu >= pod.RequestMilliCpu {
				uploadResult(pod.Pod, clustersInfo[pod.ClusterId].Ip, clustersInfo[clusterId].Ip)
				fixContributedResource(pod, clusterId)
				glog.Infof("Successfully schedule %s of %s to %s.", pod.Pod.Name, pod.ClusterId, clusterId)
				break
			}
			time.Sleep(3 * time.Second)
		}
	}
}

func uploadResult(pod types.Pod, sourceIp, destIp string) {
	client, err := rpc.DialHTTP("tcp", sourceIp+":4321")
	if err != nil {
		glog.Info(err)
	}
	result := &types.ScheduleResult{
		Pod:    pod,
		DestIp: destIp,
	}
	var reply int
	err = client.Call("Server.ScheduleResult", result, &reply)
	if err != nil {
		glog.Info(err)
	}
}
