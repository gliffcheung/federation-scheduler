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
	IdleResource      map[string]types.Resource
	ShareOrNot        map[string]bool
	TotalResource     types.Resource
)

func init() {
	clustersPresent = make(map[string]bool)
	clustersActiveQ = make(chan string, 10)
	clustersPodsQ = make(map[string]chan types.InterPod)
	clustersInfo = make(map[string]types.Cluster)
	IdleResource = make(map[string]types.Resource)
	ShareOrNot = make(map[string]bool)
}

func RegisterCluster(cluster types.Cluster) {
	var res types.Resource
	allocatedResource[cluster.Id] = res
	contributedResource[cluster.Id] = res
	clustersShare[cluster.Id] = 0
	clustersInfo[cluster.Id] = cluster
	TotalResource.Memory += cluster.TotalResource.Memory
	TotalResource.MilliCpu += cluster.TotalResource.MilliCpu
	glog.Info("TotalResource:", TotalResource)
}

func UpdateCluster(cluster types.Cluster) {
	IdleResource[cluster.Id] = cluster.IdleResource
	ShareOrNot[cluster.Id] = cluster.Share
	glog.Infof("Update %s %v share:%t", cluster.Id, cluster.IdleResource, cluster.Share)
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
				destClusterId := schedulePod(firstPod)
				if destClusterId != firstPod.ClusterId {
					fixContributedResource(firstPod, destClusterId)
					topCluster.Priority = fixClusterShare(firstPod)
				}
				heap.Push(&clustersPriorityQ, topCluster)
				glog.Info("After Schedule()")
				printShare()
				glog.Info("=============================")
				clusterData := types.UserData{
					Uid:         firstPod.Uid,
					CurrentTime: time.Now().Unix(),
					Share:       topCluster.Priority,
					Resource:    allocatedResource[firstPod.Uid],
				}
				clusterDataQ <- clusterData
			default:
				clustersPresent[topCluster.Id] = false
			}
		}
		time.Sleep(time.Second)
	}
}

func schedulePod(pod types.InterPod) string {
	var idleMemory, idleCpu int64
	var destClusterId string
	idleMemory = 0
	idleCpu = 0
	destClusterId = ""
	for clusterId, share := range ShareOrNot {
		if share == true && IdleResource[clusterId].Memory > idleMemory && IdleResource[clusterId].MilliCpu > idleCpu {
			idleMemory = IdleResource[clusterId].Memory
			idleCpu = IdleResource[clusterId].MilliCpu
			destClusterId = clusterId
		}
	}
	if destClusterId != "" {
		memory := IdleResource[destClusterId].Memory - pod.RequestMemory
		cpu := IdleResource[destClusterId].MilliCpu - pod.RequestMilliCpu
		IdleResource[destClusterId] = types.Resource{Memory: memory, MilliCpu: cpu}
		uploadResult(pod.Pod, clustersInfo[pod.ClusterId].Ip, clustersInfo[destClusterId].Ip)
		glog.Infof("Successfully schedule %s of %s to %s.", pod.Name, pod.ClusterId, destClusterId)
		return destClusterId
	}
	clusterId := pod.ClusterId
	share := clustersShare[clusterId]
	destClusterId = clusterId
	minShare := share
	for c, s := range clustersShare {
		if s < minShare {
			minShare = s
			destClusterId = c
		}
	}
	uploadResult(pod.Pod, clustersInfo[pod.ClusterId].Ip, clustersInfo[destClusterId].Ip)
	glog.Infof("Successfully schedule %s of %s to %s.", pod.Name, pod.ClusterId, destClusterId)
	return destClusterId
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
