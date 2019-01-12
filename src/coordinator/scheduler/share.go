package scheduler

import (
	"types"

	"github.com/golang/glog"
)

var (
	allocatedResource   map[string]types.Resource
	contributedResource map[string]types.Resource
	clustersShare       map[string]float64
)

func init() {
	allocatedResource = make(map[string]types.Resource)
	contributedResource = make(map[string]types.Resource)
	clustersShare = make(map[string]float64)
}

func printShare() {
	for k, v := range clustersShare {
		glog.Infof("%s's dominant share:%.2f", k, v)
	}
}

func fixClusterShare(pod types.InterPod) float64 {
	var res types.Resource
	res, ok := allocatedResource[pod.ClusterId]
	if ok {
		res.MilliCpu += pod.Pod.RequestMilliCpu
		res.Memory += pod.Pod.RequestMemory
		allocatedResource[pod.ClusterId] = res
	} else {
		res.MilliCpu = pod.Pod.RequestMilliCpu
		res.Memory = pod.Pod.RequestMemory
		allocatedResource[pod.ClusterId] = res
	}
	dominantShare := max(float64(allocatedResource[pod.ClusterId].Memory)/float64(totalResource.Memory), float64(allocatedResource[pod.ClusterId].MilliCpu)/float64(totalResource.MilliCpu))
	clustersShare[pod.ClusterId] = dominantShare
	return dominantShare
}

func fixContributedResource(pod types.InterPod, clusterId string) {
	var res types.Resource
	res, ok := contributedResource[pod.ClusterId]
	if ok {
		res.MilliCpu += pod.Pod.RequestMilliCpu
		res.Memory += pod.Pod.RequestMemory
		contributedResource[pod.ClusterId] = res
	} else {
		res.MilliCpu = pod.Pod.RequestMilliCpu
		res.Memory = pod.Pod.RequestMemory
		contributedResource[pod.ClusterId] = res
	}
}

func getClusterShare(id string) float64 {
	share, _ := clustersShare[id]
	return share
}

func max(x, y float64) float64 {
	if x > y {
		return x
	}
	return y
}
