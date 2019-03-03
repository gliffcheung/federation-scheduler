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
		glog.Infof("%s's allocated resource:%v", k, allocatedResource[k])
		glog.Infof("%s's contributed resource:%v", k, contributedResource[k])
		glog.Infof("%s's dominant share:%.2f", k, v)
	}
}

func fixClusterShare(pod types.InterPod) float64 {
	allocRes := allocatedResource[pod.ClusterId]
	allocRes.MilliCpu += pod.RequestMilliCpu
	allocRes.Memory += pod.RequestMemory
	allocatedResource[pod.ClusterId] = allocRes
	//contRes := contributedResource[pod.ClusterId]
	//dominantContribution := Max(float64(contRes.MilliCpu)/float64(TotalResource.MilliCpu), float64(contRes.Memory)/float64(TotalResource.Memory))
	dominantShare := Max(float64(allocRes.MilliCpu)/float64(TotalResource.MilliCpu), float64(allocRes.Memory)/float64(TotalResource.Memory))
	clustersShare[pod.ClusterId] = dominantShare
	return dominantShare
}

func fixContributedResource(pod types.InterPod, clusterId string) {
	res := contributedResource[clusterId]
	res.MilliCpu += pod.RequestMilliCpu
	res.Memory += pod.RequestMemory
	contributedResource[clusterId] = res
}

func getClusterShare(id string) float64 {
	return clustersShare[id]
}

func Max(x, y float64) float64 {
	if x > y {
		return x
	}
	return y
}
