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
		glog.Infof("%s's share:%.2f", k, v)
	}
}

func fixClusterShare(pod types.InterPod, discount float64, destClusterId string) float64 {
	share := Max(float64(pod.RequestMilliCpu), float64(pod.RequestMemory)) * discount
	clustersShare[pod.ClusterId] -= share
	clustersShare[destClusterId] += share
	return clustersShare[pod.ClusterId]
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
