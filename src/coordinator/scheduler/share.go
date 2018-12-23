package scheduler

import (
	"types"

	"github.com/golang/glog"
)

var (
	clustersShare         map[string]float64
	totalCpu, totalMemory int64
)

func init() {
	clustersShare = make(map[string]float64)
}

func printShare() {
	for k, v := range clustersShare {
		glog.Infof("%s's dominant share:%.2f", k, v)
	}
}

func fixClusterShare(id string, pod types.InterPod) float64 {
	contributedShare := fixClusterContributedShare(id, pod)
	dominantShare := max(float64(pod.RequestMilliCpu)/float64(totalCpu), float64(pod.RequestMemory)/float64(totalMemory)) - contributedShare
	share, _ := clustersShare[pod.ClusterId]
	clustersShare[id] = share + dominantShare
	return clustersShare[id]
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
