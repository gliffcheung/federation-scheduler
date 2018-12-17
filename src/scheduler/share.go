package scheduler

import (
	"types"

	"github.com/golang/glog"
)

var (
	usersShare            map[string]float64
	totalCpu, totalMemory int64
)

func init() {
	usersShare = make(map[string]float64)
}

func initShare() {
	namespaces := getNamespaces()
	for _, ns := range namespaces {
		usersShare[ns] = 0
	}
	nodes := getNodes()
	for _, node := range nodes {
		totalCpu += node.AllocatableMilliCpu
		totalMemory += node.AllocatableMemory
	}
	pods := getRunningPods()
	for _, pod := range pods {
		dominantShare := max(float64(pod.RequestMilliCpu)/float64(totalCpu), float64(pod.RequestMemory)/float64(totalMemory))
		share, _ := usersShare[pod.Uid]
		usersShare[pod.Uid] = share + dominantShare
	}
	glog.Info("share is completed.")
}

func printShare() {
	for k, v := range usersShare {
		glog.Infof("%s's dominant share:%.2f", k, v)
	}
}

func fixUserShare(uid string, pod types.Pod) float64 {
	dominantShare := max(float64(pod.RequestMilliCpu)/float64(totalCpu), float64(pod.RequestMemory)/float64(totalMemory))
	share, _ := usersShare[pod.Uid]
	usersShare[uid] = share + dominantShare
	return usersShare[uid]
}

func getUserShare(uid string) float64 {
	share, _ := usersShare[uid]
	return share
}

func max(x, y float64) float64 {
	if x > y {
		return x
	}
	return y
}
