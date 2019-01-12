package scheduler

import (
	"types"

	"github.com/golang/glog"
)

var (
	usersShare            map[string]float64
	usersAllocatedRes     map[string]types.Resource
	totalCpu, totalMemory int64
)

func init() {
	usersShare = make(map[string]float64)
	usersAllocatedRes = make(map[string]types.Resource)
}

func initShare() {
	namespaces := getNamespaces()
	for _, ns := range namespaces {
		usersShare[ns] = 0
	}
	nodes := getNodes()
	for _, node := range nodes {
		totalCpu += node.MilliCpu
		totalMemory += node.Memory
	}
	pods := getRunningPods()
	for _, pod := range pods {
		var res types.Resource
		res, ok := usersAllocatedRes[pod.Uid]
		if ok {
			res.MilliCpu += pod.RequestMilliCpu
			res.Memory += pod.RequestMemory
		} else {
			res.MilliCpu = pod.RequestMilliCpu
			res.Memory = pod.RequestMemory
		}
		usersAllocatedRes[pod.Uid] = res
		usersShare[pod.Uid] = max(float64(res.MilliCpu)/float64(totalCpu), float64(res.Memory)/float64(totalMemory))
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
