package scheduler

import (
	"types"

	"github.com/golang/glog"
)

var (
	usersShare            map[string]float64
	usersAllocatedRes     map[string]types.Resource
	usersWeight           map[string]float64
	totalCpu, totalMemory int64
)

func init() {
	usersShare = make(map[string]float64)
	usersAllocatedRes = make(map[string]types.Resource)
	usersWeight = make(map[string]float64)
}

func initShare() {
	namespaces := getNamespaces()
	for _, ns := range namespaces {
		var res types.Resource
		usersAllocatedRes[ns] = res
		usersShare[ns] = 0
		usersWeight[ns] = 1
	}
	nodes := getNodes()
	for _, node := range nodes {
		totalCpu += node.MilliCpu
		totalMemory += node.Memory
	}
	pods := getRunningPods()
	for _, pod := range pods {
		res := usersAllocatedRes[pod.Uid]
		res.MilliCpu += pod.RequestMilliCpu
		res.Memory += pod.RequestMemory
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

func fixUserShare(pod types.Pod, weight float64) float64 {
	res := usersAllocatedRes[pod.Uid]
	res.MilliCpu += pod.RequestMilliCpu
	res.Memory += pod.RequestMemory
	usersAllocatedRes[pod.Uid] = res
	w := usersWeight[pod.Uid]
	w += weight
	dominantShare := max(float64(res.MilliCpu)/float64(totalCpu), float64(res.Memory)/float64(totalMemory)) / w
	usersShare[pod.Uid] = dominantShare
	return dominantShare
}

func getUserShare(uid string) float64 {
	return usersShare[uid]
}

func max(x, y float64) float64 {
	if x >= y {
		return x
	}
	return y
}
