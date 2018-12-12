package scheduler

import "types"

var etcd map[string] float64

func init() {
	etcd = make(map[string] float64)
}

func fixUserShare(uid string, pod types.Pod) float64 {
	nodes := getNodes()
	var totalCpu, totalMmeory int64
	for _, node := range nodes {
		totalCpu += node.AllocatableMilliCpu
		totalMmeory += node.AllocatableMemory
	}
	dominantShare := min(float64(pod.RequestMilliCpu) / float64(totalCpu), float64(pod.RequestMemory) / float64(totalMmeory))
	etcd[uid] += dominantShare
	return etcd[uid]
}

func getUserShare(uid string) float64{
	share, ok := etcd[uid]
	if !ok {
		etcd[uid] = 0
	}
	return share
}

func min(x, y float64) float64 {
	if x < y {
		return x
	}
	return y
}