package scheduler

import (
	"fmt"
	"sort"
	"types"

	"github.com/golang/glog"
)

var (
	clustersIdleNode         map[string][]types.InterNode
	clustersContributedShare map[string]float64
)

func init() {
	clustersIdleNode = make(map[string][]types.InterNode)
	clustersContributedShare = make(map[string]float64)
}

func DispatchNodes(idleNodeCh chan types.InterNode) {
	for node := range idleNodeCh {
		clustersIdleNode[node.ClusterId] = append(clustersIdleNode[node.ClusterId], node)
		_, ok := clustersContributedShare[node.ClusterId]
		if !ok {
			clustersContributedShare[node.ClusterId] = 0
		}
		totalCpu += node.AllocatableMilliCpu
		totalMemory += node.AllocatableMemory
	}
}

func UnloadNode(clusterId string) (types.InterNode, error) {
	if len(clustersIdleNode[clusterId]) == 0 {
		err := fmt.Errorf("%s doesn't have any node", clusterId)
		return types.InterNode{}, err
	} else {
		node := clustersIdleNode[clusterId][len(clustersIdleNode[clusterId])-1]
		clustersIdleNode[clusterId] = clustersIdleNode[clusterId][0 : len(clustersIdleNode[clusterId])-1]
		return node, nil
	}
}

func getNodes() []types.InterNode {
	var clustersCS types.ClusterSlice
	for k, v := range clustersContributedShare {
		cluster := types.Cluster{Id: k, ContributedShare: v}
		clustersCS = append(clustersCS, cluster)
	}
	sort.Sort(clustersCS)
	nodes := make([]types.InterNode, 0)
	for _, cluster := range clustersCS {
		nodes = append(nodes, clustersIdleNode[cluster.Id]...)
	}
	return nodes
}

func fixClusterContributedShare(clusterId string, pod types.InterPod) float64 {
	clustersContributedShare[clusterId] += max(float64(pod.RequestMilliCpu)/float64(totalCpu), float64(pod.RequestMemory)/float64(totalMemory))
	glog.Infof("%s's contributed share:%.2f", clusterId, clustersContributedShare[clusterId])
	return clustersContributedShare[clusterId]
}
