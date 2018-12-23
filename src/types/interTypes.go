package types

type InterPod struct {
	Pod
	ClusterId string
}

type InterNode struct {
	Node
	ClusterId         string
	AllocatedMilliCpu int64
	AllocatedMemory   int64
}

type Cluster struct {
	Id               string
	Priority         float64
	index            int
	Ip               string
	ContributedShare float64
}

type ClustersPriorityQueue []*Cluster

func (pq ClustersPriorityQueue) Len() int { return len(pq) }

func (pq ClustersPriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the lowest Priority, so we use smaller than here.
	return pq[i].Priority < pq[j].Priority
}

func (pq ClustersPriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *ClustersPriorityQueue) Push(x interface{}) {
	n := len(*pq)
	cluster := x.(*Cluster)
	cluster.index = n
	*pq = append(*pq, cluster)
}

func (pq *ClustersPriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	cluster := old[n-1]
	cluster.index = -1 // for safety
	*pq = old[0 : n-1]
	return cluster
}

type ClusterSlice []Cluster

func (c ClusterSlice) Len() int      { return len(c) }
func (c ClusterSlice) Swap(i, j int) { c[i], c[j] = c[j], c[i] }
func (c ClusterSlice) Less(i, j int) bool {
	return c[i].ContributedShare < c[j].ContributedShare
}
