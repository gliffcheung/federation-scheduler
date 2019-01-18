package scheduler

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"types"

	"github.com/golang/glog"
)

const (
	serverAddress = "localhost"
	serverPort    = "1234"
	clusterId     = "cluster1"
	clientAddress = "localhost"
	clientPort    = "4321"
)

var (
	client *rpc.Client
)

type Server int

func (t *Server) CreatePod(outsourcePod *types.OutsourcePod, reply *int) error {
	err := createPod(*outsourcePod)
	if err == nil {
		glog.Info("CreatePod:", outsourcePod.Pod.Name)
	} else {
		glog.Error(err)
	}
	*reply = 1
	return err
}

func (t *Server) ReturnScheduleResult(result *types.ScheduleResult, reply *int) error {
	cli, err := rpc.DialHTTP("tcp", result.DestIp+":"+clientPort)
	if err == nil {
		glog.Info("UploadResult:", result)
	} else {
		glog.Error(err)
	}
	// create a outsourcePod
	var reply2 int
	var requestsMilliCpu, requestsMemory int64
	pod := podInfo[result.Pod.Name]
	for _, ctn := range pod.Spec.Containers {
		requestsMilliCpu += ctn.Resources.Requests.Cpu().MilliValue()
		requestsMemory += ctn.Resources.Requests.Memory().Value() / 1024 / 1024
	}
	outsourcePod := types.OutsourcePod{
		Pod:       podInfo[result.Pod.Name],
		ClusterId: clusterId,
		SourceIP:  clientAddress,
		Resource: types.Resource{
			MilliCpu: requestsMilliCpu,
			Memory:   requestsMemory,
		},
	}
	err = cli.Call("Server.CreatePod", &outsourcePod, &reply2)
	if err == nil {
		glog.Info("Server.CreatePod:", result.Pod)
	} else {
		glog.Error(err)
	}
	return err
}

func (t *Server) ReturnScheduleData(result *types.ScheduleData, reply *int) error {
	scheduleDataQ <- *result
	*reply = 1
	return nil
}

func RpcInit() {
	// connect to coordinator
	var err error
	client, err = rpc.DialHTTP("tcp", serverAddress+":"+serverPort)
	if err != nil {
		glog.Info(err)
	}
	RegisterCluster()
	Heartbeat()

	// create server
	rpc.Register(new(Server))
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":"+clientPort)
	if err != nil {
		fmt.Println(err)
	}
	go http.Serve(listener, nil)
}

func RegisterCluster() {
	nodes := getNodes()
	var totalResource types.Resource
	for _, node := range nodes {
		totalResource.MilliCpu += node.MilliCpu
		totalResource.Memory += node.Memory
	}
	cluster := types.Cluster{Id: clusterId, Ip: clientAddress, TotalResource: totalResource}
	var reply int
	err := client.Call("Server.RegisterCluster", cluster, &reply)
	if err != nil {
		glog.Info(err)
	}
}

func Heartbeat() {
	nodes := getNodes()
	var mostCpuNode, mostMemoryNode types.InterNode
	var mostCpu, mostMemory int64
	mostCpu = 0
	mostMemory = 0
	for _, node := range nodes {
		allocatedRes := allocatedResource[node.Name]
		idleCpu := node.MilliCpu - allocatedRes.MilliCpu
		idleMemory := node.Memory - allocatedRes.Memory
		if idleCpu > mostCpu {
			mostCpuNode.Node = node
			mostCpuNode.IdleResource.Memory = idleMemory
			mostCpuNode.IdleResource.MilliCpu = idleCpu
			mostCpu = idleCpu
		}
		if idleMemory > mostMemory {
			mostMemoryNode.Node = node
			mostMemoryNode.IdleResource.Memory = idleMemory
			mostMemoryNode.IdleResource.MilliCpu = idleCpu
			mostMemory = idleMemory
		}
	}
	idleNodes := make([]types.InterNode, 0)
	mostCpuNode.ClusterId = clusterId
	idleNodes = append(idleNodes, mostCpuNode)
	if mostCpuNode.Node.Name != mostMemoryNode.Node.Name {
		mostMemoryNode.ClusterId = clusterId
		idleNodes = append(idleNodes, mostMemoryNode)
	}
	cluster := types.Cluster{Id: clusterId, IdleNodes: idleNodes}
	var reply int
	err := client.Call("Server.Heartbeat", cluster, &reply)
	if err != nil {
		glog.Info(err)
	}
}

func UploadPod(pod types.Pod) float64 {
	interPod := &types.InterPod{Pod: pod, ClusterId: clusterId}
	var reply float64
	err := client.Call("Server.UploadPod", interPod, &reply)
	if err != nil {
		glog.Info(err)
	}
	return reply
}

func ReturnScheduleData(result types.ScheduleData) {
	// connect to otherCluster
	var err error
	clusterIp := otherClustersPod[result.Pod.Name]
	cli, err := rpc.DialHTTP("tcp", clusterIp+":"+clientPort)
	if err != nil {
		glog.Info(err)
	}

	var reply int
	err = cli.Call("Server.ReturnScheduleData", &result, &reply)
	if err != nil {
		glog.Info(err)
	}
}
