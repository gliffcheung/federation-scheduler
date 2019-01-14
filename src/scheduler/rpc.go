package scheduler

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"time"
	"types"

	"github.com/golang/glog"
)

const (
	serverAddress = "localhost"
	serverPort    = "1234"
	clusterId     = "cluster1"
	clientAddress = "8.8.8.8"
	clientPort    = "4321"
)

var (
	client *rpc.Client
)

type Server int

func (t *Server) CreatePod(outsourcePod *types.OutsourcePod, reply *int) error {
	err := createPod(*outsourcePod)
	if err == nil {
		glog.Info("CreatePod:", outsourcePod.Pod)
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
		Pod:      podInfo[result.Pod.Name],
		SourceIP: clientAddress,
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

func (t *Server) ReturnExecuteResult(result *types.ExecuteResult, reply *int) error {
	executeResultQ <- *result
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

	// create server
	rpc.Register(new(Server))
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":"+clientPort)
	if err != nil {
		fmt.Println(err)
	}
	go http.Serve(listener, nil)
	go Heartbeat()
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
	for {
		nodes := getNodes()
		var idleResource types.Resource
		for _, node := range nodes {
			allocatedRes := allocatedResource[node.Name]
			idleResource.MilliCpu += node.MilliCpu - allocatedRes.MilliCpu
			idleResource.Memory += node.Memory - allocatedRes.Memory
		}
		cluster := types.Cluster{Id: clusterId, IdleResource: idleResource}
		var reply int
		err := client.Call("Server.Heartbeat", cluster, &reply)
		if err != nil {
			glog.Info(err)
		}
		time.Sleep(10 * time.Second)
	}
}

func UploadPod(pod types.Pod) {
	interPod := &types.InterPod{Pod: pod, ClusterId: clusterId}
	var reply int
	err := client.Call("Server.UploadPod", interPod, &reply)
	if err != nil {
		glog.Info(err)
	}
}

func ReturnExecuteResult(result types.ExecuteResult) {
	// connect to otherCluster
	var err error
	clusterIp := otherClustersPod[result.Pod.Name]
	cli, err := rpc.DialHTTP("tcp", clusterIp+":"+clientPort)
	if err != nil {
		glog.Info(err)
	}

	var reply int
	err = cli.Call("Server.ReturnExecuteResult", &result, &reply)
	if err != nil {
		glog.Info(err)
	}
}
