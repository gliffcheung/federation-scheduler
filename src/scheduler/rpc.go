package scheduler

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"types"

	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
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

func (t *Server) CreatePod(pod *v1.Pod, reply *int) error {
	err := createPod(*pod)
	if err == nil {
		glog.Info("CreatePod:", pod)
	} else {
		glog.Error(err)
	}
	*reply = 1
	return err
}

func (t *Server) UploadResult(result *types.Result, reply *int) error {
	cli, err := rpc.DialHTTP("tcp", result.DestIp+":"+clientPort)
	if err == nil {
		glog.Info("UploadResult:", result)
	} else {
		glog.Error(err)
	}
	var reply2 int
	err = cli.Call("Server.CreatePod", &result.Pod, &reply2)
	if err == nil {
		glog.Info("Server.CreatePod:", result.Pod)
	} else {
		glog.Error(err)
	}
	return err
}

func init() {
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
}

func RegisterCluster() {
	cluster := types.Cluster{Id: clusterId, Ip: clientAddress}
	var reply int
	err := client.Call("Server.RegisterCluster", cluster, &reply)
	if err != nil {
		glog.Info(err)
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

func UploadNode(node types.Node) {
	interNode := &types.InterNode{Node: node, ClusterId: clusterId}
	var reply int
	err := client.Call("Server.UploadNode", interNode, &reply)
	if err != nil {
		glog.Info(err)
	}
}

func UnloadNode() (types.InterNode, error) {
	cluster := types.Cluster{Id: clusterId}
	var interNode types.InterNode
	err := client.Call("Server.UnloadNode", &cluster, &interNode)
	if err != nil {
		glog.Info(err)
	}
	return interNode, err
}
