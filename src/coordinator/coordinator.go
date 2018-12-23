package main

import (
	"coordinator/scheduler"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"types"

	"github.com/golang/glog"
)

var (
	pendingPodCh chan types.InterPod
	idleNodeCh   chan types.InterNode
)

func init() {
	pendingPodCh = make(chan types.InterPod, 10)
	idleNodeCh = make(chan types.InterNode, 10)
}

type Server int

func (t *Server) RegisterCluster(cluster *types.Cluster, reply *int) error {
	scheduler.RegisterCluster(*cluster)
	glog.Info("Register cluster:", cluster)
	*reply = 1
	return nil
}

func (t *Server) UploadPod(pod *types.InterPod, reply *int) error {
	pendingPodCh <- *pod
	glog.Info("UploadPod:", pod)
	*reply = 1
	return nil
}

func (t *Server) UploadNode(node *types.InterNode, reply *int) error {
	idleNodeCh <- *node
	glog.Info("UploadNode:", node)
	*reply = 1
	return nil
}

func (t *Server) UnloadNode(cluster *types.Cluster, node *types.InterNode) error {
	var err error
	*node, err = scheduler.UnloadNode((*cluster).Id)
	if err == nil {
		glog.Info("UnloadNode:", node)
	}
	return err
}

func main() {
	// setup glog
	flag.Parse()
	flag.Lookup("log_dir").Value.Set("./log")
	defer glog.Flush()

	glog.Info("scheduler starts.")

	rpc.Register(new(Server))
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":1234")
	if err != nil {
		fmt.Println(err)
	}
	go http.Serve(listener, nil)
	go scheduler.DispatchPods(pendingPodCh)
	go scheduler.DispatchNodes(idleNodeCh)
	scheduler.Schedule()
}
