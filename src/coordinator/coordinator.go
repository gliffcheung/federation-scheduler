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

const (
	clientPort = "4321"
)

var (
	pendingPodCh chan types.InterPod
)

func init() {
	pendingPodCh = make(chan types.InterPod, 10)
}

type Server int

func (t *Server) RegisterCluster(cluster *types.Cluster, reply *int) error {
	scheduler.RegisterCluster(*cluster)
	glog.Info("Register cluster:", cluster)
	*reply = 1
	return nil
}

func (t *Server) Heartbeat(cluster *types.Cluster, reply *int) error {
	scheduler.UpdateCluster(*cluster)
	glog.Info("Update cluster:", cluster)
	*reply = 1
	return nil
}

func (t *Server) UploadPod(pod *types.InterPod, reply *int) error {
	pendingPodCh <- *pod
	glog.Info("UploadPod:", pod)
	*reply = 1
	return nil
}

func main() {
	// setup glog
	flag.Parse()
	flag.Lookup("log_dir").Value.Set("./log")
	defer glog.Flush()

	glog.Info("scheduler starts.")

	// create server
	rpc.Register(new(Server))
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":1234")
	if err != nil {
		fmt.Println(err)
	}
	go http.Serve(listener, nil)
	go scheduler.DispatchPods(pendingPodCh)
	scheduler.Schedule()
}

func ReturnScheduleResult(result *types.ScheduleResult, sourceIp string) {
	cli, err := rpc.DialHTTP("tcp", sourceIp+":"+clientPort)
	if err == nil {
		glog.Info("ReturnScheduleResult:", result, " to ", sourceIp)
	} else {
		glog.Error(err)
	}
	var reply int
	err = cli.Call("Server.ReturnExecuteResult", result, &reply)
	if err != nil {
		glog.Error(err)
	}
}
