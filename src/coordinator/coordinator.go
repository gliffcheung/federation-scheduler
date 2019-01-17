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
	glog.Infof("Register cluster:%s, ip:%s, totalResource:%v", cluster.Id, cluster.Ip, cluster.TotalResource)
	scheduler.RegisterCluster(*cluster)
	*reply = 1
	return nil
}

func (t *Server) Heartbeat(cluster *types.Cluster, reply *int) error {
	scheduler.UpdateCluster(*cluster)
	*reply = 1
	return nil
}

func (t *Server) UploadPod(pod *types.InterPod, reply *float64) error {
	pendingPodCh <- *pod
	*reply = scheduler.Max(float64(pod.RequestMilliCpu)/float64(scheduler.TotalResource.MilliCpu), float64(pod.RequestMemory)/float64(scheduler.TotalResource.Memory))
	glog.Infof("UploadPod:%v, reply:%f", *pod, *reply)
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
