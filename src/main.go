package main

import (
	"flag"
	"scheduler"

	"github.com/golang/glog"
)

func main() {
	// setup glog
	flag.Parse()
	flag.Lookup("log_dir").Value.Set("../log")
	defer glog.Flush()

	glog.Info("scheduler starts.")
	scheduler.Init()
	scheduler.RpcInit()
	go scheduler.DispatchPods()
	go scheduler.Schedule()
	go scheduler.HandleExecuteResult()
	scheduler.WatchPods()
}
