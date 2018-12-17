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
	go scheduler.DispatchPods()
	go scheduler.Schedule()
	scheduler.WatchPods()
}
