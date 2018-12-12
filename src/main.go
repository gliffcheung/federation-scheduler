package main

import (
	"flag"
	"github.com/golang/glog"

	"scheduler"
)

func main()  {
	// setup glog
	flag.Parse()
	flag.Lookup("log_dir").Value.Set("../log")
	defer glog.Flush()

	go scheduler.DispatchPods()
	go scheduler.Schedule()
	for{}
}