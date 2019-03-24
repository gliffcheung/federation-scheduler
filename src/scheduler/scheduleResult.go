package scheduler

import (
	"os"
	"strconv"
	"strings"
	"time"
	"types"

	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
)

var (
	podInfo             map[string]v1.Pod // local pod
	scheduleDataQ       chan types.ScheduleData
	executeDataQ        chan types.ExecuteData
	userDataQ           chan types.UserData
	startTime           int64
	usedCpu, usedMemory int64
)

func init() {
	podInfo = make(map[string]v1.Pod)
	scheduleDataQ = make(chan types.ScheduleData, 10)
	executeDataQ = make(chan types.ExecuteData, 10)
	userDataQ = make(chan types.UserData, 10)
	startTime = time.Now().Unix()
}

func HandleData() {
	go HandleScheduleData()
	go HandleExecuteData()
	go HandleUserData()
}

func HandleScheduleData() {
	filename := "../scheduleData.csv"
	fd, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		glog.Error()
	}
	var content string
	var totalWaitTime int64
	totalWaitTime = 0
	for data := range scheduleDataQ {
		podName := data.Name[strings.IndexAny(data.Name, "-")+1:]
		stamp := podInfo[podName].CreationTimestamp
		waitTime := data.StartTime - stamp.ProtoTime().Seconds
		totalWaitTime += waitTime
		content = strconv.FormatInt(time.Now().Unix()-startTime, 10) + "," + podInfo[podName].Namespace + "," + podName +
			"," + strconv.FormatInt(stamp.ProtoTime().Seconds, 10) + "," + strconv.FormatInt(data.CreateTime, 10) +
			"," + strconv.FormatInt(data.StartTime, 10) + "," + strconv.FormatInt(totalWaitTime, 10) + "\n"
		buf := []byte(content)
		fd.Write(buf)
	}
	defer fd.Close()
}

func HandleExecuteData() {
	filename := "../clusterData.csv"
	fd, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		glog.Error()
	}
	var content string
	for data := range executeDataQ {
		if data.Status == "running" {
			usedCpu += data.RequestMilliCpu
			usedMemory += data.RequestMemory
		} else if data.Status == "finish" {
			usedCpu -= data.RequestMilliCpu
			usedMemory -= data.RequestMemory
		}
		cpuUsedRate := float64(usedCpu) / float64(totalCpu)
		memUsedRate := float64(usedMemory) / float64(totalMemory)
		content = strconv.FormatInt(data.CurrentTime-startTime, 10) + "," + strconv.FormatFloat(cpuUsedRate, 'f', 4, 64) + "," +
			strconv.FormatFloat(memUsedRate, 'f', 4, 64) + "\n"
		buf := []byte(content)
		fd.Write(buf)
	}
	defer fd.Close()
}

func HandleUserData() {
	filename := "../userData.csv"
	fd, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		glog.Error()
	}
	var content string
	for data := range userDataQ {
		content = data.Uid + "," + strconv.FormatInt(data.CurrentTime-startTime, 10) + "," + strconv.FormatFloat(data.Share, 'f', 4, 64) + "," +
			strconv.FormatInt(data.MilliCpu, 10) + "," + strconv.FormatInt(data.Memory, 10) + "," + "\n"
		buf := []byte(content)
		fd.Write(buf)
	}
	defer fd.Close()
}
