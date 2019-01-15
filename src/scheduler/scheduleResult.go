package scheduler

import (
	"os"
	"strconv"
	"strings"
	"types"

	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
)

var (
	podInfo        map[string]v1.Pod // local pod
	executeResultQ chan types.ExecuteResult
	//userConsumeResource map[string]types.Resource
)

func init() {
	podInfo = make(map[string]v1.Pod)
	executeResultQ = make(chan types.ExecuteResult, 10)
}

func HandleExecuteResult() {
	filename := "../scheduleResult.csv"
	fd, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		glog.Error()
	}
	var content string
	for result := range executeResultQ {
		podName := result.Name[strings.IndexAny(result.Name, "-")+1:]
		stamp := podInfo[podName].CreationTimestamp
		waitTime := result.StartTime - stamp.ProtoTime().Seconds
		content = podInfo[podName].Namespace + "," + podName + "," + strconv.FormatInt(result.RequestMemory, 10) + "," +
			strconv.FormatInt(result.RequestMilliCpu, 10) + "," + strconv.FormatInt(waitTime, 10) + "\n"
		buf := []byte(content)
		fd.Write(buf)
	}
	defer fd.Close()
}
