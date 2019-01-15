package scheduler

import (
	"os"
	"strconv"
	"types"

	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
)

var (
	podInfo        map[string]v1.Pod
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
		stamp := podInfo[result.Name].CreationTimestamp
		waitTime := result.StartTime - stamp.ProtoTime().Seconds
		content = podInfo[result.Name].Namespace + "," + result.Name + "," + strconv.FormatInt(result.RequestMemory, 10) + "," +
			strconv.FormatInt(result.RequestMilliCpu, 10) + "," + strconv.FormatInt(waitTime, 10) + "\n"
		buf := []byte(content)
		fd.Write(buf)
	}
	defer fd.Close()
}
