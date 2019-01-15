package scheduler

import (
	"io/ioutil"
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
	var content string
	for result := range executeResultQ {
		stamp := podInfo[result.Name].CreationTimestamp
		createTime := strconv.FormatInt(stamp.ProtoTime().Seconds, 10)
		startTime := strconv.FormatInt(result.StartTime, 10)
		content += podInfo[result.Name].Namespace + "," + result.Name + "," + createTime + "," + startTime + "\n"
	}
	WriteWithIoutil(filename, content)
}

func WriteWithIoutil(filename, content string) {
	data := []byte(content)
	err := ioutil.WriteFile(filename, data, 0644)
	if err != nil {
		glog.Error()
	}
}
