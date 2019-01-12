package scheduler

import (
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
	for result := range executeResultQ {
		glog.Warning(result)
	}
}
