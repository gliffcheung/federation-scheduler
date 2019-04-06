package scheduler

import (
	"os"
	"strconv"
	"time"
	"types"

	"github.com/golang/glog"
)

var (
	clusterDataQ chan types.UserData
	startTime    int64
)

func init() {
	clusterDataQ = make(chan types.UserData, 500)
	startTime = time.Now().Unix()
}

func HandleClusterData() {
	filename := "../../federationData.csv"
	fd, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		glog.Error()
	}
	var content string
	for data := range clusterDataQ {
		content = data.Uid + "," + strconv.FormatInt(data.CurrentTime-startTime, 10) + "," + strconv.FormatFloat(data.DC, 'f', 4, 64) + "," +
			strconv.FormatFloat(data.DS, 'f', 4, 64) + "," + strconv.FormatFloat(data.Share, 'f', 4, 64) + "," + "\n"
		buf := []byte(content)
		fd.Write(buf)
	}
	defer fd.Close()
}
