package scheduler

import (
	"container/heap"
	"time"
	"types"

	"github.com/golang/glog"
)

var (
	usersPriorityQ types.PriorityQueue
	usersPresent   map[string]bool //userPresent[Uid] == true means that the Uid has been in usersPriorityQ.
	usersActiveQ   chan string
	usersPodsQ     map[string]chan types.Pod
)

func init() {
	usersPresent = make(map[string]bool)
	usersActiveQ = make(chan string, 10)
	usersPodsQ = make(map[string]chan types.Pod)
}

func DispatchPods() {
	for pod := range pendingPodCh {
		value, ok := usersPodsQ[pod.Uid]
		if !ok {
			usersPodsQ[pod.Uid] = make(chan types.Pod, 20)
			value = usersPodsQ[pod.Uid]
		}
		if len(value) == 0 {
			usersActiveQ <- pod.Uid
		}
		value <- pod
	}
}

func Schedule() {
	for {
		// fix UsersPriorityQ
		usersActiveQLen := len(usersActiveQ)
		for i := 0; i < usersActiveQLen; i++ {
			uid := <-usersActiveQ
			present, ok := usersPresent[uid]
			if ok && present {
				continue
			} else {
				usersPresent[uid] = true
				user := &types.User{
					Uid:      uid,
					Priority: getUserShare(uid),
				}
				heap.Push(&usersPriorityQ, user)
			}
		}

		// schedule pod
		if len(usersPriorityQ) > 0 {
			topUser := heap.Pop(&usersPriorityQ).(*types.User)
			select {
			case firstPod := <-usersPodsQ[topUser.Uid]:
				glog.Info("=============================")
				glog.Info("Before Schedule()")
				printShare()
				schedulePod(firstPod)
				topUser.Priority = fixUserShare(topUser.Uid, firstPod)
				heap.Push(&usersPriorityQ, topUser)
				glog.Info("After Schedule()")
				printShare()
				glog.Info("=============================")
			default:
				usersPresent[topUser.Uid] = false
			}
		} else {
			// cluster is idle, and its resource could be shared.
		}
		time.Sleep(3 * time.Second)
	}
}

func schedulePod(pod types.Pod) {
	for {
		nodes := getNodes()
		for _, node := range nodes {
			res := allocatedResource[node.Name]
			if res.allocatedMilliCpu+pod.RequestMilliCpu <= node.AllocatableMilliCpu &&
				res.allocatedMemory+pod.RequestMemory <= node.AllocatableMemory {
				schedulePodToNode(pod, node)
				return
			}
		}
		time.Sleep(3 * time.Second)
	}
	// if cluster doesn't have enough resourse, outsource the pod.
	/*
		To-do
	*/
}
