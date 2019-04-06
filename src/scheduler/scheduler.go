package scheduler

import (
	"container/heap"
	"time"
	"types"

	"github.com/golang/glog"
)

var (
	usersPriorityQ    types.PriorityQueue
	usersPresent      map[string]bool //userPresent[Uid] == true means that the Uid has been in usersPriorityQ.
	usersActiveQ      chan string
	usersPodsQ        map[string]chan types.Pod
	highPriorityCh    chan types.Pod
	secHighPriorityCh chan types.Pod
)

func init() {
	usersPresent = make(map[string]bool)
	usersActiveQ = make(chan string, 10)
	usersPodsQ = make(map[string]chan types.Pod)
	highPriorityCh = make(chan types.Pod, 500)
	secHighPriorityCh = make(chan types.Pod, 500)
}

func DispatchPods() {
	for pod := range pendingPodCh {
		value, ok := usersPodsQ[pod.Uid]
		if !ok {
			usersPodsQ[pod.Uid] = make(chan types.Pod, 500)
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
	Loop:
		// schedule pod in highPriorityCh at first
		select {
		case pod := <-highPriorityCh:
			schedulePod(pod, false)
			if len(usersActiveQ) > 0 {
				Heartbeat(false)
			}
			goto Loop
		default:
		}

		// schedule pod in secHighPriorityCh next
		select {
		case pod := <-secHighPriorityCh:
			schedulePod(pod, false)
			Heartbeat(false)
			goto Loop
		default:
		}

		// fix usersPriorityQ
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

		// schedule local pod
		if len(usersPriorityQ) > 0 {
			Heartbeat(false)
			topUser := heap.Pop(&usersPriorityQ).(*types.User)
			select {
			case firstPod := <-usersPodsQ[topUser.Uid]:
				glog.Info("=============================")
				glog.Info("Before Schedule()")
				printShare()
				weight := schedulePod(firstPod, true)
				topUser.Priority = fixUserShare(firstPod, weight)
				heap.Push(&usersPriorityQ, topUser)
				glog.Info("After Schedule()")
				printShare()
				glog.Info("=============================")
				userData := types.UserData{
					Uid:         firstPod.Uid,
					CurrentTime: time.Now().Unix(),
					Share:       topUser.Priority,
					Resource:    usersAllocatedRes[firstPod.Uid],
				}
				userDataQ <- userData
			default:
				usersPresent[topUser.Uid] = false
			}
		} else {
			Heartbeat(true)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func schedulePod(pod types.Pod, outsource bool) float64 {
	for {
		nodes := getNodes()
		for _, node := range nodes {
			res := allocatedResource[node.Name]
			if res.MilliCpu+pod.RequestMilliCpu <= node.MilliCpu && res.Memory+pod.RequestMemory <= node.Memory {
				schedulePodToNode(pod, node)
				return 0
			}
		}
		if local == false && outsource == true {
			// if cluster doesn't have enough resourse, outsource the pod.
			weight := UploadPod(pod)
			deletePodByName(pod.Name, pod.Uid)
			return weight
		}
		time.Sleep(time.Second)
	}
}
