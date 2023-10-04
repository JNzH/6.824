package mr

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	IDLE               = "IDLE"
	IN_PROGRESS        = "IN_PROGRESS"
	COMPLETED          = "COMPLETED"
	MRTaskPhaseUnstart = 0
	MRTaskPhaseMap     = 1
	MRTaskPhaseReduce  = 2
	MRTaskPhaseDone    = 3
)

const MaxTaskRunTime = 10000 * time.Millisecond

type Master struct {
	mu                  sync.Mutex
	Tasks               map[string]MRTask
	WorkerStatus        map[string]*WorkerInfo
	files               []string
	UnassignedFiles     []string
	IntermediateFiles   []string
	CompletedReduceTask map[string]bool
	MRTaskPhase         int
	nReduce             int
	done                bool
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) RequestTask(args *RequestTaskInfo, reply *ReplyTaskInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	task := m.PickTaskFromQueue(args.WorkerId)
	//reply = &task
	reply.MRTask = task
	return nil
}

func (m *Master) RegisterWorker(_, info *WorkerInfo) error {
	Id := len(m.WorkerStatus) + 1
	info.WorkerId = strconv.Itoa(Id)
	m.WorkerStatus[info.WorkerId] = info
	return nil
}

func (m *Master) ReportTask(args *MRTask, _ *EmptyInterface) error {
	DebugTask("Receive ReportTask in server", args)
	if args.TaskStatus == COMPLETED {
		k := ""
		if args.TaskType == TaskMap {
			k = args.FileName
		} else if args.TaskType == TaskReduce {
			k = strconv.Itoa(args.ReduceTaskIndex)
		} else {
			fmt.Printf("Error in ReportTask: TaskType\n")
		}
		if v, ok := m.Tasks[k]; ok {
			v.TaskStatus = COMPLETED
			m.Tasks[k] = v
		}
	}
	return nil
}

// Schedule task mechanism
// 1. Assign all map task
// 2. Assign all reduce task after no map task remaining
// 3.
func (m *Master) ScheduleTask() {
	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Printf("ScheduleTask Called\n")

	allFinish := true
	for i, t := range m.Tasks {
		DebugTask(fmt.Sprintf("Task %s in queue", i), &t)
		switch t.TaskStatus {
		case IDLE:
			allFinish = false
		case IN_PROGRESS:
			allFinish = false
			//m.HeartBeatCheck(t)
		case COMPLETED:
		default:
			fmt.Printf("task status invalid")
		}
	}
	if allFinish {
		m.InitTaskQueue()
	}
}

func (m *Master) HeartBeatCheck(task MRTask) {
	if time.Now().Sub(task.StartTime) > MaxTaskRunTime {
		task.TaskStatus = IDLE
	}
}

func (m *Master) PickTaskFromQueue(WorkerId string) MRTask {
	task := MRTask{}
	for _, t := range m.Tasks {
		switch t.TaskStatus {
		case IDLE:
			t.TaskStatus = IN_PROGRESS
			t.StartTime = time.Now()
			task = t
		case IN_PROGRESS:
		case COMPLETED:
		default:
			fmt.Printf("task status invalid")
		}
	}
	DebugTask("PickTaskFromQueue", &task)
	return task
}

func (m *Master) InitTaskQueue() {
	if m.MRTaskPhase == MRTaskPhaseUnstart {
		m.MRTaskPhase = MRTaskPhaseMap
		m.Tasks = make(map[string]MRTask)
		for index, file := range m.files {
			m.Tasks[file] = MRTask{
				FileName:     file,
				NReduce:      m.nReduce,
				TaskStatus:   IDLE,
				TaskType:     TaskMap,
				MapTaskIndex: index,
			}
		}
	} else if m.MRTaskPhase == MRTaskPhaseMap {
		m.MRTaskPhase = MRTaskPhaseReduce
		m.Tasks = make(map[string]MRTask)
		for index := 0; index < m.nReduce; index++ {
			m.Tasks[strconv.Itoa(index)] = MRTask{
				TaskStatus:      IDLE,
				TaskType:        TaskReduce,
				NReduce:         m.nReduce,
				ReduceTaskIndex: index,
				MapDoneCount:    len(m.files),
			}
		}
		fmt.Printf("Init reduce task queue\n")
		//for index, file := range m.files {
		//	m.Tasks[file] = MRTask{
		//		TaskStatus:      IDLE,
		//		TaskType:        TaskMap,
		//		NReduce:         m.nReduce,
		//		ReduceTaskIndex: index,
		//		MapDoneCount:    len(m.files),
		//	}
		//}
	} else if m.MRTaskPhase == MRTaskPhaseReduce {
		m.MRTaskPhase = MRTaskPhaseDone
		m.done = true
	} else if m.MRTaskPhase == MRTaskPhaseDone {
		fmt.Printf("[Warning]: Potential phase loop causing")
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	ret := m.done
	fmt.Printf("Call Done() func -> %v\n", ret)
	return ret
}

func (m *Master) Schedule(wg *sync.WaitGroup) {
	for !m.Done() {
		go m.ScheduleTask()
		time.Sleep(100 * time.Millisecond)
	}
}

func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files:       files,
		nReduce:     nReduce,
		MRTaskPhase: MRTaskPhaseUnstart,
	}

	for _, file := range m.files {
		m.UnassignedFiles = append(m.UnassignedFiles, file)
	}
	m.InitTaskQueue()
	wg := sync.WaitGroup{}
	go m.Schedule(&wg)

	m.server()
	m.WorkerStatus = make(map[string]*WorkerInfo)
	return &m
}
