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
	FAILED             = "FAILED"
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
	//m.mu.Lock()
	//defer m.mu.Unlock()

	task := m.PickTaskFromQueue(args.WorkerId)
	task.WorkerId = args.WorkerId
	//reply = &task
	reply.MRTask = task
	//if task.TaskType == TaskMap {
	//	fmt.Printf("[%v] Assign map task %s to WorkerId %s\n", time.Now(), task.FileName, task.WorkerId)
	//} else if task.TaskType == TaskReduce {
	//	fmt.Printf("[%v] Assign reduce task %d to WorkerId %s\n", time.Now(), task.ReduceTaskIndex, task.WorkerId)
	//} else {
	//	fmt.Printf("[%v] Assign unknown task %d to WorkerId %s\n", time.Now(), task.ReduceTaskIndex, task.WorkerId)
	//}
	return nil
}

func (m *Master) RegisterWorker(_, info *WorkerInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	Id := len(m.WorkerStatus) + 1
	info.WorkerId = strconv.Itoa(Id)
	m.WorkerStatus[info.WorkerId] = info
	return nil
}

func (m *Master) ReportTask(args *MRTask, _ *EmptyInterface) error {
	//DebugTask("Receive ReportTask in server", args)
	m.mu.Lock()
	defer m.mu.Unlock()

	k := ""
	if args.TaskType == TaskMap {
		k = args.FileName
	} else if args.TaskType == TaskReduce {
		k = strconv.Itoa(args.ReduceTaskIndex)
	} else {
		fmt.Printf("Error in ReportTask: TaskType\n")
	}

	if args.TaskStatus == COMPLETED {
		if v, ok := m.Tasks[k]; ok {
			v.TaskStatus = COMPLETED
			m.Tasks[k] = v
		}
	} else if args.TaskStatus == FAILED {
		if v, ok := m.Tasks[k]; ok {
			v.TaskStatus = IDLE
			m.Tasks[k] = v
		}
	}

	//go m.ScheduleTask()
	return nil
}

// Schedule task mechanism
// 1. Assign all map task
// 2. Assign all reduce task after no map task remaining
// 3.
func (m *Master) ScheduleTask() {
	m.mu.Lock()
	defer m.mu.Unlock()

	//fmt.Printf("ScheduleTask Called\n")

	allFinish := true
	for k, t := range m.Tasks {
		//DebugTask(fmt.Sprintf("Task %s in queue", i), &t)
		switch t.TaskStatus {
		case IDLE:
			allFinish = false
		case IN_PROGRESS:
			allFinish = false
			if !m.HeartBeatCheck(t) {
				t.TaskStatus = IDLE
				m.Tasks[k] = t
			}
		case COMPLETED:
		default:
			fmt.Printf("task status invalid")
		}
	}
	if allFinish {
		m.InitTaskQueue()
	}
}

func (m *Master) CheckTaskQueue(stage string) {
	fmt.Printf("Check task queue in stage %s\n", stage)
	idx := 0
	for k, v := range m.Tasks {
		DebugTask(fmt.Sprintf("Key: %s index: %d", k, idx), &v)
		idx++
	}
}

func (m *Master) HeartBeatCheck(task MRTask) bool {
	if time.Now().Sub(task.StartTime) > MaxTaskRunTime {
		fmt.Printf("Trigger max runtime %s %d\n", task.FileName, task.ReduceTaskIndex)
		return false
	}
	return true
}

func (m *Master) PickTaskFromQueue(WorkerId string) MRTask {
	m.mu.Lock()
	defer m.mu.Unlock()

	task := MRTask{}
	if m.MRTaskPhase == MRTaskPhaseDone {
		task.TaskType = TaskQuit
		return task
	}

	found := false

	for k, t := range m.Tasks {
		switch t.TaskStatus {
		case IDLE:
			t.TaskStatus = IN_PROGRESS
			t.StartTime = time.Now()
			t.WorkerId = WorkerId
			task = t
			m.Tasks[k] = task
			//tmp := m.Tasks[k]
			found = true
			//DebugTask("After update tmp", &tmp)
			//DebugTask("After update task", &task)
		case IN_PROGRESS:
		case COMPLETED:
		default:
			fmt.Printf("task status invalid")
		}
		if found {
			break
		}
	}
	if !found {
		task.TaskType = TaskNull
	}
	return task
}

func (m *Master) InitTaskQueue() {
	if m.MRTaskPhase == MRTaskPhaseUnstart {
		m.MRTaskPhase = MRTaskPhaseMap
		m.Tasks = make(map[string]MRTask)
		//fmt.Printf("Init TaskQueue MRTaskPhaseMap\n")
		for index, file := range m.files {
			t := MRTask{
				FileName:     file,
				NReduce:      m.nReduce,
				TaskStatus:   IDLE,
				TaskType:     TaskMap,
				MapTaskIndex: index,
			}
			m.Tasks[file] = t
			//DebugTask(fmt.Sprintf("Init TaskQueue MRTaskPhaseMap %s", file), &t)
		}
		//m.CheckTaskQueue("Phase Map")
	} else if m.MRTaskPhase == MRTaskPhaseMap {
		m.MRTaskPhase = MRTaskPhaseReduce
		m.Tasks = make(map[string]MRTask)
		//fmt.Printf("Init TaskQueue MRTaskPhaseReduce\n")
		for index := 0; index < m.nReduce; index++ {
			t := MRTask{
				TaskStatus:      IDLE,
				TaskType:        TaskReduce,
				NReduce:         m.nReduce,
				ReduceTaskIndex: index,
				MapDoneCount:    len(m.files),
			}
			m.Tasks[strconv.Itoa(index)] = t
			//DebugTask(fmt.Sprintf("Init TaskQueue MRTaskPhaseMap %d", index), &t)
		}
		//m.CheckTaskQueue("Phase Reduce")
		//fmt.Printf("Init reduce task queue\n")
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
		m.Tasks = make(map[string]MRTask)
		m.done = true
		//m.CheckTaskQueue("Phase Done")
	} else if m.MRTaskPhase == MRTaskPhaseDone {
		fmt.Printf("[Warning]: Potential phase loop causing\n")
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
	return m.done
}

func (m *Master) Schedule() {
	for !m.Done() {
		go m.ScheduleTask()
		time.Sleep(500 * time.Millisecond)
	}
}

func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files:       files,
		nReduce:     nReduce,
		MRTaskPhase: MRTaskPhaseUnstart,
		mu:          sync.Mutex{},
	}

	for _, file := range m.files {
		m.UnassignedFiles = append(m.UnassignedFiles, file)
	}
	m.InitTaskQueue()
	m.WorkerStatus = make(map[string]*WorkerInfo)

	go m.Schedule()

	m.server()
	return &m
}
