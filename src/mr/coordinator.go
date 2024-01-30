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

type Coordinator struct {
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

func (c *Coordinator) RequestTask(args *RequestTaskInfo, reply *ReplyTaskInfo) error {
	//m.mu.Lock()
	//defer m.mu.Unlock()

	task := c.PickTaskFromQueue(args.WorkerId)
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

func (c *Coordinator) RegisterWorker(_, info *WorkerInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	Id := len(c.WorkerStatus) + 1
	info.WorkerId = strconv.Itoa(Id)
	c.WorkerStatus[info.WorkerId] = info
	return nil
}

func (c *Coordinator) ReportTask(args *MRTask, _ *EmptyInterface) error {
	//DebugTask("Receive ReportTask in server", args)
	c.mu.Lock()
	defer c.mu.Unlock()

	k := ""
	if args.TaskType == TaskMap {
		k = args.FileName
	} else if args.TaskType == TaskReduce {
		k = strconv.Itoa(args.ReduceTaskIndex)
	} else {
		fmt.Printf("Error in ReportTask: TaskType\n")
	}

	if args.TaskStatus == COMPLETED {
		if v, ok := c.Tasks[k]; ok {
			v.TaskStatus = COMPLETED
			c.Tasks[k] = v
		}
	} else if args.TaskStatus == FAILED {
		if v, ok := c.Tasks[k]; ok {
			v.TaskStatus = IDLE
			c.Tasks[k] = v
		}
	}

	//go c.ScheduleTask()
	return nil
}

// Schedule task mechanism
// 1. Assign all map task
// 2. Assign all reduce task after no map task remaining
// 3.
func (c *Coordinator) ScheduleTask() {
	c.mu.Lock()
	defer c.mu.Unlock()

	//fmt.Printf("ScheduleTask Called\n")

	allFinish := true
	for k, t := range c.Tasks {
		//DebugTask(fmt.Sprintf("Task %s in queue", i), &t)
		switch t.TaskStatus {
		case IDLE:
			allFinish = false
		case IN_PROGRESS:
			allFinish = false
			if !c.HeartBeatCheck(t) {
				t.TaskStatus = IDLE
				c.Tasks[k] = t
			}
		case COMPLETED:
		default:
			fmt.Printf("task status invalid")
		}
	}
	if allFinish {
		c.InitTaskQueue()
	}
}

func (c *Coordinator) CheckTaskQueue(stage string) {
	fmt.Printf("Check task queue in stage %s\n", stage)
	idx := 0
	for k, v := range c.Tasks {
		DebugTask(fmt.Sprintf("Key: %s index: %d", k, idx), &v)
		idx++
	}
}

func (c *Coordinator) HeartBeatCheck(task MRTask) bool {
	if time.Now().Sub(task.StartTime) > MaxTaskRunTime {
		fmt.Printf("Trigger max runtime %s %d\n", task.FileName, task.ReduceTaskIndex)
		return false
	}
	return true
}

func (c *Coordinator) PickTaskFromQueue(WorkerId string) MRTask {
	c.mu.Lock()
	defer c.mu.Unlock()

	task := MRTask{}
	if c.MRTaskPhase == MRTaskPhaseDone {
		task.TaskType = TaskQuit
		return task
	}

	found := false

	for k, t := range c.Tasks {
		switch t.TaskStatus {
		case IDLE:
			t.TaskStatus = IN_PROGRESS
			t.StartTime = time.Now()
			t.WorkerId = WorkerId
			task = t
			c.Tasks[k] = task
			//tmp := c.Tasks[k]
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

func (c *Coordinator) InitTaskQueue() {
	if c.MRTaskPhase == MRTaskPhaseUnstart {
		c.MRTaskPhase = MRTaskPhaseMap
		c.Tasks = make(map[string]MRTask)
		//fmt.Printf("Init TaskQueue MRTaskPhaseMap\n")
		for index, file := range c.files {
			t := MRTask{
				FileName:     file,
				NReduce:      c.nReduce,
				TaskStatus:   IDLE,
				TaskType:     TaskMap,
				MapTaskIndex: index,
			}
			c.Tasks[file] = t
			//DebugTask(fmt.Sprintf("Init TaskQueue MRTaskPhaseMap %s", file), &t)
		}
		//c.CheckTaskQueue("Phase Map")
	} else if c.MRTaskPhase == MRTaskPhaseMap {
		c.MRTaskPhase = MRTaskPhaseReduce
		c.Tasks = make(map[string]MRTask)
		//fmt.Printf("Init TaskQueue MRTaskPhaseReduce\n")
		for index := 0; index < c.nReduce; index++ {
			t := MRTask{
				TaskStatus:      IDLE,
				TaskType:        TaskReduce,
				NReduce:         c.nReduce,
				ReduceTaskIndex: index,
				MapDoneCount:    len(c.files),
			}
			c.Tasks[strconv.Itoa(index)] = t
			//DebugTask(fmt.Sprintf("Init TaskQueue MRTaskPhaseMap %d", index), &t)
		}
		//c.CheckTaskQueue("Phase Reduce")
		//fmt.Printf("Init reduce task queue\n")
		//for index, file := range c.files {
		//	c.Tasks[file] = MRTask{
		//		TaskStatus:      IDLE,
		//		TaskType:        TaskMap,
		//		NReduce:         c.nReduce,
		//		ReduceTaskIndex: index,
		//		MapDoneCount:    len(c.files),
		//	}
		//}
	} else if c.MRTaskPhase == MRTaskPhaseReduce {
		c.MRTaskPhase = MRTaskPhaseDone
		c.Tasks = make(map[string]MRTask)
		c.done = true
		//c.CheckTaskQueue("Phase Done")
	} else if c.MRTaskPhase == MRTaskPhaseDone {
		fmt.Printf("[Warning]: Potential phase loop causing\n")
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

func (c *Coordinator) Schedule() {
	for !c.Done() {
		go c.ScheduleTask()
		time.Sleep(500 * time.Millisecond)
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		MRTaskPhase: MRTaskPhaseUnstart,
		mu:          sync.Mutex{},
	}

	for _, file := range c.files {
		c.UnassignedFiles = append(c.UnassignedFiles, file)
	}
	c.InitTaskQueue()
	c.WorkerStatus = make(map[string]*WorkerInfo)

	go c.Schedule()

	c.server()
	return &c
}
