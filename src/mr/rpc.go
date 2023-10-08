package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskType int

const (
	TaskNull   TaskType = -1
	TaskMap    TaskType = 1
	TaskReduce TaskType = 2
	TaskQuit   TaskType = 3
)

type MRTask struct {
	WorkerId        string
	FileName        string
	FileSlice       int
	TaskType        TaskType
	NReduce         int
	TaskStatus      string
	MapTaskIndex    int
	ReduceTaskIndex int
	StartTime       time.Time
	MapDoneCount    int
}

type RequestTaskInfo struct {
	WorkerId string
}

type ReplyTaskInfo struct {
	MRTask
}

type WorkerInfo struct {
	WorkerId string
}

type EmptyInterface struct {
}

func DebugTask(label string, t *MRTask) {
	fmt.Printf("====== [ %s ] ======\n", label)
	fmt.Printf("WorkerId: %s\n", t.WorkerId)
	fmt.Printf("FileName: %s\n", t.FileName)
	fmt.Printf("TaskType: %d\n", t.TaskType)
	fmt.Printf("TaskStatus: %s\n", t.TaskStatus)
	fmt.Printf("MapTaskIndex: %d\n", t.MapTaskIndex)
	fmt.Printf("ReduceTaskIndex: %d\n", t.ReduceTaskIndex)
	fmt.Printf("StartTime: %v\n", t.StartTime)
	fmt.Printf("MapDoneCount: %d\n", t.MapDoneCount)
	fmt.Printf("========================\n")
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
