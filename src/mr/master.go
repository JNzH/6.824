package mr

import (
	"fmt"
	"log"
	"strconv"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	IDLE        = "IDLE"
	IN_PROGRESS = "IN_PROGRESS"
	COMPLETED   = "COMPLETED"
)

type Master struct {
	mu           sync.Mutex
	Status       map[string]int
	WorkerStatus map[string]*WorkerInfo
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) RequestTask(args *RequestTaskInfo, reply *ReplyTaskInfo) error {
	fmt.Printf("Request task from %s\n", args.WorkerId)
	reply = ChooseTask(args.WorkerId)
	fmt.Printf("Assign task %s %d to %s\n", reply.FileName, reply.FileSlice, args.WorkerId)
	return nil
}

func (m *Master) RegisterWorker(args *WorkerInfo, info *WorkerInfo) error {
	Id := len(m.WorkerStatus) + 1
	info.WorkerId = strconv.Itoa(Id)
	m.WorkerStatus[info.WorkerId] = info
	fmt.Printf("Register worker, assign ID with %s\n", info.WorkerId)
	return nil
}

func ChooseTask(WorkerId string) *ReplyTaskInfo {
	reply := &ReplyTaskInfo{}
	Id, _ := strconv.Atoi(WorkerId)
	if Id%2 == 0 {
		reply.TaskInfo.WorkerId = WorkerId
		reply.TaskInfo.FileName = "1"
		reply.TaskInfo.FileSlice = 2
	} else {
		reply.TaskInfo.WorkerId = WorkerId
		reply.TaskInfo.FileName = "2"
		reply.TaskInfo.FileSlice = 1
	}
	return reply
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) Multiply(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X * 2
	return nil
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
	ret := false

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.server()
	m.WorkerStatus = make(map[string]*WorkerInfo)
	return &m
}
