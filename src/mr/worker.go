package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type worker struct {
	id      string
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	w := worker{
		mapf:    mapf,
		reducef: reducef,
	}
	w.RegisterWorker()
	w.run()
}

func (w *worker) run() {
	for {
		t := w.RequestTask()
		if t.TaskType == TaskQuit {
			break
		}
		w.ProcessTask(t)
		time.Sleep(500 * time.Millisecond)
	}
}

func (w *worker) RegisterWorker() {
	reply := &WorkerInfo{}
	if ok := call("Master.RegisterWorker", &WorkerInfo{}, reply); !ok {
		fmt.Printf("Error calling RegisterWorker\n")
	}
	w.id = reply.WorkerId
}

func (w *worker) RequestTask() *MRTask {
	args := &RequestTaskInfo{WorkerId: w.id}
	reply := &ReplyTaskInfo{}
	if ok := call("Master.RequestTask", args, &reply); !ok {
		fmt.Printf("Error calling RequestTask\n")
	}
	return &reply.MRTask
}

func (w *worker) ProcessTask(t *MRTask) {
	switch t.TaskType {
	case TaskMap:
		w.ProcessMapTask(t)
	case TaskReduce:
		w.ProcessReduceTask(t)
	case TaskNull:
		return
	default:
		return
	}
}

func (w *worker) ReportTaskDone(t *MRTask) {
	//args := &MRTask{
	//	WorkerId:   t.WorkerId,
	//	FileName:   t.FileName,
	//	TaskStatus: COMPLETED,
	//}
	args := t
	if ok := call("Master.ReportTask", args, &EmptyInterface{}); !ok {
		fmt.Printf("Error calling ReportTask\n")
	}
}

func (w *worker) ReportTaskFailed(t *MRTask) {
	t.TaskStatus = FAILED
	args := t
	if ok := call("Master.ReportTask", args, &EmptyInterface{}); !ok {
		fmt.Printf("Error calling ReportTask\n")
	}
}

func (w *worker) ProcessMapTask(t *MRTask) {
	//DebugTask("Process map task", t)
	contents, err := ioutil.ReadFile(t.FileName)
	if err != nil {
		DebugTask("Read file failed task", t)
		fmt.Printf("Read file %s failed\n", t.FileName)
		w.ReportTaskFailed(t)
		return
	}
	kvs := w.mapf(t.FileName, string(contents))
	sort.Sort(ByKey(kvs))
	kva := make(map[string][]KeyValue)
	for i := 0; i < t.NReduce; i++ {
		kva[strconv.Itoa(i)] = make([]KeyValue, 0)
	}
	for _, kv := range kvs {
		idx := ihash(kv.Key) % t.NReduce
		sidx := strconv.Itoa(idx)
		kva[sidx] = append(kva[sidx], kv)
	}
	for idx, kv := range kva {
		fileName := fmt.Sprintf("mr-%d-%s", t.MapTaskIndex, idx)
		EncodeFile(fileName, kv)
	}
	t.TaskStatus = COMPLETED
	w.ReportTaskDone(t)
}

func (w *worker) ProcessReduceTask(t *MRTask) {
	//DebugTask("Process reduce task", t)
	kvm := make(map[string][]string)
	outFileName := fmt.Sprintf("mr-out-%d", t.ReduceTaskIndex)
	//fmt.Printf("Ouput FileName: %s\n", outFileName)
	outFile, _ := os.Create(outFileName)
	for i := 0; i < t.MapDoneCount; i++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", i, t.ReduceTaskIndex)
		kva := DecodeFile(intermediateFileName)
		for _, kv := range kva {
			if _, ok := kvm[kv.Key]; !ok {
				kvm[kv.Key] = make([]string, 0, 100)
			}
			kvm[kv.Key] = append(kvm[kv.Key], kv.Value)
		}
	}
	res := make([]string, 0, 100)
	for k, kvs := range kvm {
		res = append(res, fmt.Sprintf("%s %s", k, w.reducef(k, kvs)))
		fmt.Fprintf(outFile, "%v %v\n", k, w.reducef(k, kvs))
	}
	outFile.Close()
	t.TaskStatus = COMPLETED
	w.ReportTaskDone(t)
}

func EncodeFile(fileName string, kva []KeyValue) {
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatal("Create file failed", err)
	}
	enc := json.NewEncoder(file)
	defer file.Close()
	for _, kvs := range kva {
		err := enc.Encode(&kvs)
		if err != nil {
			log.Fatal("Encode failed", err)
		}
	}
}

func DecodeFile(fileName string) []KeyValue {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal("Open file failed", err)
	}
	kvs := []KeyValue{}
	dec := json.NewDecoder(file)
	for {
		kv := KeyValue{}
		if err := dec.Decode(&kv); err != nil {
			//log.Fatal("Decode failed", err)
			break
		}
		kvs = append(kvs, kv)
	}
	return kvs
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
