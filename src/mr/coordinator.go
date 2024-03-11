package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	taskStates taskState
}

// state constants
const (
	IDLE = iota
	INPROGRESS
	COMPLETED
)

// task type constants
const (
	MAP = iota
	REDUCE
)

type taskInfo struct {
	status   int
	fileName string
	taskType int
	nReduce  int
}

type taskState struct {
	mu sync.Mutex
	// index: task number
	mapTasks    []taskInfo
	reduceTasks []taskInfo
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// GetTask RPC handler distributes tasks to workers.
// Need to check if the map tasks are completed. If so, distribute reduce tasks.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	return nil
}

// ReportTask RPC handler reports the status of a task to the coordinator.
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	return nil
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
	ret := false

	// Your code here.
	c.taskStates.mu.Lock()
	defer c.taskStates.mu.Unlock()
	for _, task := range c.taskStates.reduceTasks {
		if task.status != COMPLETED {
			return false
		}
	}
	ret = true

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	// Initialize the tasks and states.
	c.taskStates.mapTasks = make([]taskInfo, len(files))
	for i, file := range files {
		c.taskStates.mapTasks[i] = taskInfo{-1, file, MAP, nReduce}
	}
	c.taskStates.reduceTasks = make([]taskInfo, nReduce)
	for i := range c.taskStates.reduceTasks {
		c.taskStates.reduceTasks[i] = taskInfo{-1, "", REDUCE, nReduce}
	}

	// start the RPC server to receive connections from workers.
	c.server()
	return &c
}
