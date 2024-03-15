package mr

import (
	"errors"
	"log"
	"sync"
	"time"
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
	filePath []string
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
// After distributing a task, start a goroutine to check if the task is completed after a delay of 10 seconds.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.taskStates.mu.Lock()
	defer c.taskStates.mu.Unlock()
	if !allTaskCompleted(c.taskStates.mapTasks) {
		// distribute a map task
		for i, task := range c.taskStates.mapTasks {
			if task.status == IDLE {
				reply.TaskID = i
				reply.TaskInfo = task
				c.taskStates.mapTasks[i].status = INPROGRESS
				go c.resetUncompletedTasks(i, MAP)
				return nil
			}
		}
		// TODO: all map tasks have been distributed but not completed
	} else if !allTaskCompleted(c.taskStates.reduceTasks) {
		// distribute a reduce task
		for i, task := range c.taskStates.reduceTasks {
			if task.status == IDLE {
				reply.TaskID = i
				reply.TaskInfo = task
				c.taskStates.reduceTasks[i].status = INPROGRESS
				go c.resetUncompletedTasks(i, REDUCE)
				return nil
			}
		}
	} else {
		// all tasks are completed
		// TODO: what to do if all the map and reduce tasks are completed?
		log.Printf("All tasks are completed")
		return errors.New("all tasks are completed")
	}
	return nil
}

// ReportTask RPC handler reports the status of a task to the coordinator.
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.taskStates.mu.Lock()
	defer c.taskStates.mu.Unlock()
	if args.TaskInfo.taskType == MAP {
		c.taskStates.mapTasks[args.TaskID].status = args.TaskInfo.status
		// update the intermediate file paths for the reduce tasks
		for i, filePath := range args.TaskInfo.filePath {
			c.taskStates.reduceTasks[i].filePath = append(c.taskStates.reduceTasks[i].filePath, filePath)
		}
	} else if args.TaskInfo.taskType == REDUCE {
		c.taskStates.reduceTasks[args.TaskID].status = args.TaskInfo.status
	} else {
		log.Printf("Unknown task type: %v", args.TaskInfo.taskType)
	}
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
		c.taskStates.mapTasks[i] = taskInfo{-1, []string{file}, MAP, nReduce}
	}
	c.taskStates.reduceTasks = make([]taskInfo, nReduce)
	for i := range c.taskStates.reduceTasks {
		c.taskStates.reduceTasks[i] = taskInfo{-1, []string{}, REDUCE, nReduce}
	}
	c.taskStates.mu = sync.Mutex{}

	// start the RPC server to receive connections from workers.
	c.server()
	return &c
}

// allTaskCompleted checks if all tasks of the given type is completed.
func allTaskCompleted(taskInfo []taskInfo) bool {
	for _, task := range taskInfo {
		if task.status != COMPLETED {
			return false
		}
	}
	return true
}

// resetUncompletedTasks checks the status of a task after a delay of 10 seconds.
// If the task is not completed within this time, it resets the task status to IDLE.
// This allows the task to be reassigned to another worker for processing.
func (c *Coordinator) resetUncompletedTasks(taskID int, taskType int) {
	time.Sleep(10 * time.Second)
	c.taskStates.mu.Lock()
	defer c.taskStates.mu.Unlock()
	if taskType == MAP {
		if c.taskStates.mapTasks[taskID].status != COMPLETED {
			c.taskStates.mapTasks[taskID].status = IDLE
		}
	} else if taskType == REDUCE {
		if c.taskStates.reduceTasks[taskID].status != COMPLETED {
			c.taskStates.reduceTasks[taskID].status = IDLE
		}
	}
}
