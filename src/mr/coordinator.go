package mr

import (
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
	EXIT
)

type taskInfo struct {
	Status   int
	FilePath []string
	TaskType int
	NReduce  int
}

// index: task number
type taskState struct {
	cond        *sync.Cond
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
	c.taskStates.cond.L.Lock()
	defer c.taskStates.cond.L.Unlock()
	for {
		if !allTaskCompleted(c.taskStates.mapTasks) {
			// distribute a map task
			for i, task := range c.taskStates.mapTasks {
				if task.Status == IDLE {
					reply.TaskID = i
					reply.TaskInfo = task
					c.taskStates.mapTasks[i].Status = INPROGRESS
					go c.resetUncompletedTasks(i, MAP)
					return nil
				}
			}
			// all map tasks have been distributed but not completed
			// either because they are in progress, or because some worker failed and the task was reset to IDLE
			c.taskStates.cond.Wait()
		} else if !allTaskCompleted(c.taskStates.reduceTasks) {
			// distribute a reduce task
			for i, task := range c.taskStates.reduceTasks {
				if task.Status == IDLE {
					reply.TaskID = i
					reply.TaskInfo = task
					c.taskStates.reduceTasks[i].Status = INPROGRESS
					go c.resetUncompletedTasks(i, REDUCE)
					return nil
				}
			}
			// all reduce tasks have been distributed but not completed
			// either because they are in progress, or because some worker failed and the task was reset to IDLE
			c.taskStates.cond.Wait()
		} else {
			// all tasks are completed, send an EXIT task
			reply.TaskInfo = taskInfo{Status: COMPLETED, TaskType: EXIT}
			return nil
		}
	}
}

// ReportTask RPC handler reports the status of a task to the coordinator.
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.taskStates.cond.L.Lock()
	defer c.taskStates.cond.L.Unlock()
	if args.TaskInfo.TaskType == MAP {
		c.taskStates.mapTasks[args.TaskID].Status = args.TaskInfo.Status
		// update the intermediate file paths for the reduce tasks
		for i, filePath := range args.TaskInfo.FilePath {
			c.taskStates.reduceTasks[i].FilePath = append(c.taskStates.reduceTasks[i].FilePath, filePath)
		}
	} else if args.TaskInfo.TaskType == REDUCE {
		c.taskStates.reduceTasks[args.TaskID].Status = args.TaskInfo.Status
	} else {
		log.Printf("Unknown task type: %v", args.TaskInfo.TaskType)
	}
	// signal the waiting workers
	c.taskStates.cond.Broadcast()
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
	c.taskStates.cond.L.Lock()
	defer c.taskStates.cond.L.Unlock()
	for _, task := range c.taskStates.reduceTasks {
		if task.Status != COMPLETED {
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
		c.taskStates.mapTasks[i] = taskInfo{IDLE, []string{file}, MAP, nReduce}
	}
	c.taskStates.reduceTasks = make([]taskInfo, nReduce)
	for i := range c.taskStates.reduceTasks {
		c.taskStates.reduceTasks[i] = taskInfo{IDLE, []string{}, REDUCE, nReduce}
	}
	c.taskStates.cond = sync.NewCond(&sync.Mutex{})

	// start the RPC server to receive connections from workers.
	c.server()
	return &c
}

// allTaskCompleted checks if all tasks of the given type is completed.
func allTaskCompleted(taskInfo []taskInfo) bool {
	for _, task := range taskInfo {
		if task.Status != COMPLETED {
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
	c.taskStates.cond.L.Lock()
	defer c.taskStates.cond.L.Unlock()
	if taskType == MAP {
		if c.taskStates.mapTasks[taskID].Status != COMPLETED {
			c.taskStates.mapTasks[taskID].Status = IDLE
		}
	} else if taskType == REDUCE {
		if c.taskStates.reduceTasks[taskID].Status != COMPLETED {
			c.taskStates.reduceTasks[taskID].Status = IDLE
		}
	}
	c.taskStates.cond.Broadcast()
}
