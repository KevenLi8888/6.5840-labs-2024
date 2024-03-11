package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// TODO: implement the worker's main loop.

	// send an RPC to the coordinator asking for a task.
	getTaskArgs := GetTaskArgs{}
	getTaskReply := GetTaskReply{}
	err := call("Coordinator.GetTask", &getTaskArgs, &getTaskReply)
	if err != nil {
		// TODO: how to handle the error? e.g. when the coordinator is down.
		log.Printf("GetTask call failed: %v", err)
		return // TODO: break
	}

	// read the file and call the corresponding function.
	if getTaskReply.TaskInfo.taskType == MAP {
		// read the input file
		file, err := os.Open(getTaskReply.TaskInfo.fileName)
		if err != nil {
			log.Printf("Cannot open file: %v, %v", getTaskReply.TaskInfo.fileName, err)
			return // TODO: break
		}

		content, err := io.ReadAll(file)
		if err != nil {
			log.Printf("Cannot read file: %v, %v", getTaskReply.TaskInfo.fileName, err)
			return // TODO: break
		}

		file.Close()
		// call the map function
		kva := mapf(getTaskReply.TaskInfo.fileName, string(content))

		// split the intermediate key-value pairs into nReduce parts
		intermediate := make([][]KeyValue, getTaskReply.TaskInfo.nReduce)
		for _, kv := range kva {
			reduceTaskNumber := ihash(kv.Key) % getTaskReply.TaskInfo.nReduce
			intermediate[reduceTaskNumber] = append(intermediate[reduceTaskNumber], kv)
		}

		// write the intermediate key-value pairs to files
		for i, kva := range intermediate {
			bytes, err := json.Marshal(kva)
			if err != nil {
				log.Printf("Cannot marshal intermediate key-value pairs: %v", err)
				return // TODO: break
			}
			fileName := fmt.Sprintf("mr-%v-%v", getTaskReply.TaskID, i)
			file, err := os.Create(fileName)
			if err != nil {
				log.Printf("Cannot create file: %v, %v", fileName, err)
				return // TODO: break
			}
			_, err = file.Write(bytes)
			if err != nil {
				log.Printf("Cannot write file: %v, %v", fileName, err)
				return // TODO: break
			}
			file.Close()
		}

		// send the task completed state to the coordinator
		reportTaskArgs := ReportTaskArgs{
			TaskID:    getTaskReply.TaskID,
			TaskState: COMPLETED,
		}
		reportTaskReply := ReportTaskReply{}
		err = call("Coordinator.ReportTask", &reportTaskArgs, &reportTaskReply)
		if err != nil {
			log.Printf("ReportTask call failed: %v", err)
			return // TODO: break
		}
	} else if getTaskReply.TaskInfo.taskType == REDUCE {

	} else {
		log.Printf("Unknown task type: %v", getTaskReply.TaskInfo.taskType)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	err := call("Coordinator.Example", &args, &reply)
	if err == nil {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// returns the error of the corresponding rpc call.
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err
}
