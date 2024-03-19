package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		// send an RPC to the coordinator asking for a task.
		getTaskArgs := GetTaskArgs{}
		getTaskReply := GetTaskReply{}
		err := call("Coordinator.GetTask", &getTaskArgs, &getTaskReply)
		if err != nil {
			// if the worker fails to contact the coordinator, it can assume
			// that the coordinator has exited because the job is done.
			// the worker should then exit.
			log.Printf("GetTask call failed: %v", err)
			return
		}

		taskID := getTaskReply.TaskID
		taskInfo := getTaskReply.TaskInfo

		// read the file and call the corresponding function.
		if taskInfo.TaskType == MAP {
			// read the input file
			inputFile, err := os.Open(taskInfo.FilePath[0])
			if err != nil {
				log.Printf("Cannot open inputFile: %v, %v", taskInfo.FilePath, err)
				continue
			}

			content, err := io.ReadAll(inputFile)
			if err != nil {
				log.Printf("Cannot read inputFile: %v, %v", taskInfo.FilePath, err)
				continue
			}

			inputFile.Close()
			// call the map function
			kva := mapf(taskInfo.FilePath[0], string(content))

			// split the intermediate key-value pairs into nReduce parts
			intermediate := make([][]KeyValue, taskInfo.NReduce)
			for _, kv := range kva {
				reduceTaskID := ihash(kv.Key) % taskInfo.NReduce
				intermediate[reduceTaskID] = append(intermediate[reduceTaskID], kv)
			}

			// write the intermediate key-value pairs to files
			intermediateFilePaths := make([]string, len(intermediate))
			for reduceTaskID, kva := range intermediate {
				bytes, err := json.Marshal(kva)
				if err != nil {
					log.Printf("Cannot marshal intermediate key-value pairs: %v", err)
					continue
				}
				fileName := fmt.Sprintf("mr-%v-%v", taskID, reduceTaskID)
				// use temporary files and atomically rename them once the file is completely written
				file, err := os.CreateTemp("", fileName)
				if err != nil {
					log.Printf("Cannot create inputFile: %v, %v", fileName, err)
					continue
				}
				_, err = file.Write(bytes)
				if err != nil {
					log.Printf("Cannot write inputFile: %v, %v", fileName, err)
					continue
				}
				file.Close()
				// rename and move the temporary file to current directory
				// When creating a temporary file using os.CreateTemp without specifying a directory, the file is created in the default directory for temporary files.
				// The Name method of the *os.File returned by os.CreateTemp returns the full file path, not just the file name.
				err = os.Rename(file.Name(), fileName)
				absPath, err := filepath.Abs(fileName)
				if err != nil {
					log.Printf("Cannot get absolute path: %v, %v", fileName, err)
					continue
				}
				intermediateFilePaths[reduceTaskID] = absPath
			}

			// send the intermediate file paths and the task status to the coordinator
			taskInfo.Status = COMPLETED
			taskInfo.FilePath = intermediateFilePaths
			reportTaskArgs := ReportTaskArgs{taskID, taskInfo}
			reportTaskReply := ReportTaskReply{}
			err = call("Coordinator.ReportTask", &reportTaskArgs, &reportTaskReply)
			if err != nil {
				log.Printf("ReportTask call failed: %v", err)
				continue
			}
		} else if taskInfo.TaskType == REDUCE {
			// read in all the intermediate files
			intermediate := []KeyValue{}
			for _, fileName := range taskInfo.FilePath {
				file, err := os.Open(fileName)
				if err != nil {
					log.Printf("Cannot open inputFile: %v, %v", fileName, err)
					continue
				}
				bytes, err := io.ReadAll(file)
				if err != nil {
					log.Printf("Cannot read inputFile: %v, %v", fileName, err)
					continue
				}
				file.Close()
				var kva []KeyValue
				err = json.Unmarshal(bytes, &kva)
				if err != nil {
					log.Printf("Cannot unmarshal intermediate key-value pairs: %v", err)
					continue
				}
				intermediate = append(intermediate, kva...)
			}
			// sort by the intermediate keys
			sort.Sort(ByKey(intermediate))
			// iterates over the sorted intermediate data, for each unique intermediate key encountered, call the reduce function
			// and write the output to a file
			oname := fmt.Sprintf("mr-out-%v", taskID)
			ofile, err := os.Create(oname)
			if err != nil {
				log.Printf("Cannot create outputFile: %v, %v", oname, err)
				continue
			}
			i := 0 // track the current position in the slice
			for i < len(intermediate) {
				j := i + 1
				// find all key-value pairs with the same key
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			ofile.Close()

			// send the task status to the coordinator
			taskInfo.Status = COMPLETED
			reportTaskArgs := ReportTaskArgs{taskID, taskInfo}
			reportTaskReply := ReportTaskReply{}
			err = call("Coordinator.ReportTask", &reportTaskArgs, &reportTaskReply)
			if err != nil {
				log.Printf("ReportTask call failed: %v", err)
				continue
			}
		} else if taskInfo.TaskType == EXIT {
			log.Printf("Received EXIT task, exiting")
			return
		} else {
			log.Printf("Unknown task type: %v", taskInfo.TaskType)
		}
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
