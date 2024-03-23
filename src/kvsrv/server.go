package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvMap       map[string]string
	requestsMap map[int64]RequestInfo // clerkID -> RequestInfo
}

type RequestInfo struct {
	RequestType int
	RequestID   int64
	Reply       interface{} // server saves the reply corresponding to the requestID, and in case of a duplicate request, it sends the reply back to the client
}

// Get fetches the current value for the key
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Get is an idempotent operation, so we don't need to filter out duplicate requests
	reply.Value = kv.kvMap[args.Key]
}

// Put installs or replaces the value for a particular key in the map
func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if filterDuplicateRequests(&kv.requestsMap, args.ClerkID, args.RequestID) {
		return
	}
	kv.kvMap[args.Key] = args.Value
}

// Append appends arg to key's value and returns the old value
func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if filterDuplicateRequests(&kv.requestsMap, args.ClerkID, args.RequestID) {
		// if the request is a duplicate, we return the reply (saved old value) saved in the requestsMap
		savedReply := kv.requestsMap[args.ClerkID].Reply.(PutAppendReply)
		reply.Value = savedReply.Value
		// note: cannot directly assign reply = savedReply, because reply is a pointer and savedReply is a value
		return
	}
	reply.Value = kv.kvMap[args.Key]
	// saves the reply (old value) in the requestsMap
	kv.requestsMap[args.ClerkID] = RequestInfo{RequestID: args.RequestID, Reply: PutAppendReply{Value: reply.Value}}
	kv.kvMap[args.Key] += args.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.requestsMap = make(map[int64]RequestInfo)
	kv.mu = sync.Mutex{}

	return kv
}

// filterDuplicateRequests filters out duplicate requests, returning true if the request is a duplicate
// otherwise, it updates the requestsMap with the requestID and returns false
// the requestsMap should be protected by a mutex, and the function should be called within a critical section
func filterDuplicateRequests(requestsMap *map[int64]RequestInfo, clerkID int64, requestID int64) bool {
	if lastRequestInfo, ok := (*requestsMap)[clerkID]; ok {
		if lastRequestInfo.RequestID == requestID {
			return true
		}
	}
	newRequestInfo := RequestInfo{RequestID: requestID}
	(*requestsMap)[clerkID] = newRequestInfo
	return false
}
