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

type AppendValue struct {
	value     string
	committed bool // 标记value是否被存储
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data                     map[string]string
	clientAppendreqValue_map map[int64]map[int]AppendValue
	// clientAppendreqValue_map map[int64]map[int]bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
	// reply.Msg = "OK"
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// reply.Value = kv.data[args.Key]
	// kv.data[args.Key] += args.Value

	id := args.ClientId
	requet_id := args.PutAppendReqId
	// 删除历史请求，降低内存缓存
	for reqid := range kv.clientAppendreqValue_map[id] {
		if reqid != requet_id {
			delete(kv.clientAppendreqValue_map[id], reqid)
		}
	}

	if value, ok := kv.clientAppendreqValue_map[args.ClientId][args.PutAppendReqId]; !ok { // key and client not exist
		// new client, key or value
		kv.clientAppendreqValue_map[args.ClientId] = make(map[int]AppendValue)
		kv.clientAppendreqValue_map[args.ClientId][args.PutAppendReqId] = AppendValue{kv.data[args.Key], true}
		reply.Value = kv.data[args.Key]
		kv.data[args.Key] += args.Value
	} else {
		// client, key and value exist
		if value.committed {
			// !======= Real Duplication Handling =========!
			// this means that the value has been put into kv storage
			// we do nothing with kv, but we should return the old value stored in past appendrequest
			reply.Value = value.value
		} else {
			// !======= Case 1 ==========!
			// last request fail in sending state
			// we still need to execute
			// !======= Case 2 ==========!
			// this request is new, it happens to be same with arguments, update the storage request
			kv.clientAppendreqValue_map[args.ClientId][args.PutAppendReqId] = AppendValue{kv.data[args.Key], true}
			reply.Value = kv.data[args.Key]
			kv.data[args.Key] += args.Value
		}
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.clientAppendreqValue_map = make(map[int64]map[int]AppendValue)
	// kv.clientAppendreqValue_map = make(map[int64]map[int]bool)
	return kv
}
