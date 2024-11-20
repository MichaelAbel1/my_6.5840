package kvsrv

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	id            int64
	appendcounter int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.appendcounter = 0
	ck.id = ck.GetClientID()
	return ck
}

func (ck *Clerk) GetClientID() int64 {
	var mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()

	// 使用当前时间戳和随机数生成唯一 ID
	timestamp := time.Now().UnixNano()
	randomPart := nrand()

	// 将时间戳和随机数组合成一个唯一的 ID
	uniqueID := (timestamp << 16) | (randomPart & 0xFFFF)

	return uniqueID
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{}
	args.Key = key
	reply := GetReply{}
	ok := false
	for !ok {
		ok = ck.server.Call("KVServer.Get", &args, &reply)
	}
	// log.Printf("key: %v , Get value: %s\n", key, reply.Value)
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.ClientId = ck.id
	if op == "Append" {
		args.PutAppendReqId = ck.appendcounter
		ck.appendcounter++
	}
	reply := PutAppendReply{}
	ok := false
	for !ok {
		ok = ck.server.Call("KVServer."+op, &args, &reply)
		// args.Retry = true
	}
	// if op == "Append" {
	// 	log.Printf("op: %v: key %v , Get old value: %s\n", op, key, reply.Value)
	// } else {
	// 	log.Printf("Put key %v Success!\n", key)
	// }
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
