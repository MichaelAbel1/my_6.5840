package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkId   int64 // the unique id of this clerk.
	commandId int64 // the command id to allocate for an op.
	leaderId  int64 // known leader, defaults to the servers[0].
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkId = nrand()
	ck.leaderId = 0
	ck.commandId = 0

	return ck
}

func (ck *Clerk) GetclerkId() int64 {
	return ck.clerkId
}

// 找到leaderid
func (ck *Clerk) GetState() int {
	for {
		args := IsLeaderArgs{}
		reply := IsLeaderReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.IsLeader", &args, &reply)
		if ok && reply.IsLeader {
			DPrintf("Client %d GetState from %d", ck.clerkId, ck.leaderId)
			return int(ck.leaderId)
		} else {
			time.Sleep(100 * time.Millisecond)
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
		}
	}

}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{
		Key:     key,
		ClerkId: ck.clerkId,
		OpId:    ck.commandId,
	}
	reply := GetReply{}
	ok := false
	for !ok || reply.Err != OK {
		ck.GetState()
		DPrintf("Client %d Getting %s from %d", ck.clerkId, key, ck.leaderId)
		ok = ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
	}
	if ok {
		ck.commandId++
	}
	DPrintf("Client %d Return Get %s from %d", ck.clerkId, key, ck.leaderId)
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:     key,
		Value:   value,
		OpType:  op,
		ClerkId: ck.clerkId,
		OpId:    ck.commandId,
	}

	reply := PutAppendReply{}
	ok := false
	for !ok || reply.Err != OK {
		ck.GetState()
		DPrintf("Client %d %s %s %s to %d", ck.clerkId, op, key, value, ck.leaderId)
		ok = ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
	}
	if ok {
		ck.commandId++
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
