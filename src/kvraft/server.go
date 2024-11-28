package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	hightWaterEnabled     bool
	db                    map[string]string
	maxAppliedOpIdOfClerk map[int64]int       // the maximum op id among all applied ops of each clerk.
	notifierOfClerk       map[int64]*Notifier // notifier for each clerk.

	persister *raft.Persister // 用于持久化数据
}

func (kv *KVServer) IsLeader(args *IsLeaderArgs, reply *IsLeaderReply) {
	reply.IsLeader = kv.rf.IsLeader()
}

func (kv *KVServer) propose(op *Op) bool {
	_, _, isLeader := kv.rf.Start(op)
	return isLeader
}

func (kv *KVServer) makeNotifier(op *Op) {
	kv.getNotifier(op, true)
	kv.makeAlarm(op)
}

func (kv *KVServer) makeAlarm(op *Op) {
	go func() {
		<-time.After(maxWaitTime)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.notify(op)
	}()
}

// 等待op.ClerkId 上的 所有操作都被applied 或者 超时才退出
func (kv *KVServer) wait(op *Op) {
	// warning: we could only use `notifier.done.Wait` but there's a risk of spurious wakeup or
	// wakeup by stale ops.
	for !kv.killed() {
		if notifier := kv.getNotifier(op, false); notifier != nil {
			notifier.done.Wait()
		} else {
			break
		}
	}
}

func (kv *KVServer) waitUntilAppliedOrTimeout(op *Op) (Err, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("Server %d waitUntilAppliedOrTimeout: op: %v", kv.me, op)
	if !kv.isApplied(op) {
		// 提交操作到raft
		if !kv.propose(op) {
			return ErrWrongLeader, ""
		}
		// wait until applied or timeout.
		kv.makeNotifier(op)
		kv.wait(op)
	}

	if kv.isApplied(op) {
		value := ""
		if op.OpType == "Get" {
			value = kv.db[op.Key]
		}
		return OK, value
	}

	return ErrNotApplied, ""
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("Server %d Get request: key: %s, client ID: %d, sequence num: %d", kv.me, args.Key, args.ClerkId, args.OpId)
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: "Get", Key: args.Key}
	reply.Err, reply.Value = kv.waitUntilAppliedOrTimeout(op)
	DPrintf("Server %d Get reply: key: %s, client ID: %d, sequence num: %d, err: %s, value: %s", kv.me, args.Key, args.ClerkId, args.OpId, reply.Err, reply.Value)

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("Server %d %s request: key: %s, value: %s, client ID: %d, sequence num: %d", kv.me, args.OpType, args.Key, args.Value, args.ClerkId, args.OpId)
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: args.OpType, Key: args.Key, Value: args.Value}
	reply.Err, _ = kv.waitUntilAppliedOrTimeout(op)
	DPrintf("Server %d %s reply: key: %s, value: %s, client ID: %d, sequence num: %d, err: %s", kv.me, args.OpType, args.Key, args.Value, args.ClerkId, args.OpId, reply.Err)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) isApplied(op *Op) bool {
	maxAppliedId, ok := kv.maxAppliedOpIdOfClerk[op.ClerkId]
	return ok && op.OpId <= maxAppliedId
}

func (kv *KVServer) applyClientOp(op *Op) {
	switch op.OpType {
	case "Put":
		kv.db[op.Key] = op.Value
	case "Append":
		kv.db[op.Key] += op.Value
	case "Get":

	default:
		panic("KVServer.applyClientOp: unexpected op type")
	}
}

func (kv *KVServer) notify(op *Op) {
	if notifer := kv.getNotifier(op, false); notifer != nil {
		// only the latest op can delete the notifier.
		if op.OpId == notifer.maxRegisteredOpId {
			delete(kv.notifierOfClerk, op.ClerkId)
		}
		notifer.done.Broadcast()
	}
}

// 一个notifier对应一个client, 但可能会等待好几个操作
func (kv *KVServer) getNotifier(op *Op, forced bool) *Notifier {
	if notifer, ok := kv.notifierOfClerk[op.ClerkId]; ok {
		notifer.maxRegisteredOpId = max(notifer.maxRegisteredOpId, op.OpId)
		return notifer
	}

	if !forced {
		return nil
	}

	notifier := new(Notifier)
	notifier.done = *sync.NewCond(&kv.mu)
	notifier.maxRegisteredOpId = op.OpId
	kv.notifierOfClerk[op.ClerkId] = notifier

	return notifier
}

func (kv *KVServer) maybeApplyClientOp(op *Op) {
	if !kv.isApplied(op) {
		kv.applyClientOp(op)
		// raft已经保证了顺序性和幂等性
		// 顺序性：操作 op.OpId 总是依次递增，因此无需比较，直接更新即可。
		// 幂等性：Raft 的日志层已经避免了重复应用操作，因此直接赋值不会出现问题。
		kv.maxAppliedOpIdOfClerk[op.ClerkId] = op.OpId
		// 操作已经applied，如果有等待该操作的client，则通知所有等待该op的client
		kv.notify(op)
	}
}

func (kv *KVServer) approachHWLimit() bool {
	// note: persister has its own mutex and hence no race would be raised with raft.
	return float32(kv.persister.RaftStateSize()) > 0.8*float32(kv.maxraftstate)
}

func (kv *KVServer) checkpoint(index int) {
	snapshot := kv.makeSnapshot()
	kv.rf.Snapshot(index, snapshot)
}

func (kv *KVServer) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.db) != nil || e.Encode(kv.maxAppliedOpIdOfClerk) != nil {
		panic("failed to encode some fields")
	}
	return w.Bytes()
}

func (kv *KVServer) engineStart() {
	for !kv.killed() {
		msg := <-kv.applyCh // 命令已经在raft中应用（绝大多数都已经同步日志，达成一致）
		kv.mu.Lock()
		if msg.CommandValid {
			op := msg.Command.(*Op)
			if op.OpType == "NoOp" {
				// skip no-ops.

			} else {
				kv.maybeApplyClientOp(op)
			}

			if kv.hightWaterEnabled && kv.approachHWLimit() {
				kv.checkpoint(msg.CommandIndex)
			}
		} else if msg.SnapshotValid {
			kv.ingestSnapshot(msg.Snapshot)
		}
		kv.mu.Unlock()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(&Op{}) // 确保 Op 类型在序列化和反序列化时被正确处理

	kv := new(KVServer)
	kv.me = me
	kv.persister = persister
	kv.mu = sync.Mutex{}

	// You may need initialization code here.
	kv.maxraftstate = maxraftstate
	kv.hightWaterEnabled = maxraftstate != -1

	if kv.hightWaterEnabled && kv.persister.SnapshotSize() > 0 {
		kv.ingestSnapshot(kv.persister.ReadSnapshot())
	} else {
		kv.db = make(map[string]string)
		kv.maxAppliedOpIdOfClerk = make(map[int64]int)
	}

	kv.notifierOfClerk = map[int64]*Notifier{}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.engineStart()

	return kv
}

func (kv *KVServer) ingestSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.db) != nil || d.Decode(&kv.maxAppliedOpIdOfClerk) != nil {
		panic("failed to decode some fields")
	}
}
