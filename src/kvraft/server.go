package kvraft

import (
	"bytes"
	"log"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CmdType     int8
	ClientId    int64
	SequenceNum int64
	Key         string
	Value       string
}

type OpReply struct {
	CmdType     int8
	ClientId    int64
	SequenceNum int64
	Err         Err
	Value       string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data                    map[string]string
	clientSequence          map[int64]int64
	waitChannels            map[int64]chan OpReply // 每一个client 对应一个 用于等待数据的返回对应的channel 发送完毕或等待超时则删除
	maxWaitChannelsSequence map[int64]int64

	persister    *raft.Persister // 用于持久化数据
	currentBytes int             // 用于生成快照
}

func max(a, b int64) int64 {
	if a >= b {
		return a
	}
	return b
}

func (kv *KVServer) getChannel(cmd *Op) chan OpReply {
	if ch, ok := kv.waitChannels[cmd.ClientId]; ok {
		kv.maxWaitChannelsSequence[cmd.ClientId] = max(kv.maxWaitChannelsSequence[cmd.ClientId], cmd.SequenceNum)
		return ch
	}

	ch := make(chan OpReply, 1)
	kv.waitChannels[cmd.ClientId] = ch
	kv.maxWaitChannelsSequence[cmd.ClientId] = cmd.SequenceNum
	return ch
}

// 等待命令
func (kv *KVServer) waitCmd(cmd *Op) OpReply {
	kv.mu.Lock()
	ch := kv.getChannel(cmd)

	// ch := make(chan OpReply, 1)
	// kv.waitChannels[cmd.ClientId] = ch // !!! 存在问题 如果一个clientId有多个等待命令，就会覆盖
	kv.mu.Unlock()

	select {
	case res := <-ch: // 覆盖后就接受不到消息 然后一直阻塞 只用一个channel无法标识不同命令，例如 一个clientId有多个等待命令，但是发过来的op 不是当前等待的命令，拒绝 然后只能继续等待应用
		DPrintf("Server %d receive reply:%v from channel: %v", kv.me, res, ch)
		if res.CmdType != cmd.CmdType || res.ClientId != cmd.ClientId || res.SequenceNum != cmd.SequenceNum {
			res.Err = ErrCmd
		}
		kv.mu.Lock()
		if kv.maxWaitChannelsSequence[cmd.ClientId] == cmd.SequenceNum {
			delete(kv.waitChannels, cmd.ClientId)
		}
		// delete(kv.waitChannels, cmd.ClientId)
		kv.mu.Unlock()
		return res
	case <-time.After(300 * time.Millisecond):
		kv.mu.Lock()
		if kv.maxWaitChannelsSequence[cmd.ClientId] == cmd.SequenceNum {
			delete(kv.waitChannels, cmd.ClientId)
		}
		// delete(kv.waitChannels, cmd.ClientId)
		kv.mu.Unlock()
		res := OpReply{
			Err: ErrTimeout,
		}
		return res
	}

}

func (kv *KVServer) IsLeader(args *IsLeaderArgs, reply *IsLeaderReply) {
	reply.IsLeader = kv.rf.IsLeader()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("Server %d Get request: key: %s, client ID: %d, sequence num: %d", kv.me, args.Key, args.ClientId, args.SequenceNum)
	kv.mu.Lock()
	if args.SequenceNum <= kv.clientSequence[args.ClientId] {
		reply.Value = kv.data[args.Key]
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	cmd := &Op{
		ClientId:    args.ClientId,
		CmdType:     GetCmd,
		SequenceNum: args.SequenceNum,
		Key:         args.Key,
	}
	_, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	res := kv.waitCmd(cmd)
	reply.Value = res.Value
	reply.Err = res.Err
	DPrintf("Server %d Get reply: key: %s, client ID: %d, sequence num: %d, err: %s, value: %s", kv.me, args.Key, args.ClientId, args.SequenceNum, reply.Err, reply.Value)

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("Server %d %s request: key: %s, value: %s, client ID: %d, sequence num: %d", kv.me, args.OpType, args.Key, args.Value, args.ClientId, args.SequenceNum)
	kv.mu.Lock()
	if args.SequenceNum <= kv.clientSequence[args.ClientId] {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	cmd := &Op{
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
		Key:         args.Key,
		Value:       args.Value,
	}
	if args.OpType == "Put" {
		cmd.CmdType = PutCmd
	} else if args.OpType == "Append" {
		cmd.CmdType = AppendCmd
	} else {
		log.Fatalf("Invalid operation: %s", args.OpType)
		return
	}
	_, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	res := kv.waitCmd(cmd)
	reply.Err = res.Err
	DPrintf("Server %d %s reply: key: %s, value: %s, client ID: %d, sequence num: %d, err: %s", kv.me, args.OpType, args.Key, args.Value, args.ClientId, args.SequenceNum, reply.Err)
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

func (kv *KVServer) isRepeated(clientId, sequenceNum int64) bool {
	seq, ok := kv.clientSequence[clientId]
	if ok && seq >= sequenceNum {
		DPrintf("Server %v receive repeated cmd from ClientId: %v sequenceNum: %v seq: %v", kv.me, clientId, sequenceNum, seq)
		return true
	}
	return false
}

// !!!!! Test: restarts, one client (4A) ... 超时
func (kv *KVServer) engineStart() {
	for !kv.killed() {
		msg := <-kv.applyCh // 命令已经在raft中应用（绝大多数都已经同步日志，达成一致）
		if msg.CommandValid {
			kv.mu.Lock()
			op := msg.Command.(*Op) // 类型断言
			clientId := op.ClientId

			if !kv.isRepeated(clientId, op.SequenceNum) { // 重复不做处理
				if op.CmdType == PutCmd {
					kv.data[op.Key] = op.Value
				} else if op.CmdType == AppendCmd {
					kv.data[op.Key] += op.Value
				} else {

				}
				kv.clientSequence[clientId] = op.SequenceNum // !! Test: partitions, one client (4A) 超时
				ch, ok := kv.waitChannels[clientId]
				if ok && ch != nil { // 如果客户端还在等待日志应用完成
					res := OpReply{
						CmdType:     op.CmdType,
						ClientId:    op.ClientId,
						SequenceNum: op.SequenceNum,
						Err:         OK,
						Value:       kv.data[op.Key],
					}
					DPrintf("Server %d reply: key: %s, client ID: %d, sequence num: %d, err: %s, value: %s to channel:%v", kv.me, op.Key, op.ClientId, op.SequenceNum, res.Err, res.Value, kv.waitChannels[clientId])
					ch <- res
				}
			}

			// unsafe.Sizeof(op) 返回的是 op 结构体的直接内存大小，即包含它所有字段的指针或基本类型所占的空间，但不包括指针指向的实际数据
			// 所以最后还需要加上字符串的长度，以计算整个 op 的大小
			kv.currentBytes += int(unsafe.Sizeof(op)) + len(op.Key) + len(op.Value)

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate && kv.currentBytes >= kv.maxraftstate {
				DPrintf("Server %d start snapshot, kv.persister.RaftStateSize(): %v, kv.currentBytes: %v, kv.maxraftstate: %v", kv.me, kv.persister.RaftStateSize(), kv.currentBytes, kv.maxraftstate)
				snapshot := kv.getSnapShot()
				kv.currentBytes = 0
				kv.mu.Unlock()
				kv.rf.Snapshot(msg.CommandIndex, snapshot)
			} else {
				kv.mu.Unlock()
			}

		} else if msg.SnapshotValid {
			kv.mu.Lock()
			kv.readSnapShot(msg.Snapshot)
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) getSnapShot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.clientSequence)
	e.Encode(kv.data)
	snapshot := w.Bytes()
	return snapshot
}

func (kv *KVServer) readSnapShot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	e_decode_clientseq := d.Decode(&kv.clientSequence)
	e_decode_data := d.Decode(&kv.data)
	has_err := false
	if e_decode_clientseq != nil {
		has_err = true
		log.Printf("decode clientseq error %v", e_decode_clientseq)
	}
	if e_decode_data != nil {
		has_err = true
		log.Printf("decode kv error %v", e_decode_data)
	}
	if has_err {
		debug.PrintStack()
		os.Exit(-1)
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
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.clientSequence = make(map[int64]int64)
	kv.maxWaitChannelsSequence = make(map[int64]int64)
	kv.data = make(map[string]string)
	kv.waitChannels = make(map[int64]chan OpReply)
	kv.persister = persister
	kv.currentBytes = 0

	kv.readSnapShot(kv.persister.ReadSnapshot())

	go kv.engineStart()

	return kv
}
