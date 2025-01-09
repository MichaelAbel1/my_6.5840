package shardctrler

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.RWMutex // 适合于读操作多、写操作少的场景
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead           int32                      // set by Kill(), to indicate the ShardCtrler instance is killed
	stateMachine   ConfigStateMachine         // the state machine to store the configuration of the shard system
	lastOperations map[int64]OperationContext // the last operation context to avoid duplicate request
	notifyChans    map[int]chan *CommandReply // the notify channel to notify the client goroutine
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

// killed checks if the ShardCtrler is killed.
func (sc *ShardCtrler) killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Command{})
	sc := new(ShardCtrler)
	sc.me = me
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.dead = 0
	sc.stateMachine = NewMemoryConfigStateMachine()
	sc.lastOperations = make(map[int64]OperationContext)
	sc.notifyChans = make(map[int]chan *CommandReply)

	go sc.applier()

	return sc
}

// applier is the goroutine to apply the log to the state machine.
func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		message := <-sc.applyCh
		if message.CommandValid {
			reply := new(CommandReply)
			command := message.Command.(Command)
			sc.mu.Lock() // 获取写锁
			// defer sc.mu.Unlock()
			if command.Op != Query && sc.isDuplicateRequest(command.ClientId, command.CommandId) { // 保证了不重复处理
				DPrintf("ShardCtrler.applier: clerkid :[%d] duplicate request commandid: %v, OpType: %s", command.ClientId, command.CommandId, command.Op)
				reply = sc.lastOperations[command.ClientId].LastReply
			} else {
				reply = sc.applyLogToStateMachine(command) // 达成一致后实现相应的操作
				if command.Op != Query {                   // 除了Query操作外，其他操作都需要记录
					sc.lastOperations[command.ClientId] = OperationContext{
						MaxAppliedCommandId: command.CommandId,
						LastReply:           reply,
					}
				}
			}

			// just notify related channel for currentTerm's log when node is leader
			// currentTerm, isLeader := sc.rf.GetState()
			// message_term, err := sc.rf.GetTermWithoutLock(message.CommandIndex)
			if currentTerm, isLeader := sc.rf.GetState(); isLeader && message.CommandTerm == currentTerm { // ! 确保只处理领导者任期内的消息
				notifyChan := sc.getNotifyChan(message.CommandIndex) // 只有领导者才回复
				DPrintf("ShardCtrler.applier: clerkid %v: commandid: %v, OpType:%s applied !!!", command.ClientId, command.CommandId, command.Op)
				notifyChan <- reply
			}
			sc.mu.Unlock() // !!!释放锁
		}
	}
}

// getNotifyChan gets the notification channel for the given index.
// If the channel doesn't exist, it creates one.
func (sc *ShardCtrler) getNotifyChan(index int) chan *CommandReply {
	notifyChan, ok := sc.notifyChans[index]
	if !ok {
		notifyChan = make(chan *CommandReply, 1)
		sc.notifyChans[index] = notifyChan
	}
	return notifyChan
}

func (sc *ShardCtrler) applyLogToStateMachine(command Command) *CommandReply {
	reply := new(CommandReply)
	switch command.Op {
	case Join:
		reply.Err = sc.stateMachine.Join(command.Servers)
	case Leave:
		reply.Err = sc.stateMachine.Leave(command.GIDs)
	case Move:
		reply.Err = sc.stateMachine.Move(command.Shard, command.GID)
	case Query:
		reply.Config, reply.Err = sc.stateMachine.Query(command.Num)
	}
	return reply
}

func (sc *ShardCtrler) isDuplicateRequest(clientId int64, commandId int64) bool {
	OperationContext, ok := sc.lastOperations[clientId]
	return ok && commandId <= OperationContext.MaxAppliedCommandId
}

// Command handles incoming commands.
// If the command is not a Query and is a duplicate request,
// it returns the previous reply.
// Otherwise, it tries to start the command in the raft layer.
// If the current server is not the leader, it returns an error.
// If the command is successfully started, it waits for the result
// or times out after a certain period.
func (sc *ShardCtrler) Command(args *CommandArgs, reply *CommandReply) {
	sc.mu.RLock()
	// check if the command is a duplicate request(only for non-Query command)
	if args.Op != Query && sc.isDuplicateRequest(args.ClientId, args.CommandId) {
		LastReply := sc.lastOperations[args.ClientId].LastReply
		reply.Config, reply.Err = LastReply.Config, LastReply.Err
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()
	// try to start the command in the raft layer
	index, _, isLeader := sc.rf.Start(Command{args}) // 提交到领导者日志中
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	sc.mu.Lock()
	// get the notify channel to wait for the result
	notifyChan := sc.getNotifyChan(index)
	sc.mu.Unlock()
	select {
	case result := <-notifyChan: // 等待raft中达成一致
		reply.Config, reply.Err = result.Config, result.Err
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}
	go func() {
		sc.mu.Lock()
		// remove the outdated notify channel to reduce memory footprint
		sc.removeOutdatedNotifyChan(index)
		sc.mu.Unlock()
	}()
}

// removeOutdatedNotifyChan removes the outdated notify channel for the given index.
func (sc *ShardCtrler) removeOutdatedNotifyChan(index int) {
	delete(sc.notifyChans, index)
}
