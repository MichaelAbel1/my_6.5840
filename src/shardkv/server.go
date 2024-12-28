package shardkv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId                int64
	OpId                   int64
	OpType                 string // "Get", "Put", "Append", "InstallConfig", "InstallShard", "DeleteShard", "NoOp".
	Key                    string
	Value                  string
	Config                 shardctrler.Config // the config to be installed.
	ReconfigureToConfigNum int                // the associated config num of the InstallShard or the DeleteShard op.
	Shard                  int                // used by InstallShard or DeleteShard to identify a shard.
	DB                     map[string]string
	MaxAppliedOpIdOfClerk  map[int64]int64 // the clerk state would also be installed upon the installation of the shard data.
}

func isSameOp(opX *Op, opY *Op) bool {
	// comparing op types is used to compare two no-ops.
	// comparing clerk id and op id is used to compare two client ops.
	// comparing config num is used to compare two install config ops.
	// comparing shard num is used to compare two install shard ops.
	// it's okay the opX and opY that literally are not the same ops as long as they have the same
	// effect when they're executed.
	return opX.OpType == opY.OpType && opX.ClerkId == opY.ClerkId && opX.OpId == opY.OpId && opX.Config.Num == opY.Config.Num && opX.Shard == opY.Shard
}

func (kv *ShardKV) isAdminOp(op *Op) bool {
	return op.OpType == "InstallConfig" || op.OpType == "InstallShard" || op.OpType == "DeleteShard"
}

func (kv *ShardKV) isNoOp(op *Op) bool {
	return op.OpType == "NoOp"
}

func (kv *ShardKV) noOpTicker() {
	for !kv.killed() {
		op := &Op{OpType: "NoOp"}
		go kv.propose(op)

		time.Sleep(proposeNoOpInterval)
	}
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	sc                *shardctrler.Clerk // client to communicate with the shardctrler
	hightWaterEnabled bool
	dead              int32 // set by Kill(), indicates if the server is killed
	// all shards of the database.
	// since the sharding is static, i.e. no shard splitting and coalescing,
	// it's convenient to store shards in a fixed-size array.
	shardDBs [shardctrler.NShards]ShardDB
	// server's current config.
	config shardctrler.Config
	// the config num of the config the server is reconfigureToConfigNum to.
	// set to -1 if the server is not reconfigureToConfigNum.
	reconfigureToConfigNum int
	// the maximum op id among all applied ops of each clerk.
	// any op with op id less than the max id won't be applied, so that the at-most-once semantics is guaranteed.
	maxAppliedOpIdOfClerk map[int64]int64
	notifierOfClerk       map[int64]*Notifier // notifier for each clerk.
	persister             *raft.Persister     // 用于持久化数据
	// migrating             bool
}

type ShardDB struct {
	DB    map[string]string // shard data, a key-value store.
	State ShardState
	// if used push-based migration, toGid is used.
	// if used pull-based migration, fromGid is used.
	FromGid int // the group (id) from which the server is waiting for it to move in shard data.
	ToGid   int // the group (id) to which the server is moving out the shard data.
}

func (kv *ShardKV) isApplied(op *Op) bool {
	maxAppliedOpId, exist := kv.maxAppliedOpIdOfClerk[op.ClerkId]
	return exist && maxAppliedOpId >= op.OpId
}

func (kv *ShardKV) isEligibleToApply(op *Op) bool {

	eligible := false

	if kv.isNoOp(op) {
		eligible = true

	} else if kv.isAdminOp(op) {
		switch op.OpType {
		case "InstallConfig":
			eligible = (kv.reconfigureToConfigNum == op.Config.Num || op.Config.Num == kv.config.Num+1) && !kv.isMigrating()

		case "InstallShard":
			eligible = kv.isEligibleToUpdateShard(op.ReconfigureToConfigNum) && kv.shardDBs[op.Shard].State == MovingIn

		case "DeleteShard":
			eligible = kv.isEligibleToUpdateShard(op.ReconfigureToConfigNum) && kv.shardDBs[op.Shard].State == MovingOut

		default:
			log.Fatalf("unexpected admin op type %v", op.OpType)
		}

	} else {
		// client op.
		eligible = !kv.isApplied(op) && kv.isServingKey(op.Key)
	}

	return eligible
}

func (kv *ShardKV) isMigrating() bool {
	for shard := 0; shard < shardctrler.NShards; shard++ {
		if kv.shardDBs[shard].State == MovingIn || kv.shardDBs[shard].State == MovingOut {
			return true
		}
	}
	return false
}

func (kv *ShardKV) isEligibleToUpdateShard(reconfigureToConfigNum int) bool {
	// these two checkings ensure that the server must have been installed the config with the config num `reconfigureToConfigNum`
	// and must be waiting to install or delete shards.
	isEligibleToUpdateShard := kv.reconfigureToConfigNum == reconfigureToConfigNum && kv.reconfigureToConfigNum == kv.config.Num
	// DPrintf("ShardKV.isEligibleToUpdateShard: S(G%v-%v) kv.reconfigureToConfigNum=%v, reconfigureToConfigNum=%v, config.Num=%v, isEligibleToUpdateShard=%v", kv.gid, kv.me, kv.reconfigureToConfigNum, reconfigureToConfigNum, kv.config.Num, isEligibleToUpdateShard)
	return isEligibleToUpdateShard
}

func (kv *ShardKV) propose(op *Op) bool {
	_, _, isLeader := kv.rf.Start(op)
	return isLeader
}
func (kv *ShardKV) waitUntilAppliedOrTimeout(op *Op) (Err, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.isServingKey(op.Key) {
		shard := key2shard(op.Key)
		DPrintf("ShardKV.waitUntilAppliedOrTimeout: S%v-%v SN:%v CN:%v rejects Op due to state=%v (C=%v Id=%v) MovingIn: from group: %v, MovingOut: to group %v", kv.gid, kv.me, shard, kv.config.Num, kv.shardDBs[key2shard(op.Key)].State, op.ClerkId, op.OpId, kv.shardDBs[shard].FromGid, kv.shardDBs[shard].ToGid)

		return ErrWrongGroup, ""
	}

	DPrintf("ShardKV.waitUntilAppliedOrTimeout: Server %d waitUntilAppliedOrTimeout: op: %v", kv.me, op)
	if !kv.isApplied(op) && kv.isEligibleToApply(op) {
		// 提交操作到raft
		if !kv.propose(op) {
			return ErrWrongLeader, ""
		}
		// wait until applied or timeout.
		kv.makeNotifier(op)
		kv.wait(op)
	}

	if kv.isApplied(op) && kv.isServingKey(op.Key) {
		value := ""
		if op.OpType == "Get" {
			value = kv.shardDBs[key2shard(op.Key)].DB[op.Key]
		}
		return OK, value
	}

	return ErrNotApplied, ""
}

// 等待op.ClerkId 上的 所有操作都被applied 或者 超时才退出
func (kv *ShardKV) wait(op *Op) {
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

func (kv *ShardKV) makeNotifier(op *Op) {
	kv.getNotifier(op, true)
	kv.makeAlarm(op)
}

func (kv *ShardKV) makeAlarm(op *Op) {
	go func() {
		<-time.After(maxWaitTime)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.notify(op)
	}()
}

func (kv *ShardKV) notify(op *Op) {
	if notifer := kv.getNotifier(op, false); notifer != nil {
		// only the latest op can delete the notifier.
		if op.OpId == notifer.maxRegisteredOpId {
			delete(kv.notifierOfClerk, op.ClerkId)
		}
		notifer.done.Broadcast()
	}
}

// 一个notifier对应一个client, 但可能会等待好几个操作
func (kv *ShardKV) getNotifier(op *Op, forced bool) *Notifier {
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

func (kv *ShardKV) isServingKey(key string) bool {
	shard := key2shard(key)
	return kv.shardDBs[shard].State == Serving
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	DPrintf("ShardKV.Get: S%d-%d CN:%v Get request: key: %s, client ID: %d, sequence num: %d", kv.gid, kv.me, kv.config.Num, args.Key, args.ClientId, args.CommandId)
	op := &Op{ClerkId: args.ClientId, OpId: args.CommandId, OpType: "Get", Key: args.Key}
	kv.mu.Unlock()
	reply.Err, reply.Value = kv.waitUntilAppliedOrTimeout(op)
	kv.mu.Lock()
	DPrintf("ShardKV.Get: S%d-%d CN:%v Get reply: key: %s, client ID: %d, sequence num: %d, err: %s, value: %s", kv.gid, kv.me, kv.config.Num, args.Key, args.ClientId, args.CommandId, reply.Err, reply.Value)
	kv.mu.Unlock()
}
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	DPrintf("ShardKV.PutAppend: S%d-%d CN:%v %s request: key: %s, value: %s, client ID: %d, sequence num: %d", kv.gid, kv.me, kv.config.Num, args.Op, args.Key, args.Value, args.ClientId, args.CommandId)
	op := &Op{ClerkId: args.ClientId, OpId: args.CommandId, OpType: args.Op, Key: args.Key, Value: args.Value}
	kv.mu.Unlock()
	reply.Err, _ = kv.waitUntilAppliedOrTimeout(op)
	kv.mu.Lock()
	DPrintf("ShardKV.PutAppend: S%d-%d CN:%v %s reply: key: %s, value: %s, client ID: %d, sequence num: %d, err: %s", kv.gid, kv.me, kv.config.Num, args.Op, args.Key, args.Value, args.ClientId, args.CommandId, reply.Err)
	kv.mu.Unlock()
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {

	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
	DPrintf("ShardKV.Kill() S%v-%v is killed", kv.gid, kv.me)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead) // 通过原子加载kv.dead的值
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(&Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers // 控制器端点

	// Your initialization code here.
	kv.persister = persister
	kv.config = shardctrler.DefaultConfig()
	kv.sc = shardctrler.MakeClerk(ctrlers)
	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.hightWaterEnabled = maxraftstate != -1
	kv.dead = 0

	if kv.hightWaterEnabled && persister.SnapshotSize() > 0 {
		kv.ingestSnapshot(persister.ReadSnapshot())
	} else {
		kv.reconfigureToConfigNum = -1
		for i := range kv.shardDBs {
			kv.shardDBs[i].DB = make(map[string]string)
			kv.shardDBs[i].State = NotServing
		}
		kv.maxAppliedOpIdOfClerk = make(map[int64]int64)
	}

	kv.notifierOfClerk = map[int64]*Notifier{}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// kv.migrating = false
	_, state := kv.rf.GetState()
	DPrintf("ShardKV.StartServer(): S%v-%v is started, raft.state:%v  !!!!!!", kv.gid, kv.me, state)
	go kv.engineStart()

	go kv.noOpTicker()

	// start a thread to monitor migration state periodically.
	go kv.migrator() // 如果配置需要更新，则更新

	go kv.ticker() // 查询配置

	return kv
}

func (kv *ShardKV) ticker() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(tickerInterval)
			continue
		}
		// the config change has to be contiguous.
		// for example, there're three groups A, B, C, and three contiguous configs X, Y, Z and a shard S.
		// config X: A serves S.
		// config Y: B serves S.
		// config Z: C serves S.
		// assume A learns config X and B learns config Y, further assume A learns Z instead of Y after installing X,
		// B is not allowed to serve S until S is moved from A to B.
		// however, A is going to hand off S to C and hence B may never get shard S.
		// say, there's a final config which assigns all shards to B.
		// since B is reconfigruing, any new config cannot be learned by B
		// and hence all client requests would be rejected and the system goes in a live lock.

		kv.mu.Lock()
		nextConfigNum := kv.config.Num + 1 // 保证得到连续的配置
		kv.mu.Unlock()

		nextConfig := kv.sc.Query(nextConfigNum) // 返回后一个配置或者最新配置（如果大于配置数，则返回最新配置	）

		kv.mu.Lock()

		// a config change is performed only if there's no pending config change.
		// however, it's not necessary to reject proposing new configs during reconfiguring
		// so long as we ensure that a reconfiguring starts only after the previous reconfiguring is completed.
		if kv.reconfigureToConfigNum == -1 && nextConfig.Num == kv.config.Num+1 {
			kv.reconfigureToConfigNum = nextConfig.Num

			op := &Op{OpType: "InstallConfig", Config: nextConfig} // InstallConfig中更新 kv.config = nextConfig
			go kv.propose(op)

			DPrintf("ShardKV.ticker(): S%v-%v starts reconfiguring to nextConfig (NCN=%v)", kv.gid, kv.me, nextConfig.Num)
		}

		kv.mu.Unlock()
		time.Sleep(tickerInterval)
	}
}

func (kv *ShardKV) migrator() {
	for !kv.killed() {
		kv.mu.Lock()

		migrating := false

		if kv.reconfigureToConfigNum != -1 {
			for shard := 0; shard < shardctrler.NShards; shard++ {
				if kv.shardDBs[shard].State == MovingIn { // InstallConfig中更新的状态
					migrating = true
					DPrintf("ShardKV.migrator() S%v-%v CN:%v need receive shard (SN=%v) from G%v", kv.gid, kv.me, kv.config.Num, shard, kv.shardDBs[shard].FromGid)
				}

				if kv.shardDBs[shard].State == MovingOut { // InstallConfig中更新的状态
					migrating = true

					// the shard migration is performed in a push-based way, i.e. the initiator of a shard migration
					// is the replica group who is going to handoff shards.
					// on contrary, if performed in a pull-based way, the initiator of the shard migration is
					// the replica group who is going to take over shards. This replica group sends a pull request
					// to another replica group, and then that replica group starts sending shard data to the sender.
					//
					// deciding on which migration way to use is tricky, but generally the pull-based is prefered since
					// there mighe be less shard data to transfer by network since shard pulling is on demand while
					// shard pushing is not.

					// we could send to a group all shards it needs in one message,
					// however, considering the data of all shards may be way too large
					// and the network is unreliable, there're much overhead for resending
					// all shard data.
					// hence, we choose to send one shard in one message.

					args := kv.makeInstallShardArgs(shard)
					servers := kv.config.Groups[kv.shardDBs[shard].ToGid]
					// this deep clone may be not necessary.
					serversClone := make([]string, 0)
					serversClone = append(serversClone, servers...)

					go kv.sendShard(&args, serversClone)

					DPrintf("ShardKV.migrator() S%v-%v CN:%v sends shard (SN=%v) to G%v", kv.gid, kv.me, kv.config.Num, shard, kv.shardDBs[shard].ToGid)
				}
			}
		}
		if kv.reconfigureToConfigNum != -1 {
			DPrintf("ShardKV.migrator(): S%v-%v is reconfiguring to config (RCN=%v)", kv.gid, kv.me, kv.reconfigureToConfigNum)
		}
		if kv.reconfigureToConfigNum != -1 && !migrating {
			kv.reconfigureToConfigNum = -1
		}

		kv.mu.Unlock()
		time.Sleep(checkMigrationStateInterval)
	}
}

func (kv *ShardKV) sendShard(args *InstallShardArgs, servers []string) {
	// send the shard to the receiver replica group.
	for _, server := range servers {
		srv := kv.make_end(server)
		reply := &InstallShardReply{}
		ok := srv.Call("ShardKV.InstallShard", args, reply) // 发送到目标组内的服务器中
		// OK if the receiver has successfully proposed an install shard op corresponding to the moved-out shard.
		// otherwise, we retry sending the shard to other servers in the receiver replica group.
		if ok && reply.Err == OK {
			kv.mu.Lock()
			// DPrintf("ShardKV.sendShard(): S%v-%v ")
			if kv.isEligibleToUpdateShard(args.ReconfigureToConfigNum) && kv.shardDBs[args.Shard].State == MovingOut {
				// propose a delete shard op to sync the uninstallation of a shard.
				op := &Op{OpType: "DeleteShard", ReconfigureToConfigNum: args.ReconfigureToConfigNum, Shard: args.Shard}
				go kv.propose(op)
			}

			kv.mu.Unlock()
			break
		}
	}
	// kv.migrating = false
	// if no server in the receiver replica group has proposed an install shard op,
	// the next round of `handoffShards` will retry.
}

func (kv *ShardKV) InstallShard(args *InstallShardArgs, reply *InstallShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// only pull shards from leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// if this server needs the shard, then it must have
	// `kv.config.Num == kv.reconfigureToConfigNum` && `kv.config.Num == args.ReconfigureToConfigNum`.
	// therefore, if args.ReconfigureToConfigNum < kv.config.Num, then the shard is stale and shall be rejected.
	//
	// however, the reply OK of the previous InstallShards may get lost due to network issues
	// and the sender would re-send the same InstallShards to the receiver.
	// if we do not reply OK, the sender will keep re-sending the requests and make no progress.
	if args.ReconfigureToConfigNum < kv.config.Num {
		reply.Err = OK
		DPrintf("ShardKV.InstallShard(): S%v-%v rejects InstallShard due to already installed (CN=%v RCN=%v SN=%v)", kv.gid, kv.me, kv.config.Num, args.ReconfigureToConfigNum, args.Shard)
		// kv.migrating = false
		return
	}

	// accept the shard only if the receiver and the sender are reconfiguring to the same config,
	// and the shard has not been installed yet.
	if kv.isEligibleToUpdateShard(args.ReconfigureToConfigNum) && kv.shardDBs[args.Shard].State == MovingIn {
		// note, there's a gap between the proposing of the op and the execution of the op.
		// we could filter the dup op so that the bandwidth pressure on the raft level could be reduced.
		// however, this would require us to assign a unique server id and op id for each admin op.
		// hence, we choose not to filter out dup admin ops but restricted to apply it at most once.

		DPrintf("ShardKV.InstallShard(): S%v-%v accepts InstallShard (RCN=%v SN=%v)", kv.gid, kv.me, args.ReconfigureToConfigNum, args.Shard)

		op := &Op{OpType: "InstallShard", ReconfigureToConfigNum: args.ReconfigureToConfigNum, Shard: args.Shard, DB: args.DB, MaxAppliedOpIdOfClerk: args.MaxAppliedOpIdOfClerk}
		go kv.propose(op)

		// reply OK so that the sender knows an install shard op is proposed.
		//
		// warning: immediately replying OK is okay only if there's no server crash.
		// if the server crashes, the proposer thread is killed and the install shard op is lost.
		// however, the sender would not re-send the shard data since OK is replied.
		// if there's server crash, the correct way is to let the receiver replies OK only if the
		// install shard op is applied.
		reply.Err = OK
		return
	}

	reply.Err = ErrNotApplied
}

func (kv *ShardKV) makeInstallShardArgs(shard int) InstallShardArgs {
	shardDB := kv.shardDBs[shard]

	args := InstallShardArgs{
		// since kv.config.Num and kv.reconfigureToConfigNum must be identical at this time,
		// either of which could be assigned to ConfigNum.
		ReconfigureToConfigNum: kv.reconfigureToConfigNum,
		Shard:                  shard,
		DB:                     make(map[string]string),
		MaxAppliedOpIdOfClerk:  make(map[int64]int64),
	}

	// deep clone shard data.
	// deep clone is necessary. See the bug described in `installShard`.
	for k, v := range shardDB.DB {
		args.DB[k] = v
	}

	// deep clone clerk state.
	for k, v := range kv.maxAppliedOpIdOfClerk {
		args.MaxAppliedOpIdOfClerk[k] = v
	}

	return args
}

func (kv *ShardKV) engineStart() {
	for !kv.killed() {
		msg := <-kv.applyCh // 命令已经在raft中应用（绝大多数都已经同步日志，达成一致）
		kv.mu.Lock()
		if msg.CommandValid {
			op := msg.Command.(*Op) // 类型断言（type assertion） 表达式，用于从接口类型断言出它的具体类型
			if op.OpType == "NoOp" {
				// skip no-ops.x

			} else if kv.isAdminOp(op) {
				kv.maybeApplyAdminOp(op)

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

func (kv *ShardKV) approachHWLimit() bool {
	// note: persister has its own mutex and hence no race would be raised with raft.
	return float32(kv.persister.RaftStateSize()) > HWRatio*float32(kv.maxraftstate)
}

func (kv *ShardKV) checkpoint(index int) {
	snapshot := kv.makeSnapshot()
	kv.rf.Snapshot(index, snapshot)
}

func (kv *ShardKV) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.shardDBs) != nil ||
		e.Encode(kv.config) != nil ||
		// e.Encode(&kv.reconfigureToConfigNum) != nil ||
		e.Encode(&kv.maxAppliedOpIdOfClerk) != nil {
		panic("ShardKV.makeSnapshot: failed to encode some fields")
	}
	return w.Bytes()
}

func (kv *ShardKV) ingestSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.shardDBs) != nil ||
		d.Decode(&kv.config) != nil ||
		// d.Decode(&kv.reconfigureToConfigNum) != nil ||
		d.Decode(&kv.maxAppliedOpIdOfClerk) != nil {
		panic("ShardKV.ingestSnapshot: failed to decode some fields")
	}
}

func (kv *ShardKV) maybeApplyClientOp(op *Op) {
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

func (kv *ShardKV) applyClientOp(op *Op) {
	// the write is applied on the corresponding shard.
	shard := key2shard(op.Key)
	db := kv.shardDBs[shard].DB

	switch op.OpType {
	case "Get":
		// only write ops are applied to the database.

	case "Put":
		db[op.Key] = op.Value

	case "Append":
		// note: the default value is returned if the key does not exist.
		db[op.Key] += op.Value

	default:
		log.Fatalf("ShardKV.applyClientOp: unexpected client op type %v", op.OpType)
	}
}

func (kv *ShardKV) maybeApplyAdminOp(op *Op) {
	switch op.OpType {
	case "InstallConfig":
		// there're two sources of an install config op:
		// (1) it's proposed by this server.
		// (2) it's proposed by another server in the same replica group.
		// if it's (1), `reconfigureToConfigNum` == `op.Config.Num`.
		// if it's (2), `reconfigureToConfigNum` may still be -1 while the `kv.config.Num + 1` == `op.Config.Num`.
		// in either cases, we need to install the config.
		// besires, this install config op gets installed only if the server is not migrating shards.

		if (kv.reconfigureToConfigNum == op.Config.Num || op.Config.Num == kv.config.Num+1) && !kv.isMigrating() {
			kv.reconfigureToConfigNum = op.Config.Num
			kv.installConfig(op.Config)

			DPrintf("ShardKV.maybeApplyAdminOp: S%v-%v installed config (ACN=%v)", kv.gid, kv.me, kv.config.Num)
		}

	case "InstallShard":
		if kv.isEligibleToUpdateShard(op.ReconfigureToConfigNum) && kv.shardDBs[op.Shard].State == MovingIn {
			kv.installShard(op)
		}

	case "DeleteShard":
		if kv.isEligibleToUpdateShard(op.ReconfigureToConfigNum) && kv.shardDBs[op.Shard].State == MovingOut {
			kv.deleteShard(op)
		}

	default:
		log.Fatalf("unexpected admin op type %v", op.OpType)
	}
}

func (kv *ShardKV) deleteShard(op *Op) {
	kv.shardDBs[op.Shard] = ShardDB{
		DB:      make(map[string]string),
		State:   NotServing,
		FromGid: 0,
		ToGid:   0,
	}
	// kv.migrating = false
	DPrintf("ShardKV.deleteShard: S%v-%v starts not serving shard (SN=%v)", kv.gid, kv.me, op.Shard)
}

func (kv *ShardKV) installShard(op *Op) {
	// install server state.
	// the installation could be performed either by replacing or updating.
	// all shard data is migrated during shard migration, so replacing is okay.
	// undeleted stale shard data will be shadowed, so updating is okay as well.
	//
	// warning: simply assign op.DB to kv.DB is incorrect since variables in Go are references
	// which may incur a tricky bug that an update in one server of the group will also
	// be reflected in another server of the same group.
	// this is because the tests run a mocking cluster wherein all servers run in the same local machine.
	for k, v := range op.DB {
		kv.shardDBs[op.Shard].DB[k] = v
	}

	// this update could also be performed by replacing.
	// the above reasoning applies on here as well since not only shard data is synced, the max apply op id is also synced.
	for clerkId, otherOpId := range op.MaxAppliedOpIdOfClerk {
		if opId, exist := kv.maxAppliedOpIdOfClerk[clerkId]; !exist || otherOpId > opId {
			kv.maxAppliedOpIdOfClerk[clerkId] = otherOpId
		}
	}

	kv.shardDBs[op.Shard].State = Serving
	// kv.migrating = false

	DPrintf("ShardKV.installShard: S%v-%v starts serving shard (SN=%v)", kv.gid, kv.me, op.Shard)
}

func (kv *ShardKV) installConfig(nextConfig shardctrler.Config) {
	hasShardsToHandOff := false
	hasShardsToTakeOver := false

	for shard := 0; shard < shardctrler.NShards; shard++ {
		currGid := kv.config.Shards[shard]
		newGid := nextConfig.Shards[shard]

		if currGid == 0 && newGid == kv.gid {
			// nothing to move in if the from group is the invalid group 0.
			kv.shardDBs[shard].State = Serving
			continue
		}

		// warning: this seems not necessary.
		if newGid == 0 {
			// noting to move out if the to group is the invalid group 0.
			kv.shardDBs[shard].State = NotServing
			continue
		}
		// currGid == kv.gid 表示可以操作的当前组里面的shard
		// newGid != kv.gid 不在当前组了 就移出

		if currGid == kv.gid && newGid != kv.gid {
			// move this shard from this group to the group with gid newGid.
			kv.shardDBs[shard].State = MovingOut // 当有新组加入的时候 shard在不同组内进行负载均衡，所以就有shard 要移出当前组
			kv.shardDBs[shard].ToGid = newGid
			hasShardsToHandOff = true // 当前组有shard需移交出去
		}
		// 其他组的shard移入
		if currGid != kv.gid && newGid == kv.gid {
			// move this shard from the group with gid currGid to this group.
			kv.shardDBs[shard].State = MovingIn // 当有新组加入的时候 shard在不同组内进行负载均衡，所以就有shard 要移入当前组
			kv.shardDBs[shard].FromGid = currGid
			hasShardsToTakeOver = true // 当前组有新shard需接手
		}
	}

	// install the next config.
	kv.config = nextConfig

	// if the served shards do not change, the reconfiguring is done.
	if !hasShardsToHandOff && !hasShardsToTakeOver {
		kv.reconfigureToConfigNum = -1

		DPrintf("ShardKV.installConfig: S%v-%v reconfigure done (CN=%v)", kv.gid, kv.me, kv.config.Num)
		return

	} else {
		// otherwise, the server has to take over moved-in shards or/and hand off moved-out shards.
		if hasShardsToHandOff {
			DPrintf("ShardKV.installConfig: S%v-%v starts handing off shards", kv.gid, kv.me)
		}

		if hasShardsToTakeOver {
			DPrintf("ShardKV.installConfig: S%v-%v waiting to take over shards", kv.gid, kv.me)
		}
	}
}
