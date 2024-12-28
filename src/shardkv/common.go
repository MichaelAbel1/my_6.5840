package shardkv

import (
	"sync"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

type InstallShardArgs struct {
	ReconfigureToConfigNum int               // the config num of the config the sender is reconfiguring to.
	Shard                  int               // shard id.
	DB                     map[string]string // shard data.
	MaxAppliedOpIdOfClerk  map[int64]int64   // necessary for implementing the at-most-once semantics.
}

type InstallShardReply struct {
	Err Err
}

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNotApplied  = "ErrNotApplied"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	CommandId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	CommandId int64
}

type GetReply struct {
	Err   Err
	Value string
}

type ShardState string

// warning: remember to place the desired default value of the enum at the beginning so that the init value is the desired default value.
const (
	NotServing = "NotServing" // the server is not serving the shard.
	Serving    = "Serving"    // the server is serving the shard.
	MovingIn   = "MovingIn"   // the server is waiting for the shard data to be moved in.
	MovingOut  = "MovingOut"  // the server is moving out the shard data.
)

// raft的timeInterval
// let the base election timeout be T.
// the election timeout is in the range [T, 2T).
// heartbeatTimeout = 250
// if the peer has not acked in this duration, it's considered inactive.
// activeWindowWidth = 2 * heartbeatTimeout
// tickInterval      = 30
// 3*tickInterval发送一次心跳

const backoffFactor = 2
const maxWaitTime = 1000 * time.Millisecond
const initSleepTime = 10 * time.Millisecond
const maxSleepTime = 500 * time.Millisecond
const checkMigrationStateInterval = 250 * time.Millisecond
const proposeNoOpInterval = 250 * time.Millisecond
const tickerInterval = 100 * time.Millisecond

type Notifier struct {
	done              sync.Cond
	maxRegisteredOpId int64
}

// a snapshotting starts if the raft state size is higher than GCRatio * maxRaftStateSize.
const HWRatio = 0.8
