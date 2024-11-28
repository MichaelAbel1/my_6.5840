package kvraft

import (
	"sync"
	"time"
)

type Err string

const (
	OK             = "OK"
	ErrNotApplied  = "ErrNotApplied"
	ErrWrongLeader = "ErrWrongLeader"
)

type PutAppendArgs struct {
	ClerkId int64
	OpId    int
	OpType  string // "Put" or "Append"
	Key     string
	Value   string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId int64
	OpId    int
}

type GetReply struct {
	Err   Err
	Value string
}

type IsLeaderArgs struct {
}

type IsLeaderReply struct {
	IsLeader bool
}

// !!  KVServer
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType  string // "Get", "Put", "Append", "NoOp".
	ClerkId int64
	OpId    int
	Key     string
	Value   string
}

type OpReply struct {
	CmdType int8
	ClerkId int64
	OpId    int64
	Err     Err
	Value   string
}

type Notifier struct {
	done              sync.Cond
	maxRegisteredOpId int
}

const maxWaitTime = 500 * time.Millisecond
