package raft

import (
	"log"
	"time"
)

type RequestVoteArgs struct {
	From         int
	To           int
	Term         uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	From  int
	To    int
	Term  uint64
	Voted bool
}

type AppendEntriesArgs struct {
	From           int
	To             int
	Term           uint64
	CommittedIndex uint64
	PrevLogIndex   uint64
	PrevLogTerm    uint64
	Entries        []Entry
}

type Err int

const (
	Rejected Err = iota
	Matched
	IndexNotMatched
	TermNotMatched
)

type AppendEntriesReply struct {
	From               int
	To                 int
	Term               uint64
	Err                Err
	LastLogIndex       uint64
	ConflictTerm       uint64
	FirstConflictIndex uint64
}

type InstallSnapshotArgs struct {
	From     int
	To       int
	Term     uint64
	Snapshot Snapshot
}

type InstallSnapshotReply struct {
	From     int
	To       int
	Term     uint64
	CaughtUp bool
}

type MessageType string

const (
	Vote        MessageType = "RequestVote"
	VoteReply   MessageType = "RequestVoteReply"
	Append      MessageType = "AppendEntries"
	AppendReply MessageType = "AppendEntriesReply"
	Snap        MessageType = "InstallSnapshot"
	SnapReply   MessageType = "InstallSnapshotReply"
)

type Message struct {
	Type         MessageType
	From         int    // warning: not used for now.
	Term         uint64 // the term in the PRC args or RPC reply.
	ArgsTerm     uint64 // the term in the RPC args. Used to different between the term in a RPC reply.
	PrevLogIndex uint64 // used for checking of AppendEntriesReply.
}

// return (termIsStale, termChanged).
// 检查传过来的term是否比raft当前term旧，旧则ok返回false，并且判断是否term是否发生了变化。
// 任期发生变化则代表节点状态变为Follower
// ignore stale messages.
func (rf *Raft) checkTerm(m Message) (bool, bool) {
	if m.Term < rf.term {
		return false, false
	}
	// step down if received a more up-to-date message or received a message from the current leader.
	if m.Term > rf.term || (m.Type == Append || m.Type == Snap) {
		termChanged := rf.becomeFollower(m.Term)
		return true, termChanged
	}
	return true, false
}

// return true if the raft peer is eligible to handle the message.
// only a follower is eligible to handle RequestVote, AppendEntries, and InstallSnapshot.
func (rf *Raft) checkState(m Message) bool {
	eligible := false

	switch m.Type {
	// only a follower is eligible to handle RequestVote, AppendEntries, and InstallSnapshot.
	case Vote:
		fallthrough
	case Append:
		eligible = rf.state == Follower
	case Snap:
		// warning: not rejecting a new snapshot if there's a pending snapshot may shadow the new snapshot.
		eligible = rf.state == Follower // && !rf.log.hasPendingSnapshot

	case VoteReply:
		// `rf.term == m.Term` ensures that the sender is in the same term as when sending the message.
		eligible = rf.state == Candidate && rf.term == m.ArgsTerm
	case AppendReply:
		// the checking of next index ensures it's exactly the reply corresponding to the last sent AppendEntries.
		eligible = rf.state == Leader && rf.term == m.ArgsTerm && rf.peerTrackers[m.From].nextIndex-1 == m.PrevLogIndex
	case SnapReply:
		// the lag-behind checking ensures the reply corresponds to the last sent InstallSnapshot.
		eligible = rf.state == Leader && rf.term == m.ArgsTerm && rf.lagBehindSnapshot(m.From)

	default:
		log.Fatalf("unexpected message type %v", m.Type)
	}

	// a follower is recommended to reset the election timer to not compete with the acknowledged leader.
	// warning: it's only recommended, not mandatory.
	if rf.state == Follower && (m.Type == Append || m.Type == Snap) {
		rf.resetElectionTimer()
	}

	return eligible
}

// 如果是回复的消息则重置计时器
func (rf *Raft) checkMessage(m Message) (bool, bool) {
	// refresh the step down timer if received a reply.
	// warning: no need to differentiate peer state.
	if m.Type == VoteReply || m.Type == AppendReply || m.Type == SnapReply {
		rf.peerTrackers[m.From].lastAck = time.Now()
	}

	ok, termChanged := rf.checkTerm(m)
	if !ok || !rf.checkState(m) {
		return false, termChanged
	}
	return true, termChanged
}
