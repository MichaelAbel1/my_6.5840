package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"errors"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

const (
	Leader State = iota
	Candidate
	Follower
)

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Server state
	state State

	// Persistent state on all servers:
	currentTerm       int
	votedFor          int        // 记录当前任期内该节点投票给的候选人 ID。如果当前任期内还未投票，则值为 -1。
	log               []LogEntry // 日志条目列表，保存了 Raft 日志的内容，每个日志条目 LogEntry 包含了操作命令和对应的任期号
	lastIncludedIndex int        // 快照机制相关。表示当前快照中包含的最后一个日志条目的全局索引，表示在日志中之前的所有条目都已经被包含在快照中
	lastIncludedTerm  int        // 快照机制相关。表示在快照中包含的最后一条日志条目的任期号

	// Volatile state on all servers:
	commitIndex int // 已经提交的最高日志条目的索引。这个索引表示已经复制到大多数节点并应用到状态机的日志条目。
	lastApplied int // 已经被应用到状态机的最高日志条目的索引。lastApplied 会逐步追上 commitIndex，即所有提交的日志都会被应用到状态机

	// Volatile state on leaders:
	nextIndex []int // 对于每个其他服务器，记录发送给该服务器的下一个日志条目的索引。初始值为该节点日志的最后一个索引加 1

	// 对于每个其他服务器，记录该服务器已经复制的日志的最高索引，帮助 Leader 跟踪每个节点的日志同步进度。
	matchIndex []int

	lastAck []time.Time

	// Channel
	applyCh   chan ApplyMsg // 当日志条目被应用到状态机时，通过该通道将日志的应用通知给上层服务。
	applyCond *sync.Cond    // 件变量，用于在某些条件满足时（如 commitIndex 变化）通知其他 goroutine

	// Timers
	lastHeartbeat time.Time // 记录上次收到心跳（或者选举）消息的时间点，用于跟踪心跳超时

	// Snapshot
	snapshot      []byte // 存储当前节点的快照数据。当状态机状态非常大时，快照可以帮助压缩日志，避免日志无限增长
	applySnapshot bool   // 标识是否应该应用快照。用来确保在从快照恢复状态时，状态机应用快照内容而不是直接应用日志。
}

const (
	// let the base election timeout be T.
	// the election timeout is in the range [T, 2T).
	heartbeatTimeout = 250
	// if the peer has not acked in this duration, it's considered inactive.
	activeWindowWidth = 2 * heartbeatTimeout
	tickInterval      = 30
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, !rf.killed() && rf.state == Leader
}

func (rf *Raft) IsLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log) != nil ||
		e.Encode(rf.lastIncludedIndex) != nil ||
		e.Encode(rf.lastIncludedTerm) != nil {
		panic("failed to encode some fields")
	}
	// 将 bytes.Buffer 中的内容转换为字节片 ([]byte)
	raftstate := w.Bytes()

	rf.persister.Save(raftstate, rf.snapshot)

	// DPrintf("Server %v %p (Term: %v) persisted state", rf.me, rf, rf.currentTerm)
}

// restore previously persisted state.
// should be in lock
func (rf *Raft) readPersist(stateData []byte) {
	if stateData == nil || len(stateData) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(stateData)
	d := labgob.NewDecoder(r)

	// 不初始化会出现 labgob warning: Decoding into a non-default variable/field int may not work
	var currentTerm, votedFor int
	var tmplog []LogEntry
	var lastIncludedIndex, lastIncludedTerm int

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&tmplog) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		log.Fatalf("Server %v %p (Term: %v) readPersist error", rf.me, rf, rf.currentTerm)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = tmplog
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm

		// rf.snapshot = rf.persister.ReadSnapshot()
		rf.lastApplied = rf.lastIncludedIndex
		rf.commitIndex = rf.lastIncludedIndex
		DPrintf("rf.readPersist: Server %v %p (Term: %v, State: %v) readPersist, rf.log: %v", rf.me, rf, rf.currentTerm, rf.state, rf.log)
	}

}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %v %p (Term: %v, State: %v) Snapshot: %v", rf.me, rf, rf.currentTerm, rf.state, index)
	if index <= rf.lastIncludedIndex {
		return
	}
	term := rf.log[index-rf.getFirstLogIndex()].Term
	rf.trimLog(index, term)
	// logIndex := index - rf.getFirstLogIndex()
	// rf.lastIncludedIndex = rf.log[logIndex].Index // 就是index
	// rf.lastIncludedTerm = rf.log[logIndex].Term
	// rf.log = rf.log[logIndex:]
	rf.snapshot = snapshot
	rf.persist()

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %v %p (Term: %v, State: %v) received RequestVote request from Server %v (Term: %v)", rf.me, rf, rf.currentTerm, rf.state, args.CandidateId, args.Term)
	// 如果请求者的任期小于当前节点的任期，直接拒绝
	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	// 如果请求者的任期大于当前节点，更新当前节点的任期和投票情况
	// 每个任期都会投票一次
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.stateChange(Follower) // 回到follower状态 并重置投票   ！！ ----这样才是正确的 但是在3C的 more persistence 会不通过----  ！！！
		defer rf.persist()
	}

	// only a follower is eligible to handle RequestVote, AppendEntries, and InstallSnapshot.
	if rf.state != Follower {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	// 如果已经投票给其他节点而不是请求者或自己，不再投票
	// 相同任期内投给自己的票，如果在别人日志比较新，可以重新投给别人
	// 如果不加这个rf.votedFor != rf.me条件，在大多数都投票给自己的情况下，不会出现领导者
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		// 给予投票
		rf.votedFor = args.CandidateId
		reply.Term, reply.VoteGranted = rf.currentTerm, true
		DPrintf("Server %v %p (Term: %v, State: %v) voted for %v", rf.me, rf, rf.currentTerm, rf.state, args.CandidateId)
		rf.resetTimer()
		// 持久化状态并更新
		rf.persist()
	} else {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

}

// --- AppendEntries ---
// AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type Err int

const (
	Rejected Err = iota
	Matched
	IndexNotMatched
	TermNotMatched
)

type AppendEntriesReply struct {
	Term               int
	LastLogIndex       int
	ConflictTerm       int // 冲突任期
	FirstConflictIndex int // 冲突任期的第一条日志的索引
	Err                Err
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		isLeader = false
	} else {
		// append log
		term = rf.currentTerm
		index = rf.getLastLogIndex() + 1
		rf.log = append(rf.log, LogEntry{Command: command, Index: index, Term: term})
		rf.persist()
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead) // 通过原子加载rf.dead的值
	return z == 1
}

func (rf *Raft) quorumActive() bool {
	activePeers := 1
	for peer := range rf.peers {
		if peer != rf.me && time.Since(rf.lastAck[peer]) <= activeWindowWidth {
			activePeers++
		}
	}
	return 2*activePeers > len(rf.peers)
}

func (rf *Raft) ticker() {
	for !rf.killed() { // 服务器还存活
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		DPrintf("ticker(): Server %v %p (Term: %v, State: %v) is ticking", rf.me, rf, rf.currentTerm, rf.state)
		if rf.state == Follower || rf.state == Candidate {
			// If the server is a follower, start an election.
			if time.Since(rf.lastHeartbeat) > time.Duration(heartbeatTimeout+(rand.Int63()%heartbeatTimeout))*time.Millisecond {
				// we shall detect the time gap since we receive leader's last heartbeat, too long means the leader is dead, we should start an election

				// 1. After Follower become Candidate fail in become leader for the fist time, try again
				// 2. Candidate become leader , try again
				// 3. if the server is candidate but disconnected, when it come back it should return to normal state
				// 4. if the server is candidate but disconnected, when it come back it should continue the election
				DPrintf("Server %v %p (Term: %v, State: %v) is now a candidate, duration: %v", rf.me, rf, rf.currentTerm, rf.state, time.Now().Sub(rf.lastHeartbeat))
				rf.stateChange(Candidate) // 当节点成为 Candidate 时，它会立即给自己投票
				// Candidate broadcasts RequestVote RPCs to all other servers and becomes leader if majority vote received
				rf.broadcastRequestVote()
			}
		} else if rf.state == Leader { // rf.state == Leader
			DPrintf("ticker(): Server %v %p (Term: %v, State: %v) is a leader, duration: %v", rf.me, rf, rf.currentTerm, rf.state, time.Since(rf.lastHeartbeat))
			// 如果很久没更新心跳了
			// if !rf.quorumActive() {
			// 	rf.stateChange(Follower) // !!! 用来处理网络分区
			// 	break
			// }

			forced := false
			if time.Since(rf.lastHeartbeat) > time.Duration(3*tickInterval)*time.Millisecond {
				forced = true // !!! 如果领导者很久没有新日志，那么需要定期发送心跳包给follower，防止follower选举时间超时
				DPrintf("ticker(): Server %v %p (Term: %v, State: %v) forced to send heartbeat, duration: %v", rf.me, rf, rf.currentTerm, rf.state, time.Since(rf.lastHeartbeat))
				rf.resetTimer()
			}
			rf.broadcastHeartBeat(forced)
		} else {
			log.Fatalf("ticker(): Server %v %p (Term: %v), Invalid state: %v!!!", rf.me, rf, rf.currentTerm, rf.state)
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 50
		// milliseconds.
		// 如果是%300 会导致warning: term changed even though there were no failures
		// 因为上面会心跳超时时间和休息时间差不多，导致进入选举
		time.Sleep(time.Duration(tickInterval) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = Follower

	rf.currentTerm = 0
	rf.votedFor = -1

	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Index: 0, Term: 0, Command: nil}) // 保证了每个raft服务器至少有一条日志

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	if rf.persister.RaftStateSize() > 0 {
		DPrintf("Make(): Server %v %p (Term: %v, State: %v) rf.persister.RaftStateSize() > 0", rf.me, rf, rf.currentTerm, rf.state)
		rf.readPersist(rf.persister.ReadRaftState())
	} else {
		rf.currentTerm = 0
		rf.votedFor = -1
	}
	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	// rf.readPersist(persister.ReadRaftState()) // 持久化一定要在commitIndex,lastApplied,snapshotLastIncludeTerm,snapshotLastIncludeIndex 初始化之后执行，否则持久化后恢复的数据会被覆盖为0
	rf.snapshot = persister.ReadSnapshot()
	rf.applySnapshot = false
	rf.resetTimer()
	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyLog()

	return rf
}

func (rf *Raft) resetTimer() {
	DPrintf("Server %v %p (Term: %v, State: %v) resetTimer", rf.me, rf, rf.currentTerm, rf.state)
	rf.lastHeartbeat = time.Now()
}

// should be called within lock
// Raft 的一致性依赖于持久化的任期（currentTerm）、投票记录（votedFor）和日志条目
// Leader 崩溃后，新的选举会选出新的 Leader，保证了系统的一致性和可用性，因此不需要将 Leader 状态持久化。
func (rf *Raft) stateChange(newState State) {
	DPrintf("Server %v %p (Term: %v, State: %v) state changed from %v to %v", rf.me, rf, rf.currentTerm, rf.state, rf.state, newState)
	if newState == Leader {
		if rf.state != Candidate {
			log.Fatalf("Server %v %p (Term %v) Invalid state change to leader from %v", rf.me, rf, rf.currentTerm, rf.state)
		}
		DPrintf("Server %v %p (Term %v) become leader", rf.me, rf, rf.currentTerm)
		rf.state = Leader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		// newIndex := len(rf.log)
		for i := range rf.peers { // 初始化
			rf.nextIndex[i] = rf.getLastLogIndex() + 1 // !!!! 之前为len(rf.log)
			rf.matchIndex[i] = 0
		}
		// Leader 状态不需要持久化：
		// 成为 Leader 的关键在于获得大多数选票，这一信息在选举过程中已经确定。
		// 成为 Leader 后的主要任务是管理日志复制，而 nextIndex 和 matchIndex 是易失性的状态，
		// 主要用于日志复制过程，并不需要持久化。
	} else if newState == Candidate {
		if rf.state != Follower && rf.state != Candidate {
			log.Fatalf("Server %v %p (Term %v) become candidate", rf.me, rf, rf.currentTerm)
		}
		DPrintf("Server %v %p (Term %v) become Candidate", rf.me, rf, rf.currentTerm)
		rf.state = Candidate
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.resetTimer()
		rf.persist() // !!! 放在状态变化里可能会出现过多持久化，导致不必要的时间消耗
	} else {
		DPrintf("Server %v %p (Term %v) become Follower", rf.me, rf, rf.currentTerm)
		rf.state = Follower
		rf.votedFor = -1 // 重置投票
		rf.persist()
	}

}

// should be in lock
func (rf *Raft) getFirstLogIndex() int {
	// log always have 1 item, check no need
	return rf.log[0].Index
}

// should be in lock
func (rf *Raft) getFirstLogTerm() int {
	// log always have 1 item, check no need
	return rf.log[0].Term
}

// should be in lock
func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

// should be in lock
func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// should be in lock
// func (rf *Raft) isLogUpToDate(index int, term int) bool {
// 	lastLogIndex, lastLogTerm := rf.getLastLogIndex(), rf.getLastLogTerm()
// 	DPrintf("Server %v %p (Term: %v) lastLogIndex %v, %v isLogUpToDate request: %v, %v",
// 		rf.me, rf, rf.currentTerm, lastLogIndex, lastLogTerm, index, term)
// 	if index < lastLogIndex {
// 		return false
// 	} else if index == lastLogIndex && term < lastLogTerm {
// 		return false
// 	}

// 	return true
// }

func (rf *Raft) isLogUpToDate(index int, term int) bool {
	lastLogIndex, lastLogTerm := rf.getLastLogIndex(), rf.getLastLogTerm()
	DPrintf("Server %v %p (Term: %v, State: %v) isLogUpToDate: %v, %v, %v, %v", rf.me, rf, rf.currentTerm, rf.state, index, term, lastLogIndex, lastLogTerm)
	// Section 5.4
	return term > lastLogTerm || (term == lastLogTerm && index >= lastLogIndex)
}

// shall be called within lock: at least we can promise that the state is not changed until all goroutines launchedd
// broadcast requestvote to all peers
func (rf *Raft) broadcastRequestVote() {
	DPrintf("Server %v %p (Term: %v, State: %v) broadcastRequestVote", rf.me, rf, rf.currentTerm, rf.state)
	if rf.state != Candidate {
		DPrintf("Invalid state for broadcastRequestVote: %v", rf.state)
		return
	}

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	// votes counter, including itself
	receivedVotes := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(peer int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(peer, args, reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// !!! 只有以下三种才设置 已经接收到 1.投票回复 2.append回复 3.快照回复
			// rf.lastAck[peer] = time.Now()

			// 在发送投票请求到接收回复这段时间内，Server的状态或任期可能发生了变化（例如收到了更高任期的投票请求）
			// 避免过时操作
			if rf.state != Candidate || args.Term != rf.currentTerm {
				return
			}
			if reply.VoteGranted {
				// someone vote us
				receivedVotes++
				DPrintf("Server %v %p (Term: %v, State: %v) received vote from %v, receivedVotes: %v", rf.me, rf, rf.currentTerm, rf.state, peer, receivedVotes)
				if receivedVotes > len(rf.peers)/2 {
					rf.stateChange(Leader)
					rf.broadcastHeartBeat(true) // 立即发送心跳包 阻止心跳超时
				}
			} else {
				// someone's log is more up to date or term is bigger
				DPrintf("Server %v %p (Term: %v, State: %v) received reject from %v", rf.me, rf, rf.currentTerm, rf.state, peer)
				if reply.Term > args.Term {
					// rf.resetTimer()     // 成为 Follower 不需要重置时间
					rf.stateChange(Follower)
					rf.currentTerm = reply.Term
					rf.persist()
				}
			}
		}(i)
	}
}

func (rf *Raft) hasNewEntries(to int) bool {
	return rf.getLastLogIndex() >= rf.nextIndex[to]
}

func (rf *Raft) sendEntries(peer int) {
	rf.mu.Lock()
	if rf.state != Leader {
		DPrintf("Server %v %p (Term: %v, State: %v) Invalid state for sendEntries: %v", rf.me, rf, rf.currentTerm, rf.state, rf.state)
		rf.mu.Unlock()
		return
	}
	// DPrintf("Raft.sendEntries : Server %v %p (Term: %v, State: %v) sendEntries to Server %v getFirstLogIndex: %v nextIndex: %v rf.log: %v", rf.me, rf, rf.currentTerm, rf.state, peer, rf.getFirstLogIndex(), rf.nextIndex[peer], rf.log)
	reply := &AppendEntriesReply{}
	prevLogIndex := rf.nextIndex[peer] - 1
	prevLogTerm, err := rf.getterm(prevLogIndex)
	if err != nil {
		DPrintf("Raft.sendEntries : Server %v %p (Term: %v, State: %v) , getterm error: %v", rf.me, rf, rf.currentTerm, rf.state, err)
		return
	}
	entriesCopy := make([]LogEntry, len(rf.log[(rf.nextIndex[peer]-rf.getFirstLogIndex()):]))
	copy(entriesCopy, rf.log[(rf.nextIndex[peer]-rf.getFirstLogIndex()):]) // 拷贝需要发送的日志条目
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		// Raft 中每个节点的日志条目是按顺序追加的，但随着日志的增长，Raft 节点
		// 可能会进行日志截断（例如通过快照机制，来节省存储空间），这时日志的起始索引
		//（getFirstLogIndex()）可能不是 0，而是一个更大的数。
		PrevLogTerm:  prevLogTerm,
		Entries:      entriesCopy, // !!! 不能直接使用rf.log[(rf.nextIndex[peer]-rf.getFirstLogIndex()):]
		LeaderCommit: rf.commitIndex,
	}
	// DPrintf("Server %v %p (Term: %v, State: %v) send AppendEntries to %v, args: %v", rf.me, rf, rf.currentTerm, rf.state, peer, args)
	rf.mu.Unlock()
	ok := rf.sendAppendEntries(peer, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// !!! 只有以下三种才设置 已经接收到 1.投票回复 2.append回复 3.快照回复
		// rf.lastAck[peer] = time.Now()

		// 丢掉旧rpc响应 否则导致不必要开销
		if reply.Term < rf.currentTerm {
			return
		}

		// 如果任期改变，转换为 Follower
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.stateChange(Follower)
			defer rf.persist()
		}

		// the checking of next index ensures it's exactly the reply corresponding to the last sent AppendEntries.
		if rf.state != Leader || rf.currentTerm != args.Term || rf.nextIndex[peer]-1 != args.PrevLogIndex {
			return
		}

		switch reply.Err {
		case Rejected:
			// do nothing.

		case Matched:
			rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1

			// broadcast immediately if the committed index got updated so that follower can
			// learn the committed index sooner.
			if rf.updateCommitIndex(rf.matchIndex[peer]) {
				rf.broadcastHeartBeat(true)
			}

		case IndexNotMatched:
			// warning: only if the follower's log is actually shorter than the leader's,
			// the leader could adopt the follower's last log index.
			// in any cases, the next index cannot be larger than the leader's last log index + 1.
			if reply.LastLogIndex < rf.getLastLogIndex() {
				rf.nextIndex[peer] = reply.LastLogIndex + 1
			} else {
				rf.nextIndex[peer] = rf.getLastLogIndex() + 1
			}

			// broadcast immediately so that followers can quickly catch up.
			rf.broadcastHeartBeat(true)

		case TermNotMatched:
			newNextIndex := reply.FirstConflictIndex
			// warning: skip the snapshot index since it cannot conflict if all goes well.
			for i := rf.getLastLogIndex(); i > rf.getFirstLogIndex(); i-- {
				if term, _ := rf.getterm(i); term == reply.ConflictTerm {
					newNextIndex = i
					break
				}
			}

			// FIXME: figure out whether the next index is eligible to be advanced.
			rf.nextIndex[peer] = newNextIndex

			// broadcast immediately so that followers can quickly catch up.
			rf.broadcastHeartBeat(true)
		}

	}
}

func (rf *Raft) needSendSnapshot(to int) bool {
	return rf.nextIndex[to] <= rf.getFirstLogIndex()
}

func (rf *Raft) broadcastHeartBeat(forced bool) {
	DPrintf("Server %v %p (Term: %v, State: %v) broadcast HeartBeat", rf.me, rf, rf.currentTerm, rf.state)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		if rf.state != Leader {
			DPrintf("Server %v %p (Term: %v) Invalid state for broadcastHeartBeat: %v", rf.me, rf, rf.currentTerm, rf.state)
			return
		}
		// if rf.lastIncludedIndex > 0 && rf.nextIndex[peer] <= rf.lastIncludedIndex { // !!!!!!!!!!! 可能有问题
		// 	go rf.sendSnapshot(peer)

		if rf.needSendSnapshot(peer) {
			// DPrintf("rf.broadcastHeartBeat: Server peer: %v needSendSnapshot from Server %v %p (Term: %v, State: %v) send Snapshot to %v", peer, rf.me, rf, rf.currentTerm, rf.state, peer)
			go rf.sendSnapshot(peer) // !!!
		} else if forced || rf.hasNewEntries(peer) {
			// 由于没有新日志或者不强制时，不会发送日志更新follower的lastheartbeat 会出现 warning: term changed even though there were no failures
			// 所以需要定时强制更新一次
			go rf.sendEntries(peer)
		}

	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

// --- Install Snapshot ---
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term     int
	CaughtUp bool
}

// shall be called as go routine
// if rf.lastIncludedIndex > 0 && rf.nextIndex[peer] <= rf.lastIncludedIndex 才调用
func (rf *Raft) sendSnapshot(peer int) {
	rf.mu.Lock()
	reply := &InstallSnapshotReply{}
	if rf.state != Leader {
		DPrintf("Server %v %p (Term: %v) Invalid state for sendSnapshot: %v", rf.me, rf, rf.currentTerm, rf.state)
		rf.mu.Unlock()
		return
	}
	args := &InstallSnapshotArgs{ // !!! 读取不加锁 是因为读到旧数据也不影响结果，如果很旧，其他会拒绝，重新发就行了
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Snapshot:          rf.snapshot,
		Done:              true,
	}
	DPrintf("rf.sendSnapshot: Server %v %p (Term: %v, State: %v) send snapshot to %v, args.Snapshot: %v", rf.me, rf, rf.currentTerm, rf.state, peer, args.Snapshot)
	rf.mu.Unlock()
	if rf.sendInstallSnapshot(peer, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// !!! 只有以下三种才设置 已经接收到 1.投票回复 2.append回复 3.快照回复
		// rf.lastAck[peer] = time.Now()

		// if reply.Term < rf.currentTerm {
		// 	return  // !!! 感觉这一段判断没必要
		// }

		if reply.Term > rf.currentTerm {
			rf.stateChange(Follower)
			rf.currentTerm = reply.Term
			rf.persist()
			return
		}

		// !! sendInstallSnapshot发出去后阻塞在网络中，再回来时，状态可能已经发生改变 args.Term == rf.currentTerm && rf.lagBehindSnapshot(m.From)保证回复和发送的rpc相符
		if rf.state == Leader && args.Term == rf.currentTerm && rf.needSendSnapshot(peer) {
			if reply.CaughtUp {
				rf.matchIndex[peer] = args.LastIncludedIndex
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1
				DPrintf("Server %v %p (Term: %v, State: %v) Snapshot to Server %v done, args.LastIncludedIndex: %v, rf.matchIndex[peer]: %v", rf.me, rf, rf.currentTerm, rf.state, peer, args.LastIncludedIndex, rf.matchIndex[peer])
				// note: there must already have a majority of followers whose match index is greater than or equal to
				// the snapshot index. Otherwise, the snapshot won't be generated.
				// hence, the update of the match index of a lag-behind follower won't drive the update
				// of the committed index. Hence, there's no need to call `maybeCommitMatched`.
				// the forcing broadcast AppendEntries is used to make lag-behind followers catch up quickly.
				rf.broadcastHeartBeat(true)
			}
		}
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, &reply)
	return ok
}

// RPC方法 首字母需要大写
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("rf.InstallSnapshot: Server %v %p (Term: %v) received InstallSnapshot from Server %v, Snapshot: %v", rf.me, rf, rf.currentTerm, args.LeaderId, args.Snapshot)
	rf.resetTimer()
	reply.Term = rf.currentTerm
	reply.CaughtUp = false

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.stateChange(Follower) // !! 这里有点问题  如果leader和follower任期相同会导致一个任期内 多次投票 但是不影响结果
		reply.Term = rf.currentTerm
	} else { // args.Term == rf.currentTerm
		rf.state = Follower
	}
	defer rf.persist()

	// only a follower is eligible to handle RequestVote, AppendEntries, and InstallSnapshot.
	if rf.state != Follower {
		return
	}

	// reject the snapshot if this peer has already caught up.
	if args.LastIncludedIndex <= rf.commitIndex {
		// return `CaughtUp` true 来 忽略网络冗余请求，不会重复应用快照。
		reply.CaughtUp = true // 该节点已经“跟上”或超越快照索引
		DPrintf("Server %v %p (Term: %v, State: %v) InstallSnapshot from Server %v rejected because args.LastIncludedIndex: %v <= rf.commitIndex: %v, already caught up", rf.me, rf, rf.currentTerm, rf.state, args.LeaderId, args.LastIncludedIndex, rf.commitIndex)
		return
	}

	rf.trimLog(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.snapshot = args.Snapshot
	reply.CaughtUp = true
	// defer rf.persist()

	rf.applySnapshot = true
	rf.applyCond.Signal()
	DPrintf("Server %v %p (Term: %v, State: %v) InstallSnapshot from Server %v success !! rf.snapshot: %v", rf.me, rf, rf.currentTerm, rf.state, args.LeaderId, rf.snapshot)
}

// should be in lock
// 使得args.lastIncludedIndex索引的日志处于raft的第一条日志，压缩
func (rf *Raft) trimLog(snapshoLastLogIndex int, snapshoLastLogTerm int) {
	suffix := make([]LogEntry, 0)
	suffixStart := snapshoLastLogIndex + 1

	if suffixStart <= rf.getLastLogIndex() {
		suffix = rf.log[suffixStart-rf.getFirstLogIndex():]
	}

	rf.log = append(make([]LogEntry, 1), suffix...)
	rf.log[0] = LogEntry{
		Command: nil,
		Index:   snapshoLastLogIndex,
		Term:    snapshoLastLogTerm,
	}

	rf.lastIncludedIndex = snapshoLastLogIndex // !!! 安装快照的时候忘记加这个会在leadder分发快照的时候被follower拒绝
	rf.lastIncludedTerm = snapshoLastLogTerm
	rf.lastApplied = max(rf.lastApplied, snapshoLastLogIndex)
	rf.commitIndex = max(rf.commitIndex, snapshoLastLogIndex)
}

// should be in lock
// 当新选出的领导者的日志比其中一个follower目前提交日志少时，可能会出现follower提交的日志超过领导者的日志最高索引，如果直接index - rf.getFirstLogIndex()会出错
func (rf *Raft) updateCommitIndex(index int) bool {
	for N := index; N > rf.commitIndex; N-- {
		if term, _ := rf.getterm(N); term == rf.currentTerm {
			count := 1 //getFirstLogIndex
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= N {
					count++
				}
			}
			DPrintf("Server %v %p (Term: %v) count: %v, N: %v len(rf.peers): %v", rf.me, rf, rf.currentTerm, count, N, len(rf.peers))
			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				DPrintf("Server %v %p (Term: %v) commitIndex: %v", rf.me, rf, rf.currentTerm, rf.commitIndex)
				rf.applyCond.Signal()
				return true
			}
		}

	}
	return false
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	return ok
}

// warning: not used actually.
var ErrOutOfBound = errors.New("index out of bound")

func (rf *Raft) getterm(index int) (int, error) {
	if index < rf.getFirstLogIndex() || index > rf.getLastLogIndex() {
		return 0, ErrOutOfBound
	}
	index = index - rf.getFirstLogIndex()
	return rf.log[index].Term, nil
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %v %p (Term: %v, State: %v) receive AppendEntries from %v, args: %v", rf.me, rf, rf.currentTerm, rf.state, args.LeaderId, args)
	rf.resetTimer()
	reply.Term = rf.currentTerm
	reply.Err = Rejected

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.stateChange(Follower) // !!! 如果是args.Term >= rf.currentTerm这里有问题  如果leader和follower任期相同会导致一个任期内 多次投票 导致一个任期出现多个leader
		reply.Term = rf.currentTerm
		// defer rf.persist()   // !!!!!! 最开始是这个函数只在这里加持久化 就会出现任期相同但也加了日志时没持久化，3C 测试会失败
	} else { // args.Term == rf.currentTerm  !!! 只有 快照和append中 leader和follower任期相同才会改变成follower
		rf.state = Follower
		rf.resetTimer()
		DPrintf("AppendEntries: Server %v %p (Term: %v, State: %v) become follower", rf.me, rf, rf.currentTerm, rf.state)
	}

	defer rf.persist() // !!!!!!!!!!!!!!!!!!!!!!!!!!!! 注意 如果不加这个 如果不是领导者就不会持久化， args.Term >= rf.currentTerm 都需要持久化

	// only a follower is eligible to handle RequestVote, AppendEntries, and InstallSnapshot.
	if rf.state != Follower {
		return
	}

	// 在跟随者args.PrevLogIndex之前的日志都必须匹配，领导者发送过的日志才能加上
	reply.Err = rf.checkLogPrefixTermMatched(args.PrevLogIndex, args.PrevLogTerm)
	if reply.Err != Matched {
		if reply.Err == IndexNotMatched {
			reply.LastLogIndex = rf.getLastLogIndex()
		} else { // TermNotMatched 日志所在任期不匹配
			reply.ConflictTerm, reply.FirstConflictIndex = rf.findFirstConflict(args.PrevLogIndex)
		}
		return
	}

	for i, entry := range args.Entries { // 在follower日志中找到第一个任期不匹配的日志
		if term, err := rf.getterm(entry.Index); err != nil || term != entry.Term {
			rf.truncateSuffix(entry.Index) // 如果 firstlogindex < index <= lastlogindex，但index对应的任期不同，则截断日志到index的前一条为止
			rf.log = append(rf.log, args.Entries[i:]...)
			// rf.persist()    // !!!!!!!!!!!!!!!!!!!!!!!!!!!! 加在这里是因为可以优化，如果上面退出就不需要持久化，但是会复杂些 不如直接在1034行加一个统一的持久化
			break
		}
	}

	lastNewLogIndex := min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	if lastNewLogIndex > rf.commitIndex {
		rf.commitIndex = lastNewLogIndex
		rf.applyCond.Signal()
	}
}

// FIXME: doubt the out of bound checking is necessary.
// seems only the `index > log.lastIndex()` checking is necessary.
func (rf *Raft) truncateSuffix(index int) {
	if index <= rf.getFirstLogIndex() || index > rf.getLastLogIndex() {
		return
	}

	index = index - rf.getFirstLogIndex()
	if len(rf.log[index:]) > 0 {
		rf.log = rf.log[:index]
	}
}

func (rf *Raft) findFirstConflict(index int) (int, int) {
	conflictTerm, _ := rf.getterm(index)
	firstConflictIndex := index
	// warning: skip the snapshot index since it cannot conflict if all goes well.
	for i := index - 1; i > rf.getFirstLogIndex(); i-- {
		if term, _ := rf.getterm(i); term != conflictTerm { // 如果是冲突任期的日志，则都不匹配
			break
		}
		firstConflictIndex = i
	}
	return conflictTerm, firstConflictIndex
}

func (rf *Raft) checkLogPrefixTermMatched(leaderPrevLogIndex, leaderPrevLogTerm int) Err {
	prevLogTerm, err := rf.getterm(leaderPrevLogIndex)
	if err != nil {
		return IndexNotMatched
	}

	if prevLogTerm != leaderPrevLogTerm {
		return TermNotMatched
	}
	return Matched
}

func (rf *Raft) applyLog() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		// DPrintf("Server %v %p (Term: %v, State: %v) rf.applySnapshot: %v; applyLog: %v, %v, %v", rf.me, rf, rf.currentTerm, rf.state, rf.applySnapshot, rf.lastApplied, rf.commitIndex, rf.log)
		if rf.applySnapshot {
			rf.applySnapshot = false
			msg := ApplyMsg{
				CommandValid:  false,
				Snapshot:      rf.snapshot,
				SnapshotValid: true,
				SnapshotIndex: rf.lastIncludedIndex,
				SnapshotTerm:  rf.lastIncludedTerm,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg // config.go 中的applier函数处理
			rf.mu.Lock()
		} else if rf.lastApplied < rf.commitIndex {
			// DPrintf("rf.applyLog() Server %d apply log , rf.lastApplied: %d , rf.commitIndex: %d", rf.me, rf.lastApplied+1, rf.commitIndex)
			msgs := make([]ApplyMsg, 0)
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i-rf.getFirstLogIndex()].Command,
					CommandIndex: i,
				}
				DPrintf("Server %v %p (Term: %v, State: %v) apply the msg: CommandValid: %v, Command: %v, CommandIndex: %v ", rf.me, rf, rf.currentTerm, rf.state, msg.CommandValid, msg.Command, msg.CommandIndex)
				msgs = append(msgs, msg)
			}
			// DPrintf("rf.applyLog() rf.lastApplied < rf.commitIndex, Server %v %p (Term: %v, State: %v) rf.lastApplied: %v, rf.commitIndex: %v, rf.log: %v, msgs: %v", rf.me, rf, rf.currentTerm, rf.state, rf.lastApplied, rf.commitIndex, rf.log, msgs)
			DPrintf("rf.applyLog() rf.lastApplied < rf.commitIndex, Server %v %p (Term: %v, State: %v) rf.lastApplied: %v, rf.commitIndex: %v", rf.me, rf, rf.currentTerm, rf.state, rf.lastApplied, rf.commitIndex)

			rf.mu.Unlock()
			for _, msg := range msgs {
				rf.applyCh <- msg
				// !!! 这里由于读取共享变量，导致数据竞争 出现死循环
				// DPrintf("Server %v %p (Term: %v, State: %v) apply the msg: CommandValid: %v, Command: %v, CommandIndex: %v ", rf.me, rf, rf.currentTerm, rf.state, msg.CommandValid, msg.Command, msg.CommandIndex)

			}
			rf.mu.Lock()
			DPrintf("Server %v %p (Term %v, State: %v) successfly commit to %v", rf.me, rf, rf.currentTerm, rf.state, rf.commitIndex)
			rf.lastApplied = max(rf.lastApplied, msgs[len(msgs)-1].CommandIndex) // !!! 不能直接是rf.committed 容易出现提交顺序错误
		}

		rf.mu.Unlock()
	}
}
