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
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	Follower = iota
	Candidate
	Leader
)
const tickInterval = 50 * time.Millisecond
const heartbeatTimeout = 150 * time.Millisecond

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at ter's Figure 2 for a description of what
	// state a Raft server must maintain.
	state            int            // 节点状态， Candidate-Follower-Leader
	currentTerm      int            //当前的任期
	votedFor         int            // 投票给谁
	heartbeatTimeout time.Duration  // 心跳定时器
	electionTimeout  time.Duration  // 选举计时器
	lastElection     time.Time      // 上一次选举时间
	lastHeartbeat    time.Time      // 上一次心跳时间
	peerTrackers     []PeerTrackers // 跟踪从节点的next index， match index， 上一次同步时间
	log              *Log           // 日志记录

	// Volatile state
	commitIndex int // commitIndex是本机提交的
	lastApplied int // lastApplied是该日志在所有的机器上都跑了一遍后才会更新？

	applyHelper *ApplyHelper
	applyCond   *sync.Cond
}

type RequestAppendEntriesArgs struct {
	LeaderTerm   int // Leader的Term
	PrevLogIndex int // 新日志条目的上一个日志的索引
	PrevLogTerm  int // 新日志的上一个日志的任期
	//Logs         []ApplyMsg //	需要被保存的日志条目，可能有多个
	LeaderCommit int // Leader已提交的最高日志项目的索引
	LeaderId     int
	Entries      []Entry
}

type RequestAppendEntriesReply struct {
	FollowerTerm  int  // Follower的Term，
	Success       bool // 是否推送成功
	ConflictIndex int  // 冲突条目的下标
	ConflictTerm  int  // 冲突条目的任期、
}

func (rf *Raft) getHeartbeatTime() time.Duration {
	return time.Millisecond * 110
}

// 随机化选举超时时间
func (rf *Raft) getElectionTime() time.Duration {
	return time.Millisecond * time.Duration(350*rand.Intn(200))
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendRequestAppendEntries(isHeartBeat bool, server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	var ok bool
	if isHeartBeat {
		ok = rf.peers[server].Call("Raft.HandleHeartbeatRPC", args, reply)
	} else {
		ok = rf.peers[server].Call("Raft.HandleAppendEntriesRPC", args, reply)
	}
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will eveed to the Raft log, since the leader
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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state != Leader {
		isLeader = false
		return index, term, isLeader
	}

	DPrintf("%v: a command index=%v cmd=%T %v come", rf.SayMeL(), index, command, command)
	rf.log.appendL(Entry{term, command})
	//rf.resetTrackedIndex()
	DPrintf("%v: check the newly added log index：%d", rf.SayMeL(), rf.log.LastLogIndex)
	go rf.StartAppendEntries(false)

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any gr be committoroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) StartAppendEntries(heart bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetElectionTimer()
	// 并行向其他节点发送心跳，通知leader已经产生
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.AppendEntries(i, heart)
	}
}

// 定义一个心跳兼日志同步处理器，这个方法是Candidate和Follower节点的处理
func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	DPrintf("receiving heartbeat from leader %d and gonna get the lock...\n", args.LeaderId)
	rf.mu.Lock()
	reply.Success = true
	DPrintf("%d receive heartbeat at leader %d's term %d, and my term is %d", rf.me, args.LeaderId, args.LeaderTerm, rf.currentTerm)
	// 旧任期的leader抛弃
	if args.LeaderTerm < rf.currentTerm {
		reply.FollowerTerm = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	rf.mu.Lock()
	rf.resetElectionTimer()
	rf.state = Follower

	// 需要转变自己的身份为Follower
	// 承认来者是个合法的新leader，则任期一定大于自己，此时需要设置votedFor为-1以及
	if args.LeaderTerm > rf.currentTerm {
		rf.votedFor = None
		rf.currentTerm = args.LeaderTerm
		reply.FollowerTerm = rf.currentTerm
	}
	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(targetServerId int, heart bool) {
	if heart {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		reply := RequestAppendEntriesReply{}
		args := RequestAppendEntriesArgs{}
		args.LeaderTerm = rf.currentTerm
		args.LeaderId = rf.me
		DPrintf("%v: %d is a leader, ready sending heartbeart to follower %d....", rf.SayMeL(), rf.me, targetServerId)
		rf.mu.Unlock()

		// 发送心跳包
		ok := rf.sendRequestAppendEntries(true, targetServerId, &args, &reply)
		if !ok {
			return
		}
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		if reply.FollowerTerm < rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		// 拒绝接收心跳，则可能是因为任期导致的
		if reply.FollowerTerm > rf.currentTerm {
			rf.votedFor = None
			rf.state = Follower
			rf.currentTerm = reply.FollowerTerm
		}
		rf.mu.Unlock()
		return
	} else {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		args := RequestAppendEntriesArgs{}
		args.PrevLogIndex = min(rf.log.LastLogIndex, rf.peerTrackers[targetServerId].nextIndex-1)
		if args.PrevLogIndex+1 < rf.log.FirstLogIndex {
			DPrintf("此时 %d 节点的nextIndex为%d,LastLogIndex为 %d, 最后一项日志为：\n", rf.me, rf.peerTrackers[rf.me].nextIndex,
				rf.log.LastLogIndex)
			return
		}
		args.LeaderTerm = rf.currentTerm
		args.LeaderId = rf.me
		args.LeaderCommit = rf.commitIndex
		args.PrevLogTerm = rf.getEntryTerm(args.PrevLogIndex)
		args.Entries = rf.log.getAppendEntries(args.PrevLogIndex + 1)
		DPrintf("%v: the len of log entries: %d is ready to send to node %d!!! and the entries are %v\n",
			rf.SayMeL(), len(args.Entries), targetServerId, args.Entries)
		rf.mu.Unlock()

		//fmt.Printf
		reply := RequestAppendEntriesReply{}

	}

}

func (rf *Raft) SayMeL() string {

	//return fmt.Sprintf("[Server %v as %v at term %v]", rf.me, rf.state, rf.currentTerm)
	return "success"
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// 如果这个raft节点没有掉线,则一直保持活跃不下线状态（可以因为网络原因掉线，也可以tester主动让其掉线以便测试）
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case Follower:
			fallthrough
		case Candidate:
			if rf.pastElectionTimeout() {
				rf.startElection()
			}
		case Leader:
			// 只有Leader节点才能发送心跳和日志给从节点
			isHeartbeat := false
			// 检测是需要发送单纯的心跳还是发送日志
			// 心跳定时器过期则发送心跳，否则发送日志
			if rf.pastHeartbeatTimeout() {
				isHeartbeat = true
				rf.resetHeartbeatTimer()
			}
			rf.StartAppendEntries(isHeartbeat)
		}
		time.Sleep(tickInterval)
	}
	DPrintf("tim")
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = None
	rf.state = Follower
	rf.heartbeatTimeout = heartbeatTimeout

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
