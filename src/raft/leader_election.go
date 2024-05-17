package raft

import (
	"math/rand"
	"time"
)

// let the base election timeout be T.
// the election timeout is in the range [T, 2T).
const baseElectionTimeout = 300
const None = -1

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetElectionTimer()
	rf.becomeCandidate()
	done := false
	votes := 1
	term := rf.currentTerm
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.log.LastLogIndex
	args.LastLogTerm = rf.getLastEntryTerm()
	DPrintf("[%d] attempting an election at term %d with my LastLogIndex %d and LastLogTerm %d", rf.me, rf.currentTerm, rf.log.LastLogIndex, args.LastLogTerm)
	defer rf.persist()
	for i, _ := range rf.peers {
		if rf.me == i {
			continue
		}
		go func(serverId int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(serverId, &args, &reply)
			if !ok || !reply.VoteGranted {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm > reply.Term { // ?
				return
			}
			// 统计票数
			votes++
			if done || votes <= len(rf.peers)/2 {
				// 在成为leader之前如果投票数不足需要继续收集选票
				// 同时在成为leader的那一刻，就不需要管剩余节点的响应了，因为已经具备成为leader的条件
				return
			}
			done = true
			if rf.state != Candidate || rf.currentTerm != term {
				return
			}
			DPrintf("\n%v: [%d] got enough votes, and now is the leader(currentTerm=%d, state=%v)!\n", rf.SayMeL(), rf.me, rf.currentTerm, rf.state)
			//rf.state = Leader // 将自身设置为leader
			rf.becomeLeader()
			DPrintf("\n[%d] got enough votes, and now is the leader(currentTerm=%d, state=%v)!starting to append heartbeat...\n", rf.me, rf.currentTerm, rf.state)

		}(i)
	}
}

func (rf *Raft) pastElectionTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	f := time.Since(rf.lastElection) > rf.electionTimeout
	return f
}

func (rf *Raft) resetElectionTimer() {
	electionTimeout := baseElectionTimeout + (rand.Int63() % baseElectionTimeout)
	rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond
	rf.lastElection = time.Now()
	DPrintf("%v: 选举的超时时间设置为：%d", rf.SayMeL(), rf.electionTimeout)
}

func (rf *Raft) becomeCandidate() {
	//defer rf.persist()
	rf.state = Candidate
	rf.currentTerm++
	//rf.votedMe = make([]bool, len(rf.peers))
	rf.votedFor = rf.me
	rf.resetElectionTimer()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	DPrintf("%v :becomes leader and reset TrackedIndex\n", rf.SayMeL())
	rf.resetTrackedIndex()
}

// 定义一个心跳兼日志同步处理器，这个方法是Candidate和Follower节点的处理
func (rf *Raft) HandleHeartbeatRPC(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock() // 加接收心跳方的锁
	defer rf.mu.Unlock()
	reply.FollowerTerm = rf.currentTerm
	reply.Success = true
	// 旧任期的leader抛弃掉
	if args.LeaderTerm < rf.currentTerm {
		reply.Success = false
		reply.FollowerTerm = rf.currentTerm
		return
	}
	//DPrintf(200, "I am %d and the dead state is %d with term %d", rf.me)
	DPrintf("%v: I am now receiving heartbeat from leader %d at term %d", rf.SayMeL(), args.LeaderId, args.LeaderTerm)
	rf.resetElectionTimer()
	// 需要转变自己的身份为Follower
	rf.state = Follower
	rf.votedFor = args.LeaderId

	// 承认来者是个合法的新leader，则任期一定大于自己，此时需要设置votedFor为-1以及
	if args.LeaderTerm > rf.currentTerm {
		DPrintf("%v: leader %d's term at %d is greater than me", rf.SayMeL(), args.LeaderId, args.LeaderTerm)
		rf.votedFor = None
		rf.currentTerm = args.LeaderTerm
		reply.FollowerTerm = rf.currentTerm
	}
	// 重置自身的选举定时器，这样自己就不会重新发出选举需求（因为它在ticker函数中被阻塞住了）
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//竞选leader的节点任期小于等于自己的任期，则反对票(为什么等于情况也反对票呢？因为candidate节点在发送requestVote rpc之前会将自己的term+1)
	if args.Term < rf.currentTerm {
		DPrintf("%v: candidate的任期是%d, 小于我，所以拒绝", rf.SayMeL(), args.Term)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.persist()
		return
	}
	DPrintf("%v:candidate为%d,任期是%d", rf.SayMeL(), args.CandidateId, args.Term)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = None
		rf.state = Follower
		DPrintf("%v:candidate %d的任期比自己大，所以修改rf.votedFor从%d到-1", rf.SayMeL(), args.CandidateId, rf.votedFor)
	}
	reply.Term = rf.currentTerm
	// candidate节点发送过来的日志索引以及任期必须大于等于自己的日志索引及任期
	update := false
	update = update || args.LastLogTerm > rf.getLastEntryTerm()
	update = update || args.LastLogTerm == rf.getLastEntryTerm() && args.LastLogIndex >= rf.log.LastLogIndex

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && update {
		//竞选任期大于自身任期，则更新自身任期，并转为follower
		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.resetElectionTimer()
		reply.VoteGranted = true
		DPrintf("%v: 同意把票投给%d, 它的任期是%d", rf.SayMeL(), args.CandidateId, args.Term)
		rf.persist()
	} else {
		reply.VoteGranted = false
		DPrintf("%v: 投出反对票给节点%d， 原因：我已经投票给%d", rf.SayMeL(), args.CandidateId, rf.votedFor)

	}
}
