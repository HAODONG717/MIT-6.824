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
	DPrintf("[%d] attempting an election at term %d...", rf.me, rf.currentTerm)
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}

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
			rf.state = Leader
			go rf.StartAppendEntries(true)

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
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	DPrintf("%v: 选举时间超时，将自身变为Candidate并且发起投票...", rf.SayMeL())
}

func (rf *Raft) ToFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Follower
	rf.votedFor = None
	DPrintf("%v: I am converting to a follower.", rf.SayMeL())
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
	// Lab2B的日志复制直接确定为true
	update := true
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && update {
		//竞选任期大于自身任期，则更新自身任期，并转为follower
		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.resetElectionTimer()
		reply.VoteGranted = true
		DPrintf("%v: 同意把票投给%d, 它的任期是%d", rf.SayMeL(), args.CandidateId, args.Term)
	} else {
		reply.VoteGranted = false
	}
}
