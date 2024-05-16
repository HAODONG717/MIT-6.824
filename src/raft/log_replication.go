package raft

import "time"

func (rf *Raft) pastHeartbeatTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Since(rf.lastHeartbeat) > rf.heartbeatTimeout
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHeartbeat = time.Now()
}

func (rf *Raft) tryCommitL(matchIndex int) {
	if matchIndex <= rf.commitIndex {
		// 首先matchIndex应该是大于leader节点的commitIndex才能提交，因为commitIndex及其之前的不需要更新
		return
	}
	// 越界的也不能提交
	if matchIndex > rf.log.LastLogIndex {
		return
	}
	if matchIndex < rf.log.FirstLogIndex {
		return
	}
	// 提交的日志必须在当前任期内
	if rf.getEntryTerm(matchIndex) != rf.currentTerm {
		return
	}

	// 计算所有已经正确匹配该matchIndex的从节点的票数
	cnt := 1 //自动计算上leader节点的一票
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// 为什么只需要保证提交的matchIndex必须小于等于其他节点的matchIndex就可以认为这个节点在这个matchIndex记录上正确匹配呢？
		// 因为matchIndex是增量的，如果一个从节点的matchIndex=10，则表示该节点从1到9的子日志都和leader节点对上了
		if matchIndex <= rf.peerTrackers[i].matchIndex {
			cnt++
		}
	}

	// 超过半数就提交
	if cnt > len(rf.peers)/2 {
		rf.commitIndex = matchIndex
		if rf.commitIndex > rf.log.LastLogIndex {
			DPrintf("%v: commitIndex > lastlogindex %v > %v", rf.SayMeL(), rf.commitIndex, rf.log.LastLogIndex)
			panic("")
		}
		// DPrintf(500, "%v: commitIndex = %v ,entries=%v", rf.SayMeL(), rf.commitIndex, rf.log.Entries)
		DPrintf("%v: 主结点已经提交了index为%d的日志，rf.applyCond.Broadcast(),rf.lastApplied=%v rf.commitIndex=%v", rf.SayMeL(), rf.commitIndex, rf.lastApplied, rf.commitIndex)
		rf.applyCond.Broadcast() // 通知对应的applier协程将日志放到状态机上验证
	} else {
		DPrintf("\n%v: 未超过半数节点在此索引上的日志相等，拒绝提交....\n", rf.SayMeL())
	}
}
