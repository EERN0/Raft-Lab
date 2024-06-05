package raft

import "time"

// 心跳、日志同步rpc请求参数
type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

// 心跳、日志同步rpc请求的返回值
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// 回调，接收方收到心跳后执行该回调
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	// 检查任期，心跳rpc的任期 和 当前rf节点的任期
	if args.Term < rf.currentTerm {
		// 丢弃这个请求
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log", args.LeaderId)
		return
	}
	if args.Term >= rf.currentTerm {
		// 变为follower，维护leader的地位
		rf.becomeFollowerLocked(args.Term)
	}

	// 重置选举超时时间
	rf.resetElectionTimerLocked()
	reply.Success = true
}

// 发送发送心跳/日志同步请求的rpc，回调函数处理后，把响应结果存入reply
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 对除自己外的peer 发送心跳/日志同步请求
func (rf *Raft) startReplication(term int) bool {
	// 定义发送给peer心跳/日志同步请求方法的逻辑（接收请求和处理响应）
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		// 回调函数中把响应结果放到reply中
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed", peer)
			return
		}
		// 判断响应的任期 和 当前节点rf的任期
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 同样得在异步调用之前，检查当前rf节点的角色、任期是否变了，变了后续无需执行
	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLeader, "Leader[T%d] -> %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if rf.me == peer {
			continue
		}

		args := &AppendEntriesArgs{
			Term:     term,
			LeaderId: rf.me,
		}
		// 对每个peer发送rpc请求
		go replicateToPeer(peer, args)
	}
	return true
}

// leader定期向所有从节点发送心跳或日志复制请求，维持leader地位。仅在给定term任期内有效
func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {

		ok := rf.startReplication(term)
		if !ok {
			return
		}

		time.Sleep(replicateInterval)
	}
}
