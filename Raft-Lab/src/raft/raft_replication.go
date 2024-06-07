package raft

import "time"

type LogEntry struct {
	Term         int         // log entry的任期
	CommandValid bool        // 有效命令将被执行
	Command      interface{} // 具体命令
}

// 心跳、日志同步rpc请求参数
type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int        // leader日志中的前一个条目的索引，检查follower日志是否一致
	PrevLogTerm  int        // leader前一条日志的任期
	Entries      []LogEntry // 需要复制到follower日志中的新日志。可以是多个日志条目，也可以是空的（心跳）
}

// 心跳、日志同步rpc请求的返回值
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// 回调   接收方(follower)收到leader发来的心跳、日志复制rpc请求后，执行该回调函数
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

	// leader前一条日志索引 > 接收方本地日志总和，日志不匹配
	if args.PrevLogIndex > len(rf.log) {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject Log, Follower log too short, Len:%d <= Prev:%d", args.LeaderId, len(rf.log), args.PrevLogIndex)
		return
	}
	// 本地日志的term 是否 等于 日志同步请求的term
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject Log, Prev log not match, [%d]: T%d != T%d", args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	}

	// 前面都没问题，本地同步leader的日志
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "Follower append logs: (%d, %d]", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	// 重置选举超时时间
	rf.resetElectionTimerLocked()
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
