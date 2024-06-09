package raft

import (
	"sort"
	"time"
)

// 如果两个节点的日志在相同的索引位置上的任期号也相同，认为这两日志相同，且从日志开头到这个索引位置之间的日志也完全相同
// 日志项条目由任期和命令组成
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
	LeaderCommit int        // leader已提交的日志索引，通过rpc发给每个follower
}

// 心跳、日志同步rpc请求的返回值
type AppendEntriesReply struct {
	Term    int
	Success bool

	// follower告诉leader自己的日志大致的位置
	ConflictIndex int
	ConflictTerm  int
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

	// 只要认可对方是leader就要重置选举时钟，不管日志匹配是否成功，避免leader和follower匹配日志时间过长
	defer rf.resetElectionTimerLocked()

	// 日志不匹配：日志的索引位置 或 任期 不同
	// 1、leader前一条日志索引 超出 follower 的日志范围，表示leader的日志比follower的多
	if args.PrevLogIndex >= len(rf.log) {
		// follower日志少了, 设置ConfilictTerm为无效任期 0, ConfilictIndex为len(rf.log)———下次直接让leader发送这条日志之后的所有日志条目，同步follower的日志
		reply.ConflictTerm = InvalidTerm
		reply.ConflictIndex = len(rf.log)
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject Log, Follower's log too short, Length:%d <= Leader's log PrevLogIndex:%d", args.LeaderId, len(rf.log), args.PrevLogIndex)
		return
	}
	// 2、本地节点rf（follower）的日志任期 不等于 leader前一条日志的任期
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// ConfilictTerm设置为Follower在Leader.PrevLogIndex处日志任期，ConfilictIndex设置为ConfilictTerm的第一条日志
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		reply.ConflictIndex = rf.firstLogIndexFor(reply.ConflictTerm)
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject Log, Prev log not match, [%d]: T%d != T%d", args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	}

	// 前面都没问题，本地同步leader的日志
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	// 节点 currentTerm || votedFor || log改变，都需要持久化
	rf.persistLocked()
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "Follower append logs: (%d, %d]", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	// follower通过AppendEntries的回调函数收到leader发来的LeaderCommit，更新本地的CommitIndex，再进行日志应用
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update the commit index %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
		if rf.commitIndex >= len(rf.log) {
			rf.commitIndex = len(rf.log) - 1
		}
		// 唤醒 日志应用 开始干活
		rf.applyCond.Signal()
	}

}

// 发送发送心跳/日志同步请求的rpc，回调函数处理后，把响应结果存入reply
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 计算大多数节点已经匹配的最大日志索引
func (rf *Raft) getMajorityIndexLocked() int {
	tmpIndexes := make([]int, len(rf.peers))
	copy(tmpIndexes, rf.matchIndex)
	sort.Ints(sort.IntSlice(tmpIndexes))
	majorityIdx := (len(tmpIndexes) - 1) / 2
	LOG(rf.me, rf.currentTerm, DDebug, "Match index after sort: %v, majority[%d]=%d", tmpIndexes, majorityIdx, tmpIndexes[majorityIdx])
	return tmpIndexes[majorityIdx]
}

// leader对follower 发送心跳/日志同步请求
func (rf *Raft) startReplication(term int) bool {
	// leader 发送给 peer 心跳/日志复制 请求方法的逻辑（接收请求和处理响应）
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		// 向指定的peer发送 AppendEntries rpc请求，把响应结果放到reply中
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed", peer)
			return
		}
		// peer（角色是follower）返回的任期 reply.Term 大于当前任期，leader退位成为follower
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// 检查节点rf的context是否改变
		if rf.contextLostLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Context Lost, T%d:Leader->T%d:%s", peer, term, rf.currentTerm, rf.role)
			return
		}

		// 处理leader和follower日志不匹配的情况（索引或者任期不同）
		if !reply.Success {
			prevNextIndex := rf.nextIndex[peer]

			// 1、follower返回的冲突任期是无效任期，follower日志数量少了
			if reply.ConflictTerm == InvalidTerm {
				// leader下次发给follower的日志同步 PrevLogIndex 从reply.ConflictIndex-1（即follower日志的长度-1）开始，快速回退到 Follower 日志末尾
				rf.nextIndex[peer] = reply.ConflictIndex
			} else {
				// 2、leader和follower日志数量一样，但是任期没对上，跳过 ConfilictTerm 的所有日志
				firstLogIndex := rf.firstLogIndexFor(reply.ConflictTerm)
				// 根据冲突任期找到leader日志中对应任期的第一个日志条目索引，下次发的日志同步请求从firstLogIndex开始
				if firstLogIndex != InvalidIndex {
					rf.nextIndex[peer] = firstLogIndex + 1
				} else {
					// leader日志中不存在ConfilictTerm的任何日志，以follower的日志为准跳过ConflictTerm
					rf.nextIndex[peer] = reply.ConflictIndex
				}

				// 存在网络中延迟，避免超时响应的响应到达，更新rf.nextIndex
				if rf.nextIndex[peer] > prevNextIndex {
					rf.nextIndex[peer] = prevNextIndex
				}
			}

			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Not matched at %d, try next=%d", peer, args.PrevLogIndex, rf.nextIndex[peer])
			return
		}
		// follower 成功追加了日志条目，更新matchIndex和nextIndex
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries) // follower已匹配的日志条目最后的索引
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1                // leader下一次发送日志条目的索引

		// 成功追加日志条目后，更新commitIndex
		majorityMatched := rf.getMajorityIndexLocked() // 多数匹配索引
		if majorityMatched > rf.commitIndex {
			LOG(rf.me, rf.currentTerm, DApply, "Leader update the commit index %d->%d", rf.commitIndex, majorityMatched)
			rf.commitIndex = majorityMatched
			rf.applyCond.Signal()
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
		// leader更新自己维护的follower日志视图
		if rf.me == peer {
			rf.matchIndex[peer] = len(rf.log) - 1
			rf.nextIndex[peer] = len(rf.log)
			continue
		}

		prevIdx := rf.nextIndex[peer] - 1
		prevTerm := rf.log[prevIdx].Term
		args := &AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log[prevIdx+1:],
			LeaderCommit: rf.commitIndex,
		}
		// 对每个peer发送rpc请求
		go replicateToPeer(peer, args)
	}
	return true
}

// leader定期向所有从节点发送心跳、日志同步请求，维持leader地位。仅在给定term任期内有效
func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {

		ok := rf.startReplication(term)
		if !ok {
			return
		}

		time.Sleep(replicateInterval)
	}
}
