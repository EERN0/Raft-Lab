package raft

import (
	"fmt"
	"math/rand"

	"time"
)

// 重置选举超时时间
func (rf *Raft) resetElectionTimerLocked() {
	rf.electionStart = time.Now()
	randRange := int64(electionTimeoutMax - electionTimeoutMin)
	// rand.Int63()%randRange: 生成一个在 0 到 randRange 之间的随机int64整数。time.Duration()把整数转成时间类型
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange)
}

// 判断选举是否超时
func (rf *Raft) isElectionTimeoutLocked() bool {
	// 判断 自rf.electionStart以来经过的时间间隔 大于 选举超时时间，选举超时
	return time.Since(rf.electionStart) > rf.electionTimeout
}

// 判断候选者Candidate的日志是否比当前节点rf的日志更新
func (rf *Raft) isMoreUpToDateLocked(candidateIndex, candidateTerm int) bool {
	l := len(rf.log)
	lastIndex, lastTerm := l-1, rf.log[l-1].Term
	LOG(rf.me, rf.currentTerm, DVote, "Compare last log, Me: [%d]T%d, Candidate: [%d]T%d", lastIndex, lastTerm, candidateIndex, candidateTerm)
	// 当前节点最新日志条目的任期 (lastTerm) 与候选者的任期 (candidateTerm) 不同，任期大的节点日志更新
	if lastTerm != candidateTerm {
		return lastTerm > candidateTerm
	}
	// 任期相同，通过索引比较日志的新旧
	return lastIndex > candidateIndex
}

// 打印日志-要票rpc
func (args *RequestVoteArgs) String() string {
	return fmt.Sprintf("Candidate-%d, T%d, LastLogIdx: [%d]T%d", args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
}

func (reply *RequestVoteReply) String() string {
	return fmt.Sprintf("T%d, VoteGranted: %v", reply.Term, reply.VoteGranted)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (PartA, PartB).

	// 候选者任期
	Term int
	// 候选者id
	CandidateId int
	// 最后一条日志索引
	LastLogIndex int
	// 最后一条日志任期
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).

	// 处理rpc请求 节点的任期号
	Term int
	// 候选者获得选票时为true，否则为false
	VoteGranted bool
}

// example RequestVote RPC handler.
// 回调，接收方执行要票rpc请求的回调函数
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (PartA, PartB).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, rpc-VoteAsked, Args=%v", args.CandidateId, args.String())

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// rpc请求的term小于当前节点的term，拒绝这个请求
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject voted, higher term, T%d>T%d", args.CandidateId, rf.currentTerm, args.Term)
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// 检查当前节点rf是否投过票
	if rf.votedFor != -1 {
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject, Already voted for S%d", args.CandidateId, rf.votedFor)
		return
	}

	// 当前节点rf的日志比候选者candidate（要票者）的日志更新
	if rf.isMoreUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject Vote, Candidate's log less up-to-date", args.CandidateId)
		return
	}
	// 接收方节点rf 投给 要票rpc请求节点一票
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId

	// 节点 currentTerm || votedFor || log改变，都需要持久化
	rf.persistLocked()

	rf.resetElectionTimerLocked()
	LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Vote granted", args.CandidateId)
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

// 发送选举rpc请求的方法
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 开始任期term的选举，rf节点（candidate）向其余的peer节点发rpc请求，要票
func (rf *Raft) startElection(term int) {
	votes := 0

	// 定义了一个要票rpc请求的逻辑
	askVoteFromPeer := func(peer int, args *RequestVoteArgs) {
		reply := &RequestVoteReply{} // 响应结果
		// 发送要票rpc请求
		ok := rf.sendRequestVote(peer, args, reply)

		// 处理响应结果
		// 需要上锁，因为要操作一些raft的全局字段
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DDebug, "AskVote from S%d, Lost or error", peer)
			return
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, AskVote-rpc, Reply=%v", peer, reply.String())
		// 根据响应结果，判断是否要调整节点任期和角色
		if reply.Term > rf.currentTerm { // 如果响应的任期 大于 要票节点任期，要票节点变为follow；否则 响应节点投给要票节点一票
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// 上下文检查（检查当前节点还是不是发送rpc要票请求之前的角色，因为rpc请求响应的时间长，避免要票节点角色在rpc期间发生变化）
		// 在发送rpc请求和响应的这个时间段内，检查节点角色和任期是否变化（candidate才会发要票rpc请求）
		if rf.contextLostLocked(Candidate, term) {
			// rf上下文变了，不属于当前任期term，直接返回
			LOG(rf.me, rf.currentTerm, DVote, "Lost context, abort RequestVoteReply for S%d", peer)
			return
		}

		// 统计选票
		if reply.VoteGranted {
			votes++
			if votes > len(rf.peers)/2 {
				// 成为leader后，发送【心跳和日志同步】rpc
				rf.becomeLeaderLocked()
				go rf.replicationTicker(term)
			}
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 执行异步操作之前，先检查rf节点的context是否改变，变了直接return，不用再处理rpc请求了
	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote, "Lost context, from Candidate to %s, abort RequestVote", rf.role)
		return
	}

	l := len(rf.log)
	for peer := 0; peer < len(rf.peers); peer++ {
		// 是自己，先给自己投一票
		if peer == rf.me {
			votes++
			continue
		}

		// Candidate要票rpc请求参数
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: l - 1,
			LastLogTerm:  rf.log[l-1].Term,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, AskVote-rpc, Args=%v", peer, args.String())

		go askVoteFromPeer(peer, args)

	}
}

// 定期检查当前节点的状态，在需要时发起领导者选举
func (rf *Raft) electionTicker() {
	for !rf.killed() {

		// Your code here (PartA)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.role != Leader && rf.isElectionTimeoutLocked() { // 不是leader 且 选举超时了，节点成为candidate，发起选举
			rf.becomeCandidateLocked()
			go rf.startElection(rf.currentTerm)
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
