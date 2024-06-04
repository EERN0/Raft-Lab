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
	"sync"
	"sync/atomic"
	"time"

	"Raft-Lab/labrpc"
)

// 选举超时时间的上下界
const (
	electionTimeoutMin time.Duration = 250 * time.Millisecond
	electionTimeoutMax time.Duration = 400 * time.Millisecond

	// 发送心跳/日志同步的时间间隔，小于选举超时下界
	replicateInterval time.Duration = 200 * time.Millisecond
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

type Role string

const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part PartD you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For PartD:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (PartA, PartB, PartC).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role        Role
	currentTerm int // 任期
	votedFor    int // -1: 在currentTerm任期没有投任何人

	electionStart   time.Time     // 选举起始时间
	electionTimeout time.Duration // 选举随机超时时间
}

// 节点角色转换
// 其它节点rpc请求中任期更大，当前节点角色直接变为follower
func (rf *Raft) becomeFollowerLocked(term int) {
	// rpc请求中的任期 小于 当前raft节点任期，不用变成follower
	if term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DError, "Can't become Follower, lower term: T%d", term)
	}

	LOG(rf.me, rf.currentTerm, DLog, "%s->Follower, For T%v->T%v", rf.role, rf.currentTerm, term)
	rf.role = Follower
	if term > rf.currentTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = term
}

func (rf *Raft) becomeCandidateLocked() {
	if rf.role == Leader {
		LOG(rf.me, rf.currentTerm, DError, "Leader can't become Candidate")
		return
	}

	LOG(rf.me, rf.currentTerm, DVote, "%s->Candidate, For T%d", rf.role, rf.currentTerm+1)
	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me
}

func (rf *Raft) becomeLeaderLocked() {
	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DError, "Only Candidate can become Leader")
		return
	}

	LOG(rf.me, rf.currentTerm, DLeader, "Become Leader in T%d", rf.currentTerm)
	rf.role = Leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (PartA).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (PartC).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (PartC).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (PartA, PartB).

	// 候选者任期
	Term int
	// 候选者id
	CandidateId int
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

	reply.Term = rf.currentTerm

	// rpc请求的term小于当前节点的term，拒绝这个请求
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject voted, higher term, T%d>T%d", args.CandidateId, rf.currentTerm, args.Term)
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// 检查当前节点rf是否投过票
	if rf.votedFor != -1 {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject, Already voted S%d", args.CandidateId, rf.votedFor)
		reply.VoteGranted = false
		return
	}

	// 响应节点 投给 要票rpc请求节点一票
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.resetElectionTimerLocked()
	LOG(rf.me, rf.currentTerm, DVote, "-> S%d", args.CandidateId)
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

	// Your code here (PartB).

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
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 节点上下文是否改变（角色、任期） true-改变，false-没改变
func (rf *Raft) contextLostLocked(role Role, term int) bool {
	return !(role == rf.role && term == rf.currentTerm)
}

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
			LOG(rf.me, rf.currentTerm, DDebug, "Ask vote from S%d, Lost or error", peer)
			return
		}
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
		LOG(rf.me, rf.currentTerm, DVote, "Lost Candidate to %s, abort RequestVote", rf.role)
		return
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		// 是自己，先给自己投一票
		if peer == rf.me {
			votes++
			continue
		}

		args := &RequestVoteArgs{ // 要票rpc请求参数
			Term:        rf.currentTerm,
			CandidateId: rf.me,
		}

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

	// Your initialization code here (PartA, PartB, PartC).
	// PartA
	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()

	return rf
}
