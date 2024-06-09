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
	"sync"
	"sync/atomic"
	"time"

	"Raft-Lab/labrpc"
)

// 选举超时时间的上下界、发送心跳/日志的时间间隔
const (
	electionTimeoutMin time.Duration = 250 * time.Millisecond
	electionTimeoutMax time.Duration = 400 * time.Millisecond

	// 发送心跳/日志同步的时间间隔，小于选举超时下界
	replicateInterval time.Duration = 100 * time.Millisecond
)

type Role string

const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

const (
	InvalidIndex int = 0
	InvalidTerm  int = 0
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

	log []LogEntry // 每个节点本地的日志。日志索引相同+日志任期相同 ==> 日志相同（从日志开头到索引位置的日志都相同）

	// 仅在leader中使用，表示每个peer节点的日志视图
	nextIndex  []int // leader尝试下一次从索引nextIndex[peer]给peer发送日志复制请求（包含一个或多个日志条目）
	matchIndex []int // 每个节点已经复制的最高日志项的索引

	commitIndex int           // 全局已提交日志的索引
	lastApplied int           // 已经应用的日志索引
	applyCond   *sync.Cond    // 通知日志条目应用的条件变量。新提交日志后，commitIndex > lastApplied，触发日志应用
	applyCh     chan ApplyMsg // 管道，传递要应用到状态机的日志条目

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
	// 节点 currentTerm || votedFor || log改变，都需要持久化
	needPersist := rf.currentTerm != term
	if term > rf.currentTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = term

	if needPersist {
		rf.persistLocked()
	}
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
	// 节点 currentTerm || votedFor || log改变，都需要持久化
	rf.persistLocked()
}

func (rf *Raft) becomeLeaderLocked() {
	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DError, "Only Candidate can become Leader")
		return
	}

	LOG(rf.me, rf.currentTerm, DLeader, "Become Leader in T%d", rf.currentTerm)
	rf.role = Leader

	// rf当选leader，初始化leader节点中维护的peers日志视图
	for peer := 0; peer < len(rf.peers); peer++ {
		rf.nextIndex[peer] = len(rf.log)
		rf.matchIndex[peer] = 0
	}
}

// 找到第一条任期为term的日志
func (rf *Raft) firstLogIndexFor(term int) int {
	for idx, entry := range rf.log {
		if entry.Term == term {
			return idx
		} else if entry.Term > term {
			break
		}
	}
	return InvalidIndex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (PartA).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.

// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// 返回值: 新命令在日志中的索引位置, 当前任期, 是否为Leader
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 只有Leader节点能够接收来自客户端的请求并将其作为日志条目追加到日志中
	// 一旦客户端的请求被附加到了Leader节点的日志中，Leader节点会将这些请求复制给其他节点，并在多数节点确认后提交这些请求

	// 非Leader，无法接受客户端的请求
	if rf.role != Leader {
		return 0, 0, false
	}
	// 是Leader，将命令附加到节点的日志中
	rf.log = append(rf.log, LogEntry{
		CommandValid: true,
		Command:      command,
		Term:         rf.currentTerm,
	})
	LOG(rf.me, rf.currentTerm, DLeader, "Leader accept log [%d]T%d", len(rf.log)-1, rf.currentTerm)
	// 节点 currentTerm || votedFor || log改变，都需要持久化
	rf.persistLocked()

	return len(rf.log) - 1, rf.currentTerm, true
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
	rf.currentTerm = 1 // 空出0，0表示InvalidTerm
	rf.votedFor = -1

	// 空日志，类似链表虚拟头节点，减少边界判断
	rf.log = append(rf.log, LogEntry{Term: InvalidTerm})

	// 初始化leader's view，有多少个peer就有多少个view
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// 初始化日志应用字段
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()
	// 日志应用
	go rf.applylication()

	return rf
}
