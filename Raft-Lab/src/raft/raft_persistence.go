package raft

import (
	"Raft-Lab/labgob"
	"bytes"
	"fmt"
)

func (rf *Raft) persistString() string {
	return fmt.Sprintf("T%d, VotedFor: %d, Log: [0: %d]", rf.currentTerm, rf.votedFor, rf.log.size()-1)
}

// 持久化保存raft的状态，可以在崩溃后重新启动并恢复状态
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persistLocked() {
	// Your code here (PartC).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	rf.log.persistLocked(e)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.log.snapshot)
	LOG(rf.me, rf.currentTerm, DPersist, "Persist: %v", rf.persistString())
}

// 反序列化，恢复先前持久化的状态
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (PartC).
	var currentTerm int
	var votedFor int

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&currentTerm); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read currentTerm error: %v", err)
		return
	}
	rf.currentTerm = currentTerm

	if err := d.Decode(&votedFor); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read votedFor error: %v", err)
		return
	}
	rf.votedFor = votedFor

	if err := rf.log.readPersistLocked(d); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read log error: %v", err)
		return
	}
	// 恢复持久化状态
	rf.log.snapshot = rf.persister.ReadSnapshot()

	// 日志提交后才能生成快照，若snapLastLogIdx > commitIndex，推高已提交和应用的日志索引
	if rf.log.snapLastLogIdx > rf.commitIndex {
		rf.commitIndex = rf.log.snapLastLogIdx
		rf.lastApplied = rf.log.snapLastLogIdx
	}
	LOG(rf.me, rf.currentTerm, DPersist, "Read from persist: %v", rf.persistString())
}
