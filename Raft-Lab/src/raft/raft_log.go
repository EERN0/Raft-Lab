package raft

import (
	"Raft-Lab/labgob"
	"fmt"
)

type RaftLog struct {
	// 日志快照的最后一条日志
	snapLastLogIdx  int
	snapLastLogTerm int

	// 完整日志 = 快照snapshot + 尾部日志tailLog

	// 1.快照日志（日志前半段），下标范围[1, snapLastIdx]
	snapshot []byte
	// 2.日志后半段（尾部日志），下标范围[snapLastIdx+1, snapLastIdx+len(tailLog)-1], 含有一个虚拟头节点mock log entry
	tailLog []LogEntry
}

// 构造RaftLog实例，初始化快照信息和日志条目
func NewLog(snapLastIdx, snapLastTerm int, snapshot []byte, entries []LogEntry) *RaftLog {
	rl := &RaftLog{
		snapLastLogIdx:  snapLastIdx,
		snapLastLogTerm: snapLastTerm,
		snapshot:        snapshot,
	}

	// 用snapshot的最后一条日志mock掉tailLog的第一个日志项
	rl.tailLog = append(rl.tailLog, LogEntry{
		Term: snapLastTerm,
	})
	rl.tailLog = append(rl.tailLog, entries...)

	return rl
}

// 下面所有的函数都得在加锁的情况下调用
// 反序列化，读取持久化日志的状态
func (rl *RaftLog) readPersistLocked(d *labgob.LabDecoder) error {
	var lastIdx int
	if err := d.Decode(&lastIdx); err != nil {
		return fmt.Errorf("decode last include index failed")
	}
	rl.snapLastLogIdx = lastIdx

	var lastTerm int
	if err := d.Decode(&lastTerm); err != nil {
		return fmt.Errorf("decode last include term failed")
	}
	rl.snapLastLogTerm = lastTerm

	var log []LogEntry
	if err := d.Decode(&log); err != nil {
		return fmt.Errorf("decode tail log failed")
	}
	rl.tailLog = log

	return nil
}

// 序列化，日志持久化
func (rl *RaftLog) persistLocked(e *labgob.LabEncoder) {
	e.Encode(rl.snapLastLogIdx)
	e.Encode(rl.snapLastLogTerm)
	e.Encode(rl.tailLog)
}

func (rl *RaftLog) size() int {
	return rl.snapLastLogIdx + len(rl.tailLog)
}

// 索引转换，把全局日志（快照+尾部日志）下标换为尾部日志tailLog的下标
func (rl *RaftLog) idx(logicIdx int) int {
	if logicIdx < rl.snapLastLogIdx || logicIdx >= rl.size() {
		panic(fmt.Sprintf("%d is out of [%d, %d]", logicIdx, rl.snapLastLogIdx, rl.size()-1))
	}
	return logicIdx - rl.snapLastLogIdx
}

func (rl *RaftLog) at(logicIdx int) LogEntry {
	return rl.tailLog[rl.idx(logicIdx)]
}

// 找到第一条任期为term的尾部日志
func (rl *RaftLog) firstLogIndexFor(term int) int {
	for idx, entry := range rl.tailLog {
		if entry.Term == term {
			return idx + rl.snapLastLogIdx
		} else if entry.Term > term {
			break
		}
	}
	return InvalidIndex
}

// 追加日志
func (rl *RaftLog) appendLog(e LogEntry) {
	rl.tailLog = append(rl.tailLog, e)
}

// 将日志条目按term分段，用于leader和follower发送日志冲突时打印日志
func (rl *RaftLog) logString() string {
	var terms string
	prevTerm := rl.snapLastLogTerm
	prevStart := rl.snapLastLogIdx
	for i := 0; i < len(rl.tailLog); i++ {
		if rl.tailLog[i].Term != prevTerm {
			terms += fmt.Sprintf(" [%d, %d]T%d", prevStart, rl.snapLastLogIdx+i-1, prevTerm)
			prevTerm = rl.tailLog[i].Term
			prevStart = i
		}
	}
	terms += fmt.Sprintf(" [%d, %d]T%d", prevStart, rl.snapLastLogIdx+len(rl.tailLog)-1, prevTerm)
	return terms
}

func (rl *RaftLog) doSnapshot(index int, snapshot []byte) {
	// 全局索引index转成tailLg索引
	idx := rl.idx(index)

	rl.snapLastLogIdx = index
	rl.snapLastLogTerm = rl.tailLog[idx].Term
	rl.snapshot = snapshot

	newLog := make([]LogEntry, 0, rl.size()-rl.snapLastLogIdx)
	newLog = append(newLog, LogEntry{
		Term: rl.snapLastLogTerm,
	})
	newLog = append(newLog, rl.tailLog[idx+1:]...)
	rl.tailLog = newLog
}
