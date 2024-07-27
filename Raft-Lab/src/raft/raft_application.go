package raft

// 日志应用
func (rf *Raft) application() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.applyCond.Wait()
		entries := make([]LogEntry, 0)
		snapPendingApply := rf.snapPending

		if !snapPendingApply {
			// 需要应用的日志: [rf.lastApplied+1, ..., rf.commitIndex]
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				entries = append(entries, rf.log.at(i))
			}
		}
		rf.mu.Unlock()

		// 阻塞的操作，不放到临界区(不加锁)
		if !snapPendingApply {
			// 不用做snapshot，执行日志应用
			for i, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: entry.CommandValid,
					Command:      entry.Command,
					// 日志条目在整个日志中的实际索引
					CommandIndex: rf.lastApplied + 1 + i,
				}
			}
		} else {
			// 执行快照
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.log.snapshot,
				SnapshotIndex: rf.log.snapLastLogIdx,
				SnapshotTerm:  rf.log.snapLastLogTerm,
			}
		}

		rf.mu.Lock()
		if !snapPendingApply {
			// 日志应用
			LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(entries))
			rf.lastApplied += len(entries)
		} else {
			// 日志快照
			LOG(rf.me, rf.currentTerm, DApply, "Apply snapshot for [0, %d]", 0, rf.log.snapLastLogIdx)
			rf.lastApplied = rf.log.snapLastLogIdx
			// 提交之后才能应用，得满足lastApplied <= commitIndex
			if rf.commitIndex < rf.lastApplied {
				rf.commitIndex = rf.lastApplied
			}
			// 快照标记置为false
			rf.snapPending = false
		}
		rf.mu.Unlock()
	}

}
