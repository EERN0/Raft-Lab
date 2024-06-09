package raft

// 日志应用
func (rf *Raft) applylication() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.applyCond.Wait()

		entries := make([]LogEntry, 0)
		// 需要应用的日志: [rf.lastApplied+1, ..., rf.commitIndex]
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			entries = append(entries, rf.log[i])
		}
		rf.mu.Unlock()

		for i, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: entry.CommandValid,
				Command:      entry.Command,
				// 日志条目在整个日志中的实际索引
				CommandIndex: rf.lastApplied + 1 + i,
			}
		}

		rf.mu.Lock()
		LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(entries))
		rf.lastApplied += len(entries)
		rf.mu.Unlock()
	}

}
