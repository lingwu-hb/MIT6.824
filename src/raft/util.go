package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) getLastLog() LogEntry {
	return rf.log[len(rf.log)-1]
}

// 判断是否匹配
func (rf *Raft) matchLog(prevLogTerm uint, prevLogIndex uint) bool {
	if rf.log[prevLogIndex].Term == prevLogTerm {
		return true
	}
	return false
}

// RandomizedElectionTimeout generate random election timeout from [300, 400] milliseconds
func RandomizedElectionTimeout() time.Duration {
	return time.Duration((rand.Float64()+3)*100) * time.Millisecond
}

// StableHeartbeatTimeout send heartbeat per 150 milliseconds
func StableHeartbeatTimeout() time.Duration {
	return time.Duration(150) * time.Millisecond
}

func (rf *Raft) ChangeState(st State) {
	rf.state = st
}
