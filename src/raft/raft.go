package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
// 通过ApplyMsg类型的通道告知Service，一条新的log entries被提交
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// Raft A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state，用于数据的持久化
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	id    uint // 当前服务器的id，最好不要和上面的me混为一谈
	state State

	cond    *sync.Cond
	applyCh chan ApplyMsg

	currentTerm uint       // 服务器最后知道的任期号
	votedFor    int        // 在当前任期内收到选票的candidate id，初始条件下设定为-1
	log         []LogEntry // 日志条目

	commitIndex int // 已知被提交的最大日志条目的索引值
	lastApplied int // 已知被状态机所执行的最大日志条目

	// 成为Leader后才需要初始化的条目，由Leader进行维护
	nextIndex  []uint // 对每个服务器而言，需要发给它的下一个日志条目的索引值
	matchIndex []uint // 对每个服务器而言，已经复制到该服务器的日志的最高索引值

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

type LogEntry struct {
	Command interface{} // 要执行的命令
	Term    uint        // 从Leader处收到的任期号
	Index   int         // 当前日志的序号
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = int(rf.currentTerm)
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any State?
		return
	}
	// Your code here (2C).
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 待优化，尽量使代码简洁高效
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.ChangeState(Follower)
		rf.votedFor = -1
	}

	rf.electionTimer.Reset(RandomizedElectionTimeout())
	reply.Term = rf.currentTerm

	DPrintf("In peer: %v, the state is %v, args.PrevLogIndex is: %v while len(rf.log) is %v", rf.me, rf.state, args.PrevLogIndex, len(rf.log))
	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	// delete conflicting entries and append new entries
	i := 0
	j := args.PrevLogIndex + 1
	for i = 0; i < len(args.Entries); i++ {
		if j >= len(rf.log) {
			break
		}
		if rf.log[j].Term == args.Entries[i].Term {
			j++
		} else {
			rf.log = append(rf.log[:j], args.Entries[i:]...)
			i = len(args.Entries)
			j = len(rf.log) - 1
			break
		}
	}
	if i < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[i:]...)
		j = len(rf.log) - 1
	} else {
		j--
	}
	reply.Success = true

	// 补充日志
	if args.LeaderCommit > rf.commitIndex {
		oriCommitIndex := rf.commitIndex
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(j)))
		if rf.commitIndex > oriCommitIndex {
			// wake up sleeping applyCommit Go routine
			rf.cond.Broadcast()
		}
	}
}

// BroadcastHeartbeat 并行向其他节点发送心跳
func (rf *Raft) BroadcastHeartbeat() {
	heartbeat := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.id,
		Entries:  make([]LogEntry, 0),
	}
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
	rf.votedFor = -1
	rf.persist()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := &AppendEntriesReply{}
			if ok := rf.sendAppendEntries(peer, heartbeat, reply); ok {
				if heartbeat.Term == rf.currentTerm && rf.state == Leader {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if !reply.Success {
						if reply.Term > rf.currentTerm {
							rf.ChangeState(Follower)
							rf.currentTerm = reply.Term
							rf.persist()
						}
					}
				}
			}
		}(peer)
	}
}

// BroadAppendEntries 并行向其他节点发送增加更新log请求
func (rf *Raft) BroadAppendEntries(command interface{}) {
	entriy := make([]LogEntry, 0)
	entriy = append(entriy, LogEntry{
		Command: command,
		Term:    rf.currentTerm,
		Index:   rf.getLastLog().Index + 1,
	})
	appendEntries := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.id,
		PrevLogIndex: rf.getLastLog().Index,
		PrevLogTerm:  rf.getLastLog().Term,
		Entries:      entriy,
		LeaderCommit: rf.commitIndex,
	}
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
	rf.persist()
	count, isSkip := 0, false
	for peer := range rf.peers {
		if peer == rf.me {
			// 补充自己的log日志
			rf.log = append(rf.log, entriy...)
		}
		go func(peer int) {
			reply := &AppendEntriesReply{}
			if ok := rf.sendAppendEntries(peer, appendEntries, reply); ok {
				if appendEntries.Term == rf.currentTerm && rf.state == Leader {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Success {
						if isSkip {
							return
						}
						if count > len(rf.peers)/2 {
							// commit the log
							rf.CommitTheLog()

							isSkip = true
							return
						}
						count++
					} else {
						if reply.Term > rf.currentTerm {
							rf.ChangeState(Follower)
							rf.currentTerm = reply.Term
							rf.persist()
						}
					}
				}
			}
		}(peer)
	}
}

// CommitTheLog Leader完成新log的提交
func (rf *Raft) CommitTheLog() {
	rf.commitIndex = rf.getLastLog().Index
}

//
// RequestVote example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != int(args.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	if args.LastLogTerm < rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term &&
		args.LastLogIndex < len(rf.log)-1) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	rf.votedFor = int(args.CandidateId)
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	reply.Term, reply.VoteGranted = rf.currentTerm, true
}

//
// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term = int(rf.currentTerm)
	if rf.state != Leader {
		isLeader = false
	} else {
		rf.BroadAppendEntries(command)
	}

	return index, term, isLeader
}

//
// Kill the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) applied() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			// to block the goroutine
			rf.cond.Wait()
		}
		rf.lastApplied++
		comIdx := rf.lastApplied
		com := rf.log[comIdx].Command
		rf.mu.Unlock()
		msg := ApplyMsg{
			CommandValid: true,
			Command:      com,
			CommandIndex: comIdx,
		}
		// here will be blocked
		rf.applyCh <- msg
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// time.Sleep(time.Duration(rand.Float64()*10) * time.Second) // 产生[0, 10]秒的随机数
		// check if a leader election should be started
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.ChangeState(Candidate)
			rf.currentTerm += 1
			rf.StartElection()
			rf.electionTimer.Reset(RandomizedElectionTimeout())
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.BroadcastHeartbeat()
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) StartElection() {
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.id,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.getLastLog().Term,
	}
	rf.votedFor = rf.me
	grantedVotes := 1
	rf.persist()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		// 开启多个goroutine并行发送请求
		go func(peer int) {
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(peer, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == args.Term && rf.state == Candidate {
					if reply.VoteGranted {
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							rf.ChangeState(Leader)
							// 发送广播
							rf.BroadcastHeartbeat()
						}
					} else if reply.Term > rf.currentTerm {
						rf.ChangeState(Follower)
						rf.currentTerm, rf.votedFor = reply.Term, -1
						rf.persist()
					}
				}
			}
		}(peer)

	}
}

//
// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent State, and also initially holds the most
// recent saved State, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		applyCh:        applyCh,
		id:             uint(me),
		currentTerm:    0,
		votedFor:       -1,
		log:            make([]LogEntry, 0),
		nextIndex:      make([]uint, len(peers)),
		matchIndex:     make([]uint, len(peers)),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
		electionTimer:  time.NewTimer(RandomizedElectionTimeout()),
	}
	rf.cond = sync.NewCond(&rf.mu)
	// init log with term 0
	rf.log = append(rf.log, LogEntry{Term: 0})

	// initialize from State persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applied()

	return rf
}
