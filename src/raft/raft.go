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
	"fmt"
	"math/rand"
	"sort"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type ServerState int

const (
	LEADER    ServerState = 1
	CANDIDATE ServerState = 2
	FOLLOWER  ServerState = 3
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

const HeartbeatInterval = 300 * time.Millisecond

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	votedTerm   int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leader
	nextIndex  []int
	matchIndex []int

	isLeader bool
	state    ServerState

	lastCommunicationTime time.Time
	electionTimeout       time.Duration

	applyChan chan ApplyMsg

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Log          []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	//fmt.Printf("GetState end %v state: %v...\n", rf.me, rf.state)
	return term, isleader
}

func (rf *Raft) init(me int) {
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.log = make([]LogEntry, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastLog().Index + 1
		rf.matchIndex[i] = 0
	}
	rf.me = me
	rf.state = FOLLOWER
	rf.electionTimeout = NextElectionTimeout()
	rf.lastCommunicationTime = time.Now()
	rf.votedFor = -1
	rf.votedTerm = -1
	rf.schedule()
}

func (rf *Raft) lastLog() *LogEntry {
	return &rf.log[len(rf.log)-1]
}

func (rf *Raft) isSameLog(log1 *LogEntry, log2 *LogEntry) bool {
	return log1.Command == log2.Command && log1.Index == log2.Index && log1.Term == log2.Term
}

func (rf *Raft) schedule() {
	go rf.heartbeatCheck()
	go rf.startElectionCheck()
	go rf.applyLog()
	go rf.updateCommitted()
}

func NextElectionTimeout() time.Duration {
	interval := rand.Uint64()%201 + 500
	return time.Duration(interval) * time.Millisecond
}

func (rf *Raft) debugSchedule() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//DebugRaftServer(rf)
}

func (rf *Raft) heartbeatCheck() {
	for {
		time.Sleep(HeartbeatInterval)
		if rf.killed() {
			break
		}
		rf.mu.Lock()
		if rf.state == LEADER {
			rf.broadcastHeartbeat()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) broadcastHeartbeat() {
	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
		Log:      nil,
		//LeaderCommit: rf.commitIndex,
		//PrevLogIndex: rf.lastLog().Index,
		//PrevLogTerm:  rf.lastLog().Term,
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.maintainAuthority(i, args)
	}
	rf.mu.Lock()
}

func (rf *Raft) maintainAuthority(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm != args.Term {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.state = FOLLOWER
		//fmt.Printf("[maintain %v] term upd: %v -> %v\n", rf.me, rf.currentTerm, reply.Term)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
	}
}

func (rf *Raft) startElectionCheck() {
	for {
		time.Sleep(rf.electionTimeout)
		if rf.killed() {
			break
		}
		rf.mu.Lock()
		if rf.state == LEADER {
			rf.mu.Unlock()
			continue
		}
		if time.Since(rf.lastCommunicationTime) > rf.electionTimeout {
			rf.startElection()
			rf.mu.Lock()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() {
	rf.electionTimeout = NextElectionTimeout()
	rf.state = CANDIDATE
	rf.currentTerm++
	//fmt.Printf("[Start election %v] try at term %v\n", rf.me, rf.currentTerm)
	rf.votedFor = rf.me
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLog().Index,
		LastLogTerm:  rf.lastLog().Term,
	}
	rf.mu.Unlock()

	vote := 1
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(index int, args *RequestVoteArgs) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(index, args, reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state != CANDIDATE || rf.currentTerm != args.Term {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.state = FOLLOWER
				rf.currentTerm = reply.Term
				//rf.votedTerm = -1
				rf.votedFor = -1
			}
			if reply.VoteGranted {
				vote++
				if rf.IsMajority(vote) {
					rf.winElection()
				}
			}
		}(index, args)
	}
}

func (rf *Raft) winElection() {
	//fmt.Printf("[Leader %v Win] term: %v\n", rf.me, rf.currentTerm)
	rf.state = LEADER
	rf.isLeader = true
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.lastLog().Index + 1
	}
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = rf.lastLog().Index
	//rf.broadcastHeartbeat()
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.lastLog().Index,
		PrevLogTerm:  rf.lastLog().Term,
		Log:          nil,
		LeaderCommit: rf.commitIndex,
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.maintainAuthority(i, args)
	}
}

func (rf *Raft) IsMajority(count int) bool {
	return count*2 >= len(rf.peers)
}

func (rf *Raft) sync() {
	//for {
	//	time.Sleep(100 * time.Millisecond)
	//	rf.mu.Lock()
	//	if rf.killed() {
	//		rf.mu.Unlock()
	//		break
	//	}
	//	if rf.state != LEADER {
	//		rf.mu.Unlock()
	//		continue
	//	}
	//	rf.mu.Unlock()
	//	//fmt.Printf("[Sync %v] Loop\n", rf.me)
	//	for i := range rf.peers {
	//		if i == rf.me {
	//			continue
	//		}
	//		go rf.replicate(i)
	//	}
	//	//rf.mu.Unlock()
	//}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.replicate(i)
	}
}

//func (rf *Raft) replicate(server int) {
//	//fmt.Printf("[%v Replicate to server %v] func begin\n", rf.me, server)
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	fmt.Printf("[%v Replicate to server %v] next index: %v, last index %v\n", rf.me, server, rf.nextIndex[server], rf.lastLog().Index)
//	if rf.nextIndex[server] > rf.lastLog().Index {
//		return
//	}
//	for i := rf.nextIndex[server]; i >= 1; i-- {
//		fmt.Printf("[Replicate for loop] i: %v\n", i)
//		args := &AppendEntriesArgs{
//			Term:         rf.currentTerm,
//			LeaderId:     rf.me,
//			PrevLogIndex: rf.log[i-1].Index,
//			PrevLogTerm:  rf.log[i-1].Term,
//			Log:          rf.log[i:len(rf.log)],
//			LeaderCommit: rf.commitIndex,
//		}
//		reply := &AppendEntriesReply{
//			Term:    0,
//			Success: false,
//		}
//		fmt.Printf("[Replicate for loop %v] Begin to send AppendEntries\n", i)
//		ok := rf.sendAppendEntries(server, args, reply)
//		fmt.Printf("[Replicate for loop %v] Finish send AppendEntries, ok: %v, suc: %v\n", i, ok, reply.Success)
//		if !ok {
//			break
//		}
//		if reply.Success {
//			rf.nextIndex[server] = rf.lastLog().Index + 1
//			rf.matchIndex[server] = rf.lastLog().Index
//			fmt.Printf("[%v Replicate to server %v] reply success next index: %v, match index %v\n", rf.me, server, rf.nextIndex[server], rf.matchIndex[server])
//			break
//		}
//	}
//}

func (rf *Raft) replicate(server int) {
	for {
		time.Sleep(100 * time.Millisecond)
		//fmt.Printf("[replicate %v] try lock\n", rf.me)
		rf.mu.Lock()
		//fmt.Printf("[replicate %v] lock\n", rf.me)
		if rf.state != LEADER {
			rf.mu.Unlock()
			continue
		}
		//fmt.Printf("[%v replicate to %v] next index: %v, last index: %v\n", rf.me, server, rf.nextIndex[server], rf.lastLog().Index)
		if rf.nextIndex[server] > rf.lastLog().Index {
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.lastLog().Index,
				PrevLogTerm:  rf.lastLog().Term,
				Log:          nil,
				LeaderCommit: rf.commitIndex,
			}
			reply := &AppendEntriesReply{
				Term:    0,
				Success: false,
			}
			rf.mu.Unlock()
			ok := rf.sendAppendEntries(server, args, reply)
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.state = FOLLOWER
				rf.mu.Unlock()
				continue
			}
			if ok && !reply.Success {
				rf.nextIndex[server]--
			}
			rf.mu.Unlock()
			continue
		}
		//suc := -1
		for i := rf.nextIndex[server]; i >= rf.matchIndex[server]+1; i-- {
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.log[i-1].Index,
				PrevLogTerm:  rf.log[i-1].Term,
				Log:          rf.log[i:len(rf.log)],
				LeaderCommit: rf.commitIndex,
			}
			reply := &AppendEntriesReply{
				Term:    0,
				Success: false,
			}
			rf.mu.Unlock()
			//fmt.Printf("[Send Append RPC to %v] len count: %v\n", server, len(args.Log))
			ok := rf.sendAppendEntries(server, args, reply)
			rf.mu.Lock()
			if !ok {
				break
			}
			if reply.Term > rf.currentTerm {
				rf.state = FOLLOWER
				break
			}
			if reply.Success {
				//fmt.Printf("[Send Append RPC to %v] success with len count: %v\n", server, len(args.Log))
				rf.nextIndex[server] = rf.lastLog().Index + 1
				rf.matchIndex[server] = rf.lastLog().Index
				//rf.updateCommitIndex()
				//res := make([]int, len(rf.peers))
				////copy(res, rf.matchIndex)
				//for k := range res {
				//	res[k] = rf.matchIndex[k]
				//}
				//sort.Ints(res)
				//rf.commitIndex = max(rf.commitIndex, res[len(rf.peers)/2])

				break
				//fmt.Printf("[%v replicate to %v success] next index %v, success: %v. res: %v\n", rf.me, server, rf.nextIndex[server], rf.lastLog().Index, res)
			}
			//rf.mu.Lock()
			//go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
			//	rf.mu.Lock()
			//	if suc >= args.PrevLogIndex {
			//		rf.mu.Unlock()
			//		return
			//	}
			//	rf.mu.Unlock()
			//	fmt.Printf("[Send Append RPC to %v] len count: %v\n", server, len(args.Log))
			//	ok := rf.sendAppendEntries(server, args, reply)
			//	if ok && reply.Success {
			//		rf.mu.Lock()
			//		defer rf.mu.Unlock()
			//		if suc >= args.PrevLogIndex {
			//			return
			//		}
			//		suc = args.PrevLogIndex
			//		rf.nextIndex[server] = rf.lastLog().Index + 1
			//		rf.matchIndex[server] = rf.lastLog().Index
			//		res := make([]int, len(rf.peers))
			//		//copy(res, rf.matchIndex)
			//		for k := range res {
			//			res[k] = rf.matchIndex[k]
			//		}
			//		sort.Ints(res)
			//		rf.commitIndex = max(rf.commitIndex, res[len(rf.peers)/2])
			//		//fmt.Printf("[%v replicate to %v success] next index %v, success: %v. res: %v\n", rf.me, server, rf.nextIndex[server], rf.lastLog().Index, res)
			//	}
			//}(server, args, reply)
		}
		//fmt.Printf("[replicate %v] unlock\n", rf.me)
		rf.mu.Unlock()
	}
}

func (rf *Raft) updateCommitIndex() {
	res := make([]int, 0)
	for i := range rf.peers {
		index := rf.matchIndex[i]
		if rf.log[index].Term == rf.currentTerm {
			res = append(res, index)
		}
	}
	if rf.IsMajority(len(res)) {
		//sort.Ints(res)
		sort.SliceIsSorted(res, func(i, j int) bool {
			return res[i] > res[j]
		})
		//fmt.Printf("[Sorted] %v\n", res)
		rf.commitIndex = max(rf.commitIndex, res[len(rf.peers)/2])
	}
}

func (rf *Raft) applyLog() {
	for {
		time.Sleep(100 * time.Millisecond)
		if rf.killed() {
			break
		}
		//fmt.Printf("[applyLog %v] try lock\n", rf.me)
		rf.mu.Lock()
		//fmt.Printf("[applyLog %v Loop (%v)] last applied: %v, commit index: %v\n", rf.me, rf.state == LEADER, rf.lastApplied, rf.commitIndex)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			//fmt.Printf("[applyLog %v] index: %v, cmd: %v\n", rf.me, rf.log[i].Index, rf.log[i].Command)
			str := fmt.Sprintf("%v", rf.log[i].Command)
			if len(str) > 10 {
				str = str[:10]
			}
			//fmt.Printf("[applyLog %v (%v)] index: %v, cmd: %v\n", rf.me, rf.state == LEADER, rf.log[i].Index, str)
			rf.applyChan <- ApplyMsg{
				true, rf.log[i].Command, rf.log[i].Index,
			}
		}
		rf.lastApplied = rf.commitIndex
		//fmt.Printf("[applyLog %v] unlock\n", rf.me)
		rf.mu.Unlock()
	}
}

func (rf *Raft) updateCommitted() {
	for {
		time.Sleep(100 * time.Millisecond)
		//fmt.Printf("[update commit %v] loop begin\n", rf.me)
		if rf.killed() {
			//fmt.Printf("[update commit %v] kill break\n", rf.me)
			break
		}
		//fmt.Printf("[update commit %v] try lock\n", rf.me)
		rf.mu.Lock()
		//fmt.Printf("[update commit %v] locked\n", rf.me)
		if rf.state != LEADER {
			//fmt.Printf("[update commit %v] not leader continue\n", rf.me)
			rf.mu.Unlock()
			continue
		}
		//fmt.Printf("[update commit %v] res sort init\n", rf.me)
		//res := make([]int, len(rf.peers))
		//fmt.Printf("[update commit %v] res sort copy\n", rf.me)
		//copy(res, rf.matchIndex)
		//fmt.Printf("[update commit %v] copied matchIndex: %v\n", rf.me, res)
		//sort.Ints(res)
		res := make([]int, 0)
		for i := range rf.peers {
			index := rf.matchIndex[i]
			if rf.log[index].Term == rf.currentTerm {
				res = append(res, index)
			}
		}
		if rf.IsMajority(len(res)) {
			//sort.Ints(res)
			sort.Slice(res, func(i, j int) bool {
				return res[i] > res[j]
			})
			//fmt.Printf("[Sorted] %v\n", res)
			rf.commitIndex = max(rf.commitIndex, res[len(rf.peers)/2])
		}
		//fmt.Printf("[update commit %v] commitIndex: %v\n", rf.me, rf.commitIndex)
		//fmt.Printf("[update commit %v] unlock\n", rf.me)
		rf.mu.Unlock()
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastCommunicationTime = time.Now()
	if args.Term >= rf.currentTerm {
		//fmt.Printf("[Append Entries %v] update term: %v -> %v\n", rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
	}
	reply.Term = rf.currentTerm
	if rf.state == FOLLOWER {
		//fmt.Printf("[Append Entries %v] args len log %v\n", rf.me, len(args.Log))
		//fmt.Printf("[Append Entries %v] prev index: %v, last index: %v\n", rf.me, args.PrevLogIndex, rf.lastLog().Index)
		if args.PrevLogIndex > rf.lastLog().Index {
			reply.Success = false
			return
		}
		if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
			reply.Success = false
			rf.log = rf.log[:args.PrevLogIndex]
			return
		}
		i := 0
		for ; i < len(args.Log); i++ {
			if args.Log[i].Index > rf.lastLog().Index {
				break
			}
			//if args.Log[i].Command != rf.log[args.Log[i].Index].Command {
			//	rf.log[args.Log[i].Index].Command = args.Log[i].Command
			//}
			if !rf.isSameLog(&args.Log[i], &rf.log[args.Log[i].Index]) {
				rf.deleteAfterIndex(args.Log[i].Index)
				break
			}
		}
		for ; i < len(args.Log); i++ {
			//args.Log[i].Index =
			//args.Log[i].Term = rf.currentTerm
			rf.log = append(rf.log, args.Log[i])
		}
		//fmt.Printf("[Append Entries %v] after append len: %v\n", rf.me, len(rf.log))
	}
	//fmt.Printf("[Append Entries %v] Before if -> leaderCommit %v, commitIndex %v, len(log) %v\n", rf.me, args.LeaderCommit, rf.commitIndex, len(rf.log))
	if args.LeaderCommit > rf.commitIndex {
		//fmt.Printf("[Append Entries %v] Update on leaderCommit %v -> commitIndex %v\n", rf.me, args.LeaderCommit, rf.commitIndex)
		rf.commitIndex = min(args.LeaderCommit, rf.lastLog().Index)
	}
	//fmt.Printf("[Append Entries %v] Update last apply %v, cidx %v\n", rf.me, rf.lastApplied, rf.commitIndex)
	//for rf.lastApplied < rf.commitIndex {
	//	rf.lastApplied++
	//	rf.applyChan <- ApplyMsg{
	//		CommandValid: true,
	//		Command:      rf.log[rf.lastApplied].Command,
	//		CommandIndex: rf.log[rf.lastApplied].Index,
	//	}
	//}
	reply.Success = true
}

func (rf *Raft) deleteAfterIndex(index int) {
	rf.log = rf.log[:index]
	//fmt.Printf("[del after %v] index: %v\n", rf.me, index)
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		//fmt.Printf("[RequestVote %v] Turn into follower by %v, Term: %v -> %v\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		//rf.votedFor = args.CandidateId
		//reply.VoteGranted = true
		//return
	}

	upToDate := rf.checkUpToDate(args)
	notYetVote := rf.notYetVote(args)

	// Check whether update and grant vote
	if upToDate && notYetVote {
		reply.VoteGranted = true
		//fmt.Printf("[RequestVote] Server %v vote for %v at Term %v\n", rf.me, args.CandidateId, args.Term)
		//rf.votedTerm = args.Term
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		//fmt.Printf("[RequestVote] Server %v reject vote for %v at Term %v\n", rf.me, args.CandidateId, args.Term)
	}
}

func (rf *Raft) notYetVote(args *RequestVoteArgs) bool {
	if rf.votedFor == -1 {
		return true
	}
	return false
}

func (rf *Raft) checkUpToDate(args *RequestVoteArgs) bool {
	if rf.lastLog().Term < args.LastLogTerm || (rf.lastLog().Term == args.LastLogTerm && args.LastLogIndex >= rf.lastLog().Index) {
		return true
	}
	return false
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		return -1, -1, false
	}
	index := len(rf.log)
	log := LogEntry{
		Term:    rf.currentTerm,
		Index:   index,
		Command: command,
	}
	rf.log = append(rf.log, log)
	rf.matchIndex[rf.me] = index
	//go rf.broadcastAppendEntries(log)
	cmd := fmt.Sprintf("%v", command)
	if len(cmd) > 10 {
		cmd = cmd[:10]
	}
	//fmt.Printf("[Server %v Start] Command: %v, index: %v, term: %v, isLeader: %v\n", rf.me, cmd, index, rf.currentTerm, rf.state == LEADER)
	return index, rf.currentTerm, true
}

func (rf *Raft) broadcastAppendEntries(log LogEntry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	count := 1
	index := log.Index
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: 0,
			Log:          []LogEntry{log},
			LeaderCommit: rf.commitIndex,
		}
		reply := &AppendEntriesReply{
			Term:    -1,
			Success: false,
		}
		ok := rf.sendAppendEntries(i, args, reply)
		if ok && reply.Success {
			count++
		}
	}
	if rf.IsMajority(count) {
		//rf.ApplyLog(rf.log[index])
		rf.commitIndex = index
		//fmt.Printf("[IsMajority Server %v] index: %v, cmd: %v\n", rf.me, rf.commitIndex, rf.log[rf.commitIndex].Command)
	}
}

func (rf *Raft) ApplyLog(entry LogEntry) {
	//fmt.Printf("[ApplyLog Server %v] term %v, index %v, cmd %v\n", rf.me, entry.Term, entry.Index, entry.Command)
	rf.applyChan <- ApplyMsg{
		CommandValid: true,
		Command:      entry.Command,
		CommandIndex: entry.Index,
	}
	rf.lastApplied = entry.Index
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
	//fmt.Printf("Make me %v, peers count %v\n", me, len(peers))
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyChan = applyCh
	rf.sync()

	// Your initialization code here (2A, 2B, 2C).
	rf.init(me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
