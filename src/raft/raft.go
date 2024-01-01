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
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
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

const HeartbeatInterval = 300 * time.Millisecond

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
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
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
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
	fmt.Printf("GetState end %v state: %v...\n", rf.me, rf.state)
	return term, isleader
}

func (rf *Raft) init(me int) {
	rf.currentTerm = 1
	rf.me = me
	rf.state = FOLLOWER
	rf.electionTimeout = NextElectionTimeout()
	rf.lastCommunicationTime = time.Now()
	rf.votedFor = -1
	rf.votedTerm = -1
	fmt.Printf("[Init %v] ElectionTimeout %v\n", rf.me, rf.electionTimeout)
	rf.schedule()
}

func (rf *Raft) schedule() {
	go rf.heartbeatCheck()
	go rf.startElectionCheck()
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
		if rf.killed() {
			break
		}
		rf.mu.Lock()
		if rf.state == LEADER {
			rf.broadcastHeartbeat()
		}
		rf.mu.Unlock()
		time.Sleep(HeartbeatInterval)
	}
}

func (rf *Raft) broadcastHeartbeat() {
	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
		Log:      nil,
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
		rf.currentTerm = reply.Term
		rf.votedFor = -1
	}
}

func (rf *Raft) startElectionCheck() {
	for {
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
		time.Sleep(rf.electionTimeout)
	}
}

func (rf *Raft) startElection() {
	rf.electionTimeout = NextElectionTimeout()
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
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
				rf.votedTerm = -1
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
	rf.state = LEADER
	rf.isLeader = true
	rf.broadcastHeartbeat()
}

func (rf *Raft) IsMajority(count int) bool {
	return count*2 >= len(rf.peers)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if len(args.Log) == 0 {
	}
	rf.lastCommunicationTime = time.Now()
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
	}
	reply.Success = true
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	upToDate := rf.checkUpToDate(args)
	notYetVote := rf.notYetVote(args)

	// Check whether update and grant vote
	if upToDate && notYetVote {
		reply.VoteGranted = true
		fmt.Printf("Server %v vote for %v at Term %v\n", rf.me, args.CandidateId, args.Term)
		rf.votedTerm = args.Term
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		fmt.Printf("Server %v reject vote for %v at Term %v\n", rf.me, args.CandidateId, args.Term)
	}
}

func (rf *Raft) notYetVote(args *RequestVoteArgs) bool {
	if rf.votedTerm < args.Term {
		return true
	}
	return false
}

func (rf *Raft) checkUpToDate(args *RequestVoteArgs) bool {
	upToDate := true
	if rf.currentTerm > args.Term {
		upToDate = false
	}
	return upToDate
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
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

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	fmt.Printf("Make me %v, peers count %v\n", me, len(peers))
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.init(me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
