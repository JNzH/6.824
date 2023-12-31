package raft

import (
	"log"
	"sync"
)

// Debugging
const Debug = 1

var mu = sync.Mutex{}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func stateToString(state ServerState) string {
	if state == FOLLOWER {
		return "Follower"
	} else if state == CANDIDATE {
		return "Candidate"
	} else if state == LEADER {
		return "Leader"
	}
	return ""
}

func DebugRaftServer(rf *Raft) {
	mu.Lock()
	defer mu.Unlock()
	DPrintf("==========[ Raft peer %v ]==========\n", rf.me)
	DPrintf("Dead: %v\n", rf.dead)
	DPrintf("CurrentTerm: %v\n", rf.currentTerm)
	DPrintf("VotedFor: %v\n", rf.votedFor)
	DPrintf("VotedTerm: %v\n", rf.votedTerm)
	DPrintf("ElectionTimeout: %v\n", rf.electionTimeout)
	DPrintf("State: %v\n", stateToString(rf.state))
	DPrintf("LastCommunicationTime: %v\n", rf.lastCommunicationTime)
	DPrintf("==============================\n")
}

func DebugRequestVoteArgs(args *RequestVoteArgs, send int, recv int) {
	mu.Lock()
	defer mu.Unlock()
	DPrintf("==========[ RequestVoteArgs (%v to %v) ]==========\n", send, recv)
	DPrintf("Term: %v\n", args.Term)
	DPrintf("CandidateId: %v\n", args.CandidateId)
	DPrintf("LastLogIndex: %v\n", args.LastLogIndex)
	DPrintf("LastLogIndex: %v\n", args.LastLogIndex)
	DPrintf("==============================\n")
}

func DebugRequestVoteReply(args *RequestVoteReply, send int, recv int) {
	mu.Lock()
	defer mu.Unlock()
	DPrintf("==========[ RequestVoteArgs (%v to %v) ]==========\n", send, recv)
	DPrintf("Term: %v\n", args.Term)
	DPrintf("VoteGranted: %v\n", args.VoteGranted)
	DPrintf("==============================\n")
}
