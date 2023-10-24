package raft

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) startElectionL() {
	rf.resetElectionTimer()
	rf.currentTerm += 1
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.persist()
	DPrintf("[%d]: term:%v start election", rf.me, rf.currentTerm)

	rf.requestVotesL()
}

func (rf *Raft) requestVotesL() {
	args := &RequestVoteArgs{rf.currentTerm, rf.me,
		rf.lastLogIndex(), rf.lastLogTerm()}
	voteCounter := 1
	for serverId, _ := range rf.peers {
		if serverId != rf.me {
			go rf.requestVote(serverId, args, &voteCounter)
		}
	}
}

func (rf *Raft) requestVote(serverId int, args *RequestVoteArgs, voteCounter *int) {
	DPrintf("[%d]: term %v send vote request to %d\n", rf.me, args.Term, serverId)
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serverId, args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm{
			rf.setNewTerm(reply.Term)
			return 
		}

		if rf.state != Candidate{
			return 
		}
	
		if reply.VoteGranted {
			DPrintf("[%d]: term:%v received vote from %v", rf.me, rf.currentTerm, serverId)

			*voteCounter++
			if *voteCounter > len(rf.peers)/2 &&
				rf.currentTerm == args.Term {
				rf.state = Leader
				for i, _ := range rf.peers {
					rf.nextIndex[i] = rf.lastLogIndex() + 1
					rf.matchIndex[i] = 0
				}
				DPrintf("[%d]: leader - nextIndex %#v", rf.me, rf.nextIndex)
				rf.sendAppendsL(true)
			}
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d]: term:%v received the vote request from: %v", rf.me, rf.currentTerm, args.CandidateId)

	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
	}
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	upToDate := args.LastLogTerm > rf.lastLogTerm() ||
		(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex())

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.resetElectionTimer()
		DPrintf("[%v]: term %v vote %v", rf.me, rf.currentTerm, rf.votedFor)
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}