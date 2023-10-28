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

//peer选举超时，开始一次新的选举
func (rf *Raft) startElectionL() {
	rf.resetElectionTimer()
	//修改当前peer的状态
	rf.currentTerm += 1
	rf.state = Candidate
	rf.votedFor = rf.me

	rf.persist()

	//向其他所有peer发送选举RPC
	rf.requestVotesL()
}

//candidate调用requestVote向其他所有peer群发RPC
func (rf *Raft) requestVotesL() {
	args := &RequestVoteArgs{rf.currentTerm, rf.me,
		rf.lastLogIndex(), rf.lastLogTerm()}
	//voteConter：统计其他peer给当前peer的投票数
	voteCounter := 1
	for serverId, _ := range rf.peers {
		if serverId != rf.me {
			go rf.requestVote(serverId, args, &voteCounter)
		}
	}
}

//candidate对单个peer发送RPC的函数实现
func (rf *Raft) requestVote(serverId int, args *RequestVoteArgs, voteCounter *int) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serverId, args, &reply)

	//candidate处理来自peer的reply
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		//peer的任期比candidate高，转为follower并return
		if reply.Term > rf.currentTerm{
			rf.setNewTerm(reply.Term)
			return 
		}

		//如果已不是candidate，则return
		if rf.state != Candidate{
			return 
		}
	
		//peer给candidate投票
		if reply.VoteGranted {
			*voteCounter++
			//如果超过1/2的peer给当前candidate投票
			//且该选举未过期（rf.currentTerm == args.Term）
			if *voteCounter > len(rf.peers)/2 &&
				rf.currentTerm == args.Term {
				//candidate选举成功变为leader
				rf.state = Leader
				
				for i, _ := range rf.peers {
					//nextindex[]:初始值为领导人最后的日志条目的索引+1
					rf.nextIndex[i] = rf.lastLogIndex() + 1
					//matchindex[]:初始值为0
					rf.matchIndex[i] = 0
				}

				//立刻向其他所有peer发送心跳包，确立leader权威
				rf.sendAppendsL(true)
			}
		}
	}
}

//peer处理来自candidate的求票请求
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	

	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
	}
	//若peer的任期比candidate大，拒绝投票并返回
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	//判断candidate的日志是否比peer更新
	//判断标准:最后一条日志的term大小；最后日志的索引值大小
	upToDate := args.LastLogTerm > rf.lastLogTerm() ||
		(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex())

	//若当前peer没有给除了candidate以外的人投票，且candidate日志比peer新，同意投票
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()

		//重置选举超时时间
		rf.resetElectionTimer()
	//否则，拒绝投票
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}