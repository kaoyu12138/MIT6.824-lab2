package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term     int
	Success  bool
	Conflict bool
	XTerm    int
	XIndex   int
	XLen     int
}

//follower处理leader发送的Append RPC请求
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.Conflict = false

	//当follower任期比leader高，则返回false
	if args.Term < rf.currentTerm {
		return
	}

	//重置选举超时时间
	rf.resetElectionTimer()

	//修改follower的任期 以及 状态
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
	}

	//当前日志长度不为零的处理逻辑
	if len(rf.log.Entries) != 0{
		//防止日志切片越界，对prelogindex进行处理，如果小于index0，则让其等于index0
		if args.PrevLogIndex < rf.log.Index0 {
			args.PrevLogIndex = rf.log.Index0
			args.PrevLogTerm = rf.log.at(rf.log.Index0).Term
		}
	
		//如果prelogindex大于当前日志的最后一条日志索引，则判定全部日志冲突
		if rf.lastLogIndex() < args.PrevLogIndex {
			reply.Conflict = true
			reply.XTerm = -1
			reply.XIndex = -1
			reply.XLen = rf.log.len()
			return
		}
	
		if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
			reply.Conflict = true
			xTerm := rf.log.at(args.PrevLogIndex).Term
			//找到冲突任期内的第一条日志
			for xIndex := args.PrevLogIndex; xIndex > rf.log.Index0 ; xIndex-- {
				if rf.log.at(xIndex-1).Term != xTerm {
					reply.XIndex = xIndex
					break
				}
			}
			reply.XTerm = xTerm
			reply.XLen = rf.log.len()
			return
		}

		//如果prelogindex日志没有发生冲突的处理逻辑
		for idx, entry := range args.Entries {
			if entry.Index < rf.log.Index0 {
				continue 
			}
			//找到prelog之后第一条冲突的日志，截断该日志后的所有日志丢弃不用
			if len(rf.log.Entries)!=0 && entry.Index <= rf.lastLogIndex() && rf.log.at(entry.Index).Term != entry.Term {
				rf.log.cutend(entry.Index)
			}
			//追加上leader发送来的日志
			if entry.Index > rf.lastLogIndex() {
				rf.log.append(args.Entries[idx:]...)
				rf.persist()
				break
			}
		}
	//日志长度为零的处理逻辑
	} else {
		//如果leader发送的是heartbeat 或 args的第一条日志大于Index0说明日志冲突，直接返回
		if len(args.Entries) == 0 || args.Entries[0].Index > rf.log.Index0 {
			return 
		}
		//否则找到args中对应Index0的第一条日志，然后追加leader发送的日志
		for idx, entry := range args.Entries {
			if entry.Index < rf.log.Index0 {
				continue 
			}
			rf.log.append(args.Entries[idx:]...)
			rf.persist()
			break
		}
	}

	//更新follower的commitindex，并尝试唤醒applier线程
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
		rf.applyCond.Broadcast()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendsL(heartbeat bool) {
	for peer, _ := range rf.peers {
		if peer != rf.me {
			if heartbeat || (len(rf.log.Entries)!=0 && rf.lastLogIndex() >= rf.nextIndex[peer]) {
				rf.sendAppendL(peer, heartbeat)
			}
		}
	}
}

func (rf *Raft) sendAppendL(peer int, heartbeat bool) {
	nextIndex := rf.nextIndex[peer]

	var prevLogIndex int
	var prevLogTerm int
	var entries []Entry

	if len(rf.log.Entries) == 0{
		prevLogIndex = rf.lastsnapshotIndex
		prevLogTerm = rf.lastsnapshotTerm
		entries = make([]Entry, 0)
	}else {
		lastLogIndex := rf.lastLogIndex()
		if nextIndex <= rf.log.Index0 {
			nextIndex = rf.log.Index0 
			prevLogIndex = rf.lastsnapshotIndex
			prevLogTerm = rf.lastsnapshotTerm
		}else if nextIndex > lastLogIndex + 1{
			nextIndex = lastLogIndex + 1
			prevLogIndex = nextIndex - 1
			prevLogTerm = rf.log.at(prevLogIndex).Term
		}else{
			prevLogIndex = nextIndex - 1
			prevLogTerm = rf.log.at(prevLogIndex).Term
		}

		entries = make([]Entry, lastLogIndex-nextIndex+1)
		copy(entries, rf.log.slice(nextIndex))
	}
    
	args := &AppendEntriesArgs{
		Term: rf.currentTerm, 
		LeaderId: rf.me, 
		PrevLogIndex: prevLogIndex,
		PrevLogTerm: prevLogTerm, 
		Entries: entries, 
		LeaderCommit: rf.commitIndex,
	}

	go func() {
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, &reply)
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.pocessAppendReplyL(peer, args, &reply)
		}
	}()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//leader处理来自follower关于Append RPC的回复
func (rf *Raft) pocessAppendReplyL(serverId int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		return
	}else if args.Term == rf.currentTerm {
		if reply.Success {
			match := args.PrevLogIndex + len(args.Entries)
			next := match + 1
			rf.nextIndex[serverId] = max(rf.nextIndex[serverId], next)
			rf.matchIndex[serverId] = max(rf.matchIndex[serverId], match)
		}else if reply.Conflict {
			//如果返回XTerm为-1，说明全部日志冲突
			if reply.XTerm == -1 {
				rf.nextIndex[serverId] = reply.XLen
			} else {
				//否则找到第一条冲突的日志索引
				lastLogInXTerm := rf.findLastLogInTermL(reply.XTerm)
				DPrintf("[%v]: lastLogInXTerm %v", rf.me, lastLogInXTerm)
				if lastLogInXTerm > 0 {
					rf.nextIndex[serverId] = lastLogInXTerm
				} else {
					rf.nextIndex[serverId] = reply.XIndex
				}
			}
			//如果待发送给follower的日志索引小于当前日志的Index0，则发送快照，并更新nextIndex[]
			if rf.nextIndex[serverId] < rf.log.Index0{
				rf.sendSnapL(serverId)
				rf.nextIndex[serverId] = rf.log.Index0
			}
		}else if rf.nextIndex[serverId] > 1 {
			rf.nextIndex[serverId]--
			if rf.nextIndex[serverId] < rf.log.Index0{
			    rf.sendSnapL(serverId)
				rf.nextIndex[serverId] = rf.log.Index0
			}
		}
		rf.updateCommitIndexL()
	}
}

func (rf *Raft) findLastLogInTermL(x int) int {
	for i := rf.lastLogIndex(); i > rf.log.Index0; i-- {
		term := rf.log.at(i).Term
		if term == x {
			return i	
		} else if term < x {
			break
		}
	}
	return -1
}
