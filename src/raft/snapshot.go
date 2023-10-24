package raft

type InstallSnapshotArgs struct{
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data []byte
}

type InstallSnapshotReply struct{
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm{
		return 
	}

	rf.snapshot = make([]byte, len(args.Data))
	copy(rf.snapshot, args.Data)

	rf.lastsnapshotIndex = args.LastIncludedIndex
	rf.lastsnapshotTerm  = args.LastIncludedTerm
	rf.trysnapshot = true
	DPrintf("[%v]:收到来自leader的snapshot: lastIndex:%v, [nowlogs:%v], [snapshot:%v]", rf.me, args.LastIncludedIndex, rf.log.Entries, args.Data)
	rf.applyCond.Broadcast() 
}

func (rf *Raft) sendSnap(peer int){
	reply := InstallSnapshotReply{}
	args := &InstallSnapshotArgs{rf.currentTerm, rf.me, rf.lastsnapshotIndex, rf.lastsnapshotTerm, make([]byte, len(rf.snapshot))}
	copy(args.Data, rf.snapshot)
	DPrintf("[%v]: sendSnap to %v: lastsnapshotIndex:%v",rf.me, peer, rf.lastsnapshotIndex)
	ok := rf.sendSnapshot(peer, args, &reply)
	if ok{
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.pocessSnapshotReplyL(peer, args, &reply)
	}
}

func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
} 

func (rf *Raft) pocessSnapshotReplyL(serverId int, args *InstallSnapshotArgs, reply *InstallSnapshotReply){
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term) 
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastsnapshotIndex = index
	rf.lastsnapshotTerm = rf.log.at(index).Term

	rf.snapshot = make([]byte, len(snapshot))
	copy(rf.snapshot, snapshot)
	if index < rf.log.lastLog().Index{
		rf.log.cutstart(index+1)
	}else{
		Entries := []Entry{}
		rf.log = mkLog(Entries,index + 1)
	}
	rf.persistsnapshot()
	DPrintf("[%v]:收到来自上层的snapshot: lastIndex:%v, [nowlogs:%v], nowlog's Index0:%v", rf.me, index, rf.log.Entries, rf.log.Index0)
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex < rf.commitIndex || lastIncludedIndex == rf.lastApplied {
		DPrintf("[%v]: snapshot应用失败, lastIncluedIndex:%v, lastIncludedTerm:%v, commitindex:%v", rf.me, lastIncludedIndex, lastIncludedTerm, rf.commitIndex)
		return false 
	} 
	
	rf.lastsnapshotIndex = lastIncludedIndex
	rf.lastsnapshotTerm = lastIncludedTerm
	
	if len(rf.log.Entries) != 0 && lastIncludedIndex < rf.log.lastLog().Index  {
		rf.log.cutstart(lastIncludedIndex + 1)
	}else{
		Entries := []Entry{}
		rf.log = mkLog(Entries,lastIncludedIndex + 1) 
	}
	rf.persistsnapshot()

	DPrintf("[%v]: snapshot应用成功, lastIncluedIndex:%v, lastIncludedTerm:%v", rf.me, lastIncludedIndex, lastIncludedTerm)
	return true
}
