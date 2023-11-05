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

//follower接收来自leader发送过来的snapshot
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	//如果leader任期比follower小，或leader的snapshot比follower的旧，则return
	if args.Term < rf.currentTerm{
		return 
	}
	if args.LastIncludedIndex < rf.lastsnapshotIndex{
		return 
	}

	//否则无条件接收来自leader的snapshot
	rf.snapshot = make([]byte, len(args.Data))
	copy(rf.snapshot, args.Data)

	rf.lastsnapshotIndex = args.LastIncludedIndex
	rf.lastsnapshotTerm  = args.LastIncludedTerm
	rf.trysnapshot = true
	DPrintf("[rf:%v %v]: InstallSnap, lastsnapindex:%v ", rf.me, rf.state, rf.lastsnapshotIndex)

	rf.persist()
	rf.applyCond.Broadcast() 
}

func (rf *Raft) sendSnapL(peer int){
	args := &InstallSnapshotArgs{rf.currentTerm, rf.me, rf.lastsnapshotIndex, rf.lastsnapshotTerm, make([]byte, len(rf.snapshot))}
	copy(args.Data, rf.snapshot)

	go func(){
		reply := InstallSnapshotReply{}
		ok := rf.sendSnapshot(peer, args, &reply)
		if ok{
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.pocessSnapshotReplyL(peer, args, &reply)
		}
	}()
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

//供上层kvserver调用，主动进行日志的快照
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.log.Index0 > index{
		return 
	}
	rf.lastsnapshotIndex = index
	rf.lastsnapshotTerm = rf.log.at(index).Term

	rf.snapshot = make([]byte, len(snapshot))
	copy(rf.snapshot, snapshot)

	//如果该snapshot包含的最后一条日志的索引，小于当前日志的最后一条日志的索引，则进行日志裁剪
	if index < rf.log.lastLog().Index{
		DPrintf("[rf:%v]: receive snapshot index:%v, rf.lastlogindex:%v", rf.me, index, rf.log.lastLog().Index)
		rf.log.cutstart(index+1)
	//否则将日志全部丢弃，创建新的空日志
	}else{
		Entries := []Entry{}
		rf.log = mkLog(Entries,index + 1)
	}
	rf.persist()
}

//供上层kvserver调用，判断是否可以安装快照
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//如果快照最后一条日志小于commitindex或等于lastapplied说明可能已经有新的日志在apply或已经apply了，旧快照不能安装
	if lastIncludedIndex < rf.commitIndex || lastIncludedIndex == rf.lastApplied || len(snapshot)==0 || lastIncludedIndex < rf.log.Index0 {
		return false 
	} 
	
	rf.lastsnapshotIndex = lastIncludedIndex
	rf.lastsnapshotTerm = lastIncludedTerm
	
	if len(rf.log.Entries) != 0 && lastIncludedIndex < rf.log.lastLog().Index {
		rf.log.cutstart(lastIncludedIndex + 1)
	}else{
		Entries := []Entry{}
		rf.log = mkLog(Entries,lastIncludedIndex + 1) 
	}
	rf.persist()

	return true
}
