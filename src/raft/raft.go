package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"6.824/labrpc"
)

type RaftState string

const (
	Follower  RaftState = "Follower"
	Candidate           = "Candidate"
	Leader              = "Leader"
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state        RaftState
	electionTime time.Time

	// Persistent state on all servers:
	currentTerm int
	votedFor    int
	log         Log

	// Volatile state on all servers:
	commitIndex int
	lastApplied int

	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	snapshot []byte
	lastsnapshotTerm int
	lastsnapshotIndex int
	trysnapshot bool
}

func (rf *Raft) RaftPersistSize() int{
	return rf.persister.RaftStateSize()
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == Leader
	return term, isleader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = Follower
	rf.votedFor = -1
	rf.resetElectionTimer()

	rf.log = makeEmptyLog()

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.applier()

	return rf
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	index := rf.lastLogIndex() + 1
	e := Entry{command, rf.currentTerm, index}

	rf.log.appendone(e)
	rf.persist()
	DPrintf("[%v]: term %v Start %v", rf.me, rf.currentTerm, e)
	rf.sendAppendsL(false)

	return index, rf.currentTerm, true
}

func (rf *Raft) setNewTerm(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
	DPrintf("[%d]: set term %v\n", rf.me, rf.currentTerm)
}


func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	t = t.Add(350 * time.Millisecond)
	ms := rand.Int63()%350
	t = t.Add(time.Duration(ms) * time.Millisecond)
	rf.electionTime = t
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.check()
		DPrintf("[%v]: wake up", rf.me)
		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) check() {
	rf.mu.Lock()
	if rf.state == Leader {
		rf.sendAppendsL(true)
	}else if time.Now().After(rf.electionTime) {
		rf.startElectionL()
	}
	rf.mu.Unlock()
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (rf *Raft) updateCommitIndexL() {
	if rf.state != Leader || len(rf.log.Entries) == 0 {
		return
	}

	start := rf.commitIndex + 1
	if start < rf.log.Index0{
		start = rf.log.Index0
	}

	for n := start ; n <= rf.log.lastLog().Index; n++ {
		if rf.log.at(n).Term != rf.currentTerm {
			continue
		}
		counter := 1
		for serverId := 0; serverId < len(rf.peers); serverId++ {
			if serverId != rf.me && rf.matchIndex[serverId] >= n {
				counter++
			}
		}
		if counter > len(rf.peers)/2 {
			rf.commitIndex = n
			DPrintf("[%v] leader尝试提交 index %v", rf.me, rf.commitIndex)
			rf.applyCond.Broadcast()
		}
	}
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastApplied = 0
	if rf.lastApplied < rf.log.Index0{
		rf.lastApplied = rf.log.Index0 - 1
	}

	for !rf.killed() {
		if rf.trysnapshot != false {
			applyMsg := ApplyMsg{
				SnapshotValid: true,
				Snapshot: rf.snapshot,
				SnapshotTerm: rf.lastsnapshotTerm,
				SnapshotIndex: rf.lastsnapshotIndex,
			}
			rf.trysnapshot = false
			
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
			DPrintf("[%v]: rf.applysnapshot success", rf.me) 
		} else if len(rf.log.Entries)!=0 && rf.commitIndex > rf.lastApplied && rf.log.lastLog().Index > rf.lastApplied {
			rf.lastApplied++
			if rf.lastApplied >= rf.log.Index0 {
				DPrintf("[%v]: try to apply lastapplied:%v", rf.me, rf.lastApplied)
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log.at(rf.lastApplied).Command,
					CommandIndex: rf.lastApplied,
					CommandTerm: rf.currentTerm,
				}
				rf.mu.Unlock()
				rf.applyCh <- applyMsg
				rf.mu.Lock()
				DPrintf("[%v]: rf.apply success", rf.me)
			}
		} else {
			rf.applyCond.Wait()
			DPrintf("[%v]: rf.applyCond.Wait()", rf.me)
		}
	}
}



