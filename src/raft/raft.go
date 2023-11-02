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

//供上层状态机调用，获取当前peer的状态
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

//供上层状态机调用，向当前peer尝试发送一条command
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//若peer不是leader，发送失败，直接返回
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	index := rf.lastLogIndex() + 1
	e := Entry{command, rf.currentTerm, index}

	//将command添加到日志中
	rf.log.appendone(e)
	rf.persist()
	
	//收到command后，立刻发送一轮追加日志，加快日志的同步
	rf.sendAppendsL(false)

	return index, rf.currentTerm, true
}

//如果接收到的 RPC 请求或响应中，任期号T > currentTerm
//则令 currentTerm = T，并切换为跟随者状态
func (rf *Raft) setNewTerm(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

//重置选举超时时间：350~700ms
func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	t = t.Add(350 * time.Millisecond)
	ms := rand.Int63()%350
	t = t.Add(time.Duration(ms) * time.Millisecond)
	rf.electionTime = t
}

//心跳时间：100ms
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.check()
		ms := 100
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

//检查当前peer状态
func (rf *Raft) check() {
	rf.mu.Lock()
	if rf.state == Leader {
		//peer为leader，发送一轮心跳包
		rf.sendAppendsL(true)
	}else if time.Now().After(rf.electionTime) {
		//peer达到选举超时时间，重新开始一轮选举
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

//用于在leader append成功时检查是否能更新commitIndex的值
func (rf *Raft) updateCommitIndexL() {
	//如果不是leader 或 当前日志为空则退出
	if rf.state != Leader || len(rf.log.Entries) == 0 {
		return
	}

	start := rf.commitIndex + 1
	if start < rf.log.Index0{
		start = rf.log.Index0
	}

	for n := start ; n <= rf.lastLogIndex(); n++ {
		//每次commit都只能提交自己当前任期的日志，故不是当前任期则continue
		if rf.log.at(n).Term != rf.currentTerm {
			continue
		}
		counter := 1
		//遍历其他raft peers，检查该日志是否在这些peer上
		for serverId := 0; serverId < len(rf.peers); serverId++ {
			if serverId != rf.me && rf.matchIndex[serverId] >= n {
				counter++
			}
		}
		//如果发现该日志存在于大部分peers的日志中，则更新commitIndex，并唤醒applier线程
		if counter > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.applyCond.Broadcast()
		}
	}
}

//等待唤醒，并进行快照或日志的apply
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
			DPrintf("[rf:%v %v]: rf.applysnapshot success: lastsnapshotindex:%v, snapshot:%v", rf.me, rf.state, rf.lastsnapshotIndex, len(rf.snapshot)) 
		} else if len(rf.log.Entries)!=0 && rf.commitIndex > rf.lastApplied && rf.lastLogIndex() > rf.lastApplied {
			rf.lastApplied++
			if rf.lastApplied >= rf.log.Index0 {
				DPrintf("[rf:%v %v]: try to apply lastapplied:%v", rf.me, rf.state, rf.lastApplied)
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log.at(rf.lastApplied).Command,
					CommandIndex: rf.lastApplied,
					CommandTerm: rf.currentTerm,
				}
				rf.mu.Unlock()
				//将applyMsg放入通道时先解锁，防止死锁阻塞
				rf.applyCh <- applyMsg
				rf.mu.Lock()
				DPrintf("[rf:%v %v]: rf.apply success", rf.me, rf.state)
			}
		} else {
			//applier线程在此处等待被唤醒
			rf.applyCond.Wait()
		}
	}
}



