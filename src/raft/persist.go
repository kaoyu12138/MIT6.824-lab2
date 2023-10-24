package raft

import(
	"6.824/labgob"
	"bytes"
	"log"
)

func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.Encoder())
	DPrintf("[%v]: 持久化状态votefor:%v, term:%v", rf.me, rf.votedFor, rf.currentTerm)
}

func (rf *Raft) Encoder() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
}

func (rf *Raft) readPersist(data []byte) {
   if data == nil || len(data) < 1 { // bootstrap without any state?
	   return
   }

   r := bytes.NewBuffer(data)
   d := labgob.NewDecoder(r)
   var currentTerm int
   var votedFor int
   var logs Log

   if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
	   log.Fatal("failed to read persist\n")
   } else {
	   rf.currentTerm = currentTerm
	   rf.votedFor = votedFor
	   rf.log = logs
	   DPrintf("[%v]:恢复状态voteFor:%v, term:%v", rf.me, votedFor, currentTerm)
   }
}

func (rf *Raft) persistsnapshot(){
	m := new(bytes.Buffer)
	b := labgob.NewEncoder(m)
	b.Encode(rf.snapshot)
	snapshot := m.Bytes()
	rf.persister.SaveStateAndSnapshot(rf.Encoder(), snapshot)
	DPrintf("[%v]: 持久化状态votefor:%v, term:%v, [snapshot:%v]", rf.me, rf.votedFor, rf.currentTerm, snapshot)
}
