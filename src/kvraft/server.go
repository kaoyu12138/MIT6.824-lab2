package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"sync"
	"sync/atomic"
	"bytes"
	"time"
)

type Op struct{
	SeqId int
	ClientId int64
	Key string
	Value string
	OpType OPType
}
 
type ResultMsg struct{
	MsgFromRaft raft.ApplyMsg
	ResultStr   string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	persister *raft.Persister
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	kvmap map[string]string 
	kvlastop map[int64]Op
	kvchan map[int]chan ResultMsg
	lastIncludeIndex int
	lastApplied int
	lastSnapshot int
}


func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}


func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvmap = make(map[string]string)
	kv.kvlastop = make(map[int64]Op)
	kv.kvchan = make(map[int]chan ResultMsg)

	kv.lastIncludeIndex = 0
	kv.lastApplied = 0
	kv.lastSnapshot = 0
	kv.persister = persister

	kv.setSnapshot(kv.persister.ReadSnapshot())

	go kv.Applyroute()
	go kv.snapshoter()

	return kv
}


func(kv *KVServer) Applyroute(){
	for applyMsg := range kv.applyCh{
		if kv.killed()!=false{
			continue;
		}

		resStr := ""
		if applyMsg.CommandValid{
		kv.mu.Lock()
		kv.lastApplied = applyMsg.CommandIndex
			op := applyMsg.Command.(Op)

			if lastOp, ok := kv.kvlastop[op.ClientId]; !ok || lastOp.SeqId != op.SeqId{
				DPrintf("[server:%v]: not duplicate OP, do the OP:%v\n", kv.me, op.OpType)
				switch op.OpType {
				case OpGet:
					resStr = kv.kvmap[op.Key]
				case OpPut:
					kv.kvmap[op.Key] = op.Value
				case OpAppend:
					kv.kvmap[op.Key] += op.Value
				default:

				}
				kv.kvlastop[op.ClientId] = op
			}else if ok && lastOp.SeqId == op.SeqId && op.OpType == OpGet{
				DPrintf("[server:%v]: duplicate Get oP, return pre result \n", kv.me)
				resStr = kv.kvmap[op.Key]
			}
			
			if curCh, ok := kv.kvchan[applyMsg.CommandIndex]; ok{
				DPrintf("[server:%v]: return chan result: %v, resultstr: %v \n", kv.me, applyMsg, resStr)
				curCh <- ResultMsg{MsgFromRaft:applyMsg, ResultStr: resStr}
				delete(kv.kvchan, applyMsg.CommandIndex - 1)
			}
			kv.mu.Unlock()
		}
		if applyMsg.SnapshotValid{
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
				kv.setSnapshot(applyMsg.Snapshot)
				kv.lastApplied = applyMsg.SnapshotIndex
	}
	kv.mu.Unlock()
}
}
}


func(kv *KVServer) Command(args *CmdArgs, reply *CmdReply){
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var cur_index int
	var cur_term int
	var is_leader bool
	op := Op{
		SeqId : args.SeqId,
		ClientId : args.ClientId,
		Key : args.Key,
		Value : args.Value,
		OpType : args.OpType,
	}
	if cur_index, cur_term, is_leader = kv.rf.Start(op); !is_leader{
		DPrintf("[server:%v]: is not leader \n", kv.me)
		reply.Err = ErrWrongLeader
	return
	}

	if pre_ch, ok := kv.kvchan[cur_index]; ok{
		fail_msg := raft.ApplyMsg{CommandValid: false, SnapshotValid: false}
		pre_ch <- ResultMsg{MsgFromRaft: fail_msg}
		DPrintf("[server:%v]:重复的index请求 \n", kv.me)
	}
	kv.kvchan[cur_index] = make(chan ResultMsg)
	cur_ch := kv.kvchan[cur_index]

		kv.mu.Unlock()
		DPrintf("[server:%v]: wait chan result\n", kv.me)

		result_msg := <- cur_ch

		kv.mu.Lock()
		if result_msg.MsgFromRaft.CommandValid {
		cur_op := result_msg.MsgFromRaft.Command.(Op)
		if result_msg.MsgFromRaft.CommandTerm == cur_term && cur_op.ClientId == args.ClientId{
			reply.Err = OK
		reply.Value = result_msg.ResultStr
			DPrintf("[server:%v]: return OK, value: %v \n", kv.me, reply.Value)
			return
		}else{
			DPrintf("[server:%v]: term change: is not leader \n", kv.me)
			reply.Err = ErrWrongLeader
		}
		}else{
		reply.Err = ErrNotCommand
	}
}
	
func (kv *KVServer) snapshoter() {
	for !kv.killed() {
		kv.mu.Lock()
		DPrintf("111")
        if kv.isNeedSnapshot() && kv.lastApplied > kv.lastSnapshot {
            kv.doSnapshot(kv.lastApplied)
            kv.lastSnapshot = kv.lastApplied
		}
		kv.mu.Unlock()
		time.Sleep(100*time.Millisecond)
    }
}

func (kv *KVServer) isNeedSnapshot() bool {
    if kv.maxraftstate != -1 && kv.rf.RaftPersistSize() > kv.maxraftstate {
        return true
    }
    return false
}

func (kv *KVServer) doSnapshot(commandIndex int) {
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
	if e.Encode(kv.kvmap) == nil &&
		e.Encode(kv.kvlastop) == nil {
		kv.rf.Snapshot(commandIndex, w.Bytes())
	}
}

func(kv *KVServer) setSnapshot(data []byte){
	if data == nil || len(data) < 1{
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvmap map[string]string
	var kvlastop map[int64]Op

	if d.Decode(&kvmap) == nil &&
		d.Decode(&kvlastop) == nil{
		kv.kvmap = kvmap
		kv.kvlastop = kvlastop
	}
}
