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
	SeqId int						//记录本次command的序号值
	ClientId int64					//记录发送本次command的clientID
	Key string						//记录本次command的key值
	Value string					//记录本次comman的value值
	OpType OPType					//记录本次command的类型
}
 
type ResultMsg struct{
	MsgFromRaft raft.ApplyMsg		//记录raft层apply的消息
	ResultStr   string				//记录本次command需要返回给client的结果
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	persister *raft.Persister
	applyCh chan raft.ApplyMsg
	dead    int32

	maxraftstate int				//定义允许的raft层持久化状态最大值，超过此值需要裁减raft层日志，进行快照
	kvmap map[string]string 		//用于server层的kv存储
	kvlastop map[int64]Op			//用于记录各client发来的command历史，防止重复执行command
	kvchan map[int]chan ResultMsg	//用于Applyroute线程正确返回消息
	lastApplied int					
	lastSnapshot int
}


func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}


func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate


	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvmap = make(map[string]string)
	kv.kvlastop = make(map[int64]Op)
	kv.kvchan = make(map[int]chan ResultMsg)

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

			//通过kvlastop检测该client发送的command是否已经被执行过
			if lastOp, ok := kv.kvlastop[op.ClientId]; !ok || lastOp.SeqId != op.SeqId{
				DPrintf("[server:%v]: not duplicate OP, do the OP:%v\n", kv.me, op.OpType)
				//若未执行，则执行相关的command命令
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
			//若已经执行过该command，且如果该command是OpGet操作，则返回value值
			}else if ok && lastOp.SeqId == op.SeqId && op.OpType == OpGet{
				DPrintf("[server:%v]: duplicate Get oP, return pre result \n", kv.me)
				resStr = kv.kvmap[op.Key]
			}
			
			//取出该commandIndex相关联的通道，传入applyMsg和resstr结果字符串
			if curCh, ok := kv.kvchan[applyMsg.CommandIndex]; ok{
				curCh <- ResultMsg{MsgFromRaft:applyMsg, ResultStr: resStr}
				delete(kv.kvchan, applyMsg.CommandIndex - 1)
			}
			kv.mu.Unlock()
		}
		if applyMsg.SnapshotValid{
			kv.mu.Lock()
			//调用raft层的CondInstallSnapshot()，判断当前的snapshot文件是否可以安装
			if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
				kv.setSnapshot(applyMsg.Snapshot)						//安装snapshot
				kv.lastApplied = applyMsg.SnapshotIndex					//更新lastapplied值
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

	//调用raft层的Start()，尝试将command发送给该server关联的raft peer
	if cur_index, cur_term, is_leader = kv.rf.Start(op); !is_leader{
		DPrintf("[server:%v]: is not leader \n", kv.me)
		reply.Err = ErrWrongLeader									//如果关联的raft peer不是leader，则返回rpc请求错误
		return
	}

	//如果已经有相同commandIndex的通道存在，则让它失效（发送一个fail_msg让它接收)
	if pre_ch, ok := kv.kvchan[cur_index]; ok{
		fail_msg := raft.ApplyMsg{CommandValid: false, SnapshotValid: false}
		pre_ch <- ResultMsg{MsgFromRaft: fail_msg}
		DPrintf("[server:%v]:重复的index请求 \n", kv.me)
	}

	kv.kvchan[cur_index] = make(chan ResultMsg)
	cur_ch := kv.kvchan[cur_index]

	kv.mu.Unlock()
	result_msg := <- cur_ch
	kv.mu.Lock()

	if result_msg.MsgFromRaft.CommandValid {
		//取出该raft applMsg的command内容
		cur_op := result_msg.MsgFromRaft.Command.(Op)
		//检测该command是否与rpc请求的command对应
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
        if kv.isNeedSnapshot() && kv.lastApplied > kv.lastSnapshot {
			//若raft持久化状态过大，且有未被快照的日志，就进行快照
            kv.doSnapshot(kv.lastApplied)
            kv.lastSnapshot = kv.lastApplied
		}
		kv.mu.Unlock()
		time.Sleep(100*time.Millisecond)
    }
}

//检测当前是否需要进行snapshot
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
