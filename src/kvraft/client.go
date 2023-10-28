package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "time"
import "sync"

const (
	RPCWait = "RPCWait"
	RPCWithRes = "RPCWithRes"
	RPCNoRes = "RPCNoRes"
	RPCWrong = "RPCWrong"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	mu sync.Mutex
	rpcstate string
	rpcres string
	rpcnum int
	clerkId int64
	seqId int
	leaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clerkId = nrand()
	ck.seqId = 0
	ck.leaderId = 0
	ck.rpcnum = 0

	return ck
}

func (ck *Clerk) sendCmd(key string, value string, OpType OPType) (string, bool) {
	for i,j := ck.leaderId, 0; j < len(ck.servers) ; j ++ {
		ck.mu.Lock()
		cur_server := (i + j)%len(ck.servers)

		ck.rpcnum += 1
		ck.rpcstate = RPCWait
		//DPrintf("[clerk:%v]: ckrpcnum:%v, ckrpcstate:%v, sendto:%v\n", ck.clerkId, ck.rpcnum, ck.rpcstate, cur_server)

		ck.mu.Unlock()
		go ck.sendCmdRpc(cur_server, ck.rpcnum, key, value, OpType)
	

		for t := 1; t <= 60; t ++ {
			ck.mu.Lock()
			if ck.rpcstate == RPCWithRes || ck.rpcstate == RPCNoRes{
				ck.seqId ++
				ck.leaderId = cur_server
				DPrintf("[clerk:%v]: return result:%v\n",ck.clerkId, ck.rpcres)
				ck.mu.Unlock()
				return ck.rpcres,true
			}else if ck.rpcstate == RPCWrong{
				//DPrintf("[clerk:%v]: return wrong rpc\n", ck.clerkId)
				ck.mu.Unlock()
				break
			}
			ck.mu.Unlock()
			time.Sleep(time.Duration(10) * time.Millisecond)
		}
	}
	return "",false
}

func (ck *Clerk) sendCmdRpc(serverID int, rpcnum int, key string, value string, OpType OPType) {
	args := &CmdArgs{
		SeqId : ck.seqId,
		ClientId : ck.clerkId,
		Key : key,
		Value : value,
		OpType : OpType,
	}
	reply := &CmdReply{}
	//DPrintf("[clerk:%v]: send rpc to server:%v, args:%v\n", ck.clerkId, serverID, args)
	ok := ck.servers[serverID].Call("KVServer.Command", args, reply)
	
	if ok {
		ck.mu.Lock()
		defer ck.mu.Unlock()
		DPrintf("[clerk:%v]: get rpc from server:%v, reply:%v", ck.clerkId, reply.ServerId, reply)
		
		if rpcnum == ck.rpcnum{
			if reply.Err == OK{
				if OpType == OpGet{
					ck.rpcstate = RPCWithRes
					ck.rpcres = reply.Value
				}else {
					ck.rpcstate = RPCNoRes
				}
			}else if reply.Err == ErrNoKey{
				ck.rpcstate = RPCNoRes
			}else{
				ck.rpcstate = RPCWrong
			}
		}
	}
}

func (ck *Clerk) sendGet(key string) string{
	var result string
	var ok bool
	for{
 		result, ok = ck.sendCmd(key, "", OpGet)
		if ok{
			break;
		}
	}
	return result
}

func (ck *Clerk) sendPut(key string, value string){
	var ok bool
	for{
		_, ok = ck.sendCmd(key, value, OpPut)
		if ok{
			break;
		}
	}
}

func (ck *Clerk) sendAppend(key string, value string){
	var ok bool
	for{
		_, ok = ck.sendCmd(key, value, OpAppend)
		if ok{
			break
		}
	}
}

func (ck *Clerk) Get(key string) string {
DPrintf("[clerk:%v]: receive OPGET\n", ck.clerkId)
    return ck.sendGet(key)
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("[clerk:%v]: receive OPPUT\n", ck.clerkId)
    ck.sendPut(key, value)
}

func (ck *Clerk) Append(key string, value string) {
	DPrintf("[clerk:%v]: receive OPAPPEND\n", ck.clerkId)
    ck.sendAppend(key, value)
}