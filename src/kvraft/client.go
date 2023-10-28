package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "time"
import "sync"

const (
	RPCWait = "RPCWait"         //等待RPC回复状态
	RPCWithRes = "RPCWithRes"   //带有返回结果的RPC状态
	RPCNoRes = "RPCNoRes"       //不带有返回结果的RPC状态
	RPCWrong = "RPCWrong"		//RPC出错状态
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	mu sync.Mutex

	rpcstate string 			//记录rpc请求的完成状态
	rpcres string				//记录rpc请求的结果
	rpcnum int					//记录rpc的序号值，方便正确接收rpc回复

	clerkId int64				//记录该client的ID
	seqId int					//记录command的序号值，防止重复执行命令
	leaderId int				//记录当前leaderID
}

//负责生成随机值，作为clerkID
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
		DPrintf("[clerk:%v]: ckrpcnum:%v, ckrpcstate:%v, sendto:%v\n", ck.clerkId, ck.rpcnum, ck.rpcstate, cur_server)

		ck.mu.Unlock()
		go ck.sendCmdRpc(cur_server, ck.rpcnum, key, value, OpType)
	
		//在client端实现超时机制，rpc请求发向一个server后会在for循环中等待60*10ms
		for t := 1; t <= 60; t ++ {
			ck.mu.Lock()
			//rpc返回预期结果
			if ck.rpcstate == RPCWithRes || ck.rpcstate == RPCNoRes{
				ck.seqId ++									//当前发送的command已经完成，递增seqId
				ck.leaderId = cur_server					//更新当前的leaderID
				DPrintf("[clerk:%v]: return result:%v\n",ck.clerkId, ck.rpcres)
				ck.mu.Unlock()								
				return ck.rpcres,true						//返回结果
			}else if ck.rpcstate == RPCWrong{
				DPrintf("[clerk:%v]: return wrong rpc\n", ck.clerkId)
				ck.mu.Unlock()
				break										//rpc请求发生错误，则跳出循环，尝试发送给下一个server
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
	DPrintf("[clerk:%v]: send rpc to server:%v, args:%v\n", ck.clerkId, serverID, args)
	ok := ck.servers[serverID].Call("KVServer.Command", args, reply)
	
	if ok {
		ck.mu.Lock()
		defer ck.mu.Unlock()
		DPrintf("[clerk:%v]: get rpc from server:%v, reply:%v", ck.clerkId, reply.ServerId, reply)
		
		//通过rpcnum确定该reply是否是本次rpc请求的回复
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
	//使用loop不断重试command，直到返回预期结果
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