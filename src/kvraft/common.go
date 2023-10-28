package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNotCommand  = "ErrNotCommand"
)

const (
	OpGet          = "OpGet"
	OpPut          = "OpPut"
	OpAppend       = "OpAppend"
)

type Err string
type OPType string

type CmdReply struct {
	Err   Err
	Value string
	ServerId int
}

type CmdArgs struct{
	SeqId int
	ClientId int64
	Key string
	Value string
	OpType OPType
}

type OpResp struct{
	Err Err
	Value string
}
