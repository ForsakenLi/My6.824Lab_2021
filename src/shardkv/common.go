package shardkv

import (
	"6.824/shardctrler"
	"log"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int
	Version  int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type             string
	Key              string
	Value            string
	Version          int64
	ID               int
	NewConfig		 shardctrler.Config
	// Shard transfer
	CleanShardNum   []int
	//ReceiveShards   []ShardData
	//ReceiveShardNum int
	ReceiveShardMap	map[int]ShardData
	ConfigNum		int		// for both clean and push
}

type OpHandlerReply struct {
	Value string
	Err   Err
}

type ShardStatus int

const (
	Regular ShardStatus = iota	// 无论本Shard归或者不归本Group管理，正常情况都会位于该状态
	Pushing		// config在update时发现前一个版本中的某个分片迁移到其他raft组去了
	WaitPush	// config在update时发现新版本出现了旧版本中未曾出现的分片
)

// ShardData 由于Lab 4b的Challenge 2要求分片数据的迁移相互独立，因此需要将分片数据独立包装,
// 同时提供类似ExpertPattern的接口直接完成Get/Put/Append相关对shard数据的操作
type ShardData struct {
	VersionMap map[int]int64 // clientId->its version
	KvMap      map[string]string
	State	   ShardStatus
}

func (sd *ShardData) deepCopy() ShardData {
	ver := make(map[int]int64)
	for k, v := range sd.VersionMap {
		ver[k] = v
	}
	kv := make(map[string]string)
	for k, v := range sd.KvMap {
		kv[k] = v
	}
	return ShardData{
		VersionMap: ver,
		KvMap:      kv,
		State:      sd.State,
	}
}

func (sd *ShardData) ModifyKV(op Op) OpHandlerReply {
	if !isRegularOp(&op) {
		panic("illegal modifyKV op type")
	}
	if sd.State == Pushing || sd.State == WaitPush {
		return OpHandlerReply{Value: "", Err: ErrWrongGroup}
	}
	if op.Type == "Get" {
		if value, ok := sd.KvMap[op.Key]; ok {
			return OpHandlerReply{Value: value, Err: OK}
		}
		return OpHandlerReply{Value: "", Err: ErrNoKey}
	} else {	// PutAppend
		if op.Version <= sd.VersionMap[op.ID] {
			// dup operation
			return OpHandlerReply{Value: "", Err: OK}
		}
		switch op.Type {
		case "Put":
			sd.KvMap[op.Key] = op.Value
		case "Append":
			if origin, ok := sd.KvMap[op.Key]; ok {
				sd.KvMap[op.Key] = origin + op.Value
			} else {
				sd.KvMap[op.Key] = op.Value
			}
		}
		sd.VersionMap[op.ID] = op.Version	//更新版本
		return OpHandlerReply{Value: "", Err: OK}
	}
}

func isRegularOp(op *Op) bool {
	if op.Type == "Get" || op.Type == "Put" || op.Type == "Append" {
		return true
	}
	return false
}

//func deepCopy(dst, src interface{}) error {
//	var buf bytes.Buffer
//	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
//		return err
//	}
//	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
//}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

