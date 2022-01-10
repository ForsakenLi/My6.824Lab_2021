package shardkv

import (
	"sync"
)

type PushShardArgs struct {
	ConfigNum int
	Shard     ShardData
	ShardNum  int
}

type PushShardReply struct {
	Reply string
}

// dataTransfer 检查所有shard， 如果处于Pushing标记状态，则发起PushShard RPC将该分片发送过去
func (kv *ShardKV) dataTransfer() {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		kv.TransferTimer.Reset(DataTransferTimeout)
		return
	}
	var wg sync.WaitGroup
	kv.lock("DataTransfer")
	for shardNum, gid := range kv.nowConfig.Shards { // Push的目标Gid需要根据上一个版本的Config找到
		if kv.ShardCollection[shardNum].State == Pushing {
			wg.Add(1)
			var copyShard = kv.ShardCollection[shardNum].deepCopy()
			copyShard.State = Regular //发送过去的应该是Regular，对方确认无误后直接覆盖为WaitPush状态的Shard
			args := PushShardArgs{
				ConfigNum: kv.nowConfig.Num, // 发送的配置版本号需要为目前版本的Config
				Shard:     copyShard,
				ShardNum:  shardNum,
			}
			go func(args *PushShardArgs, servers []string, shardNum int) {
				defer wg.Done()
				for _, serverName := range servers {
					server := kv.make_end(serverName)
					var reply PushShardReply
					if server.Call("ShardKV.PushShard", args, &reply) && reply.Reply == "OK" {
						DPrintf("[Peer %d] Push Shard to raft group %d success %+v", kv.me, kv.gid, args)
						cleanOp := Op{
							Type:          "CleanShard",
							CleanShardNum: shardNum,
						}
						kv.rf.Start(cleanOp)
					}
				}
			}(&args, kv.nowConfig.Groups[gid], shardNum)	//最后这个shardNum一定要传进去，否则会导致协程内读取外部的for-each迭代变量
		}
	}
	kv.unlock("DataTransfer")
	wg.Wait()
	kv.TransferTimer.Reset(DataTransferTimeout)
}

// PushShard 需要检查args的config.Num和当前的是否对应，并check分片是否处于WaitPush状态，妥当后方可确认接收并返回OK
func (kv *ShardKV) PushShard(args *PushShardArgs, reply *PushShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	kv.lock("PushShard")
	if args.ConfigNum == kv.nowConfig.Num && kv.ShardCollection[args.ShardNum].State == WaitPush {
		DPrintf("[Peer %d] push success in config num %d\n", kv.me, kv.nowConfig.Num)
		var copyShard = args.Shard.deepCopy()
		recShardOp := Op{
			Type:            "ReceiveShard",
			ReceiveShard:    copyShard,
			ReceiveShardNum: args.ShardNum,
		}
		kv.rf.Start(recShardOp)
		reply.Reply = OK
	} else if args.ConfigNum == kv.nowConfig.Num && kv.ShardCollection[args.ShardNum].State == Regular {
		reply.Reply = OK
		DPrintf("[Peer %d] receive duplicate Push RPC, my config: %+v, shard %d state %d, args %+v\n", kv.me, kv.nowConfig, args.ShardNum, kv.ShardCollection[args.ShardNum].State, args)
	} else {
		DPrintf("[Peer %d] receive outdated Push RPC, my config: %+v, shard %d state %d, args %+v\n", kv.me, kv.nowConfig, args.ShardNum, kv.ShardCollection[args.ShardNum].State, args)
	}
	kv.unlock("PushShard")
}

func (kv *ShardKV) cleanShardHandle(op *Op) {
	kv.lock("cleanShardHandle")
	defer kv.unlock("cleanShardHandle")
	kv.ShardCollection[op.CleanShardNum] = &ShardData{
		VersionMap: make(map[int]int64),
		KvMap:      make(map[string]string),
		State:      Regular,
	}
}

func (kv *ShardKV) receiveShardHandle(op *Op) {
	kv.lock("receiveShardHandle")
	defer kv.unlock("receiveShardHandle")
	var copyShard = op.ReceiveShard.deepCopy()
	kv.ShardCollection[op.ReceiveShardNum] = &copyShard
}
