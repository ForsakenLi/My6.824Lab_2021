package shardkv

import (
	"6.824/shardctrler"
	"sync"
	"time"
)

type PushShardArgs struct {
	ConfigNum int
	//Shard     ShardData
	//ShardNum  int
	Shards 		map[int]ShardData
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
	shardByGid := kv.getPushingShardByGid()
	for gid, shardIds := range shardByGid {
		wg.Add(1)
		go func (GID int, shardIDs []int) {
			defer wg.Done()
			kv.sendPushRPC(GID, shardIDs)
		}(gid, shardIds)
	}
	//for shardNum, gid := range kv.nowConfig.Shards { // Push的目标Gid需要根据上一个版本的Config找到
	//	if kv.ShardCollection[shardNum].State == Pushing {
	//		wg.Add(1)
	//		var copyShard = kv.ShardCollection[shardNum].deepCopy()
	//		copyShard.State = Regular //发送过去的应该是Regular，对方确认无误后直接覆盖为WaitPush状态的Shard
	//		args := PushShardArgs{
	//			ConfigNum: kv.nowConfig.Num, // 发送的配置版本号需要为目前版本的Config
	//			Shard:     copyShard,
	//			ShardNum:  shardNum,
	//		}
	//		go func(args *PushShardArgs, servers []string, shardNum int) {
	//			defer wg.Done()
	//			for _, serverName := range servers {
	//				server := kv.make_end(serverName)
	//				var reply PushShardReply
	//				if server.Call("ShardKV.PushShard", args, &reply) && reply.Reply == "OK" {
	//					DPrintf("[Peer %d Group %d] Push Shard success %+v", kv.me, kv.gid, args)
	//					cleanOp := Op{
	//						Type:          "CleanShard",
	//						CleanShardNum: shardNum,
	//						ConfigNum:     args.ConfigNum,
	//					}
	//					kv.rf.Start(cleanOp)
	//				}
	//			}
	//		}(&args, kv.nowConfig.Groups[gid], shardNum) //最后这个shardNum一定要传进去，否则会导致协程内读取外部的for-each迭代变量
	//	}
	//}
	kv.unlock("DataTransfer")
	wg.Wait()
	kv.TransferTimer.Reset(DataTransferTimeout)
}

// 如果shard的状态是Pushing，则将其ID打包为 Gid -> []ShardNum 的结构，便于在一次RPC中发送所有Shard
func (kv *ShardKV) getPushingShardByGid() map[int][]int {
	res := make(map[int][]int)
	for shardNum := 0; shardNum < shardctrler.NShards; shardNum++ {
		if kv.ShardCollection[shardNum].State == Pushing {
			gid := kv.nowConfig.Shards[shardNum]
			res[gid] = append(res[gid], shardNum)
		}
	}
	return res
}

func (kv *ShardKV) sendPushRPC (GID int, shardIDs []int) {
	copyShards := make(map[int]ShardData)
	kv.lock("CopyShardToPush")
	for _, shardNum := range shardIDs {
		shardDataCopy := kv.ShardCollection[shardNum].deepCopy()
		shardDataCopy.State = Regular
		copyShards[shardNum] = shardDataCopy
	}
	configNum := kv.nowConfig.Num
	pushGroup := kv.nowConfig.Groups[GID]
	kv.unlock("CopyShardToPush")
	args := PushShardArgs{
		ConfigNum: configNum,
		Shards:    copyShards,
	}

	for _, serverName := range pushGroup {
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			return
		}
		var reply PushShardReply
		client := kv.make_end(serverName)
		ok := client.Call("ShardKV.PushShard", &args, &reply)
		if ok && reply.Reply == OK {
			kv.lock("ConfigNumCheck")
			nowConfigNum := kv.nowConfig.Num
			kv.unlock("ConfigNumCheck")
			if nowConfigNum == configNum {
				cleanOp := Op{
					Type:          "CleanShard",
					CleanShardNum: shardIDs,
					ConfigNum:     configNum,
				}
				kv.rf.Start(cleanOp)
			}
		}
	}
}

// PushShard 需要检查args的config.Num和当前的是否对应，并check分片是否处于WaitPush状态，妥当后方可确认接收并返回OK
func (kv *ShardKV) PushShard(args *PushShardArgs, reply *PushShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	kv.lock("PushShard")
	myConfigNum := kv.nowConfig.Num
	kv.unlock("PushShard")
	if myConfigNum < args.ConfigNum {
		DPrintf("[Peer %d, Group %d] receive advanced config, now config %d, args %+v\n", kv.me, kv.gid, kv.nowConfig.Num, args)
		return
	}

	shardOp := Op{
		Type:            "ReceiveShards",
		ReceiveShardMap: args.Shards,
		ConfigNum:       args.ConfigNum,
	}

	handlerReply := kv.startAndApply(shardOp)
	reply.Reply = string(handlerReply.Err)
}

func (kv *ShardKV) startAndApply(command interface{}) OpHandlerReply {
	lastIndex, currentTerm, isLeader := kv.rf.Start(command)
	if !isLeader {
		return OpHandlerReply{
			Err: ErrWrongLeader,
		}
	}

	doneChan := kv.getMsgChan(lastIndex)
	select {
	case <-time.After(200 * time.Millisecond):
		return OpHandlerReply{
			Err: "Timeout",
		}
	case handlerReply := <-doneChan:
		if handlerReply.Err != OK {
			return handlerReply
		}
		term, isLeader := kv.rf.GetState()
		if !isLeader || currentTerm != term {
			return OpHandlerReply{
				Err: ErrWrongLeader,
			}
		}
	}
	return OpHandlerReply{
		Err: OK,
	}
}

func (kv *ShardKV) getMsgChan(index int) chan OpHandlerReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	doneChan, ok := kv.opWaitChs[index]
	if !ok {
		doneChan = make(chan OpHandlerReply, 1)
		kv.opWaitChs[index] = doneChan
	}
	return doneChan
}

func (kv *ShardKV) cleanShardHandle(op *Op) {
	kv.lock("cleanShardHandle")
	defer kv.unlock("cleanShardHandle")
	//if op.ConfigNum == kv.nowConfig.Num && kv.ShardCollection[op.CleanShardNum].State == Pushing {
	//	kv.ShardCollection[op.CleanShardNum] = &ShardData{
	//		VersionMap: make(map[int]int64),
	//		KvMap:      make(map[string]string),
	//		State:      Regular,
	//	}
	//}
	if op.ConfigNum != kv.nowConfig.Num {
		return
	}
	for _, shardNum := range op.CleanShardNum {
		kv.ShardCollection[shardNum] = &ShardData{
			VersionMap: make(map[int]int64),
			KvMap:      make(map[string]string),
			State:      Regular,
		}
	}
}

func (kv *ShardKV) receiveShardHandle(op *Op) {
	kv.lock("receiveShardHandle")
	defer kv.unlock("receiveShardHandle")
	//if op.ConfigNum == kv.nowConfig.Num && kv.ShardCollection[op.ReceiveShardNum].State == WaitPush {
	//	var copyShard = op.ReceiveShards.deepCopy()
	//	kv.ShardCollection[op.ReceiveShardNum] = &copyShard
	//}
	if op.ConfigNum != kv.nowConfig.Num {
		return
	}
	for shardNum, shardData := range op.ReceiveShardMap {
		if kv.ShardCollection[shardNum].State == WaitPush {
			copyShard := shardData.deepCopy()
			kv.ShardCollection[shardNum] = &copyShard
		}
	}
}
