package shardkv

import "6.824/shardctrler"

func (kv *ShardKV) updateConfig() {
	kv.PullConfigTimer.Reset(PullConfigTimeout)

	canUpdate := true
	kv.lock("updateConfig")
	for _, shard := range kv.ShardCollection {
		// 如果当前有shard正在处于非Regular状态，说明上一次的状态迁移尚未完成，都为Regular后才能确定完成
		if shard.State != Regular {
			canUpdate = false
			break
		}
	}
	nowConfigVersion := kv.nowConfig.Num
	kv.unlock("updateConfig")
	if canUpdate {
		nextConfig := kv.ctrlerClient.Query(nowConfigVersion + 1)
		if nextConfig.Num == nowConfigVersion + 1 {
			updateCfgOp := Op{
				Type:      "UpdateConfig",
				NewConfig: nextConfig,
			}
			kv.rf.Start(updateCfgOp)
		}
	}
}

func (kv *ShardKV) updateConfigHandle(nextConfig *shardctrler.Config)  {
	kv.lock("updateConfigHandle")
	defer kv.unlock("updateConfigHandle")
	if nextConfig.Num == kv.nowConfig.Num + 1 {
		kv.updateShardState(nextConfig)	// 更新分片state
		kv.prevConfig = kv.nowConfig
		kv.nowConfig = *nextConfig
	}
}

func (kv *ShardKV) updateShardState(nextConfig *shardctrler.Config)  {
	// already have lock
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.nowConfig.Shards[i] != kv.gid && nextConfig.Shards[i] == kv.gid {
			// 新增的shard
			gid := kv.nowConfig.Shards[i]
			if gid != 0 {
				kv.ShardCollection[i].State = WaitPush
			}
		}
		if kv.nowConfig.Shards[i] == kv.gid && nextConfig.Shards[i] != kv.gid {
			// 本轮不属于自己的shard
			gid := nextConfig.Shards[i]
			if gid != 0 {
				kv.ShardCollection[i].State = Pushing
			}
		}
	}
}
