package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"crypto/rand"
	"fmt"
	"math/big"
	rand2 "math/rand"
	"time"
)

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm      *shardctrler.Clerk
	config  shardctrler.Config
	makeEnd func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId        int
	requestId       int64
	gid2LeaderIdMap map[int]int // gid -> leaderId
}

func (ck *Clerk) num() string {
	return fmt.Sprintf("%v-%v", ck.clientId, ck.requestId)
}

// MakeClerk
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *Clerk {
	rand2.Seed(time.Now().UnixNano())
	ck := &Clerk{
		sm:              shardctrler.MakeClerk(ctrlers),
		config:          shardctrler.Config{},
		makeEnd:         makeEnd,
		clientId:        rand2.Int(),
		gid2LeaderIdMap: make(map[int]int),
	}
	// You'll have to add code here.
	return ck
}

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	ck.requestId++
	args := GetArgs{
		Key:       key,
		//ClientId:  ck.clientId,
		//RequestId: ck.requestId,
	}

	/**
	1. 先 key2shard，拿到 key 对应的 shard
	2. shard -> gid，映射拿到对应负责该 shard 的 gid
	3. gid -> servers，在一个 raft 副本组内逐个 server 尝试，失败直到拿到 leader
	4. 请求 leader
	*/

	shard := key2shard(key)
	// 外层循环：找到一个 group
	for {
		// 1. 能从配置中拿到有效的 gid
		if gid := ck.config.Shards[shard]; gid != 0 {
			leaderId := ck.gid2LeaderIdMap[gid]
			oldLeaderId := leaderId
			// 2. 继续从配置中用 gid 拿到有效的 group 信息
			if group, ok := ck.config.Groups[gid]; ok {
				// 内层循环：在一个 group 里找 leader
				for {
					var reply GetReply
					serverName := group[leaderId]
					ok := ck.sendGet(serverName, &args, &reply)
					if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
						ck.gid2LeaderIdMap[gid] = leaderId // 记录下该 gid 对应的 leader
						return reply.Value
					}
					if ok && reply.Err == ErrWrongGroup {
						break
					}
					// ... not ok, or ErrWrongLeader or timeout
					leaderId = (leaderId + 1) % len(group)
					// 请求了一个周期都不行，再尝试拉取最新配置重新请求
					if leaderId == oldLeaderId {
						break
					}
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

// PutAppend
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.requestId++
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientID:  ck.clientId,
		Version: ck.requestId,
	}

	shard := key2shard(key)
	// 外层循环：找到一个 group
	for {
		// 1. 能从配置中拿到有效的 gid
		if gid := ck.config.Shards[shard]; gid != 0 {
			leaderId := ck.gid2LeaderIdMap[gid]
			oldLeaderId := leaderId
			// 2. 继续从配置中用 gid 拿到有效的 group 信息
			if group, ok := ck.config.Groups[gid]; ok {
				// 内层循环：在一个 group 里找 leader
				for {
					var reply PutAppendReply
					serverName := group[leaderId]
					ok := ck.sendPutAppend(serverName, &args, &reply)
					if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
						ck.gid2LeaderIdMap[gid] = leaderId // 记录下该 gid 对应的 leader
						return
					}
					if ok && reply.Err == ErrWrongGroup {
						break
					}
					// ... not ok, or ErrWrongLeader or timeout
					leaderId = (leaderId + 1) % len(group)
					// 请求了一个周期都不行，再尝试拉取最新配置重新请求
					if leaderId == oldLeaderId {
						break
					}
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		//DPrintf("[PutAppend] %v %v %v", ck.num(), ck.config.Num, ck.config.Shards)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) sendGet(serverName string, args *GetArgs, reply *GetReply) bool {
	srv := ck.makeEnd(serverName)
	return srv.Call("ShardKV.Get", args, reply)
}

func (ck *Clerk) sendPutAppend(serverName string, args *PutAppendArgs, reply *PutAppendReply) bool {
	srv := ck.makeEnd(serverName)
	return srv.Call("ShardKV.PutAppend", args, reply)
}