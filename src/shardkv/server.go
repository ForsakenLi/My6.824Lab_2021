package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

const (
	PullConfigTimeout = time.Millisecond * 200
	DataTransferTimeout = time.Millisecond * 100
	Debug = true
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	maxraftstate int   // snapshot if log grows this big
	dead         int32 // set by Kill()

	// shardctrler server and client
	ctrlers      []*labrpc.ClientEnd
	ctrlerClient *shardctrler.Clerk

	//versionMap map[int]int64 // clientId->its version
	//kvMap      map[string]string
	ShardCollection	[shardctrler.NShards]*ShardData

	// for handling Op
	opWaitChs map[int]chan OpHandlerReply // handling commitIndex -> chan for wait reply
	waitOpMap map[int]Op                  // handling commitIndex -> Op(insert by Start())

	// lock debug
	lockName  string
	lockStart time.Time
	lockEnd   time.Time

	persister *raft.Persister

	// config
	PullConfigTimer *time.Timer
	nowConfig	shardctrler.Config

	// shard transfer
	TransferTimer *time.Timer

}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.lock("Get")
	defer kv.unlock("Get")

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	newOp := Op{
		Type: "Get",
		Key:  args.Key,
	}
	index, _, _ := kv.rf.Start(newOp)

	ch := make(chan OpHandlerReply)
	kv.waitOpMap[index] = newOp
	kv.opWaitChs[index] = ch
	kv.unlock("Get")
	opHandlerReply := <-ch
	kv.lock("Get")

	reply.Err = opHandlerReply.Err
	reply.Value = opHandlerReply.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.lock("PutAppend")
	defer kv.unlock("PutAppend")

	// 只有leader才能有效的发送Start
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 需要通过版本号来确认这个请求是否是重复发送的，如果一个请求被确认append成功，则版本号才会被修改为args中的
	shardNum := key2shard(args.Key)
	ver, _ := kv.ShardCollection[shardNum].VersionMap[args.ClientID]
	//if !ok {
	//	kv.ShardCollection[shardNum].VersionMap[args.ClientID] = -1 // 目前还不能直接改为arg.Version，需要等待applyCh返回
	//} else
	if args.Version <= ver {
		// 重复请求
		reply.Err = OK
		return
	}

	newOp := Op{
		Type:    args.Op,
		Key:     args.Key,
		Value:   args.Value,
		Version: args.Version,
		ID:      args.ClientID,
	}

	index, _, _ := kv.rf.Start(newOp)

	// 用Channel来和处理applyCh的Handler通信，等待结果完成后即可给客户端返回确认

	ch := make(chan OpHandlerReply)
	kv.waitOpMap[index] = newOp
	kv.opWaitChs[index] = ch
	kv.unlock("PutAppend")
	opHandlerReply := <-ch
	kv.lock("PutAppend")

	// create reply
	reply.Err = opHandlerReply.Err
}

// Kill
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartServer
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(PushShardArgs{})
	labgob.Register(PushShardReply{})
	labgob.Register(ShardData{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.persister = persister

	// Your initialization code here.
	kv.ctrlerClient = shardctrler.MakeClerk(ctrlers)
	kv.opWaitChs = make(map[int]chan OpHandlerReply)
	kv.waitOpMap = make(map[int]Op)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.ShardCollection[i] = &ShardData{
			VersionMap: make(map[int]int64),
			KvMap:      make(map[string]string),
			State:      Regular,
		}
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.PullConfigTimer = time.NewTimer(PullConfigTimeout)
	// 配置更新协程
	go func() {
		for !kv.killed() {
			select {
			case <-kv.PullConfigTimer.C:
				kv.updateConfig()
			}
		}
	}()

	kv.TransferTimer = time.NewTimer(DataTransferTimeout)
	// 数据迁移&清理协程
	go func() {
		for !kv.killed() {
			select {
			case <-kv.TransferTimer.C:
				kv.dataTransfer()
			}
		}
	}()

	kv.readPersist(persister.ReadSnapshot())

	// 日志处理协程
	go kv.applyChHandler()
	return kv
}

func (kv *ShardKV) lock(name string) {
	kv.mu.Lock()
	kv.lockStart = time.Now()
	kv.lockName = name
}

func (kv *ShardKV) unlock(name string) {
	kv.lockEnd = time.Now()
	kv.lockName = ""
	duration := kv.lockEnd.Sub(kv.lockStart)
	if duration > 10*time.Millisecond {
		DPrintf("long lock: %s, time: %s\n", name, duration)
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) getPersistStateBytes() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.ShardCollection)
	//e.Encode(kv.prevConfig)
	e.Encode(kv.nowConfig)
	data := w.Bytes()
	return data
}

func (kv *ShardKV) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var ShardCollection [shardctrler.NShards]*ShardData
	var nowConfig shardctrler.Config
	if d.Decode(&ShardCollection) != nil ||
		d.Decode(&nowConfig) != nil {
		DPrintf("server %d readPersist(kv): decode error!", kv.me)
	} else {
		kv.ShardCollection = ShardCollection
		kv.nowConfig = nowConfig
		//kv.prevConfig = prevConfig
	}
}

func (kv *ShardKV) applyChHandler() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			kv.CheckPersist(applyMsg)
			if applyMsg.Command == nil {
				continue
			}
			op := applyMsg.Command.(Op)
			if op.Type == "UpdateConfig" {
				kv.updateConfigHandle(&op.NewConfig)
				continue
			}
			if op.Type == "LeaderChange" {
				kv.LeaderChangeHandle(op)
				continue
			}
			if op.Type == "CleanShard" {
				kv.cleanShardHandle(&op)
				continue
			}
			if op.Type == "ReceiveShards" {
				kv.receiveShardHandle(&op)
				continue
			}
			if isRegularOp(&op) {
				kv.lock("RegularOp")
				var handlerReply OpHandlerReply
				shardNum := key2shard(op.Key)
				if kv.nowConfig.Shards[shardNum] != kv.gid {
					handlerReply.Err = ErrWrongGroup
				} else {
					//DPrintf("[Peer %d] execute op: %+v\n", kv.me, op)
					handlerReply = kv.ShardCollection[shardNum].ModifyKV(op)
				}
				startOp := kv.waitOpMap[applyMsg.CommandIndex]
				waitCh, existCh := kv.opWaitChs[applyMsg.CommandIndex]
				//kv.unlock()
				if startOp.Version == op.Version && existCh {
					// sent to wait chan
					DPrintf("[Peer %d, Group %d] sent handler reply: %+v\n", kv.me, kv.gid, handlerReply)
					waitCh <- handlerReply
					// close channel and delete the index from waitMap
					close(waitCh)
					//kv.lock()
					delete(kv.opWaitChs, applyMsg.CommandIndex)
					delete(kv.waitOpMap, applyMsg.CommandIndex)
					//kv.unlock()
				} else {
					// sent ErrLeader to all ch and close all ch
					//kv.lock()
					if len(kv.opWaitChs) > 0 {
						DPrintf("[Peer %d Group %d] sent ErrLeader to all ch and close all ch\n", kv.me, kv.gid)
						for index, ch := range kv.opWaitChs {
							ch <- OpHandlerReply{ErrWrongLeader, ""} // 理论上这里不会被阻塞，Op发起方应该在等待其返回
							close(ch)
							delete(kv.opWaitChs, index)
							delete(kv.waitOpMap, index)
						}
					}
				}

				kv.unlock("RegularOp")
			}
		} else if applyMsg.SnapshotValid {
			if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
				kv.readPersist(applyMsg.Snapshot)
			}
		} else {
			// applyMsg.SnapshotValid == false && applyMsg.CommandValid == false
			kv.LeaderChange()
		}

	}
}

func (kv *ShardKV) CheckPersist(applyMsg raft.ApplyMsg) {
	kv.lock("CheckPersist")
	if kv.maxraftstate != -1 && float32(kv.persister.RaftStateSize()) > float32(kv.maxraftstate)*0.9 {
		kv.rf.Snapshot(applyMsg.CommandIndex, kv.getPersistStateBytes())
	}
	kv.unlock("CheckPersist")
}

func (kv *ShardKV) LeaderChangeHandle(op Op) {
	//kv.lock("LeaderChangeHandle")
	//if op.ID == kv.me { // ignore this Op
	//	kv.unlock("LeaderChangeHandle")
	//	return
	//}
	//if len(kv.opWaitChs) > 0 {
	//	DPrintf("[Peer %d Group %d] receive new Leader ApplyMsg, close all ch\n", kv.me, kv.gid)
	//}
	//for index, ch := range kv.opWaitChs {
	//	ch <- OpHandlerReply{ErrWrongLeader, ""}
	//	close(ch)
	//	delete(kv.opWaitChs, index)
	//	delete(kv.waitOpMap, index)
	//}
	//kv.unlock("LeaderChangeHandle")
}

func (kv *ShardKV) LeaderChange() {
	//kv.lock("LeaderChange")
	//newOp := Op{
	//	Type: "LeaderChange",
	//	ID:   kv.me, // 如果我收到这条消息，则不需要做推出chan的处理
	//}
	//kv.rf.Start(newOp)
	//kv.unlock("LeaderChange")
}
