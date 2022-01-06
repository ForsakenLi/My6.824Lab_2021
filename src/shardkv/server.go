package shardkv


import (
	"6.824/labrpc"
	"bytes"
	"fmt"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type    string
	Key     string
	Value   string
	Version int64
	ID      int
}

type OpHandlerReply struct {
	Err   Err
	Value string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	dead    int32 // set by Kill()

	// Your definitions here.
	versionMap	map[int]int64	// clientId->its version
	kvMap	map[string]string

	// for handling Op
	opWaitChs map[int]chan OpHandlerReply // handling commitIndex -> chan for wait reply
	waitOpMap map[int]Op                  // handling commitIndex -> Op(insert by Start())

	// lock debug
	lockName  string
	lockStart time.Time
	lockEnd   time.Time

	persister *raft.Persister
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
		Key: args.Key,
	}
	index, _, _ := kv.rf.Start(newOp)

	ch := make(chan OpHandlerReply)
	kv.waitOpMap[index] = newOp
	kv.opWaitChs[index] = ch
	kv.unlock("Get")
	opHandlerReply := <- ch
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

	ver, ok := kv.versionMap[args.ClientID]
	if !ok {
		kv.versionMap[args.ClientID] = -1	// 目前还不能直接改为arg.Version，需要等待applyCh返回
	} else if args.Version <= ver {
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
	opHandlerReply := <- ch
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

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.persister = persister

	// Your initialization code here.
	kv.kvMap = make(map[string]string)
	kv.opWaitChs = make(map[int]chan OpHandlerReply)
	kv.waitOpMap = make(map[int]Op)
	kv.versionMap = make(map[int]int64)

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.readPersist(persister.ReadSnapshot())

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
	if duration > 10 * time.Millisecond {
		fmt.Printf("long lock: %s, time: %s\n", name, duration)
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) getPersistStateBytes() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.versionMap)
	e.Encode(kv.kvMap)
	data := w.Bytes()
	return data
}

func (kv *ShardKV) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var versionMap map[int]int64
	var kvMap map[string]string
	if d.Decode(&versionMap) != nil ||
		d.Decode(&kvMap) != nil {
		fmt.Printf("server %d readPersist(kv): decode error!", kv.me)
	} else {
		kv.versionMap = versionMap
		kv.kvMap = kvMap
	}
}

func (kv *ShardKV) applyChHandler() {
	for !kv.killed(){
		applyMsg := <- kv.applyCh
		if applyMsg.CommandValid {	//是Op类型的applyMsg
			if applyMsg.Command == nil {
				continue
			}
			op := applyMsg.Command.(Op)
			// 对于Leader，Get/Put/Append都需要找到对应这个Op的channel，以返回给接收方确认这个
			// 请求已经被处理完成
			// 在这里还有Leader身份判断的问题，如果先前Start写入这个index的Op和ApplyMsg返回的Op
			// 有所不同，那么说明之前这个cmd我调用Start时没有被写入成功, 其实就是当时我已经不是Raft
			// 多数派的Leader了，我的LogEntry没有被多数派认可，因此这里ApplyMsg返回的Op和我之前
			// Start进去的Op不同，此时需要做返回ErrWrongLeader并关闭channel的操作。因此需要
			// 验证返回的Op和之前Start写入的Op是否相同
			if op.Type != "LeaderChange" {
				kv.lock("RegularOp")
				// check version to avoid duplicate Op, and update version (everyone)
				isDupOp := false
				if op.Type != "Get" {
					_, exist := kv.versionMap[op.ID]
					if !exist {
						// new Op
						kv.versionMap[op.ID] = -1
					}
					if op.Version <= kv.versionMap[op.ID] {
						fmt.Printf("[Peer %d]receive duplicate op:%+v", kv.me, op)
						isDupOp = true
					}
				}
				// update kvMap (everyone)
				if !isDupOp {
					kv.versionMap[op.ID] = op.Version
					if op.Type == "Put" {
						kv.kvMap[op.Key] = op.Value
					} else if op.Type == "Append" {
						if origin, ok := kv.kvMap[op.Key]; ok {
							kv.kvMap[op.Key] = origin + op.Value
						} else {
							kv.kvMap[op.Key] = op.Value
						}
					}
				}
				// 比对waitOpMap确定Leader身份
				startOp := kv.waitOpMap[applyMsg.CommandIndex]
				waitCh, existCh := kv.opWaitChs[applyMsg.CommandIndex]
				//kv.unlock()
				if startOp == op && existCh {
					fmt.Printf("[Peer %d] leader handling Op reply of op %+v\n", kv.me, op)
					var handlerReply OpHandlerReply
					if op.Type == "Get" {
						//kv.lock()
						value, existKey := kv.kvMap[op.Key]
						//kv.unlock()
						if existKey {
							handlerReply.Value = value
							handlerReply.Err = OK
						} else {
							handlerReply.Err = ErrNoKey
						}
					} else {
						//op.Type == "Put"/"Append"
						handlerReply.Err = OK
					}
					// sent to wait chan
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
						fmt.Printf("[Peer %d] sent ErrLeader to all ch and close all ch\n", kv.me)
						for index, ch := range kv.opWaitChs {
							ch <- OpHandlerReply{ErrWrongLeader, ""} // 理论上这里不会被阻塞，Op发起方应该在等待其返回
							close(ch)
							delete(kv.opWaitChs, index)
							delete(kv.waitOpMap, index)
						}
					}
				}
				if kv.maxraftstate != -1 && float32(kv.persister.RaftStateSize()) > float32(kv.maxraftstate)*0.9 {
					kv.rf.Snapshot(applyMsg.CommandIndex, kv.getPersistStateBytes())
				}
				kv.unlock("RegularOp")
			} else {
				// op.Type == "LeaderChange"
				kv.lock("LeaderChangeHandle")
				if op.ID == kv.me {	// ignore this Op
					kv.unlock("LeaderChangeHandle")
					continue
				}
				if len(kv.opWaitChs) > 0 {
					fmt.Printf("[Peer %d] receive new Leader ApplyMsg, close all ch\n", kv.me)
				}
				//kv.unlock()
				for index, ch := range kv.opWaitChs {
					ch <- OpHandlerReply{ErrWrongLeader, ""}
					close(ch)
					//kv.lock()
					delete(kv.opWaitChs, index)
					delete(kv.waitOpMap, index)
					//kv.unlock()
				}
				kv.unlock("LeaderChangeHandle")
			}
		} else if applyMsg.SnapshotValid {
			if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
				kv.readPersist(applyMsg.Snapshot)
			}
		} else {
			// I win the election, send a Op to let the old leader know, because we have
			// to notify it to close its opWaitChs(it have lost the qualification)
			kv.lock("LeaderChange")
			newOp := Op{
				Type: "LeaderChange",
				ID: kv.me,	// 如果我收到这条消息，则不需要做推出chan的处理
			}
			kv.rf.Start(newOp)
			kv.unlock("LeaderChange")
		}
	}
}
