package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


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

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	versionMap	map[int]int64	// clientId->its version
	kvMap	map[string]string

	// for handling Op
	opWaitChs map[int]chan OpHandlerReply // handling commitIndex -> chan for wait reply
	waitOpMap map[int]Op                  // handling commitIndex -> Op(insert by Start())

	// lock debug
	lockName  string
	lockStart time.Time
	lockEnd   time.Time
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
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

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.opWaitChs = make(map[int]chan OpHandlerReply)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.opWaitChs = make(map[int]chan OpHandlerReply)
	kv.waitOpMap = make(map[int]Op)
	kv.versionMap = make(map[int]int64)

	go kv.applyChHandler()
	return kv

}

// 注意applyCh收到的消息其实是raft确认保存的消息，我们在使用Start方法发送到raft集群后，等待作为的leader
// 的server在applyCh中返回确认该Op被Commit的消息
// 所有Follower也会维护一个kvMap，因为所有人都会收到相同的ApplyMsg(仅限写的Op,Get的Op Follower直接忽略)
// 类似Raft的主从复制，不知道从机有没有资格处理Get，但是我认为不行，因为从机无法确定自己的Log是最新的，正确的
// 做法应该是读和写都由Leader完成
func (kv *KVServer) applyChHandler() {
	for !kv.killed(){
		applyMsg := <- kv.applyCh
		if applyMsg.CommandValid {	//是Op类型的applyMsg
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
						DPrintf("[Peer %d]receive duplicate op:%+v", kv.me, op)
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
					DPrintf("[Peer %d] leader handling Op reply of op %+v\n", kv.me, op)
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
						DPrintf("[Peer %d] sent ErrLeader to all ch and close all ch\n", kv.me)
						for index, ch := range kv.opWaitChs {
							ch <- OpHandlerReply{ErrWrongLeader, ""} // 理论上这里不会被阻塞，Op发起方应该在等待其返回
							close(ch)
							delete(kv.opWaitChs, index)
							delete(kv.waitOpMap, index)
						}
					}
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
					DPrintf("[Peer %d] receive new Leader ApplyMsg, close all ch\n", kv.me)
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
			// todo handle snapshot Lab_3B
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

func (kv *KVServer) lock(name string) {
	kv.mu.Lock()
	kv.lockStart = time.Now()
	kv.lockName = name
}

func (kv *KVServer) unlock(name string) {
	kv.lockEnd = time.Now()
	kv.lockName = ""
	duration := kv.lockEnd.Sub(kv.lockStart)
	if duration > 10 * time.Millisecond {
		fmt.Printf("long lock: %s, time: %s\n", name, duration)
	}
	kv.mu.Unlock()
}
