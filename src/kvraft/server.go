package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"go/ast"
	"log"
	"sync"
	"sync/atomic"
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
	opHandlerChs	map[int]chan OpHandlerReply	// handling index -> chan for wait reply

}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

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

	kv.opHandlerChs[index] = make(chan OpHandlerReply)
	kv.mu.Unlock()
	opHandlerReply := <- kv.opHandlerChs[index]
	kv.mu.Lock()

	reply.Err = opHandlerReply.Err
	reply.Value = opHandlerReply.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

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

	kv.opHandlerChs[index] = make(chan OpHandlerReply)
	kv.mu.Unlock()
	opHandlerReply := <- kv.opHandlerChs[index]
	kv.mu.Lock()

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
	kv.opHandlerChs = make(map[int]chan OpHandlerReply)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
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

		} else if applyMsg.SnapshotValid {
			// todo handle snapshot
		} else {
			// I win the election, send a Op to let the old leader know, because we have
			// to notify it to close its opHandlerChs(it have lost the qualification)

		}
	}
}
