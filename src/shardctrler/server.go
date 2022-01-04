package shardctrler

import (
	"6.824/raft"
	"fmt"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead    int32    // set by Kill()
	configs []Config // indexed by config num

	// for handling Op
	opWaitChs map[int]chan interface{} // handling commitIndex -> chan for wait reply
	waitOpMap map[int]Op               // handling commitIndex -> Op(insert by Start())
	versionMap	map[int]int64

	// lock debug
	lockName  string
	lockStart time.Time
	lockEnd   time.Time
}

type Op struct {
	// Your data here.
	Type      string
	ID        int
	Version   int64
	ArgsJoin  JoinArgs
	ArgsLeave LeaveArgs
	ArgsMove  MoveArgs
	ArgsQuery QueryArgs
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.lock("Join")

	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		sc.unlock("Join")
		return
	}

	ver, ok := sc.versionMap[args.ClientID]
	if !ok {
		sc.versionMap[args.ClientID] = -1	// 目前还不能直接改为arg.Version，需要等待applyCh返回
	} else if args.Version <= ver {
		// 重复请求
		reply.Err = OK
		return
	}

	joinOp := Op{
		Type:     "Join",
		ID:       args.ClientID,
		Version:  args.Version,
		ArgsJoin: *args,
	}

	index, _, _ := sc.rf.Start(joinOp)
	ch := make(chan interface{})
	sc.waitOpMap[index] = joinOp
	sc.opWaitChs[index] = ch
	sc.unlock("Join")

	opHandlerReply := <- ch

	if joinReply, ok := opHandlerReply.(JoinReply); ok {
		reply = &joinReply
	} else {
		panic("get error type reply")
	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.lock("Leave")

	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		sc.unlock("Leave")
		return
	}

	ver, ok := sc.versionMap[args.ClientID]
	if !ok {
		sc.versionMap[args.ClientID] = -1	// 目前还不能直接改为arg.Version，需要等待applyCh返回
	} else if args.Version <= ver {
		// 重复请求
		reply.Err = OK
		return
	}

	leaveOp := Op{
		Type:     "Leave",
		ID:       args.ClientID,
		Version:  args.Version,
		ArgsLeave: *args,
	}

	index, _, _ := sc.rf.Start(leaveOp)
	ch := make(chan interface{})
	sc.waitOpMap[index] = leaveOp
	sc.opWaitChs[index] = ch
	sc.unlock("Leave")

	opHandlerReply := <- ch

	if leaveReply, ok := opHandlerReply.(LeaveReply); ok {
		reply = &leaveReply
	} else {
		panic("get error type reply")
	}

}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.lock("Move")

	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		sc.unlock("Move")
		return
	}

	ver, ok := sc.versionMap[args.ClientID]
	if !ok {
		sc.versionMap[args.ClientID] = -1	// 目前还不能直接改为arg.Version，需要等待applyCh返回
	} else if args.Version <= ver {
		// 重复请求
		reply.Err = OK
		return
	}

	moveOp := Op{
		Type:     "Move",
		ID:       args.ClientID,
		Version:  args.Version,
		ArgsMove: *args,
	}

	index, _, _ := sc.rf.Start(moveOp)
	ch := make(chan interface{})
	sc.waitOpMap[index] = moveOp
	sc.opWaitChs[index] = ch
	sc.unlock("Move")

	opHandlerReply := <- ch

	if moveReply, ok := opHandlerReply.(MoveReply); ok {
		reply = &moveReply
	} else {
		panic("get error type reply")
	}

}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.lock("Query")

	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		sc.unlock("Query")
		return
	}

	ver, ok := sc.versionMap[args.ClientID]
	if !ok {
		sc.versionMap[args.ClientID] = -1	// 目前还不能直接改为arg.Version，需要等待applyCh返回
	} else if args.Version <= ver {
		// 重复请求
		reply.Err = OK
		return
	}

	queryOp := Op{
		Type:     "Query",
		ID:       args.ClientID,
		Version:  args.Version,
		ArgsQuery: *args,
	}

	index, _, _ := sc.rf.Start(queryOp)
	ch := make(chan interface{})
	sc.waitOpMap[index] = queryOp
	sc.opWaitChs[index] = ch
	sc.unlock("Query")

	opHandlerReply := <- ch

	if queryReply, ok := opHandlerReply.(QueryReply); ok {
		reply = &queryReply
	} else {
		panic("get error type reply")
	}

}

// Kill
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// Raft needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// StartServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	go sc.applyChHandler()
	return sc
}

func (sc *ShardCtrler) applyChHandler() {
	for !sc.killed() {
		applyMsg := <-sc.applyCh
		if applyMsg.CommandValid { //是Op类型的applyMsg
			if applyMsg.Command == nil {
				continue
			}
			op := applyMsg.Command.(Op)

		} else if applyMsg.SnapshotValid {
			// 无需快照
		} else {
			// I win the election, send a Op to let the old leader know, because we have
			// to notify it to close its opWaitChs(it have lost the qualification)
			sc.lock("LeaderChange")
			newOp := Op{
				Type: "LeaderChange",
				ID:   sc.me, // 如果我收到这条消息，则不需要做推出chan的处理
			}
			sc.rf.Start(newOp)
			sc.unlock("LeaderChange")
		}
	}
}

func (sc *ShardCtrler) lock(name string) {
	sc.mu.Lock()
	sc.lockStart = time.Now()
	sc.lockName = name
}

func (sc *ShardCtrler) unlock(name string) {
	sc.lockEnd = time.Now()
	sc.lockName = ""
	duration := sc.lockEnd.Sub(sc.lockStart)
	if duration > 10*time.Millisecond {
		fmt.Printf("long lock: %s, time: %s\n", name, duration)
	}
	sc.mu.Unlock()
}
