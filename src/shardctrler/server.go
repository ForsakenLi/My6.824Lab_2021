package shardctrler

import (
	"6.824/raft"
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"math"
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
	opWaitChs  map[int]chan interface{} // handling commitIndex -> chan for wait reply
	waitOpMap  map[int]Op               // handling commitIndex -> Op(insert by Start())
	versionMap map[int]int64

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
		sc.versionMap[args.ClientID] = -1 // 目前还不能直接改为arg.Version，需要等待applyCh返回
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

	opHandlerReply := <-ch

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
		sc.versionMap[args.ClientID] = -1 // 目前还不能直接改为arg.Version，需要等待applyCh返回
	} else if args.Version <= ver {
		// 重复请求
		reply.Err = OK
		return
	}

	leaveOp := Op{
		Type:      "Leave",
		ID:        args.ClientID,
		Version:   args.Version,
		ArgsLeave: *args,
	}

	index, _, _ := sc.rf.Start(leaveOp)
	ch := make(chan interface{})
	sc.waitOpMap[index] = leaveOp
	sc.opWaitChs[index] = ch
	sc.unlock("Leave")

	opHandlerReply := <-ch

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
		sc.versionMap[args.ClientID] = -1 // 目前还不能直接改为arg.Version，需要等待applyCh返回
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

	opHandlerReply := <-ch

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

	queryOp := Op{
		Type:      "Query",
		ID:        args.ClientID,
		//Version:   args.Version,
		ArgsQuery: *args,
	}

	index, _, _ := sc.rf.Start(queryOp)
	ch := make(chan interface{})
	sc.waitOpMap[index] = queryOp
	sc.opWaitChs[index] = ch
	sc.unlock("Query")

	opHandlerReply := <-ch

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
	sc.configs[0].Groups = map[int][]string{} //初始配置

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.opWaitChs = make(map[int]chan interface{})
	sc.waitOpMap = make(map[int]Op)
	sc.versionMap = make(map[int]int64)

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
			if op.Type != "LeaderChange" {
				sc.lock("RegularOp")
				if op.Type != "Query" {
					isDupOp := false
					_, exist := sc.versionMap[op.ID]
					if !exist {
						// new Op
						sc.versionMap[op.ID] = -1
					}
					if op.Version <= sc.versionMap[op.ID] {
						fmt.Printf("[Peer %d]receive duplicate op:%+v", sc.me, op)
						isDupOp = true
					}
					if !isDupOp {
						// create new config according to previous config
						newConfig := new(Config)
						err := deepCopy(newConfig, &sc.configs[len(sc.configs)-1])
						if err != nil {
							panic("deepCopy failed")
						}
						newConfig.Num++

						switch op.Type {
						case "Join":
							for gid, servers := range op.ArgsJoin.Servers {
								newConfig.Groups[gid] = servers
							}
							avg := NShards / len(newConfig.Groups)
							groupNumMap := make(map[int]int)
							for gid := range newConfig.Groups {
								groupNumMap[gid] = 0
							}

							for shard, gid := range newConfig.Shards {
								// gid为非零的值，如果为0则忽略
								if gid == 0 || groupNumMap[gid] >= avg {
									targetGid := findMinGid(groupNumMap)
									newConfig.Shards[shard] = targetGid
									groupNumMap[targetGid]++
								} else if gid != 0 {
									groupNumMap[gid]++
								}
							}

						case "Leave":
							leaveSet := make(map[int]bool)
							for _, removeGid := range op.ArgsLeave.GIDs {
								delete(newConfig.Groups, removeGid)
								leaveSet[removeGid] = true
							}
							var avg int
							if len(newConfig.Groups) == 0 {
								avg = NShards
							} else {
								avg = NShards / len(newConfig.Groups)
							}
							groupNumMap := make(map[int]int)
							for gid := range newConfig.Groups {
								groupNumMap[gid] = 0
							}

							for shard, gid := range newConfig.Shards {
								if leaveSet[gid] {
									// 需要重新分配该shard
									if len(groupNumMap) > 0 {
										targetGid := findMinGid(groupNumMap)
										newConfig.Shards[shard] = targetGid
										groupNumMap[targetGid]++
									} else {
										newConfig.Shards[shard] = 0 // 找不到分配的gid，直接置为0表示未分配，等待下次join分配
									}
								} else {
									if groupNumMap[gid] >= avg {
										targetGid := findMinGid(groupNumMap)
										newConfig.Shards[shard] = targetGid
										groupNumMap[targetGid]++
									} else {
										groupNumMap[gid]++
									}
								}
							}

						case "Move":
							newConfig.Shards[op.ArgsMove.Shard] = op.ArgsMove.GID
						}
					}
				}


				// 比对waitOpMap确定Leader身份
				startOp := sc.waitOpMap[applyMsg.CommandIndex]
				waitCh, existCh := sc.opWaitChs[applyMsg.CommandIndex]

				if opEqual(startOp, op) && existCh {
					var sendReply interface{}
					switch op.Type {
					case "Query":
						var replyConfig Config
						if op.ArgsQuery.Num < 0 || op.ArgsQuery.Num >= len(sc.configs) {
							replyConfig = sc.configs[len(sc.configs)-1]
						} else {
							replyConfig = sc.configs[op.ArgsQuery.Num]
						}
						reply := QueryReply{
							WrongLeader: true,
							Err:         OK,
							Config:      replyConfig,
						}
						sendReply = reply
					case "Join":
						reply := JoinReply{
							WrongLeader: true,
							Err:         OK,
						}
						sendReply = reply
					case "Leave":
						reply := LeaveReply{
							WrongLeader: true,
							Err:         OK,
						}
						sendReply = reply
					case "Move":
						reply := MoveReply{
							WrongLeader: true,
							Err:         OK,
						}
						sendReply = reply
					}
					// sent to wait chan
					waitCh <- sendReply
					// close channel and delete the index from waitMap
					close(waitCh)
					delete(sc.opWaitChs, applyMsg.CommandIndex)
					delete(sc.waitOpMap, applyMsg.CommandIndex)
				} else {
					// sent ErrLeader to all ch and close all ch
					//kv.lock()
					if len(sc.opWaitChs) > 0 {
						fmt.Printf("[Peer %d] sent ErrLeader to all ch and close all ch\n", sc.me)
						for index, ch := range sc.opWaitChs {
							ch <- sc.genWrongLeaderReply(index)
							close(ch)
							delete(sc.opWaitChs, index)
							delete(sc.waitOpMap, index)
						}
					}
				}

				sc.unlock("RegularOp")
			} else {
				// op.Type == "LeaderChange"
				sc.lock("LeaderChangeHandle")
				if op.ID == sc.me { // ignore this Op
					sc.unlock("LeaderChangeHandle")
					continue
				}
				if len(sc.opWaitChs) > 0 {
					fmt.Printf("[Peer %d] receive new Leader ApplyMsg, close all ch\n", sc.me)
				}
				//kv.unlock()
				for index, ch := range sc.opWaitChs {
					ch <- sc.genWrongLeaderReply(index)
					close(ch)
					delete(sc.opWaitChs, index)
					delete(sc.waitOpMap, index)
				}
				sc.unlock("LeaderChangeHandle")

			}
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

func (sc *ShardCtrler) genWrongLeaderReply(index int) interface{} {
	switch sc.waitOpMap[index].Type {
	case "Join":
		return JoinReply{
			WrongLeader: true,
		}
	case "Leave":
		return LeaveReply{
			WrongLeader: true,
		}
	case "Move":
		return MoveReply{
			WrongLeader: true,
		}
	case "Query":
		return QueryReply{
			WrongLeader: true,
		}
	}
	panic("impossible type reply")
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

// 使用反射实现，目标类型变量需要导出
func deepCopy(dst, src interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}

func findMinGid(gidNumMap map[int]int) int {
	min := math.MaxInt32
	resGid := 0
	for gid, num := range gidNumMap {
		if num < min {
			resGid = gid
			min = num
		}
	}
	return resGid
}

func opEqual(op1, op2 Op) bool {
	j1, _ := json.Marshal(op1)
	j2, _ := json.Marshal(op2)
	return string(j1) == string(j2)
}
