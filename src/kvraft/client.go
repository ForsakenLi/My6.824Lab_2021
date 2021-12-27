package kvraft

import (
	"6.824/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

var myID = 0

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ID int
	version int64
	lastServer int	// 上次联系为leader，下次直接从它开始就不用找了
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.ID = myID
	myID++
	ck.version = nrand()
	return ck
}

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key}
	for i := 0; ; i++ {
		reply := GetReply{}
		nowTryServer := (i + ck.lastServer) % len(ck.servers)
		// 没有锁不需要异步发送RPC
		ok := ck.servers[nowTryServer].Call("KVServer.Get", &args, &reply)

		if ok {
			switch reply.Err {
			case OK:
				DPrintf("[Clerk %d] get key:%s successful", ck.ID, key)
				ck.lastServer = nowTryServer
				return reply.Value
			case ErrNoKey:
				DPrintf("[Clerk %d] get key:%s fail cause not exist", ck.ID, key)
				ck.lastServer = nowTryServer
				return ""
			case ErrWrongLeader:
				if i != 0 && i % len(ck.servers) == 0 {
					// 试了一圈还没有找到，可能是正在选主，sleep一下再试(外部应该是异步调用的Get，不会阻塞外部)
					DPrintf("[Clerk %d] get key:%s fail, still trying to find leader", ck.ID, key)
					time.Sleep(10 * time.Millisecond)
				}
				continue
			}
		}
	}
}

// PutAppend
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{key, value, op, ck.version, ck.ID}
	ck.version++

	for i := 0; ; i++ {
		reply := PutAppendReply{}
		nowTryServer := (i + ck.lastServer) % len(ck.servers)
		ok := ck.servers[(i+ck.lastServer)%len(ck.servers)].Call("KVServer.PutAppend", &args, &reply)

		if ok {
			switch reply.Err {
			case OK:
				DPrintf("[Clerk %d] put %s->%s successful", ck.ID, key, value)
				ck.lastServer = nowTryServer
				return
			case ErrNoKey:
				// impossible
			case ErrWrongLeader:
				if i != 0 && i % len(ck.servers) == 0 {
					// 试了一圈还没有找到，可能是正在选主，sleep一下再试(外部应该是异步调用的Get，不会阻塞外部)
					DPrintf("[Clerk %d] put %s->%s fail, still trying to find leader", ck.ID, key, value)
					time.Sleep(10 * time.Millisecond)
				}
				continue
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
