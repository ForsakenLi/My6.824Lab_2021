package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	rand2 "math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const emptyVotedFor = -1

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogTemp struct {
}

type State int

const (
	Follower State = 0
	Candidate State = 1
	Leader State = 2
)

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//下面三组是论文中指定的
	// persistent
	currentTerm int //服务器已知最新的任期（在服务器首次启动的时候初始化为0，单调递增）
	votedFor    int //当前任期内收到选票的候选者id 如果没有投给任何候选者 则为空
	log         []LogTemp

	// volatile
	commitIndex int //已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int //已被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）

	// volatile on leader (Reinitialized after election)
	nextIndex  []int // 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导者最后的日志条目的索引+1)
	matchIndex []int // 对每一台服务器，已知的已经复制到该服务器的最高日志条目的索引 (初始值为0 单调递增）

	// for election
	electionMu       sync.Mutex // 仅为electionCond使用的锁
	electionEndMu	 sync.Mutex	// 仅为electionEndCond使用的锁
	heartbeatsLeftMs int        // 心跳倒计时，不应在所有机器上一致，单位毫秒，归零后触发选举, 初值根据lab要求应大于150～300
	electionCond     sync.Cond  // 选举同步原语, 计时归零后调用Signal
	electionEndCond  sync.Cond  // 选举结束同步原语，确定选举完成后调用该方法重新开始electionTimeouts计时
	state 			State
}

// GetState return currentTerm and whether this server
// believes itself is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if rf.votedFor == emptyVotedFor {
		// todo check the log term and index for 2B
		rf.mu.Lock()
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.mu.Unlock()
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// Kill
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.electionMu.Lock()
		rf.electionCond.Wait()
		rf.electionMu.Unlock()
		rf.mu.Lock()
		rf.votedFor = emptyVotedFor
		rf.mu.Unlock()
		rand2.Seed(time.Now().UnixNano())
		sleep := time.Duration(rand2.Intn(150) + 50)
		fmt.Printf("[Peer %d Term %d] election joining will be started after %dms\n", rf.me, rf.currentTerm, sleep)
		time.Sleep(time.Millisecond * sleep)
		rf.mu.RLock()
		if rf.votedFor != emptyVotedFor {
			fmt.Printf("[Peer %d Term %d] have vote for %d, quit election\n", rf.me, rf.currentTerm, rf.votedFor)
			continue
		}
		if rf.state != Candidate {
			fmt.Printf("[Peer %d Term %d] election have ended, quit election\n", rf.me, rf.currentTerm)
			continue
		}
		rf.mu.RUnlock()
		rf.joinElection()
	}
}

func (rf *Raft) joinElection() {
	// start election and become candidate
	fmt.Printf("[Peer %d Term %d] become candidate\n", rf.me, rf.currentTerm)
	rf.mu.Lock()
	rf.currentTerm++	// term迭代
	rf.votedFor = rf.me // 给自己投票
	rf.mu.Unlock()
	voteNum := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,
			// lastLogIndex: 0,// 2A暂时不用
			// lastLogTerm:  0,
		}
		reply := RequestVoteReply{}
		ok := rf.sendRequestVote(i, &args, &reply)
		if ok {
			if reply.VoteGranted {
				voteNum++
				fmt.Printf("[Peer %d Term %d] get a vote from peer%d\n", rf.me, rf.currentTerm, i)
			}
			if voteNum > len(rf.peers)/2 {
				// become leader
				fmt.Printf("[Peer %d Term %d] win the election with voteNum: %d\n", rf.me, rf.currentTerm, voteNum)
				rf.mu.Lock()
				rf.votedFor = emptyVotedFor
				rf.state = Leader
				rf.mu.Unlock()
				rf.electionEndCond.Broadcast()
				rf.resetHeartbeatsTimeouts()
				break
			}
		}
	}
}

type AppendEntriesArgs struct {
	Term         int       //当前领导者的任期
	LeaderId     int       //领导者ID 因此跟随者可以对客户端进行重定向
	PrevLogIndex int       //紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int       //紧邻新日志条目之前的那个日志条目的任期
	Entries      []LogTemp //需要被保存的日志条目（被当做心跳使用是 则日志条目内容为空；为了提高效率可能一次性发送多个）
	LeaderCommit int       //领导者的已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term    int  //当前任期,对于领导者而言 它会更新自己的任期
	Success bool //结果为真 如果跟随者所含有的条目和prevLogIndex以及prevLogTerm匹配上了
}

// AppendEntries leader发起调用：追加日志&&心跳, follower接收
// 1. 客户端发起写命令请求时 2.发送心跳时 3.日志匹配失败时
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < rf.currentTerm {
		fmt.Printf("[Peer %d Term %d] receive out-date AppendEntries RPC from leader: peer%d\n", rf.me, rf.currentTerm, args.LeaderId)
		// 收到的term小于自身时无论是什么情况都会被丢弃
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// todo check prevLogIndex/Term or something (for 2B)
	// for 2A, just use for heartbeats
	if len(args.Entries) == 0 { // heartbeats
		fmt.Printf("[Peer %d Term %d] receive heartbeats from leader: peer%d\n", rf.me, rf.currentTerm, args.LeaderId)
		if rf.state == Candidate {
			// 在选举期间收到了其他leader的心跳，说明选举失败
			fmt.Printf("[Peer %d Term %d] stop election because receive heartbeats from leader: peer%d\n", rf.me, rf.currentTerm, args.LeaderId)
			rf.mu.Lock()
			rf.state = Follower
			rf.currentTerm = args.Term
			rf.votedFor = emptyVotedFor
			rf.mu.Unlock()
			rf.electionEndCond.Broadcast()
		}
		rf.resetHeartbeatsTimeouts()
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.mu.Unlock()
		reply.Success = true
		reply.Term = rf.currentTerm
		return
	}

}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = emptyVotedFor
	rf.log = make([]LogTemp, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)
	rf.state = Follower
	rf.mu = sync.RWMutex{}
	rf.electionMu = sync.Mutex{}
	rf.electionEndMu = sync.Mutex{}
	rf.electionCond = *sync.NewCond(&rf.electionMu)
	rf.electionEndCond = *sync.NewCond(&rf.electionEndMu)
	rf.resetHeartbeatsTimeouts()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// 心跳超时计时
	go func() {
		for {
			if rf.state == Leader {
				time.Sleep(time.Millisecond * 10)
				continue
			}
			if rf.heartbeatsLeftMs <= 0 {
				// 交给ticker方法处理
				fmt.Printf("[Peer %d Term %d] heartbeats have turn to 0, prepare to joinElection\n", rf.me, rf.currentTerm)
				rf.electionCond.Broadcast()
				rf.mu.Lock()
				rf.state = Candidate
				rf.mu.Unlock()
				rf.electionEndMu.Lock()
				rf.electionEndCond.Wait()
				rf.electionEndMu.Unlock()
			}
			rf.heartbeatsLeftMs--
			time.Sleep(time.Millisecond)
		}
	}()

	// 选举过程超时,如果超时需要再次发起选举
	go func() {
		for {
			rf.electionMu.Lock()
			rf.electionCond.Wait()
			rf.electionMu.Unlock()
			//开始选举过程计时
			electionProcess := 1000
			for rf.state == Candidate && electionProcess > 0 {
				time.Sleep(time.Millisecond)
				electionProcess--
			}
			if electionProcess <= 0 && rf.state == Candidate {
				// 重新发起一次选举
				rf.mu.Lock()
				rf.votedFor = emptyVotedFor
				rf.mu.Unlock()
				fmt.Printf("[Peer %d Term %d] election timeout, rejoin election\n", rf.me, rf.currentTerm)
				rf.joinElection()
			}
		}
	}()

	// 如果是leader, 则向其他机器发送心跳
	go func() {
		for {
			if rf.state != Leader {
				time.Sleep(time.Millisecond)
				continue
			}
			for i, p := range rf.peers {
				if i == rf.me {
					continue
				}
				args := AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderId: rf.me,
				}
				reply := AppendEntriesReply{}
				fmt.Printf("[Peer %d Term %d] send heartbeats to %d\n", rf.me, rf.currentTerm, i)
				ok := p.Call("Raft.AppendEntries", &args, &reply)
				if ok {
					if reply.Term > rf.currentTerm {
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.mu.Unlock()
					}
				}
			}

			time.Sleep(150 * time.Millisecond)
		}
	}()

	return rf
}

func (rf *Raft) resetHeartbeatsTimeouts() {
	fmt.Printf("[Peer %d Term %d] heartbeat have been reset\n", rf.me, rf.currentTerm)
	rand2.Seed(time.Now().UnixNano())
	random := rand2.Intn(150)
	rf.heartbeatsLeftMs = 300 + random
}
