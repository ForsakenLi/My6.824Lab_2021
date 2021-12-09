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
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

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

type State int

type LogTemp struct {
}

const (
	emptyVotedFor          = -1
	Follower         State = 0
	Candidate        State = 1
	Leader           State = 2
	HeartBeatTimeout       = time.Millisecond * 150
	ElectionTimeout        = time.Millisecond * 300
	RPCThreshold           = time.Millisecond * 50
	LockThreshold          = time.Millisecond * 10
)

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's State
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted State
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// State a Raft server must maintain.

	//下面三组是论文中指定的
	// persistent
	currentTerm int //服务器已知最新的任期（在服务器首次启动的时候初始化为0，单调递增）
	votedFor    int //当前任期内收到选票的候选者id 如果没有投给任何候选者 则为空
	log         []LogTemp

	// volatile
	commitIndex int //已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int //已被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）

	// volatile on leader (Reinitialized after election)
	nextIndex  []int // 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导者最后的日志条目的索引 +1)
	matchIndex []int // 对每一台服务器，已知的已经复制到该服务器的最高日志条目的索引 (初始值为0 单调递增）

	// election
	state         State // 由于golang实现状态模式比较困难，因此用变量表示，使用modifyState()进行改变
	electionTimer *time.Timer
	SendTimer     []*time.Timer // leader发起AppendEntries RPC调用计时,如果超时发送一条空的心跳
	appendEntryCh chan struct{} //收到合法的appendEntry时才会导入该chan

	// lock debug
	lockName  string
	lockStart time.Time
	lockEnd   time.Time

	stopCh chan struct{}
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

// 对rf具体值的修改尽量使用modify原语，降低锁的粒度
// for pre-job to change state
func (rf *Raft) modifyState(s State) {
	defer rf.unlock("modifyState")
	rf.lock("modifyState")
	rf.state = s
	switch s {
	case Follower:
	case Candidate:
	case Leader:
	}
}

// if selfIncrease, newTerm can be any value
func (rf *Raft) modifyTerm(newTerm int, selfIncrease bool) {
	defer rf.unlock("modifyTerm")
	rf.lock("modifyTerm")
	if selfIncrease {
		rf.currentTerm++
		rf.printLog("term self-increased")
	} else if rf.currentTerm != newTerm {
		rf.currentTerm = newTerm
		rf.printLog("term modified")
	}
}

func (rf *Raft) modifyVoteFor(who int) {
	defer rf.unlock("modifyVoteFor")
	rf.lock("modifyVoteFor")
	rf.votedFor = who
}

// for lock debug
func (rf *Raft) lock(name string) {
	rf.mu.Lock()
	rf.lockStart = time.Now()
	rf.lockName = name
}

func (rf *Raft) unlock(name string) {
	rf.lockEnd = time.Now()
	rf.lockName = ""
	duration := rf.lockEnd.Sub(rf.lockStart)
	if duration > LockThreshold {
		rf.printLog("long lock: %s, time: %s", name, duration)
	}
	rf.mu.Unlock()
}

//
// save Raft's persistent State to stable storage,
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
// restore previously persisted State.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any State?
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
		rf.modifyTerm(args.Term, false)
		rf.modifyVoteFor(args.CandidateId)
		rf.modifyState(Follower)
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
	rpcTimer := time.NewTimer(RPCThreshold)
	voteTimer := time.NewTimer(2 * RPCThreshold)
	defer rpcTimer.Stop()
	defer voteTimer.Stop()

	for !rf.killed() {
		rpcTimer.Stop()
		rpcTimer.Reset(RPCThreshold)
		ch := make(chan bool, 1)
		r := RequestVoteReply{}

		go func() {
			ok := rf.peers[server].Call("Raft.RequestVote", args, &r)
			if ok == false {
				time.Sleep(time.Millisecond * 10)
			}
			ch <- ok
		}()

		select {
		case <-voteTimer.C:
			rf.printLog("Vote request failed: %d -> %d", rf.me, server)
			return false
		case <-rpcTimer.C:
			rf.printLog("RequestVote RPC timeout: %d -> %d", rf.me, server)
			continue
		case ok := <-ch:
			if !ok {
				continue
			} else {
				reply.Term = r.Term
				reply.VoteGranted = r.VoteGranted
				return true
			}
		}
	}
	return false
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
	close(rf.stopCh) // 向所有 <-rf.stopCh 发送一个0
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

	}
}

type AppendEntriesArgs struct {
	Term         int       // 当前领导者的任期
	LeaderId     int       // 领导者ID 因此跟随者可以对客户端进行重定向
	PrevLogIndex int       // 紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int       // 紧邻新日志条目之前的那个日志条目的任期
	Entries      []LogTemp // 需要被保存的日志条目（被当做心跳使用是 则日志条目内容为空；为了提高效率可能一次性发送多个）
	LeaderCommit int       // 领导者的已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term    int  // 当前任期,对于领导者而言 它会更新自己的任期
	Success bool // 结果为真 如果跟随者所含有的条目和prevLogIndex以及prevLogTerm匹配上了
}

// AppendEntries leader发起调用：追加日志&&心跳, follower接收
// 1. 客户端发起写命令请求时 2.发送心跳时 3.日志匹配失败时
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < rf.currentTerm {
		rf.printLog("receive outdated AppendEntries RPC from leader peer:%d", args.LeaderId)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// todo check prevLogIndex/Term or something (for 2B)
	// for 2A, just use for heartbeats
	if len(args.Entries) == 0 { // heartbeats
		rf.printLog("receive heartbeats from leader peer:%d", args.LeaderId)
		rf.modifyTerm(args.Term, false)
		rf.appendEntryCh <- struct{}{}
		reply.Success = true
		reply.Term = rf.currentTerm
	}

}

// 为打印内容增加固定前缀
func (rf *Raft) printLog(format string, i ...interface{}) {
	in := fmt.Sprintf(format, i...)
	pre := fmt.Sprintf("[Peer:%d Term:%d VoteFor:%d]", rf.me, rf.currentTerm, rf.votedFor)
	fmt.Println(pre + in)
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent State, and also initially holds the most
// recent saved State, if any. applyCh is a channel on which the
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
	rf.mu = sync.Mutex{}
	rf.appendEntryCh = make(chan struct{}, 100)
	rf.stopCh = make(chan struct{})
	rf.electionTimer = time.NewTimer(ElectionTimeout + getRandomTime())
	// 由于rpc发送存在延迟，因此需要为每个follower设置独立的定时器
	rf.SendTimer = make([]*time.Timer, len(rf.peers))
	for i := range rf.peers {
		rf.SendTimer[i] = time.NewTimer(HeartBeatTimeout)
	}
	// initialize from State persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()

	go func() {
		for {
			select {
			case <-rf.stopCh:
				return
			case <-rf.electionTimer.C: //无论是选举超时还是心跳超时，都会发起选举
				rf.modifyVoteFor(emptyVotedFor)
				rf.startElection()
				rf.resetElectionTimeout()
			case <-rf.appendEntryCh: //收到合法的appendEntry(心跳)时
				rf.modifyState(Follower)
				rf.modifyVoteFor(emptyVotedFor)
				rf.resetElectionTimeout()
			}
		}
	}()

	// leader
	for i := range peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			for {
				select {
				case <-rf.stopCh:
					return
				case <-rf.SendTimer[i].C:
					rf.sendHeartbeatsToFollower(i)
				}
			}
		}(i)
	}

	return rf
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(ElectionTimeout + getRandomTime())
}

func getRandomTime() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(rand.Intn(100)) //[0,100)
}

func (rf *Raft) startElection() {
	if rf.state == Leader {
		return
	}
	rf.modifyState(Candidate)
	nowTerm := rf.currentTerm                                       // 防止sleep结束后term已经更新
	time.Sleep(getRandomTime()*time.Millisecond + HeartBeatTimeout) // 保证该周期至少可以收到一个心跳
	if rf.state != Candidate || rf.votedFor != emptyVotedFor || rf.currentTerm != nowTerm {
		rf.printLog("quit election because %v %v %v", rf.state != Candidate, rf.votedFor != emptyVotedFor, rf.currentTerm != nowTerm)
		rf.modifyState(Follower)
		return
	}
	rf.printLog("join election")
	rf.modifyTerm(0, true)
	rf.modifyVoteFor(rf.me)

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
	}
	ch := make(chan bool, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// 异步发起RequestVote RPC
		go func(i int) {
			reply := RequestVoteReply{
				VoteGranted: false,
			}
			ok := rf.sendRequestVote(i, &args, &reply)
			ch <- reply.VoteGranted
			// 如果在投票时出现了Follower term比自己大的情况，则需要做自降Follower操作
			if ok && reply.Term > args.Term {
				rf.modifyTerm(reply.Term, false)
				rf.modifyState(Follower)
				rf.modifyVoteFor(emptyVotedFor)
				rf.resetElectionTimeout()
			}
		}(i)
	}

	replyNum := 1
	voteNum := 1
	for {
		res := <- ch
		replyNum++
		if res {
			voteNum++
		}
		if replyNum == len(rf.peers) || voteNum > len(rf.peers)/2 || replyNum - voteNum > len(rf.peers)/2 {
			break
		}
	}

	if voteNum <= len(rf.peers)/2 {
		rf.printLog("fail in the election because only get voteNum: %d", voteNum)
		rf.modifyState(Follower)
		rf.modifyVoteFor(emptyVotedFor)
		return
	}

	rf.printLog("try to become leader with voteNum: %d", voteNum)
	if rf.currentTerm == args.Term && rf.state == Candidate { // 防止其他人已经在新的Term成为leader
		rf.modifyState(Leader)
		rf.resetAllSendTimer()
	} else {
		rf.printLog("fail to become leader because: %v %v", rf.currentTerm == args.Term, rf.state == Candidate)
		rf.modifyState(Follower)
		rf.modifyVoteFor(emptyVotedFor)
	}
}

func (rf *Raft) sendHeartbeatsToFollower(i int) {
	RPCTimer := time.NewTimer(RPCThreshold)
	defer RPCTimer.Stop()

	for !rf.killed() {
		if rf.state != Leader {
			rf.resetSendTimer(i)
			return
		}

		rf.resetSendTimer(i)
		ch := make(chan bool, 1)
		args := AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}
		reply := AppendEntriesReply{}
		go func(args *AppendEntriesArgs, reply *AppendEntriesReply) {
			ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			ch <- ok
		}(&args, &reply)

		select {
		case <-rf.stopCh:
			return
		case <-RPCTimer.C:
			rf.printLog("AppendEntries RPC timeout: follower:%d, args:%+v", i, args)
			continue
		case ok := <-ch:
			if !ok {
				rf.printLog("AppendEntries failed")
				continue
			}
		}

		if reply.Term > rf.currentTerm {
			// 	leader 发现自己已经落后于接受者，自降为Follower
			rf.modifyState(Follower)
			rf.modifyTerm(reply.Term, false)
			rf.resetElectionTimeout()
			return
		}
		// todo 2B review reply
	}
}

func (rf *Raft) resetSendTimer(i int) {
	rf.SendTimer[i].Stop()
	rf.SendTimer[i].Reset(HeartBeatTimeout)
}

func (rf *Raft) resetAllSendTimer() {
	for i := range rf.peers {
		rf.resetSendTimer(i)
	}
}
