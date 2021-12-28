package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new Log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// ApplyMsg
// as each Raft peer becomes aware that successive Log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Log entry.
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

type LogEntry struct {
	Term    int
	Command interface{}
}

type LogBind struct {
	Entries 			[]LogEntry
	LastIncludedIndex 	int
	LastIncludedTerm 	int
}

// 获取真实index对应的值
func (l *LogBind) get(index int) LogEntry {
	if index > l.LastIncludedIndex+len(l.Entries) {
		panic("index is greater than log boundary")
	} else if index < l.LastIncludedIndex {
		panic("index is smaller than the last include index")
	}
	return l.Entries[index - l.LastIncludedIndex]	// 下标为0不使用，正常情况不应访问下标为0的成员
}

func (l *LogBind) getLastIndexAndTerm() (lastIndex, lastTerm int) {
	lastIndex = l.LastIncludedIndex + len(l.Entries) - 1
	lastTerm = l.Entries[len(l.Entries) - 1].Term
	return
}

func (l *LogBind) getLastIndex() int {
	return l.LastIncludedIndex + len(l.Entries) - 1
}

const (
	emptyVotedFor             = -1
	Follower            State = 0
	Candidate           State = 1
	Leader              State = 2
	HeartBeatTimeout          = time.Millisecond * 200
	ElectionTimeout           = time.Millisecond * 300
	RPCThreshold              = time.Millisecond * 50
	LockThreshold             = time.Millisecond * 1	//Lock会变成饥饿状态
	ApplyMsgSendTimeout       = time.Millisecond * 200
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

	applyCh chan ApplyMsg // send ApplyMsg to tester

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// State a Raft server must maintain.

	//下面三组是论文中指定的
	// persistent
	CurrentTerm int //服务器已知最新的任期（在服务器首次启动的时候初始化为0，单调递增）
	VotedFor    int //当前任期内收到选票的候选者id 如果没有投给任何候选者 则为空
	Log         LogBind

	// volatile
	CommitIndex int // CommitIndex 已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int // lastApplied 已被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）

	// volatile on leader (Reinitialized after election)
	nextIndex  []int // nextIndex 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导者最后的日志条目的索引 +1)
	matchIndex []int // matchIndex 对每一台服务器，已知的已经复制到该服务器的最高日志条目的索引 (初始值为0 单调递增）

	// election
	state         State
	electionTimer *time.Timer
	SendTimer     []*time.Timer // leader发起AppendEntries RPC调用计时,如果超时发送一条空的心跳
	//appendEntryCh        chan struct{} // 收到合法的appendEntry时才会导入该chan
	needToSendApplyMsgCh chan struct{}
	// Frans教授在讲解2A/2B时，特意提到不要在向applyCh发送log entry时持有锁。
	//在他的实现中，向applyCh发送是采用一个专门goroutine执行来确保串行的
	applyMsgSendTimer    *time.Timer

	// lock debug
	lockName  string
	lockStart time.Time
	lockEnd   time.Time

	stopCh chan struct{}

	// 立即发送AppendEntriesRPC，以加速上游Server
	immediatelySendCh	[]chan struct{}
}

// GetState return CurrentTerm and whether this server
// believes itself is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.lock("GetState")
	term = rf.CurrentTerm
	isleader = rf.state == Leader
	rf.unlock("GetState")
	return term, isleader
}

func (rf *Raft) modifyState(s State) {
	rf.state = s
	switch s {
	case Follower:
	case Candidate:
		rf.CurrentTerm += 1
		rf.VotedFor = rf.me
		rf.resetElectionTimeout()
	case Leader:
		rf.nextIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.Log.getLastIndex() + 1
		}
		rf.matchIndex = make([]int, len(rf.peers))
		rf.matchIndex[rf.me] = rf.Log.getLastIndex()
	}
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
		fmt.Printf("long lock: %s, time: %s\n", name, duration)
	}
	rf.mu.Unlock()
}

//
// save Raft's persistent State to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	data := rf.getPersistStateBytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) getPersistStateBytes() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.Log)
	e.Encode(rf.VotedFor)
	// e.Encode(rf.CommitIndex)
	data := w.Bytes()
	return data
}

//
// restore previously persisted State.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any State?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var Log LogBind
	var VotedFor int
	//var CommitIndex int
	if d.Decode(&CurrentTerm) != nil || d.Decode(&Log) != nil || d.Decode(&VotedFor) != nil {
	//|| d.Decode(&CommitIndex) != nil {
		rf.printLog("Read Persist failed\n")
	} else {
		rf.VotedFor = VotedFor
		rf.CurrentTerm = CurrentTerm
		rf.Log = Log
		//rf.CommitIndex = CommitIndex
	}

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
	rf.lock("RequestVote")
	defer rf.unlock("RequestVote")
	defer rf.persist()
	// Your code here (2A, 2B).
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term == rf.CurrentTerm {
		if rf.state == Leader {
			reply.Term = rf.CurrentTerm
			reply.VoteGranted = false
			return
		}
		if rf.VotedFor != emptyVotedFor && rf.VotedFor != args.CandidateId {
			// 已投给其他人
			reply.Term = rf.CurrentTerm
			reply.VoteGranted = false
			return
		}
		if rf.VotedFor == args.CandidateId {
			// 如果voteFor和CandidateId相同，则说明已经为其投过票，可能是对方接收失败，再次请求时直接投给它即可
			reply.Term = rf.CurrentTerm
			reply.VoteGranted = true
			//rf.resetElectionTimeout()	//这里不应重置选举时间，因为可能是由于候选者RPC返回未收到重复发送导致的
			return
		}
		// 运行到此的确定未投票
	} else if args.Term > rf.CurrentTerm {
		// args.Term > rf.CurrentTerm
		rf.CurrentTerm = args.Term
		rf.VotedFor = emptyVotedFor
		rf.state = Follower
	}
	lastLogIndex, lastLogTerm := rf.Log.getLastIndexAndTerm()
	// todo review this
	if lastLogTerm > args.LastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		// 必须保证Candidate的Log要新于自己才能投给他
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}
	if rf.VotedFor == emptyVotedFor {
		rf.CurrentTerm = args.Term
		rf.VotedFor = args.CandidateId
		rf.modifyState(Follower)
		reply.VoteGranted = true
		reply.Term = rf.CurrentTerm
		rf.resetElectionTimeout()
		return
	} else {
		reply.Term = rf.CurrentTerm
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
// agreement on the next command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
// 其实Client会依次遍历各个server, 在isLeader为true时才认为写入成功
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.lock("start")
	index := rf.Log.getLastIndex() + 1
	term := rf.CurrentTerm
	isLeader := rf.state == Leader

	if rf.state == Leader { // 如果写入的对象不是Leader, 则写入失败
		rf.Log.Entries = append(rf.Log.Entries, LogEntry{
			Term:    term,
			Command: command,
		})
		rf.matchIndex[rf.me] = index
		rf.persist()
	}
	//rf.resetAllSendTimer()
	rf.unlock("start")
	go func(){
		for i := range rf.peers {
			rf.immediatelySendCh[i] <- struct{}{}
		}
	}()

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

type AppendEntriesArgs struct {
	Term         int        // 当前领导者的任期
	LeaderId     int        // 领导者ID 因此跟随者可以对客户端进行重定向
	PrevLogIndex int        // 紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int        // 紧邻新日志条目之前的那个日志条目的任期
	Entries      []LogEntry // 需要被保存的日志条目（被当做心跳使用则日志条目内容为空；为了提高效率可能一次性发送多个）
	LeaderCommit int        // 领导者的已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term      int  // 当前任期,对于领导者而言 它会更新自己的任期
	Success   bool // 结果为真 如果跟随者所含有的条目和prevLogIndex以及prevLogTerm匹配上了
	NextIndex int  // Follower希望收到的下一个Entries的下标，如果按照论文没有该变量，实现起来有很多麻烦
}

// 返回true表示在当前日志中出现了错误的日志使得本Follower自己的日志最后Index已经超过了Leader的日志最后Index
func (rf *Raft) outOfOrderAppendEntries(args *AppendEntriesArgs) bool {
	argsLastIndex := args.PrevLogIndex + len(args.Entries)
	lastIndex, lastTerm := rf.Log.getLastIndexAndTerm()
	if argsLastIndex < lastIndex && lastTerm == args.Term {
		return true
	}
	return false
}

// AppendEntries leader发起调用：追加日志&&心跳, follower接收
// 1. 客户端发起写命令请求时 2.发送心跳时 3.日志匹配失败时
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock("AppendEntries")
	defer rf.unlock("AppendEntries")
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		rf.printLog("receive outdated AppendEntries RPC from leader peer:%d", args.LeaderId)
		reply.Success = false
		return
	}
	rf.state = Follower
	rf.CurrentTerm = args.Term
	rf.resetElectionTimeout()
	if args.PrevLogIndex < rf.Log.LastIncludedIndex {
		// 因为 lastsnapshotindex 应该已经被 apply，正常情况不该发生
		rf.printLog("lack of middle part before LastIncludedIndex")
		reply.Success = false
		reply.NextIndex = rf.Log.LastIncludedIndex + 1
	} else if args.PrevLogIndex > rf.Log.getLastIndex() {
		// 缺少中间的部分
		rf.printLog("can't find prevLogIndex AppendEntries RPC from leader peer:%d, args:%+v, myLog:%+v", args.LeaderId, args, rf.Log)
		reply.Success = false
		reply.NextIndex = rf.Log.getLastIndex() + 1
	} else if args.PrevLogIndex == rf.Log.LastIncludedIndex {
		// PrevLogIndex就是快照
		if rf.outOfOrderAppendEntries(args) {
			rf.printLog("out of order")
			reply.Success = false
			reply.NextIndex = 0
		} else {
			reply.Success = true
			rf.Log.Entries = append(rf.Log.Entries[:1], args.Entries...) // 保留 logs[0]
			reply.NextIndex = rf.Log.getLastIndex() + 1
		}
	} else if rf.Log.get(args.PrevLogIndex).Term == args.PrevLogTerm {
		// 正常情况
		if rf.outOfOrderAppendEntries(args) {
			rf.printLog("out of order")
			reply.Success = false
			reply.NextIndex = 0
		} else {
			reply.Success = true
			realIdx := args.PrevLogIndex - rf.Log.LastIncludedIndex
			rf.Log.Entries = append(rf.Log.Entries[0: realIdx + 1], args.Entries...)
			reply.NextIndex = rf.Log.getLastIndex() + 1
		}
	} else {
		// term 对不上
		rf.printLog("can't find prevLogTerm AppendEntries RPC from leader peer:%d, args:%+v", args.LeaderId, args)
		reply.Success = false
		term := rf.Log.get(args.PrevLogIndex).Term
		idx := args.PrevLogIndex
		for idx > rf.CommitIndex && rf.Log.get(idx).Term == term && idx > rf.Log.LastIncludedIndex {
			idx -= 1
		}
		reply.NextIndex = idx + 1
	}

	if reply.Success {
		if rf.CommitIndex < args.LeaderCommit {
			rf.printLog("some entries add")
			rf.CommitIndex = args.LeaderCommit
			rf.needToSendApplyMsgCh <- struct{}{}	// 有缓冲区，不会阻塞
		} else {
			rf.printLog("no entries add")
		}
	}
	rf.persist()
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

// 为打印内容增加固定前缀
func (rf *Raft) printLog(format string, i ...interface{}) {
	in := fmt.Sprintf(format, i...)
	pre := fmt.Sprintf("[Peer:%d Term:%d LastInclIndex/Term: %d/%d]\n", rf.me, rf.CurrentTerm,
		 rf.Log.LastIncludedIndex, rf.Log.LastIncludedTerm)
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
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).
	rf.CurrentTerm = 0
	rf.VotedFor = emptyVotedFor
	rf.Log = LogBind{
		Entries:           make([]LogEntry, 1),
		LastIncludedIndex: 0,
		LastIncludedTerm:  0,
	}
	rf.CommitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)
	rf.mu = sync.Mutex{}
	rf.stopCh = make(chan struct{})
	rf.needToSendApplyMsgCh = make(chan struct{}, 100)

	rf.electionTimer = time.NewTimer(ElectionTimeout + getRandomTime())
	// 由于rpc发送存在延迟，因此需要为每个follower设置独立的定时器
	rf.SendTimer = make([]*time.Timer, len(rf.peers))
	//需要设定一个定期提交的Timer, 使得Leader也可以提交
	rf.applyMsgSendTimer = time.NewTimer(ApplyMsgSendTimeout)
	for i := range rf.peers {
		rf.SendTimer[i] = time.NewTimer(HeartBeatTimeout)
	}
	// initialize from State persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.lastApplied = rf.Log.LastIncludedIndex

	fmt.Printf("\n%d Make finish %+v\n", rf.me, rf)

	rf.immediatelySendCh = make([]chan struct{}, len(rf.peers))
	for i := range rf.peers {
		rf.immediatelySendCh[i] = make(chan struct{}, 10)
	}
	// Apply Log协程
	go func() {
		for {
			select {
			case <-rf.stopCh:
				return
			case <-rf.applyMsgSendTimer.C: //定期提交ApplyMsg到tester
				rf.needToSendApplyMsgCh <- struct{}{}
			case <-rf.needToSendApplyMsgCh:
				rf.sendApplyMsg()
			}
		}
	}()

	// 选举协程
	go func() {
		for {
			select {
			case <-rf.stopCh:
				return
			case <-rf.electionTimer.C:
				rf.startElection()
			}
		}
	}()

	// leader协程
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
					// 检查Leader身份后发送
					rf.sendAppendEntriesToFollower(i)
				case <-rf.immediatelySendCh[i]:
					rf.sendAppendEntriesToFollower(i)
				}
			}
		}(i)
	}

	return rf
}

func (rf *Raft) sendApplyMsg() {
	defer rf.applyMsgSendTimer.Reset(ApplyMsgSendTimeout)
	// 发送lastApplied到commitIndex之间的Entries
	msgs := make([]ApplyMsg, 0)
	rf.lock("sendApplyMsg")
	if rf.CommitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.CommitIndex; i++ {
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.Log.get(i).Command,
				CommandIndex: i,
			})
		}
	}
	rf.unlock("sendApplyMsg")
	rf.printLog("sent to tester %+v", msgs)
	for _, msg := range msgs {
		rf.applyCh <- msg
		rf.lock("modifyLastApplied")
		rf.lastApplied = msg.CommandIndex
		rf.unlock("modifyLastApplied")
	}
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randElectionTimeout())
}

func getRandomTime() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(rand.Intn(100)) //[0,100)
}

func randElectionTimeout() time.Duration {
	r := time.Duration(rand.Int63()) % ElectionTimeout
	return ElectionTimeout + r
}

func (rf *Raft) startElection() {
	rf.lock("startElection")
	if rf.state == Leader {
		rf.unlock("startElection")
		return
	}
	rf.VotedFor = emptyVotedFor
	rf.modifyState(Candidate)
	rf.unlock("startElection")
	rf.lock("joinElection")
	rf.printLog("join election")
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.persist()
	lastIndex, lastTerm := rf.Log.getLastIndexAndTerm()
	args := RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}
	rf.unlock("joinElection")
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
				rf.lock("ElectionChangeTerm")
				rf.CurrentTerm = reply.Term
				rf.modifyState(Follower)
				rf.VotedFor = emptyVotedFor
				rf.persist()
				rf.resetElectionTimeout()
				rf.unlock("ElectionChangeTerm")
			}
		}(i)
	}

	replyNum := 1
	voteNum := 1
	for {
		res := <-ch
		replyNum++
		if res {
			voteNum++
		}
		if replyNum == len(rf.peers) || voteNum > len(rf.peers)/2 || replyNum-voteNum > len(rf.peers)/2 {
			break
		}
	}

	if voteNum <= len(rf.peers)/2 {
		rf.printLog("fail in the election because only get voteNum: %d", voteNum)
		rf.lock("failedElection")
		rf.modifyState(Follower)
		rf.VotedFor = emptyVotedFor
		rf.persist()
		rf.unlock("failedElection")
		return
	}

	rf.printLog("try to become leader with voteNum: %d", voteNum)
	rf.lock("gonnaWinElection")
	if rf.CurrentTerm == args.Term && rf.state == Candidate { // 防止其他人已经在新的Term成为leader
		rf.modifyState(Leader)
		rf.persist()
		rf.resetAllSendTimer()
		// Lab3A: 发送一条双false的消息表示Leader发生了改变
		go func() {rf.applyCh <- ApplyMsg{CommandValid: false, SnapshotValid: false}}()
	} else {
		rf.printLog("fail to become leader because: %v %v", rf.CurrentTerm == args.Term, rf.state == Candidate)
		rf.modifyState(Follower)
		rf.VotedFor = emptyVotedFor
		rf.persist()
	}
	rf.unlock("gonnaWinElection")
}

func (rf *Raft) getAppendEntriesArgs(followerIdx int) *AppendEntriesArgs {
	// 已经有锁
	nextIndex := rf.nextIndex[followerIdx]
	lastIndex, lastTerm := rf.Log.getLastIndexAndTerm()
	entries := make([]LogEntry, 0)
	if nextIndex > lastIndex || nextIndex <= rf.Log.LastIncludedIndex { // 无需发送任何内容
		args := AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderId:     rf.me,
			Entries:      entries,
			PrevLogIndex: lastIndex,
			PrevLogTerm:  lastTerm,
			LeaderCommit: rf.CommitIndex,
		}
		return &args
	}
	entries = append(entries, rf.Log.Entries[nextIndex - rf.Log.LastIncludedIndex:]...)
	prevLogIndex := nextIndex - 1
	var prevLogTerm int
	if prevLogIndex == rf.Log.LastIncludedIndex {
		prevLogTerm = rf.Log.LastIncludedTerm
	} else {
		prevLogTerm = rf.Log.get(prevLogIndex).Term
	}
	args := AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		Entries:      entries,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: rf.CommitIndex,
	}
	return &args
}

func (rf *Raft) sendAppendEntriesToFollower(followerIdx int) {
	RPCTimer := time.NewTimer(RPCThreshold)
	defer RPCTimer.Stop()

	for !rf.killed() {
		rf.lock("getAppendEntriesArgs")
		if rf.state != Leader {
			rf.resetSendTimer(followerIdx)
			rf.unlock("getAppendEntriesArgs")
			return
		}
		rf.resetSendTimer(followerIdx)
		args := rf.getAppendEntriesArgs(followerIdx)
		rf.unlock("getAppendEntriesArgs")

		RPCTimer.Stop()
		RPCTimer.Reset(RPCThreshold)
		rpcResCh := make(chan bool, 1)
		reply := AppendEntriesReply{}
		go func(args *AppendEntriesArgs, reply *AppendEntriesReply) {
			ok := rf.peers[followerIdx].Call("Raft.AppendEntries", args, reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			rpcResCh <- ok
		}(args, &reply)

		select {
		case <-rf.stopCh:
			return
		case <-RPCTimer.C:
			rf.printLog("AppendEntries RPC timeout: follower:%d, args:%+v", followerIdx, args)
			continue
		case ok := <-rpcResCh:
			if !ok {
				rf.printLog("AppendEntries failed")
				continue
			}
		}

		rf.lock("handleReplyOfAppendEntries")
		if reply.Term > rf.CurrentTerm {
			// 	leader 发现自己已经落后于接受者，自降为Follower
			rf.modifyState(Follower)
			rf.CurrentTerm = reply.Term
			rf.persist()
			rf.resetElectionTimeout()
			rf.unlock("handleReplyOfAppendEntries")
			return
		}

		if rf.state != Leader || rf.CurrentTerm != args.Term {
			// review state
			rf.unlock("handleReplyOfAppendEntries")
			return
		}

		if reply.Success {
			// 更新next和matchIndex表示下次该peer的位置
			// fixed: TestFailAgree2B无法通过
			// 这种写法看似正确，但其实会出现在出现故障时matchIndex被重复计算导致元素重复的问题
			// 主要原因是args.Entries并不一定是从NextIndex位置Append的
			//if len(args.Entries) > 0 {
			//	rf.printLog("append %d entries to follower: %d", len(args.Entries), followerIdx)
			//	rf.modifyMatchIndex(followerIdx, len(args.Entries))
			//	rf.modifyNextIndex(followerIdx, len(args.Entries))
			//}
			// 下面为修改的写法，matchIndex直接在args.PrevLogIndex加上len(args.Entries)即可，可以正常通过Lab 2B测试
			//if len(args.Entries) > 0 {
			//	rf.printLog("append %d entries to follower: %d", len(args.Entries), followerIdx)
			//	rf.lock("modify next and match index")
			//	rf.nextIndex[followerIdx] = args.PrevLogIndex + len(args.Entries) + 1
			//	rf.matchIndex[followerIdx] = args.PrevLogIndex + len(args.Entries)
			//	rf.unlock("modify next and match index")
			//}
			// 根据论文p7-p8的灰色描述，这种设计似乎是正确的
			if reply.NextIndex > rf.nextIndex[followerIdx] {
				rf.nextIndex[followerIdx] = reply.NextIndex
				rf.matchIndex[followerIdx] = reply.NextIndex - 1
			}
			// 更新commitIndex，只要有一半的peer已经确认该index被复制到该机器上，就可以确认该index被commit
			if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Term == rf.CurrentTerm {
				// 根据Figure 8的描述，leader cannot determine commitment using log entries from older terms
				hasChanged := false // 这个变量可以减少ApplyMsg的次数
				for j := rf.CommitIndex + 1; j <= rf.Log.LastIncludedIndex + len(rf.Log.Entries); j++ {
					confirmed := 0
					for _, m := range rf.matchIndex {
						if m >= j {
							confirmed++
							if confirmed > len(rf.peers)/2 {
								rf.CommitIndex = j
								hasChanged = true
								rf.printLog("CommitIndex has been updated to %d", j)
								break
							}
						}
					}
					if rf.CommitIndex != j {
						// 小的确定不能被commit，大j的就更不会了
						break
					}
				}
				if hasChanged {
					rf.needToSendApplyMsgCh <- struct{}{}
				}
			}
			rf.persist()
			rf.unlock("handleReplyOfAppendEntries")
			return
		} else {
			// Success == false
			if reply.NextIndex != 0 {
				if reply.NextIndex > rf.Log.LastIncludedIndex {
					// 没有出现论文中所说的"远远落后"的情况，更新该peer的nextIndex后重试
					rf.nextIndex[followerIdx] = reply.NextIndex
					rf.unlock("handleReplyOfAppendEntries")
					continue
				} else {	// 如果reply.NextIndex == rf.Log.LastIncludedIndex, 同样也应发送快照
					// 出现论文中所说的"远远落后"的情况，发送InstallSnapshot RPC
					go rf.sendInstallSnapshotToFollower(followerIdx)
					rf.unlock("handleReplyOfAppendEntries")
					return
				}
			} else {
				rf.unlock("handleReplyOfAppendEntries")
			}
		}
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
