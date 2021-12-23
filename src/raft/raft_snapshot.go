package raft

import (
	"time"
)

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// InstallSnapshot Follow Figure 13
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lock("InstallSnapshot")
	defer rf.unlock("InstallSnapshot")
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		return
	} else if args.Term > rf.CurrentTerm || rf.state != Follower {
		rf.CurrentTerm = args.Term
		rf.state = Follower
		rf.resetElectionTimeout()
		defer rf.persist()
	}

	if args.LastIncludedIndex <= rf.Log.LastIncludedIndex {
		rf.printLog("receive old LastIncludedIndex %d", args.LastIncludedIndex)
		return
	}
	// 指南中说：当Follower收到并处理 InstallSnapshot RPC 时，它必须使用 Raft 将包含的快照交给服务。InstallSnapshot 处理程序可以使用
	// applyCh 来将快照发送给服务，方法是将快照放在 ApplyMsg 中。服务从applyCh中读取，并用快照调用CondInstallSnapshot来告诉Raft，
	// 服务正在切换到传入的快照状态，并且Raft应该同时更新它的日志。(参见config.go中的applierSnap()，看看tester服务是如何做到这一点的)

	// 也就是说，收到InstallSnapshot并不需要Follower修改其自身的日志，仅需要Follower将这个收到这个快照的信息通过applyCh发送给上一层，
	// 真正修改日志是在CondInstallSnapshot RPC调用之时完成的
	rf.printLog("receive InstallSnapshot with snapIndex/Term %d/%d", args.LastIncludedIndex, args.LastIncludedTerm)
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	// 由于applyCh无buf，因此开启单独协程发送消息避免阻塞进程而导致无法释放锁
	go func() { rf.applyCh <- applyMsg }()
}

// 什么时候Leader会向Follower发送InstallSnapshot RPC?
// 根据论文中的描述，当Follower远远落后于Leader时才会发送。其实就是当Follower自身的NextIndex小于Leader已经快照的LastIncludedIndex时，
// 由于Follower需要的NextIndex后面的部分已经不可找回(已经被快照保存，日志已经删除)，所以Leader需要向该"远远落后"的Follower发送自己已经保存
// 好的快照，发起InstallSnapshot RPC使得这个Follower跟上进度，这个Follower之后还需要通过接收AppendEntries RPC才能完全跟上Leader

// 注意在Follower收到InstallSnapshot后，Follower并不会马上安装这个快照，而是在收到服务方通过 CondInstallSnapshot 调用确认该follower
// 可以安装这个快照后其才会真正安装，但是Leader此时也应该切换这个Follower的nextIndex到lastIncludedIndex,即使Follower没有收到
// CondInstallSnapshot也无妨，因为此时AppendEntries返回的Reply同样会纠正这个nextIndex， Follower会再次发送InstallSnapshot RPC
func (rf *Raft) sendInstallSnapshotToFollower(followerIdx int) {
	rf.lock("sendInstallSnapshotToFollower")
	if rf.state != Leader {
		rf.resetSendTimer(followerIdx)
		rf.unlock("sendInstallSnapshotToFollower")
		return
	}
	args := InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.Log.LastIncludedIndex,
		LastIncludedTerm:  rf.Log.LastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.unlock("sendInstallSnapshotToFollower")
	RPCTimer := time.NewTimer(RPCThreshold)
	defer RPCTimer.Stop()
	for !rf.killed() {
		resCh := make(chan bool, 1)
		reply := InstallSnapshotReply{}
		go func() {
			ok := rf.peers[followerIdx].Call("Raft.InstallSnapshot", &args, &reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			resCh <- ok
		}()

		select {
		case <-rf.stopCh:
			return
		case <-RPCTimer.C:
			continue
		case ok := <-resCh:
			if !ok {
				continue
			}
		}

		// handle reply
		rf.lock("handleInstallSnapshotReply")
		if rf.CurrentTerm != args.Term || rf.state != Leader { // valid check
			rf.unlock("handleInstallSnapshotReply")
			return
		}
		if reply.Term > rf.CurrentTerm {
			rf.modifyState(Follower)
			rf.resetElectionTimeout()
			rf.CurrentTerm = reply.Term
			rf.persist()
			rf.unlock("handleInstallSnapshotReply")
			return
		}
		// 不管现在Snapshot到底安装没有(只有在收到CondInstallSnapshot后才会安装)，先修改nextIndex
		if args.LastIncludedIndex > rf.matchIndex[followerIdx] {
			rf.matchIndex[followerIdx] = args.LastIncludedIndex
		}
		if args.LastIncludedIndex + 1 > rf.nextIndex[followerIdx] {
			rf.nextIndex[followerIdx] = args.LastIncludedIndex + 1
		}
		rf.unlock("handleInstallSnapshotReply")
		return
	}

}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
// 其实CondInstallSnapshot传入的参数就是在InstallSnapshot中发给applyCh的
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	rf.printLog("CondInstallSnapshot to lastIncludedTerm: %d; lastIncludedIndex: %d", lastIncludedTerm, lastIncludedIndex)
	rf.lock("CondInstallSnapshot")
	defer rf.unlock("CondInstallSnapshot") // defer的顺序是后进先出

	if lastIncludedIndex <= rf.Log.LastIncludedIndex {
		// 如果这个快照已经被加载了，就不需要再次载入了
		return false
	}

	defer func() {
		rf.printLog("CondInstallSnapshot finish")
		rf.Log.LastIncludedIndex = lastIncludedIndex
		rf.Log.LastIncludedTerm = lastIncludedTerm
		rf.CommitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
		rf.persister.SaveStateAndSnapshot(rf.getPersistStateBytes(), snapshot)

	}()
	lastIndex, lastTerm := rf.Log.getLastIndexAndTerm()
	if lastIncludedIndex <= lastIndex && lastTerm == lastIncludedTerm {
		// 当该peer的LogEntries比快照点更新时，我们需要保留比快照点更新的部分
		rf.printLog("[CondInstallSnapshot] entries change before: %+v", rf.Log.Entries)
		rf.Log.Entries = append(make([]LogEntry, 1), rf.Log.Entries[lastIncludedIndex-rf.Log.LastIncludedIndex:]...)
		rf.printLog("[CondInstallSnapshot] entries change to: %+v", rf.Log.Entries)
		return true
	}
	//否则就直接清空LogEntries
	rf.printLog("[CondInstallSnapshot] entries change before: %+v", rf.Log.Entries)
	rf.Log.Entries = make([]LogEntry, 1)	//保留Log[0]
	rf.Log.Entries[0] = LogEntry{
		Term: lastIncludedTerm,
	}
	rf.printLog("[CondInstallSnapshot] entries change to: %+v", rf.Log.Entries)
	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the Log through (and including)
// that index. Raft should now trim its Log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.printLog("snapshot to index %d", index)
	rf.lock("Snapshot")
	defer rf.unlock("Snapshot")
	if index <= rf.Log.LastIncludedIndex {
		//already created a snapshot
		return
	}
	rf.Log.Entries = append(make([]LogEntry, 0), rf.Log.Entries[index-rf.Log.LastIncludedIndex:]...)
	rf.Log.LastIncludedIndex = index
	rf.Log.LastIncludedTerm = rf.Log.get(index).Term
	rf.persister.SaveStateAndSnapshot(rf.getPersistStateBytes(), snapshot)
}
