## My 6.824 Lab 2021 version
Lab2 link: https://pdos.csail.mit.edu/6.824/labs/lab-raft.html

### Lab 2A: Leader election
Checkpoint saved in `branch: Lab_2A_rebuild`.

Lab2A由于第一版`branch: Lab_2A_Leader_Election`无法通过所有测试，将第一版进行了重构，主要的改变有：

1. 优化RPC请求, 对RPC进行了超时处理与异步化处理, 如在投票发起RequestVote RPC时, 为RPC设置超时上限(避免hung在已经disconnect的机器上), 同时发送RPC时采用异步发送的方式,单独启动一个异步协程进行发送和重试操作, 通过channel处理RPC结果。
   
2. 优化锁，对变量进行修改均通过modify函数进行操作，不在外部直接使用mutex，因此对变量的变更类似原语，保证了锁的粒度。此外通过使用命名锁的方式便于调试查看锁锁定的时间。

3. 优化协程同步模式，对于定时器和其他操作，原先使用sync.Cond时出现了很多问题，因此将协程同步的方式改为了Channel, 定时器直接改为使用time.Timer。

### Lab 2B: Log
Checkpoint saved in `branch: Lab_2B_Log`.

基本实现思路按照Raft论文实现，~~不过论文中给出AppendEntries的返回Reply仅有Term和Success两个参数，我认为应该加上一个NextIndex参数便于Leader
更新matchIndex和nextIndex参数，简单的为matchIndex加上len(args.Entries)是不正确的，因为Follower在执行AppendEntries方法时不是从末尾Append的，而是从
prevLogIndex处Append的，如果在成功Append后在reply中加上表示Follower希望的下一个Index，可以便于Leader正确的更新matchIndex和nextIndex参数。~~

当然最好的方法是将matchIndex和nextIndex在PrevLogIndex的基础上加上len(args.Entries), 经过测试，这种方式不需要修改AppendEntries的参数，且可以正常通过全部测试。