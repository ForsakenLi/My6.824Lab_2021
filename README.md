# My 6.824 Lab 2021 version
Lab2 link: https://pdos.csail.mit.edu/6.824/labs/lab-raft.html

## Lab 2: Raft
Raft paper: https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf

### Lab 2A: Leader election
Checkpoint saved in `branch: Lab_2A_rebuild`.

Lab2A由于第一版`branch: Lab_2A_Leader_Election`无法通过所有测试, 将第一版进行了重构, 主要的改变有：

1. 优化RPC请求, 对RPC进行了超时处理与异步化处理, 如在投票发起RequestVote RPC时, 为RPC设置超时上限(避免hung在已经disconnect的机器上), 同时发送RPC时采用异步发送的方式,单独启动一个异步协程进行发送和重试操作, 通过channel处理RPC结果。
   
2. ~~优化锁, 对变量进行修改均通过modify函数进行操作, 不在外部直接使用mutex, 因此对变量的变更类似原语, 保证了锁的粒度。此外通过使用命名锁的方式便于调试查看锁锁定的时间。~~没有对读加锁是完全错误的。

3. 优化协程同步模式, 对于定时器和其他操作, 原先使用sync.Cond时出现了很多问题, 因此将协程同步的方式改为了Channel, 定时器直接改为使用time.Timer。

### Lab 2B: Log
Checkpoint saved in `branch: Lab_2B_Log`.

基本实现思路按照Raft论文实现, ~~不过论文中给出AppendEntries的返回Reply仅有Term和Success两个参数, 我认为应该加上一个NextIndex参数便于Leader
更新matchIndex和nextIndex参数, 简单的为matchIndex加上len(args.Entries)是不正确的, 因为Follower在执行AppendEntries方法时不是从末尾Append的, 而是从
prevLogIndex处Append的, 如果在成功Append后在reply中加上表示Follower希望的下一个Index, 可以便于Leader正确的更新matchIndex和nextIndex参数。~~

当然最好的方法是将matchIndex和nextIndex在PrevLogIndex的基础上加上len(args.Entries), 经过测试, 这种方式不需要修改AppendEntries的参数, 且可以正常通过全部测试。

### Lab 2C: Persistence
Checkpoint saved in `branch: Lab_2C_Persistence`.

#### Dec 19:
主要基于6.824助教写的debug文档 https://thesquareplanet.com/blog/students-guide-to-raft/ 进行debug

#### Dec 20:
经过与其他人正确的代码进行比对, 导致`TestFigure8Unreliable2C`测试一直无法通过的原因可能是：
1. 锁的问题, 之前仅对变量修改加了锁, 对读没有加锁
2. channel变量设计问题, 其他人往往不会对AppendEntries设计一个Channel, 这个Channel可能会影响结果的正确性
3. 应该将用于处理发送ApplyMsg管道的协程和处理选举管道的协程分离开(Make函数中), 否则可能影响效率
4. 代码逻辑问题

#### Dec 21:
最终确定并fix的我的代码中存在的问题, 经过确认, 这些问题都会导致`TestFigure8Unreliable2C`测试无法通过:

1. AppendEntries中忘记了重置State为Follower, 修改Term值为args的值(如果大于自己),以及重置选举计时器。

2. 实现论文要求的选举等待时间有误, Raft论文要求在选举开始前等待一个随机时间后再开始选举, 但其实这个等待过程不需要在startElection这个方法中手动的去通过time.Sleep来完成, 我们仅需要通过在reset选举Timer时在一个基础值上加上一个随机值, 就相当于完成了随机等待这个过程。使用在startElection后手动sleep则需要在sleep完成后再次检查相关变量是否符合选举开始的条件, 显然这是一个非常失败的设计。

3. 锁的问题, 使用modify方法仅对变量写加锁是完全错误的做法, 且用lock锁住的临界区不应该包含RPC调用、time.Sleep等任何存在耗时过程的代码, 在测试时应使用go test -race参数来检查数据争用问题。

4. 在Leader发送AppendEntries RPC完成后, 更新其自身CommitIndex值时, 一个优化性能的点。由于按照要求当CommitIndex被更新时, 需要向ApplyMsgCh发送ApplyMsg表示确认该Entry被写入集群成功, 在这里检查时我每当CommitIndex被加1就发送一条ApplyMsg, 经检查这样会大幅降低该过程的执行速度, 而hasChanged这个标志位可以降低sendApplyMsg方法被调用的次数。

5. 根据助教写的debug指南, 发现之前的RequestVote存在逻辑漏洞, 例如确定投票给出后, 应该立即重置选举计数器; 如果自己是leader, 则应直接拒绝投票; 如果args中的Term大于自己则无论为什么state都需要自降为Follower, 这些逻辑在之前的代码里没有被if覆盖完整导致出现了问题。

6. 应专门开设一个协程用于发送ApplyMsg到ApplyCh, 同时在发送时获取锁, 保证向ApplyCh发送是完全串行的, 不能将发送ApplyCh功能的协程和发起选举或投票的协程混用, 否则会导致ApplyMsg发送不及时而无法通过测试。

#### Lab2C debug总结
经过这几天的dubug, 没有想到自己的代码竟然有这么多问题, 过程虽然很痛苦, 但是完成后还是非常有成就感的。其实这个`TestFigure8Unreliable2C`就是一个照妖镜, 之前Lab2a、Lab2b的测试其实很容易就可以通过, 并不能发现代码中存在的问题, `TestFigure8Unreliable2C`主要检测Raft集群能否快速的在网络出现长时间故障, 恢复后能否在10s内达成共识, 且会对刚完成写入的leader进行disconnect, 这个测试可以说是一个比较完整的测试了, 对性能也有一定的要求。我出现的那些问题大部分也不是玄学问题, 基本上就是逻辑漏洞, 写并发代码的能力还需提高。

### Lab 2D: Log compaction
Checkpoint saved in `branch: Lab_2C_Persistence`.

2D 部分是2021版新加入的，这个部分我花了很多时间才弄清楚Snapshot/InstallSnapshot/CondInstallShot三个RPC具体的含义和处理逻辑，在后文会对整个Raft的内部和外部调用进行整理。

主要Debug过程：

1. TestSnapshotInstall2D无法通过: sendAppendEntriesToFollower函数中忘记在正常的返回时修改跟随者NextIndex。

2. TestSnapshotInstallUnreliable2D无法通过: 在AppendEntries函数中如果reply.NextIndex == rf.Log.LastIncludedIndex, 同样也应该发送InstallSnapshot RPC。

3. TestSnapshotInstallCrash2D无法通过: peer crash从快照恢复后，应将lastApplied修改为LastIncludedIndex，直接置为0可能会导致活锁现象。

参考资料：

https://zhuanlan.zhihu.com/p/425615927

https://www.cnblogs.com/sun-lingyu/p/14591757.html


### Raft Milestone Review

Function fast review:

1. RequestVote: 内部RPC, 当ElectionTimer超时后, 任何Follower和Candidate都会发起一次选举，即使现在已经在选举Candidate状态也会发起一此新的选举。选举过程中Candidate会向所有人发送RequestVote RPC，在验证Candidate身份(term和index必须不能比自己旧)后，被调用方才会把票投过调用方，并重置自己的选举计时器。

2. AppendEntries: 内部RPC，当属于某个Follower的SendTimer超时后，Leader会根据自己维护的nextIndex切片查找该Follower的nextIndex，将自己所有的在其nextIndex后的logEntries发送给该Follower。AppendEntries被调用方会检查调用方的合法性，如果合法就会重置自己的ElectionTimer,然后会检查发送的entries能否被正确拼接，在找到和prevLogTerm和prevLogIndex匹配的位置才会执行拼接，原先的entries会直接被覆盖。优化后的AppendEntries会返回Follower希望收到的NextIndex，可以帮助Leader快速更新属于该Follower的next和match index, 同时Leader会检查所有Follower的matchIndex，过半数的Follower的matchIndex大于一个值后就将自己commitIndex修改为这个值，确定成功
   AppendEntries还会冲重置这个Follower的SendTimer。

3. Start: 外部调用，调用方会遍历所有peer，直到找到Leader，调用方会发送一条指令给leader，leader会将该指令加入自己的logEntries。

4. GetState: 外部调用，用于检查集群节点状态。

5. InstallSnapshot: 内部RPC, 当Leader完成对Follower的AppendEntries, 如果这个Follower返回的NextIndex的值小于该Leader的LastIncludedIndex(相当于这个Follower没有达到上一个快照点)，就会给这个Follower发送这个快照。InstallSnapshot接收方并不会立刻装载这个镜像，而是先在ApplyCh中发送将要装载这个镜像的请求给用户服务，真正装载这个快照则是在CondInstallSnapshot被外部调用之后。

6. CondInstallSnapshot: 外部调用，外部服务确认一个镜像可以被装载后，通过发起CondInstallSnapshot调用确认其可以安装这个镜像，这个设计是为了保证镜像切换的原子性，具体原因可以参考https://www.cnblogs.com/sun-lingyu/p/14591757.html。

7. Snapshot: 外部调用，外部服务通过这个调用主动的为一个peer装载一个镜像。


## Lab 3: Fault-tolerant Key/Value Service

### Lab 3A: Key/value service without snapshots

还有一些设计没有思考清楚：

1. 实验指南中的请求去重问题
应该是通过版本号机制解决，靠leader维护一个versionMap，对应每个client发送请求的版本进行维护，在applyCh返回确认后就为其版本号++，但需要解决如何跟踪正在处理中但还没被commit的Op

2. 是否应该在每个server上维护一个kvMap
每个server都可能成为leader，按理说经过raft投票的leader，其收到的Log应该是最新的状态，每个raft都维护一个kvMap应该可行。问题是这个leader切换的过程应该怎么处理，细节
   
3. Leader切换机制
我们应该以何种方式来知道Leader已经发生切换了，因为
