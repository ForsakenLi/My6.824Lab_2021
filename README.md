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

2D 部分是2021版新加入的, 这个部分我花了很多时间才弄清楚Snapshot/InstallSnapshot/CondInstallShot三个RPC具体的含义和处理逻辑, 在后文会对整个Raft的内部和外部调用进行整理。

主要Debug过程：

1. TestSnapshotInstall2D无法通过: sendAppendEntriesToFollower函数中忘记在正常的返回时修改跟随者NextIndex。

2. TestSnapshotInstallUnreliable2D无法通过: 在AppendEntries函数中如果reply.NextIndex == rf.Log.LastIncludedIndex, 同样也应该发送InstallSnapshot RPC。

3. TestSnapshotInstallCrash2D无法通过: peer crash从快照恢复后, 应将lastApplied修改为LastIncludedIndex, 直接置为0可能会导致活锁现象。

参考资料：

https://zhuanlan.zhihu.com/p/425615927

https://www.cnblogs.com/sun-lingyu/p/14591757.html


### Raft Milestone Review

Function fast review:

1. RequestVote: 内部RPC, 当ElectionTimer超时后, 任何Follower和Candidate都会发起一次选举, 即使现在已经在选举Candidate状态也会发起一此新的选举。选举过程中Candidate会向所有人发送RequestVote RPC, 在验证Candidate身份(term和index必须不能比自己旧)后, 被调用方才会把票投过调用方, 并重置自己的选举计时器。

2. AppendEntries: 内部RPC, 当属于某个Follower的SendTimer超时后, Leader会根据自己维护的nextIndex切片查找该Follower的nextIndex, 将自己所有的在其nextIndex后的logEntries发送给该Follower。AppendEntries被调用方会检查调用方的合法性, 如果合法就会重置自己的ElectionTimer,然后会检查发送的entries能否被正确拼接, 在找到和prevLogTerm和prevLogIndex匹配的位置才会执行拼接, 原先的entries会直接被覆盖。优化后的AppendEntries会返回Follower希望收到的NextIndex, 可以帮助Leader快速更新属于该Follower的next和match index, 同时Leader会检查所有Follower的matchIndex, 过半数的Follower的matchIndex大于一个值后就将自己commitIndex修改为这个值, 确定成功后AppendEntries还会重置这个Follower的SendTimer。

3. Start: 外部调用, 调用方会遍历所有peer, 直到找到Leader, 调用方会发送一条指令给leader, leader会将该指令加入自己的logEntries。

4. GetState: 外部调用, 用于检查集群节点状态。

5. InstallSnapshot: 内部RPC, 当Leader完成对Follower的AppendEntries, 如果这个Follower返回的NextIndex的值小于该Leader的LastIncludedIndex(相当于这个Follower没有达到上一个快照点), 就会给这个Follower发送这个快照。InstallSnapshot接收方并不会立刻装载这个镜像, 而是先在ApplyCh中发送将要装载这个镜像的请求给用户服务, 真正装载这个快照则是在CondInstallSnapshot被外部调用之后。

6. CondInstallSnapshot: 外部调用, 外部Server层确认一个镜像可以被装载后, 也就是从applyCh中收到Snapshot请求后, 通过发起CondInstallSnapshot调用确认Raft层可以安装这个镜像。

7. Snapshot: 外部调用, 当Server层希望创建一个镜像时, 会通过这个调用告诉Raft层希望快照的index, 并会将Server层和Raft层的所有需要保存的数据一并存入persister。


## Lab 3: Fault-tolerant Key/Value Service

### Lab 3A: Key/value service without snapshots

Checkpoint saved in `branch: Lab_3A_KV_WIO_Snapshots`.

#### 主要设计思路

使用Raft作为底层来完成kvServer, 集群里的每台机器都维护一个kvMap, 但仅由Leader响应Client的Get/Put/Append, 集群的非Leader机器仅通过applyCh返回的Op(相当于已经被Leader Commit过)来维护自己的kvMap, 因此无论是Leader还是Follower都需要使用一个协程来处理Raft层applyCh返回的数据。作为Leader的kvServer同时还会维护多个waitCh, 用于给Get/Put/Append等RPC回传Operation结果, 这些RPC在从waitCh收到Operation结果后才会给Client返回结果。

#### 请求去重

通过版本号机制解决, 针对每一个ClientID, 我们在一个Map里保存属于该Client的收到的请求的最大版本号。请求去重主要通过两个位置拦截, 一个是Leader Server端的Get/Put/Append RPC内(因为正常情况非Leader会直接返回ErrWrongLeader), 通过我们维护的versionMap来保证不会收到重复的请求。另一个地方是在响应applyCh的协程里, 在我们根据收到的Op数据修改我们的kvMap前, 同样需要检验该Op的Version是否大于之前保存的该ClientID对应的版本号, 这是为了防止由于某些特殊的网络故障导致重复的Op被发送到Raft中, 版本号仅会在Op成功执行后才会被更新。

#### 性能优化(For SpeedTest3A)

2021版的6.824的Lab 3新加入了一个针对本实验的kv的性能测试, 要求平均读写(写一条然后读取)时间小于33ms, 这个测试我花了几乎一天时间性能调优才通过。我发现在运行时测试时CPU的占用率很低, 说明瓶颈是由于某些同步原语等待导致的。我使用了golang的pprof工具来查找问题, 可以通过在go test后加上参数`-bench=. -blockprofile=block.prof -mutexprofile=mutex.prof`来生成性能评估文件, block.prof是阻塞等待的时间消耗而mutex.prof是锁的时间消耗, 我们可以使用`go tool pprof -http=:8080 block.prof`来以图形化的方式打开这些性能评估文件, 可以清晰的显示每行代码的时间消耗。评估显示我们的代码在等待发送AppendEntries的计时器超时处花了非常久, 这个SpeedTest3A是一个串行的测试, 只有确定写入操作成功完成并可以读出后(需要等待这个Op被半数以上commit)才会发送下一条指令, 这个指令等待被AppendEntries发送到其他Follower的时间(之前的设计是等待心跳计时器超时才会发送)就是我们的瓶颈所在。我通过在写入Raft的入口方法Start处加入一个channel来通知Leader一条指令被写入, 负责向其他Follower发送AppendEntries的协程收到channel通知后会立刻发送AppendEntries。经过这个修改, 我的读写延迟降低为了1ms左右, SpeedTest3A的1000条读写指令对仅需要1.3s。

### Lab 3B: Key/value service with snapshots

本轮实验较为容易, 加上了一个对Raft状态大小的判定, 如果当前的状态超过设定的阈值Server层就会发起一个Snapshot调用；对于落后于快照的peer, 则需要通过InstallSnapshot调用为其安装当前Leader的快照(和2D部分相同), 该peer会向applyCh发送一个snapshot请求, Server层收到该请求后向raft层发起CondInstallSnapshot调用, raft层就会安装该Snapshot。

## Lab 4: Sharded Key/Value Service

### Lab 4A: The Shard controller

本论实验的coding难度不高，有了Lab3的基础基本上可以很顺利的完成。主要思路是在每台机器上维护相同的config状态机，需要注意的一点是在shard需要重新balance的时候，我们需要找一台目前已经被分配的shard最少的gid, 这时我们需要使用一个map来记录每个gid当前已经被分配了多少个shard，在寻找最少的那个gid时，我们不能简单的随意取一个值最小的，因为golang的map在迭代时顺序是被刻意打乱的，在每台机器上访问的顺序都不一致，如果我们只是选择值最小的gid，那么在每台机器上选择的gid可能会不一致，导致最终各台机器上的状态不一致。所以我们在选择gid时需要加上一个条件，即值一致时选择gid更小的那一个，这样就可以保证在不同机器上选择的gid也是一样的了。
