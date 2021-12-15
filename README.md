## My 6.824 Lab 2021 version
Lab2 link: https://pdos.csail.mit.edu/6.824/labs/lab-raft.html

### Lab 2A: Leader election
Checkpoint saved in `branch: Lab_2A_rebuild`.

Lab2A由于第一版`branch: Lab_2A_Leader_Election`无法通过所有测试，将第一版进行了重构，主要的改变有：

1. 优化RPC请求, 对RPC进行了超时处理与异步化处理, 如在投票发起RequestVote RPC时, 为RPC设置超时上限(避免hung在已经disconnect的机器上), 同时发送RPC时采用异步发 送的方式,单独启动一个异步协程进行发送和重试操作, 通过channel处理RPC结果。
   
2. 优化锁，对变量进行修改均通过modify函数进行操作，不在外部直接使用mutex，因此对变量的变更类似原语，保证了锁的粒度。此外通过使用命名锁的方式便于调试查看锁锁定的时间。

3. 优化协程同步模式，对于定时器和其他操作，原先使用sync.Cond时出现了很多问题，因此将协程同步的方式改为了Channel, 定时器直接改为使用time.Timer。