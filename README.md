# MIT6.824-lab2

这是MIT6.824分布式系统lab2实验记录，该实验在课程组所给出的框架下，初步实现了共识算法raft，包括领导人选举、日志追加、日志持久化、日志快照等功能，并提供了几个接口供上层应用可以使用该raft层实现简单的分布式应用（比如lab3就是基于lab2的raft算法 实现了一个键值对存储的服务）

##  整体架构

![raft架构](raft架构.jpg)

##  实现细节

**节点状态**：raft算法中每个节点都有三种状态，分别是leader，follower，candidate，其互相转换规则如下![图 4 ](https://github.com/maemual/raft-zh_cn/raw/master/images/raft-%E5%9B%BE4.png)

* 正常情况下，只有一个leader，其余都是follower，leader会持续运行，直到leader出现故障crush或网络出现波动，导致follower变成candidate竞选新leader
* follower从不主动发起RPC请求，而只是响应leader或candidate的RPC，如果它在一段时间内（election timeout）没有收到任何RPC，它就成为candidate，并发起election（leader如果正常运转，会在空闲时间不断发送heartbeat消息保证它的权威）

**任期Term**：raft将term作为逻辑时钟使用，让每个节点可以分辨过期的信息以及过期的leader

**日志log**：raft中每个日志都存储了三个信息，一个是上层应用传下来的command，一个是接收到命令时的term值，一个是顺序增长的日志索引index值，定义如下，(Index0存储了整个日志最开始的日志索引值)

```go
type Log struct {
	Entries []Entry
	Index0  int
}

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}
```

由于在实现日志快照后，index0将会不断变化，日志的裁剪、求日志的长度、取最后一个日志等等操作都会变得较为复杂，故在log.go中封装了一系列针对日志的函数，比如cutend()函数：裁剪尾部日志

```go
func (l *Log) cutend(idx int) {
	l.Entries = l.Entries[0 : idx-l.Index0]
}
```

**领导人选举**

* follower增加自己的current term，并切换为candidate

* 它给自己投票，并调用Request Vote RPC让其他server投票（投票者应该将竞选的term记录到自己的current term）

  * 判断规则：用log里最后一个条目的term和idx来判断哪个更新，更大的term更新，相同term更长的更新
  * 投票规则：如果投票者自己的日志比candidate更新，它就会拒绝投票

* 如果一个candidate首个赢得了大多数同一term的server的投票，即变成leader，而一旦变成leader，立刻给所有server发送heartbeat消息，宣告权威并停止其他竞选

* 如果一个candidate在竞选期间，收到了自称是leader的Append Entries RPC

  * 该”leader“的term比candidate低：candidate拒绝该请求，继续竞选
  * 该“leader”的term不小于candidate：candidate放弃竞选，转为follower

* 如果多个candidate分票，导致选不出leader，每个candidate就超时并增加current term重新开始另一轮RequestVote RPC

  * raft使用随机选举超时来确保这种情况很少发生：每个server的election timeout事件是从一个区间里随机选取的，采用分散的方法来应对这种分票的情况

    ```go
    //重置选举超时时间：350~700ms
    func (rf *Raft) resetElectionTimer() {
    	t := time.Now()
    	t = t.Add(350 * time.Millisecond)
    	ms := rand.Int63()%350
    	t = t.Add(time.Duration(ms) * time.Millisecond)
    	rf.electionTime = t
    }
    ```

**日志追加**

* leader接收client的请求，并将这些命令放到自己的log里，然后并行调用Append Entries RPC把这些日志条目发给所有的follower

* 在收到大多数follower的确认收到后，leader执行（committed）该条目，并apply，同时在每个Append Entries RPC中，它都会将自己的commitindex传给其他的follower以让它们也更新自己的commitindex

  ```go
  //更新follower的commitindex，并尝试唤醒applier线程
  if args.LeaderCommit > rf.commitIndex {
  	rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
  	rf.applyCond.Broadcast()
  }
  ```

* leader会给每个follower都维护一个nextidx，是leader即将发给这个follower的下个log entries的索引

  * 当leader刚选中时，每个follower的nextidx就是leader最后一个日志后的索引

  * leader发送给follower的log entries里包含了prelogindex和prelogterm，指向了leader要发送日志的前一个日志，如果follower发现前一个日志跟leader不同，则AppendEntriesRPC失败

  * leader随即修改这个follower的nextidx将其递减，然后继续重复步骤二，直到follower的前一个日志跟leader要发送日志的前一个日志匹配

    * 优化：在follower返回调用AppendEntries失败的时候，附带上Xterm，Xindex，Xlen，帮助leader定位到发生冲突的term值下的第一个日志

      ```go
      	for xIndex := args.PrevLogIndex; xIndex > rf.log.Index0 ; xIndex-- {
      	if rf.log.at(xIndex-1).Term != xTerm {
      		reply.XIndex = xIndex
      		break
      	}
      }
      ```

  * 随后follower那个匹配成功日志后的所有日志都删除，存上自己发给他的日志

**日志持久化**

该部分较简单，只需在需要持久化的状态currentTerm，votedFor，log三者任一发生变化时，立刻调用`persist()`保存raftstate即可

其中currentTerm保存leader的当前任期值，votedFor保存follower的投票选择，log保存节点的日志，而诸如commitindex、lastapplied、nextindex[]、matchindex[]等状态，都可以通过保存的log，在Append Entries RPC的发送和返回中逐步调整恢复，故不需要保存

**日志快照**

* 为了防止日志随着增长越来越大重放时间越来越长，所以使用snapshot，对一部分早期的日志进行快照

* 快照内容除了包括应用数据信息，还包括了一些元数据信息：如快照中最后一个日志包含的term以及index

* 一旦系统将快照持久化到了磁盘上，就可以删除快照最后一个日志之前的日志以及快照

* 有时候，follower的进度太慢了，leader已经把应该发给它的日志给删除了，这个时候leader必须通过installsnapshot RPC给follower发送快照

  * 如果这个快照里包含了follower所没有的日志信息（也就是快照里的日志进度比follower所有的日志进度快），follower就会接收该快照，更新到自己的快照信息中

  > 在本处实现上，我选择follower在接收installsnapshot RPC时并不直接删除快照中所包含的日志，而是在上层应用调用`CondInstallSnapshot`判断成功应用快照后，才真正进行日志的裁剪
  >
  > 因为在实现逻辑上，只有当上层应用调用`CondInstallSnapshot`判断可以安装快照之后，raft节点上快照所包含的日志信息才真正失效

该部分较为繁琐，涉及到很多关于日志的判断和操作，由于在该部分中日志的Index0也将发生变化，故也需要重构之前的日志追加功能，包括follower日志为空时的处理、包括leader日志为空时的处理等等

我选择在节点创造后的最开始时，在日志下标为0处保存一个空日志（方便还未接收上层应用的command时，可以顺利实现leader所发送的心跳包逻辑），而在日志进行快照而裁剪后，日志下标为0处即为有效日志，此时有两种情况

* 日志不为空，该情况不影响心跳包逻辑的处理
* 日志为空，在该情况下，我选择持久化lastsnapshotIndex、lastsnapshotTerm，使用此两个变量代行心跳包逻辑处理中prevLogIndex，prevLogTerm的功能
