# MIT6.824

MIT6.824课程学习

* 关于多线程和并发的一些基础知识

对于worker而言，它的工作只是处理coordinator分配给它的任务即可，所以它不需要进行多线程操作，完成一个单线程的串行任务即可

对于coordinator而言，它需要维护一个主线程（goroutine），每收到一个worker发来的请求，就开启一个goroutine进行并行处理

* 待学习任务

1. go channel
2. shell脚本
3. linux文件系统

## lab1 

使用Mutex锁进行多线程数据的同步

## lab2

* rpc通信原理理解

Client和Server通过chan进行通信。下面是典型的利用chan进行通信的结构体设计：

```go
type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	me        int                 // this peer's index into peers[]
}

type ClientEnd struct {
    endname interface{}   // this end-point's name
    ch      chan reqMsg   // copy of Network.endCh
    done    chan struct{} // closed when Network is cleaned up
}

type reqMsg struct {
    endname  interface{} // name of sending ClientEnd
    svcMeth  string      // e.g. "Raft.AppendEntries"
    argsType reflect.Type
    args     []byte
    replyCh  chan replyMsg
}

type replyMsg struct {
    ok    bool
    reply []byte
}
```

rpc call过程：首先把请求数据从args中解析出到req中，然后把req发送到e.ch通道。

发送过去后，等待从req.replyCh中获取处理结果。根据从管道中返回的结果判断是否处理完成

完整的call函数如下所示：

```go
// Call send an RPC, wait for the reply.
// the return value indicates success; false means that
// no reply was received from the server.
func (e *ClientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	req := reqMsg{}
	req.endname = e.endname
	req.svcMeth = svcMeth
	req.argsType = reflect.TypeOf(args)
	req.replyCh = make(chan replyMsg)

	qb := new(bytes.Buffer)
	qe := labgob.NewEncoder(qb)
	if err := qe.Encode(args); err != nil {
		panic(err)
	}
	req.args = qb.Bytes()

	//
	// send the request.
	//
	select {
	case e.ch <- req:
		// the request has been sent.
	case <-e.done:
		// entire Network has been destroyed.
		return false
	}

	//
	// wait for the reply.
	//
	rep := <-req.replyCh
	if rep.ok {
		rb := bytes.NewBuffer(rep.reply)
		rd := labgob.NewDecoder(rb)
		if err := rd.Decode(reply); err != nil {
			log.Fatalf("ClientEnd.Call(): decode reply: %v\n", err)
		}
		return true
	} else {
		return false
	}
}
```

* raft算法分解

为了增加raft算法的可理解性，将raft算法分为四个部分：leader选举，日志复制，安全性和成员变更

2A：完成leader选举部分代码

* RequestVote rpc方法：

需要考虑因素：双方的任期、自己是否已经投过票（或者投票对象是否为candidateId）、双方的日志是否一样新

如果args.term < rf.currentTerm，直接返回false；
如果args.term > rf.currentTerm，做出投票，状态转为follower
任期相同：
    如果自己已经投过票，不再投票；否则判断日志是否一样新，再决定是否投票

如果决定要投票，那么需要rf.voteFor = args.CandidateId，重置超时时间，设定返回参数的任期和是否投票
只要决定要投票，这些事情执行过程相同，所以可以将不投票的情况排除，只留下投票的情况，降低代码复杂度和可读性

决定不投票的情况：
1. args.term < rf.currentTerm
2. 自己已经投过票
3. 日志和自己不一样新

决定投票但是还需要做一些额外的事情：
1. args.term > rf.currentTerm时，需要转换状态为follower

选举超时时间:[300ms, 400ms]
固定心跳间隔:150ms