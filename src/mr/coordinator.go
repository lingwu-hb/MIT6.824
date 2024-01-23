package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type State int

const (
	Idle State = iota
	InProgress
	Completed
)

type Coordinator struct {
	// Your definitions here.
	mu            sync.RWMutex
	MapTaskInc    int
	ReduceTaskInc int
	// 待处理的任务信息
	PendingTaskNum  int
	nMap            int // not edit, show the number of map number
	nReduce         int // not edit, show the number of reduce number
	taskInfos       []TaskInfo
	MapTaskState    map[int]State
	ReduceTaskState map[int]State
	// Workder的信息

	// 中间数据的信息
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) InitCoordinator(files []string, nReduce int) {
	c.nMap = len(files)
	c.nReduce = nReduce
	c.PendingTaskNum = c.nMap + c.nReduce
	// 默认状态下为idle状态
	c.MapTaskState = make(map[int]State, 0)
	c.ReduceTaskState = make(map[int]State, 0)
	c.taskInfos = make([]TaskInfo, 0)
	c.makeMapTasks(files) // 待分配的任务
}

func (c *Coordinator) GetTaskId(tasktype TaskTypes) (id int) {
	if tasktype == Map {
		id = c.MapTaskInc
		c.MapTaskInc += 1
	} else if tasktype == Reduce {
		id = c.ReduceTaskInc
		c.ReduceTaskInc += 1
	}
	return
}

// MapInProgress the v taskInfo is being progress state
func (c *Coordinator) MapInProgress(v TaskInfo) {
	c.MapTaskState[v.MapId] = InProgress
}

// ReduceInProgress the v taskInfo is being progress state
func (c *Coordinator) ReduceInProgress(i int) {
	c.ReduceTaskState[i] = InProgress
}

// TaskDone one Task is completed done
//func (c *Coordinator) TaskDone() {
//	c.mu.Lock()
//	defer c.mu.Unlock()
//	c.PendingTaskNum -= 1
//}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// DistributeTask worker call the function to get a task
func (c *Coordinator) DistributeTask(args *ExampleArgs, reply *TaskInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// log.Printf("the number of tasks is: %v", c.PendingTaskNum)
	if !c.mapIsDone() {
		// 返回一个没有处理过的map task
		for _, v := range c.taskInfos {
			if c.MapTaskState[v.MapId] == Idle {
				// log.Printf("the number of c.taskInfos: %v", len(c.taskInfos))
				*reply = TaskInfo{
					TaskFile: v.TaskFile,
					TaskType: v.TaskType,
					MapId:    v.MapId,
					ReduceId: v.ReduceId,
				}
				c.MapInProgress(v)
				go func(id int) {
					// 宕机时间设定为一秒
					time.Sleep(10 * time.Second)
					c.mu.Lock()
					defer c.mu.Unlock()
					if c.MapTaskState[id] != Completed {
						c.MapTaskState[id] = Idle
					}
				}(v.MapId)
				break
			}
		}
	} else {
		for i := 0; i < c.nReduce; i++ {
			if c.ReduceTaskState[i] == Idle {
				*reply = TaskInfo{
					TaskType: Reduce,
					ReduceId: i,
				}
				c.ReduceInProgress(i)
				go func(id int) {
					time.Sleep(1 * time.Second)
					c.mu.Lock()
					defer c.mu.Unlock()
					if c.ReduceTaskState[id] != Completed {
						c.ReduceTaskState[id] = Idle
					}
				}(i)
				break
			}
		}
	}
	return nil
}

func (c *Coordinator) makeMapTasks(files []string) {
	for _, v := range files {
		task := TaskInfo{
			TaskFile: []string{v},
			TaskType: Map,
			MapId:    c.GetTaskId(Map),
			ReduceId: 0,
		}
		c.taskInfos = append(c.taskInfos, task)
	}
	return
}

// MapCompleted worker call the function to tell
// the coordinator one map task is completed
func (c *Coordinator) MapCompleted(args *TaskInfo, reply *ExampleReply) error {
	// 共享数据加锁进行保护
	c.mu.Lock()
	defer c.mu.Unlock()
	c.MapTaskState[args.MapId] = Completed
	if c.PendingTaskNum > 0 {
		c.PendingTaskNum -= 1
	}
	//c.taskInfos[args.MapId].TaskType = Reduce
	//c.taskInfos[args.MapId].ReduceId = c.GetTaskId(Reduce)
	return nil
}

// ReduceCompleted worker call the function to tell
// the coordinator one reduce task is completed
func (c *Coordinator) ReduceCompleted(args *TaskInfo, reply *ExampleReply) error {
	// 共享数据加锁进行保护
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ReduceTaskState[args.ReduceId] = Completed
	if c.PendingTaskNum > 0 {
		c.PendingTaskNum -= 1
	}
	return nil
}

// mapIsDone judge if all map tasks have been done
func (c *Coordinator) mapIsDone() bool {
	for _, v := range c.taskInfos {
		if c.MapTaskState[v.MapId] != Completed {
			return false
		}
	}
	return true
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	// 将c中满足条件的方法注册为rpc服务
	rpc.Register(c)
	// 将RPC信息注册到对应的http path中
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	// distribute tasks
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.PendingTaskNum == 0 {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.InitCoordinator(files, nReduce)

	c.server()
	return &c
}
