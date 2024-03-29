package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Exists(path string) bool {
	_, err := os.Stat(path) //os.Stat获取文件信息
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// 通过rpc通信询问task name 和 task type
	for {
		// 设定为other，如果没有从call函数接收到具体的要求，就不允许进行task任务操作
		taskinfo := TaskInfo{}
		ok := call("Coordinator.DistributeTask", &ExampleArgs{}, &taskinfo)
		if !ok {
			// 调用失败
			log.Printf("Coordinator.DistributeTask call failed!\n")
			break
		}
		// log.Printf("test for shell output: %v", taskinfo.TaskType)

		// fmt.Printf("%v\n", taskinfo)
		if taskinfo.TaskType == Map {
			// log.Printf("handling map id is :%v", taskinfo.MapId)
			if err := mapTask(taskinfo, mapf); err != nil {
				fmt.Printf("handle map task failed!\n")
			}

			if ok := call("Coordinator.MapCompleted", &taskinfo, &ExampleReply{}); !ok {
				// 调用失败
				log.Printf("Coordinator.MapCompleted call failed!\n")
				break
			}
		} else if taskinfo.TaskType == Reduce {
			// log.Printf("handling reduce id is :%v", taskinfo.ReduceId)
			if err := reduceTask(taskinfo, reducef); err != nil {
				fmt.Printf("handle reduce task failed!\n")
			}

			if ok := call("Coordinator.ReduceCompleted", &taskinfo, &ExampleReply{}); !ok {
				// 调用失败
				log.Printf("Coordinator.ReduceCompleted call failed! the ReduceId is %v\n", taskinfo.ReduceId)
				break
			}
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func mapTask(taskInfo TaskInfo, mapf func(string, string) []KeyValue) (err error) {
	// mapf函数进行处理操作
	// 由于此处的文件都不大，所有TaskFile里面只有一个文件
	// file, err := os.Open("../main/" + taskinfo.TaskFile[0])
	file, err := os.Open(taskInfo.TaskFile[0])
	if err != nil {
		log.Fatalf("cannot open %v", taskInfo.TaskFile[0])
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", taskInfo.TaskFile[0])
	}
	file.Close()
	intermediate := mapf(taskInfo.TaskFile[0], string(content))

	sort.Sort(ByKey(intermediate))

	// 此处的10 = NReduce，应该由coordinator传过来，简化处理
	buckets := make([][]KeyValue, 10)
	for i := range buckets {
		buckets[i] = []KeyValue{}
	}

	for _, kva := range intermediate {
		buckets[ihash(kva.Key)%10] = append(buckets[ihash(kva.Key)%10], kva)
	}

	// write into intermediate files
	for i := range buckets {
		oname := "mr-" + strconv.Itoa(taskInfo.MapId) + "-" + strconv.Itoa(i)
		// look up for the function
		ofile, _ := ioutil.TempFile("", oname+"*")
		enc := json.NewEncoder(ofile)
		for _, kva := range buckets[i] {
			err := enc.Encode(&kva)
			if err != nil {
				log.Fatalf("cannot write into %v", oname)
			}
		}
		os.Rename(ofile.Name(), oname)
		ofile.Close()
	}

	// 应该分成十份内容，然后保存到对应的文件夹中 rm-X-Y （待优化）
	//X := strconv.Itoa(taskInfo.MapId)
	//onamePre := "mr-" + X + "-"
	//for _, v := range intermediate {
	//	Y := strconv.Itoa(ihash(v.Key) % 10)
	//	oname := onamePre + Y
	//	var Ofile *os.File
	//	var err1 error
	//	if Exists(oname) { // 如果文件存在
	//		Ofile, err1 = os.OpenFile(oname, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	//		if err1 != nil {
	//			log.Fatalf("can not open file: %v", oname)
	//		}
	//	} else {
	//		Ofile, err1 = os.Create(oname)
	//		if err1 != nil {
	//			log.Fatalf("can not create file: %v", oname)
	//		}
	//	}
	//	// fmt.Fprintf(Ofile, "%v %v\n", v.Key, v.Value)
	//	enc := json.NewEncoder(Ofile)
	//	if err2 := enc.Encode(&v); err2 != nil {
	//		log.Fatalf("can not encoder file: %v; the err is: %v", oname, err2)
	//	}
	//	Ofile.Close()
	//}
	return nil
}

func reduceTask(taskInfo TaskInfo, reducef func(string, []string) string) (err error) {
	// 将结果保存到mr-out-Y中
	Y := strconv.Itoa(taskInfo.ReduceId)
	oname := "mr-out-" + Y
	Ofile, _ := os.Create(oname)

	// 读取文件到intermediate中
	intermediate := []KeyValue{}
	i := 0
	for {
		interFileName := "mr-" + strconv.Itoa(i) + "-" + Y
		if !Exists(interFileName) { // 文件不存在
			break
		}
		interFile, err1 := os.Open(interFileName)
		if err1 != nil {
			log.Fatalf("can not open file: %v", interFileName)
		}
		dec := json.NewDecoder(interFile)
		for {
			var kv KeyValue
			if err2 := dec.Decode(&kv); err2 != nil {
				// 当读取到文件结尾时候，err2会等于EOF
				// log.Fatalln(err2)
				break
			}
			intermediate = append(intermediate, kv)
		}
		interFile.Close()
		i++
	}

	sort.Sort(ByKey(intermediate))

	i = 0
	for i < len(intermediate) {
		j := i + 1
		// find the next place with different key
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(Ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	Ofile.Close()
	return
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
		// log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}
