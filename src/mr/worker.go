package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		// declare an argument structure.
		args := TaskRequest{}
		// declare a reply structure.
		reply := TaskResponse{}
		CallGetTask(&args, &reply)
		state := reply.State
		//fileName := reply.XTask.FileName 不需要再判断fileName
		//(如果返回有fileName说明是map任务)
		if state == 0 {
			id := strconv.Itoa(reply.XTask.IdMap)
			fileName := reply.XTask.FileName
			file, err := os.Open(fileName)
			if err != nil {
				log.Fatalf("cannot open mapTask %v", fileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", fileName)
			}
			file.Close()
			kva := mapf(fileName, string(content))
			//接下来要把kva写到中间文件中去
			//生成与reduce任务个数相同的中间文件
			numReduce := reply.NumReduceTask
			bucket := make([][]KeyValue, numReduce)
			//把kva中的每一个键值对取哈希，放到对应的bucket中,分桶
			for _, kv := range kva {
				num := ihash(kv.Key) % numReduce
				bucket[num] = append(bucket[num], kv)
			}
			//分完桶以后根据reduce的个数写入临时文件tmpFile
			for i := 0; i < numReduce; i++ {
				tmpFile, error := ioutil.TempFile("", "mr-map-*")
				if error != nil {
					fmt.Printf("error is : %+v\n", error)
					log.Fatal("cannot open map tmpFile")
				}
				//用json向tmpFile中写bucket，参考课程提示
				//enc为 *json.Encoder格式，代表以json格式编码往tmpFile文件中写
				enc := json.NewEncoder(tmpFile)
				//把bucket[i]的内容传递给enc
				err := enc.Encode(bucket[i])
				if err != nil {
					log.Fatal("encode bucket error")
				}
				tmpFile.Close()
				//根据课程提示，把中间文件rename
				outFileName := `mr-` + id + `-` + strconv.Itoa(i)
				os.Rename(tmpFile.Name(), outFileName)
			}
			//map任务完成后向MapTaskFin中发送一个true
			//reply.MapTaskFin <- true 改为RPC调用向channel中发送true
			CallTaskFin()
		} else if state == 1 {
			//否则就是reduce
			//每一个reduce的任务取nMap个中间文件，取对应的最后一个数字为自己task id的文件，因为中间的文件的名字最后一个数字是取哈希值得到的
			numMap := reply.NumMapTask
			id := strconv.Itoa(reply.XTask.IdReduce)
			//说明所有的map任务完成，开始reduce
			//kva保存从中间文件中读出的key value对
			intermediate := []KeyValue{}
			//reduce开始要开始读中间文件
			for i := 0; i < numMap; i++ {
				mapFileName := "mr-" + strconv.Itoa(i) + "-" + id
				//inputFile为 *os.File格式，读中间文件
				inputFile, err := os.OpenFile(mapFileName, os.O_RDONLY, 0777)
				if err != nil {
					log.Fatalf("cannot open reduceTask %v", mapFileName)
				}
				// 将inputFile按照json格式解析
				dec := json.NewDecoder(inputFile)
				for {
					var kv []KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv...)
				}
				//以上把中间文件中读出的键值对保存到了intermediate中
				sort.Sort(ByKey(intermediate))

				//准备整合，去重

				//创建一个tmpFile，存放reduce输出结果
				outFileName := "mr-out-" + id
				tmpFile, err := ioutil.TempFile("", "mr-reduce-*")
				if err != nil {
					log.Fatalf("cannot open reduce tmpFile")
				}

				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)

					//向tmpFile里去写入拼好的output
					fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)
					//再遍历下一批key相等的键值对
					i = j
				}
				tmpFile.Close()
				//改名字
				os.Rename(tmpFile.Name(), outFileName)
			}
			//reply.ReduceTaskFin <- true 改为调用RPC向Fin的channel中写入true
			CallTaskFin()
			//退出for循环的条件，reduce的任务已全部完成
			//if len(reply.ReduceTaskFin) == reply.NumReduceTask {
			//break
			//}
		} else {
			//state == 2的情况
			break
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func CallGetTask(args *TaskRequest, reply *TaskResponse) {
	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		fmt.Printf("CallGetTask method - reply.FileName %s\n", reply.XTask.FileName)
	} else {
		fmt.Printf("CallGetTask method failed!\n")
	}
}

func CallTaskFin() {
	// 这两个参数没有用，但是call方法必须要给参数
	args := ExampleArgs{}
	// declare a reply structure.
	reply := ExampleReply{}
	ok := call("Coordinator.TaskFin", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("CallTaskFin method ok\n")
	} else {
		fmt.Printf("CallTaskFin method failed!\n")
	}
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
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
