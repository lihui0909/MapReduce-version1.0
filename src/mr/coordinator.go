package mr

import (
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Task struct {
	FileName string
	IdMap    int
	IdReduce int
}

type Coordinator struct {
	// Your definitions here.
	State         int // 0 start 1 map 2 reduce
	NumMapTask    int
	NumReduceTask int
	MapTask       chan Task
	ReduceTask    chan Task
	MapTaskFin    chan bool
	ReduceTaskFin chan bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskResponse) error {
	if len(c.MapTaskFin) != c.NumMapTask {
		mapTask, ok := <-c.MapTask
		if ok {
			reply.XTask = mapTask
		}
	} else if len(c.ReduceTaskFin) != c.NumReduceTask {
		reduceTask, ok := <-c.ReduceTask
		if ok {
			reply.XTask = reduceTask
		}
	}
	//fmt.Printf("00000000000000000000  MapTask'len is %d, MapTaskFin's len is %d\n", len(c.MapTask), len(c.MapTaskFin))
	//fmt.Printf("00000000000000000000  ReduceTask'len is %d, ReduceTaskFin's len is %d\n", len(c.ReduceTask), len(c.ReduceTaskFin))
	reply.NumMapTask = c.NumMapTask
	reply.NumReduceTask = c.NumReduceTask
	reply.State = c.State
	return nil
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) TaskFin(args *ExampleArgs, reply *ExampleReply) error {
	if len(c.MapTaskFin) != c.NumMapTask {
		c.MapTaskFin <- true
		if len(c.MapTaskFin) == c.NumMapTask {
			c.State = 1
		}
	} else if len(c.ReduceTaskFin) != c.NumReduceTask {
		c.ReduceTaskFin <- true
		if len(c.ReduceTaskFin) == c.NumReduceTask {
			c.State = 2
		}
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
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

	if len(c.ReduceTaskFin) == c.NumReduceTask {
		ret = true
	}
	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		State:         0,
		NumMapTask:    len(files),
		NumReduceTask: nReduce,
		MapTask:       make(chan Task, len(files)),
		ReduceTask:    make(chan Task, nReduce),
		MapTaskFin:    make(chan bool, len(files)),
		ReduceTaskFin: make(chan bool, nReduce),
	}

	for id, file := range files {
		c.MapTask <- Task{FileName: file, IdMap: id}
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTask <- Task{IdReduce: i}
	}
	// Your code here.

	c.server()
	return &c
}
