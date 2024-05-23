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


type Coordinator struct {
	// Your definitions here.
	files []string
	nReduce int
	jobId int
	mapTasks []MapReduceTask
	reduceTasks []MapReduceTask
	mapComplete bool
	reduceComplete bool
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *EmptyRequest, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.jobId < len(c.files) {
		reply.Status = Map
		reply.Filename = c.files[c.jobId]
		reply.NReduce = c.nReduce
		reply.WorkerId = c.jobId
		reply.Timestamp = time.Now()
		c.mapTasks[c.jobId] = MapReduceTask{Timestamp: reply.Timestamp, Status: Map}
		c.jobId++
		return nil
	}
	for i := 0; i < len(c.mapTasks); i++ {
		if c.mapTasks[i].Status != Done {
			if time.Since(c.mapTasks[i].Timestamp) > 10 * time.Second {
				reply.Status = Map
				reply.Filename = c.files[i]
				reply.NReduce = c.nReduce
				reply.WorkerId = i
				reply.Timestamp = time.Now()
				c.mapTasks[i] = MapReduceTask{Timestamp: reply.Timestamp, Status: Map}
				return nil
			} else {
				reply.Status = Wait
				return nil
			}
		}
	}
	c.mapComplete = true
	if c.jobId < len(c.files) + c.nReduce {
		reply.Status = Reduce
		reply.NMap = len(c.files)
		reply.WorkerId = c.jobId - len(c.files)
		reply.Timestamp = time.Now()
		c.reduceTasks[reply.WorkerId] = MapReduceTask{Timestamp: reply.Timestamp, Status: Reduce}
		c.jobId++
		return nil
	}
	for i := 0; i < len(c.reduceTasks); i++ {
		if c.reduceTasks[i].Status != Done {
			if time.Since(c.reduceTasks[i].Timestamp) > 10 * time.Second {
				reply.Status = Reduce
				reply.NMap = len(c.files)
				reply.WorkerId = i
				reply.Timestamp = time.Now()
				c.reduceTasks[reply.WorkerId] = MapReduceTask{Timestamp: reply.Timestamp, Status: Reduce}
				return nil
			} else {
				reply.Status = Wait
				return nil
			}
		}
	}
	c.reduceComplete = true
	reply.Status = Done
	return nil
}

func (c *Coordinator) FinishTask(args *Request, reply *EmptyReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Status == Map {
		c.mapTasks[args.WorkerId].Status = Done
	} else if args.Status == Reduce {
		c.reduceTasks[args.WorkerId].Status = Done
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
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.mapComplete && c.reduceComplete
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files, nReduce: nReduce, jobId: 0, mapTasks: make([]MapReduceTask, len(files)), reduceTasks: make([]MapReduceTask, nReduce), mapComplete: false, reduceComplete: false}

	// Your code here.
	c.server()
	return &c
}
