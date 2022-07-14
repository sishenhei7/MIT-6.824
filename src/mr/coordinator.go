package mr

import "log"
import "net"
import "os"
import "sync"
import "time"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
	files []string
	fileStatus []int // 0表示未分配，1表示已分配，2表示已成功
	reduceStatus []int // 0表示未分配，1表示已分配，2表示已成功
	mapDone bool
	reduceDone bool
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func checkEvery(list []int, target int) bool {
	for _, val := range list {
		if (val != target) {
			return false
		}
	}
	return true
}

func (c *Coordinator) Work(args *RpcArgs, reply *RpcReply) error {
	c.mu.Lock()

	if (args.Name == "map") {
		c.fileStatus[args.Id] = 2
		c.mapDone = checkEvery(c.fileStatus, 2)
	} else if (args.Name == "reduce") {
		c.reduceStatus[args.Id] = 2
		c.reduceDone = checkEvery(c.reduceStatus, 2)
	}

	reply.Name = "done"
	reply.NReduce = c.nReduce

	if (!c.mapDone) {
		reply.Name = "wait"
		for id, file := range c.files {
			if (c.fileStatus[id] == 0) {
				reply.Id = id
				reply.Name = "map"
				reply.File = file
				c.fileStatus[id] = 1
				go c.timeoutRecover(reply.Name, reply.Id)
				break
			}
		}
	} else if (!c.reduceDone) {
		reply.Name = "wait"
		for id, val := range c.reduceStatus {
			if (val == 0) {
				reply.Id = id
				reply.Name = "reduce"
				c.reduceStatus[id] = 1
				go c.timeoutRecover(reply.Name, reply.Id)
				break
			}
		}
	}

	c.mu.Unlock()

	return nil
}

func (c *Coordinator) timeoutRecover(name string, id int) {
	time.Sleep(60 * time.Second)

	c.mu.Lock()
	if (name == "map" && c.fileStatus[id] == 1) {
		c.fileStatus[id] = 0
	} else if (name == "reduce" && c.reduceStatus[id] == 1) {
		c.reduceStatus[id] = 0
	}
	c.mu.Unlock()
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

	// Your code here.
	c.mu.Lock()
	ret = c.reduceDone
	c.mu.Unlock()

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
	c.files = files
	c.fileStatus = make([]int, len(files))
	c.reduceStatus = make([]int, nReduce)
	c.mapDone = false
	c.reduceDone = false
	c.nReduce = nReduce

	c.server()
	return &c
}
