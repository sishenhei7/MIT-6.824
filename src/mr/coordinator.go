package mr

import "log"
import "net"
import "os"
import "sync"
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
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func checkEvery(list int[], target int) bool {
	for _, val := range list {
		if (val != target) {
			return false
		}
	}
	return true
}

func (c *Coordinator) Work(args *RpcArgs, reply *RpcReply) error {
	c.mu.Lock()

	if (args.type == "map") {
		c.fileStatus[args.id] = 2
		c.mapDone = checkEvery(c.fileStatus, 2)
	} else if (args.type == "reduce") {
		c.reduceStatus[args.id] = 2
		c.reduceDone = checkEvery(c.fileStatus, 2)
	}

	reply := RpcReply{0, "done", ''}

	if (!c.mapDone) {
		reply.type = "wait"
		for id, file := range c.files {
			if (c.fileStatus[id] == 0) {
				reply.id = id
				reply.type = "map"
				reply.file = file
				c.fileStatus[id] = 1 // todo: 检测超时
				break
			}
		}
	} else if (!c.reduceDone) {
		reply.type = "wait"
		for id, val := range c.reduceStatus {
			if (val == 0) {
				reply.id = id
				reply.type = "reduce"
				c.fileStatus[id] = 1 // todo: 检测超时
				break
			}
		}
	}

	c.mu.Unlock()

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

	// Your code here.
	ret = c.reduceDone
	// for _, val := range c.reduceStatus {
	// 	if val != 2 {
	// 		ret = false
	// 	}
	// }

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

	c.server()
	return &c
}
