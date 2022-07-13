package mr

import "fmt"
import "log"
import "time"
import "strconv"
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
type ByKey []mr.KeyValue

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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	reply := CallWork(0, "")

	for {
		if (reply.type == "map") {
			doMapWork(reply.id, reply.file, reply.nReduce, mapf)
			reply = CallWork(reply.id, "map")
		} else if (reply.type == "reduce") {
			doReduceWork(reply.id, reply.nReduce, reducef)
			reply = CallWork(reply.id, "reduce")
		} else if (reply.type == "wait") {
			time.Sleep(5 * time.Second)
		} else {
			break
		}
	}
}

func readFile(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return content
}

func doMapWork(id int, file string, nReduce int, mapf func(string, string) []KeyValue) {
	content := readFile(file)
	kva := mapf(filename, string(content))
	intermediate := []mr.KeyValue{}
	intermediate = append(intermediate, kva...)

	i := 0
	fileList := make([]*json.Encoder, nReduce)
	for i < nReduce {
		fileList[i] = json.NewEncoder("mr-" + strconv.Itoa(id) + "-" + strconv.Itoa(i))
	}

	for _, kv := range intermediate {
		idx = ihash(kv.key) % nReduce
		fileList[idx].Encode(&kv)
	}
}

func readJSONFile(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	dec := json.NewDecoder(file)

	var kva := []KeyValue{}
	for {
    var kv KeyValue
    if err := dec.Decode(&kv); err != nil {
      break
    }
    kva = append(kva, kv)
  }
	return kva
}

func doReduceWork(id int, nReduce int, reducef func(string, []string) string) {
	intermediate := readJSONFile("mr-*-" + strconv.Itoa(id))
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(id)
	ofile, _ := os.Create(oname)

	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallWork(id int, type string) {

	// declare an argument structure.
	args := RpcArgs{}

	// fill in the argument(s).
	args.id = id
	args.type = type

	// declare a reply structure.
	reply := RpcReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Work", &args, &reply)

	return reply
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
