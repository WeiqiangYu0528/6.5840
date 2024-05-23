package mr

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type Counter struct {
	wg sync.WaitGroup
}

func NewCounter(n int) *Counter {
	c := &Counter{}
	c.wg.Add(n)
	return c
}

func (c *Counter) Done() {
	c.wg.Done()
}

func (c *Counter) Wait() {
	c.wg.Wait()
}

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
		err := GetTask(mapf, reducef)
		if err != nil {
			break
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func GetTask(mapf func(string, string) []KeyValue, reducef func(string, []string) string) error {
	emptyArgs := EmptyRequest{}
	EmptyReply := EmptyReply{}
	args := Request{}
	reply := Reply{}
	ok := call("Coordinator.AssignTask", &emptyArgs, &reply)
	if !ok {
		return errors.New("error getting task")
	}
	if reply.Status == Map {
		runMap(mapf, &reply)
		args = Request{Status: Map, WorkerId: reply.WorkerId}
		ok := call("Coordinator.FinishTask", &args, &EmptyReply)
		if !ok {
			return errors.New("error finishing map task")
		} 
	} else if reply.Status == Reduce {
		runReduce(reducef, &reply)
		args = Request{Status: Reduce, WorkerId: reply.WorkerId}
		ok := call("Coordinator.FinishTask", &args, &EmptyReply)
		if !ok {
			return errors.New("error finishing reduce task")
		} 
	} else if reply.Status == Wait {
		time.Sleep(time.Second)	
	} else {
		os.Exit(0)
	}
	return nil;
}

func runMap(mapf func(string, string) []KeyValue, reply *Reply) {
	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("cannot open map %v", reply.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}
	file.Close()
	intermediate := make([][]KeyValue, reply.NReduce)
	temp := mapf(reply.Filename, string(content))
	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	sort.Sort(ByKey(temp))

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(temp) {
		j := i + 1
		for j < len(temp) && temp[j].Key == temp[i].Key {
			j++
		}
		key := ihash(temp[i].Key) % reply.NReduce
		for k := i; k < j; k++ {
			intermediate[key] = append(intermediate[key], KeyValue{temp[k].Key, temp[k].Value})
		}
		i = j
	}

	counter := NewCounter(reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		go func(idx int) {
			oname := fmt.Sprintf("mr-%v-%v", reply.WorkerId, idx)
			ofile, err := os.OpenFile(oname, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
			if err != nil {
				fmt.Println("Error opening file:", err)
				return
			}
			for i := 0; i < len(intermediate[idx]); i++ {
				fmt.Fprintf(ofile, "%v %v\n", intermediate[idx][i].Key, intermediate[idx][i].Value)
			}
			counter.Done()
		}(i)
	}
	counter.Wait()
}

func runReduce(reducef func(string, []string) string, reply *Reply) {
	intermediate := []KeyValue{}
	counter := NewCounter(reply.NMap)
	var mu sync.Mutex

	for i := 0; i < reply.NMap; i++ {
		go func(idx int) {
			filename := fmt.Sprintf("mr-%v-%v", idx, reply.WorkerId)

			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open file of %v: %v", filename, err)
			}
			defer file.Close()

			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v: %v", filename, err)
			}

			lines := strings.Split(string(content), "\n")
			if len(lines) > 0 && lines[len(lines)-1] == "" {
				lines = lines[:len(lines)-1]
			}

			mu.Lock()
			for _, line := range lines {
				kv := strings.Split(line, " ")
				if len(kv) == 2 {
					intermediate = append(intermediate, KeyValue{kv[0], kv[1]})
				}
			}
			mu.Unlock()
			counter.Done()
		}(i)
	}

	counter.Wait()

	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%v", reply.WorkerId)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create %v: %v", oname, err)
	}
	defer ofile.Close()
	

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

		// This is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
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
