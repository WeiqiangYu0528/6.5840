package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Define the enum using iota
type Status int

const (
    Map Status = iota
    Reduce
	Wait
	Done
)

// Add your RPC definitions here.

type EmptyRequest struct {}

type EmptyReply struct {}

type Request struct {
	Status Status
	WorkerId int
}

type Reply struct {
	Status Status
	WorkerId int
	NReduce int
	NMap int
	Filename string
	Timestamp time.Time
}

type MapReduceTask struct {
	Timestamp time.Time
	Status Status
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
