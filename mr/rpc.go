package mr

// 
// RPC definitions
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//
/*
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}
*/

//
// define task types.
//
type TaskType int

const (
	Map  TaskType = 1
	Reduce  TaskType = 2
	Done  TaskType = 3
)
//
// GetTask RPCs are sent from idle workers to coordinator to ask for the next task to perform.
//
// no arguments to send to coordiator to ask for a task.
type GetTaskArgs struct{}
// NOTE: RPC fields need to be captitalized in order to be sent!
type GetTaskReply struct {
	// what type of task is this?
	TaskType TaskType

	// task number of either map or reduce tasks
	TaskNum int

	// needed for Map (to know which file to write)
	NReduceTasks int

	// needed for Map (to know which file to read)
	MapFile string

	// needed for Reduce (to know how many intermediate map files to read)
	NMapTasks int
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
