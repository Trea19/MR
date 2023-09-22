package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"

type Coordinator struct {
	// protect coordinator state from concurrent access
	mu sync.Mutex

	// nMap == len(mapfiles)
	mapFiles  []string
	nMapTasks  int
	nReduceTasks  int

	// keep trace of when tasks are assigned,
	// and which tasks have finished.
	mapTasksIssued  []time.Time
	mapTasksFinished  []bool
	reduceTasksIssued  []time.Time
	reduceTasksFinished  []bool

	// set to True when all reduce tasks are comleted
	isDone  bool

}

// 
// an example RPC handler
// 
// the RPC argument and reply types are defined in rpc.go
//
/*
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
*/

//
// Handle GetTask RPCs from workers.
//
func (c *Coordinator) HandleGetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.NReduceTasks = c.nReduceTasks
	reply.NMapTasks = c.nMapTasks

	// issue map tasks util there is no map tasks left
	for {
		mapDone := true
		for m, done := range c.mapTasksFinished {
			if !done {
				// assign a task if it's either never been issued, or if it's been 
				// too long since it was issued so the worker may crashed.
				// NOTE: if task has never been issued, time is intialized to 0
				if c.mapTasksIssued[m].IsZero() || time.Since(c.mapTasksIssued[m]).Seconds() > 10 {
					reply.TaskType = Map
					reply.TaskNum = m
					reply.MapFile = c.mapFiles[m]
					c.mapTasksIssued[m] = time.Now()
					return nil
				} else {
					mapDone = false
				}
			}
		}

		// if all map tasks are in progress and haven't timed out, wait to give another task
		if !mapDone {
			// todo:wait
		} else {
			// we are done with all map tasks.
			break
		}
	}

	// all map tasks are done, issue reduce tasks now
	for {
		reduceDone := true
		for m, done := range c.reduceTasksFinished {
			if !done {
				// assign a task if it's either never been issued, or if it's been
				// too long since it was issued so the worker may crashed.
				// NOTE: if task has never been issued, time is intialized to 0
				if c.reduceTasksIssued[m].IsZero() || time.Since(c.reduceTasksIssued[m]).Seconds() > 10 {
					reply.TaskType = Reduce
					reply.TaskNum = m
					reply.reduceTasksIssued[m] = time.Now()
					return nil
				} else {
					reduceDone = false
				}
			}
		}

		// if all reduce tasks are in progress and haven't timed out, wait to give another task
		if !reduceDone {
			// todo: wait
		} else {
			// we are done with are reduce tasks.
			break
		}
	}

	// if all map and reduce tasks are done, 
	// send the querying worker a Done TaskType,
	// and set isDone to True
	reply.TaskType = Done
	c.isDone = true

	return nil

}

//
// Handle FinishedTask RPCs from workers.
//
func (c *Coordinator) HandleFinishedTask(args *FinishedTaskArgs, reply *FinishedTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	swich args.TaskType {
	case Map:
		c.mapTasksFinished[args.TaskNum] = true
	case Reduce:
		c.reduceTasksFinished[args.TaskNum] = true
	default:
		log.Fatalf("Bad finished task? %s", args.TaskType)
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool{
	ret := false
	
	// todo
	
	return ret
}

//
// create a coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	
	// todo
	
	c.server()
	return &c
}
