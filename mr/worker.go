package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce task number
// for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// for sorting by key.
///
type ByKey []KeyValue

func (a ByKey) len() int  {return len(a)}
func (a ByKey) Swap(i,j int) {a[i],a[j] = a[j],a[i]}
func (a ByKey) Less(i,j int) bool {return a[i].Key < a[j].Key}


//
// finalizeReduceFile atomically renames temporary reduce file to a completed reduce task file.
//
func finalizeReduceFile(tmpFile string, taskN int){
	finalFileName := fmt.Sprintf("mr-out-%d", taskN)
	os.Rename(tmpFile, fileFileName)
}

//
// finalizeIntermediateFile atomically renames temporary intermediate file to a completed intermediate file.
//
func finalizeIntermediateFile(tmpFile string, mapTaskN int, reduceTaskN int){
	finalFileName := fmt.Sprintf("mr-%d-%d", mapTaskN, reduceTaskN)
	os.Rename(tmpFile, finalFileName)
}

//
// implementation of map task.
//
func performMap(filename string, taskNum int, nReduceTasks int, mapf func(string, string) []KeyValue){
	// read contents
	file, err := os.Open(filename)
	if err != nil{
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// apply mapf to contents of file, 
	// and collect kv pairs
	kva := mapf(filename, string(content))

	// create temporary files and encoders for each file
	tmpFiles := []*os.File{}
	tmpFileNames := []string{}
	encoders := []*json.Encoder{}
	for i:= 0; i < nReduceTasks; i ++ {
		tmpFile, err := ioutil.TempFile("","")
		if err != nil{
			log.Fatal("cannot open temp file")
		}
		tmpFiles = append(tmpFiles, tmpFile)
		tmpFileName := tmpFile.Name()
		tmpFileNames = append(tmpFileNames, tmpFilenName)
		enc := json.NewEncoder(tmpFile)
		encoders = append(encoders, enc)
	}

	// write output keys to appropriate (temporary!) intermeidate files,
	// using ihash function above
	for _, kv := range kva {
		r := ihash(kv.Key) % nReduceTasks
		encoders[r].Encode(&kv)
	}

	// close tmp files
	for _, f := range tmpFiles {
		f.Close()
	}

	// atomically rename temp files to final intermediate files
	for i := 0; i < nReduceTasks; i ++ {
		finalizeIntermediateFile(tmpFileNames[i], taskNum, i)
	}
}

//
// implementation of reduce task.
//
func performReduce(taskNum int, nMapTasks int, reducef func(string, []string) string){
	// get all intermediate files corresponding to this reduce task,
	// and collecting the corresponding key-value pairs.
	kva := []KeyValue{}
	for m := 0; m < nMapTasks; m ++ {
		iFileName := fmt.Sprintf("mr-%d-%d", m, taskNum) // get intermediate file name
		file, err := os.Open(iFileName)
		if err != nil {
			log.Fatalf("cannot open %v", iFileName)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break;
			}
			kva = append(kva, kv)
		}

		file.Close()
	}

	// sort the keys
	sort.Sort(ByKey(kva))

	// get temporary reduce file to write values
	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		log.Fatal("cannot open temp file")
	}
	tmpFileName := tmpFile.Name()

	// apply reducef to all values with the same key
	key_begin := 0
	for key_begin < len(kva){
		key_end := key_begin + 1
		// find the positon of the last same key
		for key_end < len(kva) && kva[key_begin].Key == kva[key_end].Key{
			key_end ++
		}
		// values for reducef
		values := []string{}
		for k := key_begin; k < key_end; k ++ {
			values = append(values, kva[k].Value)
		}
		// reducef
		output := reducef(kva[key_begin].Key, values)
		// write output to reduce task tmp file
		fmt.Fprintf(tmpFile, "%v %v\n", kva[key_begin].Key, output)
		// update key_begin
		key_begin = key_end
	}

	// atomically rename temp file to final reduce task file.
	finalizeReduceFile(tmpFileName, taskNum)

}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string){
	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		
		// this will wait until we get assigned a task
		call("Coordinator.HandleGetTask", &args, &reply)

		switch reply.TaskType {
		case Map:
			performMap(reply.MapFile, reply.TaskNum, reply.NReduceTasks, mapf)
		case Reduce:
			performReduce(reply.TaskNum, reply.NMapTasks, reducef)
		case Done:
			os.Exit(0)
		default:
			fmt.Errorf("Bad task type? %s", reply.TaskType)
		}

		// tell coordinator that we're done
		finargs := FinishedTaskArgs {
			TaskType: reply.TaskType,
			TaskNum: reply.TaskNum,
		}
		finreply := FinishedTaskReply{}
		call("Coordinator.HandleFinishedTask", &finargs, &finreply)

	}
	// uncomment to send the Example RPC to the coordinator
	// CallExample()
}

//
// example function to show how to make a RPC call to the coordinator
//
// the RPC argument and reply types are define in rpc.go
//
/*
func CallExample(){
	// declare an argument structure
	args := ExampleArgs{}
	
	// fill in the argument(s).
	args.X = 99
	
	// declare a reply structure
	reply := ExampleReply{}
	
	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}
*/

//
// send a RPC request to the coordinator, wait for the response
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing: ", err)
	}
	defer c.Close()
	
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	
	fmt.Println(err)
	return false
}
