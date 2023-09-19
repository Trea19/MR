package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import "fmt"
import "6.5840/mr"
import "plugin"
import "os"
import "log"
import "io/ioutil"
import "sort"

// for sorting by key.
type ByKey []mr.KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func main(){
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles ...\n")
		os.Exit(1)
	}
	
	mapf, reducef := loadPlugin(os.Args[1])
	
	//
	// read each input file,
	// pass it to Map,
	// accumulate te intermediate Map output.
	//
	intermediate := []mr.KeyValue{}
	for _, filename := range os.Args[2:]{
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}
	//
	// a big difference from real MR is that all the intermediate data is in one place,
	// intermediate[], rather than being partitioned into NxM buckets.
	//
	
	sort.Sort(ByKey(intermediate))
	
	oname := "mr-out-0"
	ofile, _ := os.Create(oname)
	
	// 
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	
}
