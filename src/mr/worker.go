package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string `json:"k"`
	Value string `json:"v"`
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

var itmdTpl = "mr-im-%v-%v-%v"   // mr-im-${worker_id}-${map_id}-${reduce_id}
var outTpl = "mr-out-%v"	  // mr-im-${reduce_id}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// we need an identifier for each worker
	// since the workers are separate processes
	// use the pid to distinguish them,
	// we can also use IP address when in a cluster
	workerId := strconv.Itoa(os.Getpid())
	curTaskId := 0
	var outputs []string
	success := true
	var err error

	for {
		args := Args{
			WorkerId: workerId,
			LastTaskId: curTaskId,
			OutputFilePaths: outputs,
			Success: success,
		}

		reply := callMaster(&args)
		curTaskId = reply.TaskId
		log.Printf("Worker : %v, Cur task %v\n", workerId, curTaskId)

		if reply.AllWorkDone {
			break
		}
		if !reply.IsReduce {
			outputs, err = doMap(mapf, reply.InputFilePaths[0], workerId, curTaskId, reply.NumReduce)
		} else {
			outputs, err = doReduce(reducef, curTaskId, reply.InputFilePaths)
		}

		if err != nil {
			// this time failed
			log.Printf("Failed task %v, err: %v\n", curTaskId, err)
			success = false
		}
		// log.Printf("Finished task %v\n", curTaskId)
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()
}

func doMap(mapf func(string, string) []KeyValue, input string, wid string, tid int, nr int) ([]string, error) {
	content := readFile(input)
	kva := mapf(input, string(content))
	kvm := make(map[int][]KeyValue)
	for _, kv := range kva {
		ikey := ihash(kv.Key) % nr
		l, _ := kvm[ikey]
		l = append(l, kv)
		kvm[ikey] = l
	}

	var outputs []string
	for i := 0; i < nr; i++ {
		outputs = append(outputs, fmt.Sprintf(itmdTpl, wid, tid, i))
		file, err := os.OpenFile(outputs[len(outputs) - 1], os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0600)
		// should use overwrite, because if the files exits, probly the previous task failed

		if err != nil {
			return nil, err
		}

		// write json to target file
		enc := json.NewEncoder(file)
		for _, kv := range kvm[i] {
			err = enc.Encode(&kv)
			if err != nil {
				return nil, err
			}
		}
	}
	return outputs, nil
}

func doReduce(reducef func(string, []string) string, tid int, files []string) ([]string, error) {
	// read all intermediate files with the suffix of the target key
	ikey := strconv.Itoa(-(tid + 1))
	var kva []KeyValue

	for _, fname := range files {
		if strings.HasSuffix(fname, ikey) {
			kva = append(kva, readFileAsKvs(fname)...)
		}
	}

	// log.Printf("kva for ihash key %v is %v\n", ikey, kva)
	sort.Sort(ByKey(kva))

	var lines []string

	// stealed from mrsequential.go
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		reduced := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		lines = append(lines, fmt.Sprintf("%v %v", kva[i].Key, reduced))

		i = j
		// if i % 10 == 0 {
		// 	log.Printf("Worker: %v, Reducing %v / %v\n", os.Getpid(), i, len(kva))
		// }
	}

	oname := fmt.Sprintf(outTpl, ikey)
	writeFile(oname, lines)
	return []string{oname}, nil
}

func readFile(fname string) []byte {
	file, err := os.Open(fname)
	if err != nil {
		log.Fatalf("cannot open %v", fname)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fname)
	}
	file.Close()
	return content
}

func readFileAsKvs(fname string) []KeyValue {
	file, err := os.Open(fname)
	if err != nil {
		log.Fatalf("cannot open %v", fname)
	}
	defer file.Close()

	var kvs []KeyValue
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kvs = append(kvs, kv)
	}

	return kvs
}

func writeFile(fname string, lines []string) {
	ofile, err := os.OpenFile(fname, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		log.Fatalf("cannot open %v", fname)
	}
	defer ofile.Close()

	for _, l := range lines {
		fmt.Fprintf(ofile, "%v\n", l)
	}
}


//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	call("Master.Example", &args, &reply)

// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

func callMaster(args* Args) *Reply {
	reply := Reply{}
	retry := 0
	for {
		ok := call("Master.Assign", args, &reply)
		if !ok {
			log.Printf("Retrying to call RPC Master.Assign... retry %v / 3", retry)
			if (retry < 3){
				retry++
			} else {
				break
			}
		}
		break
	}
	return &reply
}


//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", "mr-socket")
	if err != nil {
		log.Fatal("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
