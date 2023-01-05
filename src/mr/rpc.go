package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// type ExampleArgs struct {
// 	X int
// }

// type ExampleReply struct {
// 	Y int
// }

// Add your RPC definitions here.
type Args struct {
	WorkerId string `json:"worker_id"`
	LastTaskId int `json:"last_task_id"`
	OutputFilePaths []string `json:"output_file_path"`
	Success bool `json:"success"`
}

type Reply struct {
	InputFilePaths []string `json:"input_file_path"`
	TaskId int `json:"task_id"`
	IsReduce bool `json:"is_reduce"`
	NumReduce int `json:"num_reduce"`
	AllWorkDone bool `json:"all_work_done"`
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
