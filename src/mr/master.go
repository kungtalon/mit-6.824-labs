package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type Master struct {
	// Your definitions here.
	nMap int
	nReduce int
	idleM chan int		// stores the task_id of idle map tasks
	idleR chan int      // stores the task_id of idle reduce tasks
	mu sync.Mutex
	timedOut chan int
	sent map[int]string			// 
	finished map[int]struct{}   // 
	doneM int
	doneR int
	filesM []string		// each file corresponds to a map task
	filesR []string
}


// Your code here -- RPC handlers for the worker to call.
func (m *Master) Assign(args *Args, reply *Reply) error {
	m.updateStatus(args)

	if m.doneM == m.nMap && m.doneR == m.nReduce {
		*reply = Reply{
			TaskId: 0,
			IsReduce: false,
			NumReduce: 0,
			AllWorkDone: true,
		}
		return nil
	}

	if m.doneM < m.nMap {
		ok := m.assignMap(args, reply)
		if ok {
			return nil
		}
	}

	if m.doneR < m.nReduce {
		ok := m.assignReduce(args, reply)
		if ok {
			return nil
		} 
	}

	*reply = Reply{
		TaskId: 0,
		IsReduce: false,
		NumReduce: 0,
		AllWorkDone: true,
	}
	return nil
}

func (m* Master) updateStatus(args *Args) {
	lastTaskId := args.LastTaskId
	workerId := args.WorkerId

	m.mu.Lock()
	defer m.mu.Unlock()
	
	// fmt.Printf("Getting request: %+v\n", args)

	sentWorker, found := m.sent[lastTaskId]
	if sentWorker != workerId || !found {
		// the task has expired, ignore it
	} else if !args.Success && args.LastTaskId >= 0 {
		// if the last task failed, we should resend it
		if lastTaskId >= 0 {
			m.idleM <- lastTaskId
		} else {
			m.idleR <- lastTaskId
		}
		delete(m.sent, lastTaskId)
	} else {
		// the task has succeeded, update it
		m.finished[lastTaskId] = struct{}{}
		delete(m.sent, lastTaskId)
		if lastTaskId >= 0 {
			m.doneM++
			m.filesR = append(m.filesR, args.OutputFilePaths...)
		} else {
			m.doneR++
		}
	}
}

func (m *Master) assignMap(args *Args, reply *Reply) bool {
	for {
		select {
		case taskId := <- m.idleM:
			m.buildReply(reply, taskId, args.WorkerId, false)
			return true
		default:
			// when program goes here, the queue is empty
			// we need to wait for all map tasks to finish or restart
			if m.doneM == m.nMap {
				return false
			}
			// else, go back to infinite loop			
		}
	}
}

func (m *Master) assignReduce(args *Args, reply *Reply) bool {
	for {
		select {
		case taskId := <- m.idleR:
			m.buildReply(reply, taskId, args.WorkerId, true)			
			return true
		default:
			// when program goes here, the queue is empty
			// we need to wait for all reduce tasks to finish or restart
			if m.doneR == m.nReduce {
				return false
			}
			// else, go back to infinite loop			
		}
	}
}

func (m* Master) buildReply(reply* Reply, tid int, wid string, isR bool) {
	// build reply here
	m.mu.Lock()
	defer m.mu.Unlock()
	var taskFiles []string
	if isR {
		taskFiles = m.filesR
	} else {
		taskFiles = []string{m.filesM[tid]}
	}

	*reply = Reply{
		InputFilePaths: taskFiles,
		TaskId: tid,
		IsReduce: isR,
		NumReduce: m.nReduce,
		AllWorkDone: false,
	}
	m.sent[tid] = wid

	go func() {
		time.Sleep(time.Second * 10)
		m.timedOut <- tid
	}()

	// fmt.Printf("Sent task %v to worker %v\n", tid, wid)
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()
	ret = (m.doneM == m.nMap) && (m.doneR == m.nReduce)
	if ret {
		fmt.Println("All Done!")
		return ret
	}

	// check time out tasks
	for {
		select {
		case taskId := <- m.timedOut:
			if _, found := m.finished[taskId]; !found {
				// put it back to the idle task queue
				// remove from the sent map
				if (taskId >= 0) {
					m.idleM <- taskId
				} else {
					m.idleR <- taskId
				}
				delete(m.sent, taskId)				
			} else {
				// task already finished, let it go
			}
		default:
			// no timeout task, do nothing
			// fmt.Printf("Cur finished: %+v\n", m.finished)
			return ret
		} 
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.filesM = files
	m.nReduce = nReduce
	m.nMap = len(files)
	fmt.Printf("Files: %v\n", files)

	m.idleM = make(chan int, len(files))
	m.idleR = make(chan int, nReduce)
	m.timedOut = make(chan int)
	m.sent = make(map[int]string)
	m.finished = make(map[int]struct{})

	for i := 0; i < len(files); i++ {
		m.idleM <- i
	}

	for i := 1; i <= m.nReduce; i++ {
		m.idleR <- -i
	}

	m.server()
	return &m
}
