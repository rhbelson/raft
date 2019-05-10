package mapreduce

import "fmt"
import "sync"

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
    fmt.Println("!!! Schedule is being called !!!")
    var isMap bool
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
        isMap = true
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
        isMap = false
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

    var avaliable_worker = make(chan string)
	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	// Your code here (Part 2, 2B).
	//If it's a map task (how can we be sure?)
    go pull_workers(registerChan, avaliable_worker)
    var wg sync.WaitGroup

    taskNumber := 0
    for taskNumber < ntasks {
        worker := <- avaliable_worker
        var task_args interface{}
        if(isMap) {
            task_args = DoTaskArgs{jobName, mapFiles[taskNumber], mapPhase, taskNumber, n_other}
        } else {
            task_args = DoTaskArgs{jobName, "", reducePhase, taskNumber, n_other}
        }

        wg.Add(1)
        fmt.Println(task_args)
        go start_worker(worker, "Worker.DoTask", task_args, nil,
                        avaliable_worker, &wg)
        taskNumber = taskNumber + 1
    }
    fmt.Printf("Start waiting taskNum: %d\n", taskNumber)
    wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}

func pull_workers(c chan string, avaliable_worker chan string) {
  for {
    worker := <- c
    fmt.Printf("Putting worker %v in avaliable\n", worker)
    avaliable_worker <- worker
  }
}

func start_worker(worker string, rpcname string, task_args interface{},
                  reply interface{}, avaliable_worker chan string, wg *sync.WaitGroup) {
    fmt.Printf("Worker %v is assigned\n", worker)
    ret := call(worker, "Worker.DoTask", task_args, reply)
    if(ret) {
        wg.Done()
        fmt.Printf("Putting worker %v in avaliable  \n", worker)
        avaliable_worker <- worker
    } else {
        fmt.Println("Task failed")
        new_worker := <- avaliable_worker
        fmt.Println("Found new worker!!!")
        fmt.Println(new_worker)
        wg.Add(1)
        go start_worker(new_worker, rpcname, task_args, nil,
                        avaliable_worker, wg)
        wg.Done()
    }
}
