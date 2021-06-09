package mapreduce

import (
	"fmt"
	"sync"
)

type WorkerStatus struct {
	ready bool
	addr  string
}

type counter struct {
	mu    sync.RWMutex
	count int
}

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
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
		handleMapPhase(n_other, registerChan, mapFiles, jobName)
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
		handleReducePhase(ntasks, n_other, registerChan, jobName)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}

func handleMapPhase(n_other int, registerChan chan string, mapFiles []string, jobName string) {
	workerChan := make(chan WorkerStatus, len(mapFiles))
	workChan := make(chan DoTaskArgs, len(mapFiles))
	done := make(chan struct{})
	// Feed the Worker chan
	go func() {
		for {
			select {
			case workerAddr := <-registerChan:
				worker_attrs := WorkerStatus{
					ready: true,
					addr:  workerAddr,
				}
				workerChan <- worker_attrs
			case <-done:
				break
			}
		}
	}()

	var wg sync.WaitGroup
	for index, fileName := range mapFiles {
		wg.Add(1)
		doTaskArgs := DoTaskArgs{
			JobName:       jobName,
			File:          fileName,
			Phase:         mapPhase,
			TaskNumber:    index,
			NumOtherPhase: n_other,
		}
		submitWork(doTaskArgs, workChan)
		go doWork(&wg, workChan, workerChan)
	}

	wg.Wait()

	done <- struct{}{}
	close(workChan)
}

func handleReducePhase(ntasks int, n_other int, registerChan chan string, jobName string) {
	workerChan := make(chan WorkerStatus, ntasks)
	workChan := make(chan DoTaskArgs, ntasks)
	done := make(chan struct{})
	// Feed the Worker chan
	go func() {
		for {
			select {
			case workerAddr := <-registerChan:
				worker_attrs := WorkerStatus{
					ready: true,
					addr:  workerAddr,
				}
				workerChan <- worker_attrs
			case <-done:
				break
			}
		}
	}()

	var wg sync.WaitGroup
	for index := 0; index < ntasks; index++ {
		wg.Add(1)
		doTaskArgs := DoTaskArgs{
			JobName:       jobName,
			File:          "",
			Phase:         reducePhase,
			TaskNumber:    index,
			NumOtherPhase: n_other,
		}
		submitWork(doTaskArgs, workChan)
		go doWork(&wg, workChan, workerChan)
	}

	wg.Wait()
	done <- struct{}{}
	close(workChan)
}

func submitWork(task_args DoTaskArgs, workChan chan DoTaskArgs) {
	workChan <- task_args
}

func getWorker(workerChan chan WorkerStatus) WorkerStatus {
	workerAttr := <-workerChan
	return workerAttr
}

func doWork(wg *sync.WaitGroup, workChan chan DoTaskArgs, workerChan chan WorkerStatus) {
	for work := range workChan {
		worker := getWorker(workerChan)
		res := call(worker.addr, "Worker.DoTask", work, nil)
		if res {
			wg.Done()
		} else {
			submitWork(work, workChan)
		}
		workerChan <- worker
	}
}
