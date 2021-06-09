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
	workerChan := make(chan WorkerStatus)
	done := make(chan struct{})
	counter := new(counter)
	counter.count = len(mapFiles)
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
		work := func(fileName string, ind int, wg *sync.WaitGroup) {
			defer wg.Done()

			getWorker := func() WorkerStatus {
				workerAttr := <-workerChan
				counter.mu.Lock()
				counter.count--
				counter.mu.Unlock()
				return workerAttr
			}

			workerAttr := getWorker()
			doTaskArgs := DoTaskArgs{
				JobName:       jobName,
				File:          fileName,
				Phase:         mapPhase,
				TaskNumber:    ind,
				NumOtherPhase: n_other,
			}
			call(workerAttr.addr, "Worker.DoTask", doTaskArgs, nil)
			counter.mu.RLock()
			if counter.count > 0 {
				workerChan <- workerAttr
			}
			counter.mu.RUnlock()
		}
		go work(fileName, index, &wg)
	}

	wg.Wait()

	done <- struct{}{}
}

func handleReducePhase(ntasks int, n_other int, registerChan chan string, jobName string) {
	workerChan := make(chan WorkerStatus, ntasks)
	done := make(chan struct{})
	counter := new(counter)
	counter.count = ntasks
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
		go func(ind int, wg *sync.WaitGroup) {
			defer wg.Done()
			getWorker := func() WorkerStatus {
				workerAttr := <-workerChan
				counter.mu.Lock()
				counter.count--
				counter.mu.Unlock()
				return workerAttr
			}

			workerAttr := getWorker()
			doTaskArgs := DoTaskArgs{
				JobName:       jobName,
				File:          "",
				Phase:         reducePhase,
				TaskNumber:    ind,
				NumOtherPhase: n_other,
			}
			call(workerAttr.addr, "Worker.DoTask", doTaskArgs, nil)
			counter.mu.RLock()
			if counter.count > 0 {
				workerChan <- workerAttr
			}
			counter.mu.RUnlock()
		}(index, &wg)
	}

	wg.Wait()
	done <- struct{}{}
}
