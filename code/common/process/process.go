package process

import "sync"

type Process struct {
	waitGroup *sync.WaitGroup
	loop      func(*Process)
	quit      chan int
	stopping  bool
}

func CreateProcess(loop func(*Process)) *Process {
	process := &Process{}
	process.waitGroup = nil
	process.loop = loop
	process.quit = make(chan int, 1)
	return process
}

func (process *Process) RegisterOnWaitGroup(waitGroup *sync.WaitGroup) {
	process.waitGroup = waitGroup
	process.waitGroup.Add(1)
}

func (process *Process) Run() {
	// Execute the main loop until the stopping flag is set.
	for !process.stopping {
		process.loop(process)
	}

	// Notify about the process finalization.
	process.waitGroup.Done()
}

func (process *Process) Stop() {
	// Send quit signal to the process.
	process.quit <- 0
}

// The following method should be called from the main loop of
// the process.
func (process *Process) SetStopping() {
	process.stopping = true
}
