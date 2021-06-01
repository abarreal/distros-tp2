package process

import "sync"

type Process struct {
	waitGroup *sync.WaitGroup
	loop      func()
	quit      chan int
}

func CreateProcess(loop func()) {
	process := &Process{}
	process.waitGroup = nil
	process.loop = loop
	process.quit = make(chan int, 1)
}

func (process *Process) RegisterOnWaitGroup(waitGroup *sync.WaitGroup) {
	process.waitGroup = waitGroup
	process.waitGroup.Add(1)
}

func (process *Process) Run() {
	// Execute the main loop until receiving the quit signal.
	stopping := false

	for !stopping {
		process.loop()
	}

	// Notify about the process finalization.
	process.waitGroup.Done()
}

func (process *Process) Stop() {
	// Send quit signal to the process.
	process.quit <- 0
}
