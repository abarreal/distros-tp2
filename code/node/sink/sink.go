package sink

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"tp2.aba.distros.fi.uba.ar/common/middleware"
)

//=================================================================================================
// Main
//-------------------------------------------------------------------------------------------------
type Sink struct {
	longMatchLock                  *sync.RWMutex
	longMatches                    []string
	largeRatingDifferenceMatchLock *sync.RWMutex
	largeRatingDifferenceMatches   []string
}

func Run() {
	// Instantiate the sink object itself.
	sink := &Sink{}
	sink.longMatches = make([]string, 0)
	sink.longMatchLock = &sync.RWMutex{}
	sink.largeRatingDifferenceMatchLock = &sync.RWMutex{}

	// Create a wait group for all consumers.
	waitGroup := &sync.WaitGroup{}

	// Initialize a long match data consumer.
	lmconsumer, err := middleware.CreateLongMatchDataConsumer()

	if err != nil {
		log.Println("could not create long match data consumer")
	} else {
		// Register the consumer on the wait group.
		lmconsumer.RegisterOnWaitGroup(waitGroup)
		// Launch consumption of long match data in a separate goroutine.
		log.Println("launching long match data consumer")
		go lmconsumer.Consume(sink.handleLongMatch)
	}

	// Initialize consumer to consume large rating difference matches.
	lrdconsumer, err := middleware.CreateLargeRatingDifferenceMatchDataConsumer()

	if err != nil {
		log.Println("could not create large rating difference match data consumer")
	} else {
		// Register on the wait group.
		lrdconsumer.RegisterOnWaitGroup(waitGroup)
		// Lauch consumption in a separate goroutine.
		log.Println("launching large rating difference match data consumer")
		go lrdconsumer.Consume(sink.handleLargeRatingDifferenceMatch)
	}

	// Initialize other consumers.
	// TODO

	// Initialize periodic statistics reports.
	statisticsQuitChannel := make(chan int, 1)
	waitGroup.Add(1)
	go sink.runPeriodicReport(waitGroup, statisticsQuitChannel)

	// Await an incoming quit signal.
	sigchannel := make(chan os.Signal, 1)
	signal.Notify(sigchannel, syscall.SIGINT, syscall.SIGTERM)
	<-sigchannel

	// Stop the statistics worker.
	statisticsQuitChannel <- 0
	// Stop all consumers.
	if lmconsumer != nil {
		lmconsumer.Stop()
	}

	// Wait for all consumers to finish.
	waitGroup.Wait()
}

func (sink *Sink) handleLongMatch(record *middleware.SingleTokenRecord) {
	// Get the match token and save it.
	sink.longMatchLock.Lock()
	sink.longMatches = append(sink.longMatches, record.Token)
	sink.longMatchLock.Unlock()
}

func (sink *Sink) handleLargeRatingDifferenceMatch(record *middleware.SingleTokenRecord) {
	sink.largeRatingDifferenceMatchLock.Lock()
	sink.largeRatingDifferenceMatches = append(sink.largeRatingDifferenceMatches, record.Token)
	sink.largeRatingDifferenceMatchLock.Unlock()
}

//=================================================================================================
// Periodic statistics reporter
//-------------------------------------------------------------------------------------------------
const StatisticsDisplayPeriod int = 15

func (sink *Sink) runPeriodicReport(waitGroup *sync.WaitGroup, quitChannel <-chan int) {
	stopping := false

	// Statistics will be displayed every StatisticsDisplayPeriod seconds.
	timer := time.After(time.Duration(StatisticsDisplayPeriod) * time.Second)

	for !stopping {
		select {
		case <-quitChannel:
			stopping = true
		case <-timer:
			sink.showStats()
			timer = time.After(time.Duration(StatisticsDisplayPeriod) * time.Second)
		}
	}

	// Send finalization signal.
	waitGroup.Done()
}

func (sink *Sink) showStats() {
	// Check long match data.
	sink.longMatchLock.RLock()
	log.Printf("%d long matches found so far\n", len(sink.longMatches))
	// Display the first 16 tokens for long matches.
	longMatchDisplayCount := len(sink.longMatches)
	if longMatchDisplayCount > 16 {
		longMatchDisplayCount = 16
	}
	for i := 0; i < longMatchDisplayCount; i++ {
		log.Printf("long match #%d: %s\n", i+1, sink.longMatches[i])
	}
	sink.longMatchLock.RUnlock()

	// Check large rating difference matches.
	sink.largeRatingDifferenceMatchLock.RLock()
	log.Printf("%d large rating difference matches found so far\n", len(sink.largeRatingDifferenceMatches))
	largeRatingDifferenceMatchDisplayCount := len(sink.largeRatingDifferenceMatches)
	if largeRatingDifferenceMatchDisplayCount > 16 {
		largeRatingDifferenceMatchDisplayCount = 16
	}
	for i := 0; i < largeRatingDifferenceMatchDisplayCount; i++ {
		log.Printf("large rating difference match #%d: %s\n", i+1, sink.largeRatingDifferenceMatches[i])
	}
	sink.largeRatingDifferenceMatchLock.RUnlock()

	// Check remaining statistics.
	// TODO
}
