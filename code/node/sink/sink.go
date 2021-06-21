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
type CivilizationUsageRecord struct {
	CivilizationName string
	UsageCount       int
}

type Sink struct {
	longMatchDataConsumer *middleware.LongMatchDataConsumer
	longMatchLock         *sync.RWMutex
	longMatches           []string

	largeRatingDifferenceConsumer  *middleware.LargeRatingDifferenceMatchDataConsumer
	largeRatingDifferenceMatchLock *sync.RWMutex
	largeRatingDifferenceMatches   []string

	usageCountStatisticsConsumer *middleware.CivilizationUsageAggregationConsumer
	usageCountStatistics         *middleware.CivilizationCounterRecord
	usageCountStatisticsLock     *sync.RWMutex

	victoryRateStatisticsConsumer *middleware.CivilizationVictoryRateConsumer
	victoryRateStatistics         *middleware.CivilizationFloatRecord
	victoryRateStatisticsLock     *sync.RWMutex
}

func Run() {
	var err error

	// Instantiate the sink object itself.
	sink := &Sink{}

	sink.longMatches = make([]string, 0)
	sink.longMatchLock = &sync.RWMutex{}

	sink.largeRatingDifferenceMatches = make([]string, 0)
	sink.largeRatingDifferenceMatchLock = &sync.RWMutex{}

	sink.usageCountStatistics = nil
	sink.usageCountStatisticsLock = &sync.RWMutex{}

	sink.victoryRateStatistics = nil
	sink.victoryRateStatisticsLock = &sync.RWMutex{}

	// Create a wait group for all consumers.
	waitGroup := &sync.WaitGroup{}

	// Initialize a long match data consumer.
	if sink.longMatchDataConsumer, err = sink.initializeLongMatchDataConsumer(waitGroup); err != nil {
		log.Println("could not create long match data consumer")
		return
	}
	defer sink.longMatchDataConsumer.Close()

	// Initialize consumer to consume large rating difference matches.
	if sink.largeRatingDifferenceConsumer, err = sink.initializeLargeRatingDifferenceConsumer(waitGroup); err != nil {
		log.Println("could not create large rating difference match data consumer")
		return
	}
	defer sink.largeRatingDifferenceConsumer.Close()

	// Initialize consumer to consume civilization victory rate updates.
	if sink.victoryRateStatisticsConsumer, err = sink.initializeVictoryRateStatisticsConsumer(waitGroup); err != nil {
		log.Println("could not create victory rate statistics consumer")
		return
	}
	defer sink.victoryRateStatisticsConsumer.Close()

	// Initialize consumer to consume civilization usage count updates.
	if sink.usageCountStatisticsConsumer, err = sink.initializeUsageCountStatisticsConsumer(waitGroup); err != nil {
		log.Println("could not create usage count statistics consumer")
		return
	}
	defer sink.usageCountStatisticsConsumer.Close()

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
	sink.longMatchDataConsumer.Stop()
	sink.largeRatingDifferenceConsumer.Stop()
	sink.victoryRateStatisticsConsumer.Stop()
	sink.usageCountStatisticsConsumer.Stop()

	// Wait for all consumers to finish.
	waitGroup.Wait()
}

//=================================================================================================
// Long match data consumer
//-------------------------------------------------------------------------------------------------
func (sink *Sink) initializeLongMatchDataConsumer(waitGroup *sync.WaitGroup) (
	*middleware.LongMatchDataConsumer, error) {

	// Instantiate the consumer.
	lmconsumer, err := middleware.CreateLongMatchDataConsumer()

	if err != nil {
		log.Println("could not create long match data consumer")
		return nil, err
	} else {
		// Register the consumer on the wait group.
		lmconsumer.RegisterOnWaitGroup(waitGroup)
		// Launch consumption of long match data in a separate goroutine.
		log.Println("launching long match data consumer")
		go lmconsumer.Consume(sink.handleLongMatch)
	}

	return lmconsumer, nil
}

func (sink *Sink) handleLongMatch(record *middleware.SingleTokenRecord) {
	// Get the match token and save it.
	sink.longMatchLock.Lock()
	sink.longMatches = append(sink.longMatches, record.Token)
	sink.longMatchLock.Unlock()
}

//=================================================================================================
// Large rating difference data consumer
//-------------------------------------------------------------------------------------------------
func (sink *Sink) initializeLargeRatingDifferenceConsumer(waitGroup *sync.WaitGroup) (
	*middleware.LargeRatingDifferenceMatchDataConsumer, error) {

	lrdconsumer, err := middleware.CreateLargeRatingDifferenceMatchDataConsumer()

	if err != nil {
		log.Println("could not create large rating difference match data consumer")
		return nil, err
	} else {
		// Register on the wait group.
		lrdconsumer.RegisterOnWaitGroup(waitGroup)
		// Lauch consumption in a separate goroutine.
		log.Println("launching large rating difference match data consumer")
		go lrdconsumer.Consume(sink.handleLargeRatingDifferenceMatch)
	}

	return lrdconsumer, nil
}

func (sink *Sink) handleLargeRatingDifferenceMatch(record *middleware.SingleTokenRecord) {
	sink.largeRatingDifferenceMatchLock.Lock()
	sink.largeRatingDifferenceMatches = append(sink.largeRatingDifferenceMatches, record.Token)
	sink.largeRatingDifferenceMatchLock.Unlock()
}

//=================================================================================================
// Victory rate statistics consumer
//-------------------------------------------------------------------------------------------------
func (sink *Sink) initializeVictoryRateStatisticsConsumer(waitGroup *sync.WaitGroup) (
	*middleware.CivilizationVictoryRateConsumer, error) {

	consumer, err := middleware.CreateCivilizationVictoryRateConsumer()

	if err != nil {
		log.Println("could not create civilization victory rate consumer")
		return nil, err
	} else {
		consumer.RegisterOnWaitGroup(waitGroup)
		log.Println("launching civilization victory rate consumer")
		go consumer.Consume(sink.handleCivilizationVictoryRateUpdates)
	}

	return consumer, nil
}

func (sink *Sink) handleCivilizationVictoryRateUpdates(record *middleware.CivilizationFloatRecord) {
	// Update known statistics with the newly received record.
	sink.victoryRateStatisticsLock.Lock()
	defer sink.victoryRateStatisticsLock.Unlock()
	sink.victoryRateStatistics = record
}

//=================================================================================================
// Usage count statistics consumer
//-------------------------------------------------------------------------------------------------
func (sink *Sink) initializeUsageCountStatisticsConsumer(waitGroup *sync.WaitGroup) (
	*middleware.CivilizationUsageAggregationConsumer, error) {

	consumer, err := middleware.CreateCivilizationUsageAggregationConsumer()

	if err != nil {
		log.Println("could not create civilization usage aggregation consumer")
		return nil, err
	} else {
		consumer.RegisterOnWaitGroup(waitGroup)
		log.Println("launching civilization usage aggregation consumer")
		go consumer.Consume(sink.handleCivilizationUsageUpdates)
	}

	return consumer, nil
}

func (sink *Sink) handleCivilizationUsageUpdates(record *middleware.CivilizationCounterRecord) {
	// Update known statistics with the newly received record.
	sink.usageCountStatisticsLock.Lock()
	defer sink.usageCountStatisticsLock.Unlock()
	sink.usageCountStatistics = record
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
	log.Println("[ ] Displaying periodic statistics report")
	log.Println("[ ]")
	// Check long match data.
	sink.displayLongMatches()
	// Check large rating difference matches.
	sink.displayLargeRatingDifferenceMatches()
	// Display victory rates.
	sink.displayVictoryRates()
	// Display usage count.
	sink.displayUsageCounters()
}

func (sink *Sink) displayLongMatches() {
	sink.longMatchLock.RLock()
	defer sink.longMatchLock.RUnlock()
	log.Printf("[o] %d long matches found so far\n", len(sink.longMatches))
	// Display the first 16 tokens for long matches.
	longMatchDisplayCount := len(sink.longMatches)
	truncated := false
	if longMatchDisplayCount > 16 {
		// Display only up to 16 matches.
		longMatchDisplayCount = 16
		truncated = true
	}
	for i := 0; i < longMatchDisplayCount; i++ {
		log.Printf("[-] Long match #%d: %s\n", i+1, sink.longMatches[i])
	}
	if truncated {
		log.Printf("[ ] %d not displayed", len(sink.longMatches)-longMatchDisplayCount)
	}
	log.Println("[ ]")
}

func (sink *Sink) displayLargeRatingDifferenceMatches() {
	sink.largeRatingDifferenceMatchLock.RLock()
	defer sink.largeRatingDifferenceMatchLock.RUnlock()
	log.Printf("[o] %d large rating difference matches found so far\n", len(sink.largeRatingDifferenceMatches))
	largeRatingDifferenceMatchDisplayCount := len(sink.largeRatingDifferenceMatches)
	truncated := false
	if largeRatingDifferenceMatchDisplayCount > 16 {
		// Display only up to 16 matches.
		largeRatingDifferenceMatchDisplayCount = 16
		truncated = true
	}
	for i := 0; i < largeRatingDifferenceMatchDisplayCount; i++ {
		log.Printf("[-] large rating difference match #%d: %s\n", i+1, sink.largeRatingDifferenceMatches[i])
	}
	if truncated {
		log.Printf("[ ] %d not displayed", len(sink.largeRatingDifferenceMatches)-largeRatingDifferenceMatchDisplayCount)
	}
	log.Println("[ ]")
}

func (sink *Sink) displayVictoryRates() {
	sink.victoryRateStatisticsLock.RLock()
	defer sink.victoryRateStatisticsLock.RUnlock()

	if sink.victoryRateStatistics == nil {
		return
	}

	// Display statistics.
	log.Println("[o] Victory rate by civilization in non-mirror 1v1 matches, in arena:")

	stats := sink.victoryRateStatistics
	names := stats.CivilizationName
	rates := stats.CivilizationFloat

	for i, name := range names {
		rate := rates[i]
		log.Printf("[-] %s : %.2f", name, rate)
	}

	log.Println("[ ]")
}

func (sink *Sink) displayUsageCounters() {
	sink.usageCountStatisticsLock.RLock()
	defer sink.usageCountStatisticsLock.RUnlock()

	if sink.usageCountStatistics == nil {
		return
	}

	// Display usage counters for top 5 most used civilizations.
	log.Println("[o] top 5 most used civilizations by pro players in team matches, in islands:")

	stats := sink.usageCountStatistics
	names := stats.CivilizationName
	count := stats.CivilizationCounter

	for i, name := range names {
		currentCount := count[i]
		log.Printf("[-] #%d : %s, used %d times\n", i+1, name, currentCount)
	}

	log.Println("[ ]")
}
