package sink

import (
	"log"
	"os"
	"os/signal"
	"sort"
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
	longMatchLock                       *sync.RWMutex
	longMatches                         []string
	largeRatingDifferenceMatchLock      *sync.RWMutex
	largeRatingDifferenceMatches        []string
	civilizationUsageCountInIslandsLock *sync.RWMutex
	civilizationUsageCountInIslands     map[string]*CivilizationUsageRecord
	civilizationVictoryStatsInArenaLock *sync.RWMutex
	civilizationVictoryStatsInArena     map[string]([]int)
}

func Run() {
	// Instantiate the sink object itself.
	sink := &Sink{}
	sink.longMatches = make([]string, 0)
	sink.civilizationUsageCountInIslands = make(map[string]*CivilizationUsageRecord)
	sink.civilizationVictoryStatsInArena = make(map[string]([]int))
	sink.longMatchLock = &sync.RWMutex{}
	sink.largeRatingDifferenceMatchLock = &sync.RWMutex{}
	sink.civilizationUsageCountInIslandsLock = &sync.RWMutex{}
	sink.civilizationVictoryStatsInArenaLock = &sync.RWMutex{}

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

	// Initialize consumer to consume civilization usage.
	islandsCivConsumer, err := middleware.CreateIslandsCivilizationUsageDataConsumer()

	if err != nil {
		log.Println("could not create islands civilization usage data consumer")
	} else {
		islandsCivConsumer.RegisterOnWaitGroup(waitGroup)
		log.Println("launching civilization usage data consumer for map islands")
		go islandsCivConsumer.Consume(sink.handleIslandsCivilizationUsageData)
	}

	// Initialize consumer to consume civilization victory statistics.
	arenaCivVictoryConsumer, err := middleware.CreateArenaCivilizationVictoryDataConsumer()

	if err != nil {
		log.Println("could not create islands civilization usage data consumer")
	} else {
		arenaCivVictoryConsumer.RegisterOnWaitGroup(waitGroup)
		log.Println("launching civilization usage data consumer for map islands")
		go arenaCivVictoryConsumer.Consume(sink.handleArenaCivilizationVictoryData)
	}

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

func (sink *Sink) handleIslandsCivilizationUsageData(batch *middleware.CivilizationInfoRecordBatch) {
	// We have to store the amount of times each civilization was used in islands.
	for _, record := range batch.Records {
		sink.civilizationUsageCountInIslandsLock.Lock()
		current, found := sink.civilizationUsageCountInIslands[record.CivilizationName]
		// Initialize counter if not found.
		if !found {
			current = &CivilizationUsageRecord{record.CivilizationName, 0}
			sink.civilizationUsageCountInIslands[record.CivilizationName] = current
		}
		// Increase counter by one in any case.
		current.UsageCount++
		sink.civilizationUsageCountInIslandsLock.Unlock()
	}
}

func (sink *Sink) handleArenaCivilizationVictoryData(batch *middleware.CivilizationInfoRecordBatch) {
	// We have to store the amount of victories and loses for each civilization.
	for _, record := range batch.Records {
		sink.civilizationVictoryStatsInArenaLock.Lock()
		// Get current stats and initialize if none.
		current, found := sink.civilizationVictoryStatsInArena[record.CivilizationName]
		if !found {
			// Create an array of length 2 to hold victories in the first element and defeats in the second.
			current = []int{0, 0}
			sink.civilizationVictoryStatsInArena[record.CivilizationName] = current
		}
		if record.IndicatesVictory() {
			current[0]++
		} else if record.IndicatesDefeat() {
			current[1]++
		}
		sink.civilizationVictoryStatsInArenaLock.Unlock()
	}
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
	// Check victory rates by civilization in arena.
	sink.displayCivilizationVictoryRates()
	// Check civilization usage statistics in islands.
	sink.displayCivilizationUsageStatistics()
	// Check long match data.
	sink.displayLongMatches()
	// Check large rating difference matches.
	sink.displayLargeRatingDifferenceMatches()
}

func (sink *Sink) displayCivilizationVictoryRates() {
	sink.civilizationVictoryStatsInArenaLock.Lock()
	defer sink.civilizationVictoryStatsInArenaLock.Unlock()
	log.Println("victory rate by civilization in non-mirror 1v1 matches, in arena:")
	for cname, data := range sink.civilizationVictoryStatsInArena {
		victories := data[0]
		total := victories + data[1]
		log.Printf("- %s : %.2f", cname, float32(victories)/(float32(total)))
	}
}

func (sink *Sink) displayCivilizationUsageStatistics() {
	sink.civilizationUsageCountInIslandsLock.Lock()
	defer sink.civilizationUsageCountInIslandsLock.Unlock()
	log.Println("top 5 most used civilizations by pro players in team matches, in islands:")
	// Copy map data into an array for sorting.
	civUsageRecords := make([]*CivilizationUsageRecord, 0)
	for _, record := range sink.civilizationUsageCountInIslands {
		civUsageRecords = append(civUsageRecords, record)
	}

	if len(civUsageRecords) == 0 {
		log.Println("no data yet")
	} else if len(civUsageRecords) == 1 {
		only := civUsageRecords[0]
		log.Printf("#%d : %s, used %d times\n", 1, only.CivilizationName, only.UsageCount)
	} else {
		// Sort by usage count.
		sort.Slice(civUsageRecords, func(i, j int) bool {
			return civUsageRecords[i].UsageCount < civUsageRecords[j].UsageCount
		})
		shown := 5
		if len(civUsageRecords) < shown {
			shown = len(civUsageRecords)
		}
		// Show top of those that we have.
		for i := 0; i < shown; i++ {
			current := civUsageRecords[i]
			log.Printf("#%d : %s, used %d times\n", i+1, current.CivilizationName, current.UsageCount)
		}
	}
}

func (sink *Sink) displayLongMatches() {
	sink.longMatchLock.RLock()
	defer sink.longMatchLock.RUnlock()
	log.Printf("%d long matches found so far\n", len(sink.longMatches))
	// Display the first 16 tokens for long matches.
	longMatchDisplayCount := len(sink.longMatches)
	if longMatchDisplayCount > 16 {
		// Display only up to 16 matches.
		longMatchDisplayCount = 16
	}
	for i := 0; i < longMatchDisplayCount; i++ {
		log.Printf("long match #%d: %s\n", i+1, sink.longMatches[i])
	}
}

func (sink *Sink) displayLargeRatingDifferenceMatches() {
	sink.largeRatingDifferenceMatchLock.RLock()
	defer sink.largeRatingDifferenceMatchLock.RUnlock()
	log.Printf("%d large rating difference matches found so far\n", len(sink.largeRatingDifferenceMatches))
	largeRatingDifferenceMatchDisplayCount := len(sink.largeRatingDifferenceMatches)
	if largeRatingDifferenceMatchDisplayCount > 16 {
		// Display only up to 16 matches.
		largeRatingDifferenceMatchDisplayCount = 16
	}
	for i := 0; i < largeRatingDifferenceMatchDisplayCount; i++ {
		log.Printf("large rating difference match #%d: %s\n", i+1, sink.largeRatingDifferenceMatches[i])
	}
}
