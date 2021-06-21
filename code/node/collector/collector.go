package collector

import (
	"log"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"

	"tp2.aba.distros.fi.uba.ar/common/middleware"
)

//=================================================================================================
// Civilization victory data collector
//-------------------------------------------------------------------------------------------------

// Collects civilization performance data information and computes win rate for each one. Then it
// publishes the latest statistics through a queue.
type CivilizationPerformanceDataCollector struct {
	// Keep track of victory and defeat count for each civilization in the map arena.
	// For a more general solution use a map from strings (game map name) to maps like this.
	performanceData map[string]([]int)
	// Define a consumer from which to read victory and defeat results.
	consumer *middleware.CivilizationInfoRecordConsumer
}

func RunCivilizationVictoryDataCollector() {
	var err error
	waitGroup := &sync.WaitGroup{}

	// Instantiate the collector.
	collector := &CivilizationPerformanceDataCollector{}
	collector.performanceData = make(map[string][]int)
	// Instantiate the consumer itself.
	if collector.consumer, err = middleware.CreateArenaCivilizationVictoryDataConsumer(); err != nil {
		log.Println("could not create civilization victory data consumer")
	}
	defer collector.consumer.Close()
	collector.consumer.RegisterOnWaitGroup(waitGroup)

	// Run the consumer.
	go collector.consumer.Consume(collector.handleCivilizationPerformanceData)

	// Wait for an incoming quit signal.
	sigchannel := make(chan os.Signal, 1)
	signal.Notify(sigchannel, syscall.SIGINT, syscall.SIGTERM)
	<-sigchannel

	// Stop the consumer.
	collector.consumer.Stop()
	// Wait for the consumer to finalize.
	waitGroup.Wait()
}

func (collector *CivilizationPerformanceDataCollector) handleCivilizationPerformanceData(
	batch *middleware.CivilizationInfoRecordBatch) {

	// Aggregate incoming data.
	// We have to store the amount of victories and loses for each civilization.
	for _, record := range batch.Records {
		// Get current stats and initialize if none.
		current, found := collector.performanceData[record.CivilizationName]
		if !found {
			// Create an array of length 2 to hold victories in the first element and defeats in the second.
			current = []int{0, 0}
			collector.performanceData[record.CivilizationName] = current
		}
		if record.IndicatesVictory() {
			current[0]++
		} else if record.IndicatesDefeat() {
			current[1]++
		}
	}

	// Publish updated statistics. To make better use of the computing resources, publish
	// the statistics only periodically to avoid repeating the computation on every message.
	victoryRate := make(map[string]float32)

	for cname, data := range collector.performanceData {
		victories := data[0]
		total := victories + data[1]
		victoryRate[cname] = float32(victories) / float32(total)
	}

	// TODO: Publish the statistics.
}

//=================================================================================================
// Civilization usage data collector
//-------------------------------------------------------------------------------------------------

type CivilizationUsageRecord struct {
	CivilizationName string
	UsageCount       int
}

// Collects civilization usage data information and aggregates usage count for each one. Then it
// publishes the aggregated count through a queue.
type CivilizationUsageDataCollector struct {
	// Keep track of civilization usage in islands.
	// For a more general solution use a map from strings (game map name) to maps like this.
	usageData map[string]*CivilizationUsageRecord
	consumer  *middleware.CivilizationInfoRecordConsumer
}

func RunCivilizationUsageDataCollector() {
	var err error
	waitGroup := &sync.WaitGroup{}

	// Instantiate the collector.
	collector := &CivilizationUsageDataCollector{}
	collector.usageData = make(map[string]*CivilizationUsageRecord)
	// Instantiate the consumer itself.
	if collector.consumer, err = middleware.CreateIslandsCivilizationUsageDataConsumer(); err != nil {
		log.Println("could not create civilization usage data consumer")
	}
	defer collector.consumer.Close()
	collector.consumer.RegisterOnWaitGroup(waitGroup)

	// Run the consumer.
	go collector.consumer.Consume(collector.handleCivilizationUsageData)

	// Wait for an incoming quit signal.
	sigchannel := make(chan os.Signal, 1)
	signal.Notify(sigchannel, syscall.SIGINT, syscall.SIGTERM)
	<-sigchannel

	// Stop the consumer.
	collector.consumer.Stop()
	// Wait for the consumer to finalize.
	waitGroup.Wait()
}

func (collector *CivilizationUsageDataCollector) handleCivilizationUsageData(
	batch *middleware.CivilizationInfoRecordBatch) {

	// We have to store the amount of times each civilization was used in islands.
	for _, record := range batch.Records {
		current, found := collector.usageData[record.CivilizationName]
		// Initialize counter if not found.
		if !found {
			current = &CivilizationUsageRecord{record.CivilizationName, 0}
			collector.usageData[record.CivilizationName] = current
		}
		// Increase counter by one in any case.
		current.UsageCount++
	}

	// Determine and publish current top 5. Push them all into an array first,
	// to sort then by usage and determine the 5 most used.
	usageRecords := make([]*CivilizationUsageRecord, 0)

	for _, record := range collector.usageData {
		usageRecords = append(usageRecords, record)
	}
	if len(usageRecords) == 0 {
		// We have no usage data yet, so we just return.
		return
	}

	// Sort by usage count.
	sort.Slice(usageRecords, func(i, j int) bool {
		return usageRecords[i].UsageCount < usageRecords[j].UsageCount
	})
	// Keep only the first 5.
	cap := len(usageRecords)
	if cap > 5 {
		cap = 5
	}
	usageRecords = usageRecords[:cap]

	// Publish current top 5.
	// TODO
}
