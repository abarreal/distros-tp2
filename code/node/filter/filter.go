package filter

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"tp2.aba.distros.fi.uba.ar/common/config"
	"tp2.aba.distros.fi.uba.ar/common/middleware"
)

//=================================================================================================
// Long matches
//-------------------------------------------------------------------------------------------------
const LongMatchFilterInputQueueVarName string = "LongMatchFilterInputQueue"
const LongMatchFilterInputQueueDefault string = "lmfilter_input_queue"

type LongMatchFilterInstance struct {
	id        int
	publisher *middleware.AggregationDataPublisher
}

func RunLongMatchFilter() error {
	waitGroup := &sync.WaitGroup{}
	var consumer *middleware.MatchDataConsumer = nil
	var err error = nil
	// Instantiate a filter object to hold some data.
	filter := &LongMatchFilterInstance{}
	filter.id, _ = config.GetIntOrDefault("InstanceId", 0)
	// Get the name of the queue shared by long match filter instances.
	queueName := config.GetStringOrDefault(
		LongMatchFilterInputQueueVarName,
		LongMatchFilterInputQueueDefault)
	// Initialize consumer.
	if consumer, err = middleware.CreateMatchDataConsumer(queueName); err != nil {
		return err
	}
	consumer.RegisterOnWaitGroup(waitGroup)
	// Initialize the publisher to publish results.
	filter.publisher, err = middleware.CreateAggregationDataPublisher()
	// Close the consumer and return if the publisher could not be initialized.
	if err != nil {
		consumer.Close()
		return err
	}
	// Begin consuming match data.
	log.Println("beginning match data consumption")
	go consumer.Consume(filter.longMatchCallback)

	// Wait for a quit signal.
	sigchannel := make(chan os.Signal, 1)
	signal.Notify(sigchannel, syscall.SIGINT, syscall.SIGTERM)
	<-sigchannel

	// Stop the consumer.
	consumer.Stop()
	waitGroup.Wait()
	return nil
}

func (filter *LongMatchFilterInstance) longMatchCallback(batch *middleware.MatchRecordBatch) {
	// For each record, determine whether it fits the long match criteria for those types of
	// match that we are interested in. For those that do, send a message through the
	// output queue.
	for _, record := range batch.Records {
		// We are interested only in matches in which the average rating is greater than 2000.
		if !record.AverageRatingAbove(2000.0) {
			continue
		}
		// We are interested only in matches in a few game servers.
		if !(record.InServer("koreacentral") || record.InServer("southeastasia") || record.InServer("eastus")) {
			continue
		}
		// We are interested only in matches above two hours.
		if !record.IsLongMatch() {
			continue
		}
		// We are interested in this match. Proceed to publish a record through the publisher.
		lmrecord := middleware.CreateSingleTokenRecord(record.Token)
		if err := filter.publisher.PublishLongMatch(lmrecord); err != nil {
			log.Println("long match record could not be published")
		}
	}
}

//=================================================================================================
// Large rating difference
//-------------------------------------------------------------------------------------------------
// TODO

//=================================================================================================
// Civilization victories
//-------------------------------------------------------------------------------------------------
// TODO

//=================================================================================================
// Top 5 civilizations
//-------------------------------------------------------------------------------------------------
// TODO
