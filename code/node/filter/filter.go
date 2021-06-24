package filter

import (
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"tp2.aba.distros.fi.uba.ar/common/config"
	"tp2.aba.distros.fi.uba.ar/common/middleware"
)

//=================================================================================================
// Long matches
//-------------------------------------------------------------------------------------------------
const LongMatchFilterInputQueue string = "lmfilter_input_queue"

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
	// All instances of this filter will be listening on the same queue.
	queueName := LongMatchFilterInputQueue

	// Initialize consumer.
	if consumer, err = middleware.CreateMatchDataConsumer(queueName); err != nil {
		log.Println("could not create match data consumer")
		return err
	}
	defer consumer.Close()
	consumer.RegisterOnWaitGroup(waitGroup)

	// Initialize the publisher to publish results.
	if filter.publisher, err = middleware.CreateAggregationDataPublisher(); err != nil {
		return err
	}
	defer filter.publisher.Close()

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
const LargeRatingDifferenceFilterInputQueue string = "ldf_input_queue"

type LargeRatingDifferenceFilterInstance struct {
	id        int
	publisher *middleware.AggregationDataPublisher
}

func RunLargeRatingDifferenceFilter() error {
	waitGroup := &sync.WaitGroup{}
	var consumer *middleware.JointDataConsumer = nil
	var err error = nil
	// Instantiate a filter object to hold some data.
	filter := &LargeRatingDifferenceFilterInstance{}
	filter.id, _ = config.GetIntOrDefault("InstanceId", 0)
	// All instances of this filter will be listening on the same queue.
	queueName := LargeRatingDifferenceFilterInputQueue

	// Initialize consumer.
	if consumer, err = middleware.CreateJointDataConsumer(queueName); err != nil {
		log.Println("could not create match data consumer")
		return err
	}
	defer consumer.Close()
	consumer.RegisterOnWaitGroup(waitGroup)

	// Initialize the publisher to publish results.
	if filter.publisher, err = middleware.CreateAggregationDataPublisher(); err != nil {
		return err
	}
	defer filter.publisher.Close()

	// Begin consuming match data.
	log.Println("beginning joint data consumption")
	go consumer.Consume(filter.largeRatingDifferenceCallback)

	// Wait for a quit signal.
	sigchannel := make(chan os.Signal, 1)
	signal.Notify(sigchannel, syscall.SIGINT, syscall.SIGTERM)
	<-sigchannel

	// Stop the consumer.
	consumer.Stop()
	waitGroup.Wait()
	return nil
}

func (filter *LargeRatingDifferenceFilterInstance) largeRatingDifferenceCallback(
	batch *middleware.JointMatchRecordBatch) {

	for _, record := range batch.Records {
		// Determine whether the match was 1v1.
		if !record.Is1v1() {
			continue
		}
		// Determine whether the rating of the winner is greater than 1000.
		_, winner := record.Winner()

		if winner == nil {
			continue
		}
		if winner.Rating < 1000 {
			continue
		}
		// Determine whether the rating of the winner is at least 30% less than that
		// of the winner.
		loser := record.Loser1v1()
		if loser == nil || !(winner.Rating < 0.7*loser.Rating) {
			continue
		}
		// Publish record through publisher.
		accepted := middleware.CreateSingleTokenRecord(record.MatchToken)
		if err := filter.publisher.PublishLargeRatingDifferenceMatch(accepted); err != nil {
			log.Println("large rating record could not be published")
		}
	}

}

//=================================================================================================
// Top 5 most used civilizations
//-------------------------------------------------------------------------------------------------
const CivilizationUsageCountInputQueue string = "civilization_usage_count_input"

type CivilizationUsageCountFilter struct {
	id        int
	publisher *middleware.AggregationDataPublisher
}

func RunCivilizationUsageCountFilter() error {
	waitGroup := &sync.WaitGroup{}
	var consumer *middleware.JointDataConsumer = nil
	var err error = nil
	// Instantiate a filter object to hold some data.
	filter := &CivilizationUsageCountFilter{}
	filter.id, _ = config.GetIntOrDefault("InstanceId", 0)
	// All instances of this filter will be listening on the same queue.
	queueName := CivilizationUsageCountInputQueue

	// Initialize consumer.
	if consumer, err = middleware.CreateJointDataConsumer(queueName); err != nil {
		log.Println("could not create joint data consumer")
		return err
	}
	defer consumer.Close()
	consumer.RegisterOnWaitGroup(waitGroup)

	// Initialize the publisher to publish results.
	if filter.publisher, err = middleware.CreateAggregationDataPublisher(); err != nil {
		return err
	}
	defer filter.publisher.Close()

	// Begin consuming match data.
	log.Println("beginning joint data consumption")
	go consumer.Consume(filter.jointDataCallbackForUsageCount)

	// Wait for a quit signal.
	sigchannel := make(chan os.Signal, 1)
	signal.Notify(sigchannel, syscall.SIGINT, syscall.SIGTERM)
	<-sigchannel

	// Stop the consumer.
	consumer.Stop()
	waitGroup.Wait()
	return nil
}

func (filter *CivilizationUsageCountFilter) jointDataCallbackForUsageCount(
	batch *middleware.JointMatchRecordBatch) {

	for _, record := range batch.Records {
		// If the map is not islands, continue.
		if strings.ToLower(record.MapName) != "islands" {
			continue
		}
		// If the game is not a team match, continue.
		if !record.IsTeamGame() {
			continue
		}
		// We have a match we are interested in. Check the players to see
		// what civilization they are using.
		for _, player := range record.Players {
			// If the player is not pro, continue.
			if !player.IsPro() {
				continue
			}
			// We have a player we are interested in. Check what civilization they are using.
			civRecord := middleware.CreateCivilizationUsageRecord(player.CivilizationName)
			civRecordBatch := middleware.CreateCivilizationInfoRecordBatch([]*middleware.CivilizationInfoRecord{civRecord})
			if err := filter.publisher.PublishCivilizationUsageRecord(civRecordBatch); err != nil {
				log.Println("civilization usage record could not be published")
			}
		}
	}
}

//=================================================================================================
// Civilization victory rate
//-------------------------------------------------------------------------------------------------
const CivilizationVictoryDataFilterInputQueue string = "civilization_victory_data_filter_input"

type CivilizationVictoryDataFilter struct {
	id        int
	publisher *middleware.AggregationDataPublisher
}

func RunCivilizationVictoryDataFilter() error {
	waitGroup := &sync.WaitGroup{}
	var consumer *middleware.JointDataConsumer = nil
	var err error = nil
	// Instantiate a filter object to hold some data.
	filter := &CivilizationVictoryDataFilter{}
	filter.id, _ = config.GetIntOrDefault("InstanceId", 0)
	// All instances of this filter will be listening on the same queue.
	queueName := CivilizationVictoryDataFilterInputQueue

	// Initialize consumer.
	if consumer, err = middleware.CreateJointDataConsumer(queueName); err != nil {
		log.Println("could not create joint data consumer")
		return err
	}
	defer consumer.Close()
	consumer.RegisterOnWaitGroup(waitGroup)

	// Initialize the publisher to publish results.
	if filter.publisher, err = middleware.CreateAggregationDataPublisher(); err != nil {
		return err
	}
	defer filter.publisher.Close()

	// Begin consuming match data.
	log.Println("beginning joint data consumption")
	go consumer.Consume(filter.jointDataCallbackForVictoryStats)

	// Wait for a quit signal.
	sigchannel := make(chan os.Signal, 1)
	signal.Notify(sigchannel, syscall.SIGINT, syscall.SIGTERM)
	<-sigchannel

	// Stop the consumer.
	consumer.Stop()
	waitGroup.Wait()
	return nil
}

func (filter *CivilizationVictoryDataFilter) jointDataCallbackForVictoryStats(
	batch *middleware.JointMatchRecordBatch) {

	for _, record := range batch.Records {
		// If the map is not arena, continue.
		if strings.ToLower(record.MapName) != "arena" {
			continue
		}
		// If the game is not 1v1, continue.
		if !record.Is1v1() {
			continue
		}
		// Get the two players of the match. If they are using the same
		// civilization, continue.
		player1 := record.Players[0]
		player2 := record.Players[1]

		if player1.CivilizationName == player2.CivilizationName {
			continue
		}

		// Publish statistics about the winner.
		var victor *middleware.JointPlayerRecord = nil
		var loser *middleware.JointPlayerRecord = nil

		if player1.Winner {
			victor = player1
			loser = player2
		} else {
			victor = player2
			loser = player1
		}

		winRecord := middleware.CreateCivilizationVictoryRecord(victor.CivilizationName)
		loseRecord := middleware.CreateCivilizationDefeatRecord(loser.CivilizationName)
		batch := middleware.CreateCivilizationInfoRecordBatch([]*middleware.CivilizationInfoRecord{
			winRecord,
			loseRecord,
		})
		if err := filter.publisher.PublishCivilizationPerformanceData(batch); err != nil {
			log.Println("civilization performance data could not be published")
		}
	}
}
