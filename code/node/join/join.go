package join

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"tp2.aba.distros.fi.uba.ar/common/config"
	"tp2.aba.distros.fi.uba.ar/common/middleware"
)

const MatchQueueForJoinVarName = "MatchQueueForJoin"
const MatchQueueForJoinDefault = "match_queue_for_joining"

const PlayerQueueForJoinVarName = "PlayerQueueForJoin"
const PlayerQueueForJoinDefault = "player_queue_for_joining"

type Join struct {
}

func Run() {
	waitGroup := &sync.WaitGroup{}

	// Initialize the join object to hold join related data.
	join := &Join{}

	// Instantiate a consumer for match data.
	mdQueue := config.GetStringOrDefault(MatchQueueForJoinVarName, MatchQueueForJoinDefault)
	mdConsumer, err := middleware.CreateMatchDataConsumer(mdQueue)

	if err != nil {
		log.Println("could not create match data consumer")
		return
	} else {
		mdConsumer.RegisterOnWaitGroup(waitGroup)
	}

	// Instantiate a consumer for player data.
	pdQueue := config.GetStringOrDefault(PlayerQueueForJoinVarName, PlayerQueueForJoinDefault)
	pdConsumer, err := middleware.CreatePlayerDataConsumer(pdQueue)

	if err != nil {
		log.Println("could not create player data consumer")
		mdConsumer.Close()
		return
	} else {
		pdConsumer.RegisterOnWaitGroup(waitGroup)
	}

	// Launch.
	go mdConsumer.Consume(join.handleMatchDataBatch)
	go pdConsumer.Consume(join.handlePlayerDataBatch)

	// Wait for a quit signal.
	sigchannel := make(chan os.Signal, 1)
	signal.Notify(sigchannel, syscall.SIGINT, syscall.SIGTERM)
	<-sigchannel

	// Stop consumers.
	mdConsumer.Stop()
	pdConsumer.Stop()
	// Wait for consumers to finish.
	waitGroup.Wait()
}

func (join *Join) handleMatchDataBatch(batch *middleware.MatchRecordBatch) {
	// TODO
	log.Println("RECEIVED NEW MATCH DATA")
}

func (join *Join) handlePlayerDataBatch(batch *middleware.PlayerRecordBatch) {
	// TODO
	log.Println("RECEIVED NEW PLAYER DATA")
}
