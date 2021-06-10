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
	publisher *middleware.JointDataPublisher
	// Keep track of unjoined matches.
	unjoinedMatches map[string]*middleware.MatchRecord
	// Keep track of unjoined players.
	unjoinedPlayers map[string]([]*middleware.PlayerRecord)
	// Use a lock for mutual exclusion between the thread reading player data
	// and the one reading match data.
	datalock *sync.Mutex
}

func Run() {
	var err error

	waitGroup := &sync.WaitGroup{}

	// Initialize the join object to hold join related data.
	join := &Join{}
	join.datalock = &sync.Mutex{}
	join.unjoinedMatches = make(map[string]*middleware.MatchRecord)
	join.unjoinedPlayers = make(map[string]([]*middleware.PlayerRecord))
	join.publisher, err = middleware.CreateJointDataPublisher()

	if err != nil {
		log.Println("could not create joint data publisher")
		return
	}

	// Instantiate a consumer for match data.
	mdQueue := config.GetStringOrDefault(MatchQueueForJoinVarName, MatchQueueForJoinDefault)
	mdConsumer, err := middleware.CreateMatchDataConsumer(mdQueue)

	if err != nil {
		log.Println("could not create match data consumer")
		join.publisher.Close()
		return
	} else {
		mdConsumer.RegisterOnWaitGroup(waitGroup)
	}

	// Instantiate a consumer for player data.
	pdQueue := config.GetStringOrDefault(PlayerQueueForJoinVarName, PlayerQueueForJoinDefault)
	pdConsumer, err := middleware.CreatePlayerDataConsumer(pdQueue)

	if err != nil {
		log.Println("could not create player data consumer")
		join.publisher.Close()
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
	// Try to join each record.
	for _, record := range batch.Records {
		join.datalock.Lock()
		join.saveMatch(record)
		// Check if we have all the players for this match already. If we do,
		// create a joint record and publish. Cache match data otherwise.
		if join.canJoinMatch(record) {
			join.doJoin(record.Token)
		}
		join.datalock.Unlock()
	}
}

func (join *Join) handlePlayerDataBatch(batch *middleware.PlayerRecordBatch) {
	// Try to join each record.
	for _, record := range batch.Records {
		join.datalock.Lock()
		join.savePlayer(record)
		if join.canJoinPlayer(record) {
			join.doJoin(record.Match)
		}
		join.datalock.Unlock()
	}
}

func (join *Join) savePlayer(record *middleware.PlayerRecord) {
	players, found := join.unjoinedPlayers[record.Match]

	if !found {
		players = make([]*middleware.PlayerRecord, 0)
	}

	players = append(players, record)
	join.unjoinedPlayers[record.Match] = players
}

func (join *Join) saveMatch(record *middleware.MatchRecord) {
	join.unjoinedMatches[record.Token] = record
}

func (join *Join) canJoinMatch(record *middleware.MatchRecord) bool {
	requiredPlayerCount := record.NumPlayers
	if players, found := join.unjoinedPlayers[record.Token]; found {
		return requiredPlayerCount == len(players)
	} else {
		return false
	}
}

func (join *Join) canJoinPlayer(record *middleware.PlayerRecord) bool {
	matchRecord, found := join.unjoinedMatches[record.Match]

	if found {
		// We have a record for the match. Get the amount of players that we need
		// and verify if we have all players now. It is assumed that there are no
		// duplicate records.
		requiredPlayerCount := matchRecord.NumPlayers

		if players, found := join.unjoinedPlayers[record.Token]; found {
			return requiredPlayerCount == len(players)
		} else {
			return false
		}

	} else {
		return false
	}
}

func (join *Join) doJoin(matchToken string) {
	// Get match and player data from storage.
	match := join.unjoinedMatches[matchToken]
	players := join.unjoinedPlayers[matchToken]
	// Do join and publish.
	joint := middleware.Join(match, players)
	batch := middleware.CreateJointMatchRecordBatch([]*middleware.JointMatchRecord{joint})
	join.publisher.PublishJointData(batch)
	// Remove data from cache.
	delete(join.unjoinedMatches, matchToken)
	delete(join.unjoinedPlayers, matchToken)
}
