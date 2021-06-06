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
	unjoinedMatchesLock *sync.RWMutex
	unjoinedMatches     map[string]*middleware.MatchRecord
	// Keep track of unjoined players.
	unjoinedPlayersLock *sync.RWMutex
	unjoinedPlayers     map[string]([]*middleware.PlayerRecord)
}

func Run() {
	var err error

	waitGroup := &sync.WaitGroup{}

	// Initialize the join object to hold join related data.
	join := &Join{}
	join.unjoinedMatchesLock = &sync.RWMutex{}
	join.unjoinedMatches = make(map[string]*middleware.MatchRecord)
	join.unjoinedPlayersLock = &sync.RWMutex{}
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
		// Check if we have all the players for this match already. If we do,
		// create a joint record and publish. Cache match data otherwise.
		if join.canJoinMatch(record) {
			join.doJoin(record.Token)
		} else {
			join.unjoinedMatchesLock.Lock()
			defer join.unjoinedMatchesLock.Unlock()
			// Keep track of the match to join later.
			join.unjoinedMatches[record.Token] = record
		}
	}
}

func (join *Join) handlePlayerDataBatch(batch *middleware.PlayerRecordBatch) {
	// Try to join each record.
	for _, record := range batch.Records {
		if join.canJoinPlayer(record) {
			join.doJoin(record.Match)
		} else {
			join.unjoinedPlayersLock.Lock()
			defer join.unjoinedPlayersLock.Unlock()
			// Keep track of the player to join later. Instantiate an empty buffer
			// to hold match players if we have none.
			players, found := join.unjoinedPlayers[record.Match]

			if !found {
				players = make([]*middleware.PlayerRecord, 0)
			} else {
				players = append(players, record)
			}

			join.unjoinedPlayers[record.Match] = players
		}
	}
}

func (join *Join) canJoinMatch(record *middleware.MatchRecord) bool {
	requiredPlayerCount := record.NumPlayers
	join.unjoinedPlayersLock.RLock()
	currentPlayerCount := len(join.unjoinedPlayers[record.Token])
	join.unjoinedPlayersLock.RUnlock()
	return requiredPlayerCount == currentPlayerCount
}

func (join *Join) canJoinPlayer(record *middleware.PlayerRecord) bool {
	join.unjoinedMatchesLock.RLock()
	matchRecord, found := join.unjoinedMatches[record.Match]
	join.unjoinedMatchesLock.RUnlock()

	if found {
		// We have a record for the match. Get the amount of players that we need
		// and verify if we have all players now. It is assumed that there are no
		// duplicate records.
		requiredPlayerCount := matchRecord.NumPlayers
		join.unjoinedPlayersLock.RLock()
		currentPlayerCount := len(join.unjoinedPlayers[record.Token]) + 1
		join.unjoinedPlayersLock.RUnlock()
		return requiredPlayerCount == currentPlayerCount
	} else {
		return false
	}
}

func (join *Join) doJoin(matchToken string) {
	// Get match and player data from storage.
	join.unjoinedMatchesLock.RLock()
	join.unjoinedPlayersLock.RLock()

	match := join.unjoinedMatches[matchToken]
	players := join.unjoinedPlayers[matchToken]

	join.unjoinedPlayersLock.RUnlock()
	join.unjoinedMatchesLock.RUnlock()

	// Do join and publish.
	joint := middleware.Join(match, players)
	join.publisher.PublishJointData(joint)

	// Remove data from cache.
	join.unjoinedMatchesLock.Lock()
	join.unjoinedPlayersLock.Lock()
	defer join.unjoinedMatchesLock.Unlock()
	defer join.unjoinedPlayersLock.Unlock()

	delete(join.unjoinedMatches, matchToken)
	delete(join.unjoinedPlayers, matchToken)
}
