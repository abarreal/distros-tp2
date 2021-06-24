package join

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

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
	// Counters to keep track of what is going on.
	receivedMatchesCount int
	receivedPlayersCount int
	joinedMatchesCount   int
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
	join.receivedMatchesCount = 0
	join.receivedPlayersCount = 0
	join.joinedMatchesCount = 0

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

	// Launch monitor.
	go monitor(join)

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
	join.receivedPlayersCount++
}

func (join *Join) saveMatch(record *middleware.MatchRecord) {
	join.unjoinedMatches[record.Token] = record
	join.receivedMatchesCount++
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

		if players, found := join.unjoinedPlayers[matchRecord.Token]; found {
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
	join.joinedMatchesCount++
	// Remove data from cache.
	delete(join.unjoinedMatches, matchToken)
	delete(join.unjoinedPlayers, matchToken)
}

func monitor(join *Join) {
	timeout := time.After(time.Duration(15) * time.Second)

	for {
		<-timeout

		// Print the amount of unjoined records.
		join.datalock.Lock()
		log.Printf("received matches: %d\n", join.receivedMatchesCount)
		log.Printf("received players: %d\n", join.receivedPlayersCount)
		log.Printf("joined records: %d\n", join.joinedMatchesCount)

		// Grab an few matches and check players.
		count := 0
		for token, match := range join.unjoinedMatches {
			players := join.unjoinedPlayers[token]
			log.Println(match.Token)
			log.Println(match.NumPlayers)
			log.Println(len(players))
			count++
			if count == 3 {
				break
			}
		}

		join.datalock.Unlock()

		timeout = time.After(time.Duration(15) * time.Second)
	}
}
