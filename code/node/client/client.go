package client

import (
	"bufio"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"tp2.aba.distros.fi.uba.ar/common/config"
	"tp2.aba.distros.fi.uba.ar/common/middleware"
)

const MatchDataPathVarName string = "MatchDataPath"
const MatchDataPathDefault string = "matches.csv"
const MatchDataBatchSizeVarName string = "MatchDataBatchSize"
const MatchDataBatchSizeDefault int = 512

const PlayerDataPathVarName string = "PlayerDataPath"
const PlayerDataPathDefault string = "match_players.csv"
const PlayerDataBatchSizeVarName string = "PlayerDataBatchSize"
const PlayerDataBatchSizeDefault int = 512

func Run() {

	// Sleep to have the system up before launching the client.
	time.Sleep(time.Duration(30) * time.Second)

	waitGroup := &sync.WaitGroup{}

	// Launch the match data writer.
	waitGroup.Add(1)
	matchWriterQuitChannel := make(chan int, 1)
	go MatchDataWriter(waitGroup, matchWriterQuitChannel)

	// Launch the player data writer.
	waitGroup.Add(1)
	playerWriterQuitChannel := make(chan int, 1)
	go PlayerDataWriter(waitGroup, playerWriterQuitChannel)

	// Wait for a quit signal.
	sigchannel := make(chan os.Signal, 1)
	signal.Notify(sigchannel, syscall.SIGINT, syscall.SIGTERM)
	<-sigchannel

	// Send quit signal to the workers.
	matchWriterQuitChannel <- 0
	playerWriterQuitChannel <- 0

	// Wait for both jobs to finish.
	waitGroup.Wait()
}

func MatchDataWriter(waitGroup *sync.WaitGroup, quitChannel <-chan int) {
	log.Println("[Match Data Writer] starting")

	// Ensure that done will be called.
	defer waitGroup.Done()
	// Connect to the exchange.
	publisher, err := middleware.CreateMatchDataPublisher()

	if err != nil {
		log.Println("[Match Data Writer] could not connect to the exchange")
		return
	} else {
		log.Println("[Match Data Writer] established a connection to the exchange")
	}

	// Get path to match data.
	path := config.GetStringOrDefault(MatchDataPathVarName, MatchDataPathDefault)
	// Open the file for reading.
	file, err := os.Open(path)

	if err != nil {
		log.Println("[Match Data Writer] could not open match data file")
	} else {
		log.Println("[Match Data Writer] opened data file for reading, reading records")

		// Get batch size for record batches and instantiate a buffer.
		batchSize, _ := config.GetIntOrDefault(MatchDataBatchSizeVarName, MatchDataBatchSizeDefault)
		batchBuffer := make([]*middleware.MatchRecord, 0, batchSize)

		// Read the file line by line and batch match records.
		scanner := bufio.NewScanner(file)
		skippedFirstLine := false

		for scanner.Scan() {
			// Skip header.
			if !skippedFirstLine {
				skippedFirstLine = true
				continue
			}
			// Get current line.
			line := scanner.Text()
			// Split line by fields.
			split := strings.Split(line, ",")
			// Construct a new match record from the data.
			record := middleware.CreateMatchRecordFromSlice(split)
			// Batch the record.
			batchBuffer = append(batchBuffer, record)
			// If the buffer is full, publish the batch and reset the buffer.
			if len(batchBuffer) == cap(batchBuffer) {
				// Create a batch from the buffered records.
				batch := middleware.CreateMatchRecordBatch(batchBuffer)
				// Publish the batch.
				if err := publisher.PublishMatchData(batch); err != nil {
					log.Println("[Match Data Writer] could not publish batch")
				}
				// Empty the buffer.
				batchBuffer = batchBuffer[:0]
			}
		}
		// If there are records left in the buffer, publish them.
		if len(batchBuffer) > 0 {
			// Create a batch from the buffered records.
			batch := middleware.CreateMatchRecordBatch(batchBuffer)
			// Publish the batch.
			if err := publisher.PublishMatchData(batch); err != nil {
				log.Println("[Match Data Writer] could not publish batch")
			}
		}
	}

	if err := publisher.Close(); err != nil {
		log.Println("[Match Data Writer] could not close connection to the exchange")
	}
}

func PlayerDataWriter(waitGroup *sync.WaitGroup, quitChannel <-chan int) {
	log.Println("[Player Data Writer] starting")

	// Ensure that done will be called.
	defer waitGroup.Done()
	// Connect to the exchange.
	publisher, err := middleware.CreatePlayerDataPublisher()

	if err != nil {
		log.Println("[Player Data Writer] could not connect to the exchange")
		return
	} else {
		log.Println("[Player Data Writer] established a connection to the exchange")
	}

	// Get path to match data.
	path := config.GetStringOrDefault(PlayerDataPathVarName, PlayerDataPathDefault)
	// Open the file for reading.
	file, err := os.Open(path)

	if err != nil {
		log.Println("[Player Data Writer] could not open player data file")
	} else {
		log.Println("[Player Data Writer] opened data file for reading, reading records")

		// Get batch size for record batches and instantiate a buffer.
		batchSize, _ := config.GetIntOrDefault(PlayerDataBatchSizeVarName, PlayerDataBatchSizeDefault)
		batchBuffer := make([]*middleware.PlayerRecord, 0, batchSize)

		// Read the file line by line and batch match records.
		scanner := bufio.NewScanner(file)
		skippedFirstLine := false

		for scanner.Scan() {
			// Skip header.
			if !skippedFirstLine {
				skippedFirstLine = true
				continue
			}
			// Get current line.
			line := scanner.Text()
			// Split line by fields.
			split := strings.Split(line, ",")
			// Construct a new match record from the data.
			record := middleware.CreatePlayerRecordFromSlice(split)
			// Batch the record.
			batchBuffer = append(batchBuffer, record)
			// If the buffer is full, publish the batch and reset the buffer.
			if len(batchBuffer) == cap(batchBuffer) {
				// Create a batch from the buffered records.
				batch := middleware.CreatePlayerRecordBatch(batchBuffer)
				// Publish the batch.
				if err := publisher.PublishPlayerData(batch); err != nil {
					log.Println("[Player Data Writer] could not publish batch")
				}
				// Empty the buffer.
				batchBuffer = batchBuffer[:0]
			}
		}
		// If there are records left in the buffer, publish them.
		if len(batchBuffer) > 0 {
			// Create a batch from the buffered records.
			batch := middleware.CreatePlayerRecordBatch(batchBuffer)
			// Publish the batch.
			if err := publisher.PublishPlayerData(batch); err != nil {
				log.Println("[Player Data Writer] could not publish batch")
			}
		}
	}

	if err := publisher.Close(); err != nil {
		log.Println("[Player Data Writer] could not close connection to the exchange")
	}
}
