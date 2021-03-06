package middleware

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
	"tp2.aba.distros.fi.uba.ar/common/config"
)

//=================================================================================================
// General
//-------------------------------------------------------------------------------------------------
type channelConsumer interface {
	consumerChannel(queueName string) (<-chan amqp.Delivery, error)
}

type Connector struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queues  map[string]*amqp.Queue
}

func (connector *Connector) initializeBaseParams() {
	connector.queues = make(map[string]*amqp.Queue)
}

func (connector *Connector) Connect() error {
	var err error = nil
	if connectionString, found := config.GetString("RMQConnectionString"); !found {
		return errors.New("RMQ connection string not defined")
	} else {
		// Open a connection to the exchange.
		// Retry connection until the exchange responds.
		for connector.conn == nil {
			log.Println("attempting connection to the exchange")
			if connector.conn, err = amqp.Dial(connectionString); err != nil {
				log.Println("connection failed, waiting 5 seconds")
				time.Sleep(time.Duration(5) * time.Second)
			}
		}
		// Open a channel to the exchange.
		if connector.channel, err = connector.conn.Channel(); err != nil {
			connector.conn.Close()
			return err
		}
	}
	// Return no error.
	return nil
}

func (connector *Connector) Close() error {
	err1 := connector.channel.Close()
	err2 := connector.conn.Close()

	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return nil
}

func (connector *Connector) declareFanOut(name string) error {
	return connector.channel.ExchangeDeclare(
		name,     // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
}

func (connector *Connector) declareDirect(name string) error {
	return connector.channel.ExchangeDeclare(
		name,     // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
}

func (connector *Connector) joinQueue(name string) error {
	log.Printf("joining queue %s\n", name)
	queue, err := connector.channel.QueueDeclare(
		name,  // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return err
	}
	connector.queues[name] = &queue
	return nil
}

func (connector *Connector) bindQueue(queueName string, exchangeName string) error {
	return connector.bindQueueWithRoutingKey(queueName, exchangeName, "")
}

func (connector *Connector) bindQueueWithRoutingKey(
	queueName string, exchangeName string, routingKey string) error {
	log.Printf("binding queue %s in exchange %s with routing key %s\n", queueName, exchangeName, routingKey)
	return connector.channel.QueueBind(
		queueName,    // queue name
		routingKey,   // routing key
		exchangeName, // exchange
		false,        // no-wait
		nil,          // arguments
	)
}

func (connector *Connector) connectToDirectExchange(exchangeName string) error {
	log.Printf("connecting to direct exchange %s\n", exchangeName)
	// Connect to the server.
	if err := connector.Connect(); err != nil {
		return err
	}
	// Declare the exchange.
	if err := connector.declareDirect(exchangeName); err != nil {
		return err
	}
	// We are connected to the exchange and we have declared it.
	// Return no error.
	return nil
}

func (connector *Connector) connectToFanOut(exchangeName string) error {
	log.Printf("connecting to fanout exchange %s\n", exchangeName)
	// Connect to the server.
	if err := connector.Connect(); err != nil {
		return err
	}
	// Declare the exchange.
	if err := connector.declareFanOut(exchangeName); err != nil {
		return err
	}
	// We are connected to the exchange and we have declared it.
	// Return no error.
	return nil
}

func (connector *Connector) publish(exchangeName string, data []byte) error {
	return connector.publishWithRoutingKey(exchangeName, data, "")
}

func (connector *Connector) publishWithRoutingKey(exchangeName string, data []byte, routingKey string) error {
	// Push data into the queue.
	return connector.channel.Publish(
		exchangeName, // exchange
		routingKey,   // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
		},
	)
}

func (connector *Connector) consumerChannel(queueName string) (<-chan amqp.Delivery, error) {
	msgs, err := connector.channel.Consume(
		queueName, // queue name
		"",        // consumer
		true,      // auto-ack
		false,     // exclusive
		false,     // no-wait
		false,     // no local
		nil,
	)
	if err != nil {
		return nil, err
	} else {
		return msgs, nil
	}
}

//=================================================================================================
// Player Records
//-------------------------------------------------------------------------------------------------
const PlayerDataExchangeVarName string = "PlayerDataExchange"
const PlayerDataExchangeDefault string = "player_record_fanout"

type playerDataExchanger struct {
	Connector
	exchangeName string
}

// Publisher
type PlayerDataPublisher struct {
	playerDataExchanger
}

func (exchanger *playerDataExchanger) initialize() error {
	// Get the name of the player data exchange from config.
	exchanger.initializeBaseParams()
	exchangeName := config.GetStringOrDefault(PlayerDataExchangeVarName, PlayerDataExchangeDefault)
	exchanger.exchangeName = exchangeName
	// Connect and declare.
	return exchanger.connectToFanOut(exchangeName)
}

func CreatePlayerDataPublisher() (*PlayerDataPublisher, error) {
	// Instantiate the data publisher.
	publisher := &PlayerDataPublisher{}
	// Connect to the message middleware.
	if err := publisher.initialize(); err != nil {
		return nil, err
	} else {
		return publisher, nil
	}
}

func (publisher *PlayerDataPublisher) PublishPlayerData(records *PlayerRecordBatch) error {
	if serialized, err := records.Serialize(); err != nil {
		return err
	} else {
		return publisher.publish(publisher.exchangeName, serialized)
	}
}

// Consumer
type PlayerDataConsumer struct {
	abstractConsumer
	playerDataExchanger
}

func CreatePlayerDataConsumer(queueName string) (*PlayerDataConsumer, error) {
	// Instantiate the data consumer.
	consumer := &PlayerDataConsumer{}
	consumer.quit = make(chan int)
	// Connect and declare the exchange.
	if err := consumer.initialize(); err != nil {
		return nil, err
	} else {
		consumer.queueName = queueName
		consumer.joinQueue(queueName)
		consumer.bindQueue(queueName, consumer.exchangeName)
		return consumer, nil
	}
}

// Blocks waiting for incoming player records. The given callback will be called for each record.
func (consumer *PlayerDataConsumer) Consume(callback func(*PlayerRecordBatch)) error {
	consumer.consumeFromQueue(consumer, consumer.queueName, func(message []byte) {
		// Deserialize the batch.
		if batch, err := DeserializePlayerRecords(message); err == nil {
			// The batch was deserialized correctly. Have it handled by the callback.
			callback(batch)
		} else {
			log.Println("could not deserialize batch")
		}
	})
	return nil
}

//=================================================================================================
// Match Records
//-------------------------------------------------------------------------------------------------
const MatchDataExchangeVarName string = "MatchDataExchange"
const MatchDataExchangeDefault string = "match_record_fanout"

type matchDataExchanger struct {
	Connector
	exchangeName string
}

func (exchanger *matchDataExchanger) initialize() error {
	// Get the name of the match data exchange from config.
	exchanger.initializeBaseParams()
	exchangeName := config.GetStringOrDefault(MatchDataExchangeVarName, MatchDataExchangeDefault)
	exchanger.exchangeName = exchangeName
	// Connect to the exchange.
	return exchanger.connectToFanOut(exchangeName)
}

// Publisher
type MatchDataPublisher struct {
	matchDataExchanger
}

func CreateMatchDataPublisher() (*MatchDataPublisher, error) {
	// Instantiate the data publisher.
	publisher := &MatchDataPublisher{}
	// Connect to the message middleware.
	if err := publisher.initialize(); err != nil {
		return nil, err
	} else {
		return publisher, nil
	}
}

func (publisher *MatchDataPublisher) PublishMatchData(records *MatchRecordBatch) error {
	if serialized, err := records.Serialize(); err != nil {
		return err
	} else {
		return publisher.publish(publisher.exchangeName, serialized)
	}
}

// Consumer
type MatchDataConsumer struct {
	abstractConsumer
	matchDataExchanger
}

func CreateMatchDataConsumer(queueName string) (*MatchDataConsumer, error) {
	// Instantiate the data consumer.
	consumer := &MatchDataConsumer{}
	// Connect and declare the exchange.
	if err := consumer.initialize(); err != nil {
		return nil, err
	} else {
		consumer.queueName = queueName
		consumer.joinQueue(queueName)
		consumer.bindQueue(queueName, consumer.exchangeName)
		return consumer, nil
	}
}

// Blocks waiting for incoming player records. The given callback will be called for each record.
func (consumer *MatchDataConsumer) Consume(callback func(*MatchRecordBatch)) error {
	consumer.consumeFromQueue(consumer, consumer.queueName, func(message []byte) {
		// Deserialize the batch.
		if batch, err := DeserializeMatchRecords(message); err == nil {
			// The batch was deserialized correctly. Have it handled by the callback.
			callback(batch)
		} else {
			log.Println("could not deserialize batch")
		}
	})
	return nil
}

//=================================================================================================
// Joint Records
//-------------------------------------------------------------------------------------------------
const JointDataExchangeVarName string = "JointDataExchange"
const JointDataExchangeDefault string = "joint_record_fanout"

type jointDataExchanger struct {
	Connector
	exchangeName string
}

func (exchanger *jointDataExchanger) initialize() error {
	// Get the name of the joint data exchange from config.
	exchanger.initializeBaseParams()
	exchangeName := config.GetStringOrDefault(JointDataExchangeVarName, JointDataExchangeDefault)
	exchanger.exchangeName = exchangeName
	// Connect to the exchange.
	return exchanger.connectToFanOut(exchangeName)
}

// Publisher
type JointDataPublisher struct {
	jointDataExchanger
}

func CreateJointDataPublisher() (*JointDataPublisher, error) {
	// Instantiate the data publisher.
	publisher := &JointDataPublisher{}
	// Connect to the message middleware.
	if err := publisher.initialize(); err != nil {
		return nil, err
	} else {
		return publisher, nil
	}
}

func (publisher *JointDataPublisher) PublishJointData(records *JointMatchRecordBatch) error {
	if serialized, err := records.Serialize(); err != nil {
		return err
	} else {
		return publisher.publish(publisher.exchangeName, serialized)
	}
}

// Consumer
type JointDataConsumer struct {
	abstractConsumer
	jointDataExchanger
}

func CreateJointDataConsumer(queueName string) (*JointDataConsumer, error) {
	// Instantiate the data consumer.
	consumer := &JointDataConsumer{}
	// Connect and declare the exchange.
	if err := consumer.initialize(); err != nil {
		return nil, err
	} else {
		consumer.queueName = queueName
		consumer.joinQueue(queueName)
		consumer.bindQueue(queueName, consumer.exchangeName)
		return consumer, nil
	}
}

// Blocks waiting for incoming player records. The given callback will be called for each record.
func (consumer *JointDataConsumer) Consume(callback func(*JointMatchRecordBatch)) error {
	consumer.consumeFromQueue(consumer, consumer.queueName, func(message []byte) {
		// Deserialize the batch.
		if batch, err := DeserializeJointMatchRecords(message); err == nil {
			// The batch was deserialized correctly. Have it handled by the callback.
			callback(batch)
		} else {
			log.Println("could not deserialize batch")
		}
	})
	return nil
}

//=================================================================================================
// Aggregation
//-------------------------------------------------------------------------------------------------
const AggregationExchangeVarName string = "AggregationExchange"
const AggregationExchangeDefault string = "aggregation_exchange"

const LongMatchQueueNameVarName string = "LongMatchQueue"
const LongMatchQueueNameDefault string = "long_matches"

const LargeRatingDifferenceMatchQueueVarName string = "LargeRatingDifferenceMatchQueue"
const LargeRatingDifferenceMatchQueueDefault string = "large_rating_difference_matches"

const Top5CivilizationsQueueVarName string = "Top5CivilizationQueue"
const Top5CivilizationsQueueDefault string = "top5_civilizations"

const CivilizationVictoryDataQueueVarName string = "CivilizationVictoryDataQueue"
const CivilizationVictoryDataQueueDefault string = "civilization_victory_data"

const CivilizationUsageAggregationQueueVarName string = "CivilizationUsageAggregationQueue"
const CivilizationUsageAggregationQueueDefault string = "civilization_usage_aggregation"

const CivilizationVictoryRateQueueVarName string = "CivilizationVictoryRateQueue"
const CivilizationVictoryRateQueueDefault string = "civilization_victory_rate_queue"

type aggregationDataExchanger struct {
	Connector
	exchangeName string
}

func (exchanger *aggregationDataExchanger) initialize() error {
	// Get the name of the joint data exchange from config.
	exchanger.initializeBaseParams()
	exchangeName := config.GetStringOrDefault(AggregationExchangeVarName, AggregationExchangeDefault)
	exchanger.exchangeName = exchangeName
	// Connect to the exchange.
	return exchanger.connectToDirectExchange(exchangeName)
}

type AggregationDataPublisher struct {
	aggregationDataExchanger
}

func CreateAggregationDataPublisher() (*AggregationDataPublisher, error) {
	// Instantiate the data publisher.
	publisher := &AggregationDataPublisher{}
	// Connect to the message middleware.
	if err := publisher.initialize(); err != nil {
		return nil, err
	} else {
		return publisher, nil
	}
}

func (publisher *AggregationDataPublisher) PublishLongMatch(record *SingleTokenRecord) error {
	// Get the name of the queue.
	qname := config.GetStringOrDefault(LongMatchQueueNameVarName, LongMatchQueueNameDefault)
	return publisher.publishThroughQueue(record, qname)
}

func (publisher *AggregationDataPublisher) PublishLargeRatingDifferenceMatch(record *SingleTokenRecord) error {
	// Get the name of the queue.
	qname := config.GetStringOrDefault(LargeRatingDifferenceMatchQueueVarName, LargeRatingDifferenceMatchQueueDefault)
	return publisher.publishThroughQueue(record, qname)
}

func (publisher *AggregationDataPublisher) PublishCivilizationUsageRecord(batch *CivilizationInfoRecordBatch) error {
	qname := config.GetStringOrDefault(
		Top5CivilizationsQueueVarName,
		Top5CivilizationsQueueDefault)
	return publisher.publishThroughQueue(batch, qname)
}

func (publisher *AggregationDataPublisher) PublishCivilizationPerformanceData(batch *CivilizationInfoRecordBatch) error {
	qname := config.GetStringOrDefault(
		CivilizationVictoryDataQueueVarName,
		CivilizationVictoryDataQueueDefault)
	return publisher.publishThroughQueue(batch, qname)
}

func (publisher *AggregationDataPublisher) PublishCivilizationUsageAggregation(data *CivilizationCounterRecord) error {
	qname := config.GetStringOrDefault(
		CivilizationUsageAggregationQueueVarName,
		CivilizationUsageAggregationQueueDefault)
	return publisher.publishThroughQueue(data, qname)
}

func (publisher *AggregationDataPublisher) PublishCivilizationVictoryRates(data *CivilizationFloatRecord) error {
	qname := config.GetStringOrDefault(
		CivilizationVictoryRateQueueVarName,
		CivilizationVictoryRateQueueDefault)
	return publisher.publishThroughQueue(data, qname)
}

func (publisher *AggregationDataPublisher) publishThroughQueue(serializable Serializable, qname string) error {
	if serialized, err := serializable.Serialize(); err != nil {
		return err
	} else {
		return publisher.publishWithRoutingKey(
			publisher.exchangeName, serialized, qname)
	}
}

//-------------------------------------------------------------------------------------------------
// Long match data consumer
//-------------------------------------------------------------------------------------------------
type LongMatchDataConsumer struct {
	abstractConsumer
	aggregationDataExchanger
}

func CreateLongMatchDataConsumer() (*LongMatchDataConsumer, error) {
	// Instantiate the data consumer.
	consumer := &LongMatchDataConsumer{}
	// Connect and declare the exchange.
	if err := consumer.initialize(); err != nil {
		return nil, err
	} else {
		// Get the name of the queue from config.
		consumer.queueName = config.GetStringOrDefault(
			LongMatchQueueNameVarName,
			LongMatchQueueNameDefault)
		consumer.joinQueue(consumer.queueName)
		consumer.bindQueueWithRoutingKey(
			consumer.queueName, consumer.exchangeName, consumer.queueName)
		return consumer, nil
	}
}

// Blocks waiting for incoming player records. The given callback will be called for each record.
func (consumer *LongMatchDataConsumer) Consume(callback func(*SingleTokenRecord)) error {
	consumer.consumeFromQueue(consumer, consumer.queueName, func(message []byte) {
		// Deserialize the batch.
		if record, err := DeserializeSingleTokenRecord(message); err == nil {
			// The batch was deserialized correctly. Have it handled by the callback.
			callback(record)
		} else {
			log.Println("could not deserialize record")
		}
	})
	return nil
}

//-------------------------------------------------------------------------------------------------
// Large rating difference consumer.
//-------------------------------------------------------------------------------------------------
type LargeRatingDifferenceMatchDataConsumer struct {
	abstractConsumer
	aggregationDataExchanger
}

func CreateLargeRatingDifferenceMatchDataConsumer() (*LargeRatingDifferenceMatchDataConsumer, error) {
	// Instantiate the data consumer.
	consumer := &LargeRatingDifferenceMatchDataConsumer{}
	// Connect and declare the exchange.
	if err := consumer.initialize(); err != nil {
		return nil, err
	} else {
		// Get the name of the queue from config.
		consumer.queueName = config.GetStringOrDefault(
			LargeRatingDifferenceMatchQueueVarName,
			LargeRatingDifferenceMatchQueueDefault)
		consumer.joinQueue(consumer.queueName)
		consumer.bindQueueWithRoutingKey(
			consumer.queueName, consumer.exchangeName, consumer.queueName)
		return consumer, nil
	}
}

// Blocks waiting for incoming player records. The given callback will be called for each record.
func (consumer *LargeRatingDifferenceMatchDataConsumer) Consume(callback func(*SingleTokenRecord)) error {
	consumer.consumeFromQueue(consumer, consumer.queueName, func(message []byte) {
		// Deserialize the batch.
		if record, err := DeserializeSingleTokenRecord(message); err == nil {
			// The batch was deserialized correctly. Have it handled by the callback.
			callback(record)
		} else {
			log.Println("could not deserialize record")
		}
	})
	return nil
}

//-------------------------------------------------------------------------------------------------
// Civilization info records consumer
//-------------------------------------------------------------------------------------------------
type CivilizationInfoRecordConsumer struct {
	aggregationDataExchanger
	abstractConsumer
}

func CreateIslandsCivilizationUsageDataConsumer() (*CivilizationInfoRecordConsumer, error) {
	queueName := config.GetStringOrDefault(
		Top5CivilizationsQueueVarName,
		Top5CivilizationsQueueDefault)
	return createCivilizationInfoRecordConsumer(queueName)
}

func CreateArenaCivilizationVictoryDataConsumer() (*CivilizationInfoRecordConsumer, error) {
	queueName := config.GetStringOrDefault(
		CivilizationVictoryDataQueueVarName,
		CivilizationVictoryDataQueueDefault)
	return createCivilizationInfoRecordConsumer(queueName)
}

func createCivilizationInfoRecordConsumer(queueName string) (*CivilizationInfoRecordConsumer, error) {
	// Instantiate the data consumer.
	consumer := &CivilizationInfoRecordConsumer{}
	consumer.queueName = queueName
	// Connect and declare the exchange.
	if err := consumer.initialize(); err != nil {
		return nil, err
	} else {
		consumer.joinQueue(consumer.queueName)
		consumer.bindQueueWithRoutingKey(
			consumer.queueName, consumer.exchangeName, consumer.queueName)
		return consumer, nil
	}
}

// Blocks waiting for incoming player records. The given callback will be called for each record.
func (consumer *CivilizationInfoRecordConsumer) Consume(callback func(*CivilizationInfoRecordBatch)) error {
	consumer.consumeFromQueue(consumer, consumer.queueName, func(message []byte) {
		// Deserialize the batch.
		if batch, err := DeserializeCivilizationInfoRecords(message); err == nil {
			// The batch was deserialized correctly. Have it handled by the callback.
			callback(batch)
		} else {
			log.Println("could not deserialize record")
		}
	})
	return nil
}

//-------------------------------------------------------------------------------------------------
// Civilization usage aggregation consumer
//-------------------------------------------------------------------------------------------------
type CivilizationUsageAggregationConsumer struct {
	aggregationDataExchanger
	abstractConsumer
}

func CreateCivilizationUsageAggregationConsumer() (*CivilizationUsageAggregationConsumer, error) {
	// Instantiate the data consumer.
	consumer := &CivilizationUsageAggregationConsumer{}
	// Connect and declare the exchange.
	if err := consumer.initialize(); err != nil {
		return nil, err
	} else {
		// Get the name of the queue from config.
		consumer.queueName = config.GetStringOrDefault(
			CivilizationUsageAggregationQueueVarName,
			CivilizationUsageAggregationQueueDefault)
		consumer.joinQueue(consumer.queueName)
		consumer.bindQueueWithRoutingKey(
			consumer.queueName, consumer.exchangeName, consumer.queueName)
		return consumer, nil
	}
}

func (consumer *CivilizationUsageAggregationConsumer) Consume(
	callback func(*CivilizationCounterRecord)) error {

	consumer.consumeFromQueue(consumer, consumer.queueName, func(message []byte) {
		// Deserialize the batch.
		if record, err := DeserializeCivilizationCounterRecord(message); err == nil {
			// The batch was deserialized correctly. Have it handled by the callback.
			callback(record)
		} else {
			log.Println("could not deserialize record")
		}
	})
	return nil
}

//-------------------------------------------------------------------------------------------------
// Civilization victory rate consumer
//-------------------------------------------------------------------------------------------------
type CivilizationVictoryRateConsumer struct {
	aggregationDataExchanger
	abstractConsumer
}

func CreateCivilizationVictoryRateConsumer() (*CivilizationVictoryRateConsumer, error) {
	// Instantiate the data consumer.
	consumer := &CivilizationVictoryRateConsumer{}
	// Connect and declare the exchange.
	if err := consumer.initialize(); err != nil {
		return nil, err
	} else {
		// Get the name of the queue from config.
		consumer.queueName = config.GetStringOrDefault(
			CivilizationVictoryRateQueueVarName,
			CivilizationVictoryRateQueueDefault)
		consumer.joinQueue(consumer.queueName)
		consumer.bindQueueWithRoutingKey(
			consumer.queueName, consumer.exchangeName, consumer.queueName)
		return consumer, nil
	}
}

func (consumer *CivilizationVictoryRateConsumer) Consume(
	callback func(*CivilizationFloatRecord)) error {

	consumer.consumeFromQueue(consumer, consumer.queueName, func(message []byte) {
		// Deserialize the batch.
		if record, err := DeserializeCivilizationFloatRecord(message); err == nil {
			// The batch was deserialized correctly. Have it handled by the callback.
			callback(record)
		} else {
			log.Println("could not deserialize record")
		}
	})
	return nil
}

//=================================================================================================
// Generic consumer
//-------------------------------------------------------------------------------------------------

type abstractConsumer struct {
	queueName string
	waitGroup *sync.WaitGroup
	quit      chan int
}

func (consumer *abstractConsumer) RegisterOnWaitGroup(waitGroup *sync.WaitGroup) {
	consumer.waitGroup = waitGroup
	consumer.waitGroup.Add(1)
}

func (consumer *abstractConsumer) consumeFromQueue(
	connector channelConsumer, queueName string, callback func(message []byte)) error {

	if consumerChannel, err := connector.consumerChannel(queueName); err != nil {
		return err
	} else {
		consumer.consumeFromChannel(consumerChannel, func(message []byte) {
			// Have the message be handled by a callback that knows how to handle it.
			callback(message)
		})
	}
	return nil
}

func (consumer *abstractConsumer) consumeFromChannel(
	consumerChannel <-chan amqp.Delivery, callback func(message []byte)) error {

	// Launch reader.
	log.Printf("consuming messages from queue %s\n", consumer.queueName)

	// Consume from the channel until the stop signal is received.
	stopping := false

	for !stopping {
		select {
		case <-consumer.quit:
			stopping = true
		case data := <-consumerChannel:
			callback(data.Body)
		}

	}

	// Send finalization notification if registered in a wait group.
	if consumer.waitGroup != nil {
		consumer.waitGroup.Done()
	}

	return nil
}

func (consumer *abstractConsumer) Stop() {
	consumer.quit <- 0
}
