package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"

	logger "github.com/blendle/go-logger"
	"github.com/blendle/pg2kafka/eventqueue"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var ( // nolint: gochecknoglobals
	topicNamespace string
	version        string
)

// Producer is the minimal required interface pg2kafka requires to produce
// events to a kafka topic.
type Producer interface {
	Close()
	Flush(int) int

	Produce(*kafka.Message, chan kafka.Event) error
}

func main() {
	conf := &logger.Config{
		App:         "pg2kafka",
		Tier:        "stream-processor",
		Version:     version,
		Production:  os.Getenv("ENV") == "production",
		Environment: os.Getenv("ENV"),
	}

	logger.Init(conf)

	conninfo := os.Getenv("DATABASE_URL")
	fmt.Println(conninfo)
	topicNamespace = parseTopicNamespace(os.Getenv("TOPIC_NAMESPACE"), parseDatabaseName(conninfo))

	eq, err := eventqueue.New(conninfo)
	if err != nil {
		logger.L.Fatal("Error opening db connection", zap.Error(err))
	}
	defer func() {
		if cerr := eq.Close(); cerr != nil {
			logger.L.Fatal("Error closing db connection", zap.Error(cerr))
		}
	}()

	if os.Getenv("PERFORM_MIGRATIONS") == "true" {
		if cerr := eq.ConfigureOutboundEventQueueAndTriggers("./sql"); cerr != nil {
			logger.L.Fatal("Error configuring outbound_event_queue and triggers", zap.Error(cerr))
		}
	} else {
		logger.L.Info("Not performing database migrations due to missing `PERFORM_MIGRATIONS`.")
	}

	producer := setupProducer()
	defer producer.Close()
	defer producer.Flush(1000)

	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			logger.L.Error("Error handling postgres notify", zap.Error(err))
		}
	}
	listener := pq.NewListener(conninfo, 10*time.Second, time.Minute, reportProblem)
	if err := listener.Listen("outbound_event_queue"); err != nil {
		logger.L.Error("Error listening to pg", zap.Error(err))
	}
	defer func() {
		if cerr := listener.Close(); cerr != nil {
			logger.L.Error("Error closing listener", zap.Error(cerr))
		}
	}()

	// Process any events left in the queue
	processQueue(producer, eq)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	logger.L.Info("pg2kafka is now listening to notifications")
	waitForNotification(listener, producer, eq, signals)
}

// ProcessEvents queries the database for unprocessed events and produces them
// to kafka.
func ProcessEvents(p Producer, eq *eventqueue.Queue) {
	events, err := eq.FetchUnprocessedRecords()
	if err != nil {
		logger.L.Error("Error listening to pg", zap.Error(err))
	}

	produceMessages(p, events, eq)
}

func processQueue(p Producer, eq *eventqueue.Queue) {
	pageCount, err := eq.UnprocessedEventPagesCount()
	if err != nil {
		logger.L.Fatal("Error selecting count", zap.Error(err))
	}

	for i := 0; i <= pageCount; i++ {
		ProcessEvents(p, eq)
	}
}

func waitForNotification(
	l *pq.Listener,
	p Producer,
	eq *eventqueue.Queue,
	signals chan os.Signal,
) {
	for {
		select {
		case <-l.Notify:
			processQueue(p, eq)
		case <-time.After(90 * time.Second):
			go func() {
				err := l.Ping()
				if err != nil {
					logger.L.Fatal("Error pinging listener", zap.Error(err))
				}
			}()
		case <-signals:
			return
		}
	}
}

func produceMessages(p Producer, events []*eventqueue.Event, eq *eventqueue.Queue) {
	deliveryChan := make(chan kafka.Event)
	for _, event := range events {
		msg, err := json.Marshal(event)
		if err != nil {
			logger.L.Fatal("Error parsing event", zap.Error(err))
		}

		topic := topicName(event.TableName)
		message := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny, // nolint: gotype
			},
			Value:     msg,
			Key:       event.ExternalID,
			Timestamp: event.CreatedAt,
		}
		if os.Getenv("DRY_RUN") != "" {
			logger.L.Info("Would produce message", zap.Any("message", message))
		} else {
			err = p.Produce(message, deliveryChan)
			if err != nil {
				logger.L.Fatal("Failed to produce", zap.Error(err))
			}
			e := <-deliveryChan

			result := e.(*kafka.Message)
			if result.TopicPartition.Error != nil {
				logger.L.Fatal("Delivery failed", zap.Error(result.TopicPartition.Error))
			}
		}
		err = eq.MarkEventAsProcessed(event.ID)
		if err != nil {
			logger.L.Fatal("Error marking record as processed", zap.Error(err))
		}
	}
}

func setupProducer() Producer {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		panic("missing KAFKA_BROKER environment")
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = os.Getenv("HOSTNAME")
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"client.id":         hostname,
		"bootstrap.servers": broker,
		"partitioner":       "murmur2",
		"compression.codec": "snappy",
	})
	if err != nil {
		panic(errors.Wrap(err, "failed to setup producer"))
	}

	return p
}

func topicName(tableName string) string {
	return fmt.Sprintf("pg2kafka.%v.%v", topicNamespace, tableName)
}

func parseDatabaseName(conninfo string) string {
	dbURL, err := url.Parse(conninfo)
	if err != nil {
		logger.L.Fatal("Error parsing db connection string", zap.Error(err))
	}
	return strings.TrimPrefix(dbURL.Path, "/")
}

func parseTopicNamespace(topicNamespace string, databaseName string) string {
	s := databaseName
	if topicNamespace != "" {
		s = topicNamespace + "." + s
	}

	return s
}
