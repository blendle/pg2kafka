package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/blendle/pg2kafka/eventqueue"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
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

	conninfo := os.Getenv("DATABASE_URL")
	topicNamespace = parseTopicNamespace(os.Getenv("TOPIC_NAMESPACE"), parseDatabaseName(conninfo))

	eq, err := eventqueue.New(conninfo)
	if err != nil {
		logrus.Fatalf("Error opening db connection %v", err)

	}
	defer func() {
		if cerr := eq.Close(); cerr != nil {
			logrus.Fatalf("Error closing db connection %v", cerr)
		}
	}()

	if os.Getenv("PERFORM_MIGRATIONS") == "true" {
		if cerr := eq.ConfigureOutboundEventQueueAndTriggers("./sql"); cerr != nil {
			logrus.Fatalf("Error configuring outbound_event_queue and triggers %v", cerr)
		}
	} else {
		logrus.Info("Not performing database migrations due to missing `PERFORM_MIGRATIONS`.")
	}

	producer := setupProducer()
	defer producer.Close()
	defer producer.Flush(1000)

	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			logrus.Errorf("Error handling postgres notify %v", err)
		}
	}
	listener := pq.NewListener(conninfo, 10*time.Second, time.Minute, reportProblem)
	if err := listener.Listen("outbound_event_queue"); err != nil {
		logrus.Errorf("Error listening to pg %v", err)
	}
	defer func() {
		if cerr := listener.Close(); cerr != nil {
			logrus.Errorf("Error closing listener %v", cerr)
		}
	}()

	// Process any events left in the queue
	processQueue(producer, eq)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	logrus.Info("pg2kafka is now listening to notifications")
	waitForNotification(listener, producer, eq, signals)
}

// ProcessEvents queries the database for unprocessed events and produces them
// to kafka.
func ProcessEvents(p Producer, eq *eventqueue.Queue) {
	events, err := eq.FetchUnprocessedRecords()
	if err != nil {
		logrus.Errorf("Error listening to pg %v", err)
	}

	produceMessages(p, events, eq)
}

func processQueue(p Producer, eq *eventqueue.Queue) {
	pageCount, err := eq.UnprocessedEventPagesCount()
	if err != nil {
		logrus.Fatalf("Error selecting count %v", err)
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
					logrus.Fatalf("Error pinging listener %v", err)
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
			logrus.Fatalf("Error parsing event %v", err)
		}

		topic := topicName(event.TableName)
		fmt.Printf("ini topic %s\n", topic)
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
			logrus.Infof("Would produce message %v", message)
		} else {
			err = p.Produce(message, deliveryChan)
			if err != nil {
				logrus.Fatalf("Failed to produce %v", err)
			}
			e := <-deliveryChan

			result := e.(*kafka.Message)
			if result.TopicPartition.Error != nil {
				logrus.Fatalf("Delivery failed %v", result.TopicPartition.Error)
			}
		}
		err = eq.MarkEventAsProcessed(event.ID)
		if err != nil {
			logrus.Fatalf("Error marking record as processed %v", err)
		}
	}
}

func setupProducer() Producer {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		panic("missing KAFKA_BROKER environment")
	}

	username := os.Getenv("KAFKA_USERNAME")
	if username == "" {
		panic("missing KAFKA_USERNAME environment")
	}

	password := os.Getenv("KAFKA_PASSWORD")
	if password == "" {
		panic("missing KAFKA_PASSWORD environment")
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
		"sasl.username":     username,
		"sasl.password":     password,
		"sasl.mechanism":    "PLAIN",
		"security.protocol": "SASL_SSL",
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
		logrus.Fatalf("Error parsing db connection string %v", err)
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
