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
	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient"
	"github.com/blendle/go-streamprocessor/streamclient/kafka"
	"github.com/blendle/go-streamprocessor/streamclient/standardstream"
	"github.com/blendle/pg2kafka/eventqueue"
	"github.com/lib/pq"
	"go.uber.org/zap"
)

var (
	databaseName string
	version      string
)

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
	databaseName = parseDatabaseName(conninfo)

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
	}

	producer := setupProducer()

	defer func() {
		if cerr := producer.Close(); cerr != nil {
			logger.L.Fatal("Error closing producer", zap.Error(err))
		}
	}()

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
func ProcessEvents(p stream.Producer, eq *eventqueue.Queue) {
	events, err := eq.FetchUnprocessedRecords()
	if err != nil {
		logger.L.Error("Error listening to pg", zap.Error(err))
	}

	produceMessages(p, events, eq)
}

func processQueue(p stream.Producer, eq *eventqueue.Queue) {
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
	p stream.Producer,
	eq *eventqueue.Queue,
	signals chan os.Signal,
) {
	for {
		select {
		case <-l.Notify:
			// We actually receive the payload from the notify here, but in order to
			// ensure that we never process events out-of-turn, we query the DB for
			// all unprocessed events.
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

func produceMessages(p stream.Producer, events []*eventqueue.Event, eq *eventqueue.Queue) {
	for _, event := range events {
		msg, err := json.Marshal(event)
		if err != nil {
			logger.L.Fatal("Error parsing event", zap.Error(err))
		}

		p.Messages() <- &stream.Message{
			Value:     msg,
			Topic:     topicName(event.TableName),
			Key:       []byte(event.ExternalID),
			Timestamp: event.CreatedAt,
		}

		err = eq.MarkEventAsProcessed(event.ID)
		if err != nil {
			logger.L.Fatal("Error marking record as processed", zap.Error(err))
		}
	}
}

func setupProducer() stream.Producer {
	options := func(sc *standardstream.Client, kc *kafka.Client) {
		sc.Logger = logger.L
		kc.Logger = logger.L
	}

	producer, err := streamclient.NewProducer(options)
	if err != nil {
		logger.L.Fatal("Unable to initialize producer", zap.Error(err))
	}

	return producer
}

func topicName(tableName string) string {
	return fmt.Sprintf("pg2kafka.%v.%v", databaseName, tableName)
}

func parseDatabaseName(conninfo string) string {
	dbURL, err := url.Parse(conninfo)
	if err != nil {
		logger.L.Fatal("Error parsing db connection string", zap.Error(err))
	}
	return strings.TrimPrefix(dbURL.Path, "/")
}
