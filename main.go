package main

import (
	"encoding/json"
	"os"
	"os/signal"
	"time"

	"github.com/blendle/go-logger"
	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient"
	"github.com/blendle/go-streamprocessor/streamclient/kafka"
	"github.com/blendle/go-streamprocessor/streamclient/standardstream"
	"github.com/lib/pq"
	"go.uber.org/zap"

	"github.com/blendle/pg2kafka/eventqueue"
)

func main() {
	conf := &logger.Config{
		App:         "pg2kafka",
		Tier:        "stream-processor",
		Production:  os.Getenv("ENV") == "production",
		Environment: os.Getenv("ENV"),
	}

	logger.Init(conf)

	conninfo := os.Getenv("DATABASE_URL")

	eq, err := eventqueue.New(conninfo)
	if err != nil {
		logger.L.Fatal("Error opening db connection", zap.Error(err))
	}
	defer func() {
		if err := eq.Close(); err != nil {
			logger.L.Fatal("Error closing db connection", zap.Error(err))
		}
	}()

	options := func(sc *standardstream.Client, kc *kafka.Client) {
		sc.Logger = logger.L
		kc.Logger = logger.L
	}

	producer, err := streamclient.NewProducer(options)
	if err != nil {
		logger.L.Fatal("Unable to initialize producer", zap.Error(err))
	}
	defer producer.Close()

	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			logger.L.Error("Error handling postgres notify", zap.Error(err))
		}
	}
	listener := pq.NewListener(conninfo, 10*time.Second, time.Minute, reportProblem)
	if err := listener.Listen("outbound_event_queue"); err != nil {
		logger.L.Error("Error listening to pg", zap.Error(err))
	}
	defer listener.Close()

	// Process any events left in the queue
	processQueue(producer, eq)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

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

func waitForNotification(l *pq.Listener, p stream.Producer, eq *eventqueue.Queue, signals chan os.Signal) {
	for {
		select {
		case <-l.Notify:
			// We actually receive the payload from the notify here, but in order to
			// ensure that we never process events out-of-turn, we query the DB for
			// all unprocessed events.
			ProcessEvents(p, eq)
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
			Key:       []byte(event.ExternalID),
			Timestamp: event.CreatedAt,
		}

		err = eq.MarkEventAsProcessed(event.ID)
		if err != nil {
			logger.L.Fatal("Error marking record as processed", zap.Error(err))
		}
	}
}
