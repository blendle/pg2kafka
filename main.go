package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/blendle/go-logger"
	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient"
	"github.com/blendle/go-streamprocessor/streamclient/kafka"
	"github.com/blendle/go-streamprocessor/streamclient/standardstream"

	_ "github.com/lib/pq"
)

// Event represents the queued event in the database
type Event struct {
	ID         int             `json:"-"`
	UUID       string          `json:"uuid"`
	ExternalID string          `json:"external_id"`
	TableName  string          `json:"-"`
	Statement  string          `json:"statement"`
	Data       json.RawMessage `json:"data"`
	CreatedAt  time.Time       `json:"created_at"`
	Processed  bool            `json:"-"`
}

func main() {
	conf := &logger.Config{
		App:         "pg2kafka",
		Tier:        "stream-processor",
		Production:  os.Getenv("ENV") == "production",
		Environment: os.Getenv("ENV"),
	}

	logger.Init(conf)

	db, err := sql.Open("postgres", os.Getenv("DB_URL"))
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			panic(err)
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

	// Process any events left in the queue
	ProcessEvents(producer, db)

	// TODO: pg notify/listen
}

// ProcessEvents queries the database for unprocessed events and produces them
// to kafka.
func ProcessEvents(p stream.Producer, db *sql.DB) {
	events, err := fetchUnprocessedRecords(db)
	if err != nil {
		panic(err)
	}

	produceMessages(p, events)
}

func produceMessages(p stream.Producer, events []*Event) {
	for _, event := range events {
		msg, err := json.Marshal(event)
		if err != nil {
			panic(err)
		}

		p.Messages() <- &stream.Message{
			Value:     msg,
			Key:       []byte(event.ExternalID),
			Timestamp: time.Now().UTC(),
		}

		// TODO: Mark event as processed
	}
}

func fetchUnprocessedRecords(db *sql.DB) ([]*Event, error) {
	rows, err := db.Query(`
		SELECT uuid, external_id, table_name, statement, data, created_at
		FROM outbound_event_queue
		WHERE processed = false
		ORDER BY created_at ASC
	`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	messages := []*Event{}
	for rows.Next() {
		var msg Event
		err = rows.Scan(
			&msg.UUID,
			&msg.ExternalID,
			&msg.TableName,
			&msg.Statement,
			&msg.Data,
			&msg.CreatedAt,
		)
		if err != nil {
			fmt.Println(err)
		}
		messages = append(messages, &msg)
	}

	return messages, nil
}
