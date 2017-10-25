package main

import (
	"database/sql"
	"encoding/json"
	"os"
	"time"

	"github.com/blendle/go-logger"
	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient"
	"github.com/blendle/go-streamprocessor/streamclient/kafka"
	"github.com/blendle/go-streamprocessor/streamclient/standardstream"
	"github.com/lib/pq"
	"go.uber.org/zap"
)

const selectUnprocessedEventsQuery = `
	SELECT id, uuid, external_id, table_name, statement, data, created_at
	FROM outbound_event_queue
	WHERE processed = false
	ORDER BY id ASC
	LIMIT 1000
`

const markEventAsProcessedQuery = `
	UPDATE outbound_event_queue
	SET processed = true
	WHERE id = $1 AND processed = false
`

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

	conninfo := os.Getenv("DATABASE_URL")

	db, err := sql.Open("postgres", conninfo)
	if err != nil {
		logger.L.Fatal("Error opening db connection", zap.Error(err))
	}
	defer func() {
		if err := db.Close(); err != nil {
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
	processQueue(producer, db)

	for {
		waitForNotification(listener, producer, db)
	}
}

// ProcessEvents queries the database for unprocessed events and produces them
// to kafka.
func ProcessEvents(p stream.Producer, db *sql.DB) {
	events, err := fetchUnprocessedRecords(db)
	if err != nil {
		logger.L.Error("Error listening to pg", zap.Error(err))
	}

	produceMessages(p, events, db)
}

func processQueue(p stream.Producer, db *sql.DB) {
	count := 0
	err := db.QueryRow("SELECT count(*) AS count FROM outbound_event_queue").Scan(&count)
	if err != nil {
		logger.L.Fatal("Error selecting count", zap.Error(err))
	}

	limit := 1000
	pageCount := (count % limit)

	for i := 0; i <= pageCount; i++ {
		ProcessEvents(p, db)
	}
}

func waitForNotification(l *pq.Listener, p stream.Producer, db *sql.DB) {
	select {
	case <-l.Notify:
		// We actually receive the payload from the notify here, but in order to
		// ensure that we never process events out-of-turn, we query the DB for
		// all unprocessed events.
		ProcessEvents(p, db)
	case <-time.After(90 * time.Second):
		go func() {
			err := l.Ping()
			if err != nil {
				logger.L.Fatal("Error pinging listener", zap.Error(err))
			}
		}()
	}
}

func produceMessages(p stream.Producer, events []*Event, db *sql.DB) {
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

		_, err = db.Exec(markEventAsProcessedQuery, event.ID)
		if err != nil {
			logger.L.Fatal("Error marking record as processed", zap.Error(err))
		}
	}
}

func fetchUnprocessedRecords(db *sql.DB) ([]*Event, error) {
	rows, err := db.Query(selectUnprocessedEventsQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	messages := []*Event{}
	for rows.Next() {
		msg := &Event{}
		err = rows.Scan(
			&msg.ID,
			&msg.UUID,
			&msg.ExternalID,
			&msg.TableName,
			&msg.Statement,
			&msg.Data,
			&msg.CreatedAt,
		)
		if err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}

	return messages, nil
}
