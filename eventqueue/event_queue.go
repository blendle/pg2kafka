package eventqueue

import (
	"database/sql"
	"encoding/json"
	"time"
)

const (
	selectUnprocessedEventsQuery = `
		SELECT id, uuid, external_id, table_name, statement, data, created_at
		FROM outbound_event_queue
		WHERE processed = false
		ORDER BY id ASC
		LIMIT 1000
	`

	markEventAsProcessedQuery = `
		UPDATE outbound_event_queue
		SET processed = true
		WHERE id = $1 AND processed = false
	`

	countUnprocessedEvents = `
		SELECT count(*) AS count
		FROM outbound_event_queue
		WHERE processed IS FALSE
	`
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

type Queue struct {
	db *sql.DB
}

func New(conninfo string) (*Queue, error) {
	db, err := sql.Open("postgres", conninfo)
	if err != nil {
		return nil, err
	}

	return &Queue{db: db}, nil
}

func NewWithDB(db *sql.DB) *Queue {
	return &Queue{db: db}
}

func (eq *Queue) FetchUnprocessedRecords() ([]*Event, error) {
	rows, err := eq.db.Query(selectUnprocessedEventsQuery)
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

func (eq *Queue) UnprocessedEventPagesCount() (int, error) {
	count := 0
	err := eq.db.QueryRow(countUnprocessedEvents).Scan(&count)
	if err != nil {
		return 0, err
	}

	limit := 1000
	return count % limit, nil
}

func (eq *Queue) MarkEventAsProcessed(eventID int) error {
	_, err := eq.db.Exec(markEventAsProcessedQuery, eventID)
	return err
}

func (eq *Queue) Close() error {
	return eq.db.Close()
}
