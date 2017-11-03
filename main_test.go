package main

import (
	"database/sql"
	"os"
	"testing"

	"github.com/blendle/go-streamprocessor/streamclient/inmem"
	"github.com/buger/jsonparser"

	"github.com/blendle/pg2kafka/eventqueue"
	_ "github.com/lib/pq"
)

func TestFetchUnprocessedRecords(t *testing.T) {
	db, eq, cleanup := setup(t)
	defer cleanup()

	// TODO: Use actual trigger to generate this?
	events := []*eventqueue.Event{
		{
			ExternalID: "fefc72b4-d8df-4039-9fb9-bfcb18066a2b",
			TableName:  "users",
			Statement:  "UPDATE",
			Data:       []byte(`{ "email": "j@blendle.com" }`),
			Processed:  true,
		},
		{
			ExternalID: "fefc72b4-d8df-4039-9fb9-bfcb18066a2b",
			TableName:  "users",
			Statement:  "UPDATE",
			Data:       []byte(`{ "email": "jurre@blendle.com" }`),
		},
		{
			ExternalID: "fefc72b4-d8df-4039-9fb9-bfcb18066a2b",
			TableName:  "users",
			Statement:  "UPDATE",
			Data:       []byte(`{ "email": "jurres@blendle.com" }`),
		},
	}
	if err := insert(db, events); err != nil {
		t.Fatalf("Error inserting events: %v", err)
	}

	opts := func(c *inmem.Client) {
		c.ProducerTopic = "users"
	}
	s := inmem.NewStore()
	pt := s.NewTopic("users")
	c := inmem.NewClientWithStore(s, opts)
	p := c.NewProducer()

	ProcessEvents(p, eq)

	expected := 2
	actual := len(pt.Messages())
	if actual != expected {
		t.Fatalf("Unexpected number of messages produced. Expected %d, got %d", expected, actual)
	}

	msg := pt.Messages()[0]
	email, err := jsonparser.GetString(msg, "data", "email")
	if err != nil {
		t.Fatal(err)
	}

	if email != "jurre@blendle.com" {
		t.Errorf("Data did not match. Expected %v, got %v", "jurre@blendle.com", email)
	}
}

// Helpers

func setup(t *testing.T) (*sql.DB, *eventqueue.Queue, func()) {
	t.Helper()
	topicNamespace = "users"
	db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	eq := eventqueue.NewWithDB(db)
	if err := eq.ConfigureOutboundEventQueueAndTriggers("./sql"); err != nil {
		t.Fatal(err)
	}

	return db, eq, func() {
		_, err := db.Exec("DELETE FROM pg2kafka.outbound_event_queue")
		if err != nil {
			t.Fatalf("failed to clear table: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("Error closing db: %v", err)
		}
	}
}

func insert(db *sql.DB, events []*eventqueue.Event) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	statement, err := tx.Prepare(`
		INSERT INTO pg2kafka.outbound_event_queue (external_id, table_name, statement, data, processed)
		VALUES ($1, $2, $3, $4, $5)
	`)
	if err != nil {
		if txerr := tx.Rollback(); txerr != nil {
			return txerr
		}
		return err
	}

	for _, e := range events {
		_, serr := statement.Exec(e.ExternalID, e.TableName, e.Statement, e.Data, e.Processed)
		if serr != nil {
			if txerr := tx.Rollback(); err != nil {
				return txerr
			}
			return serr
		}
	}
	return tx.Commit()
}

var parseTopicNamespacetests = []struct {
	in1, in2, out string
}{
	{"", "", ""},
	{"", "world", "world"},
	{"hello", "", "hello."},
	{"hello", "world", "hello.world"},
}

func TestParseTopicNamespace(t *testing.T) {
	for _, tt := range parseTopicNamespacetests {
		t.Run(tt.out, func(t *testing.T) {
			actual := parseTopicNamespace(tt.in1, tt.in2)

			if actual != tt.out {
				t.Errorf("parseTopicNamespace(%q, %q) => %v, want: %v", tt.in1, tt.in2, actual, tt.out)
			}
		})
	}
}
