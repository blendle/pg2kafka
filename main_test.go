package main

import (
	"database/sql"
	"os"
	"testing"

	"github.com/buger/jsonparser"
	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/blendle/pg2kafka/eventqueue"
	_ "github.com/lib/pq"
)

func TestFetchUnprocessedRecords(t *testing.T) {
	db, eq, cleanup := setup(t)
	defer cleanup()

	// TODO: Use actual trigger to generate this?
	events := []*eventqueue.Event{
		{
			ExternalID: []byte("fefc72b4-d8df-4039-9fb9-bfcb18066a2b"),
			TableName:  "users",
			Statement:  "UPDATE",
			Data:       []byte(`{ "email": "j@blendle.com" }`),
			Processed:  true,
		},
		{
			ExternalID: []byte("fefc72b4-d8df-4039-9fb9-bfcb18066a2b"),
			TableName:  "users",
			Statement:  "UPDATE",
			Data:       []byte(`{ "email": "jurre@blendle.com" }`),
		},
		{
			ExternalID: []byte("fefc72b4-d8df-4039-9fb9-bfcb18066a2b"),
			TableName:  "users",
			Statement:  "UPDATE",
			Data:       []byte(`{ "email": "jurres@blendle.com" }`),
		},
		{
			ExternalID: nil,
			TableName:  "users",
			Statement:  "CREATE",
			Data:       []byte(`{ "email": "bart@simpsons.com" }`),
		},
		{
			ExternalID: nil,
			TableName:  "users",
			Statement:  "UPDATE",
			Data:       []byte(`{ "email": "bartman@simpsons.com" }`),
		},
	}
	if err := insert(db, events); err != nil {
		t.Fatalf("Error inserting events: %v", err)
	}

	p := &mockProducer{
		messages: make([]*kafka.Message, 0),
	}

	ProcessEvents(p, eq)

	expected := 4
	actual := len(p.messages)
	if actual != expected {
		t.Fatalf("Unexpected number of messages produced. Expected %d, got %d", expected, actual)
	}

	msg := p.messages[0]
	email, err := jsonparser.GetString(msg.Value, "data", "email")
	if err != nil {
		t.Fatal(err)
	}

	if email != "jurre@blendle.com" {
		t.Errorf("Data did not match. Expected %v, got %v", "jurre@blendle.com", email)
	}

	externalID, err := jsonparser.GetString(msg.Value, "external_id")
	if err != nil {
		t.Fatal(err)
	}

	if externalID != "fefc72b4-d8df-4039-9fb9-bfcb18066a2b" {
		t.Errorf("Expected %v, got %v", "fefc72b4-d8df-4039-9fb9-bfcb18066a2b", externalID)
	}

	msg = p.messages[3]
	email, err = jsonparser.GetString(msg.Value, "data", "email")
	if err != nil {
		t.Fatal(err)
	}

	if email != "bartman@simpsons.com" {
		t.Errorf("Data did not match. Expected %v, got %v", "bartman@simpsons.com", email)
	}

	if len(msg.Key) != 0 {
		t.Errorf("Expected empty key, got %v", msg.Key)
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

type mockProducer struct {
	messages []*kafka.Message
}

func (p *mockProducer) Close() {
}
func (p *mockProducer) Flush(timeout int) int {
	return 0
}
func (p *mockProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	p.messages = append(p.messages, msg)
	go func() {
		deliveryChan <- msg
	}()
	return nil
}
