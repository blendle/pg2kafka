package main

import (
	"database/sql"
	"testing"

	streaminmem "github.com/blendle/go-streamprocessor/streamclient/inmem"
	"github.com/buger/jsonparser"

	_ "github.com/lib/pq"
)

const migration = `
	CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
	CREATE SEQUENCE IF NOT EXISTS outbound_event_queue_id;
	CREATE TABLE IF NOT EXISTS outbound_event_queue (
		id 						integer NOT NULL DEFAULT nextval('outbound_event_queue_id'::regclass),
		uuid 					uuid NOT NULL DEFAULT uuid_generate_v4(),
		external_id 	varchar(100) NOT NULL,
		table_name		varchar(100) NOT NULL,
		statement 		varchar(20) NOT NULL,
		data 					jsonb NOT NULL,
		created_at 		timestamp NOT NULL DEFAULT current_timestamp,
		processed 		boolean DEFAULT false
	);
`

func TestFetchUnprocessedRecords(t *testing.T) {
	db, cleanup := setup(t)
	defer cleanup()

	// TODO: Use actual trigger to generate this?
	events := []*Event{
		{ExternalID: "fefc72b4-d8df-4039-9fb9-bfcb18066a2b", TableName: "users", Statement: "UPDATE", Data: []byte(`{ "email": "j@blendle.com" }`), Processed: true},
		{ExternalID: "fefc72b4-d8df-4039-9fb9-bfcb18066a2b", TableName: "users", Statement: "UPDATE", Data: []byte(`{ "email": "jurre@blendle.com" }`)},
		{ExternalID: "fefc72b4-d8df-4039-9fb9-bfcb18066a2b", TableName: "users", Statement: "UPDATE", Data: []byte(`{ "email": "jurres@blendle.com" }`)},
	}
	if err := insert(db, events); err != nil {
		t.Fatalf("Error inserting events: %v", err)
	}

	opts := func(c *streaminmem.Client) {
		c.ProducerTopic = "users"
	}
	s := streaminmem.NewStore()
	pt := s.NewTopic("users")
	c := streaminmem.NewClientWithStore(s, opts)
	p := c.NewProducer()

	ProcessEvents(p, db)

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

func setup(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	db, err := sql.Open("postgres", "postgres://jurrestender:@localhost/pg2kafka_test?sslmode=disable")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	_, err = db.Exec(migration)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	return db, func() {
		_, err := db.Exec("DELETE FROM outbound_event_queue")
		if err != nil {
			t.Fatalf("failed to clear table: %v", err)
		}
		db.Close()
	}
}

func insert(db *sql.DB, events []*Event) error {
	tx, err := db.Begin()
	statement, err := tx.Prepare(`
		INSERT INTO outbound_event_queue (external_id, table_name, statement, data, processed)
		VALUES ($1, $2, $3, $4, $5)
	`)
	if err != nil {
		tx.Rollback()
		return err
	}

	for _, e := range events {
		if _, err := statement.Exec(e.ExternalID, e.TableName, e.Statement, e.Data, e.Processed); err != nil {
			tx.Rollback()
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	tx.Commit()

	return nil
}
