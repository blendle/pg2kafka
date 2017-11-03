package sql_test

import (
	"bytes"
	"database/sql"
	"os"
	"testing"

	"github.com/blendle/pg2kafka/eventqueue"
	"github.com/buger/jsonparser"
	_ "github.com/lib/pq"
)

const selectTriggerNamesQuery = `
SELECT tgname
FROM pg_trigger
WHERE tgisinternal = false
AND tgrelid = 'users'::regclass;
`

func TestSQL_SetupPG2Kafka(t *testing.T) {
	db, _, cleanup := setupTriggers(t)
	defer cleanup()

	triggerName := ""
	err := db.QueryRow(selectTriggerNamesQuery).Scan(&triggerName)
	if err != nil {
		t.Fatalf("Error fetching triggers: %v", err)
	}

	if triggerName != "users_enqueue_event" {
		t.Fatalf("Expected trigger 'users_enqueue_event', got: '%v'", triggerName)
	}
}

func TestSQL_SetupPG2Kafka_Idempotency(t *testing.T) {
	db, _, cleanup := setupTriggers(t)
	defer cleanup()

	_, err := db.Exec(`SELECT pg2kafka.setup('users', 'uuid');`)
	if err != nil {
		t.Fatal(err)
	}

	triggerName := ""
	err = db.QueryRow(selectTriggerNamesQuery).Scan(&triggerName)
	if err != nil {
		t.Fatalf("Error fetching triggers: %v", err)
	}

	if triggerName != "users_enqueue_event" {
		t.Fatalf("Expected trigger 'users_enqueue_event', got: '%v'", triggerName)
	}
}

func TestSQL_Trigger_Insert(t *testing.T) {
	db, eq, cleanup := setupTriggers(t)
	defer cleanup()

	_, err := db.Exec(`INSERT INTO users (name, email) VALUES ('jurre', 'jurre@blendle.com')`)
	if err != nil {
		t.Fatal(err)
	}

	events, err := eq.FetchUnprocessedRecords()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(events))
	}

	if events[0].Statement != "INSERT" {
		t.Errorf("Expected 'INSERT', got %s", events[0].Statement)
	}

	if events[0].TableName != "users" {
		t.Errorf("Expected 'users', got %s", events[0].TableName)
	}

	email, _ := jsonparser.GetString(events[0].Data, "email")
	if email != "jurre@blendle.com" {
		t.Errorf("Expected 'jurre@blendle.com', got %s", email)
	}

	name, _ := jsonparser.GetString(events[0].Data, "name")
	if name != "jurre" {
		t.Errorf("Expected 'jurre', got %s", name)
	}
}

func TestSQL_Trigger_CreateWithNull(t *testing.T) {
	db, eq, cleanup := setupTriggers(t)
	defer cleanup()

	_, err := db.Exec(`INSERT INTO users (name, email) VALUES ('niels', null)`)
	if err != nil {
		t.Fatal(err)
	}

	events, err := eq.FetchUnprocessedRecords()
	if err != nil {
		t.Fatal(err)
	}

	_, valueType, _, _ := jsonparser.Get(events[0].Data, "email")
	if valueType != jsonparser.Null {
		t.Errorf("Expected null, got %v", valueType)
	}
}

func TestSQL_Trigger_UpdateToNull(t *testing.T) {
	db, eq, cleanup := setupTriggers(t)
	defer cleanup()

	_, err := db.Exec(`INSERT INTO users (name, email) VALUES ('jurre', 'jurre@blendle.com')`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`UPDATE users SET email = null WHERE name = 'jurre'`)
	if err != nil {
		t.Fatal(err)
	}

	events, err := eq.FetchUnprocessedRecords()
	if err != nil {
		t.Fatal(err)
	}

	if len(events) != 2 {
		t.Fatalf("Expected 2 events, got %d", len(events))
	}

	_, valueType, _, _ := jsonparser.Get(events[1].Data, "email")
	if valueType != jsonparser.Null {
		t.Errorf("Expected null, got %v", valueType)
	}
}

func TestSQL_Snapshot(t *testing.T) {
	db, eq, cleanup := setupTriggers(t)
	defer cleanup()

	_, err := db.Exec(`
	DROP TABLE IF EXISTS products;
	CREATE TABLE products (
		uid  varchar,
		name varchar
	);
	INSERT INTO products (uid, name) VALUES ('duff-1', 'Duffs Beer');
	INSERT INTO products (uid, name) VALUES ('duff-2', null);
	INSERT INTO products (uid, name) VALUES (null, 'Duff Dry');
	SELECT pg2kafka.setup('products', 'uid')
	`)
	if err != nil {
		t.Fatalf("Error creating products table: %v", err)
	}

	events, err := eq.FetchUnprocessedRecords()
	if err != nil {
		t.Fatal(err)
	}

	if len(events) != 3 {
		t.Fatalf("Expected 3 events, got %d", len(events))
	}

	if !bytes.Equal(events[0].ExternalID, []byte("duff-1")) {
		t.Fatalf("Incorrect external id, expected 'duff-1', got '%v'", events[0].ExternalID)
	}

	_, valueType, _, _ := jsonparser.Get(events[1].Data, "name")
	if valueType != jsonparser.Null {
		t.Errorf("Expected null, got %v", valueType)
	}

	if events[2].ExternalID != nil {
		t.Fatalf("Incorrect external id, expected NULL, got %q", events[2].ExternalID)
	}
}

func setupTriggers(t *testing.T) (*sql.DB, *eventqueue.Queue, func()) {
	t.Helper()
	db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	eq := eventqueue.NewWithDB(db)

	err = eq.ConfigureOutboundEventQueueAndTriggers("./")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`
	DROP TABLE IF EXISTS users cascade;
	CREATE TABLE users (
		uuid  uuid NOT NULL DEFAULT uuid_generate_v4(),
		name  varchar,
		email text
	);
	SELECT pg2kafka.setup('users', 'uuid');
	`)
	if err != nil {
		t.Fatalf("Error creating users table: %v", err)
	}

	return db, eq, func() {
		_, err := db.Exec("DROP SCHEMA pg2kafka CASCADE")
		if err != nil {
			t.Fatalf("failed to drop pg2kafka schema: %v", err)
		}

		if cerr := eq.Close(); cerr != nil {
			t.Fatalf("failed to close eventqueue %v", err)
		}
	}
}
