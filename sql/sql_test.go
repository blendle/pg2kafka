package sql_test

import (
	"database/sql"
	"io/ioutil"
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
	db, cleanup := setupTriggers(t)
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

func TestSQL_TriggerIsFired(t *testing.T) {
	db, cleanup := setupTriggers(t)
	defer cleanup()

	_, err := db.Exec(`INSERT INTO users (name, email) VALUES ('jurre', 'jurre@blendle.com')`)
	if err != nil {
		t.Fatal(err)
	}

	eq := eventqueue.NewWithDB(db)

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

func setupTriggers(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	migration, err := ioutil.ReadFile("./migrations.sql")
	if err != nil {
		t.Fatalf("Error reading migration: %v", err)
	}

	_, err = db.Exec(string(migration))
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	functions, err := ioutil.ReadFile("./triggers.sql")
	if err != nil {
		t.Fatalf("Error loading functions: %v", err)
	}

	_, err = db.Exec(string(functions))
	if err != nil {
		t.Fatalf("Error creating triggers")
	}

	_, err = db.Exec(`
	DROP TABLE IF EXISTS users;
	CREATE TABLE users (
		uuid  uuid NOT NULL DEFAULT uuid_generate_v4(),
		name  varchar,
		email text
	);
	SELECT setup_pg2kafka('users');
	`)
	if err != nil {
		t.Fatalf("Error creating users table: %v", err)
	}

	return db, func() {
		_, err := db.Exec("DELETE FROM outbound_event_queue")
		if err != nil {
			t.Fatalf("failed to clear table: %v", err)
		}
		db.Close()
	}
}
