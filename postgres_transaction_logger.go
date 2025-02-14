package main

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

// PostgresTransactionLogger is a transaction logger that writes to a PostgreSQL database
type PostgresTransactionLogger struct {
	events chan<- Event // Write-only channel for sending events
	errors <-chan error // Read-only channel for receiving errors
	db     *sql.DB      // The database access inteface
}

// PostgresDBParams holds the parameters for connecting to a PostgreSQL database
type PostgresDBParams struct {
	host     string
	dbName   string
	user     string
	password string
}

func NewPostgresTransactionLogger(config PostgresDBParams) (TransactionLogger, error) {

	connStr := fmt.Sprintf("host=%s dbname=%s user=%s password=%s sslmode=disable", config.host, config.dbName, config.user, config.password)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	err = db.Ping() // Test the db connection
	if err != nil {
		return nil, fmt.Errorf("failed to open db connection: %w", err)
	}

	logger := &PostgresTransactionLogger{db: db}

	exists, err := logger.verfifyTableExists()
	if err != nil {
		return nil, fmt.Errorf("failed to verify table exists: %w", err)
	}
	if !exists {
		err = logger.createTable()
		if err != nil {
			return nil, fmt.Errorf("failed to create table: %w", err)
		}
	}

	return logger, nil
}

func (l *PostgresTransactionLogger) WritePut(key, value string) {
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (l *PostgresTransactionLogger) WriteDelete(key string) {
	l.events <- Event{EventType: EventDelete, Key: key}
}

func (l *PostgresTransactionLogger) Err() <-chan error {
	return l.errors
}

func (l *PostgresTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	outEvent := make(chan Event)
	outError := make(chan error, 1)

	go func() {
		defer close(outEvent)
		defer close(outError)

		query := `SELECT sequence, event_type, key, value FROM transactions ORDER BY sequence`
		rows, err := l.db.Query(query)
		if err != nil {
			outError <- fmt.Errorf("failed to query trasactions: %w", err)
			return
		}

		defer rows.Close() // This is important!

		e := Event{} // Create an Event to hold the values

		for rows.Next() { // Iterate over the rows

			err = rows.Scan(&e.Sequence, &e.EventType, &e.Key, &e.Value) // Read the values from the row into the Event
			if err != nil {
				outError <- fmt.Errorf("error reading row: %w", err)
				return
			}

			outEvent <- e // Send the Event to the outEvent channel
		}

		err = rows.Err() // Check for errors during the iteration
		if err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
		}

	}()

	return outEvent, outError
}

func (l *PostgresTransactionLogger) Run() {
	events := make(chan Event, 16) // Make an events channel
	l.events = events

	errors := make(chan error, 1) // Make an errors channel
	l.errors = errors

	go func() {
		query := `INSERT INTO transactions (event_type, key, value) VALUES ($1, $2, $3) RETURNING sequence`

		for e := range events { // Retrieve the next Event

			_, err := l.db.Exec(query, e.EventType, e.Key, e.Value) // Write the event to the log
			if err != nil {
				errors <- err
			}
		}
	}()
}

func (l *PostgresTransactionLogger) verfifyTableExists() (bool, error) {
	const query = `SELECT EXISTS (
		SELECT FROM information_schema.tables 
		WHERE table_name = 'transactions'
	);`

	var exists bool
	err := l.db.QueryRow(query).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to query for table: %w", err)
	}
	return exists, nil
}

func (l *PostgresTransactionLogger) createTable() error {
	const query = `CREATE TABLE transactions (
		sequence BIGSERIAL PRIMARY KEY,
		type BYTE NOT NULL,
		key TEXT NOT NULL,
		value TEXT
	);`

	_, err := l.db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	return nil
}
