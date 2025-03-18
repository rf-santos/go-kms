package datasink

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

// PostgresOutputSink implements the OutputSink interface for PostgreSQL
type PostgresOutputSink struct {
	uri            string
	tableName      string
	idField        string
	plaintextField string
	updateExisting bool
	db             *sql.DB
	insertStmt     *sql.Stmt
	updateStmt     *sql.Stmt
	debug          bool
}

// NewPostgresOutputSink creates a new PostgreSQL output sink
func NewPostgresOutputSink(uri, tableName, idField, plaintextField string, updateExisting bool) (*PostgresOutputSink, error) {
	if uri == "" {
		return nil, fmt.Errorf("PostgreSQL URI cannot be empty")
	}
	if tableName == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}

	return &PostgresOutputSink{
		uri:            uri,
		tableName:      tableName,
		idField:        idField,
		plaintextField: plaintextField,
		updateExisting: updateExisting,
		debug:          true, // Enable debugging by default
	}, nil
}

// logDebug logs a message if debug is enabled
func (p *PostgresOutputSink) logDebug(format string, args ...interface{}) {
	if p.debug {
		log.Printf("[PostgreSQL Sink] "+format, args...)
	}
}

// Open initializes the connection to PostgreSQL
func (p *PostgresOutputSink) Open() error {
	p.logDebug("Opening PostgreSQL connection to %s, table: %s", p.uri, p.tableName)

	// Connect to PostgreSQL
	db, err := sql.Open("postgres", p.uri)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Minute * 5)

	// Test the connection
	p.logDebug("Testing database connection")
	if err := db.Ping(); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}

	p.db = db

	// Check if table exists, if not create it
	exists, err := p.tableExists()
	if err != nil {
		return fmt.Errorf("error checking if table exists: %w", err)
	}

	if !exists {
		p.logDebug("Table %s does not exist, creating it", p.tableName)
		err = p.createTable()
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	// Prepare statements
	if p.updateExisting {
		// Prepare update statement
		updateQuery := fmt.Sprintf(
			"UPDATE %s SET %s = $2 WHERE %s = $1",
			p.tableName, p.plaintextField, p.idField,
		)
		p.logDebug("Preparing update statement: %s", updateQuery)
		p.updateStmt, err = p.db.Prepare(updateQuery)
		if err != nil {
			p.db.Close()
			return fmt.Errorf("failed to prepare update statement: %w", err)
		}
	}

	// Prepare insert statement
	insertQuery := fmt.Sprintf(
		"INSERT INTO %s (%s, %s) VALUES ($1, $2) ON CONFLICT (%s) DO NOTHING",
		p.tableName, p.idField, p.plaintextField, p.idField,
	)
	p.logDebug("Preparing insert statement: %s", insertQuery)
	p.insertStmt, err = p.db.Prepare(insertQuery)
	if err != nil {
		if p.updateStmt != nil {
			p.updateStmt.Close()
		}
		p.db.Close()
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}

	p.logDebug("PostgreSQL connection established")
	return nil
}

// tableExists checks if the output table exists
func (p *PostgresOutputSink) tableExists() (bool, error) {
	query := `
		SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_name = $1
		)
	`
	var exists bool
	err := p.db.QueryRow(query, p.tableName).Scan(&exists)
	return exists, err
}

// createTable creates the output table with the necessary columns
func (p *PostgresOutputSink) createTable() error {
	// Create a simple table with id and plaintext columns
	query := fmt.Sprintf(`
		CREATE TABLE %s (
			%s VARCHAR(255) PRIMARY KEY,
			%s TEXT NOT NULL
		)
	`, p.tableName, p.idField, p.plaintextField)

	_, err := p.db.Exec(query)
	return err
}

// Write writes a batch of records to PostgreSQL
func (p *PostgresOutputSink) Write(records []OutputRecord) error {
	if p.db == nil {
		return fmt.Errorf("PostgreSQL output sink not opened")
	}

	p.logDebug("Writing %d records to PostgreSQL", len(records))

	// Start a transaction
	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
			p.logDebug("Transaction rolled back due to error")
		}
	}()

	// Insert or update statements with the transaction
	insertStmt := tx.Stmt(p.insertStmt)
	defer insertStmt.Close()

	var updateStmt *sql.Stmt
	if p.updateExisting && p.updateStmt != nil {
		updateStmt = tx.Stmt(p.updateStmt)
		defer updateStmt.Close()
	}

	for _, record := range records {
		// If update is enabled, try to update first
		if p.updateExisting && updateStmt != nil {
			result, err := updateStmt.Exec(record.ID, record.Plaintext)
			if err != nil {
				p.logDebug("Error updating record %s: %v", record.ID, err)
				return fmt.Errorf("failed to update record %s: %w", record.ID, err)
			}

			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("error getting rows affected: %w", err)
			}

			// If record was updated, no need to insert
			if rowsAffected > 0 {
				continue
			}
		}

		// Insert the record
		_, err := insertStmt.Exec(record.ID, record.Plaintext)
		if err != nil {
			// If error is duplicate key, we can ignore it if updating
			if p.updateExisting && strings.Contains(err.Error(), "duplicate key") {
				continue
			}
			p.logDebug("Error inserting record %s: %v", record.ID, err)
			return fmt.Errorf("failed to insert record %s: %w", record.ID, err)
		}
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	p.logDebug("Successfully wrote %d records to PostgreSQL", len(records))
	return nil
}

// Close closes the PostgreSQL connection
func (p *PostgresOutputSink) Close() error {
	p.logDebug("Closing PostgreSQL connection")

	if p.insertStmt != nil {
		p.insertStmt.Close()
	}
	if p.updateStmt != nil {
		p.updateStmt.Close()
	}
	if p.db != nil {
		return p.db.Close()
	}

	return nil
}
