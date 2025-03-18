package datasource

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

// PostgresDataSource implements DataSource interface for PostgreSQL
type PostgresDataSource struct {
	uri           string
	tableName     string
	db            *sql.DB
	rows          *sql.Rows
	idFieldName   string
	encFieldName  string
	filterExp     string
	whereClause   string
	columns       []string
	idFieldIndex  int
	encFieldIndex int
	query         string
	currentOffset int             // Current offset for pagination
	hasMore       bool            // Flag to indicate if more records are available
	totalCount    int             // Total record count for progress tracking
	currentCount  int             // Current count of processed records
	processedIDs  map[string]bool // To track processed IDs and avoid duplicates
	debug         bool            // Debug logging flag
}

// NewPostgresDataSource creates a new PostgreSQL data source
func NewPostgresDataSource(uri string, tableName string) (*PostgresDataSource, error) {
	if uri == "" {
		return nil, errors.New("PostgreSQL URI cannot be empty")
	}
	if tableName == "" {
		return nil, errors.New("table name cannot be empty")
	}

	return &PostgresDataSource{
		uri:           uri,
		tableName:     tableName,
		idFieldName:   "id",             // Default ID field name
		encFieldName:  "encrypted_data", // Default encrypted field name
		idFieldIndex:  -1,
		encFieldIndex: -1,
		hasMore:       true, // Initially assume there are records
		processedIDs:  make(map[string]bool),
		debug:         true, // Enable debug logging by default
	}, nil
}

// SetDebug enables or disables debug logging
func (p *PostgresDataSource) SetDebug(debug bool) {
	p.debug = debug
}

// logDebug logs a message if debug is enabled
func (p *PostgresDataSource) logDebug(format string, args ...interface{}) {
	if p.debug {
		log.Printf("[PostgreSQL] "+format, args...)
	}
}

// SetIDField sets the ID field name
func (p *PostgresDataSource) SetIDField(fieldName string) {
	if fieldName != "" {
		p.idFieldName = fieldName
		p.logDebug("ID field set to: %s", fieldName)
	}
}

// SetEncryptedField sets the encrypted data field name
func (p *PostgresDataSource) SetEncryptedField(fieldName string) {
	if fieldName != "" {
		p.encFieldName = fieldName
		p.logDebug("Encrypted field set to: %s", fieldName)
	}
}

// SetFilter sets a filter condition for PostgreSQL queries
func (p *PostgresDataSource) SetFilter(filter string) error {
	p.filterExp = filter
	p.logDebug("Setting filter: %s", filter)

	// If filter is empty, clear the WHERE clause
	if filter == "" {
		p.whereClause = ""
		return nil
	}

	// For PostgreSQL, we can mostly use the SQL WHERE clause directly
	// But we should sanitize it a bit to prevent SQL injection
	if strings.Contains(filter, ";") {
		return errors.New("filter cannot contain semicolons")
	}

	// Check for LIKE expressions and ensure proper Postgres syntax
	filter = strings.ReplaceAll(filter, "LIKE", "ILIKE") // Case insensitive LIKE in Postgres

	p.whereClause = "WHERE " + filter
	p.logDebug("WHERE clause set to: %s", p.whereClause)
	return nil
}

// GetTotalCount returns the total record count
func (p *PostgresDataSource) GetTotalCount() (int, error) {
	if p.totalCount > 0 {
		return p.totalCount, nil
	}

	if p.db == nil {
		return 0, errors.New("PostgreSQL data source not opened")
	}

	// Build count query
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", p.tableName)
	if p.whereClause != "" {
		countQuery += " " + p.whereClause
	}

	p.logDebug("Executing count query: %s", countQuery)

	// Start timer for query execution
	startTime := time.Now()

	// Execute the query with a timeout
	var count int
	err := p.db.QueryRow(countQuery).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count records: %w", err)
	}

	p.logDebug("Count query completed in %v, found %d records", time.Since(startTime), count)

	p.totalCount = count
	return count, nil
}

// Open establishes a connection to PostgreSQL and prepares the query
func (p *PostgresDataSource) Open() error {
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

	// Get the columns from the table
	p.logDebug("Retrieving table columns")
	columnQuery := fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_name = '%s'", p.tableName)
	rows, err := db.Query(columnQuery)
	if err != nil {
		return fmt.Errorf("failed to retrieve table columns: %w", err)
	}
	defer rows.Close()

	// Get column names
	var columns []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return err
		}
		columns = append(columns, colName)

		// Check if this is our ID or encrypted field
		if strings.EqualFold(colName, p.idFieldName) {
			p.idFieldIndex = len(columns) - 1
			p.logDebug("Found ID field '%s' at index %d", colName, p.idFieldIndex)
		} else if strings.EqualFold(colName, p.encFieldName) {
			p.encFieldIndex = len(columns) - 1
			p.logDebug("Found encrypted field '%s' at index %d", colName, p.encFieldIndex)
		}
	}

	p.logDebug("Found %d columns in table %s", len(columns), p.tableName)

	// Ensure we found the required columns
	if p.idFieldIndex == -1 {
		return fmt.Errorf("ID field '%s' not found in table columns", p.idFieldName)
	}
	if p.encFieldIndex == -1 {
		return fmt.Errorf("encrypted field '%s' not found in table columns", p.encFieldName)
	}

	p.columns = columns

	// Build the base query
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columns, ", "), p.tableName)
	if p.whereClause != "" {
		query += " " + p.whereClause
	}
	p.query = query
	p.logDebug("Base query: %s", p.query)

	// Get total count for progress reporting
	count, err := p.GetTotalCount()
	if err != nil {
		p.logDebug("Warning: Could not get accurate record count: %v", err)
	} else {
		p.logDebug("Total record count: %d", count)
	}

	return nil
}

// Next retrieves the next batch of records from PostgreSQL
func (p *PostgresDataSource) Next(batchSize int) ([]EncryptedRecord, error) {
	if p.db == nil {
		return nil, errors.New("PostgreSQL data source not opened")
	}

	// If we've already determined there are no more records, return empty slice
	if !p.hasMore {
		p.logDebug("No more records available, returning empty batch")
		return []EncryptedRecord{}, nil
	}

	p.logDebug("Fetching next batch of %d records (processed so far: %d, offset: %d)",
		batchSize, p.currentCount, p.currentOffset)

	// Close previous result set if it exists
	if p.rows != nil {
		p.rows.Close()
		p.rows = nil
	}

	// Build query with LIMIT and OFFSET for pagination
	limitedQuery := fmt.Sprintf("%s ORDER BY %s LIMIT %d OFFSET %d",
		p.query, p.idFieldName, batchSize, p.currentOffset)
	p.logDebug("Executing query: %s", limitedQuery)

	// Start timer for query execution
	startTime := time.Now()

	// Execute the query with a timeout context
	rows, err := p.db.Query(limitedQuery)
	if err != nil {
		return nil, fmt.Errorf("PostgreSQL query failed: %w", err)
	}

	p.logDebug("Query executed in %v", time.Since(startTime))

	p.rows = rows

	// Collect results
	records := make([]EncryptedRecord, 0, batchSize)

	// Create a slice to hold the column values for each row
	values := make([]sql.NullString, len(p.columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Process the rows
	rowCount := 0
	for rows.Next() {
		// Scan the row into the values slice
		if err := rows.Scan(scanArgs...); err != nil {
			p.logDebug("Error scanning row: %v", err)
			continue
		}

		// Create a record from the scanned values
		record := EncryptedRecord{
			AdditionalData: make(map[string]interface{}),
		}

		// Extract values using the correct indices
		for i, colName := range p.columns {
			if i == p.idFieldIndex {
				record.ID = values[i].String
			} else if i == p.encFieldIndex {
				record.CiphertextB64 = values[i].String
			} else if values[i].Valid {
				record.AdditionalData[colName] = values[i].String
			}
		}

		// Skip duplicate records (using ID as key)
		if record.ID == "" {
			p.logDebug("Skipping record with empty ID")
			continue
		}

		if p.processedIDs[record.ID] {
			p.logDebug("Skipping already processed record ID: %s", record.ID)
			continue
		}

		// Validate encrypted data field
		if record.CiphertextB64 == "" {
			p.logDebug("Record ID %s has empty encrypted data, skipping", record.ID)
			continue
		}

		p.processedIDs[record.ID] = true
		records = append(records, record)
		rowCount++

		if rowCount%10 == 0 {
			p.logDebug("Processed %d records in current batch", rowCount)
		}
	}

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		p.logDebug("Error iterating rows: %v", err)
		return nil, fmt.Errorf("error iterating PostgreSQL rows: %w", err)
	}

	// Update counters and check if more records are available
	p.currentCount += len(records)
	p.currentOffset += len(records)

	// If we got fewer records than requested, we're done
	if len(records) < batchSize {
		p.hasMore = false
		p.logDebug("Reached end of data (got %d records when requested %d)",
			len(records), batchSize)
	}

	p.logDebug("Returning batch of %d unique records, total processed: %d",
		len(records), p.currentCount)

	return records, nil
}

// Close closes the PostgreSQL connection
func (p *PostgresDataSource) Close() error {
	p.logDebug("Closing PostgreSQL connection, processed %d records in total",
		p.currentCount)
	p.logDebug("Number of unique record IDs processed: %d", len(p.processedIDs))

	if p.rows != nil {
		p.rows.Close()
	}

	if p.db != nil {
		return p.db.Close()
	}

	return nil
}
