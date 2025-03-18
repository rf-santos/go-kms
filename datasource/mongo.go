package datasource

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDataSource implements DataSource interface for MongoDB
type MongoDataSource struct {
	uri          string
	database     string
	collection   string
	client       *mongo.Client
	coll         *mongo.Collection
	cursor       *mongo.Cursor
	ctx          context.Context
	idFieldName  string
	encFieldName string
	filterExp    string
	filterDoc    bson.M          // Changed from bson.D to bson.M for simpler filter handling
	totalCount   int             // Total document count for progress reporting
	currentCount int             // Current count of processed documents
	debug        bool            // Debug logging flag
	skip         int64           // Number of documents to skip for pagination
	hasMore      bool            // Flag to indicate if more documents are available
	processedIDs map[string]bool // Track processed IDs to avoid duplicates
}

// NewMongoDataSource creates a new MongoDB data source
func NewMongoDataSource(uri string, database string, collection string) (*MongoDataSource, error) {
	if uri == "" {
		return nil, errors.New("MongoDB URI cannot be empty")
	}
	if database == "" {
		return nil, errors.New("MongoDB database name cannot be empty")
	}
	if collection == "" {
		return nil, errors.New("MongoDB collection name cannot be empty")
	}

	return &MongoDataSource{
		uri:          uri,
		database:     database,
		collection:   collection,
		idFieldName:  "_id",            // Default MongoDB ID field
		encFieldName: "encrypted_data", // Default encrypted field name
		ctx:          context.Background(),
		filterDoc:    bson.M{},              // Initialize with empty filter document
		debug:        true,                  // Enable debug logging by default
		hasMore:      true,                  // Initially assume there are documents
		processedIDs: make(map[string]bool), // To track processed IDs
	}, nil
}

// SetDebug enables or disables debug logging
func (m *MongoDataSource) SetDebug(debug bool) {
	m.debug = debug
}

// logDebug logs a message if debug is enabled
func (m *MongoDataSource) logDebug(format string, args ...interface{}) {
	if m.debug {
		log.Printf("[MongoDB] "+format, args...)
	}
}

// SetIDField sets the ID field name
func (m *MongoDataSource) SetIDField(fieldName string) {
	if fieldName != "" {
		m.idFieldName = fieldName
		m.logDebug("ID field set to: %s", fieldName)
	}
}

// SetEncryptedField sets the encrypted data field name
func (m *MongoDataSource) SetEncryptedField(fieldName string) {
	if fieldName != "" {
		m.encFieldName = fieldName
		m.logDebug("Encrypted field set to: %s", fieldName)
	}
}

// SetFilter sets a filter condition for MongoDB queries
func (m *MongoDataSource) SetFilter(filter string) error {
	m.filterExp = filter
	m.logDebug("Setting filter: %s", filter)

	// Parse the filter expression into a MongoDB query
	if filter == "" {
		m.filterDoc = bson.M{} // Empty filter
		return nil
	}

	// Very basic WHERE clause parser for MongoDB
	// Supports patterns like: "field = value" or "field LIKE %value%"
	parts := strings.Fields(filter)
	if len(parts) < 3 {
		return fmt.Errorf("invalid filter expression: %s", filter)
	}

	field := parts[0]
	operator := parts[1]
	value := strings.Trim(strings.Join(parts[2:], " "), "'\"")

	// Convert string values to appropriate types
	var typedValue interface{}
	switch strings.ToLower(value) {
	case "true":
		typedValue = true
	case "false":
		typedValue = false
	default:
		// Try to convert to number if possible
		if num, err := strconv.ParseFloat(value, 64); err == nil {
			typedValue = num
		} else {
			typedValue = value
		}
	}

	switch operator {
	case "=":
		m.filterDoc = bson.M{field: typedValue}
	case "!=":
		m.filterDoc = bson.M{field: bson.M{"$ne": typedValue}}
	case ">":
		m.filterDoc = bson.M{field: bson.M{"$gt": typedValue}}
	case "<":
		m.filterDoc = bson.M{field: bson.M{"$lt": typedValue}}
	case ">=":
		m.filterDoc = bson.M{field: bson.M{"$gte": typedValue}}
	case "<=":
		m.filterDoc = bson.M{field: bson.M{"$lte": typedValue}}
	case "LIKE":
		// Convert SQL LIKE pattern to MongoDB regex
		pattern := strings.ReplaceAll(value, "%", ".*")
		m.filterDoc = bson.M{field: bson.M{"$regex": pattern, "$options": "i"}}
	default:
		return fmt.Errorf("unsupported operator in filter expression: %s", operator)
	}

	m.logDebug("Filter parsed as: %v", m.filterDoc)
	return nil
}

// GetTotalCount returns the total estimated document count
func (m *MongoDataSource) GetTotalCount() (int, error) {
	if m.totalCount > 0 {
		return m.totalCount, nil
	}

	if m.coll == nil {
		return 0, errors.New("MongoDB data source not opened")
	}

	// Count documents with the current filter
	ctx, cancel := context.WithTimeout(m.ctx, 30*time.Second)
	defer cancel()

	count, err := m.coll.CountDocuments(ctx, m.filterDoc)
	if err != nil {
		return 0, fmt.Errorf("failed to count documents: %w", err)
	}

	m.totalCount = int(count)
	m.logDebug("Total document count: %d", m.totalCount)
	return m.totalCount, nil
}

// Open establishes a connection to MongoDB
func (m *MongoDataSource) Open() error {
	m.logDebug("Opening MongoDB connection to %s, database: %s, collection: %s",
		m.uri, m.database, m.collection)

	// Set timeout for connection
	ctx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
	defer cancel()

	// Connect to MongoDB
	clientOptions := options.Client().ApplyURI(m.uri)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Check the connection
	err = client.Ping(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	m.client = client
	m.coll = client.Database(m.database).Collection(m.collection)

	// Verify that required fields exist
	m.logDebug("Verifying field existence: %s", m.encFieldName)
	distinctOptions := options.Distinct().SetMaxTime(5 * time.Second)
	values, err := m.coll.Distinct(ctx, m.encFieldName, bson.M{}, distinctOptions)
	if err != nil {
		return fmt.Errorf("encrypted field '%s' validation failed: %w", m.encFieldName, err)
	}
	m.logDebug("Found %d distinct values for field %s", len(values), m.encFieldName)

	// Get total count for progress tracking
	count, err := m.GetTotalCount()
	if err != nil {
		m.logDebug("Warning: Could not get accurate document count: %v", err)
		// Continue anyway, we'll just have an inaccurate progress bar
	} else {
		m.logDebug("Initial document count: %d", count)
	}

	return nil
}

// Next retrieves the next batch of records from MongoDB
func (m *MongoDataSource) Next(batchSize int) ([]EncryptedRecord, error) {
	if m.client == nil || m.coll == nil {
		return nil, errors.New("MongoDB data source not opened")
	}

	// If we've already determined there are no more records, return empty slice
	if !m.hasMore {
		m.logDebug("No more records available, returning empty batch")
		return []EncryptedRecord{}, nil
	}

	m.logDebug("Fetching next batch of %d records (processed so far: %d, skip: %d)",
		batchSize, m.currentCount, m.skip)

	// Set up options for batch retrieval with pagination
	findOptions := options.Find().
		SetBatchSize(int32(batchSize)).
		SetLimit(int64(batchSize)).
		SetSkip(m.skip)

	// Set up separate context with timeout for the query
	ctx, cancel := context.WithTimeout(m.ctx, 30*time.Second)
	defer cancel()

	// Close previous cursor if it exists
	if m.cursor != nil {
		m.cursor.Close(ctx)
		m.cursor = nil
	}

	// Execute the query
	m.logDebug("Executing MongoDB query with filter: %v", m.filterDoc)
	cursor, err := m.coll.Find(ctx, m.filterDoc, findOptions)
	if err != nil {
		return nil, fmt.Errorf("MongoDB query failed: %w", err)
	}

	m.cursor = cursor

	// Collect results
	records := make([]EncryptedRecord, 0, batchSize)
	recordCount := 0

	// Process documents in the batch
	for m.cursor.Next(ctx) {
		var doc bson.M
		if err := m.cursor.Decode(&doc); err != nil {
			m.logDebug("Error decoding document: %v", err)
			continue
		}

		record := m.documentToRecord(doc)

		// Skip duplicate records (using ID as key)
		if m.processedIDs[record.ID] {
			m.logDebug("Skipping already processed document ID: %s", record.ID)
			continue
		}

		m.processedIDs[record.ID] = true
		records = append(records, record)
		recordCount++

		if recordCount%10 == 0 && recordCount > 0 {
			m.logDebug("Processed %d documents in current batch", recordCount)
		}
	}

	// Check for cursor errors
	if err := m.cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	// Update counters and check if more records are available
	m.currentCount += len(records)
	m.skip += int64(len(records))

	// If we got fewer records than requested, we're done
	if len(records) < batchSize {
		m.hasMore = false
		m.logDebug("Reached end of collection (got %d records when requested %d)",
			len(records), batchSize)
	}

	m.logDebug("Returning batch of %d unique records, total processed: %d",
		len(records), m.currentCount)

	return records, nil
}

// documentToRecord converts a MongoDB document to an EncryptedRecord
func (m *MongoDataSource) documentToRecord(doc bson.M) EncryptedRecord {
	record := EncryptedRecord{
		AdditionalData: make(map[string]interface{}),
	}

	// Extract fields from document
	for key, value := range doc {
		switch key {
		case m.idFieldName:
			// Convert ID to string (handles both string IDs and ObjectIDs)
			switch v := value.(type) {
			case primitive.ObjectID:
				record.ID = v.Hex()
			default:
				record.ID = fmt.Sprintf("%v", value)
			}
		case m.encFieldName:
			if strVal, ok := value.(string); ok {
				record.CiphertextB64 = strVal
			} else {
				// If not string, convert to string
				record.CiphertextB64 = fmt.Sprintf("%v", value)
			}
		default:
			record.AdditionalData[key] = value
		}
	}

	return record
}

// Close closes the MongoDB connection
func (m *MongoDataSource) Close() error {
	m.logDebug("Closing MongoDB connection, processed %d documents in total",
		m.currentCount)
	m.logDebug("Number of unique document IDs processed: %d", len(m.processedIDs))

	if m.cursor != nil {
		m.cursor.Close(m.ctx)
	}

	if m.client != nil {
		return m.client.Disconnect(m.ctx)
	}

	return nil
}
