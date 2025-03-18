package datasink

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoOutputSink implements the OutputSink interface for MongoDB
type MongoOutputSink struct {
	uri            string
	database       string
	collection     string
	idField        string
	plaintextField string
	updateExisting bool
	client         *mongo.Client
	coll           *mongo.Collection
	ctx            context.Context
	debug          bool
}

// NewMongoOutputSink creates a new MongoDB output sink
func NewMongoOutputSink(uri, database, collection, idField, plaintextField string, updateExisting bool) (*MongoOutputSink, error) {
	if uri == "" {
		return nil, fmt.Errorf("MongoDB URI cannot be empty")
	}
	if database == "" {
		return nil, fmt.Errorf("MongoDB database name cannot be empty")
	}
	if collection == "" {
		return nil, fmt.Errorf("MongoDB collection name cannot be empty")
	}

	return &MongoOutputSink{
		uri:            uri,
		database:       database,
		collection:     collection,
		idField:        idField,
		plaintextField: plaintextField,
		updateExisting: updateExisting,
		ctx:            context.Background(),
		debug:          true, // Enable debugging by default
	}, nil
}

// logDebug logs a message if debug is enabled
func (m *MongoOutputSink) logDebug(format string, args ...interface{}) {
	if m.debug {
		log.Printf("[MongoDB Sink] "+format, args...)
	}
}

// createCollection creates the collection if it doesn't exist
func (m *MongoOutputSink) createCollection() error {
	// List collections to check if ours exists
	ctx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
	defer cancel()

	collections, err := m.client.Database(m.database).ListCollectionNames(ctx, bson.M{"name": m.collection})
	if err != nil {
		return fmt.Errorf("failed to list collections: %w", err)
	}

	// If collection doesn't exist, create it
	if len(collections) == 0 {
		m.logDebug("Collection %s does not exist, creating it", m.collection)
		err = m.client.Database(m.database).CreateCollection(ctx, m.collection)
		if err != nil {
			return fmt.Errorf("failed to create collection: %w", err)
		}
		m.logDebug("Collection %s created successfully", m.collection)
	}

	return nil
}

// Open initializes the connection to MongoDB
func (m *MongoOutputSink) Open() error {
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
	
	// Create collection if it doesn't exist
	if err := m.createCollection(); err != nil {
		return err
	}

	m.logDebug("MongoDB connection established")
	return nil
}

// Write writes a batch of records to MongoDB
func (m *MongoOutputSink) Write(records []OutputRecord) error {
	if m.client == nil || m.coll == nil {
		return fmt.Errorf("MongoDB output sink not opened")
	}

	m.logDebug("Writing %d records to MongoDB", len(records))

	// Set timeout for the write operation
	ctx, cancel := context.WithTimeout(m.ctx, 30*time.Second)
	defer cancel()

	// Process each record
	for _, record := range records {
		// Create document to insert/update
		doc := bson.M{
			m.idField:        record.ID,
			m.plaintextField: record.Plaintext,
		}

		if m.updateExisting {
			// Update existing record or insert if not exists (upsert)
			filter := bson.M{m.idField: record.ID}
			update := bson.M{"$set": doc}
			opts := options.Update().SetUpsert(true)

			_, err := m.coll.UpdateOne(ctx, filter, update, opts)
			if err != nil {
				m.logDebug("Error updating record %s: %v", record.ID, err)
				return fmt.Errorf("failed to update/insert record %s: %w", record.ID, err)
			}
		} else {
			// Insert new record
			_, err := m.coll.InsertOne(ctx, doc)
			if err != nil {
				m.logDebug("Error inserting record %s: %v", record.ID, err)
				return fmt.Errorf("failed to insert record %s: %w", record.ID, err)
			}
		}
	}

	m.logDebug("Successfully wrote %d records to MongoDB", len(records))
	return nil
}

// Close closes the MongoDB connection
func (m *MongoOutputSink) Close() error {
	m.logDebug("Closing MongoDB connection")

	if m.client != nil {
		return m.client.Disconnect(m.ctx)
	}

	return nil
}
