package datasource

import (
	"fmt"

	"github.com/rf-santos/go-kms/config"
	"github.com/rf-santos/go-kms/logger"
)

// EncryptedRecord represents a record with encrypted data
type EncryptedRecord struct {
	ID            string
	CiphertextB64 string
	AdditionalData map[string]interface{}
}

// DataSource is an interface for data sources
type DataSource interface {
	// Open initializes the data source
	Open() error

	// Close releases resources
	Close() error

	// Next retrieves the next batch of records
	Next(batchSize int) ([]EncryptedRecord, error)

	// SetFilter sets a filter expression for the data source
	SetFilter(expression string) error

	// SetIDField sets the ID field name
	SetIDField(fieldName string)

	// SetEncryptedField sets the encrypted data field name
	SetEncryptedField(fieldName string)
}

// DataSourceFactory creates a data source based on configuration
func DataSourceFactory(cfg *config.Config) (DataSource, error) {
	logger.Info("Creating data source of type: %s", cfg.DataSourceType)

	var source DataSource
	var err error

	switch cfg.DataSourceType {
	case "csv":
		logger.Debug("Creating CSV data source with file: %s", cfg.InputFile)
		source, err = NewCSVDataSource(cfg.InputFile)

	case "mongo":
		logger.Debug("Creating MongoDB data source with URI: %s, DB: %s, Collection: %s", 
			cfg.MongoURI, cfg.MongoDatabase, cfg.MongoCollection)
		source, err = NewMongoDataSource(cfg.MongoURI, cfg.MongoDatabase, cfg.MongoCollection)

	case "postgres":
		logger.Debug("Creating PostgreSQL data source with URI: %s, Table: %s", 
			cfg.PostgresURI, cfg.PostgresTable)
		source, err = NewPostgresDataSource(cfg.PostgresURI, cfg.PostgresTable)

	case "gcs":
		// Determine which bucket to use for input
		inputBucket := cfg.GCSInputBucket
		if inputBucket == "" {
			inputBucket = cfg.GCSBucket
		}
		
		logger.Debug("Creating GCS data source with Bucket: %s, Object: %s", 
			inputBucket, cfg.GCSInputObject)
		source, err = NewGCSDataSource(inputBucket, cfg.GCSInputObject, cfg.GCSCredentialsFile)

	default:
		return nil, fmt.Errorf("unsupported data source type: %s", cfg.DataSourceType)
	}

	if err != nil {
		return nil, err
	}

	// Set common fields
	source.SetIDField(cfg.IDFieldName)
	source.SetEncryptedField(cfg.EncryptedFieldName)
	if cfg.FilterExpression != "" {
		if err := source.SetFilter(cfg.FilterExpression); err != nil {
			return nil, fmt.Errorf("invalid filter expression: %w", err)
		}
	}

	return source, nil
}
