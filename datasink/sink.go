package datasink

import (
	"fmt"

	"github.com/rf-santos/go-kms/config"
	"github.com/rf-santos/go-kms/logger"
)

// OutputRecord represents a decrypted record
type OutputRecord struct {
	ID        string
	Plaintext string
}

// OutputSink is an interface for output destinations
type OutputSink interface {
	// Open initializes the output sink
	Open() error

	// Write writes records to the output sink
	Write(records []OutputRecord) error

	// Close releases resources
	Close() error
}

// OutputSinkFactory creates an output sink based on configuration
func OutputSinkFactory(cfg *config.Config) (OutputSink, error) {
	logger.Info("Creating output sink of type: %s", cfg.OutputSinkType)

	switch cfg.OutputSinkType {
	case "csv":
		logger.Debug("Creating CSV output sink with file: %s", cfg.OutputFile)
		return NewCSVOutputSink(cfg.OutputFile), nil

	case "mongo":
		logger.Debug("Creating MongoDB output sink with URI: %s, DB: %s, Collection: %s",
			cfg.OutputMongoURI, cfg.OutputMongoDatabase, cfg.OutputMongoCollection)
		sink, err := NewMongoOutputSink(
			cfg.OutputMongoURI,
			cfg.OutputMongoDatabase,
			cfg.OutputMongoCollection,
			cfg.IDFieldName,
			cfg.PlaintextFieldName,
			cfg.UpdateExistingRecords,
		)
		if err != nil {
			return nil, err
		}
		return sink, nil

	case "postgres":
		logger.Debug("Creating PostgreSQL output sink with URI: %s, Table: %s",
			cfg.OutputPostgresURI, cfg.OutputPostgresTable)
		sink, err := NewPostgresOutputSink(
			cfg.OutputPostgresURI,
			cfg.OutputPostgresTable,
			cfg.IDFieldName,
			cfg.PlaintextFieldName,
			cfg.UpdateExistingRecords,
		)
		if err != nil {
			return nil, err
		}
		return sink, nil

	case "gcs":
		// Determine which bucket to use for output
		outputBucket := cfg.GCSOutputBucket
		if outputBucket == "" {
			outputBucket = cfg.GCSBucket
		}
		
		logger.Debug("Creating GCS output sink with Bucket: %s, Object: %s",
			outputBucket, cfg.GCSOutputObject)
		sink, err := NewGCSOutputSink(
			outputBucket,
			cfg.GCSOutputObject,
			cfg.GCSCredentialsFile,
		)
		if err != nil {
			return nil, err
		}
		return sink, nil

	default:
		return nil, fmt.Errorf("unsupported output sink type: %s", cfg.OutputSinkType)
	}
}
