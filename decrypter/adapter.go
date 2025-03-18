package decrypter

import (
	"fmt"

	"github.com/rf-santos/go-kms/datasource"
	"github.com/rf-santos/go-kms/logger"
)

// DataSourceAdapter adapts our datasource.DataSource to the decrypter.DataSourceInput interface
type DataSourceAdapter struct {
	source datasource.DataSource
}

// NewDataSourceAdapter creates a new adapter
func NewDataSourceAdapter(source datasource.DataSource) *DataSourceAdapter {
	return &DataSourceAdapter{
		source: source,
	}
}

// logDebug logs a message at debug level
func (a *DataSourceAdapter) logDebug(format string, args ...interface{}) {
	logger.Debug("[Adapter] "+format, args...)
}

// NextBatch implements DataSourceInput interface
func (a *DataSourceAdapter) NextBatch(batchSize int) ([]Record, error) {
	a.logDebug("Requesting batch of %d records from data source", batchSize)

	encRecords, err := a.source.Next(batchSize)
	if err != nil {
		return nil, fmt.Errorf("error fetching records from data source: %w", err)
	}

	a.logDebug("Received %d records from data source", len(encRecords))

	records := make([]Record, len(encRecords))
	for i, encRecord := range encRecords {
		// Validate that we have both ID and ciphertext
		if encRecord.ID == "" {
			a.logDebug("Warning: Record at index %d has empty ID", i)
		}

		if encRecord.CiphertextB64 == "" {
			a.logDebug("Warning: Record with ID '%s' has empty ciphertext", encRecord.ID)
		} else {
			a.logDebug("Record %d: ID='%s', Ciphertext length=%d", i, encRecord.ID, len(encRecord.CiphertextB64))
		}

		records[i] = Record{
			ID:            encRecord.ID,
			CiphertextB64: encRecord.CiphertextB64,
		}
	}

	return records, nil
}
