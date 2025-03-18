package datasource

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// GCSDataSource implements DataSource interface for Google Cloud Storage
type GCSDataSource struct {
	bucketName       string
	objectPath       string
	client           *storage.Client
	reader           *csv.Reader
	currentRow       int
	hasHeaders       bool
	headerMap        map[string]int
	idFieldName      string
	idFieldIndex     int
	encFieldName     string
	encFieldIndex    int
	filterExp        string
	filterParsed     *filterCondition
	filterApplicable bool
	ctx              context.Context
	readCloser       io.ReadCloser
	credentialsFile  string
}

// NewGCSDataSource creates a new Google Cloud Storage data source
func NewGCSDataSource(bucketName, objectPath string, credentialsFile string) (*GCSDataSource, error) {
	if bucketName == "" || objectPath == "" {
		return nil, errors.New("bucket name and object path cannot be empty")
	}

	return &GCSDataSource{
		bucketName:       bucketName,
		objectPath:       objectPath,
		hasHeaders:       true,
		headerMap:        make(map[string]int),
		idFieldName:      "id",             // Default ID field name
		encFieldName:     "encrypted_data", // Default encrypted field name
		filterApplicable: false,
		ctx:              context.Background(),
		credentialsFile:  credentialsFile,
	}, nil
}

// SetIDField sets the ID field/column name
func (g *GCSDataSource) SetIDField(fieldName string) {
	if fieldName != "" {
		g.idFieldName = fieldName
	}
}

// SetEncryptedField sets the encrypted data field/column name
func (g *GCSDataSource) SetEncryptedField(fieldName string) {
	if fieldName != "" {
		g.encFieldName = fieldName
	}
}

// SetFilter sets a filter condition for the GCS data source
func (g *GCSDataSource) SetFilter(filter string) error {
	g.filterExp = filter

	// Only parse if we have a non-empty filter
	if filter == "" {
		g.filterApplicable = false
		return nil
	}

	// Very basic WHERE clause parser
	// Supports patterns like: "column_name = value" or "column_name LIKE %value%"
	re := regexp.MustCompile(`^\s*([a-zA-Z0-9_]+)\s+(=|!=|LIKE|>|<|>=|<=)\s+(.+?)\s*$`)
	matches := re.FindStringSubmatch(filter)

	if len(matches) != 4 {
		return fmt.Errorf("invalid filter expression: %s", filter)
	}

	g.filterParsed = &filterCondition{
		field:    matches[1],
		operator: matches[2],
		value:    strings.Trim(matches[3], "'\""),
	}

	g.filterApplicable = true
	return nil
}

// Open initializes the connection to GCS and prepares the CSV reader
func (g *GCSDataSource) Open() error {
	var err error
	var clientOpts []option.ClientOption

	// Add credentials file if provided
	if g.credentialsFile != "" {
		clientOpts = append(clientOpts, option.WithCredentialsFile(g.credentialsFile))
	}

	// Create GCS client
	if len(clientOpts) > 0 {
		g.client, err = storage.NewClient(g.ctx, clientOpts...)
	} else {
		g.client, err = storage.NewClient(g.ctx)
	}
	
	if err != nil {
		return fmt.Errorf("failed to create GCS client: %w", err)
	}

	// Get the object
	bucket := g.client.Bucket(g.bucketName)
	object := bucket.Object(g.objectPath)

	// Open the object for reading
	g.readCloser, err = object.NewReader(g.ctx)
	if err != nil {
		return fmt.Errorf("failed to open GCS object: %w", err)
	}

	// Create CSV reader
	g.reader = csv.NewReader(g.readCloser)
	g.currentRow = 0

	// Process header if exists
	if g.hasHeaders {
		headers, err := g.reader.Read()
		if err != nil {
			return err
		}

		// Map column names to indices
		for i, header := range headers {
			g.headerMap[header] = i

			// Check if this is our ID or encrypted field
			if header == g.idFieldName {
				g.idFieldIndex = i
			} else if header == g.encFieldName {
				g.encFieldIndex = i
			}
		}

		// Verify we found our required fields
		if _, ok := g.headerMap[g.idFieldName]; !ok {
			return fmt.Errorf("ID field '%s' not found in CSV headers", g.idFieldName)
		}

		if _, ok := g.headerMap[g.encFieldName]; !ok {
			return fmt.Errorf("encrypted field '%s' not found in CSV headers", g.encFieldName)
		}

		// Verify filter field exists if filter is applicable
		if g.filterApplicable {
			if _, ok := g.headerMap[g.filterParsed.field]; !ok {
				return fmt.Errorf("filter field '%s' not found in CSV headers", g.filterParsed.field)
			}
		}
	}

	return nil
}

// applyFilter checks if a row matches the filter condition
func (g *GCSDataSource) applyFilter(row []string) bool {
	// If no filter or filter not applicable, include all rows
	if !g.filterApplicable || g.filterParsed == nil {
		return true
	}

	// Get the index of the filter field
	fieldIdx, ok := g.headerMap[g.filterParsed.field]
	if !ok || fieldIdx >= len(row) {
		return false
	}

	fieldValue := row[fieldIdx]

	switch g.filterParsed.operator {
	case "=":
		return fieldValue == g.filterParsed.value
	case "!=":
		return fieldValue != g.filterParsed.value
	case "LIKE":
		// Replace SQL LIKE wildcards with regex patterns
		pattern := strings.ReplaceAll(g.filterParsed.value, "%", ".*")
		re, err := regexp.Compile("(?i)" + pattern)
		if err != nil {
			return false
		}
		return re.MatchString(fieldValue)
	case ">":
		return fieldValue > g.filterParsed.value
	case "<":
		return fieldValue < g.filterParsed.value
	case ">=":
		return fieldValue >= g.filterParsed.value
	case "<=":
		return fieldValue <= g.filterParsed.value
	default:
		return false
	}
}

// Next reads the next batch of records from the CSV in GCS
func (g *GCSDataSource) Next(batchSize int) ([]EncryptedRecord, error) {
	if g.reader == nil {
		return nil, errors.New("data source not opened")
	}

	records := make([]EncryptedRecord, 0, batchSize)

	// Read up to batchSize records
	for i := 0; i < batchSize; i++ {
		row, err := g.reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Apply filter if applicable
		if !g.applyFilter(row) {
			// Skip this row and continue, but don't count it against our batch size
			i--
			continue
		}

		// Ensure we have enough columns
		if len(row) <= g.idFieldIndex || len(row) <= g.encFieldIndex {
			return nil, fmt.Errorf("row has insufficient columns: expected at least %d, got %d",
				max(g.idFieldIndex, g.encFieldIndex)+1, len(row))
		}

		// Create record using the configured field indices
		record := EncryptedRecord{
			ID:             row[g.idFieldIndex],
			CiphertextB64:  row[g.encFieldIndex],
			AdditionalData: make(map[string]interface{}),
		}

		// Add any additional columns as metadata
		for header, idx := range g.headerMap {
			if idx != g.idFieldIndex && idx != g.encFieldIndex && idx < len(row) {
				record.AdditionalData[header] = row[idx]
			}
		}

		records = append(records, record)
		g.currentRow++
	}

	return records, nil
}

// Close closes the GCS connection and resources
func (g *GCSDataSource) Close() error {
	var err error

	if g.readCloser != nil {
		err = g.readCloser.Close()
	}

	if g.client != nil {
		err2 := g.client.Close()
		if err == nil {
			err = err2
		}
	}

	return err
}

// ListObjects lists objects in a GCS bucket with an optional prefix
func ListObjects(ctx context.Context, bucketName, prefix string) ([]string, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}
	defer client.Close()

	bucket := client.Bucket(bucketName)
	it := bucket.Objects(ctx, &storage.Query{Prefix: prefix})

	var objects []string
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error listing objects: %w", err)
		}
		objects = append(objects, attrs.Name)
	}

	return objects, nil
}

// GetTotalCount returns the total number of records in the CSV file
func (g *GCSDataSource) GetTotalCount() (int, error) {
	if g.client == nil {
		return 0, errors.New("GCS client not initialized")
	}

	// Get the object
	bucket := g.client.Bucket(g.bucketName)
	object := bucket.Object(g.objectPath)

	// Open the object for reading
	reader, err := object.NewReader(g.ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to open GCS object: %w", err)
	}
	defer reader.Close()

	// Create CSV reader
	csvReader := csv.NewReader(reader)
	
	// Skip header if exists
	if g.hasHeaders {
		_, err := csvReader.Read()
		if err != nil {
			return 0, fmt.Errorf("failed to read header: %w", err)
		}
	}

	// Count records
	count := 0
	for {
		_, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("error reading CSV: %w", err)
		}
		count++
	}

	return count, nil
} 