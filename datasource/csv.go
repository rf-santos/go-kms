package datasource

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

// CSVDataSource implements DataSource interface for CSV files
type CSVDataSource struct {
	filePath         string
	file             *os.File
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
}

// filterCondition represents a simple filter condition
type filterCondition struct {
	field    string
	operator string
	value    string
}

// NewCSVDataSource creates a new CSV data source
func NewCSVDataSource(filePath string) (*CSVDataSource, error) {
	if filePath == "" {
		return nil, errors.New("file path cannot be empty")
	}

	return &CSVDataSource{
		filePath:         filePath,
		hasHeaders:       true,
		headerMap:        make(map[string]int),
		idFieldName:      "id",             // Default ID field name
		encFieldName:     "encrypted_data", // Default encrypted field name
		filterApplicable: false,
	}, nil
}

// SetIDField sets the ID field/column name
func (c *CSVDataSource) SetIDField(fieldName string) {
	if fieldName != "" {
		c.idFieldName = fieldName
	}
}

// SetEncryptedField sets the encrypted data field/column name
func (c *CSVDataSource) SetEncryptedField(fieldName string) {
	if fieldName != "" {
		c.encFieldName = fieldName
	}
}

// SetFilter sets a filter condition for the CSV data source
func (c *CSVDataSource) SetFilter(filter string) error {
	c.filterExp = filter

	// Only parse if we have a non-empty filter
	if filter == "" {
		c.filterApplicable = false
		return nil
	}

	// Very basic WHERE clause parser
	// Supports patterns like: "column_name = value" or "column_name LIKE %value%"
	re := regexp.MustCompile(`^\s*([a-zA-Z0-9_]+)\s+(=|!=|LIKE|>|<|>=|<=)\s+(.+?)\s*$`)
	matches := re.FindStringSubmatch(filter)

	if len(matches) != 4 {
		return fmt.Errorf("invalid filter expression: %s", filter)
	}

	c.filterParsed = &filterCondition{
		field:    matches[1],
		operator: matches[2],
		value:    strings.Trim(matches[3], "'\""),
	}

	c.filterApplicable = true
	return nil
}

// Open opens the CSV file and initializes the reader
func (c *CSVDataSource) Open() error {
	file, err := os.Open(c.filePath)
	if err != nil {
		return err
	}

	c.file = file
	c.reader = csv.NewReader(file)
	c.currentRow = 0

	// Process header if exists
	if c.hasHeaders {
		headers, err := c.reader.Read()
		if err != nil {
			return err
		}

		// Map column names to indices
		for i, header := range headers {
			c.headerMap[header] = i

			// Check if this is our ID or encrypted field
			if header == c.idFieldName {
				c.idFieldIndex = i
			} else if header == c.encFieldName {
				c.encFieldIndex = i
			}
		}

		// Verify we found our required fields
		if _, ok := c.headerMap[c.idFieldName]; !ok {
			return fmt.Errorf("ID field '%s' not found in CSV headers", c.idFieldName)
		}

		if _, ok := c.headerMap[c.encFieldName]; !ok {
			return fmt.Errorf("encrypted field '%s' not found in CSV headers", c.encFieldName)
		}

		// Verify filter field exists if filter is applicable
		if c.filterApplicable {
			if _, ok := c.headerMap[c.filterParsed.field]; !ok {
				return fmt.Errorf("filter field '%s' not found in CSV headers", c.filterParsed.field)
			}
		}
	}

	return nil
}

// applyFilter checks if a row matches the filter condition
func (c *CSVDataSource) applyFilter(row []string) bool {
	// If no filter or filter not applicable, include all rows
	if !c.filterApplicable || c.filterParsed == nil {
		return true
	}

	// Get the index of the filter field
	fieldIdx, ok := c.headerMap[c.filterParsed.field]
	if !ok || fieldIdx >= len(row) {
		return false
	}

	fieldValue := row[fieldIdx]

	switch c.filterParsed.operator {
	case "=":
		return fieldValue == c.filterParsed.value
	case "!=":
		return fieldValue != c.filterParsed.value
	case "LIKE":
		// Replace SQL LIKE wildcards with regex patterns
		pattern := strings.ReplaceAll(c.filterParsed.value, "%", ".*")
		re, err := regexp.Compile("(?i)" + pattern)
		if err != nil {
			return false
		}
		return re.MatchString(fieldValue)
	case ">":
		return fieldValue > c.filterParsed.value
	case "<":
		return fieldValue < c.filterParsed.value
	case ">=":
		return fieldValue >= c.filterParsed.value
	case "<=":
		return fieldValue <= c.filterParsed.value
	default:
		return false
	}
}

// Next reads the next batch of records from the CSV
func (c *CSVDataSource) Next(batchSize int) ([]EncryptedRecord, error) {
	if c.reader == nil {
		return nil, errors.New("data source not opened")
	}

	records := make([]EncryptedRecord, 0, batchSize)

	// Read up to batchSize records
	for i := 0; i < batchSize; i++ {
		row, err := c.reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Apply filter if applicable
		if !c.applyFilter(row) {
			// Skip this row and continue, but don't count it against our batch size
			i--
			continue
		}

		// Ensure we have enough columns
		if len(row) <= c.idFieldIndex || len(row) <= c.encFieldIndex {
			return nil, fmt.Errorf("row has insufficient columns: expected at least %d, got %d",
				max(c.idFieldIndex, c.encFieldIndex)+1, len(row))
		}

		// Create record using the configured field indices
		record := EncryptedRecord{
			ID:             row[c.idFieldIndex],
			CiphertextB64:  row[c.encFieldIndex],
			AdditionalData: make(map[string]interface{}),
		}

		// Add any additional columns as metadata
		for header, idx := range c.headerMap {
			if idx != c.idFieldIndex && idx != c.encFieldIndex && idx < len(row) {
				record.AdditionalData[header] = row[idx]
			}
		}

		records = append(records, record)
		c.currentRow++
	}

	return records, nil
}

// Close closes the CSV file
func (c *CSVDataSource) Close() error {
	if c.file != nil {
		return c.file.Close()
	}
	return nil
}

// max returns the larger of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
