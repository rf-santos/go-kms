package datasink

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
)

// CSVOutputSink implements the OutputSink interface for CSV files
type CSVOutputSink struct {
	filePath string
	file     *os.File
	writer   *csv.Writer
	isOpen   bool
}

// NewCSVOutputSink creates a new CSV output sink
func NewCSVOutputSink(filePath string) *CSVOutputSink {
	return &CSVOutputSink{
		filePath: filePath,
	}
}

// Open creates the CSV file and writes the header
func (c *CSVOutputSink) Open() error {
	// Create the output file
	file, err := os.Create(c.filePath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	c.file = file

	// Create the CSV writer
	c.writer = csv.NewWriter(file)

	// Write the header
	err = c.writer.Write([]string{"ID", "Plaintext"})
	if err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	c.isOpen = true
	log.Printf("[CSV] Output file created: %s", c.filePath)
	return nil
}

// Write writes a batch of records to the CSV file
func (c *CSVOutputSink) Write(records []OutputRecord) error {
	if !c.isOpen {
		return fmt.Errorf("CSV output sink not opened")
	}

	for _, record := range records {
		err := c.writer.Write([]string{record.ID, record.Plaintext})
		if err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	c.writer.Flush()
	if err := c.writer.Error(); err != nil {
		return fmt.Errorf("error flushing CSV writer: %w", err)
	}

	log.Printf("[CSV] Wrote %d records to output file", len(records))
	return nil
}

// Close closes the CSV file
func (c *CSVOutputSink) Close() error {
	if !c.isOpen {
		return nil
	}

	c.writer.Flush()
	err := c.file.Close()
	c.isOpen = false
	log.Printf("[CSV] Output file closed")
	return err
}
