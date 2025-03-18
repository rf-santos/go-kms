package datasink

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

// GCSOutputSink implements the OutputSink interface for Google Cloud Storage
type GCSOutputSink struct {
	bucketName      string
	objectPath      string
	client          *storage.Client
	writer          *csv.Writer
	buffer          *csvBuffer
	isOpen          bool
	ctx             context.Context
	credentialsFile string
}

// csvBuffer is a wrapper around a buffer that implements io.Writer
type csvBuffer struct {
	data []byte
}

func (b *csvBuffer) Write(p []byte) (n int, err error) {
	b.data = append(b.data, p...)
	return len(p), nil
}

// NewGCSOutputSink creates a new Google Cloud Storage output sink
func NewGCSOutputSink(bucketName, objectPath string, credentialsFile string) (*GCSOutputSink, error) {
	if bucketName == "" || objectPath == "" {
		return nil, fmt.Errorf("bucket name and object path cannot be empty")
	}

	return &GCSOutputSink{
		bucketName:      bucketName,
		objectPath:      objectPath,
		buffer:          &csvBuffer{data: make([]byte, 0)},
		ctx:             context.Background(),
		credentialsFile: credentialsFile,
	}, nil
}

// Open initializes the connection to GCS and prepares the CSV writer
func (g *GCSOutputSink) Open() error {
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

	// Create the CSV writer that writes to our buffer
	g.writer = csv.NewWriter(g.buffer)

	// Write the header
	err = g.writer.Write([]string{"ID", "Plaintext"})
	if err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	g.writer.Flush()

	g.isOpen = true
	log.Printf("[GCS] Output initialized for gs://%s/%s", g.bucketName, g.objectPath)
	return nil
}

// Write writes a batch of records to the buffer
func (g *GCSOutputSink) Write(records []OutputRecord) error {
	if !g.isOpen {
		return fmt.Errorf("GCS output sink not opened")
	}

	for _, record := range records {
		err := g.writer.Write([]string{record.ID, record.Plaintext})
		if err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	g.writer.Flush()
	if err := g.writer.Error(); err != nil {
		return fmt.Errorf("error flushing CSV writer: %w", err)
	}

	log.Printf("[GCS] Buffered %d records for output", len(records))
	return nil
}

// Close flushes the buffer to GCS and closes the connection
func (g *GCSOutputSink) Close() error {
	if !g.isOpen {
		return nil
	}

	g.writer.Flush()

	// Upload the buffer to GCS
	bucket := g.client.Bucket(g.bucketName)
	obj := bucket.Object(g.objectPath)
	w := obj.NewWriter(g.ctx)

	_, err := w.Write(g.buffer.data)
	if err != nil {
		return fmt.Errorf("failed to write to GCS: %w", err)
	}

	// Close the writer to finalize the upload
	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to close GCS writer: %w", err)
	}

	// Close the client
	err = g.client.Close()
	g.isOpen = false
	log.Printf("[GCS] Output file uploaded to gs://%s/%s", g.bucketName, g.objectPath)
	return err
} 