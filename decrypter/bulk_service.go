package decrypter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rf-santos/go-kms/logger"
)

// BulkDecryptionService defines the interface for bulk decryption services
type BulkDecryptionService interface {
	DecryptBulk(ciphertexts []string) ([]string, error)
}

// HTTPBulkDecryptionService implements BulkDecryptionService using HTTP
type HTTPBulkDecryptionService struct {
	serviceURL string
	client     *http.Client
}

// BulkDecryptRequest represents a request to the bulk decryption service
type BulkDecryptRequest struct {
	Ciphertexts []string `json:"ciphertexts"`
}

// BulkDecryptResponse represents a response from the bulk decryption service
type BulkDecryptResponse struct {
	Plaintexts []string `json:"plaintexts"`
	Error      string   `json:"error,omitempty"`
}

// NewHTTPBulkDecryptionService creates a new HTTP-based bulk decryption service
func NewHTTPBulkDecryptionService(serviceURL string) *HTTPBulkDecryptionService {
	// Create an optimized HTTP client with connection pooling
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		MaxConnsPerHost:     100,
		IdleConnTimeout:     90 * time.Second,
	}
	
	client := &http.Client{
		Transport: transport,
		Timeout:   120 * time.Second, // Longer timeout for bulk operations
	}
	
	return &HTTPBulkDecryptionService{
		serviceURL: serviceURL,
		client:     client,
	}
}

// DecryptBulk sends a batch of ciphertexts to the bulk decryption service
func (s *HTTPBulkDecryptionService) DecryptBulk(ciphertexts []string) ([]string, error) {
	if len(ciphertexts) == 0 {
		return []string{}, nil
	}
	
	logger.Debug("Sending %d ciphertexts to bulk decryption service", len(ciphertexts))
	
	// Create request payload with preallocated capacity
	reqBody := BulkDecryptRequest{
		Ciphertexts: ciphertexts,
	}
	
	// Preallocate a buffer for JSON marshaling to reduce memory allocations
	// Estimate ~100 bytes per ciphertext plus some overhead
	bufSize := len(ciphertexts)*100 + 256
	buf := bytes.NewBuffer(make([]byte, 0, bufSize))
	
	// Use the json encoder directly on the buffer for better performance
	encoder := json.NewEncoder(buf)
	if err := encoder.Encode(reqBody); err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}
	
	// Create HTTP request with context and timeout
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	
	req, err := http.NewRequestWithContext(ctx, "POST", s.serviceURL, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	
	// Set headers for better performance
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Connection", "keep-alive")
	
	// Send request
	logger.Debug("Sending request to %s", s.serviceURL)
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()
	
	// Check status code first before reading the entire body
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("service returned error status: %d, body: %s", resp.StatusCode, string(body))
	}
	
	// Use a streaming decoder for better memory efficiency
	var response BulkDecryptResponse
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	
	// Check for service-reported error
	if response.Error != "" {
		return nil, fmt.Errorf("service reported error: %s", response.Error)
	}
	
	// Validate response
	if len(response.Plaintexts) != len(ciphertexts) {
		return nil, fmt.Errorf("service returned %d plaintexts, expected %d", len(response.Plaintexts), len(ciphertexts))
	}
	
	logger.Debug("Successfully decrypted %d ciphertexts via bulk service", len(response.Plaintexts))
	return response.Plaintexts, nil
}
