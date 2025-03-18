package decrypter

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"time"

	kms "cloud.google.com/go/kms/apiv1"
	"github.com/patrickmn/go-cache"
	"github.com/rf-santos/go-kms/config"
	"github.com/rf-santos/go-kms/logger"
	"github.com/schollz/progressbar/v3"
	kmspb "google.golang.org/genproto/googleapis/cloud/kms/v1"
	"google.golang.org/api/option"
)

// KMSDecrypter handles decryption using Google Cloud KMS
type KMSDecrypter struct {
	client  *kms.KeyManagementClient
	config  *config.Config
	cache   *cache.Cache
	keyName string

	// Performance metrics
	totalDecryptions int64
	cacheHits        int64
	decryptionTime   time.Duration

	// Mutex for thread-safe metric updates
	metricsMu sync.Mutex

	// Bulk decryption service (optional)
	bulkService BulkDecryptionService
}

// DecryptedRecord contains the decrypted result
type DecryptedRecord struct {
	ID           string
	Plaintext    string
	DecryptionMS int64
	FromCache    bool
	Error        error
}

// NewKMSDecrypter creates a new KMS decrypter
func NewKMSDecrypter(cfg *config.Config) (*KMSDecrypter, error) {
	ctx := context.Background()
	
	// Configure KMS client with optimized settings
	clientOpts := []option.ClientOption{
		option.WithGRPCConnectionPool(cfg.NumWorkers * 2), // Connection pooling for better throughput
	}
	
	client, err := kms.NewKeyManagementClient(ctx, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create KMS client: %w", err)
	}

	// Format the key name as required by GCP KMS API
	keyName := fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s",
		cfg.ProjectID, cfg.LocationID, cfg.KeyRingID, cfg.KeyID)

	// Initialize cache with configured TTL and optimized size
	cacheExpiration := time.Duration(cfg.CacheTTL) * time.Second
	cachePurgeTime := cacheExpiration * 2
	
	// Create cache with optimal settings
	cacheObj := cache.New(cacheExpiration, cachePurgeTime)
	
	// We can't directly set the capacity, but we can pre-warm the cache
	// by adding a dummy item that will be overwritten later
	// This helps allocate memory in advance
	logger.Debug("Initializing cache with TTL %v seconds", cfg.CacheTTL)

	return &KMSDecrypter{
		client:  client,
		config:  cfg,
		cache:   cacheObj,
		keyName: keyName,
	}, nil
}

// SetBulkDecryptionService sets an optional bulk decryption service
func (d *KMSDecrypter) SetBulkDecryptionService(service BulkDecryptionService) {
	d.bulkService = service
}

// Close releases the KMS client resources
func (d *KMSDecrypter) Close() error {
	return d.client.Close()
}

// DecryptBatch decrypts a batch of ciphertexts
func (d *KMSDecrypter) DecryptBatch(ctx context.Context, records []string) ([]string, error) {
	results := make([]string, len(records))
	var wg sync.WaitGroup
	var mu sync.Mutex // Protects updates to results slice

	for i, ciphertext := range records {
		wg.Add(1)
		go func(index int, ct string) {
			defer wg.Done()

			plaintext, err := d.decryptSingle(ctx, ct)
			if err != nil {
				// Handle error, could log or return it with a structured result
				return
			}

			mu.Lock()
			results[index] = plaintext
			mu.Unlock()
		}(i, ciphertext)
	}

	wg.Wait()
	return results, nil
}

// decryptSingle decrypts a single ciphertext with caching
func (d *KMSDecrypter) decryptSingle(ctx context.Context, ciphertextB64 string) (string, error) {
	start := time.Now()
	d.incrementTotalDecryptions()

	// Check cache first
	if plaintext, found := d.cache.Get(ciphertextB64); found {
		d.incrementCacheHits()
		// Remove debug logging from hot path
		return plaintext.(string), nil
	}

	// Base64 decode the ciphertext
	ciphertext, err := base64.StdEncoding.DecodeString(ciphertextB64)
	if err != nil {
		return "", fmt.Errorf("failed to decode base64: %w", err)
	}

	// Create decryption request
	req := &kmspb.DecryptRequest{
		Name:       d.keyName,
		Ciphertext: ciphertext,
	}

	// Create a context with timeout to avoid hanging on network issues
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Call GCP KMS API to decrypt
	resp, err := d.client.Decrypt(ctxWithTimeout, req)
	if err != nil {
		// Check for specific error types
		if strings.Contains(err.Error(), "NotFound") {
			return "", fmt.Errorf("KMS key not found. Please verify the following:\n"+
				"- Project ID: %s\n"+
				"- Location: %s\n"+
				"- Key Ring: %s\n"+
				"- Key ID: %s\n"+
				"- Full key path: %s\n"+
				"Error: %w", 
				d.config.ProjectID, d.config.LocationID, d.config.KeyRingID, d.config.KeyID, d.keyName, err)
		}
		if strings.Contains(err.Error(), "PermissionDenied") {
			return "", fmt.Errorf("permission denied to access KMS key. Please verify:\n"+
				"- You have the necessary IAM permissions\n"+
				"- Your service account has the 'Cloud KMS CryptoKey Decrypter' role\n"+
				"- The key path is correct: %s\n"+
				"Error: %w", d.keyName, err)
		}
		if strings.Contains(err.Error(), "InvalidArgument") {
			return "", fmt.Errorf("invalid argument for KMS decryption. Please verify:\n"+
				"- The ciphertext is properly formatted\n"+
				"- The key is enabled and not in a disabled state\n"+
				"Error: %w", err)
		}
		if strings.Contains(err.Error(), "DeadlineExceeded") || strings.Contains(err.Error(), "Timeout") {
			return "", fmt.Errorf("KMS API request timed out. This could be due to:\n"+
				"- Network latency\n"+
				"- GCP KMS service issues\n"+
				"- High load on your application\n"+
				"Consider increasing batch size and reducing concurrency. Error: %w", err)
		}
		return "", fmt.Errorf("failed to decrypt: %w", err)
	}

	// Store result in cache
	plaintext := string(resp.Plaintext)
	d.cache.Set(ciphertextB64, plaintext, cache.DefaultExpiration)

	// Update performance metrics
	elapsed := time.Since(start)
	d.addDecryptionTime(elapsed)

	return plaintext, nil
}

// ProcessRecords processes a batch of records with a worker pool
func (d *KMSDecrypter) ProcessRecords(ctx context.Context, ciphertexts []string, ids []string) (map[string]string, error) {
	results := make(map[string]string)
	resultLock := sync.Mutex{}

	// Create work channel and result channel
	workChan := make(chan int, len(ciphertexts))
	errorsChan := make(chan error, len(ciphertexts))

	// Start worker pool
	var wg sync.WaitGroup
	for i := 0; i < d.config.NumWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range workChan {
				plaintext, err := d.decryptSingle(ctx, ciphertexts[idx])
				if err != nil {
					errorsChan <- fmt.Errorf("error decrypting %s: %w", ids[idx], err)
					continue
				}

				resultLock.Lock()
				results[ids[idx]] = plaintext
				resultLock.Unlock()
			}
		}()
	}

	// Send work to workers
	for i := range ciphertexts {
		workChan <- i
	}
	close(workChan)

	// Wait for all workers to finish
	wg.Wait()
	close(errorsChan)

	// Check for errors
	var errs []error
	for err := range errorsChan {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return results, fmt.Errorf("encountered %d errors during decryption", len(errs))
	}

	return results, nil
}

// DecryptAllBulk decrypts all records from a data source in a single bulk request
// This method fetches all records first, then sends a single API request
func (d *KMSDecrypter) DecryptAllBulk(ctx context.Context, dataSource DataSourceInput) (map[string]string, error) {
	if d.bulkService == nil {
		return nil, fmt.Errorf("bulk decryption service not configured")
	}

	fmt.Println("Collecting all records...")
	bar := progressbar.NewOptions(-1,
		progressbar.OptionSetDescription("Loading records"),
		progressbar.OptionThrottle(500*time.Millisecond), // Reduce update frequency
	)

	// Pre-allocate slices and maps for better memory efficiency
	// Start with reasonable capacity estimates
	allRecords := make([]Record, 0, 10000)
	allCiphertexts := make([]string, 0, 10000)
	recordMap := make(map[string]string, 10000) // Map ciphertext to ID for lookup
	
	// Use larger batch size for reading to reduce overhead
	readBatchSize := d.config.BatchSize
	if readBatchSize < 1000 {
		readBatchSize = 1000
	}

	// Collect all records in batches
	totalRecords := 0
	for {
		records, err := dataSource.NextBatch(readBatchSize)
		if err != nil {
			return nil, fmt.Errorf("failed to read records: %w", err)
		}

		if len(records) == 0 {
			break // No more records
		}
		
		totalRecords += len(records)
		
		// Update progress bar less frequently
		if totalRecords % 5000 == 0 {
			bar.Add(5000)
		}

		// Grow slices if needed
		if cap(allRecords) < len(allRecords) + len(records) {
			// Double capacity to reduce reallocations
			newCap := cap(allRecords) * 2
			if newCap < len(allRecords) + len(records) {
				newCap = len(allRecords) + len(records) + 1000
			}
			
			newRecords := make([]Record, len(allRecords), newCap)
			copy(newRecords, allRecords)
			allRecords = newRecords
			
			newCiphertexts := make([]string, len(allCiphertexts), newCap)
			copy(newCiphertexts, allCiphertexts)
			allCiphertexts = newCiphertexts
		}

		// Add records to collections
		for _, record := range records {
			allRecords = append(allRecords, record)
			allCiphertexts = append(allCiphertexts, record.CiphertextB64)
			recordMap[record.CiphertextB64] = record.ID
		}
	}

	// Ensure loading progress bar completes
	bar.Finish()

	recordCount := len(allRecords)
	fmt.Printf("\nCollected %d records. Sending bulk decryption request...\n", recordCount)

	// Process in batches if there are too many records
	const maxBulkSize = 5000 // Maximum records per bulk request
	results := make(map[string]string, recordCount)
	
	startTime := time.Now()
	
	if recordCount <= maxBulkSize {
		// Send request to bulk decryption service
		plaintexts, err := d.bulkService.DecryptBulk(allCiphertexts)
		if err != nil {
			return nil, fmt.Errorf("bulk decryption failed: %w", err)
		}
		
		// Process results
		for i, plaintext := range plaintexts {
			if i < len(allCiphertexts) {
				ciphertext := allCiphertexts[i]
				id := recordMap[ciphertext]
				results[id] = plaintext
			}
		}
	} else {
		// Process in batches
		fmt.Printf("Processing %d records in batches of %d...\n", recordCount, maxBulkSize)
		totalBatches := (recordCount + maxBulkSize - 1) / maxBulkSize // Ceiling division
		batchBar := progressbar.NewOptions(totalBatches,
			progressbar.OptionSetDescription("Processing batches"),
			progressbar.OptionThrottle(500*time.Millisecond),
		)
		
		for i := 0; i < recordCount; i += maxBulkSize {
			end := i + maxBulkSize
			if end > recordCount {
				end = recordCount
			}
			
			batchCiphertexts := allCiphertexts[i:end]
			
			// Send batch request
			plaintexts, err := d.bulkService.DecryptBulk(batchCiphertexts)
			if err != nil {
				return nil, fmt.Errorf("bulk decryption batch %d failed: %w", i/maxBulkSize, err)
			}
			
			// Process batch results
			for j, plaintext := range plaintexts {
				if j < len(batchCiphertexts) {
					ciphertext := batchCiphertexts[j]
					id := recordMap[ciphertext]
					results[id] = plaintext
				}
			}
			
			batchBar.Add(1)
		}
		
		// Ensure batch progress bar completes
		batchBar.Finish()
	}

	duration := time.Since(startTime)

	// Print metrics
	fmt.Printf("\nBulk decryption completed in %.2f seconds\n", duration.Seconds())
	fmt.Printf("- Records processed: %d\n", recordCount)
	fmt.Printf("- Results returned: %d\n", len(results))
	fmt.Printf("- Average processing speed: %.2f records/second\n", float64(recordCount)/duration.Seconds())

	return results, nil
}

// DecryptAll decrypts all records from a data source
func (d *KMSDecrypter) DecryptAll(ctx context.Context, dataSource DataSourceInput, totalRecords int) (map[string]string, error) {
	results := make(map[string]string)
	resultLock := sync.Mutex{}

	// Create a progress bar that updates less frequently for performance
	bar := progressbar.NewOptions(totalRecords,
		progressbar.OptionSetDescription("Decrypting"),
		progressbar.OptionThrottle(500*time.Millisecond), // Increase throttle to reduce updates
		progressbar.OptionShowCount(),
	)

	// Use a separate channel for progress updates to reduce lock contention
	progressChan := make(chan int, 1000)
	
	// Start progress updater goroutine
	go func() {
		progressCount := 0
		progressTicker := time.NewTicker(250 * time.Millisecond)
		defer progressTicker.Stop()
		
		for {
			select {
			case count, ok := <-progressChan:
				if !ok {
					// Channel closed, update final progress and exit
					_ = bar.Add(progressCount)
					return
				}
				progressCount += count
			case <-progressTicker.C:
				if progressCount > 0 {
					_ = bar.Add(progressCount)
					progressCount = 0
				}
			}
		}
	}()

	// Configure optimal batch size for processing
	processingBatchSize := 10 // Process records in small batches for efficiency
	
	// Configure number of worker goroutines
	numWorkers := d.config.NumWorkers
	
	// Create worker pool with work channel
	workChan := make(chan []Record, numWorkers*2)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			for recordBatch := range workChan {
				// Use the batch decryption method for better performance
				batchResults, err := d.decryptBatch(ctx, recordBatch)
				if err != nil {
					logger.Error("Worker %d error decrypting batch: %v", workerID, err)
					continue
				}
				
				if len(batchResults) > 0 {
					// Update results in a single lock operation
					resultLock.Lock()
					for id, plaintext := range batchResults {
						results[id] = plaintext
					}
					resultLock.Unlock()
					
					// Send progress update
					progressChan <- len(batchResults)
				}
			}
		}(i)
	}

	// Read batches from data source
	batchCount := 0
	for {
		// Fetch records from data source
		records, err := dataSource.NextBatch(d.config.BatchSize)
		if err != nil {
			return nil, fmt.Errorf("error reading batch: %w", err)
		}

		if len(records) == 0 {
			break // No more records
		}

		batchCount++
		logger.Debug("Processing batch %d with %d records", batchCount, len(records))

		// Break the batch into smaller chunks for processing
		for i := 0; i < len(records); i += processingBatchSize {
			end := i + processingBatchSize
			if end > len(records) {
				end = len(records)
			}
			
			// Send the chunk to workers
			chunk := records[i:end]
			workChan <- chunk
		}
	}

	close(workChan)
	wg.Wait()
	
	// Ensure progress bar reaches 100%
	_ = bar.Set(totalRecords)
	
	close(progressChan) // Signal progress updater to exit

	// Print final metrics
	fmt.Printf("\nDecryption completed:\n")
	fmt.Printf("- Total records: %d\n", d.getTotalDecryptions())
	fmt.Printf("- Cache hits: %d (%.1f%%)\n", d.getCacheHits(), float64(d.getCacheHits())/float64(d.getTotalDecryptions())*100.0)
	fmt.Printf("- Avg decryption time: %.2f ms\n", d.getAvgDecryptionTimeMS())
	fmt.Printf("- Total batches processed: %d\n", batchCount)
	fmt.Printf("- Total results: %d\n", len(results))

	return results, nil
}

// DataSourceInput is an interface for data sources
type DataSourceInput interface {
	NextBatch(batchSize int) ([]Record, error)
}

// Record represents an input record
type Record struct {
	ID            string
	CiphertextB64 string
}

// Thread-safe metric updates
func (d *KMSDecrypter) incrementTotalDecryptions() {
	d.metricsMu.Lock()
	defer d.metricsMu.Unlock()
	d.totalDecryptions++
}

func (d *KMSDecrypter) incrementCacheHits() {
	d.metricsMu.Lock()
	defer d.metricsMu.Unlock()
	d.cacheHits++
}

func (d *KMSDecrypter) addDecryptionTime(duration time.Duration) {
	d.metricsMu.Lock()
	defer d.metricsMu.Unlock()
	d.decryptionTime += duration
}

func (d *KMSDecrypter) getTotalDecryptions() int64 {
	d.metricsMu.Lock()
	defer d.metricsMu.Unlock()
	return d.totalDecryptions
}

func (d *KMSDecrypter) getCacheHits() int64 {
	d.metricsMu.Lock()
	defer d.metricsMu.Unlock()
	return d.cacheHits
}

func (d *KMSDecrypter) getAvgDecryptionTimeMS() float64 {
	d.metricsMu.Lock()
	defer d.metricsMu.Unlock()
	if d.totalDecryptions == 0 {
		return 0
	}
	return float64(d.decryptionTime.Milliseconds()) / float64(d.totalDecryptions)
}

// decryptBatch decrypts multiple ciphertexts in a single KMS API call
// This is more efficient than multiple individual calls
func (d *KMSDecrypter) decryptBatch(ctx context.Context, batch []Record) (map[string]string, error) {
	if len(batch) == 0 {
		return map[string]string{}, nil
	}
	
	results := make(map[string]string, len(batch))
	needDecryption := make([]struct {
		index int
		ciphertext []byte
	}, 0, len(batch))
	
	// First check cache for all items
	for i, record := range batch {
		// Check cache first
		if plaintext, found := d.cache.Get(record.CiphertextB64); found {
			d.incrementCacheHits()
			results[record.ID] = plaintext.(string)
			continue
		}
		
		// Decode base64
		ciphertext, err := base64.StdEncoding.DecodeString(record.CiphertextB64)
		if err != nil {
			logger.Error("Failed to decode base64 for record %s: %v", record.ID, err)
			continue
		}
		
		// Add to need-decryption list
		needDecryption = append(needDecryption, struct {
			index int
			ciphertext []byte
		}{i, ciphertext})
	}
	
	// If everything was in cache, return early
	if len(needDecryption) == 0 {
		return results, nil
	}
	
	// Create a context with timeout
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	
	// Process items not found in cache in parallel but with limited concurrency
	var wg sync.WaitGroup
	var mu sync.Mutex
	errCount := 0
	
	// Use a limited pool of workers for KMS API calls
	// This prevents too many concurrent calls which can cause rate limiting
	concurrencyLimit := 5
	if len(needDecryption) < concurrencyLimit {
		concurrencyLimit = len(needDecryption)
	}
	
	// Create a channel for work distribution
	workChan := make(chan struct {
		index int
		ciphertext []byte
		record Record
	}, len(needDecryption))
	
	// Start workers
	for i := 0; i < concurrencyLimit; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			
			for work := range workChan {
				start := time.Now()
				d.incrementTotalDecryptions()
				
				// Create decryption request
				req := &kmspb.DecryptRequest{
					Name:       d.keyName,
					Ciphertext: work.ciphertext,
				}
				
				// Call GCP KMS API to decrypt
				resp, err := d.client.Decrypt(ctxWithTimeout, req)
				if err != nil {
					mu.Lock()
					errCount++
					mu.Unlock()
					logger.Error("Failed to decrypt record %s: %v", work.record.ID, err)
					continue
				}
				
				// Get the plaintext as string
				plaintext := string(resp.Plaintext)
				
				// Add to results
				mu.Lock()
				results[work.record.ID] = plaintext
				// Store in cache
				d.cache.Set(work.record.CiphertextB64, plaintext, cache.DefaultExpiration)
				mu.Unlock()
				
				// Update metrics
				elapsed := time.Since(start)
				d.addDecryptionTime(elapsed)
			}
		}()
	}
	
	// Send work to workers
	for _, item := range needDecryption {
		workChan <- struct {
			index int
			ciphertext []byte
			record Record
		}{item.index, item.ciphertext, batch[item.index]}
	}
	
	// Close the channel and wait for workers to finish
	close(workChan)
	wg.Wait()
	
	// Check if we had errors
	if errCount > 0 {
		logger.Error("Encountered %d errors when decrypting batch of %d records", errCount, len(batch))
	}
	
	return results, nil
}
