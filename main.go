package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/rf-santos/go-kms/config"
	"github.com/rf-santos/go-kms/datasink"
	"github.com/rf-santos/go-kms/datasource"
	"github.com/rf-santos/go-kms/decrypter"
	"github.com/rf-santos/go-kms/logger"
	"cloud.google.com/go/storage"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/api/option"
)

// Command line flags
var (
	// Configuration file
	configFile = flag.String("config", "", "Path to configuration YAML file")

	// GCP KMS configuration
	projectID  = flag.String("project", "", "GCP project ID")
	locationID = flag.String("location", "", "GCP KMS location")
	keyRingID  = flag.String("keyring", "", "GCP KMS key ring")
	keyID      = flag.String("key", "", "GCP KMS key ID")

	// Processing mode
	mode           = flag.String("mode", "", "Processing mode: parallel, bulk")
	bulkServiceURL = flag.String("bulk-service", "", "URL of bulk decryption service (required for bulk mode)")

	// Data source selection
	datasourceType = flag.String("source", "", "Data source type (csv, mongo, postgres, gcs)")

	// CSV configuration
	inputFile = flag.String("input", "", "Input CSV file with encrypted data")

	// MongoDB configuration
	mongoURI  = flag.String("mongo-uri", "", "MongoDB connection URI")
	mongoDb   = flag.String("mongo-db", "", "MongoDB database name")
	mongoColl = flag.String("mongo-coll", "", "MongoDB collection name")

	// PostgreSQL configuration
	pgURI   = flag.String("pg-uri", "", "PostgreSQL connection URI")
	pgTable = flag.String("pg-table", "", "PostgreSQL table name")

	// GCS configuration
	gcsBucket          = flag.String("gcs-bucket", "", "GCS bucket name (for backward compatibility)")
	gcsInputBucket     = flag.String("gcs-input-bucket", "", "GCS input bucket name")
	gcsOutputBucket    = flag.String("gcs-output-bucket", "", "GCS output bucket name")
	gcsInputObject     = flag.String("gcs-input", "", "GCS object path for input CSV file")
	gcsOutputObject    = flag.String("gcs-output", "", "GCS object path for output CSV file")
	gcsCredentialsFile = flag.String("gcs-creds", "", "Path to GCS credentials file (optional)")

	// Field/column configuration
	idField        = flag.String("id-field", "", "Field/column name for record ID")
	encryptedField = flag.String("enc-field", "", "Field/column name for encrypted data")
	plaintextField = flag.String("pt-field", "", "Field/column name for plaintext output")

	// Filter configuration
	filterExpr = flag.String("filter", "", "Filter expression (like SQL WHERE clause)")

	// Output configuration
	outputFile = flag.String("output", "", "Output CSV file for decrypted data")
	outputType = flag.String("output-type", "", "Output type (csv, mongo, postgres, gcs)")

	// Output MongoDB configuration
	outMongoURI  = flag.String("out-mongo-uri", "", "Output MongoDB connection URI")
	outMongoDb   = flag.String("out-mongo-db", "", "Output MongoDB database name")
	outMongoColl = flag.String("out-mongo-coll", "", "Output MongoDB collection name")

	// Output PostgreSQL configuration
	outPgURI   = flag.String("out-pg-uri", "", "Output PostgreSQL connection URI")
	outPgTable = flag.String("out-pg-table", "", "Output PostgreSQL table name")

	// Output behavior
	updateExisting = flag.Bool("update-existing", false, "Update existing records in output destination")

	// Performance configuration
	numWorkers = flag.Int("workers", 0, "Number of worker goroutines (default: number of CPUs)")
	batchSize  = flag.Int("batch", 0, "Batch size for processing records")
	cacheTTL   = flag.Int("cache-ttl", 0, "Cache TTL in seconds")
	cpuProfile = flag.String("cpu-profile", "", "Write CPU profile to file")
	verbose    = flag.Bool("v", false, "Enable verbose output")

	// Logging configuration
	logLevel = flag.String("log-level", "info", "Log level: debug, info, error")

	// Version information
	version = flag.Bool("version", false, "Print version information and exit")
)

// Version information
const (
	AppVersion = "0.1.0"
	AppAuthor  = "Ricardo F. dos Santos"
)

// setDefaultConfigValues sets optimal default values for performance-critical parameters
func setDefaultConfigValues(cfg *config.Config) {
	// Set optimal number of workers if not specified
	if cfg.NumWorkers <= 0 {
		cpuCount := runtime.NumCPU()
		// Use 75% of available CPUs for optimal performance
		cfg.NumWorkers = int(float64(cpuCount) * 0.75)
		if cfg.NumWorkers < 2 {
			cfg.NumWorkers = 2 // Minimum of 2 workers
		}
		logger.Info("Setting optimal worker count to %d (based on %d CPUs)", cfg.NumWorkers, cpuCount)
	}

	// Set optimal batch size if not specified
	if cfg.BatchSize <= 0 {
		// Default batch size based on worker count
		cfg.BatchSize = 1000
		if cfg.NumWorkers > 8 {
			cfg.BatchSize = 2000 // Larger batch size for more workers
		}
		logger.Info("Setting optimal batch size to %d", cfg.BatchSize)
	}

	// Set optimal cache TTL if not specified
	if cfg.CacheTTL <= 0 {
		cfg.CacheTTL = 300 // 5 minutes default
		logger.Info("Setting optimal cache TTL to %d seconds", cfg.CacheTTL)
	}
}

func main() {
	// Parse command line flags
	flag.Parse()

	// Check if version flag is set
	if *version {
		fmt.Println("go-kms version 1.0.0")
		os.Exit(0)
	}

	// Initialize logger with default level
	logger.InitGlobalLogger(logger.ParseLogLevel(*logLevel), os.Stdout)
	logger.Info("Initializing go-kms with log level: %s", *logLevel)

	// Start CPU profiling if requested
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			logger.Fatal("Could not create CPU profile: %v", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			logger.Fatal("Could not start CPU profile: %v", err)
		}
		defer pprof.StopCPUProfile()
	}

	// Load configuration
	var cfg *config.Config
	var err error

	if *configFile != "" {
		// Load from config file
		logger.Info("Loading configuration from file: %s", *configFile)
		cfg, err = config.LoadConfig(*configFile)
		if err != nil {
			logger.Fatal("Failed to load configuration: %v", err)
		}

		// Override log level from config file if not specified on command line
		logLevelFlag := flag.Lookup("log-level")
		if logLevelFlag != nil && logLevelFlag.Value.String() == "info" && cfg.LogLevel != "" {
			logger.SetGlobalLevel(cfg.GetLogLevel())
			logger.Info("Using log level from config file: %s", cfg.LogLevel)
		}
	} else {
		// Create config from command line flags
		logger.Info("Using command line flags for configuration")
		cfg = &config.Config{
			ProjectID:             *projectID,
			LocationID:            *locationID,
			KeyRingID:             *keyRingID,
			KeyID:                 *keyID,
			Mode:                  *mode,
			NumWorkers:            *numWorkers,
			BatchSize:             *batchSize,
			CacheTTL:              *cacheTTL,
			InputFile:             *inputFile,
			OutputFile:            *outputFile,
			EncryptedFieldName:    *encryptedField,
			IDFieldName:           *idField,
			PlaintextFieldName:    *plaintextField,
			FilterExpression:      *filterExpr,
			MongoURI:              *mongoURI,
			MongoDatabase:         *mongoDb,
			MongoCollection:       *mongoColl,
			PostgresURI:           *pgURI,
			PostgresTable:         *pgTable,
			GCSBucket:             *gcsBucket,
			GCSInputBucket:        *gcsInputBucket,
			GCSOutputBucket:       *gcsOutputBucket,
			GCSInputObject:        *gcsInputObject,
			GCSOutputObject:       *gcsOutputObject,
			GCSCredentialsFile:    *gcsCredentialsFile,
			DataSourceType:        *datasourceType,
			OutputSinkType:        *outputType,
			OutputMongoURI:        *outMongoURI,
			OutputMongoDatabase:   *outMongoDb,
			OutputMongoCollection: *outMongoColl,
			OutputPostgresURI:     *outPgURI,
			OutputPostgresTable:   *outPgTable,
			UpdateExistingRecords: *updateExisting,
			LogLevel:              *logLevel,
		}
	}

	// Set optimal default values for performance-critical parameters
	setDefaultConfigValues(cfg)

	// Validate required fields based on source and output type
	if cfg.DataSourceType == "" {
		logger.Fatal("Error: Data source type is required (use -source flag or config file)")
	}

	// Validate KMS configuration (always required)
	if err := cfg.ValidateKMSConfig(); err != nil {
		logger.Fatal("Error: %v", err)
	}

	// Validate data source specific flags
	validateDataSourceFlags(cfg)

	// Validate output sink specific flags
	validateOutputSinkFlags(cfg)

	// Display configuration
	if *verbose {
		fmt.Println("Configuration:")
		fmt.Printf("- Project ID: %s\n", cfg.ProjectID)
		fmt.Printf("- Location ID: %s\n", cfg.LocationID)
		fmt.Printf("- Key Ring ID: %s\n", cfg.KeyRingID)
		fmt.Printf("- Key ID: %s\n", cfg.KeyID)
		fmt.Printf("- Processing mode: %s\n", cfg.Mode)
		fmt.Printf("- Data source: %s\n", cfg.DataSourceType)
		fmt.Printf("- ID field: %s\n", cfg.IDFieldName)
		fmt.Printf("- Encrypted field: %s\n", cfg.EncryptedFieldName)
		fmt.Printf("- Plaintext field: %s\n", cfg.PlaintextFieldName)
		fmt.Printf("- Output sink: %s\n", cfg.OutputSinkType)
		if cfg.FilterExpression != "" {
			fmt.Printf("- Filter: %s\n", cfg.FilterExpression)
		}
		fmt.Printf("- Workers: %d\n", cfg.NumWorkers)
		fmt.Printf("- Batch Size: %d\n", cfg.BatchSize)
		fmt.Printf("- Cache TTL: %d seconds\n", cfg.CacheTTL)
		fmt.Printf("- Update existing records: %v\n", cfg.UpdateExistingRecords)
		
		// Display GCS configuration if applicable
		if cfg.DataSourceType == "gcs" || cfg.OutputSinkType == "gcs" {
			fmt.Printf("- GCS Bucket: %s\n", cfg.GCSBucket)
			if cfg.DataSourceType == "gcs" {
				fmt.Printf("- GCS Input Object: %s\n", cfg.GCSInputObject)
			}
			if cfg.OutputSinkType == "gcs" {
				fmt.Printf("- GCS Output Object: %s\n", cfg.GCSOutputObject)
			}
		}
	}

	// Create KMS decrypter
	kmsDecrypter, err := decrypter.NewKMSDecrypter(cfg)
	if err != nil {
		logger.Fatal("Failed to create KMS decrypter: %v", err)
	}
	defer kmsDecrypter.Close()

	// Set up bulk service if in bulk mode
	if *mode == "bulk" {
		bulkService := decrypter.NewHTTPBulkDecryptionService(*bulkServiceURL)
		kmsDecrypter.SetBulkDecryptionService(bulkService)
	}

	// Create appropriate data source
	logger.Info("Creating %s data source", cfg.DataSourceType)
	source, err := datasource.DataSourceFactory(cfg)
	if err != nil {
		logger.Fatal("Failed to create data source: %v", err)
	}

	// Open data source
	logger.Info("Opening data source")
	err = source.Open()
	if err != nil {
		logger.Fatal("Failed to open data source: %v", err)
	}
	defer source.Close()

	// Create output sink
	logger.Info("Creating %s output sink", cfg.OutputSinkType)
	sink, err := datasink.OutputSinkFactory(cfg)
	if err != nil {
		logger.Fatal("Failed to create output sink: %v", err)
	}

	// Open output sink
	logger.Info("Opening output sink")
	err = sink.Open()
	if err != nil {
		logger.Fatal("Failed to open output sink: %v", err)
	}
	defer sink.Close()

	// Create adapter between datasource and decrypter
	adapter := decrypter.NewDataSourceAdapter(source)

	// Start decryption based on selected mode
	ctx := context.Background()
	startTime := time.Now()
	var results map[string]string

	// Get accurate record count for progress bar
	recordCount := getAccurateRecordCount(cfg, source)
	logger.Info("Processing %d records", recordCount)

	switch cfg.Mode {
	case "bulk":
		logger.Info("Using bulk decryption mode with service: %s", *bulkServiceURL)
		results, err = kmsDecrypter.DecryptAllBulk(ctx, adapter)
	case "parallel":
		logger.Info("Using parallel decryption mode with %d workers", cfg.NumWorkers)
		results, err = kmsDecrypter.DecryptAll(ctx, adapter, recordCount)
	}

	if err != nil {
		logger.Fatal("Failed to decrypt records: %v", err)
	}

	// Convert results to OutputRecord format
	outputRecords := make([]datasink.OutputRecord, 0, len(results))
	for id, plaintext := range results {
		outputRecords = append(outputRecords, datasink.OutputRecord{
			ID:        id,
			Plaintext: plaintext,
		})
	}

	// Write results to output sink
	logger.Info("Writing %d decrypted records to output sink", len(outputRecords))
	err = sink.Write(outputRecords)
	if err != nil {
		logger.Fatal("Failed to write results: %v", err)
	}

	// Display timing information
	duration := time.Since(startTime)
	logger.Info("Total processing time: %.2f seconds", duration.Seconds())
	logger.Info("Total records processed: %d", len(results))
	logger.Info("Processing speed: %.2f records/second", float64(len(results))/duration.Seconds())
	logger.Info("Average decryption time: %.2f ms", float64(duration.Milliseconds())/float64(len(results)))
}

// validateDataSourceFlags validates data source configuration
func validateDataSourceFlags(cfg *config.Config) {
	// Common required fields for all data sources
	if cfg.IDFieldName == "" {
		logger.Fatal("Error: ID field name is required for all data sources")
	}
	if cfg.EncryptedFieldName == "" {
		logger.Fatal("Error: Encrypted field name is required for all data sources")
	}

	// Create a temporary data source to validate filter and check record count
	var tempSource datasource.DataSource
	var err error

	switch cfg.DataSourceType {
	case "csv":
		if cfg.InputFile == "" {
			logger.Fatal("Error: Input file path is required for CSV data source")
		}
		if _, err := os.Stat(cfg.InputFile); os.IsNotExist(err) {
			logger.Fatalf("Error: Input file '%s' does not exist", cfg.InputFile)
		}
		tempSource, err = datasource.NewCSVDataSource(cfg.InputFile)

	case "mongo":
		if cfg.MongoURI == "" {
			logger.Fatal("Error: MongoDB connection URI is required")
		}
		if cfg.MongoDatabase == "" {
			logger.Fatal("Error: MongoDB database name is required")
		}
		if cfg.MongoCollection == "" {
			logger.Fatal("Error: MongoDB collection name is required")
		}

		// Test MongoDB connection and validate resources
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		client, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.MongoURI))
		if err != nil {
			logger.Fatalf("Error: Failed to connect to MongoDB: %v", err)
		}
		defer client.Disconnect(ctx)

		// Ping the database to verify connection
		if err := client.Ping(ctx, nil); err != nil {
			logger.Fatalf("Error: Failed to ping MongoDB: %v", err)
		}

		// Check if database exists
		databases, err := client.ListDatabaseNames(ctx, bson.M{"name": cfg.MongoDatabase})
		if err != nil {
			logger.Fatalf("Error: Failed to list MongoDB databases: %v", err)
		}
		if len(databases) == 0 {
			logger.Fatalf("Error: Database '%s' does not exist in MongoDB", cfg.MongoDatabase)
		}

		// Check if collection exists
		db := client.Database(cfg.MongoDatabase)
		collections, err := db.ListCollectionNames(ctx, bson.M{"name": cfg.MongoCollection})
		if err != nil {
			logger.Fatalf("Error: Failed to list MongoDB collections: %v", err)
		}
		if len(collections) == 0 {
			logger.Fatalf("Error: Collection '%s' does not exist in database '%s'", cfg.MongoCollection, cfg.MongoDatabase)
		}

		tempSource, err = datasource.NewMongoDataSource(cfg.MongoURI, cfg.MongoDatabase, cfg.MongoCollection)

	case "postgres":
		if cfg.PostgresURI == "" {
			logger.Fatal("Error: PostgreSQL connection URI is required")
		}
		if cfg.PostgresTable == "" {
			logger.Fatal("Error: PostgreSQL table name is required")
		}

		// Test PostgreSQL connection and validate resources
		db, err := sql.Open("postgres", cfg.PostgresURI)
		if err != nil {
			logger.Fatalf("Error: Failed to connect to PostgreSQL: %v", err)
		}
		defer db.Close()

		// Test connection
		if err := db.Ping(); err != nil {
			logger.Fatalf("Error: Failed to ping PostgreSQL: %v", err)
		}

		// Check if table exists
		var tableExists bool
		err = db.QueryRow(`
			SELECT EXISTS (
				SELECT FROM information_schema.tables 
				WHERE table_schema = 'public' 
				AND table_name = $1
			)`, cfg.PostgresTable).Scan(&tableExists)
		if err != nil {
			logger.Fatalf("Error: Failed to check if table exists: %v", err)
		}
		if !tableExists {
			logger.Fatalf("Error: Table '%s' does not exist in PostgreSQL database", cfg.PostgresTable)
		}

		tempSource, err = datasource.NewPostgresDataSource(cfg.PostgresURI, cfg.PostgresTable)

	case "gcs":
		// Determine which bucket to use for input
		inputBucket := cfg.GCSInputBucket
		if inputBucket == "" {
			inputBucket = cfg.GCSBucket
		}
		
		if inputBucket == "" {
			logger.Fatal("Error: GCS input bucket name is required")
		}
		if cfg.GCSInputObject == "" {
			logger.Fatal("Error: GCS input object name is required")
		}

		// Test GCS connection and validate resources
		ctx := context.Background()
		var clientOpts []option.ClientOption
		if cfg.GCSCredentialsFile != "" {
			clientOpts = append(clientOpts, option.WithCredentialsFile(cfg.GCSCredentialsFile))
		}

		client, err := storage.NewClient(ctx, clientOpts...)
		if err != nil {
			logger.Fatalf("Error: Failed to create GCS client: %v", err)
		}
		defer client.Close()

		// Check if bucket exists
		bucket := client.Bucket(inputBucket)
		_, err = bucket.Attrs(ctx)
		if err != nil {
			if err == storage.ErrBucketNotExist {
				logger.Fatalf("Error: Bucket '%s' does not exist in GCS", inputBucket)
			}
			logger.Fatalf("Error: Failed to access bucket '%s': %v", inputBucket, err)
		}

		// Check if object exists
		obj := bucket.Object(cfg.GCSInputObject)
		_, err = obj.Attrs(ctx)
		if err != nil {
			if err == storage.ErrObjectNotExist {
				logger.Fatalf("Error: Object '%s' does not exist in bucket '%s'", cfg.GCSInputObject, inputBucket)
			}
			logger.Fatalf("Error: Failed to access object '%s': %v", cfg.GCSInputObject, err)
		}

		tempSource, err = datasource.NewGCSDataSource(inputBucket, cfg.GCSInputObject, cfg.GCSCredentialsFile)

	default:
		logger.Fatalf("Error: Invalid data source type '%s'. Must be one of: csv, mongo, postgres, gcs", cfg.DataSourceType)
	}

	if err != nil {
		logger.Fatalf("Error: Failed to create temporary data source for validation: %v", err)
	}

	// Set common fields for validation
	if tempSource != nil {
		tempSource.SetIDField(cfg.IDFieldName)
		tempSource.SetEncryptedField(cfg.EncryptedFieldName)
	}

	// Validate filter expression if provided
	if cfg.FilterExpression != "" {
		// Try to set the filter
		if err := tempSource.SetFilter(cfg.FilterExpression); err != nil {
			logger.Fatalf("Error: Invalid filter expression '%s': %v", cfg.FilterExpression, err)
		}

		// Open the data source
		if err := tempSource.Open(); err != nil {
			logger.Fatalf("Error: Failed to open data source for validation: %v", err)
		}
		defer tempSource.Close()

		// Try to read a batch to validate the filter
		records, err := tempSource.Next(1)
		if err != nil {
			logger.Fatalf("Error: Failed to validate filter expression: %v", err)
		}

		// Check if any records match the filter
		if len(records) == 0 {
			logger.Fatal("Error: No records found matching the filter expression")
		}

		logger.Info("Found %d records matching filter expression", len(records))
	}
}

// validateOutputSinkFlags validates output sink configuration
func validateOutputSinkFlags(cfg *config.Config) {
	if cfg.OutputSinkType == "" {
		logger.Fatal("Error: Output sink type is required")
	}

	// Common required field for all output sinks
	if cfg.PlaintextFieldName == "" {
		logger.Fatal("Error: Plaintext field name is required for all output sinks")
	}

	switch cfg.OutputSinkType {
	case "csv":
		if cfg.OutputFile == "" {
			logger.Fatal("Error: Output file path is required for CSV output sink")
		}
		// Check if output directory exists and is writable
		outputDir := filepath.Dir(cfg.OutputFile)
		if outputDir != "." {
			if _, err := os.Stat(outputDir); os.IsNotExist(err) {
				logger.Fatalf("Error: Output directory '%s' does not exist", outputDir)
			}
			// Try to create a temporary file to test write permissions
			tmpFile := filepath.Join(outputDir, ".write_test")
			if err := os.WriteFile(tmpFile, []byte("test"), 0644); err != nil {
				logger.Fatalf("Error: No write permission in output directory '%s': %v", outputDir, err)
			}
			os.Remove(tmpFile)
		}

	case "mongo":
		if cfg.OutputMongoURI == "" {
			logger.Fatal("Error: Output MongoDB connection URI is required")
		}
		if cfg.OutputMongoDatabase == "" {
			logger.Fatal("Error: Output MongoDB database name is required")
		}
		if cfg.OutputMongoCollection == "" {
			logger.Fatal("Error: Output MongoDB collection name is required")
		}

		// Test MongoDB connection and validate resources
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		client, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.OutputMongoURI))
		if err != nil {
			logger.Fatalf("Error: Failed to connect to MongoDB: %v", err)
		}
		defer client.Disconnect(ctx)

		// Ping the database to verify connection
		if err := client.Ping(ctx, nil); err != nil {
			logger.Fatalf("Error: Failed to ping MongoDB: %v", err)
		}

		// Check if database exists
		databases, err := client.ListDatabaseNames(ctx, bson.M{"name": cfg.OutputMongoDatabase})
		if err != nil {
			logger.Fatalf("Error: Failed to list MongoDB databases: %v", err)
		}
		if len(databases) == 0 {
			logger.Fatalf("Error: Database '%s' does not exist in MongoDB", cfg.OutputMongoDatabase)
		}

		// Check if collection exists
		db := client.Database(cfg.OutputMongoDatabase)
		collections, err := db.ListCollectionNames(ctx, bson.M{"name": cfg.OutputMongoCollection})
		if err != nil {
			logger.Fatalf("Error: Failed to list MongoDB collections: %v", err)
		}
		if len(collections) == 0 {
			logger.Fatalf("Error: Collection '%s' does not exist in database '%s'", cfg.OutputMongoCollection, cfg.OutputMongoDatabase)
		}

	case "postgres":
		if cfg.OutputPostgresURI == "" {
			logger.Fatal("Error: Output PostgreSQL connection URI is required")
		}
		if cfg.OutputPostgresTable == "" {
			logger.Fatal("Error: Output PostgreSQL table name is required")
		}

		// Test PostgreSQL connection and validate resources
		db, err := sql.Open("postgres", cfg.OutputPostgresURI)
		if err != nil {
			logger.Fatalf("Error: Failed to connect to PostgreSQL: %v", err)
		}
		defer db.Close()

		// Test connection
		if err := db.Ping(); err != nil {
			logger.Fatalf("Error: Failed to ping PostgreSQL: %v", err)
		}

		// Check if table exists
		var tableExists bool
		err = db.QueryRow(`
			SELECT EXISTS (
				SELECT FROM information_schema.tables 
				WHERE table_schema = 'public' 
				AND table_name = $1
			)`, cfg.OutputPostgresTable).Scan(&tableExists)
		if err != nil {
			logger.Fatalf("Error: Failed to check if table exists: %v", err)
		}
		if !tableExists {
			logger.Fatalf("Error: Table '%s' does not exist in PostgreSQL database", cfg.OutputPostgresTable)
		}

	case "gcs":
		// Determine which bucket to use for output
		outputBucket := cfg.GCSOutputBucket
		if outputBucket == "" {
			outputBucket = cfg.GCSBucket
		}
		
		if outputBucket == "" {
			logger.Fatal("Error: GCS output bucket name is required for GCS output sink")
		}
		if cfg.GCSOutputObject == "" {
			logger.Fatal("Error: GCS output object path is required for GCS output sink")
		}

		// Test GCS connection and validate resources
		ctx := context.Background()
		var clientOpts []option.ClientOption
		if cfg.GCSCredentialsFile != "" {
			clientOpts = append(clientOpts, option.WithCredentialsFile(cfg.GCSCredentialsFile))
		}

		client, err := storage.NewClient(ctx, clientOpts...)
		if err != nil {
			logger.Fatalf("Error: Failed to create GCS client: %v", err)
		}
		defer client.Close()

		// Check if bucket exists
		bucket := client.Bucket(outputBucket)
		_, err = bucket.Attrs(ctx)
		if err != nil {
			if err == storage.ErrBucketNotExist {
				logger.Fatalf("Error: Bucket '%s' does not exist in GCS", outputBucket)
			}
			logger.Fatalf("Error: Failed to access bucket '%s': %v", outputBucket, err)
		}
		
	default:
		logger.Fatalf("Error: Invalid output sink type '%s'. Must be one of: csv, mongo, postgres, gcs", cfg.OutputSinkType)
	}
}

// getAccurateRecordCount gets the most accurate record count possible
func getAccurateRecordCount(cfg *config.Config, src datasource.DataSource) int {
	switch cfg.DataSourceType {
	case "csv":
		count := countCSVRecords(cfg.InputFile)
		return count
	case "gcs":
		// For GCS, we can't get an accurate count easily
		return 1000 // Default estimate
	case "mongo":
		// For MongoDB, we can't get an accurate count easily
		return 1000 // Default estimate
	case "postgres":
		// For PostgreSQL, we can't get an accurate count easily
		return 1000 // Default estimate
	default:
		return 100 // Safe default
	}
}

// countCSVRecords counts the number of records in a CSV file
func countCSVRecords(filePath string) int {
	file, err := os.Open(filePath)
	if err != nil {
		logger.Printf("Warning: Failed to open file for counting: %v", err)
		return 100 // Default if we can't count
	}
	defer file.Close()

	reader := csv.NewReader(file)
	count := 0

	// Skip header
	_, err = reader.Read()
	if err != nil {
		return 0
	}

	// Count records
	for {
		_, err := reader.Read()
		if err != nil {
			break
		}
		count++
	}

	return count
}
