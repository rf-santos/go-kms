package config

import (
	"fmt"
	"os"

	"github.com/rf-santos/go-kms/logger"
	"gopkg.in/yaml.v3"
)

// Config holds the application configuration
type Config struct {
	// GCP KMS configuration
	ProjectID  string `yaml:"project_id"`
	LocationID string `yaml:"location_id"`
	KeyRingID  string `yaml:"keyring_id"`
	KeyID      string `yaml:"key_id"`

	// Processing mode
	Mode string `yaml:"mode"` // "parallel" or "bulk"

	// Worker pool configuration
	NumWorkers int `yaml:"workers"`
	BatchSize  int `yaml:"batch_size"`

	// Cache configuration
	CacheTTL int `yaml:"cache_ttl"` // Time to live in seconds

	// Input/Output configuration
	InputFile  string `yaml:"input_file"`
	OutputFile string `yaml:"output_file"`

	// Column/field configuration
	EncryptedFieldName string `yaml:"encrypted_field"` // Name of the field/column that contains encrypted data
	IDFieldName        string `yaml:"id_field"`        // Name of the field/column that contains record IDs
	PlaintextFieldName string `yaml:"plaintext_field"` // Name of the field/column for plaintext output data

	// Filter configuration
	FilterExpression string `yaml:"filter"` // Expression to filter records (SQL-like WHERE clause)

	// Input Database configuration
	MongoURI        string `yaml:"mongo_uri"`
	MongoDatabase   string `yaml:"mongo_db"`
	MongoCollection string `yaml:"mongo_coll"`
	PostgresURI     string `yaml:"postgres_uri"`
	PostgresTable   string `yaml:"postgres_table"`

	// GCS configuration
	GCSBucket           string `yaml:"gcs_bucket"`            // GCS bucket name (for backward compatibility)
	GCSInputBucket      string `yaml:"gcs_input_bucket"`      // GCS input bucket name
	GCSOutputBucket     string `yaml:"gcs_output_bucket"`     // GCS output bucket name
	GCSInputObject      string `yaml:"gcs_input"`             // GCS object path for input
	GCSOutputObject     string `yaml:"gcs_output"`            // GCS object path for output
	GCSCredentialsFile  string `yaml:"gcs_credentials"`       // Path to GCS credentials file (optional)

	// Data source type
	DataSourceType string `yaml:"source"` // "csv", "mongo", "postgres", or "gcs"

	// Output sink configuration
	OutputSinkType        string `yaml:"output_type"`         // "csv", "mongo", "postgres", or "gcs"
	OutputMongoURI        string `yaml:"out_mongo_uri"`
	OutputMongoDatabase   string `yaml:"out_mongo_db"`
	OutputMongoCollection string `yaml:"out_mongo_coll"`
	OutputPostgresURI     string `yaml:"out_postgres_uri"`
	OutputPostgresTable   string `yaml:"out_postgres_table"`
	UpdateExistingRecords bool   `yaml:"update_existing"`     // Whether to update existing records or insert new ones

	// Logging configuration
	LogLevel string `yaml:"log_level"` // "debug", "info", or "error"
}

// ValidateKMSConfig validates that all required KMS configuration fields are present
func (c *Config) ValidateKMSConfig() error {
	if c.ProjectID == "" {
		return fmt.Errorf("project_id is required")
	}
	if c.LocationID == "" {
		return fmt.Errorf("location_id is required")
	}
	if c.KeyRingID == "" {
		return fmt.Errorf("keyring_id is required")
	}
	if c.KeyID == "" {
		return fmt.Errorf("key_id is required")
	}
	return nil
}

// LoadConfig loads configuration from a YAML file
func LoadConfig(configFile string) (*Config, error) {
	data, err := os.ReadFile(configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Validate required KMS configuration
	if err := cfg.ValidateKMSConfig(); err != nil {
		return nil, err
	}

	// Set default log level if not specified
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}

	return &cfg, nil
}

// GetLogLevel returns the parsed log level from the config
func (c *Config) GetLogLevel() logger.Level {
	return logger.ParseLogLevel(c.LogLevel)
}

// DefaultConfig returns a configuration with default values
func DefaultConfig() *Config {
	return &Config{
		// GCP KMS configuration
		ProjectID:  "",
		LocationID: "",
		KeyRingID:  "",
		KeyID:      "",

		// Processing mode
		Mode: "parallel",

		// Worker pool configuration
		NumWorkers: 0, // Will be set to number of CPUs if not specified
		BatchSize:  100,
		CacheTTL:   300,

		// Input/Output configuration
		InputFile:  "encrypted.csv",
		OutputFile: "decrypted.csv",

		// Column/field configuration
		EncryptedFieldName: "encrypted_data",
		IDFieldName:        "id",
		PlaintextFieldName: "plaintext",

		// Data source type
		DataSourceType: "csv",

		// Output sink configuration
		OutputSinkType:        "csv",
		UpdateExistingRecords: false,
	}
}
