# go-kms

A high-performance tool for bulk decryption of data using Google Cloud KMS (Key Management Service). This tool supports multiple data sources and output sinks, making it versatile for various use cases.

## Features

- Multiple data source support:
  - CSV files
  - MongoDB collections
  - PostgreSQL databases
  - Google Cloud Storage (GCS)
- Multiple output sink support:
  - CSV files
  - MongoDB collections
  - PostgreSQL databases
  - Google Cloud Storage (GCS)
- Two processing modes:
  - Parallel processing with worker pool
  - Bulk processing using external service
- Performance optimizations:
  - Connection pooling
  - Batch processing
  - Result caching
  - Memory-efficient processing
- Progress monitoring with metrics:
  - Records processed per second
  - Average decryption time
  - Cache hit rate
  - Total processing time

## Installation

```bash
# Clone the repository
git clone https://github.com/rf-santos/go-kms.git
cd go-kms

# Build the application
go build -o go-kms
```

## Configuration

### Command Line Flags

#### Required Flags

- `-source`: Data source type (csv, mongo, postgres, gcs)
- `-project`: GCP project ID
- `-location`: GCP KMS location
- `-keyring`: GCP KMS key ring
- `-key`: GCP KMS key ID
- `-id-field`: Field/column name for record ID
- `-enc-field`: Field/column name for encrypted data
- `-pt-field`: Field/column name for plaintext output
- `-output-type`: Output type (csv, mongo, postgres, gcs)

#### Data Source Specific Flags

##### CSV Source
- `-input`: Input CSV file path

##### MongoDB Source
- `-mongo-uri`: MongoDB connection URI
- `-mongo-db`: MongoDB database name
- `-mongo-coll`: MongoDB collection name

##### PostgreSQL Source
- `-pg-uri`: PostgreSQL connection URI
- `-pg-table`: PostgreSQL table name

##### GCS Source
- `-gcs-bucket`: GCS bucket name (legacy)
- `-gcs-input-bucket`: GCS input bucket name
- `-gcs-output-bucket`: GCS output bucket name
- `-gcs-input`: GCS object path for input CSV file
- `-gcs-output`: GCS object path for output CSV file
- `-gcs-creds`: Path to GCS credentials file (optional)

#### Output Sink Specific Flags

##### CSV Output
- `-output`: Output CSV file path

##### MongoDB Output
- `-out-mongo-uri`: Output MongoDB connection URI
- `-out-mongo-db`: Output MongoDB database name
- `-out-mongo-coll`: Output MongoDB collection name

##### PostgreSQL Output
- `-out-pg-uri`: Output PostgreSQL connection URI
- `-out-pg-table`: Output PostgreSQL table name

#### Performance Flags

- `-mode`: Processing mode (parallel, bulk)
- `-workers`: Number of worker goroutines (default: 75% of CPUs)
- `-batch`: Batch size for processing records (default: 1000)
- `-cache-ttl`: Cache TTL in seconds (default: 300)
- `-bulk-service`: URL of bulk decryption service (required for bulk mode)

#### Other Flags

- `-config`: Path to configuration YAML file
- `-filter`: Filter expression (like SQL WHERE clause)
- `-update-existing`: Update existing records in output destination
- `-cpu-profile`: Write CPU profile to file
- `-v`: Enable verbose output
- `-log-level`: Log level (debug, info, error)
- `-version`: Print version information and exit

### Configuration File (config.yml)

```yaml
# GCP KMS Configuration
project_id: "your-project-id"
location_id: "your-location"
key_ring_id: "your-keyring"
key_id: "your-key"

# Processing Mode
mode: "parallel"  # or "bulk"
num_workers: 4    # number of worker goroutines
batch_size: 1000  # batch size for processing
cache_ttl: 300    # cache TTL in seconds

# Data Source Configuration
data_source_type: "csv"  # csv, mongo, postgres, gcs
input_file: "input.csv"  # for CSV source
mongo_uri: "mongodb://localhost:27017"  # for MongoDB source
mongo_database: "mydb"
mongo_collection: "mycoll"
postgres_uri: "postgres://user:pass@localhost:5432/mydb"  # for PostgreSQL source
postgres_table: "mytable"
gcs_bucket: "my-bucket"  # for GCS source
gcs_input_bucket: "input-bucket"
gcs_output_bucket: "output-bucket"
gcs_input_object: "input.csv"
gcs_output_object: "output.csv"
gcs_credentials_file: "path/to/credentials.json"

# Field Configuration
id_field_name: "id"
encrypted_field_name: "encrypted_data"
plaintext_field_name: "plaintext_data"

# Output Configuration
output_sink_type: "csv"  # csv, mongo, postgres, gcs
output_file: "output.csv"  # for CSV output
output_mongo_uri: "mongodb://localhost:27017"  # for MongoDB output
output_mongo_database: "outputdb"
output_mongo_collection: "outputcoll"
output_postgres_uri: "postgres://user:pass@localhost:5432/outputdb"  # for PostgreSQL output
output_postgres_table: "outputtable"
update_existing_records: false

# Filter Configuration
filter_expression: "field = 'value'"  # optional filter

# Logging Configuration
log_level: "info"  # debug, info, error
```

## Usage Examples

### CSV to CSV Processing

```bash
./go-kms \
  -source csv \
  -input input.csv \
  -output-type csv \
  -output output.csv \
  -project my-project \
  -location us-central1 \
  -keyring my-keyring \
  -key my-key \
  -id-field id \
  -enc-field encrypted_data \
  -pt-field plaintext_data
```

### MongoDB to PostgreSQL Processing

```bash
./go-kms \
  -source mongo \
  -mongo-uri mongodb://localhost:27017 \
  -mongo-db mydb \
  -mongo-coll mycoll \
  -output-type postgres \
  -out-pg-uri postgres://user:pass@localhost:5432/outputdb \
  -out-pg-table outputtable \
  -project my-project \
  -location us-central1 \
  -keyring my-keyring \
  -key my-key \
  -id-field _id \
  -enc-field encrypted_data \
  -pt-field plaintext_data
```

### GCS to GCS Processing with Different Buckets

```bash
./go-kms \
  -source gcs \
  -gcs-input-bucket input-bucket \
  -gcs-input input.csv \
  -output-type gcs \
  -gcs-output-bucket output-bucket \
  -gcs-output output.csv \
  -project my-project \
  -location us-central1 \
  -keyring my-keyring \
  -key my-key \
  -id-field id \
  -enc-field encrypted_data \
  -pt-field plaintext_data
```

### Using Configuration File

```bash
./go-kms -config config.yml
```

## Performance Tuning

### Worker Count
- Default: 2 * available CPUs (optimized for network IO)
- Adjust with `-workers` flag
- Recommended: 2-16 workers for most workloads
- Higher values may increase contention

### Batch Size
- Default: 1000 records
- Adjust with `-batch` flag
- Larger batches reduce API calls but increase memory usage
- Recommended: 500-2000 for most workloads

### Cache TTL
- Default: 300 seconds (5 minutes)
- Adjust with `-cache-ttl` flag
- Longer TTL reduces API calls but increases memory usage
- Recommended: 300-3600 seconds based on data patterns

### Processing Mode
- `parallel`: Uses worker pool for concurrent processing
- `bulk`: Uses external bulk decryption service
- Choose based on workload size and requirements

## Troubleshooting

### Common Issues

1. **High Memory Usage**
   - Reduce batch size
   - Enable result caching
   - Use streaming mode for large datasets

2. **Slow Processing**
   - Increase worker count
   - Increase batch size
   - Check network latency
   - Enable caching

3. **API Rate Limits**
   - Reduce worker count
   - Increase batch size
   - Enable caching
   - Use bulk mode

4. **Connection Issues**
   - Check credentials
   - Verify network connectivity
   - Check firewall rules
   - Validate connection strings

### Debug Mode

Enable debug logging for detailed information:
```bash
./go-kms -log-level debug ...
```

### CPU Profiling

Generate CPU profile for performance analysis:
```bash
./go-kms -cpu-profile profile.pprof ...
```

## Metrics

The tool provides the following metrics:

1. **Processing Speed**
   - Records processed per second
   - Average decryption time per record

2. **Cache Performance**
   - Cache hit rate
   - Cache miss rate

3. **Resource Usage**
   - Total processing time
   - Memory usage
   - CPU usage (when profiling enabled)

4. **Batch Statistics**
   - Total batches processed
   - Average batch processing time
   - Batch success rate

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 