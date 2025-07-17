# R2 Client Documentation

## Overview

The R2 Client (`fh/r2_client.py`) provides a streamlined interface for uploading fund holdings data to Cloudflare R2 object storage. It serves as the final stage of the data pipeline, converting CSV files to structured JSON and organizing them for efficient web consumption.

## Architecture Philosophy

### JSON-First Approach

- **Input**: Reads CSV files from the local data pipeline
- **Processing**: Converts CSV data to structured JSON with rich metadata
- **Output**: Uploads JSON to R2 for web application consumption
- **Rationale**: JSON provides better structure, metadata, and web API compatibility than CSV

### Data Organization Strategy

#### R2 Bucket Structure

```
{bucket}/
├── summary_tickers.json              # Root-level ticker listing for website
├── latest/
│   ├── IVV/
│   │   └── holdings_enriched.json
│   ├── VOO/
│   │   └── holdings_enriched.json
│   └── {fund_ticker}/
│       └── holdings_enriched.json
└── historical/ (future use)
    └── {fund_ticker}/
        └── {timestamp}/
            └── holdings_enriched.json
```

#### Latest Folder Design

- **Purpose**: Contains the most recent holdings data for each fund
- **Structure**: `latest/{fund_ticker}/holdings_enriched.json`
- **Benefits**:
  - Simple, predictable URLs for web applications
  - No timestamp complexity in API calls
  - Automatic overwrite of stale data
  - Clean separation by fund ticker

## Core Functionality

### CSV to JSON Conversion

The `read_csv_to_json()` method transforms pipeline CSV outputs into structured JSON:

```python
json_data = {
    "metadata": {
        "fund_ticker": "IVV",
        "cik": "1100663",
        "total_holdings": 500,
        "data_timestamp": "20250711_143022",
        "source_file": "holdings_enriched_IVV_1100663_S000004310_20250711_20250711_143022.csv",
        "upload_timestamp": "2025-07-11T14:30:22.123456"
    },
    "holdings": [
        {
            "name": "Apple Inc",
            "cusip": "037833100",
            "ticker": "AAPL",
            "value_usd": 1000000.00,
            "percent_value": 0.025,
            "fund_ticker": "IVV",
            "series_id": "S000004310",
            // ... all other CSV columns
        }
        // ... additional holdings
    ]
}
```

### Key Methods

#### `upload_enriched_holdings_to_latest(file_path, cik)`

- **Purpose**: Upload CSV file as JSON to latest folder
- **Process**:
  1. Read and parse CSV file
  2. Extract fund ticker from filename
  3. Convert to structured JSON
  4. Upload to `latest/{fund_ticker}/holdings_enriched.json`

#### `upload_enriched_holdings_dataframe_to_latest(df, fund_ticker, cik)`

- **Purpose**: Upload DataFrame directly as JSON
- **Use case**: Direct integration with pipeline without intermediate CSV

#### `read_csv_to_json(file_path)`

- **Purpose**: Convert CSV to JSON structure without upload
- **Use case**: Testing, validation, or custom processing

## Filename Convention Integration

### Expected CSV Format

The client extracts metadata from standardized filenames:

```
holdings_enriched_{fund_ticker}_{cik}_{series_id}_{report_date}_{timestamp}.csv
```

Example: `holdings_enriched_IVV_1100663_S000004310_20250711_20250711_143022.csv`

### Extraction Logic

- **Fund Ticker**: Position 2 in underscore-split filename
- **Timestamp**: Last component matching `YYYYMMDD_HHMMSS` pattern
- **Validation**: Fails gracefully if format doesn't match

## Environment Configuration

### Required Variables (.env)

```bash
CLOUDFLARE_R2_ENDPOINT_URL=https://your-account-id.r2.cloudflarestorage.com
CLOUDFLARE_R2_ACCESS_KEY_ID=your_access_key
CLOUDFLARE_R2_SECRET_ACCESS_KEY=your_secret_key
CLOUDFLARE_R2_BUCKET_NAME=your-bucket-name
```

### Client Initialization

```python
from fh.r2_client import R2Client

# Automatically loads .env file
client = R2Client()

# Custom env file location
client = R2Client(env_file="custom.env")
```

## Integration with Pipeline

### Workflow Integration

The R2 client integrates seamlessly with the existing pipeline:

1. **SEC Client**: Fetches series data and extracts fund tickers
2. **N-PORT Parser**: Extracts holdings from XML files
3. **OpenFIGI Client**: Enriches holdings with ticker symbols
4. **Workflow**: Saves enriched data to CSV with fund ticker in filename
5. **R2 Client**: Reads CSV, converts to JSON, uploads to latest folder

### Example Usage in Pipeline

```python
from fh.r2_client import R2Client

# After workflow completes and generates enriched CSV
r2_client = R2Client()

# Find enriched holdings file
enriched_file = "data/holdings_enriched_IVV_1100663_S000004310_20250711_20250711_143022.csv"

# Upload to R2 as JSON
success = r2_client.upload_enriched_holdings_to_latest(enriched_file, "1100663")

if success:
    print("Data available at: latest/IVV/holdings_enriched.json")
```

## Web Application Integration

### API Endpoints

With the R2 structure, web applications can fetch data via simple URLs:

```javascript
// Fetch latest holdings for IVV
const response = await fetch(
  "https://your-r2-domain.com/latest/IVV/holdings_enriched.json",
);
const data = await response.json();

console.log(
  `${data.metadata.fund_ticker} has ${data.metadata.total_holdings} holdings`,
);
console.log(`Data timestamp: ${data.metadata.data_timestamp}`);
```

### Data Structure Benefits

- **Metadata**: Rich information about the dataset
- **Holdings Array**: Clean array of holding objects for easy iteration
- **Type Safety**: Consistent JSON structure for frontend type definitions
- **Performance**: Single file per fund for efficient loading

## Error Handling and Logging

### Robust Error Handling

- **File Validation**: Checks file existence before processing
- **Format Validation**: Validates filename format and extracts metadata
- **Upload Verification**: Confirms successful R2 upload
- **Graceful Degradation**: Returns False on errors with detailed logging

### Logging Strategy

```python
# Success logging
logger.info(f"Uploaded enriched holdings JSON to latest folder: {r2_key}")
logger.info(f"Fund ticker: {fund_ticker}")
logger.info(f"Holdings count: {json_data['metadata']['total_holdings']}")

# Error logging
logger.error(f"Could not extract fund ticker from filename: {filename}")
logger.warning(f"Object {key} not found or error getting info: {e}")
```

## Utility Methods

### Object Management

- `list_objects(prefix)`: List objects with optional prefix filter
- `delete_object(key)`: Remove objects from R2
- `get_object_info(key)`: Retrieve object metadata

### Filename Parsing

- `extract_fund_ticker_from_filename()`: Parse fund ticker from standardized names
- `extract_timestamp_from_filename()`: Extract processing timestamp

## Future Enhancements

### Historical Data Storage

- Archive older datasets to `historical/{fund_ticker}/{timestamp}/`
- Implement data retention policies
- Support for historical data API endpoints

### Data Validation

- JSON schema validation before upload
- Holdings data quality checks
- Metadata completeness verification

### Performance Optimization

- Batch upload capabilities for multiple funds
- Compression for large datasets
- CDN integration for global distribution

## Security Considerations

### Access Control

- R2 credentials stored in environment variables
- No hardcoded secrets in codebase
- Principle of least privilege for R2 permissions

### Data Privacy

- Holdings data is public market information
- No personally identifiable information (PII)
- Compliance with SEC data usage guidelines

## Monitoring and Observability

### Success Metrics

- Upload success rate
- Data freshness (timestamp tracking)
- File size and holdings count trends

### Alerting Opportunities

- Failed uploads
- Missing fund data
- Stale data detection (if upload timestamps fall behind)

## Development and Testing

### Local Testing

```python
# Test CSV to JSON conversion
client = R2Client()
json_data = client.read_csv_to_json("test_file.csv")

# Test upload without affecting production
# Use test bucket or prefix for development
```

### Production Deployment

- Use separate R2 buckets for dev/staging/production
- Implement blue-green deployment for data updates
- Monitor upload logs for production health
