# Data Pipeline Documentation

## Overview

The GetFundHoldings data pipeline is a comprehensive 5-stage workflow system that extracts, processes, and enriches SEC N-PORT filing data to create structured investment holdings datasets. The pipeline transforms raw SEC filings into enriched, ticker-enhanced CSV files ready for upload to Cloudflare R2.

## Pipeline Architecture

```
CIK → Series/Class → N-PORT Filings → XML Data → Holdings → Enriched Artifacts
```

## Stage-by-Stage Workflow

### Stage 1: Series Discovery

**Purpose**: Identify fund series and classes from SEC submissions data  
**Input**: Company CIK numbers (e.g., "1100663" for iShares)  
**Process**: Query SEC submissions API for series metadata  
**Output**: `series_data_{cik}_{timestamp}.json`

**Key Functions**:

- `SECHTTPClient.fetch_series_data(cik)`
- Extracts series IDs, fund names, and ticker symbols
- Filters out invalid series (Home, EDGAR references)

### Stage 2: Filing Collection

**Purpose**: Gather N-PORT filing metadata for each series  
**Input**: Series IDs from Stage 1  
**Process**: Query SEC EDGAR for N-PORT filing submissions  
**Output**: `series_filings_{series_id}_{cik}_{timestamp}.json`

**Key Functions**:

- `SECHTTPClient.fetch_series_filings(series_id)`
- Collects accession numbers, filing dates, document URLs
- Respects SEC rate limits (10 requests/second max)

### Stage 3: XML Download

**Purpose**: Download actual N-PORT XML files  
**Input**: Filing metadata from Stage 2  
**Process**: Retrieve XML files from SEC EDGAR via accession numbers  
**Output**: `nport_{cik}_{accession_number}_{series_id}.xml`

**Key Functions**:

- `SECHTTPClient.download_and_save_nport(cik, accession_number, series_id)`
- Handles large XML files (up to 500K investment entries)
- Implements retry logic and error handling

### Stage 4: Holdings Extraction

**Purpose**: Parse XML files to extract structured holdings data  
**Input**: N-PORT XML files from Stage 3  
**Process**: Parse XML using custom N-PORT parser  
**Output**: `holdings_{ticker}_{cik}_{series_id}_{report_date}_{timestamp}.csv`

**Key Functions**:

- `parse_nport_file(xml_file)` - Custom XML parser
- Extracts: security identifiers (CUSIP, ISIN), market values, names, titles
- Adds metadata: source file, fund ticker, series ID

### Stage 5: Holdings Enrichment

**Purpose**: Enhance holdings with ticker symbols and data quality notes  
**Input**: Holdings CSV files from Stage 4  
**Process**: Multi-step enrichment pipeline  
**Output**: `holdings_enriched_{ticker}_{cik}_{series_id}_{report_date}_{timestamp}.csv`

**Enrichment Steps**:

1. **Metadata Enrichment**: CIK, series, report date
2. **Timestamp Enrichment**: UTC enrichment datetime
3. **Ticker Enrichment**: CUSIP → ISIN fallback lookup via OpenFIGI
4. **Notes Enrichment**: Data quality flags

## Ticker Enrichment Logic

### Dual-Identifier Strategy

```python
# Primary: CUSIP lookup (US domestic securities)
enriched_df = openfigi_client.add_tickers_to_dataframe_by_cusip(holdings_df, 'cusip')

# Fallback: ISIN lookup (international securities)
failed_cusip_holdings = enriched_df[enriched_df['ticker'].isna()]
isin_enriched = openfigi_client.add_tickers_to_dataframe_by_isin(failed_cusip_holdings, 'isin')
```

### Derivative Detection

- Automatically identifies and excludes derivative instruments from ticker lookup
- Detects: ELNs, total return swaps, equity linked notes
- Prevents unnecessary API calls for non-equity securities

### Data Quality Tracking

- `enrichment_notes` column tracks data quality issues
- Flags: `derivative_instrument`, `missing_cusip`, `missing_isin`, `missing_ticker`

## R2 Upload Pipeline

### Summary Ticker Generation

**Script**: `create_summary_tickers.py`  
**Purpose**: Create website-ready ticker metadata for public listing  
**Output**: `summary_tickers.json`

**Process**:

1. Find latest enriched holdings file per ticker using same logic as R2Client
2. Load series metadata to get fund names
3. Combine into structured JSON with metadata

### Multi-Environment Upload

**Script**: `fh/r2_client.py`  
**Environments**: Development (`-e dev`) and Production (`-e prod`)  
**Configuration**: `.env.dev` and `.env.prod` files

**Upload Structure**:

```
R2 Bucket Structure:
├── summary_tickers.json              # Root-level ticker listing
└── latest/
    ├── IVV/holdings_enriched.json    # Per-ticker holdings data
    ├── JEPI/holdings_enriched.json
    └── {ticker}/holdings_enriched.json
```

## Configuration & Execution

### Workflow Configuration

```python
config = WorkflowConfig(
    cik_list=["1100663", "0001485894"],  # iShares, JPMorgan
    enable_ticker_enrichment=True,
    max_series_per_cik=None,             # No limit
    max_filings_per_series=1,            # Latest filing only
    interested_etf_tickers=["IVV", "JEPI"]  # Optional filtering
)
```

### Execution Commands

```bash
# Full pipeline execution
uv run python -m fh.workflow

# Create summary tickers file
uv run python create_summary_tickers.py

# Upload to R2 (dev environment)
uv run python create_summary_tickers.py --upload -e dev
uv run python -m fh.r2_client -e dev

# Upload to R2 (prod environment)
uv run python -m fh.r2_client -e prod
```

## Output Artifacts

### Raw Holdings Data

- **Format**: CSV with quoted fields
- **Columns**: name, title, cusip, isin, market_value, percentage, source_file, fund_ticker, series_id
- **Naming**: `holdings_{ticker}_{cik}_{series_id}_{report_date}_{timestamp}.csv`

### Enriched Holdings Data

- **Format**: CSV with quoted fields
- **Additional Columns**: ticker, enrichment_datetime, enrichment_notes, report_period_date
- **Naming**: `holdings_enriched_{ticker}_{cik}_{series_id}_{report_date}_{timestamp}.csv`

### Summary Ticker Data

- **Format**: JSON with metadata and ticker array
- **Fields**: ticker, name, title, cik, series_id, latest_report_date, latest_timestamp
- **Purpose**: Website ticker listing and metadata

### R2 JSON Data

- **Format**: Structured JSON with metadata and holdings array
- **Location**: `latest/{ticker}/holdings_enriched.json`
- **Purpose**: API consumption by public website

## Error Handling & Monitoring

### SEC Compliance

- **Rate Limiting**: Maximum 10 requests/second with proper intervals
- **Headers**: Required User-Agent, Accept headers per SEC guidelines
- **Retry Logic**: Exponential backoff for temporary failures

### Data Quality Assurance

- **Validation**: CIK format validation (10-digit with leading zeros)
- **Logging**: Comprehensive logging at INFO/DEBUG/WARNING levels
- **Error Recovery**: Stage-by-stage resilience with intermediate file persistence

### Performance Optimization

- **Caching**: Prevents duplicate API calls to OpenFIGI
- **Filtering**: Optional ticker-based filtering to reduce processing
- **Parallel Processing**: Concurrent series processing where possible

## Maintenance & Operations

### Regular Operations

1. **Daily Pipeline Execution**: Process latest N-PORT filings
2. **Summary Ticker Update**: Regenerate ticker metadata
3. **R2 Upload**: Sync latest holdings data to cloud storage
4. **Quality Monitoring**: Review enrichment success rates

### Troubleshooting

- **SEC Rate Limits**: Check for HTTP 403 responses, implement delays
- **OpenFIGI Failures**: Review CUSIP/ISIN quality, check rate limits
- **File Parsing**: Validate XML structure, handle malformed filings
- **R2 Upload**: Verify credentials, check bucket permissions

## Future Enhancements

### Pipeline Improvements

- **Incremental Processing**: Only process new/updated filings
- **Advanced Caching**: Redis-based caching for API responses
- **Data Versioning**: Track historical changes in holdings data

### Data Enrichment

- **Additional Identifiers**: LEI, FIGI, Bloomberg ID support
- **Sector Classification**: GICS/industry code enhancement
- **Price Data**: Real-time pricing integration

### Monitoring & Alerting

- **Pipeline Health**: Automated success/failure notifications
- **Data Quality Metrics**: Enrichment rate tracking
- **Performance Monitoring**: Execution time and resource usage tracking
