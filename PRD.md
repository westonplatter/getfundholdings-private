# PRD: workflow.py - End-to-End Fund Holdings Data Pipeline

## Overview

Create a formal, production-ready workflow system that orchestrates the complete data pipeline from fund CIK lookup to enriched holdings data. This replaces the incremental development approach in `main.py` with a structured, error-handling workflow system.

## Current Code Analysis

### Existing Components
- **SEC Client (`fh/sec_client.py`)**: Complete SEC EDGAR-compliant HTTP client with rate limiting, retry logic, and comprehensive series/filing data fetching
- **N-PORT Parser (`parse_nport.py`)**: Full XML parsing for N-PORT filings with holdings extraction
- **CUSIP-to-Ticker Mapper (`fh/openfigi_client.py`)**: OpenFIGI API client for enriching holdings with ticker symbols
- **Data Processing Examples (`main.py`)**: Prototype functions showing individual steps

### Data Flow Architecture
1. **CIK ’ Series/Class Data**: Fetch fund series information via SEC series lookup
2. **Series ’ N-PORT Filings**: Get all N-PORT filings for each series
3. **Filings ’ XML Data**: Download N-PORT XML files from SEC EDGAR
4. **XML ’ Holdings Data**: Parse XML to extract structured holdings information
5. **Holdings ’ Enriched Data**: Add ticker symbols via CUSIP lookup

## Requirements

### 1. Core Workflow Engine

**File**: `fh/workflow.py`

**Main Class**: `FundHoldingsWorkflow`

**Key Features**:
- Orchestrates complete end-to-end pipeline
- Configurable steps with error handling
- Progress tracking and logging
- Resumable execution for large datasets
- Data validation at each step

### 2. Pipeline Steps

#### Step 1: CIK Series Discovery
```python
def fetch_cik_series_data(self, cik: str) -> SeriesDataResult:
    """Fetch and validate all series for a given CIK"""
```
- Use `SECHTTPClient.fetch_series_data()`
- Validate series data structure
- Save to `data/series_data_{cik}.json`
- Return series IDs list

#### Step 2: N-PORT Filings Collection
```python
def collect_nport_filings(self, cik: str, series_ids: List[str]) -> FilingsResult:
    """Collect all N-PORT filings for all series"""
```
- Use `SECHTTPClient.fetch_series_filings()` per series
- Aggregate filings across all series
- Save to `data/nport_filings_{cik}.json`
- Return filing metadata

#### Step 3: XML Data Download
```python
def download_nport_xml_files(self, cik: str, filings: List[Filing]) -> DownloadResult:
    """Download N-PORT XML files for processing"""
```
- Use `SECHTTPClient.download_and_save_nport()`
- Handle rate limiting and errors
- Save to `data/nport_{cik}_{series_id}_{accession}.xml`
- Return file paths

#### Step 4: Holdings Data Extraction
```python
def extract_holdings_data(self, xml_files: List[str]) -> HoldingsResult:
    """Parse XML files and extract holdings data"""
```
- Use `parse_nport_file()` from parse_nport module
- Combine holdings from multiple files
- Save to `data/holdings_{cik}_{timestamp}.csv`
- Return consolidated DataFrame

#### Step 5: Ticker Enrichment
```python
def enrich_with_tickers(self, holdings_df: pd.DataFrame) -> EnrichedResult:
    """Add ticker symbols to holdings data"""
```
- Use `OpenFIGIClient.add_tickers_to_dataframe()`
- Handle API rate limiting
- Save to `data/holdings_enriched_{cik}_{timestamp}.csv`
- Return enriched DataFrame

### 3. Configuration System

**File**: `fh/workflow_config.py`

```python
@dataclass
class WorkflowConfig:
    cik_list: List[str]
    data_dir: str = "data"
    max_concurrent_downloads: int = 3
    enable_ticker_enrichment: bool = True
    resumable: bool = True
    validate_xml: bool = True
```

### 4. Error Handling & Resilience

**Features**:
- Exponential backoff for SEC API calls
- Retry logic for failed downloads
- Graceful handling of malformed XML
- Checkpoint system for resumable execution
- Comprehensive logging for debugging

### 5. Data Models

**File**: `fh/data_models.py`

```python
@dataclass
class SeriesInfo:
    cik: str
    series_id: str
    classes: List[Dict]
    
@dataclass
class Filing:
    cik: str
    series_id: str
    accession_number: str
    filing_date: str
    form_type: str
    
@dataclass
class WorkflowResult:
    cik: str
    total_series: int
    total_filings: int
    total_holdings: int
    enriched_holdings: int
    execution_time: float
```

### 6. Usage Interface

**Primary Interface**:
```python
from fh.workflow import FundHoldingsWorkflow
from fh.workflow_config import WorkflowConfig

config = WorkflowConfig(
    cik_list=["1100663"],  # iShares
    enable_ticker_enrichment=True
)

workflow = FundHoldingsWorkflow(config)
results = workflow.run()
```

**CLI Interface**:
```bash
uv run python -m fh.workflow --cik 1100663 --enrich-tickers
```

## Implementation Plan

### Phase 1: Core Workflow Structure
1. Create `fh/workflow.py` with main workflow class
2. Implement configuration system
3. Add basic error handling and logging
4. Create data models for type safety

### Phase 2: Pipeline Implementation
1. Implement each workflow step as separate method
2. Add checkpoint system for resumability
3. Integrate existing client classes
4. Add progress tracking

### Phase 3: Enhanced Features
1. Add concurrent processing for downloads
2. Implement data validation
3. Add CLI interface
4. Create comprehensive testing

### Phase 4: Production Readiness
1. Add monitoring and metrics
2. Implement data quality checks
3. Add documentation
4. Performance optimization

## Technical Considerations

### SEC Compliance
- Maintain 10 requests/second rate limit
- Use proper User-Agent headers
- Implement exponential backoff
- Log all requests for compliance

### Data Storage
- Use consistent naming conventions
- Implement data versioning
- Add metadata to output files
- Support resume from checkpoints

### Error Handling
- Distinguish between retryable and permanent errors
- Log detailed error information
- Provide meaningful error messages
- Support partial completion

### Performance
- Batch API calls where possible
- Use connection pooling
- Implement caching for repeated data
- Monitor memory usage for large files

## Success Metrics

1. **Reliability**: 99%+ success rate for complete workflows
2. **Performance**: Process 1000+ holdings per minute
3. **Compliance**: Zero SEC rate limit violations
4. **Resumability**: Ability to resume from any checkpoint
5. **Data Quality**: 95%+ ticker enrichment success rate

## Migration Path

1. **Phase 1**: Run alongside existing `main.py` for validation
2. **Phase 2**: Migrate incremental functions to workflow system
3. **Phase 3**: Replace `main.py` with workflow-based approach
4. **Phase 4**: Add production monitoring and alerts

This PRD provides a comprehensive plan for transforming the existing prototype code into a production-ready, end-to-end workflow system that reliably processes fund holdings data from CIK to enriched holdings.