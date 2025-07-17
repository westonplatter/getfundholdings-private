# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **fund holdings data pipeline** - a private, closed-source Python workflow system that extracts, processes, and enriches SEC filing data to create structured investment holdings datasets. It's the third component of the GetFundHoldings.com ecosystem, complementing the public website and open-source download scripts.

### Core Pipeline Architecture

The system implements a **5-stage data workflow** orchestrated by `fh/workflow.py`:

```
CIK → Series/Class → N-PORT Filings → XML Data → Holdings → Enriched Artifacts
```

**Pipeline Stages:**
1. **Series Discovery**: Extract fund series/class data from SEC submissions API
2. **Filing Collection**: Gather N-PORT XML filing metadata for each series
3. **XML Download**: Retrieve actual N-PORT XML files from SEC EDGAR
4. **Holdings Extraction**: Parse XML to extract structured investment holdings data
5. **Ticker Enrichment**: Enhance holdings with ticker symbols via OpenFIGI API

**Output Artifacts:**
- `holdings_{cik}_{timestamp}.csv` - Raw extracted holdings data
- `holdings_enriched_{cik}_{timestamp}.csv` - Holdings enhanced with ticker symbols

## Primary Workflow Usage

**Core Interface** (`fh/workflow.py`):
```python
from fh.workflow import FundHoldingsWorkflow, WorkflowConfig

config = WorkflowConfig(
    cik_list=["1100663"],  # Target fund companies
    enable_ticker_enrichment=True,
    interested_etf_tickers=["IVV"]  # Optional filtering
)

workflow = FundHoldingsWorkflow(config)
results = workflow.run()  # Execute complete pipeline
```

**Entry Points:**
- `uv run python -m fh.workflow` - CLI execution
- `uv run python main.py` - Legacy interface
- Direct import for programmatic use

## R2 Upload & Multi-Environment Support

### Summary Ticker Generation & Upload
- `create_summary_tickers.py` - Generates summary_tickers.json with all ETF metadata using same logic as R2Client
- Supports multi-environment upload via `-e dev|prod` flag using `.env.dev` and `.env.prod` files
- `fh/r2_client.py` supports environment-based bucket selection and summary ticker upload

### Environment Configuration
- `.env.dev` and `.env.prod` files contain environment-specific R2 credentials and bucket names
- R2Client automatically loads appropriate environment file based on bucket parameter
- CLI tools use `-e/--env` flag for consistent environment selection across scripts

## Data Enhancement Pipeline

### Ticker Symbol Enrichment
The pipeline implements **dual-identifier ticker lookup** via OpenFIGI API:

1. **Primary**: CUSIP-based ticker lookup for US securities
2. **Fallback**: ISIN-based ticker lookup for international securities

**Enhancement Logic:**
```python
# Step 1: CUSIP lookup (US domestic securities)
enriched_df = openfigi_client.add_tickers_to_dataframe_by_cusip(holdings_df, 'cusip')

# Step 2: ISIN fallback (international securities traded on US exchanges)
failed_cusip_holdings = enriched_df[enriched_df['ticker'].isna()]
isin_enriched = openfigi_client.add_tickers_to_dataframe_by_isin(
    failed_cusip_holdings, 'isin'
)
```

This approach maximizes ticker coverage for both domestic and international holdings.

### Modular Enrichment Architecture
The enrichment process uses a **modular design pattern** in `fh/workflow.py` for maintainability:

**Parent Method:** `enrich_holdings()` - Coordinates all enrichment steps  
**Child Methods:**
- `_extract_file_metadata()` - Extracts metadata from filename/DataFrame
- `_enrich_metadata()` - Adds CIK, series, report date metadata  
- `_enrich_timestamps()` - Adds enrichment timestamps
- `_enrich_tickers()` - Ticker lookup (CUSIP → ISIN fallback)
- `_enrich_notes()` - Data quality notes (`enrichment_notes` column)

**Key Benefits:**
- Each enrichment type is isolated and testable
- Easy to add new enrichment steps or modify existing ones
- Automatic derivative instrument detection (ELNs, swaps) - excludes from ticker lookup
- Data quality tracking via structured notes

**Data Quality Notes Added:**
- `derivative_instrument` - ELNs, total return swaps, etc.
- `missing_cusip/isin/ticker` - Missing identifier flags

## Python Development Commands

- Run Python commands: `uv run python <script>`
- Execute workflow: `uv run python -m fh.workflow`
- Debug ISIN lookups: `uv run python debug_isin_lookup.py`
- Project uses pyproject.toml for dependency management

## SEC EDGAR API Requirements

**CRITICAL COMPLIANCE REQUIREMENTS** - Non-compliance results in immediate IP blocking:

### Mandatory Headers
- **User-Agent**: Must be in format `"Company Name email@domain.com"`
- **Accept**: `application/json`
- **Accept-Encoding**: `gzip, deflate`
- **Connection**: `keep-alive`

### Rate Limiting
- **10 requests per second maximum** - strictly enforced
- Implement minimum 0.1 second interval between requests
- Use exponential backoff for retry logic
- Monitor for HTTP 403 "Request Rate Threshold Exceeded" responses

### Legal Compliance
- Subject to Computer Fraud and Abuse Act of 1986
- Enhanced monitoring since July 2021
- Violations can result in prosecution for unauthorized computer access
- Maintain detailed access logs

## Data Processing Requirements

### N-PORT XML Processing
- Handle up to 500,000 investment entries per filing
- Process security identifiers: CUSIPs, ISINs, LEIs
- Extract market values, percentage allocations, security classifications
- Support complex nested XML structures per N-PORT Technical Specification v2.0

### OpenFIGI Ticker Enhancement
- Rate limited to 25 requests/minute (without API key)
- Dual-identifier support: CUSIP and ISIN lookups
- Caching system prevents duplicate API calls
- Filters for US exchanges and equity securities

## Architecture Notes

### Pipeline Orchestration
- **Stateful Processing**: Each stage saves intermediate artifacts to `data/` directory
- **Error Resilience**: Individual stage failures don't break entire pipeline
- **Resumable Execution**: Can restart from any completed stage
- **Configurable Limits**: Support for partial processing (max series, max filings)

### Data Flow Patterns
- **SEC Compliance**: All HTTP clients implement proper rate limiting and headers
- **File-Based Persistence**: JSON for metadata, CSV for tabular holdings data
- **Comprehensive Logging**: Detailed execution logs for debugging and compliance

### Common Pitfalls to Avoid
- Using default library User-Agent strings (e.g., "Python-requests")
- Failing to implement proper rate limiting for both SEC and OpenFIGI APIs
- Malformed CIK formatting (must be 10-digit with leading zeros)
- Caching null/failed API responses (now prevented)