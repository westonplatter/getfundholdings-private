# PostgreSQL Data Pipeline Guide

## Overview

This guide covers the PostgreSQL-based fund holdings data pipeline that extracts, processes, and enriches SEC filing data to create structured investment holdings datasets. The pipeline consists of 5 stages orchestrated by `fh/workflow_postgres.py`.

## Pipeline Architecture

```
CIK → Series/Class → N-PORT Filings → XML Data → Holdings → Enriched Artifacts
```

**5-Stage Workflow:**
1. **Series Discovery**: Extract fund series/class data from SEC submissions API
2. **Filing Collection**: Gather N-PORT XML filing metadata for each series  
3. **XML Download**: Retrieve actual N-PORT XML files from SEC EDGAR
4. **Holdings Extraction**: Parse XML to extract structured investment holdings data
5. **Ticker Enrichment**: Enhance holdings with ticker symbols via OpenFIGI API

**Output Artifacts:**
- `holdings_raw_{cik}_{series_id}_{report_date}_{timestamp}.csv` - Raw extracted holdings data
- `holdings_enriched_{cik}_{series_id}_{report_date}_{timestamp}.csv` - Holdings enhanced with ticker symbols

## Prerequisites

### 1. Database Setup
```bash
# Run database migrations (first time setup)
ENVIRONMENT=prod uv run python -m alembic upgrade head

# Or for development environment
ENVIRONMENT=dev uv run python -m alembic upgrade head
```

### 2. Environment Configuration
Ensure you have the appropriate environment file (`.env.prod` or `.env.dev`) with:
- `SUPABASE_DATABASE_URL` - PostgreSQL connection string
- `OPENFIGI_API_KEY` - OpenFIGI API key for ticker enrichment

### 3. Security Mapping Cache (Optional)
Load existing CUSIP/ISIN to ticker mappings for better performance:
```bash
# Load bulk cache from JSON file
uv run python load_cusip_cache.py --bulk -e prod cusip_ticker_cache.json
```

## CLI Reference

### Basic Commands

**View Available Providers:**
```bash
uv run python -m fh.workflow_postgres --summary-only -e prod
```

**Process All ETFs for a Provider:**
```bash
uv run python -m fh.workflow_postgres --provider invesco --enable-all-stages -e prod
```

**Process Single ETF Ticker:**
```bash
uv run python -m fh.workflow_postgres --provider invesco --ticker SPY --enable-all-stages -e prod
```

### Command Line Options

| Option | Description | Example |
|--------|-------------|---------|
| `--provider`, `-p` | Filter by provider name (supports regex) | `--provider "invesco"` |
| `--ticker`, `-t` | Filter by specific ticker symbol | `--ticker SPY` |
| `-e`, `--env` | Environment (dev/prod) | `-e prod` |
| `--enable-all-stages` | Run complete pipeline (all 5 stages) | |
| `--summary-only` | Show provider summary, don't process | |
| `--max-filings` | Limit filings per series | `--max-filings 1` |

### Individual Stage Control

| Option | Description |
|--------|-------------|
| `--disable-filing-discovery` | Skip Stage 2: SEC filing discovery |
| `--enable-xml-download` | Enable Stage 3: Download XML files |
| `--enable-holdings-processing` | Enable Stage 4: Extract holdings data |
| `--enable-ticker-enrichment` | Enable Stage 5: Enrich with ticker symbols |

## Common Workflows

### 1. Complete Pipeline - Single Provider (e.g., Invesco)

**Process All Invesco ETFs:**
```bash
uv run python -m fh.workflow_postgres --provider invesco --enable-all-stages -e prod
```

This will:
- Find all CIKs associated with "Invesco" provider
- Discover all series/classes for each CIK  
- Collect N-PORT filings for each series
- Download XML files for all filings
- Extract holdings data from XML files
- Enrich holdings with ticker symbols
- Generate enriched CSV files for each fund

**Expected Output:**
```
=== Fund Provider Summary ===
Total Providers: 17
Total Active CIKs: 40

CIKs by Provider:
  Invesco: 4 CIKs

=== Processing 4 CIKs ===
Processing CIK 0001100663: Invesco Exchange-Traded Fund Trust (Provider: Invesco)
  └─ Found 350 series for CIK 0001100663
  └─ Stage 2: Discovering SEC filings for 350 series
    │ Series S000004310: 15 filings discovered
  └─ Stage 3: Downloading XML files
    │ Downloaded 523 XML files
  └─ Stage 4: Processing XML files  
    │ Processed 523 XML files
  └─ Stage 5: Enriching Holdings with Tickers
    │ Enriched 523 holdings files
```

### 2. Complete Pipeline - Single ETF (e.g., SPY)

**Process Only SPY ETF:**
```bash
uv run python -m fh.workflow_postgres --provider invesco --ticker SPY --enable-all-stages -e prod
```

This will:
- Filter to only series/classes with ticker "SPY"
- Run complete 5-stage pipeline for just that ETF
- Generate holdings data specifically for SPY

**Expected Output:**
```
Processing CIK 0001100663: Invesco Exchange-Traded Fund Trust (Provider: Invesco)
  └─ Found 350 series for CIK 0001100663
  └─ Filtered to 1 series with ticker: SPY
    │ Found series S000004310 with ticker SPY
  └─ Stage 2: Discovering SEC filings for 1 series
    │ Series S000004310: 15 filings discovered
  └─ Enriched 1 holdings files
```

### 3. Staged Pipeline Execution

For large providers, you may want to run stages separately:

**Stage 1-2: Discovery and Filing Collection**
```bash
uv run python -m fh.workflow_postgres --provider blackrock -e prod
```

**Stage 3: Download XML Files**
```bash
uv run python -m fh.workflow_postgres --provider blackrock --enable-xml-download -e prod
```

**Stage 4: Process XML to Holdings**
```bash
uv run python -m fh.workflow_postgres --provider blackrock --enable-holdings-processing -e prod
```

**Stage 5: Enrich with Tickers**
```bash
uv run python -m fh.workflow_postgres --provider blackrock --enable-ticker-enrichment -e prod
```

### 4. Development and Testing

**Test with Limited Data:**
```bash
# Process only 1 filing per series for faster testing
uv run python -m fh.workflow_postgres --provider simplify --max-filings 1 --enable-all-stages -e dev
```

**Process Specific Provider Pattern:**
```bash
# Use regex to match multiple providers
uv run python -m fh.workflow_postgres --provider "vanguard|blackrock" --enable-all-stages -e prod
```

## Database Status Monitoring

### Check Processing Status
```bash
# View summary of SEC reports in database
uv run python -c "
from fh.workflow_postgres import FundHoldingsWorkflowPostgres, WorkflowPostgresConfig
config = WorkflowPostgresConfig(environment='prod')
workflow = FundHoldingsWorkflowPostgres(config)
workflow.print_sec_reports_summary()
"
```

### Check Provider Data
```bash
# View all available providers and CIK counts
uv run python -m fh.workflow_postgres --summary-only -e prod
```

## Ticker Enrichment Details

The pipeline uses an **optimized cache-first approach** for ticker enrichment:

### Enrichment Order
1. **CUSIP Cache Lookup** - Check database cache for all CUSIP identifiers
2. **ISIN Cache Lookup** - Check database cache for remaining holdings with ISIN identifiers  
3. **CUSIP API Calls** - Make OpenFIGI API calls for remaining CUSIP identifiers
4. **ISIN API Calls** - Make OpenFIGI API calls for remaining ISIN identifiers

### Expected Enrichment Output
```
│ Starting optimized ticker enrichment for 15,420 holdings
│ CUSIP cache hits: 12,336
│ ISIN cache hits: 1,892  
│ CUSIP API calls: 967
│ ISIN API calls: 225
│ Enriched 15,195/15,420 holdings (98.5%) → holdings_enriched_SPY_...csv
```

### Benefits
- **Minimizes API calls** by exhausting cached data first
- **Better performance** since database lookups are faster than API calls
- **Rate limiting compliance** with OpenFIGI API constraints
- **Detailed logging** shows cache hits vs API calls at each step

## File Output Structure

### Directory Layout
```
data/
├── series_data_0001100663.json                    # Stage 1: Series discovery
├── series_filings_0001100663_S000004310_nport-p.json  # Stage 2: Filing metadata
├── nport_0001100663_S000004310_0001100663_25_119791.xml   # Stage 3: Downloaded XML
├── holdings_raw_0001100663_S000004310_20241231_20250123_143022.csv    # Stage 4: Extracted holdings
└── holdings_enriched_0001100663_S000004310_20241231_20250123_143155.csv # Stage 5: Enriched holdings
```

### CSV Output Columns

**Raw Holdings (`holdings_raw_*.csv`):**
- Basic security information (name, cusip, isin, value, etc.)
- Portfolio allocation percentages
- Security classification data

**Enriched Holdings (`holdings_enriched_*.csv`):**
- All raw holdings columns
- `ticker` - Exchange ticker symbol (when available)
- `enrichment_datetime` - When enrichment was performed
- `enrichment_notes` - Data quality notes (missing identifiers, derivative instruments, etc.)

## Error Handling and Troubleshooting

### Common Issues

**1. Database Connection Errors**
```bash
# Check database connectivity
uv run python -c "
from fh.config_utils import load_environment_config, get_database_url_from_env
load_environment_config('prod')
print('Database URL loaded successfully')
"
```

**2. XML Download Failures**
The pipeline includes automatic discovery of XML filenames to handle SEC filing variations. If downloads fail:
- Check SEC EDGAR system status
- Verify User-Agent compliance in `fh/sec_client.py`
- Monitor rate limiting (10 requests/second max)

**3. Empty Results**
```bash
# Check if provider name matches database records
uv run python -m fh.workflow_postgres --summary-only -e prod | grep -i "your_provider"
```

### Performance Optimization

**For Large Providers (BlackRock, Vanguard):**
- Use staged execution to monitor progress
- Consider `--max-filings 1` for initial testing
- Run during off-peak hours for SEC API compliance

**For Development:**
- Use `-e dev` environment for testing
- Process single tickers first: `--ticker SPY`
- Use `--summary-only` to verify data before processing

## Integration with File-Based Workflow

For comparison or fallback, you can still use the original file-based workflow:

```bash
# File-based approach (no database required)
uv run python -m fh.workflow --issuer simplify --ticker AGGH --max-filings 1
```

The PostgreSQL-based workflow provides:
- **Better error recovery** - Resume from any stage
- **Status tracking** - Monitor progress in database
- **Scalability** - Handle large provider datasets efficiently
- **Audit trail** - Complete history of data changes
- **Multi-environment** - Separate dev/prod processing

## Next Steps

1. **Monitor Processing**: Use database status commands to track pipeline progress
2. **Validate Output**: Review generated CSV files for data quality
3. **Scale Up**: After testing with single ETFs, process entire providers
4. **Automate**: Consider scheduling regular pipeline runs for fresh data
5. **Extend**: Add custom enrichment steps or output formats as needed