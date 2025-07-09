  # PRD: OpenFIGI ISIN to Ticker Lookup

## Overview

Extend the existing OpenFIGI client (`fh/openfigi_client.py`) to support ISIN (International Securities Identification Number) to ticker symbol lookups for US stocks, complementing the existing CUSIP lookup functionality.

## Requirements Implemented

### 1. Core ISIN Lookup Methods

#### `get_ticker_from_isin(isin: str) -> Optional[str]`
- Validates 12-character ISIN format
- Utilizes existing cache infrastructure 
- Returns US stock ticker symbol or None

#### `get_multiple_tickers_from_isins(isins: List[str]) -> Dict[str, Optional[str]]`
- Batch processing for multiple ISIN lookups
- Progress logging for large datasets
- Returns mapping dictionary of ISIN → ticker

#### `add_tickers_to_dataframe_by_isin(df: pd.DataFrame, isin_column: str = 'isin') -> pd.DataFrame`
- Pandas DataFrame integration
- Automatic deduplication of ISIN values
- Success rate reporting and missing ISIN logging

### 2. API Implementation Details

#### Request Format
```python
payload = [{
    "idType": "ID_ISIN",
    "idValue": isin,
    "exchCode": "US",           # Focus on US exchanges
    "securityType2": "Equity"   # Focus on equity securities
}]
```

#### Response Parsing
- Filters for `marketSector` in ['Equity', 'Corp']
- Validates `exchCode` == 'US' for US stock focus
- Extracts ticker symbol from OpenFIGI response

### 3. Rate Limiting & Caching

#### Existing Infrastructure Utilized
- **Rate Limiting**: 25 requests/minute (2.4 second intervals)
- **Caching**: JSON file-based cache (`cusip_ticker_cache.json`)
- **Error Handling**: Exponential backoff for 429 responses
- **Retry Logic**: 3 attempts with progressive delays

#### Cache Strategy
- Unified cache supports both CUSIP and ISIN identifiers
- Caches negative results to avoid repeated failed API calls
- Automatic cache persistence after each lookup

### 4. Integration Patterns

#### Compatible with Existing Workflow
```python
from fh.openfigi_client import OpenFIGIClient

# Initialize client (same as before)
client = OpenFIGIClient()

# ISIN-based lookups
ticker = client.get_ticker_from_isin("US0378331005")  # AAPL
multiple_tickers = client.get_multiple_tickers_from_isins(["US0378331005", "US5949181045"])

# DataFrame integration
holdings_df = client.add_tickers_to_dataframe_by_isin(df, isin_column='isin')
```

#### Data Source Compatibility
- N-PORT filings often contain both CUSIP and ISIN identifiers
- Provides fallback option when CUSIP lookups fail
- Maintains same output format as CUSIP-based methods

### 5. Error Handling

#### Input Validation
- **ISIN Format**: Validates 12-character string format
- **Type Checking**: Handles non-string and NaN values gracefully
- **Column Validation**: Verifies ISIN column existence in DataFrames

#### API Error Management
- **HTTP 429**: Rate limit handling with exponential backoff
- **HTTP 404**: Graceful handling of unknown ISINs
- **Network Errors**: Retry logic with progressive delays
- **Parsing Errors**: Comprehensive exception handling

### 6. Logging & Monitoring

#### Progress Tracking
```
INFO: Processing ISIN 1/150: US0378331005
DEBUG: Found ticker AAPL for ISIN US0378331005
INFO: Found tickers for 142/150 holdings (94.7%)
WARNING: Could not find tickers for 8 ISINs: [...]
```

#### Cache Performance
- Cache hit/miss logging for performance monitoring
- Cache statistics via `get_cache_stats()` method
- Automatic cache size management

### 7. Use Cases

#### Primary Use Case: Fund Holdings Enrichment
```python
# Parse N-PORT filing
holdings_df, fund_info = parse_nport_file(xml_file)

# Try CUSIP lookup first
holdings_with_cusip_tickers = client.add_tickers_to_dataframe(holdings_df, 'cusip')

# For failed CUSIP lookups, try ISIN lookup
failed_cusip_mask = holdings_with_cusip_tickers['ticker'].isna()
failed_cusip_holdings = holdings_with_cusip_tickers[failed_cusip_mask]

if not failed_cusip_holdings.empty and 'isin' in failed_cusip_holdings.columns:
    failed_cusip_holdings = client.add_tickers_to_dataframe_by_isin(
        failed_cusip_holdings, 'isin'
    )
    # Merge back results
    holdings_with_cusip_tickers.update(failed_cusip_holdings)
```

#### International Securities Support
- Extends coverage beyond US-only CUSIP system
- Provides unified lookup interface for global identifiers
- Maintains focus on US exchange listings

### 8. Technical Specifications

#### Dependencies
- **Existing**: Same dependencies as CUSIP implementation
  - `requests` for HTTP client
  - `pandas` for DataFrame operations
  - `loguru` for logging
- **No Additional**: No new dependencies required

#### Performance Characteristics
- **Rate Limit**: 25 requests/minute maximum
- **Cache Performance**: O(1) lookup for cached ISINs  
- **Memory Usage**: Minimal additional overhead
- **API Response Time**: ~200-500ms per request

#### File Structure
```
fh/openfigi_client.py
├── get_ticker_from_isin()
├── _fetch_ticker_from_api_isin()
├── get_multiple_tickers_from_isins()
├── add_tickers_to_dataframe_by_isin()
└── [existing CUSIP methods unchanged]
```

### 9. Validation & Testing

#### Manual Testing Approach
```python
# Test known US stock ISINs
test_isins = [
    "US0378331005",  # Apple Inc
    "US5949181045",  # Microsoft Corp  
    "US6174464486",  # NVIDIA Corp
]

for isin in test_isins:
    ticker = client.get_ticker_from_isin(isin)
    print(f"ISIN {isin} → {ticker}")
```

#### Success Criteria
- **Accuracy**: >90% success rate for major US equity ISINs
- **Performance**: Maintains existing rate limit compliance
- **Integration**: No disruption to existing CUSIP functionality
- **Caching**: Effective cache hit rates reducing API calls

### 10. Future Enhancements

#### Potential Improvements
1. **Batch API Requests**: OpenFIGI supports up to 100 identifiers per request
2. **International Exchanges**: Expand beyond US exchanges  
3. **Additional Security Types**: Support bonds, options, futures via ISIN
4. **Performance Optimization**: Implement connection pooling

#### Migration Considerations
- **Backward Compatibility**: All existing CUSIP functionality preserved
- **Cache Migration**: Existing cache structure supports mixed identifiers
- **API Limits**: No changes to rate limiting or authentication requirements

## Implementation Status

✅ **Core Methods**: ISIN lookup methods implemented  
✅ **API Integration**: OpenFIGI mapping endpoint utilized  
✅ **Caching**: Unified cache supporting both CUSIP and ISIN  
✅ **DataFrame Integration**: Pandas-compatible ISIN processing  
✅ **Error Handling**: Comprehensive validation and retry logic  
✅ **Documentation**: Method signatures and usage patterns documented

## Usage Summary

The ISIN lookup functionality extends the existing OpenFIGI client with minimal changes, maintaining the same patterns and reliability as the CUSIP implementation while expanding identifier coverage for international securities trading on US exchanges.