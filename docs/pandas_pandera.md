# Pandas & Pandera Standards Practice

This document outlines our approach to DataFrame validation and schema management using Pandas and Pandera in the fund holdings data pipeline.

## Overview

We use Pandera to define and validate DataFrame schemas for structured data validation throughout the pipeline. This ensures data quality, type safety, and consistent structure across all processing stages.

## Schema Architecture

### Two-Tier Schema Design

```python
class HoldingsRawSchema(pa.DataFrameModel):
    """Schema for raw holdings data from N-PORT filings"""
    # 24 columns covering all N-PORT extracted data

class HoldingsEnrichedSchema(HoldingsRawSchema):
    """Schema for enriched holdings with ticker data"""
    # Inherits all raw fields + enrichment fields
    ticker: Series[str] = pa.Field(nullable=True)
    enrichment_datetime: Series[datetime] = pa.Field()
```

**Benefits:**
- Inheritance reduces duplication
- Clear separation between raw and enriched data
- Easy to extend with additional enrichment stages

### Field Type Principles

#### Datetime Fields
- **Use timezone-aware datetime objects** for timestamps
- **Store in UTC** for consistency across environments
- **Let pandas handle serialization** to ISO 8601 format

```python
# Good: Timezone-aware datetime
enrichment_datetime: Series[datetime] = pa.Field(description="UTC timestamp")
enriched_df['enrichment_datetime'] = datetime.now(timezone.utc)

# Avoid: String timestamps
enrichment_datetime: Series[str] = pa.Field()  # Less type-safe
```

#### Nullable Fields
- **Use `nullable=True`** in Field definition, not `Optional[Type]` in Series annotation
- **Specify base type** in Series annotation

```python
# Good: Proper nullable syntax
cusip: Series[str] = pa.Field(nullable=True, description="CUSIP identifier")

# Avoid: Optional in type annotation
cusip: Series[Optional[str]] = pa.Field(nullable=True)  # Causes pandera errors
```

#### Numeric Fields
- **Avoid value constraints** for external data sources (SEC filings)
- **Focus on type validation** rather than business rule validation
- **Allow negative values** for financial data (short positions, derivatives)

```python
# Good: Type-only validation
value_usd: Series[float] = pa.Field(description="Market value in USD")

# Avoid: Value constraints on external data
value_usd: Series[float] = pa.Field(ge=0)  # SEC data may have negatives
```

#### String Fields
- **No value constraints** on regulatory data
- **Document expected values** in descriptions only

```python
# Good: Flexible string validation
is_restricted_security: Series[str] = pa.Field(description="Restricted security flag")

# Avoid: Constraining SEC values
is_restricted_security: Series[str] = pa.Field(isin=["Y", "N"])  # SEC may use other values
```

## Validation Strategy

### When to Validate
- **Always validate** DataFrames returned from parsing functions
- **Validate before** writing to files
- **Validate after** loading from files (optional, for debugging)

### Validation Functions
```python
def validate_holdings_raw(df: pd.DataFrame) -> pd.DataFrame:
    """Validate raw holdings against schema"""
    return HoldingsRawSchema.validate(df)

def validate_holdings_enriched(df: pd.DataFrame) -> pd.DataFrame:
    """Validate enriched holdings against schema"""
    return HoldingsEnrichedSchema.validate(df)
```

### Error Handling
- **Log validation errors** but continue processing when possible
- **Return empty DataFrames** on critical validation failures
- **Use specific error messages** for debugging

```python
try:
    validated_df = validate_holdings_raw(df)
except pa.errors.SchemaError as e:
    logger.warning(f"Validation failed: {e}")
    return pd.DataFrame()  # Return empty on failure
```

## Type Annotations

### Function Signatures
```python
from fh.schemas import HoldingsRawDF, HoldingsEnrichedDF

def parse_nport_file(xml_file_path: str) -> tuple[HoldingsRawDF, Dict[str, Any]]:
    """Returns validated raw holdings DataFrame"""
    
def enrich_with_tickers(holdings_df: HoldingsRawDF) -> HoldingsEnrichedDF:
    """Takes raw holdings, returns enriched holdings"""
```

### Benefits of Type Annotations
- **IDE support** with autocomplete and type checking
- **Documentation** of expected DataFrame structure
- **Runtime validation** when combined with pandera

## Configuration

### Schema Configuration
```python
class Config:
    strict = True      # Fail on unexpected columns
    coerce = True      # Attempt type coercion
```

**Rationale:**
- `strict=True`: Catches schema drift early
- `coerce=True`: Handles string→numeric conversions from XML/CSV

## File Format Compatibility

### CSV Export/Import
- **Pandas handles timezone serialization** automatically
- **Use `quoting=1` (QUOTE_ALL)** for consistent CSV format
- **Preserve index=False** for clean CSVs

```python
df.to_csv(filename, index=False, quoting=1)
```

### Parquet Support
- **Native datetime with timezone support**
- **Better type preservation** than CSV
- **Smaller file sizes** for large datasets

## Metadata Columns

### Required Metadata
Every DataFrame should include:
- `source_file`: Originating XML filename
- `report_period_date`: N-PORT report date
- `enrichment_datetime`: Processing timestamp (for enriched data)

### Metadata Principles
- **Add metadata early** in the pipeline
- **Preserve through transformations**
- **Use for debugging and traceability**

## Example Implementation

### Raw Holdings Parser
```python
def to_dataframes(self) -> tuple[HoldingsRawDF, Dict[str, Any]]:
    """Parse XML and return validated holdings DataFrame"""
    holdings_df = pd.DataFrame(holdings_data)
    
    # Add required metadata
    holdings_df['source_file'] = os.path.basename(self.xml_file_path)
    holdings_df['report_period_date'] = fund_info.get('report_period_date', '')
    
    # Validate against schema
    holdings_df = validate_holdings_raw(holdings_df)
    
    return holdings_df, fund_info
```

### Enrichment Process
```python
def enrich_with_tickers(holdings_df: HoldingsRawDF) -> HoldingsEnrichedDF:
    """Enrich raw holdings with ticker data"""
    enriched_df = holdings_df.copy()
    enriched_df['ticker'] = None
    enriched_df['enrichment_datetime'] = datetime.now(timezone.utc)
    
    # Perform ticker lookup...
    
    # Validate enriched result
    enriched_df = validate_holdings_enriched(enriched_df)
    
    return enriched_df
```

## Common Patterns

### Schema Evolution
- **Add new fields** to enriched schema as needed
- **Maintain backward compatibility** with raw schema
- **Version schemas** if breaking changes needed

### Performance Considerations
- **Validate once per processing stage** (not repeatedly)
- **Use lazy validation** for large DataFrames when possible
- **Consider chunked processing** for very large datasets

### Testing
- **Test with real SEC data** to catch edge cases
- **Validate schema assumptions** against actual N-PORT content
- **Test both valid and invalid data scenarios**

## Current Schema Status

### HoldingsRawSchema (24 columns)
- Security identification: name, lei, title, cusip, isin, other_id, other_id_desc
- Position data: balance, units, currency, value_usd, percent_value
- Classification: payoff_profile, asset_category, issuer_category, investment_country
- Regulatory flags: is_restricted_security, fair_value_level, is_cash_collateral, is_non_cash_collateral, is_loan_by_fund
- Loan data: loan_value
- Metadata: source_file, report_period_date

### HoldingsEnrichedSchema (26 columns)
- All raw schema fields
- Enrichment: ticker, enrichment_datetime

## Notes for Further Discussion

<!-- Add comments and feedback here for refinement -->