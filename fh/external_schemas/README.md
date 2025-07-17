# External Schema Specifications

This directory contains **TypeScript definitions and JSON schemas** for the GetFundHoldings data structures. These schemas are designed specifically for frontend applications (Next.js/React) that fetch and display fund holdings data from R2 storage.

## Schema Types vs Internal Schemas

### External Schemas (this directory)

- **Purpose**: Define TypeScript types and JSON validation for frontend applications
- **Format**: Single TypeScript definition file + JSON Schema for validation
- **Consumers**: Next.js/React applications, TypeScript frontends
- **Location**: `fh/external_schemas/`

### Internal Schemas (`fh/schemas.py`)

- **Purpose**: Data validation within Python pipeline using pandas/pandera
- **Format**: Python/Pandera schema classes
- **Consumers**: Internal data processing pipeline
- **Location**: `fh/schemas.py`

## Files in this Directory

### Primary Schema File

#### `fund-holdings-types.d.ts` - Complete TypeScript Definitions

- **Purpose**: Single TypeScript file containing all type definitions for both summary tickers and holdings data
- **Includes**:
  - Types for `SummaryTickersResponse` and `HoldingsResponse`
  - Utility functions for data manipulation and analysis
  - Type guards for runtime validation
  - Simple API client class
- **Use cases**:
  - Complete type safety for TypeScript/React frontends
  - IDE autocompletion and error checking
  - Built-in utility functions for common operations
  - Ready-to-use API client
- **Import**: Copy this single file to your frontend project

### Validation Schema Files (Optional)

#### `json_schema.json` - Holdings JSON Schema

- **Purpose**: JSON Schema specification for holdings data validation
- **Use cases**:
  - Validate API responses in testing
  - Runtime data validation
  - Document expected data structure
- **Standard**: JSON Schema Draft 07

#### `summary_tickers_schema.json` - Summary Tickers JSON Schema

- **Purpose**: JSON Schema specification for summary tickers data validation
- **Use cases**:
  - Validate ticker summary responses
  - Test data structure compliance
  - Documentation of ticker metadata format
- **Standard**: JSON Schema Draft 07

### Utility Files

#### `generator.py`

- **Purpose**: Generate and validate schemas from actual data
- **Use cases**:
  - Keep schemas in sync with actual pipeline output
  - Validate schemas against real data
  - Auto-update schemas when data structure changes

#### `__init__.py`

- **Purpose**: Package initialization and documentation
- **Contains**: Version info and package description

## Usage Examples

### Frontend Integration (Next.js/React)

```typescript
// Copy fund-holdings-types.d.ts to your frontend project
import {
  FundHoldingsClient,
  SummaryTickersResponse,
  HoldingsResponse,
  SummaryTickersUtils,
  HoldingsUtils,
} from "./types/fund-holdings-types";

// Initialize the API client
const client = new FundHoldingsClient("https://your-r2-domain.com");

// Fetch all available tickers
const summary = await client.getSummaryTickers();
const tickers = SummaryTickersUtils.sortByName(summary.tickers);

// Fetch specific fund holdings
const holdings = await client.getHoldings("IVV");

// Type-safe access to data
console.log(
  `${holdings.metadata.fund_ticker} has ${holdings.metadata.total_holdings} holdings`,
);

// Use utility functions
const topHoldings = HoldingsUtils.getTopHoldings(holdings.holdings, 10);
const enrichmentRate = HoldingsUtils.getEnrichmentRate(holdings.holdings);
```

### React Component Example

```tsx
import { useState, useEffect } from "react";
import {
  FundHoldingsClient,
  SummaryTickersResponse,
  HoldingsResponse,
  SummaryTickersUtils,
} from "./types/fund-holdings-types";

const client = new FundHoldingsClient();

export function FundSelector() {
  const [tickers, setTickers] = useState<SummaryTickersResponse | null>(null);
  const [holdings, setHoldings] = useState<HoldingsResponse | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    client.getSummaryTickers().then(setTickers);
  }, []);

  const loadHoldings = async (ticker: string) => {
    setLoading(true);
    try {
      const data = await client.getHoldings(ticker);
      setHoldings(data);
    } catch (error) {
      console.error("Failed to load holdings:", error);
    } finally {
      setLoading(false);
    }
  };

  const sortedTickers = tickers
    ? SummaryTickersUtils.sortByName(tickers.tickers)
    : [];

  return (
    <div>
      <select onChange={(e) => loadHoldings(e.target.value)} disabled={loading}>
        <option value="">Select a fund...</option>
        {sortedTickers.map((ticker) => (
          <option key={ticker.ticker} value={ticker.ticker}>
            {ticker.ticker} - {ticker.name}
          </option>
        ))}
      </select>

      {loading && <p>Loading...</p>}

      {holdings && (
        <div>
          <h2>{holdings.metadata.fund_ticker} Holdings</h2>
          <p>Total Holdings: {holdings.metadata.total_holdings}</p>
          <p>Data from: {holdings.metadata.data_timestamp}</p>
          {/* Render holdings list */}
        </div>
      )}
    </div>
  );
}
```

### Data Validation (Optional)

```typescript
import Ajv from "ajv";
import {
  isSummaryTickersResponse,
  isHoldingsResponse,
} from "./types/fund-holdings-types";
import holdingsSchema from "./json_schema.json";
import summarySchema from "./summary_tickers_schema.json";

// Runtime type checking with built-in type guards
const summaryData = await fetch("/summary_tickers.json").then((r) => r.json());
if (isSummaryTickersResponse(summaryData)) {
  // TypeScript now knows this is a valid SummaryTickersResponse
  console.log(`Found ${summaryData.metadata.total_tickers} tickers`);
}

// Optional JSON schema validation for testing
const ajv = new Ajv();
const validateHoldings = ajv.compile(holdingsSchema);
const validateSummary = ajv.compile(summarySchema);

const isValidHoldings = validateHoldings(holdingsData);
const isValidSummary = validateSummary(summaryData);
```

## R2 Storage Structure

Fund holdings data is organized in R2 as:

```
{bucket}/
├── summary_tickers.json                    # Root-level ticker listing
├── latest/
│   ├── IVV/holdings_enriched.json
│   ├── VOO/holdings_enriched.json
│   └── {fund_ticker}/holdings_enriched.json
```

### Data Flow for Frontend Applications

1. **Fetch summary tickers**: `GET /summary_tickers.json` - Get list of available funds
2. **Fetch fund holdings**: `GET /latest/{fund_ticker}/holdings_enriched.json` - Get specific fund data

## Schema Generation and Maintenance

### Auto-Generate from Data

Generate schemas from actual pipeline output:

```bash
# Analyze specific CSV file and update schemas
uv run python -m fh.external_schemas.generator --csv-file data/holdings_enriched_IVV_1100663_S000004310_20250711_20250711_143022.csv

# Validate existing schemas against data
uv run python -m fh.external_schemas.generator --csv-file data/holdings_enriched_IVV_1100663_S000004310_20250711_20250711_143022.csv --validate-only
```

### Manual Updates

1. **JSON Schema**: Edit schema files directly
2. **TypeScript**: Edit `.d.ts` files directly
3. **Test**: Run generator with `--validate-only` to check consistency

## Best Practices

### Frontend Development

1. **Use TypeScript types** for type safety and IDE support
2. **Validate responses** with JSON Schema in development/testing
3. **Handle nullable fields** (ticker enrichment may fail for some securities)
4. **Cache responses** appropriately (data typically updates quarterly)
5. **Use utility functions** provided in TypeScript definitions

### Schema Maintenance

1. **Run generator** after pipeline changes
2. **Validate schemas** against sample data
3. **Copy updated .d.ts files** to your frontend project
4. **Document changes** in git commit messages
