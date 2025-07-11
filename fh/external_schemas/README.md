# External Schema Specifications

This directory contains **external schema specifications** for the GetFundHoldings API. These schemas define the JSON structure for data consumed by frontend applications, third-party integrations, and public APIs.

## Schema Types vs Internal Schemas

### External Schemas (this directory)
- **Purpose**: Define JSON API contracts for external consumption
- **Format**: JSON, TypeScript, OpenAPI specifications
- **Consumers**: Frontend applications, third-party developers, API clients
- **Location**: `fh/external_schemas/`

### Internal Schemas (`fh/schemas.py`)
- **Purpose**: Data validation within Python pipeline using pandas/pandera
- **Format**: Python/Pandera schema classes
- **Consumers**: Internal data processing pipeline
- **Location**: `fh/schemas.py`

## Files in this Directory

### Core Schema Files

#### `json_schema.json`
- **Purpose**: JSON Schema specification for data validation
- **Use cases**: 
  - Validate API responses in testing
  - Generate validation code for client applications
  - Document expected data structure
- **Standard**: JSON Schema Draft 07

#### `typescript.d.ts`
- **Purpose**: TypeScript type definitions for frontend applications
- **Use cases**:
  - Type safety in TypeScript/JavaScript frontends
  - IDE autocompletion and error checking
  - Generate typed API clients
- **Import**: Copy to your frontend project and import types

#### `openapi.yaml`
- **Purpose**: OpenAPI 3.0 specification for API documentation
- **Use cases**:
  - Generate interactive API documentation (Swagger UI)
  - Generate client SDKs in multiple languages
  - API testing and validation
- **Standard**: OpenAPI 3.0.3

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

### Frontend Integration (TypeScript)

```typescript
// Copy typescript.d.ts to your frontend project
import { HoldingsResponse, Holding, HoldingsUtils } from './types/holdings';

// Fetch holdings data
const response = await fetch('https://your-r2-domain.com/latest/IVV/holdings_enriched.json');
const data: HoldingsResponse = await response.json();

// Type-safe access to data
console.log(`${data.metadata.fund_ticker} has ${data.metadata.total_holdings} holdings`);

// Use utility functions
const topHoldings = HoldingsUtils.getTopHoldings(data.holdings, 10);
const enrichmentRate = HoldingsUtils.getEnrichmentRate(data.holdings);
```

### API Documentation

```bash
# Generate Swagger UI documentation
swagger-ui-serve openapi.yaml

# Generate client SDK
openapi-generator generate -i openapi.yaml -g typescript-fetch -o ./generated-client
```

### Data Validation (Node.js)

```javascript
const Ajv = require('ajv');
const schema = require('./json_schema.json');

const ajv = new Ajv();
const validate = ajv.compile(schema);

// Validate API response
const isValid = validate(apiResponse);
if (!isValid) {
  console.log('Validation errors:', validate.errors);
}
```

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

1. **JSON Schema**: Edit `json_schema.json` directly
2. **TypeScript**: Edit `typescript.d.ts` directly  
3. **OpenAPI**: Edit `openapi.yaml` directly
4. **Test**: Run generator with `--validate-only` to check consistency

## Data Structure Overview

### API Response Format

```json
{
  "metadata": {
    "fund_ticker": "IVV",
    "cik": "1100663",
    "total_holdings": 503,
    "data_timestamp": "20250711_143022",
    "upload_timestamp": "2025-07-11T14:30:22.123456Z"
  },
  "holdings": [
    {
      "name": "Apple Inc",
      "ticker": "AAPL",
      "cusip": "037833100",
      "value_usd": 1500000000.00,
      "percent_value": 0.074,
      "fund_ticker": "IVV",
      // ... additional fields
    }
  ]
}
```

### Key Fields

#### Metadata
- `fund_ticker`: ETF/fund ticker symbol
- `cik`: SEC company identifier
- `total_holdings`: Number of holdings in dataset
- `data_timestamp`: When data was processed
- `upload_timestamp`: When uploaded to R2

#### Holdings
- `name`, `title`: Security identification
- `ticker`: Stock ticker (from OpenFIGI enrichment)
- `cusip`, `isin`: Security identifiers
- `value_usd`: Market value in USD
- `percent_value`: Portfolio percentage (decimal)
- `fund_ticker`: Parent fund ticker
- Additional SEC N-PORT fields

## R2 Storage Structure

Holdings data is organized in R2 as:

```
{bucket}/
├── latest/
│   ├── IVV/holdings_enriched.json
│   ├── VOO/holdings_enriched.json
│   └── {fund_ticker}/holdings_enriched.json
```

### API Endpoints

- **Latest Holdings**: `GET /latest/{fund_ticker}/holdings_enriched.json`
- **Example**: `https://your-r2-domain.com/latest/IVV/holdings_enriched.json`

## Best Practices

### Frontend Development
1. **Use TypeScript types** for type safety
2. **Validate responses** with JSON Schema in development
3. **Handle nullable fields** (ticker enrichment may fail)
4. **Cache responses** appropriately (data updates quarterly)

### API Integration
1. **Check upload_timestamp** to detect stale data
2. **Handle 404 errors** for unknown fund tickers
3. **Use enrichment_rate** to understand data quality
4. **Filter by ticker** for enriched holdings only

### Schema Maintenance
1. **Run generator** after pipeline changes
2. **Validate schemas** against sample data
3. **Version schemas** when making breaking changes
4. **Document changes** in git commit messages

## Troubleshooting

### Common Issues

#### Schema Validation Failures
- **Cause**: Data structure changed in pipeline
- **Solution**: Run generator to update schemas
- **Prevention**: Include schema validation in CI/CD

#### Missing TypeScript Types
- **Cause**: TypeScript file not copied to frontend project
- **Solution**: Copy `typescript.d.ts` to your project
- **Prevention**: Use npm package or git submodule

#### Outdated API Documentation
- **Cause**: OpenAPI spec not updated after changes
- **Solution**: Regenerate with updated data
- **Prevention**: Automate schema updates in pipeline

### Getting Help

1. **Check the generator output** for validation errors
2. **Compare schemas** with actual R2 data structure
3. **Review pipeline logs** for data structure changes
4. **Test with sample data** using the schemas

## Version History

- **v1.0.0**: Initial schema specifications
  - JSON Schema for validation
  - TypeScript types for frontend
  - OpenAPI specification for documentation
  - Auto-generation from pipeline data