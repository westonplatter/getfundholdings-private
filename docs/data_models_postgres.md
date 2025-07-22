# PostgreSQL Data Models

## Overview

This document describes the PostgreSQL data models used for managing fund holdings data. The system uses Supabase as the PostgreSQL provider with Alembic for schema migrations.

## Database Interaction

The project uses **SQLModel** classes in `fh/db_models.py` to interact with database tables. SQLModel provides type-safe database operations with Pydantic validation and SQLAlchemy ORM functionality.

**Key Components:**

- **SQLModel Classes**: Define table structure and relationships
- **DatabaseManager**: Handles connection pooling and session management with optimized settings
- **Service Classes**: Provide business logic for CRUD operations
  - `SecurityMappingService` - Ticker caching operations
  - `FundDataSCDService` - Type 6 SCD operations for series/class data
- **Session Context**: Use `with db_manager.get_session()` for automatic transaction handling

## Alembic Configuration

- **Migration Tool**: Alembic with multi-environment support
- **Environment Files**: `.env.dev` and `.env.prod` for environment-specific database credentials
- **Migration Commands**: Use `ENVIRONMENT=dev|prod` prefix to target specific database

## Core Tables

### fund_providers

**Purpose**: Parent table that groups related fund companies under a single provider brand.

Enables querying all CIKs for a fund provider (e.g., "BlackRock") to retrieve all associated ETFs, even when the provider operates through multiple legal entities with different CIKs.

### fund_issuers

**Purpose**: Child table containing individual CIK records and their legal entity names.

Links to fund_providers via foreign key relationship. Replaces the constants-based CIK management approach from `fh/constants.py`. Each row represents a specific SEC registrant with their 10-digit CIK identifier.

### fund_series (Type 6 SCD)

(https://en.wikipedia.org/wiki/Slowly_changing_dimension)
**Purpose**: Tracks CIK to Series ID relationships with full change history.

Uses Type 6 Slowly Changing Dimension (SCD) pattern to maintain complete audit trail of when series are added, modified, or removed. Each series represents a fund registration (e.g., S000004310) that can contain multiple tradeable classes.

**Key Fields:**

- `series_id` (max 100 chars) - SEC series identifier (e.g., S000004310)
- `is_current` - Boolean flag for active records
- `effective_date` / `end_date` - Validity period tracking
- `last_verified_date` - When data was last confirmed from SEC

### fund_classes (Type 6 SCD)

**Purpose**: Tracks Series to Class relationships with change tracking for fund names and ticker symbols.

Maintains complete history of fund class attributes that change over time (name changes, ticker assignments, etc.). Each class represents a tradeable share class (e.g., C000219740) within a series.

**Key Fields:**

- `series_id` (max 100 chars) - References fund_series.series_id
- `class_id` (max 100 chars) - SEC class identifier (e.g., C000219740)
- `class_name` - Fund name (can change over time)
- `ticker` - Exchange ticker symbol (can change over time)
- `change_reason` - Audit field explaining why new record was created
- Type 6 SCD fields (same as fund_series)

## Existing Tables

### security_mappings

**Purpose**: Caches CUSIP/ISIN to ticker symbol mappings from OpenFIGI API.

Prevents duplicate API calls and tracks mapping validity periods for efficient ticker enrichment during holdings processing.

## Migration History

1. `create_security_mappings_table` - Initial ticker caching system
2. `increase_ticker_column_size` - Expanded ticker field for longer symbols
3. `create_cik_management_tables` - Added fund provider/issuer hierarchy
4. `populate_cik_data_from_constants` - Migrated CIK data from constants.py
5. `create_fund_series_classes_type6_scd` - Added Type 6 SCD tables for series/class tracking
6. `fix_series_id_column_length` - Expanded series_id/class_id columns from 15 to 100 chars

## Architecture Benefits

- **Provider Grouping**: Query by brand name to get all related CIKs
- **Incremental Migration**: Tables added progressively to replace file-based constants
- **Multi-Environment**: Separate dev/prod databases with identical schemas
- **Complete Audit Trail**: Type 6 SCD provides full history of all data changes
- **Performance Optimized**: Batch processing and connection pooling for efficient operations
- **Data Quality**: Validation filters prevent invalid SEC identifiers from being stored
- **Change Tracking**: Automatic detection and logging of fund name/ticker changes

## Type 6 SCD Implementation

**Slowly Changing Dimension (Type 6)** combines the best of Type 1 (current flag) and Type 2 (historical versions):

**Key Features:**

- `is_current = TRUE` for active records (fast current data queries)
- `effective_date` and `end_date` for precise change tracking
- `change_reason` field documenting why changes occurred
- Batch processing for performance optimization
- Automatic validation of SEC identifier formats

**Query Patterns:**

```sql
-- Get current series for a CIK
SELECT * FROM fund_series WHERE issuer_id = ? AND is_current = TRUE;

-- Get history of changes for a class
SELECT * FROM fund_classes WHERE class_id = ? ORDER BY effective_date;

-- Find ticker changes
SELECT * FROM fund_classes WHERE change_reason LIKE '%ticker:%';
```

**Performance Optimizations:**

- Connection pooling (pool_size=10, max_overflow=20)
- Batch loading of existing records before processing
- Single transaction commits for all changes
- Optimized indexes on current record lookups
