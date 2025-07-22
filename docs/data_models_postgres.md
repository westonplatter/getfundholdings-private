# PostgreSQL Data Models

## Overview

This document describes the PostgreSQL data models used for managing fund holdings data. The system uses Supabase as the PostgreSQL provider with Alembic for schema migrations.

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

## Existing Tables

### security_mappings
**Purpose**: Caches CUSIP/ISIN to ticker symbol mappings from OpenFIGI API.

Prevents duplicate API calls and tracks mapping validity periods for efficient ticker enrichment during holdings processing.

## Migration History

1. `create_security_mappings_table` - Initial ticker caching system
2. `increase_ticker_column_size` - Expanded ticker field for longer symbols
3. `create_cik_management_tables` - Added fund provider/issuer hierarchy
4. `populate_cik_data_from_constants` - Migrated CIK data from constants.py

## Architecture Benefits

- **Provider Grouping**: Query by brand name to get all related CIKs
- **Incremental Migration**: Tables added progressively to replace file-based constants
- **Multi-Environment**: Separate dev/prod databases with identical schemas
- **Audit Trail**: All tables include created_at/updated_at timestamps