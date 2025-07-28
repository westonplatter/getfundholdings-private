#!/usr/bin/env python3
"""
Load CUSIP ticker cache JSON file into the database.

This utility script loads the existing cusip_ticker_cache.json file
into the security_mappings table for use by the holdings enrichment pipeline.
"""

import json
import os
import argparse
from typing import Optional

from loguru import logger

from fh.config_utils import load_environment_config
from fh.db_models import DatabaseManager, SecurityMappingService, SecurityMapping
from sqlmodel import Session, select
from datetime import datetime, timezone


def bulk_load_cusip_ticker_cache(cache_file_path: str, environment: str = "prod", database_url: Optional[str] = None, batch_size: int = 1000) -> int:
    """
    Bulk load CUSIP ticker cache JSON into the database using batch operations.
    
    This is significantly faster than the individual loading approach for large datasets.

    Args:
        cache_file_path: Path to the cusip_ticker_cache.json file
        environment: Environment to load ('dev' or 'prod', default: 'prod')
        database_url: Optional database URL (will be loaded from env if not provided)
        batch_size: Number of records to process in each batch (default: 1000)

    Returns:
        Number of CUSIP mappings loaded
    """
    if not os.path.exists(cache_file_path):
        logger.error(f"CUSIP ticker cache file not found: {cache_file_path}")
        return 0

    logger.info(f"=== Bulk Loading CUSIP Ticker Cache from {cache_file_path} ===")
    logger.info(f"Using environment: {environment}")
    logger.info(f"Batch size: {batch_size}")

    try:
        # Load the cache file
        with open(cache_file_path, 'r') as f:
            cache_data = json.load(f)

        if not isinstance(cache_data, dict):
            logger.error("Cache file does not contain a dictionary")
            return 0

        logger.info(f"Found {len(cache_data)} CUSIP-ticker mappings in cache file")
        
        # Initialize database connection
        if not database_url:
            # Load environment-specific configuration
            load_environment_config(environment)
            
            # Get database URL from loaded environment
            from fh.config_utils import get_database_url_from_env
            database_url = get_database_url_from_env()
        
        db_manager = DatabaseManager(database_url)
        
        # Track loading statistics
        loaded_count = 0
        updated_count = 0
        error_count = 0
        skipped_count = 0
        
        # Validate and prepare data - support both CUSIPs and ISINs
        valid_mappings = []
        cusip_count = 0
        isin_count = 0
        
        for identifier, ticker in cache_data.items():
            # Determine identifier type and validate format
            identifier_type = None
            is_valid = False
            
            if identifier and isinstance(identifier, str):
                identifier = identifier.strip().upper()
                
                # Check if it's a CUSIP (9 characters, alphanumeric)
                if len(identifier) == 9 and identifier.isalnum():
                    identifier_type = "CUSIP"
                    is_valid = True
                    cusip_count += 1
                
                # Check if it's an ISIN (12 characters, starts with 2-letter country code)
                elif len(identifier) == 12 and identifier[:2].isalpha() and identifier[2:].isalnum():
                    identifier_type = "ISIN"
                    is_valid = True
                    isin_count += 1
            
            if not is_valid:
                logger.debug(f"Skipping invalid identifier format: '{identifier}' (not a valid CUSIP or ISIN)")
                error_count += 1
                continue

            # Validate ticker format
            if not ticker or not isinstance(ticker, str) or len(ticker) > 20:
                logger.debug(f"Skipping invalid ticker for {identifier_type} {identifier}: '{ticker}'")
                error_count += 1
                continue
                
            valid_mappings.append((identifier, ticker, identifier_type))
        
        logger.info(f"Found {cusip_count} CUSIPs and {isin_count} ISINs")
        
        logger.info(f"Processing {len(valid_mappings)} valid identifier-ticker mappings")
        
        # Process in batches
        for i in range(0, len(valid_mappings), batch_size):
            batch = valid_mappings[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(valid_mappings) + batch_size - 1) // batch_size
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} mappings)")
            
            try:
                with db_manager.get_session() as session:
                    # Group identifiers by type for batch queries
                    cusips_in_batch = [identifier for identifier, _, id_type in batch if id_type == "CUSIP"]
                    isins_in_batch = [identifier for identifier, _, id_type in batch if id_type == "ISIN"]
                    
                    existing_mappings = {}
                    
                    # Query existing CUSIP mappings for this batch
                    if cusips_in_batch:
                        cusip_stmt = select(SecurityMapping).where(
                            SecurityMapping.identifier_type == "CUSIP",
                            SecurityMapping.identifier_value.in_(cusips_in_batch),
                            SecurityMapping.end_date.is_(None)  # Active mappings only
                        )
                        for mapping in session.exec(cusip_stmt):
                            existing_mappings[(mapping.identifier_value, mapping.identifier_type)] = mapping
                    
                    # Query existing ISIN mappings for this batch
                    if isins_in_batch:
                        isin_stmt = select(SecurityMapping).where(
                            SecurityMapping.identifier_type == "ISIN",
                            SecurityMapping.identifier_value.in_(isins_in_batch),
                            SecurityMapping.end_date.is_(None)  # Active mappings only
                        )
                        for mapping in session.exec(isin_stmt):
                            existing_mappings[(mapping.identifier_value, mapping.identifier_type)] = mapping
                    
                    # Prepare objects for batch operations
                    new_mappings = []
                    updated_mappings = []
                    
                    for identifier, ticker, identifier_type in batch:
                        existing = existing_mappings.get((identifier, identifier_type))
                        
                        if existing:
                            if existing.ticker != ticker:
                                # Need to update - end the current mapping and create new one
                                existing.end_date = datetime.now(timezone.utc)
                                updated_mappings.append(existing)
                                
                                # Create new mapping
                                new_mapping = SecurityMapping(
                                    identifier_type=identifier_type,
                                    identifier_value=identifier,
                                    ticker=ticker,
                                    has_no_results=False,
                                    start_date=datetime.now(timezone.utc),
                                    end_date=None,
                                    created_at=datetime.now(timezone.utc),
                                    updated_at=datetime.now(timezone.utc)
                                )
                                new_mappings.append(new_mapping)
                                updated_count += 1
                            else:
                                # Already exists with same ticker
                                skipped_count += 1
                        else:
                            # Create new mapping
                            new_mapping = SecurityMapping(
                                identifier_type=identifier_type,
                                identifier_value=identifier,
                                ticker=ticker,
                                has_no_results=False,
                                start_date=datetime.now(timezone.utc),
                                end_date=None,
                                created_at=datetime.now(timezone.utc),
                                updated_at=datetime.now(timezone.utc)
                            )
                            new_mappings.append(new_mapping)
                            loaded_count += 1
                    
                    # Batch update existing mappings (end them)
                    if updated_mappings:
                        for mapping in updated_mappings:
                            session.add(mapping)
                    
                    # Batch insert new mappings
                    if new_mappings:
                        session.add_all(new_mappings)
                    
                    # Commit the batch
                    session.commit()
                    
                    logger.debug(f"Batch {batch_num}: {len(new_mappings)} new, {len(updated_mappings)} updated")
                    
            except Exception as e:
                logger.error(f"Error processing batch {batch_num}: {e}")
                error_count += len(batch)
                continue

        # Summary
        total_processed = loaded_count + updated_count
        logger.info(f"Bulk identifier cache loading complete:")
        logger.info(f"  New mappings loaded: {loaded_count}")
        logger.info(f"  Existing mappings updated: {updated_count}")
        logger.info(f"  Already up-to-date (skipped): {skipped_count}")
        logger.info(f"  Errors/invalid entries: {error_count}")
        logger.info(f"  Total processed: {total_processed}")

        return total_processed

    except Exception as e:
        logger.error(f"Failed to bulk load CUSIP ticker cache: {e}")
        return 0




def load_cusip_ticker_cache(cache_file_path: str, environment: str = "prod", database_url: Optional[str] = None) -> int:
    """
    Load existing CUSIP ticker cache JSON into the database.

    Args:
        cache_file_path: Path to the cusip_ticker_cache.json file
        environment: Environment to load ('dev' or 'prod', default: 'prod')
        database_url: Optional database URL (will be loaded from env if not provided)

    Returns:
        Number of CUSIP mappings loaded
    """
    if not os.path.exists(cache_file_path):
        logger.error(f"CUSIP ticker cache file not found: {cache_file_path}")
        return 0

    logger.info(f"=== Loading CUSIP Ticker Cache from {cache_file_path} ===")
    logger.info(f"Using environment: {environment}")

    try:
        # Load the cache file
        with open(cache_file_path, 'r') as f:
            cache_data = json.load(f)

        if not isinstance(cache_data, dict):
            logger.error("Cache file does not contain a dictionary")
            return 0

        logger.info(f"Found {len(cache_data)} CUSIP-ticker mappings in cache file")
        
        # Initialize database connection
        if not database_url:
            # Load environment-specific configuration
            load_environment_config(environment)
            
            # Get database URL from loaded environment
            from fh.config_utils import get_database_url_from_env
            database_url = get_database_url_from_env()
        
        db_manager = DatabaseManager(database_url)
        security_mapping_service = SecurityMappingService(db_manager)
        
        # Track loading statistics
        loaded_count = 0
        updated_count = 0
        error_count = 0
        skipped_count = 0

        for identifier, ticker in cache_data.items():
            try:
                # Determine identifier type and validate format
                identifier_type = None
                is_valid = False
                
                if identifier and isinstance(identifier, str):
                    identifier = identifier.strip().upper()
                    
                    # Check if it's a CUSIP (9 characters, alphanumeric)
                    if len(identifier) == 9 and identifier.isalnum():
                        identifier_type = "CUSIP"
                        is_valid = True
                    
                    # Check if it's an ISIN (12 characters, starts with 2-letter country code)
                    elif len(identifier) == 12 and identifier[:2].isalpha() and identifier[2:].isalnum():
                        identifier_type = "ISIN"
                        is_valid = True
                
                if not is_valid:
                    logger.debug(f"Skipping invalid identifier format: '{identifier}' (not a valid CUSIP or ISIN)")
                    error_count += 1
                    continue

                # Validate ticker format
                if not ticker or not isinstance(ticker, str) or len(ticker) > 20:
                    logger.debug(f"Skipping invalid ticker for {identifier_type} {identifier}: '{ticker}'")
                    error_count += 1
                    continue

                # Check if mapping already exists
                existing_mapping = security_mapping_service.get_active_mapping(identifier_type, identifier)
                
                if existing_mapping:
                    # Update if ticker is different
                    if existing_mapping.ticker != ticker:
                        mapping = security_mapping_service.create_or_update_mapping(
                            identifier_type, identifier, ticker, has_no_results=False
                        )
                        if mapping:
                            updated_count += 1
                            logger.debug(f"Updated {identifier_type} {identifier}: {existing_mapping.ticker} → {ticker}")
                        else:
                            error_count += 1
                    else:
                        # Already exists with same ticker, no action needed
                        skipped_count += 1
                        logger.debug(f"Skipped {identifier_type} {identifier} (already exists with same ticker: {ticker})")
                else:
                    # Create new mapping
                    mapping = security_mapping_service.create_or_update_mapping(
                        identifier_type, identifier, ticker, has_no_results=False
                    )
                    if mapping:
                        loaded_count += 1
                        logger.debug(f"Loaded {identifier_type} {identifier} → {ticker}")
                    else:
                        error_count += 1

            except Exception as e:
                logger.debug(f"Error processing identifier {identifier}: {e}")
                error_count += 1

        # Summary
        total_processed = loaded_count + updated_count
        logger.info(f"Identifier cache loading complete:")
        logger.info(f"  New mappings loaded: {loaded_count}")
        logger.info(f"  Existing mappings updated: {updated_count}")
        logger.info(f"  Already up-to-date (skipped): {skipped_count}")
        logger.info(f"  Errors/invalid entries: {error_count}")
        logger.info(f"  Total processed: {total_processed}")

        return total_processed

    except Exception as e:
        logger.error(f"Failed to load CUSIP ticker cache: {e}")
        return 0


def main():
    """CLI entry point for CUSIP cache loader."""
    parser = argparse.ArgumentParser(
        description="Load CUSIP ticker cache JSON file into the database"
    )
    parser.add_argument(
        "cache_file",
        nargs="?",
        default="cusip_ticker_cache.json",
        help="Path to CUSIP ticker cache JSON file (default: cusip_ticker_cache.json)"
    )
    parser.add_argument(
        "-e", "--env",
        type=str,
        choices=["dev", "prod"],
        default="prod",
        help="Environment to use (dev or prod, default: prod)"
    )
    parser.add_argument(
        "--database-url",
        type=str,
        help="Database URL (will be loaded from environment if not provided)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be loaded without actually loading into database"
    )
    parser.add_argument(
        "--bulk",
        action="store_true",
        help="Use bulk loading for better performance with large datasets"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for bulk loading (default: 1000)"
    )

    args = parser.parse_args()

    if args.dry_run:
        logger.info("=== DRY RUN MODE - No database changes will be made ===")
        logger.info(f"Would use environment: {args.env}")
        if args.bulk:
            logger.info(f"Would use bulk loading with batch size: {args.batch_size}")
        
        if not os.path.exists(args.cache_file):
            logger.error(f"CUSIP ticker cache file not found: {args.cache_file}")
            return
        
        try:
            with open(args.cache_file, 'r') as f:
                cache_data = json.load(f)
            
            if isinstance(cache_data, dict):
                logger.info(f"Would load {len(cache_data)} CUSIP-ticker mappings from {args.cache_file}")
                
                # Show first few entries as examples
                for i, (cusip, ticker) in enumerate(cache_data.items()):
                    if i < 5:  # Show first 5 entries
                        logger.info(f"  Example: {cusip} → {ticker}")
                    elif i == 5:
                        logger.info(f"  ... and {len(cache_data) - 5} more entries")
                        break
            else:
                logger.error("Cache file does not contain a dictionary")
        except Exception as e:
            logger.error(f"Failed to read cache file: {e}")
    else:
        # Actually load the cache
        if args.bulk:
            loaded_count = bulk_load_cusip_ticker_cache(
                args.cache_file, args.env, args.database_url, args.batch_size
            )
        else:
            loaded_count = load_cusip_ticker_cache(args.cache_file, args.env, args.database_url)
            
        if loaded_count > 0:
            logger.info(f"Successfully loaded {loaded_count} CUSIP-ticker mappings into database")
        else:
            logger.error("No CUSIP mappings were loaded")


if __name__ == "__main__":
    main()