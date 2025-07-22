#!/usr/bin/env python3
"""
PostgreSQL-centric workflow orchestration for fund holdings data pipeline.

This module provides a workflow that sources CIK data from PostgreSQL database
instead of constants.py, enabling dynamic fund management through the database.
"""

import os
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple

from dotenv import load_dotenv
from loguru import logger
from sqlmodel import Session, select

from fh.db_models import DatabaseManager, FundDataSCDService, FundIssuer, FundProvider
from fh.sec_client import SECHTTPClient


@dataclass
class WorkflowPostgresConfig:
    """Configuration for PostgreSQL-based workflow execution."""

    data_dir: str = "data"
    database_url: Optional[str] = None  # Will be loaded from env if not provided
    provider_filter: Optional[str] = None  # Filter by provider name (regex supported)
    max_series_per_cik: Optional[int] = None  # None = no limit
    max_filings_per_series: Optional[int] = None  # None = no limit
    user_agent: str = "GetFundHoldings.com admin@getfundholdings.com"
    interested_etf_tickers: Optional[List[str]] = None
    ticker_filter: Optional[str] = None  # Filter by specific ticker symbol


def get_database_url_from_env() -> str:
    """Get database URL from environment variables with multi-environment support."""
    # Load environment-specific config if ENVIRONMENT is set
    environment = os.getenv("ENVIRONMENT", "")
    if environment in ["dev", "prod"]:
        env_file = f".env.{environment}"
        if os.path.exists(env_file):
            load_dotenv(env_file, override=True)
            logger.info(f"Loaded environment config: {env_file}")
    else:
        load_dotenv()

    # Try SUPABASE_DATABASE_URL first, then fall back to DATABASE_URL
    database_url = os.getenv("SUPABASE_DATABASE_URL") or os.getenv("DATABASE_URL")

    if not database_url:
        raise ValueError(
            "No database URL found. Set SUPABASE_DATABASE_URL or DATABASE_URL environment variable, "
            "or pass database_url parameter."
        )

    return database_url


class FundDataService:
    """Service for retrieving fund data from PostgreSQL."""

    def __init__(self, db_manager: DatabaseManager):
        """Initialize service with database manager."""
        self.db_manager = db_manager

    def get_all_active_ciks(self) -> List[Tuple[str, str, str]]:
        """
        Get all active CIKs from database.

        Returns:
            List of tuples: (cik, company_name, provider_name)
        """
        try:
            with self.db_manager.get_session() as session:
                # Join fund_issuers with fund_providers to get provider names
                statement = (
                    select(
                        FundIssuer.cik,
                        FundIssuer.company_name,
                        FundProvider.provider_name,
                    )
                    .join(FundProvider, FundIssuer.provider_id == FundProvider.id)
                    .where(FundIssuer.is_active == True, FundProvider.is_active == True)
                    .order_by(FundProvider.provider_name, FundIssuer.company_name)
                )

                results = session.exec(statement).all()
                return [(row[0], row[1], row[2]) for row in results]

        except Exception as e:
            logger.error(f"Failed to get active CIKs: {e}")
            return []

    def get_ciks_by_provider(self, provider_filter: str) -> List[Tuple[str, str, str]]:
        """
        Get CIKs filtered by provider name (supports regex).

        Args:
            provider_filter: Provider name or regex pattern

        Returns:
            List of tuples: (cik, company_name, provider_name)
        """
        try:
            with self.db_manager.get_session() as session:
                # Use PostgreSQL regex matching with ILIKE for case-insensitive search
                statement = (
                    select(
                        FundIssuer.cik,
                        FundIssuer.company_name,
                        FundProvider.provider_name,
                    )
                    .join(FundProvider, FundIssuer.provider_id == FundProvider.id)
                    .where(
                        FundIssuer.is_active == True,
                        FundProvider.is_active == True,
                        FundProvider.provider_name.op("~*")(
                            provider_filter
                        ),  # PostgreSQL case-insensitive regex
                    )
                    .order_by(FundProvider.provider_name, FundIssuer.company_name)
                )

                results = session.exec(statement).all()
                return [(row[0], row[1], row[2]) for row in results]

        except Exception as e:
            logger.error(f"Failed to get CIKs by provider '{provider_filter}': {e}")
            return []

    def get_provider_summary(self) -> dict:
        """
        Get summary of providers and their CIK counts.

        Returns:
            Dictionary with provider statistics
        """
        try:
            with self.db_manager.get_session() as session:
                # Get provider counts
                statement = (
                    select(FundProvider.provider_name, FundIssuer.id)
                    .join(FundIssuer, FundProvider.id == FundIssuer.provider_id)
                    .where(FundProvider.is_active == True, FundIssuer.is_active == True)
                )

                results = session.exec(statement).all()

                # Count by provider
                provider_counts = {}
                for provider_name, _ in results:
                    provider_counts[provider_name] = (
                        provider_counts.get(provider_name, 0) + 1
                    )

                return {
                    "total_providers": len(provider_counts),
                    "total_ciks": len(results),
                    "provider_counts": provider_counts,
                }

        except Exception as e:
            logger.error(f"Failed to get provider summary: {e}")
            return {"total_providers": 0, "total_ciks": 0, "provider_counts": {}}

    def get_issuer_id_by_cik(self, cik: str) -> Optional[int]:
        """
        Get issuer ID by CIK.

        Args:
            cik: CIK string

        Returns:
            Issuer ID if found, None otherwise
        """
        try:
            with self.db_manager.get_session() as session:
                statement = select(FundIssuer.id).where(
                    FundIssuer.cik == cik, FundIssuer.is_active == True
                )

                result = session.exec(statement).first()
                return result

        except Exception as e:
            logger.error(f"Failed to get issuer ID for CIK {cik}: {e}")
            return None


class FundHoldingsWorkflowPostgres:
    """
    PostgreSQL-centric workflow orchestrator for fund holdings data pipeline.

    Sources CIK data from PostgreSQL database instead of constants.py.
    """

    def __init__(self, config: WorkflowPostgresConfig):
        """Initialize workflow with configuration."""
        self.config = config

        # Initialize database components
        database_url = config.database_url or get_database_url_from_env()
        self.db_manager = DatabaseManager(database_url)
        self.fund_service = FundDataService(self.db_manager)
        self.scd_service = FundDataSCDService(self.db_manager)

        # Initialize SEC client
        self.sec_client = SECHTTPClient(user_agent=config.user_agent)

        logger.info("PostgreSQL workflow initialized")

    def get_target_ciks(self) -> List[Tuple[str, str, str]]:
        """
        Get list of target CIKs based on configuration.

        Returns:
            List of tuples: (cik, company_name, provider_name)
        """
        if self.config.provider_filter:
            logger.info(
                f"Getting CIKs for provider filter: {self.config.provider_filter}"
            )
            ciks = self.fund_service.get_ciks_by_provider(self.config.provider_filter)
        else:
            logger.info("Getting all active CIKs")
            ciks = self.fund_service.get_all_active_ciks()

        logger.info(f"Found {len(ciks)} target CIKs")
        return ciks

    def print_provider_summary(self):
        """Print summary of available providers and their CIK counts."""
        summary = self.fund_service.get_provider_summary()

        logger.info("=== Fund Provider Summary ===")
        logger.info(f"Total Providers: {summary['total_providers']}")
        logger.info(f"Total Active CIKs: {summary['total_ciks']}")
        logger.info("")
        logger.info("CIKs by Provider:")

        for provider, count in sorted(summary["provider_counts"].items()):
            logger.info(f"  {provider}: {count} CIKs")

    def run_basic_iteration(self):
        """
        Basic workflow that iterates over all target CIKs and fetches series/class data.

        This implements the first pipeline stage: SEC series discovery.
        """
        logger.info("=== Starting PostgreSQL-based Fund Holdings Workflow ===")

        # Print provider summary first
        self.print_provider_summary()
        logger.info("")

        # Get target CIKs
        target_ciks = self.get_target_ciks()

        if not target_ciks:
            logger.warning("No CIKs found matching criteria")
            return

        logger.info(f"=== Processing {len(target_ciks)} CIKs ===")

        # Track results
        successful_ciks = 0
        failed_ciks = 0
        total_series_found = 0

        # Iterate over CIKs and fetch series data
        for cik, company_name, provider_name in target_ciks:
            logger.info(
                f"Processing CIK {cik}: {company_name} (Provider: {provider_name})"
            )

            try:
                # Get issuer ID for database operations
                issuer_id = self.fund_service.get_issuer_id_by_cik(cik)
                if not issuer_id:
                    logger.error(
                        f"  └─ Could not find issuer ID for CIK {cik} in database"
                    )
                    failed_ciks += 1
                    continue

                # Stage 1: Get series/class data from SEC
                logger.info(f"  └─ Fetching series/class data from SEC for CIK {cik}")
                series_data = self.sec_client.fetch_series_data(cik)

                if series_data:
                    # Count series with valid data
                    valid_series = [
                        s
                        for s in series_data
                        if "series_id" in s and not s.get("error")
                    ]

                    logger.info(f"  └─ Found {len(valid_series)} series for CIK {cik}")
                    total_series_found += len(valid_series)

                    # Save to database using Type 6 SCD
                    if valid_series:
                        logger.info(f"  └─ Saving series/class data to database...")
                        stats = self.scd_service.upsert_series_data(
                            issuer_id, valid_series
                        )

                        # Log database operation results
                        logger.info(f"    │ Database stats:")
                        logger.info(f"    │   New series: {stats['series_new']}")
                        logger.info(
                            f"    │   Verified series: {stats['series_verified']}"
                        )
                        if stats["series_skipped_invalid"] > 0:
                            logger.info(
                                f"    │   Skipped invalid series: {stats['series_skipped_invalid']}"
                            )
                        logger.info(f"    │   New classes: {stats['classes_new']}")
                        logger.info(
                            f"    │   Updated classes: {stats['classes_updated']}"
                        )
                        logger.info(
                            f"    │   Verified classes: {stats['classes_verified']}"
                        )
                        if stats["classes_skipped_invalid"] > 0:
                            logger.info(
                                f"    │   Skipped invalid classes: {stats['classes_skipped_invalid']}"
                            )

                    # Log series details
                    for series in valid_series:
                        series_id = series.get("series_id", "Unknown")
                        class_count = len(series.get("classes", []))
                        logger.info(f"    │ Series {series_id}: {class_count} classes")

                        # Log class details if available (limit to avoid spam)
                        classes = series.get("classes", [])
                        for i, class_info in enumerate(classes):
                            if i < 3:  # Show first 3 classes
                                class_id = class_info.get("class_id", "Unknown")
                                class_name = class_info.get("class_name", "Unknown")
                                ticker = class_info.get("ticker", "N/A")
                                logger.info(
                                    f"      └─ {class_id}: {class_name} ({ticker})"
                                )
                            elif i == 3 and len(classes) > 3:
                                logger.info(
                                    f"      └─ ... and {len(classes) - 3} more classes"
                                )
                                break

                    successful_ciks += 1
                else:
                    logger.warning(f"  └─ No series data found for CIK {cik}")
                    failed_ciks += 1

            except Exception as e:
                logger.error(f"  └─ Failed to process CIK {cik}: {e}")
                failed_ciks += 1

            logger.info("")  # Add spacing between CIKs

        # Summary
        logger.info("=== Workflow Results ===")
        logger.info(f"Total CIKs processed: {len(target_ciks)}")
        logger.info(f"Successful: {successful_ciks}")
        logger.info(f"Failed: {failed_ciks}")
        logger.info(f"Total series found: {total_series_found}")

        # Database statistics
        try:
            db_stats = self.scd_service.get_stats()
            logger.info(f"Database Statistics:")
            logger.info(f"  Current series: {db_stats.get('current_series', 0)}")
            logger.info(f"  Current classes: {db_stats.get('current_classes', 0)}")
            logger.info(
                f"  Total series history: {db_stats.get('total_series_history', 0)}"
            )
            logger.info(
                f"  Total classes history: {db_stats.get('total_classes_history', 0)}"
            )
        except Exception as e:
            logger.warning(f"Could not get database statistics: {e}")

        logger.info("=== Workflow Completed ===")


def main():
    """CLI entry point for PostgreSQL workflow."""
    import argparse

    parser = argparse.ArgumentParser(
        description="PostgreSQL-based Fund Holdings Workflow"
    )
    parser.add_argument(
        "--provider", "-p", type=str, help="Filter by provider name (supports regex)"
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Show provider summary only, don't process CIKs",
    )

    args = parser.parse_args()

    # Create config
    config = WorkflowPostgresConfig(
        provider_filter=args.provider,
    )

    # Create and run workflow
    workflow = FundHoldingsWorkflowPostgres(config)

    if args.summary_only:
        workflow.print_provider_summary()
    else:
        workflow.run_basic_iteration()


if __name__ == "__main__":
    main()
