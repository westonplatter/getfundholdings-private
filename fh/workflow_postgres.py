#!/usr/bin/env python3
"""
PostgreSQL-centric workflow orchestration for fund holdings data pipeline.

This module provides a workflow that sources CIK data from PostgreSQL database
instead of constants.py, enabling dynamic fund management through the database.
"""

import os
from dataclasses import dataclass
from datetime import datetime, date
from typing import List, Optional, Tuple

from loguru import logger
from sqlmodel import Session, select
import pandas as pd

from fh.config_utils import load_environment_config
from fh.db_models import DatabaseManager, FundDataSCDService, SECReportService, FundIssuer, FundProvider
from fh.sec_client import SECHTTPClient
from fh.openfigi_client import OpenFIGIClient
from parse_nport import NPortParser


@dataclass
class WorkflowPostgresConfig:
    """Configuration for PostgreSQL-based workflow execution."""

    data_dir: str = "data"
    database_url: Optional[str] = None  # Will be loaded from env if not provided
    environment: str = "prod"  # Environment to use ('dev' or 'prod')
    provider_filter: Optional[str] = None  # Filter by provider name (regex supported)
    max_series_per_cik: Optional[int] = None  # None = no limit
    max_filings_per_series: Optional[int] = None  # None = no limit
    user_agent: str = "GetFundHoldings.com admin@getfundholdings.com"
    interested_etf_tickers: Optional[List[str]] = None
    ticker_filter: Optional[str] = None  # Filter by specific ticker symbol
    
    # SEC filing processing options
    enable_filing_discovery: bool = True  # Enable Stage 2: Series -> Filing discovery
    target_form_types: List[str] = None  # Form types to discover (default: ["NPORT-P"])
    
    # XML download and processing options
    enable_xml_download: bool = False  # Enable Stage 3: Download XML files
    enable_holdings_processing: bool = False  # Enable Stage 4: Extract holdings from XML
    enable_ticker_enrichment: bool = False  # Enable Stage 5: Enrich holdings with tickers
    
    def __post_init__(self):
        """Set default form types if not provided."""
        if self.target_form_types is None:
            self.target_form_types = ["NPORT-P"]




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
        if not config.database_url:
            # Load environment-specific configuration
            load_environment_config(config.environment)
            
            # Get database URL from loaded environment
            from fh.config_utils import get_database_url_from_env
            database_url = get_database_url_from_env()
        else:
            database_url = config.database_url
            
        self.db_manager = DatabaseManager(database_url)
        self.fund_service = FundDataService(self.db_manager)
        self.scd_service = FundDataSCDService(self.db_manager)
        self.sec_report_service = SECReportService(self.db_manager)

        # Initialize SEC client
        self.sec_client = SECHTTPClient(user_agent=config.user_agent)
        
        # Initialize OpenFIGI client (if ticker enrichment is enabled)
        self.openfigi_client = None
        if config.enable_ticker_enrichment:
            self.openfigi_client = OpenFIGIClient()

        # Track current run state for proper stage isolation
        self.current_run_series_ids = set()  # Series IDs processed in current run
        self.current_run_ciks = set()  # CIKs processed in current run

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

    def discover_series_filings(self, series_id: str) -> int:
        """
        Discover and save SEC filings for a specific series (Stage 2 of pipeline).

        Args:
            series_id: Series ID (e.g., S000004310)

        Returns:
            Number of filings discovered and saved
        """
        filings_saved = 0

        for form_type in self.config.target_form_types:
            try:
                logger.info(f"  └─ Discovering {form_type} filings for series {series_id}")
                
                # Fetch filings from SEC API
                filings_data = self.sec_client.fetch_series_filings(series_id, form_type)
                
                if not filings_data:
                    logger.info(f"    │ No {form_type} filings found for series {series_id}")
                    continue

                # Filter out error entries
                valid_filings = [f for f in filings_data if not f.get("error")]
                if not valid_filings:
                    logger.warning(f"    │ All {form_type} filings for series {series_id} had errors")
                    continue

                logger.info(f"    │ Found {len(valid_filings)} valid {form_type} filings")

                # Save filings to database
                for filing_data in valid_filings:
                    accession_number = filing_data.get("accession_number")
                    if not accession_number:
                        logger.warning(f"    │ Skipping filing without accession number: {filing_data}")
                        continue

                    # Parse dates
                    filing_date = self._parse_date(filing_data.get("filing_date"))
                    report_date = self._parse_date(filing_data.get("report_date"))

                    # Create report metadata
                    report_metadata = {
                        "raw_data": filing_data.get("raw_data", []),
                        "parse_method": filing_data.get("parse_method", "structured"),
                        "link_text": filing_data.get("link_text"),
                    }

                    # Save to database
                    report = self.sec_report_service.upsert_report(
                        series_id=series_id,
                        accession_number=accession_number,
                        form_type=form_type,
                        filing_date=filing_date,
                        report_date=report_date,
                        report_metadata=report_metadata,
                        raw_data=filing_data,
                    )

                    if report:
                        filings_saved += 1
                        logger.debug(f"      └─ Saved {form_type} filing {accession_number}")
                    else:
                        logger.warning(f"      └─ Failed to save {form_type} filing {accession_number}")

                # Apply filing limit if configured
                if (self.config.max_filings_per_series and 
                    filings_saved >= self.config.max_filings_per_series):
                    logger.info(f"    │ Reached max filings limit ({self.config.max_filings_per_series})")
                    break

            except Exception as e:
                logger.error(f"    │ Failed to discover {form_type} filings for series {series_id}: {e}")

        return filings_saved

    def _parse_date(self, date_str: Optional[str]) -> Optional[date]:
        """
        Parse date string from SEC filing data.

        Args:
            date_str: Date string in format "YYYY-MM-DD"

        Returns:
            Parsed date or None if invalid
        """
        if not date_str or not isinstance(date_str, str):
            return None

        try:
            # Handle common SEC date formats
            if len(date_str) == 10 and "-" in date_str:
                parts = date_str.split("-")
                if len(parts) == 3:
                    year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
                    return date(year, month, day)
        except (ValueError, IndexError):
            logger.debug(f"Could not parse date: '{date_str}'")

        return None

    def print_sec_reports_summary(self):
        """Print summary of SEC reports in the database."""
        stats = self.sec_report_service.get_reports_stats()

        logger.info("=== SEC Reports Summary ===")
        logger.info(f"Total Reports: {stats['total_reports']}")
        logger.info("")

        if stats["by_form_type"]:
            logger.info("Reports by Form Type:")
            for form_type, count in sorted(stats["by_form_type"].items()):
                logger.info(f"  {form_type}: {count} reports")
            logger.info("")

        if stats["by_download_status"]:
            logger.info("Reports by Download Status:")
            for status, count in sorted(stats["by_download_status"].items()):
                logger.info(f"  {status}: {count} reports")
            logger.info("")

        if stats["by_processing_status"]:
            logger.info("Reports by Processing Status:")
            for status, count in sorted(stats["by_processing_status"].items()):
                logger.info(f"  {status}: {count} reports")

    def download_pending_xml_files(self) -> int:
        """
        Download XML files for pending SEC reports from current run only (Stage 3 of pipeline).

        Returns:
            Number of XML files successfully downloaded
        """
        if not self.config.enable_xml_download:
            return 0

        logger.info("=== Stage 3: Downloading XML Files ===")
        
        # Get reports that need XML download
        all_pending_reports = self.sec_report_service.get_pending_downloads(form_type="NPORT-P")
        
        if not all_pending_reports:
            logger.info("No pending XML downloads found")
            return 0

        # Filter to only reports from current run to avoid cross-contamination
        if self.current_run_series_ids:
            pending_reports = [
                r for r in all_pending_reports 
                if r.series_id in self.current_run_series_ids
            ]
            logger.info(f"Found {len(all_pending_reports)} total pending reports, {len(pending_reports)} from current run")
        else:
            pending_reports = all_pending_reports
            logger.info(f"Found {len(pending_reports)} reports needing XML download (no run filter applied)")

        if not pending_reports:
            logger.info("No pending XML downloads found for current run")
            return 0

        downloaded_count = 0

        for report in pending_reports:
            try:
                logger.info(f"  └─ Downloading XML for {report.accession_number}")
                
                # Extract CIK from series data (we'll need to get it from the database)
                # For now, we'll extract it from the accession number pattern if possible
                cik = self._extract_cik_from_report(report)
                if not cik:
                    logger.warning(f"    │ Could not determine CIK for report {report.accession_number}")
                    continue

                # Download XML content
                xml_content = self.sec_client.download_nport_xml(cik, report.accession_number)
                
                if xml_content:
                    # Save XML to file
                    xml_filename = f"nport_{cik}_{report.series_id}_{report.accession_number.replace('-', '_')}.xml"
                    xml_file_path = os.path.join(self.config.data_dir, xml_filename)
                    
                    # Ensure data directory exists
                    os.makedirs(self.config.data_dir, exist_ok=True)
                    
                    with open(xml_file_path, 'w', encoding='utf-8') as f:
                        f.write(xml_content)
                    
                    # Update database with file path and download status
                    file_paths = {"xml": xml_file_path}
                    success = self.sec_report_service.update_download_status(
                        report.id, "downloaded", file_paths=file_paths
                    )
                    
                    if success:
                        downloaded_count += 1
                        logger.info(f"    │ Downloaded and saved: {xml_filename}")
                    else:
                        logger.error(f"    │ Failed to update database for {report.accession_number}")
                else:
                    # Mark as failed
                    self.sec_report_service.update_download_status(
                        report.id, "failed", error_message="Failed to download XML content"
                    )
                    logger.warning(f"    │ Failed to download XML for {report.accession_number}")

            except Exception as e:
                logger.error(f"    │ Error downloading XML for {report.accession_number}: {e}")
                self.sec_report_service.update_download_status(
                    report.id, "failed", error_message=str(e)
                )

        logger.info(f"Downloaded {downloaded_count} XML files")
        return downloaded_count

    def process_downloaded_xml_files(self) -> int:
        """
        Process downloaded XML files from current run to extract holdings data (Stage 4 of pipeline).

        Returns:
            Number of XML files successfully processed
        """
        if not self.config.enable_holdings_processing:
            return 0

        logger.info("=== Stage 4: Processing XML Files ===")
        
        # Get reports with downloaded XML that need processing
        all_downloaded_reports = [
            r for r in self.sec_report_service.get_pending_downloads() 
            if r.download_status == "downloaded" and r.processing_status == "pending"
        ]
        
        # Filter to only reports from current run
        if self.current_run_series_ids:
            downloaded_reports = [
                r for r in all_downloaded_reports 
                if r.series_id in self.current_run_series_ids
            ]
            logger.info(f"Found {len(all_downloaded_reports)} total downloaded files, {len(downloaded_reports)} from current run")
        else:
            downloaded_reports = all_downloaded_reports
            logger.info(f"Found {len(downloaded_reports)} XML files to process (no run filter applied)")
        
        if not downloaded_reports:
            logger.info("No downloaded XML files needing processing from current run")
            return 0

        processed_count = 0

        for report in downloaded_reports:
            try:
                # Get XML file path
                xml_file_path = report.file_paths.get("xml") if report.file_paths else None
                if not xml_file_path or not os.path.exists(xml_file_path):
                    logger.warning(f"    │ XML file not found for {report.accession_number}")
                    continue

                logger.info(f"  └─ Processing XML: {os.path.basename(xml_file_path)}")

                # Parse XML to extract holdings
                parser = NPortParser(xml_file_path)
                if not parser.load_xml():
                    logger.error(f"    │ Failed to load XML file: {xml_file_path}")
                    continue

                # Extract fund info and holdings
                fund_info = parser.get_fund_info()
                holdings_df = parser.get_holdings_dataframe()

                if holdings_df is not None and not holdings_df.empty:
                    # Save holdings to CSV
                    report_date_str = report.report_date.strftime("%Y%m%d") if report.report_date else "unknown"
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    
                    cik = self._extract_cik_from_report(report)
                    csv_filename = f"holdings_raw_{cik}_{report.series_id}_{report_date_str}_{timestamp}.csv"
                    csv_file_path = os.path.join(self.config.data_dir, csv_filename)
                    
                    holdings_df.to_csv(csv_file_path, index=False)
                    
                    # Update database with processing results
                    file_paths = report.file_paths or {}
                    file_paths["holdings_raw"] = csv_file_path
                    
                    success = self.sec_report_service.update_download_status(
                        report.id, "downloaded", file_paths=file_paths
                    )
                    
                    if success:
                        success = self.sec_report_service.update_processing_status(
                            report.id, "processed"
                        )
                    
                    if success:
                        processed_count += 1
                        logger.info(f"    │ Processed {len(holdings_df)} holdings → {csv_filename}")
                    else:
                        logger.error(f"    │ Failed to update database for {report.accession_number}")
                else:
                    logger.warning(f"    │ No holdings data found in {xml_file_path}")
                    self.sec_report_service.update_processing_status(
                        report.id, "failed", error_message="No holdings data found"
                    )

            except Exception as e:
                logger.error(f"    │ Error processing XML for {report.accession_number}: {e}")
                self.sec_report_service.update_processing_status(
                    report.id, "failed", error_message=str(e)
                )

        logger.info(f"Processed {processed_count} XML files")
        return processed_count

    def enrich_processed_holdings(self) -> int:
        """
        Enrich processed holdings with ticker symbols (Stage 5 of pipeline).

        Returns:
            Number of holdings files successfully enriched
        """
        if not self.config.enable_ticker_enrichment or not self.openfigi_client:
            return 0

        logger.info("=== Stage 5: Enriching Holdings with Tickers ===")
        
        # Get reports with processed holdings that need enrichment
        processed_reports = [
            r for r in self.sec_report_service.get_pending_downloads() 
            if r.processing_status == "processed" and 
               r.file_paths and 
               "holdings_raw" in r.file_paths and
               "holdings_enriched" not in r.file_paths
        ]
        
        if not processed_reports:
            logger.info("No processed holdings files needing enrichment")
            return 0

        logger.info(f"Found {len(processed_reports)} holdings files to enrich")
        enriched_count = 0

        for report in processed_reports:
            try:
                holdings_csv_path = report.file_paths["holdings_raw"]
                if not os.path.exists(holdings_csv_path):
                    logger.warning(f"    │ Holdings file not found: {holdings_csv_path}")
                    continue

                logger.info(f"  └─ Enriching: {os.path.basename(holdings_csv_path)}")

                # Load holdings data
                holdings_df = pd.read_csv(holdings_csv_path)
                original_count = len(holdings_df)

                # Enrich with tickers using optimized cache-first approach:
                # 1) CUSIP cache, 2) ISIN cache, 3) CUSIP API calls, 4) ISIN API calls
                enriched_df = self._enrich_holdings_optimized(holdings_df, original_count)

                # Calculate success rate
                tickers_found = enriched_df['ticker'].notna().sum() if 'ticker' in enriched_df.columns else 0
                success_rate = (tickers_found / original_count * 100) if original_count > 0 else 0

                # Save enriched holdings
                base_name = os.path.basename(holdings_csv_path)
                enriched_filename = base_name.replace("holdings_raw_", "holdings_enriched_")
                enriched_file_path = os.path.join(self.config.data_dir, enriched_filename)
                
                enriched_df.to_csv(enriched_file_path, index=False)

                # Update database with enriched file path
                file_paths = report.file_paths.copy()
                file_paths["holdings_enriched"] = enriched_file_path
                
                success = self.sec_report_service.update_download_status(
                    report.id, "downloaded", file_paths=file_paths
                )

                if success:
                    enriched_count += 1
                    logger.info(f"    │ Enriched {tickers_found}/{original_count} holdings ({success_rate:.1f}%) → {enriched_filename}")
                else:
                    logger.error(f"    │ Failed to update database for {report.accession_number}")

            except Exception as e:
                logger.error(f"    │ Error enriching holdings for {report.accession_number}: {e}")

        logger.info(f"Enriched {enriched_count} holdings files")
        return enriched_count

    def _extract_cik_from_report(self, report) -> Optional[str]:
        """
        Extract CIK from SEC report data with improved fallbacks for filtered runs.
        
        Args:
            report: SECReport object
            
        Returns:
            CIK string or None if not found
        """
        # Try to get CIK from raw_data first
        if report.raw_data and isinstance(report.raw_data, dict):
            # Look for CIK in various possible locations
            for key in ['cik', 'reg_cik', 'regCik']:
                if key in report.raw_data:
                    return str(report.raw_data[key]).zfill(10)  # Ensure 10-digit format
        
        # Fallback: try to extract from accession number pattern
        # Accession numbers often contain CIK: "0001234567-25-123456"
        if report.accession_number:
            parts = report.accession_number.split('-')
            if len(parts) >= 1:
                potential_cik = parts[0]
                if potential_cik.isdigit() and len(potential_cik) == 10:
                    return potential_cik
        
        # Enhanced fallback: if this is a filtered run and we only have one CIK, use it
        if len(self.current_run_ciks) == 1:
            cik = next(iter(self.current_run_ciks))
            logger.debug(f"Using current run CIK {cik} for report {report.accession_number}")
            return cik
        
        # Database lookup: look up CIK by series_id in database
        try:
            with self.db_manager.get_session() as session:
                from fh.db_models import FundSeries
                series = session.exec(
                    select(FundSeries.issuer_id).where(
                        FundSeries.series_id == report.series_id,
                        FundSeries.is_current == True
                    )
                ).first()
                
                if series:
                    issuer = session.exec(
                        select(FundIssuer.cik).where(FundIssuer.id == series)
                    ).first()
                    if issuer:
                        return issuer
        except Exception as e:
            logger.debug(f"Failed to lookup CIK for series {report.series_id}: {e}")
        
        # Final fallback: if we have multiple CIKs but the report series is in current run
        if report.series_id in self.current_run_series_ids and self.current_run_ciks:
            # Try to find the most likely CIK by checking if any current run CIK works
            for cik in self.current_run_ciks:
                logger.debug(f"Trying current run CIK {cik} for report {report.accession_number}")
                return cik  # Return first available CIK as last resort
        
        return None

    def _enrich_holdings_optimized(self, holdings_df: pd.DataFrame, original_count: int) -> pd.DataFrame:
        """
        Enrich holdings with ticker symbols using optimized cache-first approach.
        
        Order of operations:
        1. Try CUSIP cache lookup for all holdings
        2. Try ISIN cache lookup for remaining holdings  
        3. Make CUSIP API calls for remaining holdings
        4. Make ISIN API calls for remaining holdings
        
        Args:
            holdings_df: DataFrame with holdings data
            original_count: Original number of holdings
            
        Returns:
            DataFrame with ticker column added/updated
        """
        enriched_df = holdings_df.copy()
        enriched_df['ticker'] = None  # Initialize ticker column
        
        # Check if we have the required columns
        has_cusip = 'cusip' in enriched_df.columns
        has_isin = 'isin' in enriched_df.columns
        
        if not has_cusip and not has_isin:
            logger.warning(f"    │ No CUSIP or ISIN columns found, skipping ticker enrichment")
            return enriched_df
            
        logger.info(f"    │ Starting optimized ticker enrichment for {original_count} holdings")
        
        # Step 1: CUSIP cache lookup
        if has_cusip:
            cache_hits = self._lookup_tickers_from_cache(enriched_df, 'cusip', 'CUSIP')
            logger.info(f"    │ CUSIP cache hits: {cache_hits}")
        
        # Step 2: ISIN cache lookup for remaining holdings
        if has_isin:
            remaining_count = enriched_df['ticker'].isna().sum()
            if remaining_count > 0:
                cache_hits = self._lookup_tickers_from_cache(enriched_df, 'isin', 'ISIN')
                logger.info(f"    │ ISIN cache hits: {cache_hits}")
        
        # Step 3: CUSIP API calls for remaining holdings
        if has_cusip:
            remaining_count = enriched_df['ticker'].isna().sum()
            if remaining_count > 0:
                api_hits = self._lookup_tickers_from_api(enriched_df, 'cusip', 'CUSIP')
                logger.info(f"    │ CUSIP API calls: {api_hits}")
        
        # Step 4: ISIN API calls for remaining holdings
        if has_isin:
            remaining_count = enriched_df['ticker'].isna().sum()
            if remaining_count > 0:
                api_hits = self._lookup_tickers_from_api(enriched_df, 'isin', 'ISIN')
                logger.info(f"    │ ISIN API calls: {api_hits}")
        
        return enriched_df
    
    def _lookup_tickers_from_cache(self, df: pd.DataFrame, id_column: str, id_type: str) -> int:
        """
        Look up tickers from database cache only (no API calls).
        
        Args:
            df: DataFrame to update (modified in place)
            id_column: Column name containing identifiers (cusip/isin)
            id_type: Type of identifier ('CUSIP' or 'ISIN')
            
        Returns:
            Number of cache hits found
        """
        if not self.openfigi_client.mapping_service:
            return 0
            
        # Get holdings that need tickers and have valid identifiers
        mask = (
            df['ticker'].isna() & 
            df[id_column].notna() & 
            (df[id_column] != '') & 
            (df[id_column] != '000000000' if id_type == 'CUSIP' else True)
        )
        
        if not mask.any():
            return 0
            
        unique_identifiers = df.loc[mask, id_column].unique()
        cache_hits = 0
        
        # Look up each unique identifier in cache
        for identifier in unique_identifiers:
            try:
                mapping = self.openfigi_client.mapping_service.get_active_mapping(id_type, identifier)
                if mapping and not mapping.has_no_results:
                    # Update all rows with this identifier
                    identifier_mask = mask & (df[id_column] == identifier)
                    df.loc[identifier_mask, 'ticker'] = mapping.ticker
                    cache_hits += identifier_mask.sum()
            except Exception as e:
                logger.debug(f"Cache lookup error for {id_type} {identifier}: {e}")
                
        return cache_hits
    
    def _lookup_tickers_from_api(self, df: pd.DataFrame, id_column: str, id_type: str) -> int:
        """
        Look up tickers from API calls only (for identifiers not in cache).
        
        Args:
            df: DataFrame to update (modified in place)
            id_column: Column name containing identifiers (cusip/isin)
            id_type: Type of identifier ('CUSIP' or 'ISIN')
            
        Returns:
            Number of API hits found
        """
        # Get holdings that still need tickers and have valid identifiers
        mask = (
            df['ticker'].isna() & 
            df[id_column].notna() & 
            (df[id_column] != '') & 
            (df[id_column] != '000000000' if id_type == 'CUSIP' else True)
        )
        
        if not mask.any():
            return 0
            
        unique_identifiers = df.loc[mask, id_column].unique()
        api_hits = 0
        
        # Make API calls for each unique identifier
        for identifier in unique_identifiers:
            try:
                if id_type == 'CUSIP':
                    ticker = self.openfigi_client._fetch_ticker_from_api(identifier)
                else:  # ISIN
                    ticker = self.openfigi_client._fetch_ticker_from_api_isin(identifier)
                
                if ticker:
                    # Update all rows with this identifier
                    identifier_mask = mask & (df[id_column] == identifier)
                    df.loc[identifier_mask, 'ticker'] = ticker
                    api_hits += identifier_mask.sum()
                
                # Cache the result (including null results)
                if self.openfigi_client.mapping_service:
                    self.openfigi_client.mapping_service.create_or_update_mapping(
                        id_type, identifier, ticker, has_no_results=(ticker is None)
                    )
                    
            except Exception as e:
                logger.debug(f"API lookup error for {id_type} {identifier}: {e}")
                
        return api_hits


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

                    # Apply ticker filter if specified
                    if self.config.ticker_filter:
                        filtered_series = []
                        for series in valid_series:
                            # Check if any class in this series has the target ticker
                            classes = series.get("classes", [])
                            for class_info in classes:
                                ticker = class_info.get("ticker", "")
                                if ticker and ticker.upper() == self.config.ticker_filter.upper():
                                    filtered_series.append(series)
                                    logger.info(f"    │ Found series {series.get('series_id')} with ticker {ticker}")
                                    break  # Found a match, no need to check other classes
                        
                        if filtered_series:
                            valid_series = filtered_series
                            logger.info(f"    │ Filtered to {len(valid_series)} series with ticker: {self.config.ticker_filter}")
                        else:
                            logger.warning(f"    │ No series found with ticker: {self.config.ticker_filter}")
                            valid_series = []

                    # Stage 2: Discover SEC filings for each series (if enabled)
                    total_filings_found = 0
                    if self.config.enable_filing_discovery and valid_series:
                        logger.info(f"  └─ Stage 2: Discovering SEC filings for {len(valid_series)} series")
                        
                        # Track series being processed in current run
                        current_series_count = 0
                        for series in valid_series:
                            series_id = series.get("series_id")
                            if not series_id:
                                continue
                            
                            # Add to current run tracking
                            self.current_run_series_ids.add(series_id)
                            self.current_run_ciks.add(cik)
                            
                            filings_count = self.discover_series_filings(series_id)
                            total_filings_found += filings_count
                            current_series_count += 1
                            
                            if filings_count > 0:
                                logger.info(f"    │ Series {series_id}: {filings_count} filings discovered")
                            
                            # Apply series limit if configured
                            if (self.config.max_series_per_cik and 
                                current_series_count >= self.config.max_series_per_cik):
                                logger.info(f"    │ Reached max series limit ({self.config.max_series_per_cik})")
                                break

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
                    
                    # Log filing discovery results
                    if self.config.enable_filing_discovery:
                        logger.info(f"    │ Total SEC filings discovered: {total_filings_found}")

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
        
        # SEC Reports summary (if filing discovery was enabled)
        if self.config.enable_filing_discovery:
            logger.info("")
            self.print_sec_reports_summary()

        # Run additional pipeline stages if enabled
        if self.config.enable_xml_download:
            logger.info("")
            xml_downloaded = self.download_pending_xml_files()
            logger.info(f"XML files downloaded: {xml_downloaded}")

        if self.config.enable_holdings_processing:
            logger.info("")
            xml_processed = self.process_downloaded_xml_files()
            logger.info(f"XML files processed: {xml_processed}")

        if self.config.enable_ticker_enrichment:
            logger.info("")
            holdings_enriched = self.enrich_processed_holdings()
            logger.info(f"Holdings files enriched: {holdings_enriched}")

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
        "-e", "--env",
        type=str,
        choices=["dev", "prod"],
        default="prod",
        help="Environment to use (dev or prod, default: prod)"
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Show provider summary only, don't process CIKs",
    )
    parser.add_argument(
        "--disable-filing-discovery",
        action="store_true",
        help="Skip Stage 2: SEC filing discovery",
    )
    parser.add_argument(
        "--form-types",
        type=str,
        default="NPORT-P",
        help="Comma-separated list of SEC form types to discover (default: NPORT-P)",
    )
    parser.add_argument(
        "--max-filings",
        type=int,
        help="Maximum number of filings to discover per series",
    )
    parser.add_argument(
        "--enable-xml-download",
        action="store_true",
        help="Enable Stage 3: Download XML files for discovered reports",
    )
    parser.add_argument(
        "--enable-holdings-processing",
        action="store_true",
        help="Enable Stage 4: Extract holdings data from XML files",
    )
    parser.add_argument(
        "--enable-ticker-enrichment",
        action="store_true",
        help="Enable Stage 5: Enrich holdings with ticker symbols via OpenFIGI",
    )
    parser.add_argument(
        "--enable-all-stages",
        action="store_true",
        help="Enable all pipeline stages (discovery, download, processing, enrichment)",
    )
    parser.add_argument(
        "--ticker", "-t",
        type=str,
        help="Filter by specific ticker symbol (e.g., AGGH, MAXI)"
    )

    args = parser.parse_args()

    # Parse form types
    form_types = [ft.strip() for ft in args.form_types.split(",") if ft.strip()]
    
    # Handle --enable-all-stages flag
    enable_all = args.enable_all_stages
    
    # Create config
    config = WorkflowPostgresConfig(
        environment=args.env,
        provider_filter=args.provider,
        ticker_filter=args.ticker,
        enable_filing_discovery=not args.disable_filing_discovery,
        target_form_types=form_types,
        max_filings_per_series=args.max_filings,
        enable_xml_download=args.enable_xml_download or enable_all,
        enable_holdings_processing=args.enable_holdings_processing or enable_all,
        enable_ticker_enrichment=args.enable_ticker_enrichment or enable_all,
    )

    # Create and run workflow
    workflow = FundHoldingsWorkflowPostgres(config)

    if args.summary_only:
        workflow.print_provider_summary()
    else:
        workflow.run_basic_iteration()


if __name__ == "__main__":
    main()
