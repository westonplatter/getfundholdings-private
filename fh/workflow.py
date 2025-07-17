#!/usr/bin/env python3
"""
Simple workflow orchestration for fund holdings data pipeline.

This module provides a clean interface to execute the complete pipeline:
CIK → Series/Class → N-PORT Filings → XML Data → Holdings → Enriched Data
"""

import time
import os
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from loguru import logger
import pandas as pd
from dataclasses import dataclass
from typing import List
from tqdm import tqdm


from fh.sec_client import SECHTTPClient
from fh.openfigi_client import OpenFIGIClient


CIK_MAP = {
    # fund issuer => cik
    "iShares Trust": "1100663",
    "iShares, Inc.": "0000930667",
    "jpmorgan": "0001485894",
    "blackrock": "0001761055",
}


@dataclass
class WorkflowConfig:
    """Simple configuration for workflow execution."""
    cik_list: List[str]
    data_dir: str = "data"
    enable_ticker_enrichment: bool = True
    max_series_per_cik: int = None  # None = no limit
    max_filings_per_series: int = None  # None = no limit
    user_agent: str = "GetFundHoldings.com admin@getfundholdings.com"
    interested_etf_tickers: Optional[List[str]] = None


class FundHoldingsWorkflow:
    """
    Simple workflow orchestrator for fund holdings data pipeline.
    
    Uses existing file-based methods from SEC client and other components.
    """
    
    def __init__(self, config: WorkflowConfig):
        """Initialize workflow with configuration."""
        self.config = config
        self.sec_client = SECHTTPClient(user_agent=config.user_agent)
        self.openfigi_client = OpenFIGIClient() if config.enable_ticker_enrichment else None
        
    def run(self) -> Dict[str, Any]:
        """
        Execute complete workflow for all CIKs in config.
        
        Returns:
            Dictionary with results summary
        """
        results = {
            "total_ciks": len(self.config.cik_list),
            "successful_ciks": 0,
            "failed_ciks": 0,
            "cik_results": {}
        }
        
        start_time = time.time()
        
        for cik in tqdm(self.config.cik_list, desc="Processing CIKs", unit="CIK"):
            logger.info(f"Processing CIK: {cik}")
            
            try:
                cik_result = self.process_cik(cik)
                results["cik_results"][cik] = cik_result
                
                if cik_result.get("success", False):
                    results["successful_ciks"] += 1
                else:
                    results["failed_ciks"] += 1
                    
            except Exception as e:
                logger.error(f"Failed to process CIK {cik}: {e}")
                results["failed_ciks"] += 1
                results["cik_results"][cik] = {
                    "success": False,
                    "error": str(e)
                }
        
        results["total_execution_time"] = time.time() - start_time
        
        logger.info(f"Workflow completed: {results['successful_ciks']}/{results['total_ciks']} CIKs successful")
        return results
    
    def process_cik(self, cik: str) -> Dict[str, Any]:
        """Process a single CIK through the complete pipeline."""
        result = {
            "cik": cik,
            "success": False,
            "steps_completed": [],
            "output_files": []
        }
        
        start_time = time.time()
        
        try:
            # Step 1: Fetch series data
            logger.info(f"Step 1: Fetching series data for CIK {cik}")
            series_data = self.sec_client.fetch_series_data(cik)
            series_file = self.sec_client.save_series_data(series_data, cik)
            result["steps_completed"].append("series_data")
            result["output_files"].append(series_file)
            result["total_series"] = len(series_data)

            # Filter series by interested ETF tickers if specified
            if self.config.interested_etf_tickers:
                filtered_series = []
                for series in series_data:
                    # Check if any class in this series has a ticker we're interested in
                    for class_info in series.get('classes', []):
                        ticker = class_info.get('ticker', '')
                        if ticker in self.config.interested_etf_tickers:
                            filtered_series.append(series)
                            break  # Found a match, no need to check other classes
                series_data = filtered_series
                logger.info(f"Filtered to {len(series_data)} series with tickers: {self.config.interested_etf_tickers}")
            
            # Step 2: Collect N-PORT filings for all series
            logger.info(f"Step 2: Collecting N-PORT filings for {len(series_data)} series")
            series_ids = self._extract_series_ids(series_data)
            
            if self.config.max_series_per_cik and self.config.max_series_per_cik > 0:
                series_ids = series_ids[:self.config.max_series_per_cik]
            
            filings_files = []
            total_filings = 0
            
            for series_id in tqdm(series_ids, desc=f"Collecting filings for {len(series_ids)} series", unit="series", leave=False):
                try:
                    series_filings = self.sec_client.fetch_series_filings(series_id)
                    if series_filings:
                        filings_file = self.sec_client.save_series_filings(series_filings, series_id, cik=cik)
                        filings_files.append(filings_file)
                        total_filings += len(series_filings)
                except Exception as e:
                    logger.warning(f"Failed to fetch filings for series {series_id}: {e}")
            
            result["steps_completed"].append("filings_collection")
            result["output_files"].extend(filings_files)
            result["total_filings"] = total_filings
            
            # Create series_id to ticker mapping
            series_ticker_map = self._create_series_ticker_mapping(series_data)
            
            # Step 3: Download N-PORT XML files
            logger.info(f"Step 3: Downloading N-PORT XML files for {total_filings} filings")
            xml_files = self._download_nport_xmls(cik, series_ids)
            result["steps_completed"].append("xml_download")
            result["output_files"].extend(xml_files)
            result["xml_files_downloaded"] = len(xml_files)
            
            # Step 4: Extract holdings data
            logger.info(f"Step 4: Extracting holdings data from {len(xml_files)} XML files")
            holdings_files = self.extract_holdings_data(cik, xml_files, series_ticker_map)
            if holdings_files:
                result["steps_completed"].append("holdings_extraction")
                result["output_files"].extend(holdings_files)
                result["holdings_files"] = holdings_files
                
                # Step 5: Enrich with ticker data (if enabled)
                if self.config.enable_ticker_enrichment and self.openfigi_client:
                    logger.info(f"Step 5: Enriching holdings with ticker data")
                    enriched_files = []
                    for holdings_file in tqdm(holdings_files, desc=f"Enriching {len(holdings_files)} holdings files", unit="file", leave=False):
                        enriched_file = self.enrich_holdings(holdings_file, cik, series_ticker_map)
                        if enriched_file:
                            enriched_files.append(enriched_file)
                    
                    if enriched_files:
                        result["steps_completed"].append("holdings_enrichment")
                        result["output_files"].extend(enriched_files)
                        result["enriched_files"] = enriched_files
            
            result["success"] = True
            result["execution_time"] = time.time() - start_time
            
        except Exception as e:
            logger.error(f"Error processing CIK {cik}: {e}")
            result["error"] = str(e)
            result["execution_time"] = time.time() - start_time
        
        return result
    
    def _extract_series_ids(self, series_data: List[Dict]) -> List[str]:
        """Extract valid series IDs from series data."""
        series_ids = []
        
        for series in series_data:
            series_id = series.get('series_id', '')
            if series_id and series_id.startswith('S') and len(series_id) > 5:
                if 'Home' not in series_id and 'EDGAR' not in series_id:
                    series_ids.append(series_id)
        
        return list(set(series_ids))  # Remove duplicates
    
    def _create_series_ticker_mapping(self, series_data: List[Dict]) -> Dict[str, str]:
        """Create mapping from series_id to fund ticker from series data."""
        series_ticker_map = {}
        
        for series in series_data:
            series_id = series.get('series_id', '')
            if series_id and series_id.startswith('S') and len(series_id) > 5:
                # Look for ticker in the classes
                classes = series.get('classes', [])
                for class_info in classes:
                    ticker = class_info.get('ticker', '')
                    if ticker:
                        series_ticker_map[series_id] = ticker
                        logger.debug(f"Mapped series {series_id} to ticker {ticker}")
                        break  # Use the first ticker found for this series
        
        logger.info(f"Created series to ticker mapping for {len(series_ticker_map)} series")
        return series_ticker_map
    
    def _download_nport_xmls(self, cik: str, series_ids: List[str]) -> List[str]:
        """Download N-PORT XML files for all series."""
        xml_files = []
        
        for series_id in tqdm(series_ids, desc=f"Downloading XMLs for {len(series_ids)} series", unit="series", leave=False):
            try:
                # Load series filings
                series_filings_data = self.sec_client.load_series_filings(cik, series_id)
                if not series_filings_data:
                    continue
                
                filings = series_filings_data.get('filings', [])
                
                # Limit filings if configured
                if self.config.max_filings_per_series:
                    filings = filings[:self.config.max_filings_per_series]
                
                for filing in tqdm(filings, desc=f"Downloading filings for {series_id}", unit="filing", leave=False):
                    accession_number = filing.get('accession_number')
                    if accession_number:
                        xml_file = self.sec_client.download_and_save_nport(cik, accession_number, series_id)
                        if xml_file:
                            xml_files.append(xml_file)
                            
            except Exception as e:
                logger.warning(f"Failed to download XML for series {series_id}: {e}")
        
        return xml_files
    
    def extract_holdings_data(self, cik: str, xml_files: List[str], series_ticker_map: Dict[str, str]) -> List[str]:
        """Extract holdings data from XML files and save individual CSV files."""
        holdings_files = []
        
        try:
            # Import here to avoid circular imports
            from parse_nport import parse_nport_file
            
            current_date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            for xml_file in tqdm(xml_files, desc=f"Extracting holdings from {len(xml_files)} XML files", unit="file", leave=False):
                try:
                    if os.path.exists(xml_file):
                        holdings_df, fund_info = parse_nport_file(xml_file)
                        if not holdings_df.empty:
                            # Extract fund info for filename
                            series_id = fund_info.get('series_id', 'unknown')
                            report_date = fund_info.get('report_period_date', 'unknown')
                            
                            # Get fund ticker from mapping
                            fund_ticker = series_ticker_map.get(series_id, 'unknown')
                            
                            # Add metadata columns
                            holdings_df['source_file'] = os.path.basename(xml_file)
                            holdings_df['fund_ticker'] = fund_ticker
                            holdings_df['series_id'] = series_id
                            
                            # Format report date if it's available
                            if report_date and report_date != 'unknown':
                                try:
                                    # Assuming report_date is in YYYY-MM-DD format already
                                    # If not, you might need to parse and reformat it
                                    report_date_str = report_date.replace('-', '')  # Remove dashes for filename
                                except:
                                    report_date_str = 'unknown'
                            else:
                                report_date_str = 'unknown'
                            
                            # Create filename with fund ticker
                            holdings_file = os.path.join(
                                self.config.data_dir, 
                                f"holdings_{fund_ticker}_{cik}_{series_id}_{report_date_str}_{current_date_str}.csv"
                            )
                            
                            # Save individual holdings file
                            holdings_df.to_csv(holdings_file, index=False, quoting=1)  # QUOTE_ALL
                            holdings_files.append(holdings_file)
                            
                            logger.info(f"Saved {len(holdings_df)} holdings for {fund_ticker} to {holdings_file}")
                            
                except Exception as e:
                    logger.warning(f"Failed to parse {xml_file}: {e}")
            
            logger.info(f"Created {len(holdings_files)} holdings files")
            return holdings_files
                
        except Exception as e:
            logger.error(f"Failed to extract holdings data: {e}")
        
        return []

    def enrich_holdings(self, holdings_file: str, cik: str, series_ticker_map: Dict[str, str]) -> Optional[str]:
        """Parent enrichment method that coordinates all enrichment steps."""
        try:
            # Load holdings data
            holdings_df = pd.read_csv(holdings_file)
            logger.info(f"Starting holdings enrichment for {len(holdings_df)} holdings")
            
            # Extract metadata from filename
            metadata = self._extract_file_metadata(holdings_file, holdings_df)
            
            # Create enriched dataframe
            enriched_df = holdings_df.copy()
            
            # Apply all enrichment steps
            enriched_df = self._enrich_metadata(enriched_df, metadata)
            enriched_df = self._enrich_timestamps(enriched_df)
            enriched_df = self._enrich_tickers(enriched_df)
            enriched_df = self._enrich_notes(enriched_df)
            
            # Save enriched data
            enriched_file = self._save_enriched_data(enriched_df, metadata)
            
            logger.info(f"Holdings enrichment complete for {metadata['fund_ticker']}")
            return enriched_file
            
        except Exception as e:
            logger.error(f"Failed to enrich holdings: {e}")
            return None

    def _extract_file_metadata(self, holdings_file: str, holdings_df: pd.DataFrame) -> Dict[str, str]:
        """Extract metadata from filename and DataFrame."""
        filename = os.path.basename(holdings_file)
        filename_parts = filename.replace('.csv', '').split('_')
        
        if len(filename_parts) >= 6:
            return {
                'fund_ticker': filename_parts[1],
                'cik': filename_parts[2],
                'series_id': filename_parts[3],
                'report_date': filename_parts[4],
                'filename': filename
            }
        else:
            # Fallback: try to get from DataFrame
            return {
                'fund_ticker': holdings_df.get('fund_ticker', ['unknown']).iloc[0] if 'fund_ticker' in holdings_df.columns and not holdings_df.empty else 'unknown',
                'cik': 'unknown',
                'series_id': holdings_df.get('series_id', ['unknown']).iloc[0] if 'series_id' in holdings_df.columns and not holdings_df.empty else 'unknown',
                'report_date': 'unknown',
                'filename': filename
            }

    def _enrich_metadata(self, enriched_df: pd.DataFrame, metadata: Dict[str, str]) -> pd.DataFrame:
        """Enrich with CIK, series, and report date metadata."""
        logger.info("Enriching with metadata (CIK, series, report date)")
        
        # Add report date column if it doesn't exist
        if 'report_period_date' not in enriched_df.columns:
            enriched_df['report_period_date'] = metadata['report_date']
        
        # Ensure fund_ticker and series_id columns exist
        if 'fund_ticker' not in enriched_df.columns:
            enriched_df['fund_ticker'] = metadata['fund_ticker']
        if 'series_id' not in enriched_df.columns:
            enriched_df['series_id'] = metadata['series_id']
            
        return enriched_df

    def _enrich_timestamps(self, enriched_df: pd.DataFrame) -> pd.DataFrame:
        """Enrich with timestamp data."""
        logger.info("Enriching with timestamps")
        
        # Add enrichment datetime in UTC (timezone-aware)
        enrichment_datetime_utc = datetime.now(timezone.utc)
        enriched_df['enrichment_datetime'] = enrichment_datetime_utc
        
        return enriched_df

    def _enrich_tickers(self, enriched_df: pd.DataFrame) -> pd.DataFrame:
        """Enrich with ticker symbols using CUSIP and ISIN lookups."""
        if not self.openfigi_client:
            logger.info("Ticker enrichment disabled - no OpenFIGI client available")
            enriched_df['ticker'] = None
            return enriched_df
            
        logger.info("Enriching with ticker symbols")
        
        # Initialize ticker column
        enriched_df['ticker'] = None
        
        # Identify derivative instruments to exclude
        derivative_mask = self._identify_derivative_instruments(enriched_df)
        logger.info(f"Excluding {derivative_mask.sum()} derivative instruments from ticker lookup")
        
        # Step 1: CUSIP lookup
        enriched_df = self._enrich_tickers_by_cusip(enriched_df, derivative_mask)
        
        # Step 2: ISIN fallback lookup
        enriched_df = self._enrich_tickers_by_isin(enriched_df, derivative_mask)
        
        # Log final ticker enrichment results
        self._log_ticker_enrichment_results(enriched_df, derivative_mask)
        
        return enriched_df

    def _identify_derivative_instruments(self, enriched_df: pd.DataFrame) -> pd.Series:
        """Identify derivative instruments that should be excluded from ticker lookup."""
        def is_derivative_instrument(name: str, title: str) -> bool:
            name_lower = str(name).lower() if pd.notna(name) else ""
            title_lower = str(title).lower() if pd.notna(title) else ""
            
            derivative_indicators = [
                "eln,", "equity linked note", "linked to nasdaq", "linked to s&p",
                "total return swap", "trs", "swap agreement", "derivative"
            ]
            
            return any(indicator in name_lower or indicator in title_lower 
                      for indicator in derivative_indicators)
        
        return enriched_df.apply(
            lambda row: is_derivative_instrument(row.get('name', ''), row.get('title', '')), 
            axis=1
        )


    def _enrich_tickers_by_cusip(self, enriched_df: pd.DataFrame, derivative_mask: pd.Series) -> pd.DataFrame:
        """Enrich tickers using CUSIP lookup."""
        missing_ticker_mask = enriched_df['ticker'].isna() & ~derivative_mask
        missing_ticker_count = missing_ticker_mask.sum()
        
        if missing_ticker_count > 0 and 'cusip' in [col.lower() for col in enriched_df.columns]:
            logger.info(f"{missing_ticker_count} holdings missing tickers, trying CUSIP lookup")
            enriched_df = self.openfigi_client.add_tickers_to_dataframe_by_cusip(enriched_df, cusip_column='cusip')
        else:
            logger.info("No CUSIP column found or no holdings need CUSIP lookup")
            
        return enriched_df

    def _enrich_tickers_by_isin(self, enriched_df: pd.DataFrame, derivative_mask: pd.Series) -> pd.DataFrame:
        """Enrich tickers using ISIN lookup."""
        missing_ticker_mask = enriched_df['ticker'].isna() & ~derivative_mask
        missing_ticker_count = missing_ticker_mask.sum()
        
        if missing_ticker_count > 0 and 'isin' in [col.lower() for col in enriched_df.columns]:
            logger.info(f"{missing_ticker_count} holdings missing tickers, trying ISIN lookup")
            enriched_df = self.openfigi_client.add_tickers_to_dataframe_by_isin(enriched_df, isin_column='isin')
        else:
            logger.info("No ISIN column found or no holdings need ISIN lookup")
            
        return enriched_df

    def _log_ticker_enrichment_results(self, enriched_df: pd.DataFrame, derivative_mask: pd.Series) -> None:
        """Log final ticker enrichment results and warnings."""
        missing_ticker_mask = enriched_df['ticker'].isna() & ~derivative_mask
        missing_ticker_count = missing_ticker_mask.sum()
        non_derivative_count = (~derivative_mask).sum()
        success_rate = (non_derivative_count - missing_ticker_count) / non_derivative_count * 100 if non_derivative_count > 0 else 0
        
        logger.info(f"Ticker enrichment complete: {non_derivative_count - missing_ticker_count}/{non_derivative_count} non-derivative holdings have tickers ({success_rate:.1f}%)")
        logger.info(f"Excluded {derivative_mask.sum()} derivative instruments (ELNs, swaps, etc.) from ticker lookup")
        
        if missing_ticker_count > 0:
            logger.warning(f"{missing_ticker_count} non-derivative holdings still missing tickers after both CUSIP and ISIN lookup attempts")
            for idx, row in enriched_df[missing_ticker_mask].iterrows():
                logger.warning(f"  - {row['name']}/{row['title']}- ISIN: {row['isin']}, CUSIP: {row['cusip']}")

    def _enrich_notes(self, enriched_df: pd.DataFrame) -> pd.DataFrame:
        """Enrich with notes about data quality issues."""
        logger.info("Enriching with data quality notes")
        
        # Initialize notes column
        enriched_df['enrichment_notes'] = ""
        
        # Add notes for missing identifiers
        missing_cusip = enriched_df['cusip'].isna() | (enriched_df['cusip'] == '')
        missing_isin = enriched_df['isin'].isna() | (enriched_df['isin'] == '')
        missing_ticker = enriched_df['ticker'].isna() | (enriched_df['ticker'] == '')
        
        # Derivative instruments note
        derivative_mask = self._identify_derivative_instruments(enriched_df)
        enriched_df.loc[derivative_mask, 'enrichment_notes'] += "derivative_instrument; "
        
        # Missing identifier notes
        enriched_df.loc[missing_cusip, 'enrichment_notes'] += "missing_cusip; "
        enriched_df.loc[missing_isin, 'enrichment_notes'] += "missing_isin; "
        enriched_df.loc[missing_ticker & ~derivative_mask, 'enrichment_notes'] += "missing_ticker; "
        
        # Clean up notes (remove trailing semicolons and spaces)
        enriched_df['enrichment_notes'] = enriched_df['enrichment_notes'].str.rstrip('; ')
        
        return enriched_df

    def _save_enriched_data(self, enriched_df: pd.DataFrame, metadata: Dict[str, str]) -> str:
        """Save the enriched DataFrame to a CSV file."""
        date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        enriched_file = os.path.join(
            self.config.data_dir, 
            f"holdings_enriched_{metadata['fund_ticker']}_{metadata['cik']}_{metadata['series_id']}_{metadata['report_date']}_{date_str}.csv"
        )
        enriched_df.to_csv(enriched_file, index=False, quoting=1)  # QUOTE_ALL
        
        logger.info(f"Saved enriched holdings for {metadata['fund_ticker']} to {enriched_file}")
        return enriched_file




def main():
    """Example usage of the workflow."""
    config = WorkflowConfig(
        cik_list=CIK_MAP.values(),
        enable_ticker_enrichment=True,
        max_series_per_cik=None,
        max_filings_per_series=1,
        # interested_etf_tickers=["JEPI", "JEPQ", "IVV"]
        interested_etf_tickers=["URTH"]
    )
    
    workflow = FundHoldingsWorkflow(config)
    results = workflow.run()
    
    logger.debug(f"Workflow Results:")
    logger.debug(f"- Total CIKs: {results['total_ciks']}")
    logger.debug(f"- Successful: {results['successful_ciks']}")
    logger.debug(f"- Failed: {results['failed_ciks']}")
    logger.debug(f"- Execution Time: {results['total_execution_time']:.2f} seconds")
    
    for cik, result in results['cik_results'].items():
        logger.debug(f"\nCIK {cik}:")
        logger.debug(f"  Success: {result.get('success', False)}")
        logger.debug(f"  Steps: {result.get('steps_completed', [])}")
        logger.debug(f"  Files: {len(result.get('output_files', []))}")


if __name__ == "__main__":
    main()