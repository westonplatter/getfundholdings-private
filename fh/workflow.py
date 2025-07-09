#!/usr/bin/env python3
"""
Simple workflow orchestration for fund holdings data pipeline.

This module provides a clean interface to execute the complete pipeline:
CIK → Series/Class → N-PORT Filings → XML Data → Holdings → Enriched Data
"""

import time
import os
from typing import List, Dict, Any, Optional
from loguru import logger
import pandas as pd
from dataclasses import dataclass
from typing import List


from fh.sec_client import SECHTTPClient
from fh.openfigi_client import OpenFIGIClient


@dataclass
class WorkflowConfig:
    """Simple configuration for workflow execution."""
    cik_list: List[str]
    data_dir: str = "data"
    enable_ticker_enrichment: bool = True
    max_series_per_cik: int = None  # None = no limit
    max_filings_per_series: int = None  # None = no limit
    user_agent: str = "Weston Platter westonplatter@gmail.com"
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
        
        for cik in self.config.cik_list:
            logger.info(f"Processing CIK: {cik}")
            
            try:
                cik_result = self._process_cik(cik)
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
    
    def _process_cik(self, cik: str) -> Dict[str, Any]:
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
            
            if self.config.max_series_per_cik:
                series_ids = series_ids[:self.config.max_series_per_cik]
            
            filings_files = []
            total_filings = 0
            
            for series_id in series_ids:
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
            
            # Step 3: Download N-PORT XML files
            logger.info(f"Step 3: Downloading N-PORT XML files for {total_filings} filings")
            xml_files = self._download_nport_xmls(cik, series_ids)
            result["steps_completed"].append("xml_download")
            result["output_files"].extend(xml_files)
            result["xml_files_downloaded"] = len(xml_files)
            
            # Step 4: Extract holdings data
            logger.info(f"Step 4: Extracting holdings data from {len(xml_files)} XML files")
            holdings_file = self._extract_holdings_data(cik, xml_files)
            if holdings_file:
                result["steps_completed"].append("holdings_extraction")
                result["output_files"].append(holdings_file)
                result["holdings_file"] = holdings_file
                
                # Step 5: Enrich with ticker data (if enabled)
                if self.config.enable_ticker_enrichment and self.openfigi_client:
                    logger.info(f"Step 5: Enriching holdings with ticker data")
                    enriched_file = self._enrich_with_tickers(holdings_file, cik)
                    if enriched_file:
                        result["steps_completed"].append("ticker_enrichment")
                        result["output_files"].append(enriched_file)
                        result["enriched_file"] = enriched_file
            
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
    
    def _download_nport_xmls(self, cik: str, series_ids: List[str]) -> List[str]:
        """Download N-PORT XML files for all series."""
        xml_files = []
        
        for series_id in series_ids:
            try:
                # Load series filings
                series_filings_data = self.sec_client.load_series_filings(cik, series_id)
                if not series_filings_data:
                    continue
                
                filings = series_filings_data.get('filings', [])
                
                # Limit filings if configured
                if self.config.max_filings_per_series:
                    filings = filings[:self.config.max_filings_per_series]
                
                for filing in filings:
                    accession_number = filing.get('accession_number')
                    if accession_number:
                        xml_file = self.sec_client.download_and_save_nport(cik, accession_number, series_id)
                        if xml_file:
                            xml_files.append(xml_file)
                            
            except Exception as e:
                logger.warning(f"Failed to download XML for series {series_id}: {e}")
        
        return xml_files
    
    def _extract_holdings_data(self, cik: str, xml_files: List[str]) -> Optional[str]:
        """Extract holdings data from XML files and combine into single CSV."""
        try:
            # Import here to avoid circular imports
            from parse_nport import parse_nport_file
            
            all_holdings = []
            all_fund_info = []
            
            for xml_file in xml_files:
                try:
                    if os.path.exists(xml_file):
                        holdings_df, fund_info = parse_nport_file(xml_file)
                        if not holdings_df.empty:
                            # Add source file info
                            holdings_df['source_file'] = os.path.basename(xml_file)
                            all_holdings.append(holdings_df)
                            all_fund_info.append(fund_info)
                except Exception as e:
                    logger.warning(f"Failed to parse {xml_file}: {e}")
            
            if all_holdings:
                # Combine all holdings
                combined_holdings = pd.concat(all_holdings, ignore_index=True)
                
                # Save combined holdings
                timestamp = int(time.time())
                holdings_file = os.path.join(self.config.data_dir, f"holdings_{cik}_{timestamp}.csv")
                combined_holdings.to_csv(holdings_file, index=False, quoting=1)  # QUOTE_ALL
                
                logger.info(f"Saved {len(combined_holdings)} holdings to {holdings_file}")
                return holdings_file
                
        except Exception as e:
            logger.error(f"Failed to extract holdings data: {e}")
        
        return None
    
    def _enrich_with_tickers(self, holdings_file: str, cik: str) -> Optional[str]:
        """Enrich holdings data with ticker symbols using both CUSIP and ISIN lookups."""
        try:
            # Load holdings data
            holdings_df = pd.read_csv(holdings_file)
            logger.info(f"Starting ticker enrichment for {len(holdings_df)} holdings")
            
            # Step 1: Try CUSIP lookup first
            logger.info("Step 1: Attempting ticker lookup via CUSIP")
            enriched_df = self.openfigi_client.add_tickers_to_dataframe(holdings_df, cusip_column='cusip')
            
            # Step 2: For failed CUSIP lookups, try ISIN lookup
            failed_cusip_mask = enriched_df['ticker'].isna()
            failed_cusip_count = failed_cusip_mask.sum()
            
            if failed_cusip_count > 0 and 'isin' in enriched_df.columns:
                logger.info(f"Step 2: {failed_cusip_count} holdings failed CUSIP lookup, trying ISIN lookup")
                
                # Get holdings that failed CUSIP lookup but have ISINs
                failed_cusip_holdings = enriched_df[failed_cusip_mask].copy()
                has_isin_mask = (failed_cusip_holdings['isin'].notna()) & (failed_cusip_holdings['isin'] != '')
                isin_candidates = failed_cusip_holdings[has_isin_mask]
                
                if not isin_candidates.empty:
                    logger.info(f"Found {len(isin_candidates)} holdings with ISINs to try")
                    
                    # Try ISIN lookup for these holdings
                    isin_enriched = self.openfigi_client.add_tickers_to_dataframe_by_isin(
                        isin_candidates, isin_column='isin'
                    )
                    
                    # Update the main dataframe with successful ISIN lookups
                    # Only update where ISIN lookup succeeded (ticker is not null)
                    successful_isin_mask = isin_enriched['ticker'].notna()
                    if successful_isin_mask.any():
                        successful_isin_lookups = isin_enriched[successful_isin_mask]
                        logger.info(f"ISIN lookup successful for {len(successful_isin_lookups)} additional holdings")
                        
                        # Update the ticker column for these rows in the original dataframe
                        for idx in successful_isin_lookups.index:
                            enriched_df.loc[idx, 'ticker'] = successful_isin_lookups.loc[idx, 'ticker']
                else:
                    logger.info("No holdings with valid ISINs found for fallback lookup")
            elif failed_cusip_count > 0:
                logger.info(f"{failed_cusip_count} holdings failed CUSIP lookup, but no ISIN column available")
            
            # Final summary
            total_with_tickers = enriched_df['ticker'].notna().sum()
            total_without_tickers = len(enriched_df) - total_with_tickers
            success_rate = total_with_tickers / len(enriched_df) * 100 if len(enriched_df) > 0 else 0
            
            logger.info(f"Ticker enrichment complete: {total_with_tickers}/{len(enriched_df)} holdings have tickers ({success_rate:.1f}%)")
            if total_without_tickers > 0:
                logger.warning(f"{total_without_tickers} holdings still missing tickers after both CUSIP and ISIN lookup attempts")
            
            # Save enriched data
            timestamp = int(time.time())
            enriched_file = os.path.join(self.config.data_dir, f"holdings_enriched_{cik}_{timestamp}.csv")
            enriched_df.to_csv(enriched_file, index=False, quoting=1)  # QUOTE_ALL
            
            logger.info(f"Saved enriched holdings to {enriched_file}")
            return enriched_file
            
        except Exception as e:
            logger.error(f"Failed to enrich holdings with tickers: {e}")
        
        return None


def main():
    """Example usage of the workflow."""
    config = WorkflowConfig(
        cik_list=["1100663"],  # iShares
        enable_ticker_enrichment=True,
        max_series_per_cik=1,
        max_filings_per_series=1,
        interested_etf_tickers=["IVV"]
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