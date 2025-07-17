#!/usr/bin/env python3
"""
Create Summary Tickers Data File

This script extracts ticker information using the same logic as r2_client.py
to create a summary data file for all ETFs that would be uploaded.
"""

import os
import re
import json
import argparse
from typing import Dict, List, Optional
from datetime import datetime
from loguru import logger


def find_latest_enriched_holdings_by_ticker(data_dir: str = "data") -> Dict[str, Dict]:
    """
    Find the latest enriched holdings CSV file for each ticker in the data directory.
    For each ticker, finds the file with the most recent report_date, and if there are
    multiple files with the same report_date, selects the one with the latest timestamp.
    
    Uses the same logic as R2Client.find_latest_enriched_holdings_by_ticker()
    
    Args:
        data_dir: Directory to search for enriched holdings files
        
    Returns:
        Dictionary mapping ticker symbols to their latest file info
    """
    try:
        if not os.path.exists(data_dir):
            logger.warning(f"Data directory does not exist: {data_dir}")
            return {}
        
        # Pattern: holdings_enriched_{ticker}_{cik}_{series_id}_{report_date}_{timestamp}.csv
        pattern = re.compile(r'^holdings_enriched_([A-Z]+)_(\d+)_([^_]+)_(\d{8})_(\d{8}_\d{6})\.csv$')
        
        ticker_files = {}
        
        for filename in os.listdir(data_dir):
            match = pattern.match(filename)
            if match:
                ticker, cik, series_id, report_date, timestamp = match.groups()
                file_path = os.path.join(data_dir, filename)
                
                # If this is the first file for this ticker
                if ticker not in ticker_files:
                    ticker_files[ticker] = {
                        'file_path': file_path,
                        'report_date': report_date,
                        'timestamp': timestamp,
                        'cik': cik,
                        'series_id': series_id,
                        'filename': filename
                    }
                else:
                    current_info = ticker_files[ticker]
                    
                    # Compare report dates first (more recent report date wins)
                    if report_date > current_info['report_date']:
                        ticker_files[ticker] = {
                            'file_path': file_path,
                            'report_date': report_date,
                            'timestamp': timestamp,
                            'cik': cik,
                            'series_id': series_id,
                            'filename': filename
                        }
                    # If same report date, compare timestamps (latest timestamp wins)
                    elif report_date == current_info['report_date'] and timestamp > current_info['timestamp']:
                        ticker_files[ticker] = {
                            'file_path': file_path,
                            'report_date': report_date,
                            'timestamp': timestamp,
                            'cik': cik,
                            'series_id': series_id,
                            'filename': filename
                        }
        
        logger.info(f"Found latest enriched holdings files for {len(ticker_files)} tickers")
        for ticker, info in ticker_files.items():
            logger.info(f"  {ticker}: {info['filename']} (report_date: {info['report_date']}, timestamp: {info['timestamp']})")
        
        return ticker_files
        
    except Exception as e:
        logger.error(f"Error finding latest enriched holdings files: {e}")
        return {}


def load_series_data(data_dir: str = "data") -> Dict[str, Dict]:
    """
    Load series data from JSON files to get fund names and metadata.
    
    Args:
        data_dir: Directory containing series data files
        
    Returns:
        Dictionary mapping series_id to fund metadata
    """
    series_metadata = {}
    
    try:
        # Find all series_data_*.json files
        for filename in os.listdir(data_dir):
            if filename.startswith('series_data_') and filename.endswith('.json'):
                file_path = os.path.join(data_dir, filename)
                
                logger.info(f"Loading series data from {filename}")
                
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                # Extract fund information from series data
                for series in data.get('series_data', []):
                    series_id = series.get('series_id')
                    if not series_id or series_id == 'Series':  # Skip header row
                        continue
                    
                    # Get the first class (fund) in the series
                    classes = series.get('classes', [])
                    if classes:
                        fund_class = classes[0]
                        class_name = fund_class.get('class_name')
                        ticker = fund_class.get('ticker')
                        
                        if ticker and ticker != 'Symbol':  # Skip header row
                            series_metadata[series_id] = {
                                'series_id': series_id,
                                'fund_name': class_name,
                                'ticker': ticker,
                                'cik': series.get('cik')
                            }
                            
    except Exception as e:
        logger.error(f"Error loading series data: {e}")
    
    logger.info(f"Loaded metadata for {len(series_metadata)} series")
    return series_metadata


def create_summary_tickers_data(data_dir: str = "data") -> List[Dict]:
    """
    Create summary ticker data combining enriched holdings files and series metadata.
    
    Args:
        data_dir: Directory containing data files
        
    Returns:
        List of ticker summary dictionaries
    """
    # Get latest enriched holdings files for each ticker
    latest_files = find_latest_enriched_holdings_by_ticker(data_dir)
    
    # Load series metadata
    series_metadata = load_series_data(data_dir)
    
    summary_data = []
    
    for ticker, file_info in latest_files.items():
        series_id = file_info['series_id']
        
        # Get fund metadata from series data
        metadata = series_metadata.get(series_id, {})
        fund_name = metadata.get('fund_name', '')
        
        # Create summary entry
        ticker_summary = {
            'ticker': ticker,
            'name': fund_name,
            'title': fund_name,  # Using fund name as title for now
            'cik': file_info['cik'],
            'series_id': series_id,
            'latest_report_date': file_info['report_date'],
            'latest_timestamp': file_info['timestamp'],
            'latest_file': file_info['filename'],
            'data_updated': datetime.now().isoformat()
        }
        
        summary_data.append(ticker_summary)
        
        logger.info(f"Added {ticker}: {fund_name}")
    
    # Sort by ticker for consistent output
    summary_data.sort(key=lambda x: x['ticker'])
    
    return summary_data


def main():
    """Main function to create summary tickers data file."""
    parser = argparse.ArgumentParser(description="Create and optionally upload summary tickers data file")
    parser.add_argument("--upload", action="store_true", help="Upload to R2 after creating the file")
    parser.add_argument("-e", "--env", choices=["dev", "prod"], default="dev", 
                       help="Environment to upload to (default: dev)")
    args = parser.parse_args()
    
    try:
        logger.info("Creating summary tickers data file...")
        
        # Create summary data
        summary_data = create_summary_tickers_data("data")
        
        if not summary_data:
            logger.warning("No ticker data found")
            return
        
        # Save to JSON file
        output_file = "data/summary_tickers.json"
        
        summary_output = {
            "metadata": {
                "total_tickers": len(summary_data),
                "generated_timestamp": datetime.now().isoformat(),
                "source_logic": "R2Client.find_latest_enriched_holdings_by_ticker",
                "description": "Summary of all ETF tickers with latest enriched holdings data available for upload"
            },
            "tickers": summary_data
        }
        
        with open(output_file, 'w') as f:
            json.dump(summary_output, f, indent=2)
        
        logger.info(f"Successfully created {output_file}")
        logger.info(f"Total tickers: {len(summary_data)}")
        logger.info(f"Sample tickers: {[t['ticker'] for t in summary_data[:5]]}")
        
        # Optionally upload to R2
        if args.upload:
            try:
                from fh.r2_client import R2Client
                logger.info(f"Uploading summary tickers to R2 ({args.env} environment)...")
                client = R2Client(bucket=args.env)
                success = client.upload_summary_tickers(output_file)
                
                if success:
                    logger.info("Successfully uploaded summary tickers to R2")
                else:
                    logger.error("Failed to upload summary tickers to R2")
                    
            except ImportError as e:
                logger.error(f"Could not import R2Client: {e}")
            except Exception as e:
                logger.error(f"Error uploading to R2: {e}")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")


if __name__ == "__main__":
    main()