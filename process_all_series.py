import argparse
from loguru import logger
from fh.sec_client import SECHTTPClient
import loguru

def find_series_by_ticker(client, cik, ticker):
    """
    Find series IDs that have classes matching the given ticker.
    
    Args:
        client: SECHTTPClient instance
        cik: Company CIK number  
        ticker: Ticker symbol to search for (e.g., "IVV")
        
    Returns:
        List of series IDs that have classes with matching ticker
    """
    logger.info(f"Searching for ticker '{ticker}' in CIK {cik}")
    
    # Load existing series data
    series_data = client.load_series_data(cik)
    
    if not series_data:
        logger.error(f"No series data found for CIK {cik}")
        return []
    
    matching_series = []
    
    for series in series_data.get('series_data', []):
        series_id = series.get('series_id', '')
        
        # Skip invalid series IDs
        if not series_id.startswith('S'):
            continue
            
        # Check classes for matching ticker
        for class_info in series.get('classes', []):
            if class_info.get('ticker') == ticker:
                matching_series.append(series_id)
                logger.info(f"Found ticker '{ticker}' in series {series_id}: {class_info.get('class_name', 'N/A')}")
                break  # Found match, no need to check other classes in this series
    
    logger.info(f"Found {len(matching_series)} series with ticker '{ticker}'")
    return matching_series

def main():
    parser = argparse.ArgumentParser(description="Process N-PORT filings for series data")
    parser.add_argument("--ticker", "-t", help="Filter by ticker symbol (e.g., IVV)")
    parser.add_argument("--cik", default="1100663", help="CIK to process (default: 1100663)")
    
    args = parser.parse_args()
    
    if args.ticker:
        print(f"Processing N-PORT filings for ticker: {args.ticker}")
    else:
        print("Processing N-PORT filings for all series from existing data")
    
    # Initialize SEC client
    client = SECHTTPClient()
    
    cik = args.cik
    logger.debug(f"\n{'='*60}")
    logger.debug(f"Processing CIK: {cik}")
    if args.ticker:
        logger.debug(f"Filtering by ticker: {args.ticker}")
    logger.debug(f"{'='*60}")
    
    try:
        if args.ticker:
            # Find series that match the ticker
            matching_series_ids = find_series_by_ticker(client, cik, args.ticker)
            
            if not matching_series_ids:
                logger.warning(f"No series found with ticker '{args.ticker}' for CIK {cik}")
                return
            
            # Process only the matching series
            saved_files = {}
            for series_id in matching_series_ids:
                logger.info(f"Processing series {series_id} for ticker {args.ticker}")
                
                series_filings = client.fetch_series_filings(series_id, "NPORT-P")
                
                if series_filings:
                    saved_file = client.save_series_filings(series_filings, series_id, "NPORT-P", cik)
                    saved_files[series_id] = saved_file
                    logger.info(f"Saved {len(series_filings)} NPORT-P filings for {series_id}")
                else:
                    logger.warning(f"No NPORT-P filings found for series {series_id}")
        else:
            # Process all series for this CIK
            saved_files = client.process_cik_series_filings(cik, "NPORT-P")
        
        if saved_files:
            logger.debug(f"\nSuccessfully processed {len(saved_files)} series:")
            for series_id, file_path in saved_files.items():
                logger.debug(f"  {series_id}: {file_path}")
        else:
            logger.debug(f"No series processed for CIK {cik}")
            
    except Exception as e:
        logger.debug(f"Error processing CIK {cik}: {e}")
        logger.exception(f"Failed to process CIK {cik}")

if __name__ == "__main__":
    main()