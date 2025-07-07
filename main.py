from loguru import logger

from fh.sec_client import SECHTTPClient


CIK_MAP = {
    # fund issuer => cik
    "ishares": "1100663",
}

def download_series_data(client, cik):
    series_data = client.fetch_series_data(cik)
    saved_file = client.save_series_data(series_data, cik)
    logger.debug(f"Series data saved to: {saved_file}")

    # Show key series information
    logger.debug("\nKey series found:")
    for i, series in enumerate(series_data[:10], 1):  # Show first 10 only
        if 'series_id' in series and 'class_info' in series:
            logger.debug(f"  {i}. Series: {series.get('series_id', 'N/A')} - {series.get('class_info', 'N/A')}")
    
    if len(series_data) > 10:
        logger.debug(f"  ... and {len(series_data) - 10} more series")
        
    return series_data, saved_file


def download_all_series_data(client):
    for fund_issuer_key, cik in CIK_MAP.items():
        logger.debug(f"Downloading series data for {fund_issuer_key} (CIK: {cik})")
        _series_data, _saved_file = download_series_data(client, cik)


def test_nport_download():
    """Test N-PORT XML download functionality."""
    client = SECHTTPClient()
    
    # Test with specific CIK and accession number from your data
    test_cik = "1100663"
    test_accession = "0001752724-25-119791"
    
    logger.info(f"Testing N-PORT download for CIK: {test_cik}, accession: {test_accession}")
    
    # Build URLs and show them
    xml_url = client.build_nport_url(test_cik, test_accession)
    index_url = client.build_nport_index_url(test_cik, test_accession)
    
    logger.info(f"N-PORT XML URL: {xml_url}")
    logger.info(f"N-PORT Index URL: {index_url}")
    logger.info(f"Expected working URL: https://www.sec.gov/Archives/edgar/data/1100663/000175272425119791/primary_doc.xml")
    
    # Download and save (using a test series ID)
    test_series_id = "S000004310"  # Add series ID for proper filename
    saved_file = client.download_and_save_nport(test_cik, test_accession, test_series_id)
    
    if saved_file:
        logger.info(f"N-PORT XML successfully saved to: {saved_file}")
        
        # Show file size
        import os
        file_size = os.path.getsize(saved_file)
        logger.info(f"File size: {file_size:,} bytes")
        
        # Show first few lines
        with open(saved_file, 'r', encoding='utf-8') as f:
            first_lines = ''.join(f.readlines()[:5])
            logger.info(f"First lines of XML:\n{first_lines}")
    else:
        logger.error("Failed to download N-PORT XML")


def download_nport_filings_for_series():
    """Download N-PORT XML files for all filings of a specific series."""
    client = SECHTTPClient()
    
    # Test parameters from your existing data
    test_cik = "1100663"
    test_series_id = "S000004310"
    
    logger.info(f"Loading series filings for CIK: {test_cik}, Series: {test_series_id}")
    
    # Load series filings data
    series_filings_data = client.load_series_filings(test_cik, test_series_id)
    
    if not series_filings_data:
        logger.error(f"No series filings data found for CIK: {test_cik}, Series: {test_series_id}")
        return
    
    filings = series_filings_data.get('filings', [])
    logger.info(f"Found {len(filings)} filings for series {test_series_id}")
    
    downloaded_files = []
    
    # Process each filing
    for i, filing in enumerate(filings[:3], 1):  # Limit to first 3 for testing
        accession_number = filing.get('accession_number')
        filing_date = filing.get('filing_date', 'Unknown')
        
        if not accession_number:
            logger.warning(f"Skipping filing {i}: No accession number found")
            continue
        
        logger.info(f"Processing filing {i}/{min(3, len(filings))}: {accession_number} (Date: {filing_date})")
        
        # Build URL and download
        xml_url = client.build_nport_url(test_cik, accession_number)
        logger.info(f"Downloading from: {xml_url}")
        
        saved_file = client.download_and_save_nport(test_cik, accession_number, test_series_id)
        
        if saved_file:
            import os
            file_size = os.path.getsize(saved_file)
            logger.info(f"✓ Downloaded: {saved_file} ({file_size:,} bytes)")
            downloaded_files.append({
                'accession': accession_number,
                'file_path': saved_file,
                'file_size': file_size,
                'filing_date': filing_date
            })
        else:
            logger.error(f"✗ Failed to download: {accession_number}")
    
    # Summary
    logger.info(f"\n=== Download Summary ===")
    logger.info(f"Series: {test_series_id}")
    logger.info(f"Total filings processed: {min(3, len(filings))}")
    logger.info(f"Successfully downloaded: {len(downloaded_files)}")
    
    for file_info in downloaded_files:
        logger.info(f"  - {file_info['accession']} ({file_info['filing_date']}) -> {file_info['file_path']}")
    
    return downloaded_files


def main():
    # Initialize SEC client
    client = SECHTTPClient()

    download_all_series_data(client)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "test-nport":
            test_nport_download()
        elif sys.argv[1] == "download-series":
            download_nport_filings_for_series()
        else:
            main()
    else:
        # Default to testing the new bulk download function
        download_nport_filings_for_series()
