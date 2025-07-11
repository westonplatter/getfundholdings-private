"""
Simple script to fetch live SEC data and save it for testing.
Run this once to get the test data, then use it in tests.
"""

import json
from datetime import datetime
from loguru import logger

from fh.sec_client import SECHTTPClient


def main():
    logger.info("Fetching live SEC series data for testing...")
    
    # Initialize client and fetch data
    client = SECHTTPClient()
    cik = "1100663"
    
    logger.info(f"Fetching series data for CIK: {cik}")
    series_data = client.fetch_series_data(cik)
    
    logger.info(f"Fetched {len(series_data)} series records")
    
    # Save to test data file
    test_data = {
        "metadata": {
            "cik": cik,
            "fetch_date": datetime.now().isoformat(),
            "total_series": len(series_data),
            "description": "SEC series data for iShares Trust - for testing"
        },
        "series_data": series_data
    }
    
    filename = f"tests/fixtures/test_series_data_{cik}.json"
    with open(filename, 'w') as f:
        json.dump(test_data, f, indent=2, default=str)
    
    logger.info(f"✓ Saved test data to: {filename}")
    logger.info(f"✓ Ready for testing!")

# if __name__ == "__main__":
#     main()