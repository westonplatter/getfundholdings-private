"""
Simple script to fetch live SEC data and save it for testing.
Run this once to get the test data, then use it in tests.
"""

import json
from datetime import datetime
from fh.sec_client import SECHTTPClient

def main():
    print("Fetching live SEC series data for testing...")
    
    # Initialize client and fetch data
    client = SECHTTPClient()
    cik = "1100663"
    
    print(f"Fetching series data for CIK: {cik}")
    series_data = client.fetch_series_data(cik)
    
    print(f"Fetched {len(series_data)} series records")
    
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
    
    print(f"✓ Saved test data to: {filename}")
    print(f"✓ Ready for testing!")

if __name__ == "__main__":
    main()