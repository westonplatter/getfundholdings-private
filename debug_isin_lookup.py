#!/usr/bin/env python3
"""
Debug script for testing ISIN to ticker lookups via OpenFIGI API.

This script allows you to test individual ISIN lookups and batch lookups
to validate the new ISIN functionality before running the full workflow.
"""

import sys
import os
from typing import List, Dict, Optional
from loguru import logger
import pandas as pd

# Add the project root to the path
sys.path.append(os.path.dirname(__file__))

from fh.openfigi_client import OpenFIGIClient


def test_single_isin(client: OpenFIGIClient, isin: str) -> Optional[str]:
    """Test a single ISIN lookup."""
    logger.info(f"Testing ISIN: {isin}")
    ticker = client.get_ticker_from_isin(isin)
    
    if ticker:
        logger.info(f"✅ Success: {isin} → {ticker}")
    else:
        logger.warning(f"❌ Failed: {isin} → No ticker found")
    
    return ticker


def test_batch_isins(client: OpenFIGIClient, isins: List[str]) -> Dict[str, Optional[str]]:
    """Test batch ISIN lookups."""
    logger.info(f"Testing batch lookup for {len(isins)} ISINs")
    results = client.get_multiple_tickers_from_isins(isins)
    
    # Summary
    successful = sum(1 for ticker in results.values() if ticker is not None)
    failed = len(results) - successful
    success_rate = successful / len(results) * 100 if results else 0
    
    logger.info(f"Batch results: {successful}/{len(results)} successful ({success_rate:.1f}%)")
    
    # Detailed results
    logger.info("Detailed results:")
    for isin, ticker in results.items():
        status = "✅" if ticker else "❌"
        ticker_str = ticker or "No ticker found"
        logger.info(f"  {status} {isin} → {ticker_str}")
    
    return results


def test_dataframe_integration(client: OpenFIGIClient, isins: List[str]) -> pd.DataFrame:
    """Test DataFrame integration with ISIN lookups."""
    logger.info(f"Testing DataFrame integration with {len(isins)} ISINs")
    
    # Create a test DataFrame
    test_data = {
        'name': [f'Test Company {i+1}' for i in range(len(isins))],
        'isin': isins,
        'value_usd': [1000000 * (i+1) for i in range(len(isins))],
        'cusip': [None] * len(isins)  # Simulate missing CUSIPs
    }
    
    df = pd.DataFrame(test_data)
    logger.info(f"Created test DataFrame with {len(df)} rows")
    
    # Test the ISIN lookup method
    enriched_df = client.add_tickers_to_dataframe_by_isin(df, isin_column='isin')
    
    # Show results
    logger.info("DataFrame results:")
    for _, row in enriched_df.iterrows():
        status = "✅" if pd.notna(row['ticker']) else "❌"
        ticker_str = row['ticker'] if pd.notna(row['ticker']) else "No ticker"
        logger.info(f"  {status} {row['name']} ({row['isin']}) → {ticker_str}")
    
    return enriched_df


def main():
    """Main debug function with various test scenarios."""
    logger.info("🚀 Starting ISIN ticker lookup debug session")
    
    # Initialize OpenFIGI client
    client = OpenFIGIClient()
    logger.info(f"Initialized OpenFIGI client (cache size: {client.get_cache_size()})")
    
    # Test ISINs from your logged output (companies with missing CUSIPs)
    test_isins = [
        # Major international companies from your log output
        "IE00B8KQN827",  # Eaton Corp. plc
        "CH0044328745",  # Chubb Ltd.
        "BMG3223R1088",  # Everest Group Ltd.
        "IE00BLS09M33",  # Pentair plc
        "IE00BY7QL619",  # Johnson Controls International plc
        "IE00BK9ZQ967",  # Trane Technologies plc
        "IE000IVNQZ81",  # TE Connectivity plc
        "LR0008862868",  # Royal Caribbean Cruises Ltd.
        "NL0009434992",  # LyondellBasell Industries NV
        "BMG491BT1088",  # Invesco Ltd.
        "IE00BTN1Y115",  # Medtronic plc
        "IE00BLP1HW54",  # Aon plc
        "IE00BDB6Q211",  # Willis Towers Watson plc
        "BMG0450A1053",  # Arch Capital Group Ltd.
        "IE00BKVD2N49",  # Seagate Technology Holdings plc
        "CH1300646267",  # Bunge Global SA
        "IE00BFRT3W74",  # Allegion plc
        "JE00BJ1F3079",  # Amcor plc
        "IE00BFY8C754",  # STERIS plc
        "NL0009538784",  # NXP Semiconductors NV
        "IE00B4BNMY34",  # Accenture plc
        "JE00BTDN8H13",  # Aptiv plc
        "BMG667211046",  # Norwegian Cruise Line Holdings Ltd.
        "CH0114405324",  # Garmin Ltd.
        "IE00028FXN24",  # Smurfit WestRock plc
        "IE000S9YS762",  # Linde plc
    ]
    
    # Known US stock ISINs for validation
    validation_isins = [
        # "US0378331005",  # Apple Inc (AAPL)
        # "US5949181045",  # Microsoft Corp (MSFT)
        # "US6174464486",  # NVIDIA Corp (NVDA)
        # "US02079K3059",  # Alphabet Inc Class A (GOOGL)
        # "US0231351067",  # Amazon.com Inc (AMZN)
        "US09247X1019",
    ]
    
    print("\n" + "="*80)
    print("🧪 ISIN TICKER LOOKUP DEBUG TESTS")
    print("="*80)
    
    # Test 1: Single ISIN lookups with known US stocks
    print("\n📋 Test 1: Known US Stock ISINs (Validation)")
    print("-" * 50)
    
    for isin in validation_isins[:3]:  # Test first 3 to avoid hitting rate limits
        test_single_isin(client, isin)
    
#     # Test 2: Single ISIN lookups with problematic holdings
#     print("\n📋 Test 2: International Holdings from Your Data")
#     print("-" * 50)
    
#     # Test a few key ones individually
#     key_test_isins = [
#         "IE00B8KQN827",  # Eaton Corp. plc  
#         "CH0044328745",  # Chubb Ltd.
#         "IE00B4BNMY34",  # Accenture plc
#         "IE000S9YS762",  # Linde plc
#         "IE00BTN1Y115",  # Medtronic plc
#     ]
    
#     for isin in key_test_isins:
#         test_single_isin(client, isin)
    
#     # Test 3: Batch lookup test
#     print("\n📋 Test 3: Batch Lookup (First 10 ISINs)")
#     print("-" * 50)
    
#     batch_test_isins = test_isins[:10]  # First 10 to avoid rate limits
#     batch_results = test_batch_isins(client, batch_test_isins)
    
#     # Test 4: DataFrame integration test
#     print("\n📋 Test 4: DataFrame Integration Test")
#     print("-" * 50)
    
#     df_test_isins = key_test_isins  # Use the key ones for DataFrame test
#     enriched_df = test_dataframe_integration(client, df_test_isins)
    
#     # Test 5: Cache statistics
#     print("\n📋 Test 5: Cache Performance")
#     print("-" * 50)
    
#     cache_stats = client.get_cache_stats()
#     logger.info(f"Cache statistics:")
#     logger.info(f"  Total cached: {cache_stats['total_cached']}")
#     logger.info(f"  Found cached: {cache_stats['found_cached']}")
#     logger.info(f"  Not found cached: {cache_stats['not_found_cached']}")
    
#     print("\n" + "="*80)
#     print("✅ Debug session complete!")
#     print("="*80)
    
#     # Interactive mode
#     print("\n🔍 Interactive Mode")
#     print("Enter ISINs to test (or 'quit' to exit):")
    
#     while True:
#         try:
#             user_input = input("\nISIN> ").strip()
            
#             if user_input.lower() in ['quit', 'exit', 'q']:
#                 break
            
#             if user_input:
#                 if len(user_input) == 12 and user_input.isalnum():
#                     test_single_isin(client, user_input.upper())
#                 else:
#                     print("❌ Invalid ISIN format. ISINs should be 12 alphanumeric characters.")
            
#         except KeyboardInterrupt:
#             break
#         except Exception as e:
#             logger.error(f"Error testing ISIN: {e}")
    
#     print("\n👋 Goodbye!")


if __name__ == "__main__":
    main()