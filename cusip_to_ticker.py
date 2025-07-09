# #!/usr/bin/env python3
# """
# CUSIP to Ticker Mapping for N-PORT Holdings

# This module provides functionality to map CUSIP identifiers to stock ticker symbols
# using various data sources and APIs.
# """

# import pandas as pd
# import requests
# import time
# from typing import Dict, Optional, List
# import json
# import os


# class CUSIPToTickerMapper:
#     """Maps CUSIP identifiers to stock ticker symbols"""
    
#     def __init__(self):
#         self.cache_file = "cusip_ticker_cache.json"
#         self.cache = self._load_cache()
#         self.last_request_time = 0
#         self.min_interval = 60 / 25  # 25 requests per minute = 2.4 seconds between requests
        
#     def _load_cache(self) -> Dict[str, str]:
#         """Load cached CUSIP-to-ticker mappings"""
#         if os.path.exists(self.cache_file):
#             try:
#                 with open(self.cache_file, 'r') as f:
#                     return json.load(f)
#             except Exception:
#                 return {}
#         return {}
    
#     def _save_cache(self):
#         """Save CUSIP-to-ticker mappings to cache"""
#         try:
#             with open(self.cache_file, 'w') as f:
#                 json.dump(self.cache, f, indent=2)
#         except Exception:
#             pass
    
#     def get_ticker_from_cusip(self, cusip: str) -> Optional[str]:
#         """
#         Get ticker symbol from CUSIP identifier
        
#         Args:
#             cusip: 9-character CUSIP identifier
            
#         Returns:
#             Ticker symbol if found, None otherwise
#         """
#         if not cusip or len(cusip) != 9:
#             return None
            
#         # Check cache first
#         if cusip in self.cache:
#             return self.cache[cusip]
        
#         # Try to get from OpenFIGI (free Bloomberg API)
#         ticker = self._get_ticker_from_openfigi(cusip)
        
#         if ticker:
#             self.cache[cusip] = ticker
#             self._save_cache()
            
#         return ticker
    
#     def _rate_limit(self):
#         """Enforce rate limiting for OpenFIGI API (25 requests per minute)"""
#         current_time = time.time()
#         time_since_last = current_time - self.last_request_time
        
#         if time_since_last < self.min_interval:
#             sleep_time = self.min_interval - time_since_last
#             print(f"Rate limiting: waiting {sleep_time:.1f} seconds...")
#             time.sleep(sleep_time)
        
#         self.last_request_time = time.time()
    
#     def _get_ticker_from_openfigi(self, cusip: str) -> Optional[str]:
#         """
#         Get ticker from OpenFIGI API (Bloomberg's free API)
        
#         Args:
#             cusip: CUSIP identifier
            
#         Returns:
#             Ticker symbol if found
#         """
#         try:
#             # Apply rate limiting before making request
#             self._rate_limit()
            
#             url = "https://api.openfigi.com/v3/mapping"
#             headers = {
#                 'Content-Type': 'application/json',
#                 'X-OPENFIGI-APIKEY': ''  # Optional API key for higher rate limits
#             }
            
#             payload = [{
#                 "idType": "ID_CUSIP",
#                 "idValue": cusip,
#                 "exchCode": "US"  # Focus on US exchanges
#             }]
            
#             response = requests.post(url, json=payload, headers=headers)
            
#             if response.status_code == 200:
#                 data = response.json()
#                 if data and len(data) > 0 and 'data' in data[0]:
#                     for item in data[0]['data']:
#                         if item.get('ticker') and item.get('marketSector') in ['Equity', 'Corp']:
#                             return item['ticker']
#             elif response.status_code == 429:
#                 print(f"Rate limit exceeded for CUSIP {cusip}, backing off...")
#                 time.sleep(10)  # Back off for 10 seconds if rate limited
#                 return None
            
#         except Exception as e:
#             print(f"Error fetching ticker for CUSIP {cusip}: {e}")
            
#         return None
    
#     def add_tickers_to_holdings(self, holdings_df: pd.DataFrame) -> pd.DataFrame:
#         """
#         Add ticker symbols to holdings DataFrame
        
#         Args:
#             holdings_df: DataFrame with holdings data including 'cusip' column
            
#         Returns:
#             DataFrame with added 'ticker' column
#         """
#         if 'cusip' not in holdings_df.columns.lower():
#             print("Warning: No 'cusip' column found in holdings DataFrame")
#             holdings_df['ticker'] = None
#             return holdings_df
        
#         print(f"Looking up tickers for {len(holdings_df)} holdings...")
        
#         tickers = []
#         for idx, cusip in enumerate(holdings_df['cusip']):
#             if idx % 10 == 0:
#                 print(f"Progress: {idx}/{len(holdings_df)}")
            
#             ticker = self.get_ticker_from_cusip(cusip)
#             tickers.append(ticker)
        
#         holdings_df['ticker'] = tickers
        
#         # Report results
#         found_tickers = sum(1 for t in tickers if t is not None)
#         print(f"Found tickers for {found_tickers}/{len(holdings_df)} holdings ({found_tickers/len(holdings_df)*100:.1f}%)")
        
#         return holdings_df


# def create_manual_cusip_ticker_mappings() -> Dict[str, str]:
#     """
#     Create manual mappings for major holdings that might not be found via API
    
#     Returns:
#         Dictionary mapping CUSIP to ticker symbol
#     """
#     return {
#         # # Major tech stocks
#         # '037833100': 'AAPL',  # Apple Inc
#         # '594918104': 'MSFT',  # Microsoft Corp
#         # '67066G104': 'NVDA',  # NVIDIA Corp
#         # '023135106': 'AMZN',  # Amazon.com Inc
#         # '30303M102': 'META',  # Meta Platforms Inc
#         # '084670702': 'BRK.B', # Berkshire Hathaway Inc Class B
#         # '02079K307': 'GOOGL', # Alphabet Inc Class A
#         # '02079K105': 'GOOG',  # Alphabet Inc Class C
#         # '11135F101': 'AVGO',  # Broadcom Inc
#         # '88160R101': 'TSLA',  # Tesla Inc
        
#         # # Major financial stocks
#         # '46625H100': 'JPM',   # JPMorgan Chase & Co
#         # '88579Y101': 'UNH',   # UnitedHealth Group Inc
#         # '38259P508': 'GS',    # Goldman Sachs Group Inc
#         # '06052K101': 'BAC',   # Bank of America Corp
#         # '17275R102': 'WFC',   # Wells Fargo & Co
        
#         # # Other major holdings
#         # '17275R102': 'WFC',   # Wells Fargo & Co
#         # '126650100': 'CVX',   # Chevron Corp
#         # '254687106': 'DIS',   # Walt Disney Co
#         # '437076102': 'HD',    # Home Depot Inc
#         # '742718109': 'PG',    # Procter & Gamble Co
#     }


# def main():
#     """Example usage of CUSIP to ticker mapping"""
#     from parse_nport import parse_nport_file
    
#     # Parse N-PORT file
#     xml_file = "/Users/weston/clients/westonplatter/getfundholdings-private/data/nport_1100663_S000004310_0001752724_25_119791.xml"
#     holdings_df, fund_info = parse_nport_file(xml_file)
    
#     if holdings_df.empty:
#         print("No holdings found in N-PORT file")
#         return
    
#     # Create mapper and add manual mappings
#     mapper = CUSIPToTickerMapper()
#     manual_mappings = create_manual_cusip_ticker_mappings()
#     mapper.cache.update(manual_mappings)
    
#     # Add tickers to holdings (this will be slow for 500+ holdings)
#     print("Adding ticker symbols to holdings...")
#     holdings_with_tickers = mapper.add_tickers_to_holdings(holdings_df)
    
#     # Show results
#     print(f"\nTop 10 holdings with tickers:")
#     top_holdings = holdings_with_tickers.nlargest(10, 'value_usd')
#     print(top_holdings[['name', 'ticker', 'cusip', 'value_usd', 'percent_value']])
    
#     # Show holdings with tickers found
#     with_tickers = holdings_with_tickers[holdings_with_tickers['ticker'].notna()]
#     print(f"\nHoldings with tickers found:")
#     print(with_tickers[['name', 'ticker', 'cusip', 'value_usd']].head(20))
    
#     # Save to CSV
#     output_file = "nport_holdings_with_tickers.csv"
#     holdings_with_tickers.to_csv(output_file, index=False, quoting=1)
#     print(f"\nSaved holdings with tickers to {output_file}")


# if __name__ == "__main__":
#     main()