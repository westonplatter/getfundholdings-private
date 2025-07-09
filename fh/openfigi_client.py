#!/usr/bin/env python3
"""
OpenFIGI API Client for CUSIP to Ticker Mapping

This module provides a Bloomberg OpenFIGI API client for mapping CUSIP identifiers
to stock ticker symbols, following the same structure as the SEC HTTP client.
"""

import pandas as pd
import requests
import time
import json
import os
import logging
from typing import Dict, Optional, List
from urllib.parse import urljoin
from loguru import logger

class OpenFIGIClient:
    """
    OpenFIGI API client for mapping CUSIP identifiers to ticker symbols.
    Implements rate limiting and caching per OpenFIGI API requirements.
    """
    
    def __init__(self, api_key: Optional[str] = None, cache_file: str = "cusip_ticker_cache.json"):
        """
        Initialize OpenFIGI API client.
        
        Args:
            api_key: Optional API key for higher rate limits
            cache_file: Path to cache file for CUSIP-to-ticker mappings
        """
        self.api_key = api_key
        self.cache_file = cache_file
        self.base_url = "https://api.openfigi.com"
        self.mapping_url = "https://api.openfigi.com/v3/mapping"
        
        # Rate limiting: 25 requests per minute without API key
        self.last_request_time = 0
        self.min_interval = 60 / 25  # 2.4 seconds between requests
        
        # Setup session with proper headers
        self.session = requests.Session()
        self._setup_headers()
        
        # Initialize cache
        self.cache = self._load_cache()
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        # logger = logging.getLogger(__name__)
    
    def _setup_headers(self):
        """Setup HTTP headers for OpenFIGI API requests."""
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        # Add API key if provided
        if self.api_key:
            headers['X-OPENFIGI-APIKEY'] = self.api_key
        
        self.session.headers.update(headers)
    
    def _rate_limit(self):
        """Enforce rate limiting for OpenFIGI API (25 requests per minute)."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_interval:
            sleep_time = self.min_interval - time_since_last
            logger.info(f"Rate limiting: waiting {sleep_time:.1f} seconds...")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _make_request(self, url: str, data: Optional[dict] = None, max_retries: int = 3) -> requests.Response:
        """
        Make HTTP request with rate limiting and retry logic.
        
        Args:
            url: Target URL
            data: JSON data to send
            max_retries: Maximum number of retry attempts
            
        Returns:
            requests.Response object
            
        Raises:
            requests.RequestException: If all retries fail
        """
        for attempt in range(max_retries + 1):
            try:
                # Apply rate limiting before making request
                self._rate_limit()
                
                # Log request for debugging
                # logger.debug(f"Making request to {url} (attempt {attempt + 1})")
                if attempt == 0:
                    logger.info(f"Making request to {url}")
                else:
                    logger.debug(f"Making request to {url} (attempt {attempt + 1})")
                
                response = self.session.post(url, json=data, timeout=30)
                
                # Handle rate limiting
                if response.status_code == 429:
                    if attempt < max_retries:
                        backoff_time = min(60, (2 ** attempt) * 5)  # Exponential backoff
                        logger.warning(f"Rate limit exceeded, waiting {backoff_time} seconds...")
                        time.sleep(backoff_time)
                        continue
                    else:
                        raise requests.RequestException("Rate limit exceeded after all retries")
                
                # Handle other HTTP errors
                if response.status_code >= 400:
                    logger.warning(f"HTTP error {response.status_code}: {response.text}")
                    if response.status_code == 404:
                        return response  # Return 404 responses for handling
                
                response.raise_for_status()
                return response
                
            except requests.RequestException as e:
                if attempt < max_retries:
                    delay = (2 ** attempt) + 1  # Simple exponential backoff
                    logger.warning(f"Request failed (attempt {attempt + 1}): {e}, retrying in {delay} seconds")
                    time.sleep(delay)
                else:
                    logger.error(f"All retries failed for {url}: {e}")
                    raise
        
        raise requests.RequestException("Max retries exceeded")
    
    def _load_cache(self) -> Dict[str, str]:
        """Load cached CUSIP-to-ticker mappings from file."""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    cache_data = json.load(f)
                    logger.info(f"Loaded {len(cache_data)} cached mappings from {self.cache_file}")
                    return cache_data
            except Exception as e:
                logger.warning(f"Failed to load cache file: {e}")
                return {}
        return {}
    
    def _save_cache(self):
        """Save CUSIP-to-ticker mappings to cache file."""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f, indent=2)
            logger.info(f"Saved {len(self.cache)} mappings to cache file")
        except Exception as e:
            logger.error(f"Failed to save cache file: {e}")
    
    def get_cache_size(self) -> int:
        """Get current cache size."""
        return len(self.cache)
    
    def clear_cache(self):
        """Clear all cached mappings."""
        self.cache.clear()
        self._save_cache()
        logger.info("Cache cleared")
    
    def get_ticker_from_cusip(self, cusip: str) -> Optional[str]:
        """
        Get ticker symbol from CUSIP identifier.
        
        Args:
            cusip: 9-character CUSIP identifier
            
        Returns:
            Ticker symbol if found, None otherwise
        """
        if not cusip or not isinstance(cusip, str) or len(cusip) != 9:
            logger.warning(f"Invalid CUSIP format: {cusip} (type: {type(cusip)})")
            return None
        
        # Check cache first
        if cusip in self.cache:
            logger.debug(f"Cache hit for CUSIP {cusip}: {self.cache[cusip]}")
            return self.cache[cusip]
        
        # Make API request
        ticker = self._fetch_ticker_from_api(cusip)
        
        # Cache the result (even if None to avoid repeated API calls)
        self.cache[cusip] = ticker
        self._save_cache()
        
        return ticker
    
    def _fetch_ticker_from_api(self, cusip: str) -> Optional[str]:
        """
        Fetch ticker from OpenFIGI API.
        
        Args:
            cusip: CUSIP identifier
            
        Returns:
            Ticker symbol if found, None otherwise
        """
        try:
            # Build request payload
            payload = [{
                "idType": "ID_CUSIP",
                "idValue": cusip,
                "exchCode": "US"  # Focus on US exchanges
            }]
            
            response = self._make_request(self.mapping_url, payload)
            
            if response.status_code == 200:
                data = response.json()
                
                # Parse response to extract ticker
                if data and len(data) > 0 and 'data' in data[0]:
                    for item in data[0]['data']:
                        # Look for equity instruments with ticker symbols
                        if (item.get('ticker') and 
                            item.get('marketSector') in ['Equity', 'Corp'] and
                            item.get('exchCode') == 'US'):
                            ticker = item['ticker']
                            logger.debug(f"Found ticker {ticker} for CUSIP {cusip}")
                            return ticker
                
                logger.debug(f"No ticker found for CUSIP {cusip}")
                return None
                
        except requests.RequestException as e:
            logger.error(f"API request failed for CUSIP {cusip}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching ticker for CUSIP {cusip}: {e}")
            return None
    
    def get_multiple_tickers(self, cusips: List[str]) -> Dict[str, Optional[str]]:
        """
        Get ticker symbols for multiple CUSIP identifiers.
        
        Args:
            cusips: List of CUSIP identifiers
            
        Returns:
            Dictionary mapping CUSIP to ticker symbol (or None if not found)
        """
        results = {}
        
        for i, cusip in enumerate(cusips):
            logger.info(f"Processing CUSIP {i+1}/{len(cusips)}: {cusip}")
            results[cusip] = self.get_ticker_from_cusip(cusip)
        
        return results
    
    def add_tickers_to_dataframe(self, df: pd.DataFrame, cusip_column: str = 'cusip') -> pd.DataFrame:
        """
        Add ticker symbols to a pandas DataFrame containing CUSIP identifiers.
        
        Args:
            df: DataFrame with CUSIP column
            cusip_column: Name of the column containing CUSIP identifiers
            
        Returns:
            DataFrame with added 'ticker' column
        """
        if cusip_column not in df.columns:
            logger.warning(f"Column '{cusip_column}' not found in DataFrame")
            df['ticker'] = None
            return df
        
        logger.info(f"Adding ticker symbols for {len(df)} holdings...")
        
        # Get unique CUSIPs to avoid duplicate API calls, filtering out NaN values
        unique_cusips = df[cusip_column].dropna().unique()
        # Filter out non-string values that might be floats
        unique_cusips = [cusip for cusip in unique_cusips if isinstance(cusip, str)]
        logger.info(f"Found {len(unique_cusips)} unique CUSIPs")
        
        # Get ticker mappings for unique CUSIPs
        ticker_mappings = self.get_multiple_tickers(unique_cusips)
        
        # Map tickers to DataFrame
        df['ticker'] = df[cusip_column].map(ticker_mappings)
        
        # Report results
        found_tickers = df['ticker'].notna().sum()
        success_rate = found_tickers / len(df) * 100
        logger.info(f"Found tickers for {found_tickers}/{len(df)} holdings ({success_rate:.1f}%)")
        
        # Log CUSIPs that couldn't be found
        missing_tickers = df[df['ticker'].isna()]
        if not missing_tickers.empty:
            unique_missing_cusips = missing_tickers[cusip_column].dropna().unique()
            logger.warning(f"Could not find tickers for {len(unique_missing_cusips)} CUSIPs: {list(unique_missing_cusips)}")
        
        return df
    
    def get_cache_stats(self) -> Dict[str, int]:
        """Get cache statistics."""
        total_cached = len(self.cache)
        found_cached = sum(1 for ticker in self.cache.values() if ticker is not None)
        not_found_cached = total_cached - found_cached
        
        return {
            'total_cached': total_cached,
            'found_cached': found_cached,
            'not_found_cached': not_found_cached
        }


def create_manual_cusip_mappings() -> Dict[str, str]:
    """
    Create manual mappings for major holdings that might not be found via API.
    
    Returns:
        Dictionary mapping CUSIP to ticker symbol
    """
    return {
        # # Major tech stocks
        # '037833100': 'AAPL',   # Apple Inc
        # '594918104': 'MSFT',   # Microsoft Corp
        # '67066G104': 'NVDA',   # NVIDIA Corp
        # '023135106': 'AMZN',   # Amazon.com Inc
        # '30303M102': 'META',   # Meta Platforms Inc
        # '084670702': 'BRK.B',  # Berkshire Hathaway Inc Class B
        # '02079K105': 'GOOGL',  # Alphabet Inc Class A
        # '02079K307': 'GOOG',   # Alphabet Inc Class C
        # '11135F101': 'AVGO',   # Broadcom Inc
        # '88160R101': 'TSLA',   # Tesla Inc
        
        # # Major financial stocks
        # '46625H100': 'JPM',    # JPMorgan Chase & Co
        # '88579Y101': 'UNH',    # UnitedHealth Group Inc
        # '38259629': 'GS',      # Goldman Sachs Group Inc
        # '060505104': 'BAC',    # Bank of America Corp
        # '953634101': 'WFC',    # Wells Fargo & Co
        
        # # Other major holdings
        # '126650100': 'CVX',    # Chevron Corp
        # '254687106': 'DIS',    # Walt Disney Co
        # '437076102': 'HD',     # Home Depot Inc
        # '742718109': 'PG',     # Procter & Gamble Co
        # '542893108': 'LLY',    # Eli Lilly & Co
        # '92826C839': 'V',      # Visa Inc
        # '30231G102': 'XOM',    # Exxon Mobil Corp
        # '57636Q104': 'MA',     # Mastercard Inc
        # '22160K105': 'COST',   # Costco Wholesale Corp
        # '64110L106': 'NFLX',   # Netflix Inc
        # '48123W3': 'JNJ',      # Johnson & Johnson
    }


def main():
    """Example usage of OpenFIGI client"""
    from parse_nport import parse_nport_file
    
    # Parse N-PORT file
    xml_file = "/Users/weston/clients/westonplatter/getfundholdings-private/data/nport_1100663_S000004310_0001752724_25_119791.xml"
    # xml_file = "/Users/weston/clients/westonplatter/getfundholdings-private/data/nport_1100663_S000004310_0001752724_25_043800.xml"
    
    if not os.path.exists(xml_file):
        logger.error(f"XML file not found: {xml_file}")
        return
    
    holdings_df, fund_info = parse_nport_file(xml_file)
    
    if holdings_df.empty:
        logger.error("No holdings found in N-PORT file")
        return
    
    # Initialize OpenFIGI client
    client = OpenFIGIClient()
    
    # # Add manual mappings to cache
    # manual_mappings = create_manual_cusip_mappings()
    # client.cache.update(manual_mappings)
    
    # Get top 20 holdings for faster testing
    top_holdings = holdings_df.nlargest(2000, 'value_usd').copy()
    
    # Add ticker symbols
    logger.info("Adding ticker symbols to top holdings...")
    top_holdings_with_tickers = client.add_tickers_to_dataframe(top_holdings)
    
    # Display results
    logger.info(f"\nTop 20 Holdings with Tickers:")
    logger.info("-" * 80)
    for _, row in top_holdings_with_tickers.iterrows():
        value_billions = row['value_usd'] / 1e9
        logger.info(f"{row['name']:<35} {row['ticker'] or 'N/A':<6} ${value_billions:>7.2f}B ({row['percent_value']:.2%})")
    
    # Display cache statistics
    stats = client.get_cache_stats()
    logger.info(f"\nCache Statistics:")
    logger.info(f"Total cached: {stats['total_cached']}")
    logger.info(f"Found: {stats['found_cached']}")
    logger.info(f"Not found: {stats['not_found_cached']}")
    
    # Save results
    output_file = "top_holdings_with_tickers.csv"
    top_holdings_with_tickers.to_csv(output_file, index=False, quoting=1)
    logger.info(f"\nSaved to {output_file}")


if __name__ == "__main__":
    main()