#!/usr/bin/env python3
"""
Debug script to find and display the latest enriched holdings for a specific ticker.
"""

import pandas as pd
import glob
import os
import sys
from datetime import datetime

def find_latest_enriched_holdings(ticker):
    """Find the most recent enriched holdings file for a given ticker."""
    pattern = f"data/holdings_enriched_{ticker}_*.csv"
    files = glob.glob(pattern)
    
    if not files:
        print(f"No enriched holdings files found for ticker: {ticker}")
        return None
    
    # Sort by modification time to get the latest
    latest_file = max(files, key=os.path.getmtime)
    return latest_file

def display_top_holdings(filepath, top_n=20, filter_tickers=None):
    """Display top N holdings with ticker, name, weight, and market value."""
    try:
        df = pd.read_csv(filepath)
        
        # Filter by specific tickers if provided
        if filter_tickers:
            df_filtered = df[df['ticker'].isin(filter_tickers)]
            if df_filtered.empty:
                print(f"No holdings found for tickers: {', '.join(filter_tickers)}")
                return None
            df = df_filtered
        
        # Sort by percentage value (weight) in descending order
        df_sorted = df.sort_values('percent_value', ascending=False)
        
        # Select top N holdings (or all if filtering by specific tickers)
        if filter_tickers:
            top_holdings = df_sorted  # Show all filtered results
        else:
            top_holdings = df_sorted.head(top_n)
        
        # Select and rename columns for display
        display_cols = {
            'ticker': 'Ticker',
            'name': 'Name', 
            'percent_value': 'Weight (%)',
            'value_usd': 'Market Value (USD)'
        }
        
        result_df = top_holdings[list(display_cols.keys())].copy()
        result_df = result_df.rename(columns=display_cols)
        
        # Format the display
        result_df['Weight (%)'] = (result_df['Weight (%)'] * 100).round(4)
        result_df['Market Value (USD)'] = result_df['Market Value (USD)'].apply(lambda x: f"${x:,.2f}")
        
        return result_df
        
    except Exception as e:
        print(f"Error reading file {filepath}: {e}")
        return None

def main():
    # Get ticker from command line argument or default to IVV
    if len(sys.argv) > 1:
        ticker = sys.argv[1].strip().upper()
    else:
        ticker = "IVV"
        print(f"No ticker provided, using default: {ticker}")
    
    # Parse filter tickers if provided
    filter_tickers = None
    if len(sys.argv) > 2:
        filter_str = sys.argv[2].strip()
        filter_tickers = [t.strip().upper() for t in filter_str.split(',')]
        print(f"Filtering for specific tickers: {', '.join(filter_tickers)}")
    
    # Find latest enriched holdings file
    latest_file = find_latest_enriched_holdings(ticker)
    
    if latest_file is None:
        return
        
    print(f"\nLatest enriched holdings file: {latest_file}")
    print(f"File modification time: {datetime.fromtimestamp(os.path.getmtime(latest_file))}")
    
    # Display holdings (filtered or top 20)
    if filter_tickers:
        holdings = display_top_holdings(latest_file, filter_tickers=filter_tickers)
        title = f"Holdings for {', '.join(filter_tickers)} in {ticker}:"
    else:
        holdings = display_top_holdings(latest_file, 20)
        title = f"Top 20 Holdings for {ticker}:"
    
    if holdings is not None:
        print(f"\n{title}")
        print("=" * 80)
        print(holdings.to_string(index=False))
        print(f"\nTotal holdings displayed: {len(holdings)}")

if __name__ == "__main__":
    main()