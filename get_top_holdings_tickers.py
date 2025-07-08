#!/usr/bin/env python3
"""
Quick ticker lookup for top N-PORT holdings
"""

import pandas as pd
from parse_nport import parse_nport_file
from cusip_to_ticker import CUSIPToTickerMapper, create_manual_cusip_ticker_mappings


def get_top_holdings_with_tickers(xml_file_path: str, top_n: int = 20) -> pd.DataFrame:
    """
    Get top N holdings with ticker symbols
    
    Args:
        xml_file_path: Path to N-PORT XML file
        top_n: Number of top holdings to process
        
    Returns:
        DataFrame with top holdings and ticker symbols
    """
    # Parse N-PORT file
    holdings_df, fund_info = parse_nport_file(xml_file_path)
    
    if holdings_df.empty:
        print("No holdings found in N-PORT file")
        return pd.DataFrame()
    
    # Get top holdings by value
    top_holdings = holdings_df.nlargest(top_n, 'value_usd').copy()
    
    # Create mapper with manual mappings for major stocks
    mapper = CUSIPToTickerMapper()
    manual_mappings = create_manual_cusip_ticker_mappings()
    mapper.cache.update(manual_mappings)
    
    # Add tickers
    print(f"Looking up tickers for top {top_n} holdings...")
    tickers = []
    for idx, cusip in enumerate(top_holdings['cusip']):
        print(f"  {idx+1}/{top_n}: {cusip}")
        ticker = mapper.get_ticker_from_cusip(cusip)
        tickers.append(ticker)
    
    top_holdings['ticker'] = tickers
    
    # Report results
    found_tickers = sum(1 for t in tickers if t is not None)
    print(f"Found tickers for {found_tickers}/{top_n} holdings")
    
    return top_holdings


def main():
    """Get top holdings with tickers"""
    xml_file = "/Users/weston/clients/westonplatter/getfundholdings-private/data/nport_1100663_S000004310_0001752724_25_119791.xml"
    
    # Get top 20 holdings with tickers
    top_holdings = get_top_holdings_with_tickers(xml_file, top_n=20)
    
    if not top_holdings.empty:
        print(f"\nTop 20 Holdings with Tickers:")
        print("-" * 80)
        display_cols = ['name', 'ticker', 'cusip', 'value_usd', 'percent_value']
        
        for idx, row in top_holdings.iterrows():
            value_billions = row['value_usd'] / 1e9
            print(f"{row['name']:<35} {row['ticker']:<6} ${value_billions:>7.2f}B ({row['percent_value']:.2%})")
        
        # Save to CSV
        output_file = "top_holdings_with_tickers.csv"
        top_holdings.to_csv(output_file, index=False)
        print(f"\nSaved to {output_file}")


if __name__ == "__main__":
    main()