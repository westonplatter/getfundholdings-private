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


def main():
    # Initialize SEC client
    client = SECHTTPClient()

    download_all_series_data(client)


if __name__ == "__main__":
    main()
