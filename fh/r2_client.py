#!/usr/bin/env python3
"""
Cloudflare R2 Client for Holdings Data Upload

This module provides a Cloudflare R2 client for uploading enriched holdings data,
following the same structure as the SEC and OpenFIGI clients.
"""

import json
import os
import re
from datetime import datetime
from typing import Dict, List, Optional

import boto3
import pandas as pd
from botocore.config import Config
from dotenv import load_dotenv
from loguru import logger


class R2Client:
    """
    Cloudflare R2 client for uploading enriched holdings data.
    Implements proper configuration, error handling, and logging per R2 requirements.
    """

    def __init__(self, env_file: str = None, bucket: str = "dev"):
        """
        Initialize R2 client with Cloudflare credentials.

        Args:
            env_file: Path to environment file containing R2 credentials.
                     If None, uses .env.{bucket} (e.g., .env.dev, .env.prod)
            bucket: Bucket environment to use ("dev" or "prod"). Defaults to "dev".
        """
        # Determine which env file to load
        if env_file is None:
            env_file = f".env.{bucket}"

        # Load environment variables
        load_dotenv(env_file)

        # Initialize R2 client
        self.client = self._setup_r2_client()

        # Get bucket name from environment
        self.bucket_name = os.getenv("CLOUDFLARE_R2_BUCKET_NAME")

        if not self.bucket_name:
            raise ValueError(f"CLOUDFLARE_R2_BUCKET_NAME not found in {env_file}")

        logger.info(
            f"R2 client initialized for {bucket} environment using {env_file}, bucket: {self.bucket_name}"
        )

    def _setup_r2_client(self):
        """Setup R2 client with proper configuration."""
        endpoint_url = os.getenv("CLOUDFLARE_R2_ENDPOINT_URL")
        access_key_id = os.getenv("CLOUDFLARE_R2_ACCESS_KEY_ID")
        secret_access_key = os.getenv("CLOUDFLARE_R2_SECRET_ACCESS_KEY")

        if not all([endpoint_url, access_key_id, secret_access_key]):
            raise ValueError("Missing required R2 environment variables")

        return boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            config=Config(signature_version="s3v4"),
            region_name="auto",
        )

    def upload_json(self, data: Dict, key: str) -> bool:
        """
        Upload JSON data to Cloudflare R2.

        Args:
            data: Dictionary data to upload as JSON
            key: R2 object key (path)

        Returns:
            True if successful, False otherwise
        """
        try:
            json_data = json.dumps(data, indent=2, default=str)

            self.client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json_data,
                ContentType="application/json",
            )

            logger.info(f"Successfully uploaded {key} to {self.bucket_name}")
            return True

        except Exception as e:
            logger.error(f"Error uploading {key} to R2: {e}")
            return False

    def read_csv_to_json(self, file_path: str) -> Optional[Dict]:
        """
        Read CSV file and convert to JSON format suitable for R2 upload.

        Args:
            file_path: Path to the CSV file

        Returns:
            Dictionary with CSV data converted to JSON format or None if failed
        """
        try:
            if not os.path.exists(file_path):
                logger.error(f"CSV file not found: {file_path}")
                return None

            # Read CSV file
            df = pd.read_csv(file_path)

            # Replace NaN values with empty strings
            df = df.where(pd.notnull(df), "")

            # Convert DataFrame to JSON-friendly format
            # Convert to records format (list of dictionaries)
            holdings_data = df.to_dict("records")

            # Extract metadata from filename
            filename = os.path.basename(file_path)
            fund_ticker = self.extract_fund_ticker_from_filename(filename)
            timestamp = self.extract_timestamp_from_filename(filename)

            # Create structured JSON data
            json_data = {
                "metadata": {
                    "fund_ticker": fund_ticker,
                    "total_holdings": len(holdings_data),
                    "data_timestamp": timestamp,
                    "source_file": filename,
                    "upload_timestamp": datetime.now().isoformat(),
                },
                "holdings": holdings_data,
            }

            logger.info(
                f"Converted CSV to JSON: {len(holdings_data)} holdings for {fund_ticker}"
            )
            return json_data

        except Exception as e:
            logger.error(f"Error reading CSV file {file_path}: {e}")
            return None

    def upload_dataframe_json(
        self, df: pd.DataFrame, key: str, metadata: Optional[Dict] = None
    ) -> bool:
        """
        Upload pandas DataFrame as JSON to Cloudflare R2.

        Args:
            df: pandas DataFrame to upload
            key: R2 object key (path)
            metadata: Optional metadata to include in JSON

        Returns:
            True if successful, False otherwise
        """
        try:
            # Replace NaN values with empty strings
            df = df.where(pd.notnull(df), "")

            # Convert DataFrame to JSON-friendly format
            holdings_data = df.to_dict("records")

            # Create structured JSON data
            json_data = {
                "metadata": {
                    "total_holdings": len(holdings_data),
                    "upload_timestamp": datetime.now().isoformat(),
                    **(metadata or {}),
                },
                "holdings": holdings_data,
            }

            return self.upload_json(json_data, key)

        except Exception as e:
            logger.error(f"Error converting DataFrame to JSON for {key}: {e}")
            return False

    def extract_timestamp_from_filename(self, filename: str) -> Optional[str]:
        """
        Extract timestamp from enriched holdings filename.

        Args:
            filename: Filename like 'holdings_enriched_IVV_1100663_S000004310_20250711_20250711_143022.csv'

        Returns:
            Timestamp string in format 'YYYYMMDD_HHMMSS' or None if not found
        """
        # Pattern to match timestamps in format: YYYYMMDD_HHMMSS at the end of filename
        timestamp_pattern = r"(\d{8}_\d{6})\.csv$"

        match = re.search(timestamp_pattern, filename)
        if match:
            return match.group(1)

        logger.warning(f"Could not extract timestamp from filename: {filename}")
        return None

    def extract_fund_ticker_from_filename(self, filename: str) -> Optional[str]:
        """
        Extract fund ticker from holdings filename.

        Args:
            filename: Filename like 'holdings_enriched_IVV_1100663_S000004310_20250711_20250711_143022.csv'

        Returns:
            Fund ticker string or None if not found
        """
        try:
            # Remove extension and split by underscore
            basename = filename.replace(".csv", "")
            parts = basename.split("_")

            # For enriched files: holdings_enriched_{ticker}_{cik}_{series_id}_{report_date}_{timestamp}
            # For regular files: holdings_{ticker}_{cik}_{series_id}_{report_date}_{timestamp}
            if basename.startswith("holdings_enriched_") and len(parts) >= 3:
                return parts[2]  # Third part is the ticker
            elif basename.startswith("holdings_") and len(parts) >= 2:
                return parts[1]  # Second part is the ticker

            logger.warning(f"Could not extract fund ticker from filename: {filename}")
            return None

        except Exception as e:
            logger.error(f"Error extracting fund ticker from filename {filename}: {e}")
            return None

    def upload_enriched_holdings_to_latest(self, file_path: str, cik: str) -> bool:
        """
        Upload enriched holdings CSV file as JSON to the 'latest' folder.

        Args:
            file_path: Path to the enriched holdings CSV file
            cik: Company CIK number

        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert CSV to JSON format
            json_data = self.read_csv_to_json(file_path)
            if not json_data:
                logger.error(f"Failed to convert CSV to JSON: {file_path}")
                return False

            # Extract fund ticker from JSON metadata
            fund_ticker = json_data["metadata"].get("fund_ticker")
            if not fund_ticker:
                logger.error(f"Could not extract fund ticker from JSON data")
                return False

            # Add CIK to metadata
            json_data["metadata"]["cik"] = cik

            # Construct R2 key for latest folder using fund ticker
            r2_key = f"latest/{fund_ticker}/holdings_enriched.json"

            # Upload JSON to R2
            success = self.upload_json(json_data, r2_key)

            if success:
                logger.info(
                    f"Uploaded enriched holdings JSON to latest folder: {r2_key}"
                )
                logger.info(f"Fund ticker: {fund_ticker}")
                logger.info(f"CIK: {cik}")
                logger.info(
                    f"Holdings count: {json_data['metadata']['total_holdings']}"
                )
                logger.info(
                    f"Data timestamp: {json_data['metadata']['data_timestamp']}"
                )

            return success

        except Exception as e:
            logger.error(f"Error uploading enriched holdings to latest: {e}")
            return False

    def upload_enriched_holdings_dataframe_to_latest(
        self,
        df: pd.DataFrame,
        fund_ticker: str,
        cik: str,
        timestamp: Optional[str] = None,
    ) -> bool:
        """
        Upload enriched holdings DataFrame as JSON to the 'latest' folder.

        Args:
            df: pandas DataFrame with enriched holdings data
            fund_ticker: Fund ticker symbol (e.g., 'IVV')
            cik: Company CIK number
            timestamp: Optional timestamp string, defaults to current time

        Returns:
            True if successful, False otherwise
        """
        try:
            if timestamp is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Construct R2 key for latest folder using fund ticker
            r2_key = f"latest/{fund_ticker}/holdings_enriched.json"

            # Create metadata for JSON
            metadata = {
                "fund_ticker": fund_ticker,
                "cik": cik,
                "data_timestamp": timestamp,
            }

            # Upload DataFrame as JSON
            success = self.upload_dataframe_json(df, r2_key, metadata)

            if success:
                logger.info(
                    f"Uploaded enriched holdings DataFrame JSON to latest folder: {r2_key}"
                )
                logger.info(f"Fund ticker: {fund_ticker}")
                logger.info(f"CIK: {cik}")
                logger.info(f"Data timestamp: {timestamp}")
                logger.info(f"Holdings count: {len(df)}")

            return success

        except Exception as e:
            logger.error(f"Error uploading enriched holdings DataFrame to latest: {e}")
            return False

    def list_objects(self, prefix: str = "") -> List[str]:
        """
        List objects in the R2 bucket with optional prefix filter.

        Args:
            prefix: Optional prefix to filter objects

        Returns:
            List of object keys
        """
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=prefix
            )

            objects = []
            if "Contents" in response:
                objects = [obj["Key"] for obj in response["Contents"]]

            logger.info(f"Found {len(objects)} objects with prefix '{prefix}'")
            return objects

        except Exception as e:
            logger.error(f"Error listing objects with prefix '{prefix}': {e}")
            return []

    def delete_object(self, key: str) -> bool:
        """
        Delete an object from the R2 bucket.

        Args:
            key: R2 object key to delete

        Returns:
            True if successful, False otherwise
        """
        try:
            self.client.delete_object(Bucket=self.bucket_name, Key=key)

            logger.info(f"Successfully deleted {key} from {self.bucket_name}")
            return True

        except Exception as e:
            logger.error(f"Error deleting {key} from R2: {e}")
            return False

    def get_object_info(self, key: str) -> Optional[Dict]:
        """
        Get metadata information about an object in the R2 bucket.

        Args:
            key: R2 object key

        Returns:
            Dictionary with object metadata or None if not found
        """
        try:
            response = self.client.head_object(Bucket=self.bucket_name, Key=key)

            info = {
                "key": key,
                "size": response.get("ContentLength"),
                "last_modified": response.get("LastModified"),
                "content_type": response.get("ContentType"),
                "etag": response.get("ETag"),
            }

            return info

        except Exception as e:
            logger.warning(f"Object {key} not found or error getting info: {e}")
            return None

    def find_latest_enriched_holdings_by_ticker(
        self, data_dir: str = "data"
    ) -> Dict[str, str]:
        """
        Find the latest enriched holdings CSV file for each ticker in the data directory.
        For each ticker, finds the file with the most recent report_date, and if there are
        multiple files with the same report_date, selects the one with the latest timestamp.

        Args:
            data_dir: Directory to search for enriched holdings files

        Returns:
            Dictionary mapping ticker symbols to their latest file paths
        """
        try:
            if not os.path.exists(data_dir):
                logger.warning(f"Data directory does not exist: {data_dir}")
                return {}

            # Pattern: holdings_enriched_{ticker}_{cik}_{series_id}_{report_date}_{timestamp}.csv
            pattern = re.compile(
                r"^holdings_enriched_([A-Z]+)_(\d+)_([^_]+)_(\d{8})_(\d{8}_\d{6})\.csv$"
            )

            ticker_files = {}

            for filename in os.listdir(data_dir):
                match = pattern.match(filename)
                if match:
                    ticker, cik, series_id, report_date, timestamp = match.groups()
                    file_path = os.path.join(data_dir, filename)

                    # If this is the first file for this ticker
                    if ticker not in ticker_files:
                        ticker_files[ticker] = {
                            "file_path": file_path,
                            "report_date": report_date,
                            "timestamp": timestamp,
                            "cik": cik,
                            "filename": filename,
                        }
                    else:
                        current_info = ticker_files[ticker]

                        # Compare report dates first (more recent report date wins)
                        if report_date > current_info["report_date"]:
                            ticker_files[ticker] = {
                                "file_path": file_path,
                                "report_date": report_date,
                                "timestamp": timestamp,
                                "cik": cik,
                                "filename": filename,
                            }
                        # If same report date, compare timestamps (latest timestamp wins)
                        elif (
                            report_date == current_info["report_date"]
                            and timestamp > current_info["timestamp"]
                        ):
                            ticker_files[ticker] = {
                                "file_path": file_path,
                                "report_date": report_date,
                                "timestamp": timestamp,
                                "cik": cik,
                                "filename": filename,
                            }

            logger.info(
                f"Found latest enriched holdings files for {len(ticker_files)} tickers"
            )
            for ticker, info in ticker_files.items():
                logger.info(
                    f"  {ticker}: {info['filename']} (report_date: {info['report_date']}, timestamp: {info['timestamp']})"
                )

            return {ticker: info["file_path"] for ticker, info in ticker_files.items()}

        except Exception as e:
            logger.error(f"Error finding latest enriched holdings files: {e}")
            return {}

    def upload_all_latest_enriched_holdings(
        self, data_dir: str = "data"
    ) -> Dict[str, bool]:
        """
        Find and upload the latest enriched holdings file for each ticker to R2.

        Args:
            data_dir: Directory to search for enriched holdings files

        Returns:
            Dictionary mapping ticker symbols to upload success status
        """
        try:
            latest_files = self.find_latest_enriched_holdings_by_ticker(data_dir)

            if not latest_files:
                logger.warning("No enriched holdings files found to upload")
                return {}

            upload_results = {}

            for ticker, file_path in latest_files.items():
                logger.info(
                    f"Uploading latest holdings for {ticker}: {os.path.basename(file_path)}"
                )

                # Extract CIK from filename
                filename = os.path.basename(file_path)
                pattern = re.compile(r"^holdings_enriched_[A-Z]+_(\d+)_")
                match = pattern.match(filename)

                if match:
                    cik = match.group(1)
                    success = self.upload_enriched_holdings_to_latest(file_path, cik)
                    upload_results[ticker] = success

                    if success:
                        logger.info(f"Successfully uploaded {ticker} holdings")
                    else:
                        logger.error(f"Failed to upload {ticker} holdings")
                else:
                    logger.error(f"Could not extract CIK from filename: {filename}")
                    upload_results[ticker] = False

            successful_uploads = sum(
                1 for success in upload_results.values() if success
            )
            logger.info(
                f"Upload completed: {successful_uploads}/{len(upload_results)} files uploaded successfully"
            )

            return upload_results

        except Exception as e:
            logger.error(f"Error uploading all latest enriched holdings: {e}")
            return {}

    def upload_summary_tickers(
        self, file_path: str = "data/summary_tickers.json"
    ) -> bool:
        """
        Upload summary tickers JSON file to R2.

        Args:
            file_path: Path to the summary_tickers.json file

        Returns:
            True if successful, False otherwise
        """
        try:
            if not os.path.exists(file_path):
                logger.error(f"Summary tickers file not found: {file_path}")
                return False

            # Read the summary tickers JSON file
            with open(file_path, "r") as f:
                summary_data = json.load(f)

            # Add upload timestamp to metadata
            if "metadata" in summary_data:
                summary_data["metadata"][
                    "upload_timestamp"
                ] = datetime.now().isoformat()

            # Upload to R2 at root level for easy access by website
            r2_key = "summary_tickers.json"

            success = self.upload_json(summary_data, r2_key)

            if success:
                total_tickers = summary_data.get("metadata", {}).get("total_tickers", 0)
                logger.info(f"Successfully uploaded summary tickers to: {r2_key}")
                logger.info(f"Total tickers: {total_tickers}")
                logger.info(
                    f"Generation timestamp: {summary_data.get('metadata', {}).get('generated_timestamp', 'N/A')}"
                )

            return success

        except Exception as e:
            logger.error(f"Error uploading summary tickers: {e}")
            return False


def main():
    """Example usage of R2 client"""
    import argparse

    parser = argparse.ArgumentParser(description="Upload holdings data to R2")
    parser.add_argument(
        "-e",
        "--env",
        choices=["dev", "prod"],
        default="dev",
        help="Environment to upload to (default: dev)",
    )
    args = parser.parse_args()

    try:
        # Initialize R2 client
        client = R2Client(bucket=args.env)

        # Example: List objects in latest folder
        latest_objects = client.list_objects("latest/")
        logger.info(f"Objects in latest folder: {latest_objects}")

        # Upload all latest enriched holdings files automatically
        logger.info("Finding and uploading latest enriched holdings files...")
        upload_results = client.upload_all_latest_enriched_holdings("data")

        if upload_results:
            logger.info(f"Upload summary:")
            for ticker, success in upload_results.items():
                status = "SUCCESS" if success else "FAILED"
                logger.info(f"  {ticker}: {status}")
        else:
            logger.warning("No enriched holdings files found to upload")

        # Upload summary tickers file for website listing
        logger.info("Uploading summary tickers file...")
        summary_success = client.upload_summary_tickers("data/summary_tickers.json")

        if summary_success:
            logger.info("Summary tickers upload: SUCCESS")
        else:
            logger.error("Summary tickers upload: FAILED")

    except Exception as e:
        logger.error(f"Error in main: {e}")


if __name__ == "__main__":
    main()
