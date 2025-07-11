#!/usr/bin/env python3
"""
Cloudflare R2 Client for Holdings Data Upload

This module provides a Cloudflare R2 client for uploading enriched holdings data,
following the same structure as the SEC and OpenFIGI clients.
"""

import boto3
import json
import os
import re
import pandas as pd
from datetime import datetime
from typing import Dict, Optional, List
from botocore.config import Config
from dotenv import load_dotenv
from loguru import logger


class R2Client:
    """
    Cloudflare R2 client for uploading enriched holdings data.
    Implements proper configuration, error handling, and logging per R2 requirements.
    """
    
    def __init__(self, env_file: str = ".env"):
        """
        Initialize R2 client with Cloudflare credentials.
        
        Args:
            env_file: Path to environment file containing R2 credentials
        """
        # Load environment variables
        load_dotenv(env_file)
        
        # Initialize R2 client
        self.client = self._setup_r2_client()
        self.bucket_name = os.getenv('CLOUDFLARE_R2_BUCKET_NAME')
        
        if not self.bucket_name:
            raise ValueError("CLOUDFLARE_R2_BUCKET_NAME not found in environment variables")
        
        logger.info(f"R2 client initialized for bucket: {self.bucket_name}")
    
    def _setup_r2_client(self):
        """Setup R2 client with proper configuration."""
        endpoint_url = os.getenv('CLOUDFLARE_R2_ENDPOINT_URL')
        access_key_id = os.getenv('CLOUDFLARE_R2_ACCESS_KEY_ID')
        secret_access_key = os.getenv('CLOUDFLARE_R2_SECRET_ACCESS_KEY')
        
        if not all([endpoint_url, access_key_id, secret_access_key]):
            raise ValueError("Missing required R2 environment variables")
        
        return boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            config=Config(signature_version='s3v4'),
            region_name='auto'
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
                ContentType='application/json'
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
            
            # Convert DataFrame to JSON-friendly format
            # Convert to records format (list of dictionaries)
            holdings_data = df.to_dict('records')
            
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
                    "upload_timestamp": datetime.now().isoformat()
                },
                "holdings": holdings_data
            }
            
            logger.info(f"Converted CSV to JSON: {len(holdings_data)} holdings for {fund_ticker}")
            return json_data
            
        except Exception as e:
            logger.error(f"Error reading CSV file {file_path}: {e}")
            return None
    
    def upload_dataframe_json(self, df: pd.DataFrame, key: str, metadata: Optional[Dict] = None) -> bool:
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
            # Convert DataFrame to JSON-friendly format
            holdings_data = df.to_dict('records')
            
            # Create structured JSON data
            json_data = {
                "metadata": {
                    "total_holdings": len(holdings_data),
                    "upload_timestamp": datetime.now().isoformat(),
                    **(metadata or {})
                },
                "holdings": holdings_data
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
        timestamp_pattern = r'(\d{8}_\d{6})\.csv$'
        
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
            basename = filename.replace('.csv', '')
            parts = basename.split('_')
            
            # For enriched files: holdings_enriched_{ticker}_{cik}_{series_id}_{report_date}_{timestamp}
            # For regular files: holdings_{ticker}_{cik}_{series_id}_{report_date}_{timestamp}
            if basename.startswith('holdings_enriched_') and len(parts) >= 3:
                return parts[2]  # Third part is the ticker
            elif basename.startswith('holdings_') and len(parts) >= 2:
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
                logger.info(f"Uploaded enriched holdings JSON to latest folder: {r2_key}")
                logger.info(f"Fund ticker: {fund_ticker}")
                logger.info(f"CIK: {cik}")
                logger.info(f"Holdings count: {json_data['metadata']['total_holdings']}")
                logger.info(f"Data timestamp: {json_data['metadata']['data_timestamp']}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error uploading enriched holdings to latest: {e}")
            return False
    
    def upload_enriched_holdings_dataframe_to_latest(self, df: pd.DataFrame, fund_ticker: str, cik: str, timestamp: Optional[str] = None) -> bool:
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
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Construct R2 key for latest folder using fund ticker
            r2_key = f"latest/{fund_ticker}/holdings_enriched.json"
            
            # Create metadata for JSON
            metadata = {
                "fund_ticker": fund_ticker,
                "cik": cik,
                "data_timestamp": timestamp
            }
            
            # Upload DataFrame as JSON
            success = self.upload_dataframe_json(df, r2_key, metadata)
            
            if success:
                logger.info(f"Uploaded enriched holdings DataFrame JSON to latest folder: {r2_key}")
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
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            objects = []
            if 'Contents' in response:
                objects = [obj['Key'] for obj in response['Contents']]
            
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
            self.client.delete_object(
                Bucket=self.bucket_name,
                Key=key
            )
            
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
            response = self.client.head_object(
                Bucket=self.bucket_name,
                Key=key
            )
            
            info = {
                'key': key,
                'size': response.get('ContentLength'),
                'last_modified': response.get('LastModified'),
                'content_type': response.get('ContentType'),
                'etag': response.get('ETag')
            }
            
            return info
            
        except Exception as e:
            logger.warning(f"Object {key} not found or error getting info: {e}")
            return None


def main():
    """Example usage of R2 client"""
    try:
        # Initialize R2 client
        client = R2Client()
        
        # Example: List objects in latest folder
        latest_objects = client.list_objects("latest/")
        logger.info(f"Objects in latest folder: {latest_objects}")
        
        # Example: Upload a test CSV file as JSON (if it exists)
        # New filename format: holdings_enriched_{fund_ticker}_{cik}_{series_id}_{report_date}_{timestamp}.csv
        test_file = "data/holdings_enriched_IVV_1100663_S000004310_20250711_20250711_143022.csv"
        if os.path.exists(test_file):
            success = client.upload_enriched_holdings_to_latest(test_file, "1100663")
            if success:
                logger.info("Test CSV to JSON upload successful")
            else:
                logger.error("Test CSV to JSON upload failed")
        else:
            logger.info(f"Test file not found: {test_file}")
            
            # Look for any enriched holdings CSV files in data directory
            data_dir = "data"
            if os.path.exists(data_dir):
                for file in os.listdir(data_dir):
                    if file.startswith("holdings_enriched_") and file.endswith(".csv"):
                        test_file = os.path.join(data_dir, file)
                        logger.info(f"Found test CSV file: {test_file}")
                        success = client.upload_enriched_holdings_to_latest(test_file, "1100663")
                        if success:
                            logger.info("Test CSV to JSON upload successful")
                        else:
                            logger.error("Test CSV to JSON upload failed")
                        break
        
        # Example: Test CSV to JSON conversion without upload
        if os.path.exists(test_file):
            json_data = client.read_csv_to_json(test_file)
            if json_data:
                logger.info(f"CSV conversion successful. Holdings: {json_data['metadata']['total_holdings']}")
                logger.info(f"Fund: {json_data['metadata']['fund_ticker']}")
                logger.info(f"Sample holding: {json_data['holdings'][0] if json_data['holdings'] else 'None'}")
            else:
                logger.error("CSV to JSON conversion failed")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")


if __name__ == "__main__":
    main()