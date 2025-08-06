import os
import re
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

from loguru import logger
from parse_nport import NPortParser

def get_data_dir():
    return Path(__file__).parent.parent / "data"


def extract_metadata_from_filename(xml_file_path: Path) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extract CIK, series ID, and accession number from XML filename.
    
    Expected format: nport_{cik}_{series_id}_{accession_number}.xml
    Example: nport_0001145549_S000077649_0001145549_23_038190.xml
    
    Returns:
        Tuple of (cik, series_id, accession_number)
    """
    filename = xml_file_path.stem
    
    # Pattern: nport_{cik}_{series_id}_{accession_number}
    pattern = r'^nport_([0-9]{10})_([A-Z][0-9]{9})_(.+)$'
    match = re.match(pattern, filename)
    
    if match:
        cik, series_id, accession_number = match.groups()
        return cik, series_id, accession_number
    else:
        logger.warning(f"Could not extract metadata from filename: {filename}")
        return None, None, None


def process_downloaded_xml_files(nport_files: List[Path]) -> int:
    """
    Process downloaded XML files to extract holdings data (standalone version).
    
    Args:
        nport_files: List of Path objects pointing to N-PORT XML files

    Returns:
        Number of XML files successfully processed
    """
    logger.info("=== Processing N-PORT XML Files ===")
    logger.info(f"Processing {len(nport_files)} XML files")
    
    processed_count = 0
    data_dir = get_data_dir()
    
    # Ensure data directory exists
    data_dir.mkdir(exist_ok=True)

    for xml_file_path in nport_files:
        try:
            logger.info(f"  └─ Processing: {xml_file_path.name}")
            
            # Extract metadata from filename
            cik, series_id, accession_number = extract_metadata_from_filename(xml_file_path)
            
            if not all([cik, series_id, accession_number]):
                logger.error(f"    │ Could not extract required metadata from filename")
                continue
            
            # Parse XML to extract holdings using updated API
            parser = NPortParser(str(xml_file_path))
            holdings_df, fund_info = parser.to_dataframes()

            if holdings_df is not None and not holdings_df.empty:
                # Extract report date from fund info or use default
                report_date = fund_info.get("report_period_date", "")
                if report_date:
                    # Convert YYYY-MM-DD to YYYYMMDD
                    try:
                        report_date_obj = datetime.strptime(report_date, "%Y-%m-%d")
                        report_date_str = report_date_obj.strftime("%Y%m%d")
                    except ValueError:
                        report_date_str = "unknown"
                else:
                    report_date_str = "unknown"
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                # Create organized folder structure: holdings_raw/{cik}/{series_id}/
                holdings_raw_dir = data_dir / "holdings_raw" / cik / series_id
                holdings_raw_dir.mkdir(parents=True, exist_ok=True)
                
                # Generate CSV filename (simplified since it's in organized folders)
                csv_filename = f"holdings_raw_{cik}_{series_id}_{report_date_str}_{timestamp}.csv"
                csv_file_path = holdings_raw_dir / csv_filename
                
                # Save holdings to CSV
                holdings_df.to_csv(csv_file_path, index=False)
                
                processed_count += 1
                logger.info(f"    │ Processed {len(holdings_df)} holdings → {csv_filename}")
                logger.info(f"    │ Saved to: holdings_raw/{cik}/{series_id}/")
                logger.info(f"    │ Fund: {fund_info.get('fund_name', 'Unknown')}")
                logger.info(f"    │ Report Date: {report_date or 'Unknown'}")
                
                # Log top holdings info if available
                if 'value_usd' in holdings_df.columns:
                    top_holdings = holdings_df.nlargest(3, 'value_usd')
                    total_value = holdings_df['value_usd'].sum()
                    logger.info(f"    │ Total Value: ${total_value:,.0f}")
                    logger.info(f"    │ Top Holdings: {', '.join(top_holdings['name'].head(3).tolist())}")
                    
            else:
                logger.warning(f"    │ No holdings data found in {xml_file_path.name}")

        except Exception as e:
            logger.error(f"    │ Error processing {xml_file_path.name}: {e}")
            continue

    logger.info(f"Successfully processed {processed_count}/{len(nport_files)} XML files")
    return processed_count


def find_xml_files_for_series(series_id: str, data_dir: Optional[Path] = None) -> List[Path]:
    """
    Find XML files for a specific series ID.
    
    Args:
        series_id: Series ID to search for (e.g., S000077649)
        data_dir: Directory to search in (defaults to project data directory)
    
    Returns:
        List of Path objects for matching XML files
    """
    if data_dir is None:
        data_dir = get_data_dir()
    
    # Search for XML files containing the series ID in the filename
    xml_files = []
    pattern = f"*{series_id}*.xml"
    
    for xml_file in data_dir.glob(pattern):
        if xml_file.is_file():
            xml_files.append(xml_file)
    
    # Sort by filename for consistent processing order
    xml_files.sort(key=lambda p: p.name)
    
    logger.info(f"Found {len(xml_files)} XML files for series {series_id}")
    return xml_files


if __name__ == "__main__":
    logger.info("=== N-PORT XML Processing POC ===")
    
    # Focus on series S000077649 as requested
    target_series = "S000077649"
    logger.info(f"Searching for XML files with series ID: {target_series}")
    
    # Find XML files for the target series
    xml_files = find_xml_files_for_series(target_series)
    
    if not xml_files:
        logger.error(f"No XML files found for series {target_series}")
        logger.info("Available XML files in data directory:")
        data_dir = get_data_dir()
        all_xml = list(data_dir.glob("*.xml"))
        for f in sorted(all_xml)[:10]:  # Show first 10
            logger.info(f"  - {f.name}")
        if len(all_xml) > 10:
            logger.info(f"  ... and {len(all_xml) - 10} more files")
        exit(1)
    
    logger.info(f"Found files for processing:")
    for xml_file in xml_files[:5]:  # Show first 5
        logger.info(f"  - {xml_file.name}")
    if len(xml_files) > 5:
        logger.info(f"  ... and {len(xml_files) - 5} more files")
    
    # Process the XML files
    logger.info("")
    processed_count = process_downloaded_xml_files(xml_files)
    
    # Summary
    logger.info("")
    logger.info("=== Processing Complete ===")
    logger.info(f"Total files processed: {processed_count}")
    logger.info(f"Success rate: {processed_count/len(xml_files)*100:.1f}%")
    
    # Show generated CSV files from organized folder structure
    data_dir = get_data_dir()
    holdings_raw_dir = data_dir / "holdings_raw"
    csv_files = list(holdings_raw_dir.glob("**/holdings_raw_*.csv"))  # Recursive search
    recent_csv = sorted(csv_files, key=lambda p: p.stat().st_mtime, reverse=True)[:processed_count]
    
    if recent_csv:
        logger.info(f"Generated CSV files:")
        for csv_file in recent_csv:
            file_size = csv_file.stat().st_size / 1024  # KB
            # Show relative path from data directory
            relative_path = csv_file.relative_to(data_dir)
            logger.info(f"  - {relative_path} ({file_size:.1f} KB)")
    
    logger.info("POC completed successfully!")


