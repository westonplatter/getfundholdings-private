import time
import requests
from bs4 import BeautifulSoup
import logging
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlencode
import random
import json
import os
import re

class SECHTTPClient:
    """
    SEC EDGAR compliant HTTP client for fetching series data.
    Implements mandatory headers, rate limiting, and retry logic per SEC requirements.
    """
    
    def __init__(self, user_agent: str = "Weston Platter westonplatter@gmail.com"):
        """
        Initialize SEC HTTP client with compliant headers.
        
        Args:
            user_agent: Company name and email in format "Company Name email@domain.com"
        """
        self.headers = {
            'User-Agent': user_agent,
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        }
        self.base_url = "https://www.sec.gov"
        self.series_url = "https://www.sec.gov/cgi-bin/series"
        self.last_request_time = 0
        self.min_interval = 0.1  # 10 requests per second maximum
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Setup logging for compliance
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def _rate_limit(self):
        """Enforce rate limiting: maximum 10 requests per second."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_interval:
            sleep_time = self.min_interval - time_since_last
            time.sleep(sleep_time)
        self.last_request_time = time.time()
    
    def _exponential_backoff(self, attempt: int, max_delay: int = 60) -> float:
        """Calculate exponential backoff delay."""
        delay = min(max_delay, (2 ** attempt) + random.uniform(0, 1))
        return delay
    
    def _make_request(self, url: str, params: Optional[Dict] = None, max_retries: int = 3) -> requests.Response:
        """
        Make HTTP request with rate limiting and retry logic.
        
        Args:
            url: Target URL
            params: Query parameters
            max_retries: Maximum number of retry attempts
            
        Returns:
            requests.Response object
            
        Raises:
            requests.RequestException: If all retries fail
        """
        for attempt in range(max_retries + 1):
            try:
                self._rate_limit()
                
                # Log request for compliance
                self.logger.info(f"Making request to {url} (attempt {attempt + 1})")
                
                response = self.session.get(url, params=params, timeout=30)
                
                # Handle rate limiting
                if response.status_code == 403:
                    if "Request Rate Threshold Exceeded" in response.text:
                        if attempt < max_retries:
                            delay = self._exponential_backoff(attempt)
                            self.logger.warning(f"Rate limit exceeded, waiting {delay:.2f} seconds")
                            time.sleep(delay)
                            continue
                        else:
                            raise requests.RequestException("Rate limit exceeded after all retries")
                
                # Handle other HTTP errors
                if response.status_code == 404:
                    self.logger.warning(f"Resource not found: {url}")
                    return response
                
                response.raise_for_status()
                return response
                
            except requests.RequestException as e:
                if attempt < max_retries:
                    delay = self._exponential_backoff(attempt)
                    self.logger.warning(f"Request failed (attempt {attempt + 1}): {e}, retrying in {delay:.2f} seconds")
                    time.sleep(delay)
                else:
                    self.logger.error(f"All retries failed for {url}: {e}")
                    raise
        
        raise requests.RequestException("Max retries exceeded")
    
    def fetch_series_data(self, cik: str) -> List[Dict]:
        """
        Fetch series data for a given CIK from SEC series lookup page with pagination support.
        
        Args:
            cik: Company CIK number (can be with or without leading zeros)
            
        Returns:
            List of dictionaries containing series information from all pages
        """
        # Format CIK to 10 digits with leading zeros
        formatted_cik = str(cik).zfill(10)
        
        all_series_data = []
        start = 0
        count = 500  # SEC default page size
        
        while True:
            # Build parameters for current page
            params = {
                'company': '',
                'CIK': cik,
                'start': start,
                'count': count
            }
            
            self.logger.info(f"Fetching series data page: start={start}, count={count}")
            
            try:
                response = self._make_request(self.series_url, params=params)
                
                if response.status_code == 404:
                    self.logger.warning(f"No series data found for CIK {cik}")
                    break
                
                # Parse current page
                page_series_data = self._parse_series_response(response.text, formatted_cik)
                
                if not page_series_data:
                    # No data on this page, we're done
                    break
                
                # Add to our collection
                all_series_data.extend(page_series_data)
                
                # Check if there are more pages by looking for pagination indicators
                has_more_pages = self._has_more_pages(response.text, start, count)
                
                if not has_more_pages:
                    self.logger.info(f"No more pages found, stopping pagination")
                    break
                
                # Move to next page
                start += count
                self.logger.info(f"Found more pages, continuing to start={start}")
                
            except requests.RequestException as e:
                self.logger.error(f"Failed to fetch series data for CIK {cik} at start={start}: {e}")
                break
        
        self.logger.info(f"Fetched total of {len(all_series_data)} series records across all pages")
        return all_series_data
    
    def _has_more_pages(self, html_content: str, current_start: int, count: int) -> bool:
        """
        Check if there are more pages of series data available.
        
        Args:
            html_content: HTML content from current page
            current_start: Current start parameter
            count: Number of records per page
            
        Returns:
            True if more pages are available
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        
        try:
            # Look for pagination links or indicators
            # Common patterns: "Next", page numbers, or "start=" in links
            
            # Method 1: Look for "Next" links
            next_links = soup.find_all('a', href=True)
            for link in next_links:
                href = link.get('href', '')
                link_text = link.get_text(strip=True).lower()
                
                if 'next' in link_text or 'start=' in href:
                    # Check if the start parameter in the link is greater than current
                    if 'start=' in href:
                        try:
                            import re
                            start_match = re.search(r'start=(\d+)', href)
                            if start_match:
                                next_start = int(start_match.group(1))
                                if next_start > current_start:
                                    return True
                        except:
                            pass
            
            # Method 2: Look for page numbers greater than current page
            page_links = soup.find_all('a', href=True)
            current_page = (current_start // count) + 1
            
            for link in page_links:
                href = link.get('href', '')
                link_text = link.get_text(strip=True)
                
                # Check if link text is a number greater than current page
                try:
                    if link_text.isdigit():
                        page_num = int(link_text)
                        if page_num > current_page and 'start=' in href:
                            return True
                except:
                    pass
            
            # Method 3: Look for any link with start parameter higher than current
            for link in page_links:
                href = link.get('href', '')
                if 'start=' in href:
                    try:
                        import re
                        start_match = re.search(r'start=(\d+)', href)
                        if start_match:
                            link_start = int(start_match.group(1))
                            if link_start > current_start:
                                return True
                    except:
                        pass
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking for more pages: {e}")
            return False
    
    def _parse_series_response(self, html_content: str, cik: str) -> List[Dict]:
        """
        Parse HTML response from SEC series lookup page to extract structured series data.
        
        Args:
            html_content: HTML content from series lookup page
            cik: Formatted CIK number
            
        Returns:
            List of dictionaries containing parsed series information
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        series_data = []

        try:
            # Look for the main data table
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                
                # Look for table headers to identify the correct table
                header_row = None
                data_rows = []
                
                for i, row in enumerate(rows):
                    cells = row.find_all(['td', 'th'])
                    cell_texts = [cell.get_text(strip=True) for cell in cells]
                    
                    # Check if this looks like a header row
                    if any('CIK' in text or 'Series' in text or 'Class' in text for text in cell_texts):
                        header_row = cell_texts
                        data_rows = rows[i+1:]  # All rows after header
                        break
                
                # If we found a structured table, parse it
                if header_row and data_rows:
                    self.logger.info(f"Found structured table with headers: {header_row}")
                    
                    # Process rows - series rows have colspan=2 and start with S in column 2
                    i = 0
                    while i < len(data_rows):
                        current_row = data_rows[i]
                        cells = current_row.find_all(['td', 'th'])
                        
                        if len(cells) >= 2:
                            cell_texts = [cell.get_text(strip=True) for cell in cells]
                            
                            # Skip empty rows or navigation elements
                            if not any(cell_texts) or 'Home' in str(cell_texts):
                                i += 1
                                continue
                            
                            # Check if this is a series row (2nd column starts with 'S')
                            # if len(cell_texts) >= 2 and cell_texts[1].startswith('S') and len(cell_texts[1]) > 5:
                            if cell_texts[1].startswith('S'):
                                # This is a series row - extract series info
                                series_info = {
                                    'cik': cik,
                                    'series_id': cell_texts[1],  # Series ID in column 2
                                    'classes': []  # Will collect class info from following rows
                                }

                                # Look ahead for class rows that belong to this series
                                j = i + 1
                                while j < len(data_rows):
                                    next_row = data_rows[j]
                                    next_cells = next_row.find_all(['td', 'th'])
                                    
                                    if len(next_cells) >= 2:
                                        next_cell_texts = [cell.get_text(strip=True) for cell in next_cells]
                                        
                                        # Stop if we hit another series row (2nd column starts with 'S')
                                        if next_cell_texts[1].startswith('S'):
                                            break
                                        
                                        # Stop if row is empty or navigation
                                        if not any(next_cell_texts) or 'Home' in str(next_cell_texts):
                                            j += 1
                                            continue
                                        
                                        # This should be a class row - extract class info
                                        class_info = {}

                                        # Extract structured data from raw_data
                                        # Typical format: ["", "", "C000219740", "iShares 0-3 Month Treasury Bond ETF", "SGOV"]
                                        for col_idx, text in enumerate(next_cell_texts):
                                            if col_idx == 2:
                                                class_info['class_id'] = text
                                            elif col_idx == 3:
                                                class_info['class_name'] = text
                                            elif col_idx == 4:
                                                class_info['ticker'] = text

                                        
                                        # Add raw data for debugging
                                        class_info['raw_data'] = next_cell_texts
                                        
                                        if class_info:  # Only add if we found some class info
                                            series_info['classes'].append(class_info)
                                            self.logger.debug(f"  Added class: {class_info}")
                                        
                                        j += 1
                                    else:
                                        j += 1
                                
                                # Add the series with all its classes
                                if 'series_id' in series_info:
                                    series_data.append(series_info)
                                
                                # Move to the next unprocessed row
                                i = j
                            else:
                                # Not a series row, skip it
                                i += 1
                        else:
                            i += 1
                    
                    break  # We found and processed the main table
            
            # If no structured table found, fall back to generic parsing
            if not series_data:
                self.logger.warning(f"No structured table found, falling back to generic parsing")
                
                # Look for any series-like patterns in the HTML
                series_pattern = r'S\d{9}'  # Series ID pattern
                import re
                
                text_content = soup.get_text()
                series_matches = re.findall(series_pattern, text_content)
                
                for match in set(series_matches):  # Remove duplicates
                    series_data.append({
                        'series_id': match,
                        'cik': cik,
                        # 'request_timestamp': time.time(),
                        'parse_method': 'regex_fallback'
                    })
            
        except Exception as e:
            self.logger.error(f"Error parsing series response for CIK {cik}: {e}")
            # Return partial data with error info
            series_data.append({
                'error': str(e),
                'cik': cik,
                # 'request_timestamp': time.time(),
                'parse_status': 'error'
            })
        
        self.logger.info(f"Parsed {len(series_data)} series records for CIK {cik}")
        return series_data
    
    def save_series_data(self, series_data: List[Dict], cik: str, filename: Optional[str] = None) -> str:
        """
        Save series data to JSON file for future N-PORT requests.
        
        Args:
            series_data: List of series information dictionaries
            cik: Company CIK number
            filename: Optional filename, defaults to 'series_data_{cik}.json'
            
        Returns:
            Path to saved file
        """
        if filename is None:
            filename = f"series_data_{cik}.json"
        
        # Create data directory if it doesn't exist
        data_dir = "data"
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        
        filepath = os.path.join(data_dir, filename)
        
        # Prepare data for export
        export_data = {
            "cik": cik,
            "export_timestamp": time.time(),
            "total_series": len(series_data),
            "series_data": series_data
        }
        
        try:
            with open(filepath, 'w') as f:
                json.dump(export_data, f, indent=2, default=str)
            
            self.logger.info(f"Series data saved to {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"Failed to save series data: {e}")
            raise
    
    def fetch_submissions(self, cik: str) -> Dict:
        """
        Fetch submissions data from SEC API for a given CIK.
        
        Args:
            cik: Company CIK number
            
        Returns:
            Dictionary containing submissions data
        """
        # Format CIK to 10 digits with leading zeros
        formatted_cik = str(cik).zfill(10)
        
        url = f"https://data.sec.gov/submissions/CIK{formatted_cik}.json"
        
        try:
            response = self._make_request(url)
            
            if response.status_code == 404:
                self.logger.warning(f"No submissions found for CIK {cik}")
                return {}
            
            return response.json()
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch submissions for CIK {cik}: {e}")
            return {}
    
    def get_nport_filings(self, cik: str) -> List[Dict]:
        """
        Get N-PORT filings for a given CIK.
        
        Args:
            cik: Company CIK number
            
        Returns:
            List of N-PORT filing information
        """
        submissions = self.fetch_submissions(cik)
        
        if not submissions:
            return []
        
        nport_filings = []
        
        # Get recent filings data
        recent_filings = submissions.get('filings', {}).get('recent', {})
        
        if not recent_filings:
            self.logger.warning(f"No recent filings found for CIK {cik}")
            return []
        
        # Extract filing data
        forms = recent_filings.get('form', [])
        filing_dates = recent_filings.get('filingDate', [])
        accession_numbers = recent_filings.get('accessionNumber', [])
        report_dates = recent_filings.get('reportDate', [])
        
        # Filter for N-PORT filings (NPORT-P is the actual form type)
        for i, form in enumerate(forms):
            if form in ['NPORT-P', 'NPORT-P/A', 'NPORT-EX']:
                filing_info = {
                    'form': form,
                    'filing_date': filing_dates[i] if i < len(filing_dates) else None,
                    'accession_number': accession_numbers[i] if i < len(accession_numbers) else None,
                    'report_date': report_dates[i] if i < len(report_dates) else None,
                    'cik': cik
                }
                nport_filings.append(filing_info)
        
        self.logger.info(f"Found {len(nport_filings)} N-PORT filings for CIK {cik}")
        return nport_filings
    
    def save_nport_filings(self, nport_filings: List[Dict], cik: str, filename: Optional[str] = None) -> str:
        """
        Save N-PORT filings data to JSON file.
        
        Args:
            nport_filings: List of N-PORT filing information
            cik: Company CIK number
            filename: Optional filename, defaults to 'nport_filings_{cik}.json'
            
        Returns:
            Path to saved file
        """
        if filename is None:
            filename = f"nport_filings_{cik}.json"
        
        # Create data directory if it doesn't exist
        data_dir = "data"
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        
        filepath = os.path.join(data_dir, filename)
        
        # Prepare data for export
        export_data = {
            "cik": cik,
            "export_timestamp": time.time(),
            "total_nport_filings": len(nport_filings),
            "nport_filings": nport_filings
        }
        
        try:
            with open(filepath, 'w') as f:
                json.dump(export_data, f, indent=2, default=str)
            
            self.logger.info(f"N-PORT filings data saved to {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"Failed to save N-PORT filings data: {e}")
            raise
    
    def fetch_series_filings(self, series_id: str, filing_type: str = "NPORT-P") -> List[Dict]:
        """
        Fetch filings for a specific series ID from SEC browse-edgar interface.
        
        Args:
            series_id: Series ID (e.g., S000004310)
            filing_type: Type of filing to filter for (default: NPORT-P)
            
        Returns:
            List of filing information dictionaries
        """
        url = "https://www.sec.gov/cgi-bin/browse-edgar"
        params = {
            'action': 'getcompany',
            'CIK': series_id,
            'type': filing_type,
            'dateb': '',
            'count': '40',
            'scd': 'filings',
            'search_text': ''
        }
        
        try:
            response = self._make_request(url, params=params)
            
            if response.status_code == 404:
                self.logger.warning(f"No filings found for series {series_id}")
                return []
            
            return self._parse_series_filings_response(response.text, series_id, filing_type)
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch series filings for {series_id}: {e}")
            return []
    
    def _parse_series_filings_response(self, html_content: str, series_id: str, filing_type: str) -> List[Dict]:
        """
        Parse HTML response from SEC browse-edgar page to extract filing information.
        
        Args:
            html_content: HTML content from browse-edgar page
            series_id: Series ID
            filing_type: Filing type being searched
            
        Returns:
            List of dictionaries containing filing information
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        filings = []
        
        try:
            # Look for the filings table
            tables = soup.find_all('table')
            
            for table in tables:
                # Look for table with filing information
                rows = table.find_all('tr')
                
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    
                    if len(cells) >= 4:  # Typical filing row has multiple columns
                        # Extract text from cells
                        cell_texts = [cell.get_text(strip=True) for cell in cells]
                        
                        # Look for rows that contain the filing type
                        if any(filing_type in text for text in cell_texts):
                            # Extract accession number from both links and raw text
                            links = row.find_all('a', href=True)
                            accession_number = None
                            
                            # First try to extract from raw text (more reliable)
                            accession_number = self._extract_accession_number(cell_texts)
                            
                            # If no accession number from raw text, try to extract from EDGAR links
                            if not accession_number:
                                for link in links:
                                    href = link.get('href', '')
                                    if 'Archives/edgar/data' in href:
                                        # Extract accession number from URL
                                        parts = href.split('/')
                                        if len(parts) > 4:
                                            accession_number = parts[4]
                                            break
                            
                            # Extract filing information
                            filing_info = {
                                'series_id': series_id,
                                'form_type': filing_type,
                                'accession_number': accession_number,
                                # 'request_timestamp': time.time()
                            }
                            
                            # Try to extract date information
                            for i, text in enumerate(cell_texts):
                                if len(text) == 10 and '-' in text:  # Looks like a date
                                    if 'filing_date' not in filing_info:
                                        filing_info['filing_date'] = text
                                    else:
                                        filing_info['report_date'] = text
                            
                            # Extract additional cell data
                            if len(cell_texts) >= 4:
                                filing_info['raw_data'] = cell_texts
                            
                            if accession_number:  # Only add if we found an accession number
                                filings.append(filing_info)
            
            # If no structured data found, look for any EDGAR links
            if not filings:
                links = soup.find_all('a', href=True)
                for link in links:
                    href = link.get('href', '')
                    if 'Archives/edgar/data' in href and filing_type.lower() in link.get_text().lower():
                        parts = href.split('/')
                        if len(parts) > 4:
                            accession_number = parts[4]
                            filing_info = {
                                'series_id': series_id,
                                'form_type': filing_type,
                                'accession_number': accession_number,
                                'link_text': link.get_text(strip=True),
                                # 'request_timestamp': time.time(),
                                'parse_method': 'link_extraction'
                            }
                            filings.append(filing_info)
                        
        except Exception as e:
            self.logger.error(f"Error parsing series filings for {series_id}: {e}")
            # Return partial data with error info
            filings.append({
                'series_id': series_id,
                'error': str(e),
                # 'request_timestamp': time.time(),
                'parse_status': 'error'
            })
        
        self.logger.info(f"Found {len(filings)} {filing_type} filings for series {series_id}")
        return filings
    
    def save_series_filings(self, series_filings: List[Dict], series_id: str, filing_type: str = "NPORT-P", cik: Optional[str] = None, filename: Optional[str] = None) -> str:
        """
        Save series-specific filings data to JSON file.
        
        Args:
            series_filings: List of filing information
            series_id: Series ID
            filing_type: Filing type
            cik: Optional CIK number to include in filename
            filename: Optional filename, defaults to 'series_filings_{cik}_{series_id}_{filing_type}.json'
            
        Returns:
            Path to saved file
        """
        if filename is None:
            if cik:
                filename = f"series_filings_{cik}_{series_id}_{filing_type.lower()}.json"
            else:
                filename = f"series_filings_{series_id}_{filing_type.lower()}.json"
        
        # Create data directory if it doesn't exist
        data_dir = "data"
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        
        filepath = os.path.join(data_dir, filename)
        
        # Prepare data for export
        export_data = {
            "series_id": series_id,
            "filing_type": filing_type,
            "export_timestamp": time.time(),
            "total_filings": len(series_filings),
            "filings": series_filings
        }
        
        try:
            with open(filepath, 'w') as f:
                json.dump(export_data, f, indent=2, default=str)
            
            self.logger.info(f"Series {filing_type} filings saved to {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"Failed to save series filings data: {e}")
            raise
    
    def load_series_data(self, cik: str) -> Dict:
        """
        Load series data from previously saved JSON file.
        
        Args:
            cik: Company CIK number
            
        Returns:
            Dictionary containing series data
        """
        filename = f"series_data_{cik}.json"
        data_dir = "data"
        filepath = os.path.join(data_dir, filename)
        
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            self.logger.info(f"Loaded series data from {filepath}")
            return data
            
        except FileNotFoundError:
            self.logger.warning(f"Series data file not found: {filepath}")
            return {}
        except Exception as e:
            self.logger.error(f"Failed to load series data: {e}")
            return {}
        
    def load_series_filings(self, cik: str, series_id: str, filing_type: str = "NPORT-P") -> Dict:
        """
        Load series filings data from previously saved JSON file.
        
        Args:
            cik: Company CIK number
            series_id: Series ID
            filing_type: Type of filing to fetch (default: NPORT-P)
            
        Returns:
            Dictionary containing series filings data
        """
        filename = f"series_filings_{cik}_{series_id}_{filing_type.lower()}.json"
        data_dir = "data"

        filepath = os.path.join(data_dir, filename)

        try:
            with open(filepath, 'r') as f:
                data = json.load(f)


            self.logger.info(f"Loaded series filings from {filepath}")
            return data

        except FileNotFoundError:
            self.logger.warning(f"Series filings file not found: {filepath}")
            return {}

    
    def extract_series_ids(self, series_data: Dict) -> List[str]:
        """
        Extract valid series IDs from series data.
        
        Args:
            series_data: Dictionary containing series data
            
        Returns:
            List of series IDs
        """
        series_ids = []
        
        if not series_data or 'series_data' not in series_data:
            return series_ids
        
        for series in series_data['series_data']:
            series_id = series.get('series_id', '')
            
            # Filter for valid series IDs (format: S followed by digits)
            if series_id and series_id.startswith('S') and len(series_id) > 5:
                # Make sure it's not navigation text
                if 'Home' not in series_id and 'EDGAR' not in series_id:
                    series_ids.append(series_id)
        
        # Remove duplicates and sort
        unique_series_ids = sorted(list(set(series_ids)))
        
        self.logger.info(f"Extracted {len(unique_series_ids)} unique series IDs")
        return unique_series_ids
    
    def process_cik_series_filings(self, cik: str, filing_type: str = "NPORT-P") -> Dict[str, str]:
        """
        Process all series for a given CIK to fetch their N-PORT filings.
        
        Args:
            cik: Company CIK number
            filing_type: Type of filing to fetch (default: NPORT-P)
            
        Returns:
            Dictionary mapping series_id to saved file path
        """
        # Load existing series data
        series_data = self.load_series_data(cik)
        
        if not series_data:
            self.logger.error(f"No series data found for CIK {cik}")
            return {}
        
        # Extract series IDs
        series_ids = self.extract_series_ids(series_data)
        
        if not series_ids:
            self.logger.warning(f"No valid series IDs found for CIK {cik}")
            return {}
        
        self.logger.info(f"Processing {len(series_ids)} series for CIK {cik}")
        
        saved_files = {}
        
        for series_id in series_ids:
            try:
                self.logger.info(f"Fetching {filing_type} filings for series {series_id}")
                
                # Fetch series-specific filings
                series_filings = self.fetch_series_filings(series_id, filing_type)
                
                if series_filings:
                    # Save to file with CIK in filename
                    saved_file = self.save_series_filings(series_filings, series_id, filing_type, cik)
                    saved_files[series_id] = saved_file
                    
                    self.logger.info(f"Found {len(series_filings)} {filing_type} filings for {series_id}")
                else:
                    self.logger.warning(f"No {filing_type} filings found for series {series_id}")
                    
            except Exception as e:
                self.logger.error(f"Failed to process series {series_id}: {e}")
                continue
        
        self.logger.info(f"Completed processing {len(saved_files)} series for CIK {cik}")
        return saved_files
    
    def _extract_accession_number(self, cell_texts: List[str]) -> Optional[str]:
        """
        Extract accession number from cell text using regex patterns.
        
        Args:
            cell_texts: List of cell text content
            
        Returns:
            Extracted accession number or None
        """
        # Pattern to match accession numbers in text like "Acc-no: 0001752724-25-119791"
        accession_pattern = r'Acc-no:\s*(\d{10}-\d{2}-\d{6})'
        
        for text in cell_texts:
            if text:  # Make sure text is not None or empty
                match = re.search(accession_pattern, text)
                if match:
                    accession_number = match.group(1)
                    self.logger.debug(f"Extracted accession number: {accession_number} from text: {text}")
                    return accession_number
        
        # If no match found, log for debugging
        self.logger.debug(f"No accession number found in cell texts: {cell_texts}")
        return None
    
    def build_nport_url(self, cik: str, accession_number: str) -> str:
        """
        Build N-PORT XML download URL from CIK and accession number.
        
        Args:
            cik: Company CIK number (e.g., "1100663")
            accession_number: e.g., "0001752724-25-119791"
        
        Returns:
            Complete URL to N-PORT XML file
        """
        # Format CIK (remove leading zeros but keep at least one digit)
        formatted_cik = str(cik).lstrip('0') or '0'
        
        # Remove dashes for directory name
        directory = accession_number.replace('-', '')
        
        # Build URL
        url = f"https://www.sec.gov/Archives/edgar/data/{formatted_cik}/{directory}/primary_doc.xml"
        
        return url
    
    def download_nport_xml(self, cik: str, accession_number: str) -> Optional[str]:
        """
        Download N-PORT XML file from SEC EDGAR.
        
        Args:
            cik: Company CIK number (e.g., "1100663")
            accession_number: e.g., "0001752724-25-119791"
            
        Returns:
            XML content as string or None if failed
        """
        url = self.build_nport_url(cik, accession_number)
        
        try:
            # Update headers for XML content
            original_accept = self.headers.get('Accept')
            self.headers['Accept'] = 'application/xml, text/xml, */*'
            self.session.headers.update(self.headers)
            
            response = self._make_request(url)
            
            if response.status_code == 404:
                self.logger.warning(f"N-PORT XML not found for accession {accession_number}")
                return None
            
            # Restore original headers
            self.headers['Accept'] = original_accept
            self.session.headers.update(self.headers)
            
            self.logger.info(f"Downloaded N-PORT XML for accession {accession_number} ({len(response.text)} bytes)")
            return response.text
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to download N-PORT XML for {accession_number}: {e}")
            return None
    
    def save_nport_xml(self, xml_content: str, accession_number: str, cik: str, series_id: str, filename: Optional[str] = None) -> str:
        """
        Save N-PORT XML content to file.
        
        Args:
            xml_content: XML content string
            accession_number: Accession number for naming
            cik: CIK number for filename
            series_id: Series ID for filename
            filename: Optional filename, defaults to structured name with CIK and series
            
        Returns:
            Path to saved file
        """
        if filename is None:
            # Clean accession number for filename (replace dashes with underscores)
            clean_accession = accession_number.replace('-', '_')
            filename = f"nport_{cik}_{series_id}_{clean_accession}.xml"
        
        # Create data directory if it doesn't exist
        data_dir = "data"
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        
        filepath = os.path.join(data_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(xml_content)
            
            self.logger.info(f"N-PORT XML saved to {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"Failed to save N-PORT XML: {e}")
            raise
    
    def build_nport_index_url(self, cik: str, accession_number: str) -> str:
        """
        Build N-PORT index page URL from CIK and accession number.
        
        Args:
            cik: Company CIK number (e.g., "1100663")
            accession_number: e.g., "0001752724-25-119791"
        
        Returns:
            Complete URL to N-PORT index page
        """
        # Format CIK (remove leading zeros but keep at least one digit)
        formatted_cik = str(cik).lstrip('0') or '0'
        
        # Remove dashes for directory name
        directory = accession_number.replace('-', '')
        
        # Build URL
        url = f"https://www.sec.gov/Archives/edgar/data/{formatted_cik}/{directory}/{accession_number}-index.htm"
        
        return url
    
    def download_and_save_nport(self, cik: str, accession_number: str, series_id: str) -> Optional[str]:
        """
        Download and save N-PORT XML file.
        
        Args:
            cik: Company CIK number (e.g., "1100663")
            accession_number: e.g., "0001752724-25-119791"
            series_id: Series ID (e.g., "S000004310")
        
        Returns:
            Path to saved file or None if failed
        """
        xml_content = self.download_nport_xml(cik, accession_number)
        
        if xml_content:
            return self.save_nport_xml(xml_content, accession_number, cik, series_id)
        
        return None