# API Client Specification

This document defines the standard pattern for API clients in the GetFundHoldings project. All API clients should follow this structure for consistency, maintainability, and reliability.

## Architecture Pattern

### Class Structure

API clients should be organized as follows:

```python
class NamedAPIClient:
    """
    Client for NamedAPI that handles [specific functionality].
    Implements proper headers, rate limiting, and error handling.
    """
    
    def __init__(self, config_params):
        # Configuration and setup
        
    # Private configuration methods
    def _setup_headers(self):
        # Configure session headers
        
    def _rate_limit(self):
        # Rate limiting implementation
        
    def _make_request(self, url, data=None, max_retries=3):
        # Core HTTP request with retry logic
        
    # Private utility methods
    def _parse_response(self, response):
        # Response parsing logic
        
    def _handle_errors(self, response):
        # Error handling logic
        
    # Cache management methods
    def _load_cache(self):
        # Load cached data
        
    def _save_cache(self):
        # Save data to cache
        
    # Public API methods
    def get_data(self, params):
        # Main API functionality
        
    def process_data(self, data):
        # Data processing methods
        
    def save_data(self, data, filename):
        # Data persistence methods
```

## Required Components

### 1. Configuration and Initialization

```python
def __init__(self, config_params):
    # API configuration
    self.base_url = "https://api.example.com"
    self.api_key = api_key
    
    # Rate limiting configuration
    self.last_request_time = 0
    self.min_interval = 60 / rate_limit  # Convert rate limit to seconds
    
    # Session setup
    self.session = requests.Session()
    self._setup_headers()
    
    # Cache management
    self.cache_file = cache_file
    self.cache = self._load_cache()
    
    # Logging
    self.logger = logging.getLogger(__name__)
```

### 2. Rate Limiting

All API clients must implement rate limiting to comply with API terms:

```python
def _rate_limit(self):
    """Enforce rate limiting based on API limits."""
    current_time = time.time()
    time_since_last = current_time - self.last_request_time
    
    if time_since_last < self.min_interval:
        sleep_time = self.min_interval - time_since_last
        self.logger.info(f"Rate limiting: waiting {sleep_time:.1f} seconds...")
        time.sleep(sleep_time)
    
    self.last_request_time = time.time()
```

### 3. HTTP Request Handling

```python
def _make_request(self, url: str, data: Optional[dict] = None, max_retries: int = 3) -> requests.Response:
    """
    Make HTTP request with rate limiting and retry logic.
    
    Args:
        url: Target URL
        data: Request data (for POST requests)
        max_retries: Maximum number of retry attempts
        
    Returns:
        requests.Response object
        
    Raises:
        requests.RequestException: If all retries fail
    """
    for attempt in range(max_retries + 1):
        try:
            # Apply rate limiting
            self._rate_limit()
            
            # Log request
            self.logger.debug(f"Making request to {url} (attempt {attempt + 1})")
            
            # Make request
            response = self.session.post(url, json=data, timeout=30)
            
            # Handle rate limiting
            if response.status_code == 429:
                if attempt < max_retries:
                    backoff_time = min(60, (2 ** attempt) * 5)
                    self.logger.warning(f"Rate limit exceeded, waiting {backoff_time} seconds...")
                    time.sleep(backoff_time)
                    continue
                else:
                    raise requests.RequestException("Rate limit exceeded after all retries")
            
            # Handle other errors
            if response.status_code >= 400:
                self.logger.warning(f"HTTP error {response.status_code}: {response.text}")
                if response.status_code == 404:
                    return response  # Return 404 for handling
            
            response.raise_for_status()
            return response
            
        except requests.RequestException as e:
            if attempt < max_retries:
                delay = (2 ** attempt) + 1
                self.logger.warning(f"Request failed (attempt {attempt + 1}): {e}, retrying in {delay} seconds")
                time.sleep(delay)
            else:
                self.logger.error(f"All retries failed for {url}: {e}")
                raise
    
    raise requests.RequestException("Max retries exceeded")
```

### 4. Cache Management

```python
def _load_cache(self) -> Dict:
    """Load cached data from file."""
    if os.path.exists(self.cache_file):
        try:
            with open(self.cache_file, 'r') as f:
                data = json.load(f)
                self.logger.info(f"Loaded {len(data)} cached entries from {self.cache_file}")
                return data
        except Exception as e:
            self.logger.warning(f"Failed to load cache file: {e}")
            return {}
    return {}

def _save_cache(self):
    """Save cache to file."""
    try:
        with open(self.cache_file, 'w') as f:
            json.dump(self.cache, f, indent=2)
        self.logger.info(f"Saved {len(self.cache)} entries to cache file")
    except Exception as e:
        self.logger.error(f"Failed to save cache file: {e}")
```

### 5. Error Handling and Logging

```python
# Use structured logging
logging.basicConfig(level=logging.INFO)
self.logger = logging.getLogger(__name__)

# Log all significant events
self.logger.info("API client initialized")
self.logger.debug(f"Making request to {url}")
self.logger.warning(f"Rate limit exceeded")
self.logger.error(f"Request failed: {error}")
```

## Implementation Examples

### SEC API Client (`sec_client.py`)

**Purpose**: Fetch SEC EDGAR data with compliance requirements
- **Rate Limit**: 10 requests per second
- **Headers**: Mandatory user agent format
- **Special Requirements**: Legal compliance, detailed logging

**Key Features**:
- Legal compliance headers
- Rate limiting enforcement
- Exponential backoff for retries
- Structured data parsing
- Pagination handling

### OpenFIGI API Client (`cusip_to_ticker.py`)

**Purpose**: Map CUSIP identifiers to ticker symbols
- **Rate Limit**: 25 requests per minute
- **Headers**: JSON content type
- **Special Requirements**: Caching to avoid repeated lookups

**Key Features**:
- Efficient caching system
- Batch processing capabilities
- Data validation
- Pandas DataFrame integration

## Best Practices

### 1. Error Handling

- Always implement exponential backoff for rate limiting
- Handle HTTP status codes appropriately
- Log all errors with context
- Return meaningful error messages
- Implement circuit breaker patterns for persistent failures

### 2. Data Persistence

- Use JSON for cache files (human-readable and debuggable)
- Include timestamps in saved data
- Create data directory if it doesn't exist
- Use structured filenames with identifiers

### 3. Performance

- Implement caching to avoid duplicate requests
- Use session objects for connection reuse
- Batch requests when possible
- Monitor and log performance metrics

### 4. Compliance

- Respect API rate limits strictly
- Include required headers
- Log all requests for audit purposes
- Implement proper user agent strings
- Handle legal compliance requirements

### 5. Testing

- Mock external API calls in tests
- Test rate limiting behavior
- Verify cache functionality
- Test error scenarios
- Validate data parsing

## Common Patterns

### Data Processing Pipeline

```python
def process_data(self, raw_data):
    """Process raw API data into structured format."""
    processed_data = []
    
    for item in raw_data:
        processed_item = {
            'id': item.get('id'),
            'name': item.get('name'),
            'processed_at': time.time()
        }
        processed_data.append(processed_item)
    
    return processed_data
```

### Batch Processing

```python
def process_batch(self, items, batch_size=100):
    """Process items in batches to manage memory and rate limits."""
    results = []
    
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        batch_results = self._process_batch_items(batch)
        results.extend(batch_results)
        
        # Log progress
        self.logger.info(f"Processed {min(i + batch_size, len(items))}/{len(items)} items")
    
    return results
```

### Data Validation

```python
def _validate_data(self, data, required_fields):
    """Validate data structure and required fields."""
    if not data:
        raise ValueError("Data cannot be empty")
    
    for field in required_fields:
        if field not in data:
            raise ValueError(f"Required field '{field}' not found in data")
    
    return True
```

## Required Dependencies

All API clients should include these dependencies:

```python
import requests
import time
import logging
import json
import os
from typing import Dict, Optional, List
from urllib.parse import urljoin, urlencode
```

## File Structure

API clients should be organized as follows:

```
project/
├── clients/
│   ├── sec_client.py          # SEC EDGAR API client
│   ├── cusip_to_ticker.py     # OpenFIGI API client
│   └── base_client.py         # Base class for common functionality
├── data/
│   ├── cache/                 # Cache files
│   └── raw/                   # Raw data files
└── tests/
    └── test_clients.py        # Client tests
```

## Configuration

API clients should support configuration through:

1. **Environment variables** for sensitive data (API keys)
2. **Configuration files** for static settings
3. **Constructor parameters** for runtime configuration

```python
# Environment variables
api_key = os.getenv('OPENFIGI_API_KEY')

# Configuration file
with open('config.json', 'r') as f:
    config = json.load(f)

# Constructor parameters
client = OpenFIGIClient(
    api_key=api_key,
    cache_file=config['cache_file'],
    rate_limit=config['rate_limit']
)
```

## Error Handling Patterns

### Rate Limiting Errors

```python
if response.status_code == 429:
    retry_after = int(response.headers.get('Retry-After', 60))
    self.logger.warning(f"Rate limited, waiting {retry_after} seconds")
    time.sleep(retry_after)
```

### API Errors

```python
if response.status_code >= 400:
    error_data = response.json() if response.content else {}
    error_message = error_data.get('error', 'Unknown error')
    self.logger.error(f"API error {response.status_code}: {error_message}")
    raise APIException(f"API error: {error_message}")
```

### Network Errors

```python
try:
    response = self.session.post(url, json=data, timeout=30)
except requests.ConnectionError:
    self.logger.error("Network connection failed")
    raise NetworkException("Unable to connect to API")
except requests.Timeout:
    self.logger.error("Request timeout")
    raise TimeoutException("Request timeout")
```

This specification ensures all API clients in the project follow consistent patterns for reliability, maintainability, and compliance with external API requirements.