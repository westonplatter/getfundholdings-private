# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a private, closed-source Python codebase for fetching data from the SEC EDGAR system and storing it in a private data warehouse. It's the third component of the GetFundHoldings.com ecosystem, complementing the public website and open-source download scripts.

## Python Development Commands

- Run Python commands: `uv run python <script>`
- Run main script: `uv run python main.py`
- Project uses pyproject.toml for dependency management

## SEC EDGAR API Requirements

**CRITICAL COMPLIANCE REQUIREMENTS** - Non-compliance results in immediate IP blocking:

### Mandatory Headers
- **User-Agent**: Must be in format `"Company Name email@domain.com"`
- **Accept**: `application/json`
- **Accept-Encoding**: `gzip, deflate`
- **Connection**: `keep-alive`

### Rate Limiting
- **10 requests per second maximum** - strictly enforced
- Implement minimum 0.1 second interval between requests
- Use exponential backoff for retry logic
- Monitor for HTTP 403 "Request Rate Threshold Exceeded" responses

### Legal Compliance
- Subject to Computer Fraud and Abuse Act of 1986
- Enhanced monitoring since July 2021
- Violations can result in prosecution for unauthorized computer access
- Maintain detailed access logs

## Technical Implementation Patterns

### SEC Client Implementation
```python
class SECEdgarClient:
    def __init__(self, user_agent: str):
        self.headers = {
            'User-Agent': user_agent,
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate'
        }
        self.last_request_time = 0
        self.min_interval = 0.1  # 10 requests per second maximum
    
    def _rate_limit(self):
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_interval:
            time.sleep(self.min_interval - time_since_last)
        self.last_request_time = time.time()
```

### API Endpoints
- **Submissions API**: `data.sec.gov/submissions/` (requires 10-digit CIK with leading zeros)
- **XBRL APIs**: Support additional query parameters
- **Direct EDGAR files**: `sec.gov/Archives/edgar/data/` (same header requirements)
- **N-PORT filings**: XML Technical Specification Version 2.0

### Data Processing Requirements
- Handle up to 500,000 investment entries per N-PORT filing
- Process security names, CUSIPs, ISINs, market values, percentage allocations
- Real-time processing capabilities (sub-300ms updates)
- Bulk download support for historical data
- Handle complex nested XML structures

## Common Pitfalls to Avoid
- Using default library User-Agent strings (e.g., "Python-requests")
- Browser-spoofing User-Agent strings
- Failing to implement proper rate limiting
- Incorrect Host headers for data.sec.gov endpoints
- Malformed CIK formatting (must be 10-digit with leading zeros)

## Architecture Notes
- No CORS support - server-side implementation required
- Implement comprehensive error handling for HTTP 404, 403, and rate limit responses
- Use proper exponential backoff retry logic
- Maintain detailed logging for compliance auditing