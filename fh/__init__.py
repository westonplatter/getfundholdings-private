"""
FH - Fund Holdings data fetcher for SEC EDGAR
"""

from .sec_client import SECHTTPClient

__version__ = "0.1.0"
__all__ = ["SECHTTPClient"]