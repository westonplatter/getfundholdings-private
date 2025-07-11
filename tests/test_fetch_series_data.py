"""
Test case for fetch_series_data method with live SEC data.

This test fetches live data from SEC and stores it as a fixture for future testing.
"""

import json
import pytest
from datetime import datetime
from pathlib import Path

from fh.sec_client import SECHTTPClient


class TestFetchSeriesData:
    """Test cases for SEC series data fetching"""
    
    @pytest.fixture
    def sec_client(self):
        """Create SEC client instance"""
        return SECHTTPClient()
    
    @pytest.fixture
    def test_cik(self):
        """Test CIK for iShares Trust"""
        return "1100663"
    
    @pytest.mark.live
    def test_fetch_series_data_live_and_store(self, sec_client, test_cik):
        """
        Test fetch_series_data with live SEC data and store results as fixture.
        
        This test:
        1. Fetches live series data from SEC for iShares Trust (CIK: 1100663)
        2. Validates the data structure
        3. Stores the results as a JSON fixture for future testing
        4. Performs comprehensive assertions on data quality
        """
        # Fetch live data from SEC
        print(f"\nFetching live series data for CIK: {test_cik}")
        series_data = sec_client.fetch_series_data(test_cik)
        
        # Basic assertions
        assert isinstance(series_data, list), "Series data should be a list"
        assert len(series_data) > 0, "Should fetch at least some series data"
        
        print(f"Fetched {len(series_data)} series records")
        
        # Validate data structure
        valid_series_count = 0
        series_with_classes = 0
        series_with_tickers = 0
        
        for series in series_data:
            # Every series should have these basic fields
            assert isinstance(series, dict), "Each series should be a dictionary"
            assert "cik" in series, "Each series should have a CIK"
            assert series["cik"] == f"{int(test_cik):010d}", "CIK should be properly formatted"
            
            # Count valid series (those with series_id starting with 'S')
            if "series_id" in series and series["series_id"].startswith("S"):
                valid_series_count += 1
                
                # Validate series ID format
                assert len(series["series_id"]) >= 6, f"Series ID should be at least 6 chars: {series['series_id']}"
                
                # Check for classes
                if "classes" in series and series["classes"]:
                    series_with_classes += 1
                    
                    # Validate class structure
                    for class_info in series["classes"]:
                        assert isinstance(class_info, dict), "Each class should be a dictionary"
                        
                        # Check for expected class fields
                        if "class_id" in class_info and class_info["class_id"]:
                            assert class_info["class_id"].startswith("C"), f"Class ID should start with 'C': {class_info['class_id']}"
                        
                        if "ticker" in class_info:
                            assert len(class_info["ticker"]) <= 10, f"Ticker should be short: {class_info['ticker']}"
                            series_with_tickers += 1
                
                # Check for promoted ticker (single-class series)
                if "ticker" in series:
                    assert len(series["ticker"]) <= 10, f"Series ticker should be short: {series['ticker']}"
        
        # Data quality assertions
        assert valid_series_count > 0, "Should have at least some valid series with IDs"
        assert series_with_classes > 0, "Should have at least some series with class information"
        
        print(f"Data quality summary:")
        print(f"  Total records: {len(series_data)}")
        print(f"  Valid series: {valid_series_count}")
        print(f"  Series with classes: {series_with_classes}")
        print(f"  Series with tickers: {series_with_tickers}")
        
        # Look for specific known series (IVV)
        ivv_found = False
        for series in series_data:
            if series.get("series_id") == "S000004310":
                ivv_found = True
                assert "classes" in series, "IVV series should have classes"
                assert len(series["classes"]) >= 1, "IVV should have at least one class"
                
                # Check if IVV ticker is present
                has_ticker = False
                if "ticker" in series and series["ticker"] == "IVV":
                    has_ticker = True
                for class_info in series.get("classes", []):
                    if class_info.get("ticker") == "IVV":
                        has_ticker = True
                
                assert has_ticker, "IVV series should have IVV ticker somewhere"
                print(f"  ✓ Found IVV series: {series['series_id']}")
                break
        
        assert ivv_found, "Should find the known IVV series (S000004310)"
        
        # Store results as fixture
        fixture_dir = Path(__file__).parent / "fixtures"
        fixture_dir.mkdir(exist_ok=True)
        
        fixture_data = {
            "test_metadata": {
                "cik": test_cik,
                "fetch_timestamp": datetime.now().isoformat(),
                "total_series": len(series_data),
                "valid_series_count": valid_series_count,
                "series_with_classes": series_with_classes,
                "series_with_tickers": series_with_tickers,
                "test_description": "Live fetch of iShares Trust series data for testing"
            },
            "series_data": series_data
        }
        
        fixture_file = fixture_dir / f"series_data_{test_cik}_live.json"
        with open(fixture_file, 'w') as f:
            json.dump(fixture_data, f, indent=2, default=str)
        
        print(f"✓ Stored test fixture: {fixture_file}")
        print(f"✓ Test completed successfully")
        
        # Test functions should not return values
    
    @pytest.mark.live
    def test_series_data_structure_validation(self, sec_client, test_cik):
        """
        Test that validates the expected structure of series data.
        This can run against either live data or stored fixtures.
        """
        # Try to load from fixture first, fall back to live data
        fixture_file = Path(__file__).parent / "fixtures" / f"series_data_{test_cik}_live.json"
        
        if fixture_file.exists():
            print(f"Loading data from fixture: {fixture_file}")
            with open(fixture_file, 'r') as f:
                fixture_data = json.load(f)
                series_data = fixture_data["series_data"]
        else:
            print(f"Fixture not found, fetching live data")
            series_data = sec_client.fetch_series_data(test_cik)
        
        # Structural validation
        assert isinstance(series_data, list)
        
        # Check that we have a reasonable amount of data for iShares
        assert len(series_data) > 100, f"iShares should have many series, got {len(series_data)}"
        
        # Validate at least one complete series record
        complete_series = None
        for series in series_data:
            if (series.get("series_id", "").startswith("S") and 
                "classes" in series and 
                len(series["classes"]) > 0):
                complete_series = series
                break
        
        assert complete_series is not None, "Should find at least one complete series record"
        
        # Validate the complete series structure
        required_fields = ["cik", "series_id", "classes"]
        for field in required_fields:
            assert field in complete_series, f"Complete series should have {field}"
        
        # Validate class structure
        first_class = complete_series["classes"][0]
        class_fields = ["class_id", "class_name", "ticker", "raw_data"]
        
        # At least some of these fields should be present
        present_fields = [field for field in class_fields if field in first_class]
        assert len(present_fields) >= 2, f"Class should have at least 2 fields, has: {present_fields}"
        
        print(f"✓ Structure validation passed for {len(series_data)} series records")


if __name__ == "__main__":
    """Run tests directly"""
    client = SECHTTPClient()
    test_cik = "1100663"
    
    print("Running live SEC series data test...")
    
    test_instance = TestFetchSeriesData()
    
    # Run the live data test
    series_data = test_instance.test_fetch_series_data_live_and_store(client, test_cik)
    
    # Run structure validation
    test_instance.test_series_data_structure_validation(client, test_cik)
    
    print("\n🎉 All tests passed!")