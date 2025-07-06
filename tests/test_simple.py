"""
Simple tests using saved test data.
"""

import json
from pathlib import Path

from fh.sec_client import SECHTTPClient

def load_test_data():
    """Load test data from saved file"""
    test_file = Path(__file__).parent / "fixtures" / "test_series_data_1100663.json"
    
    if not test_file.exists():
        raise FileNotFoundError(f"Test data not found: {test_file}. Run fetch_test_data.py first.")
    
    with open(test_file, 'r') as f:
        return json.load(f)

def test_series_data_exists():
    """Test that we can load series data"""
    data = load_test_data()
    
    assert "series_data" in data
    assert "metadata" in data
    assert len(data["series_data"]) > 0
    
    print(f"✓ Loaded {len(data['series_data'])} series records")

def test_ivv_series_exists():
    """Test that IVV series (S000004310) exists in the data"""
    data = load_test_data()
    series_data = data["series_data"]
    
    # Find IVV series
    ivv_series = None
    for series in series_data:
        if series.get("series_id") == "S000004310":
            ivv_series = series
            break
    
    assert ivv_series is not None, "Should find IVV series S000004310"
    assert "classes" in ivv_series, "IVV should have classes"
    assert len(ivv_series["classes"]) > 0, "IVV should have at least one class"
    
    # Check for IVV ticker
    found_ivv_ticker = False
    for class_info in ivv_series["classes"]:
        if class_info.get("ticker") == "IVV":
            found_ivv_ticker = True
            break
    
    assert found_ivv_ticker, "Should find IVV ticker in classes"
    print("✓ Found IVV series with correct ticker")

def test_data_structure():
    """Test basic data structure"""
    data = load_test_data()
    series_data = data["series_data"]
    
    # Check that we have a reasonable number of series
    assert len(series_data) > 100, f"Should have many series, got {len(series_data)}"
    
    # Check that most series have the expected structure
    valid_series = 0
    for series in series_data:
        if series.get("series_id", "").startswith("S"):
            valid_series += 1
            assert "cik" in series
            assert "classes" in series
    
    assert valid_series > 100, f"Should have many valid series, got {valid_series}"
    print(f"✓ Found {valid_series} valid series records")

if __name__ == "__main__":
    """Run tests directly"""
    print("Running simple tests...")
    
    try:
        test_series_data_exists()
        test_ivv_series_exists()
        test_data_structure()
        
        print("\n🎉 All tests passed!")
        
    except FileNotFoundError as e:
        print(f"\n❌ {e}")
        print("Run: uv run python fetch_test_data.py")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        raise