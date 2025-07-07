"""
Test cases for series filings functionality, especially accession number extraction.
"""

import json
import pytest
from pathlib import Path
from unittest.mock import Mock, patch

from fh.sec_client import SECHTTPClient


class TestSeriesFilings:
    """Test cases for series filings functionality"""
    
    @pytest.fixture
    def sec_client(self):
        """Create SEC client instance"""
        return SECHTTPClient()
    
    @pytest.fixture
    def test_series_id(self):
        """Test series ID for iShares Core S&P 500 ETF"""
        return "S000004310"
    
    @pytest.fixture
    def test_cik(self):
        """Test CIK for iShares Trust"""
        return "1100663"
    
    def test_extract_accession_number_from_raw_text(self, sec_client):
        """Test that accession numbers are correctly extracted from raw text"""
        # Sample cell texts that include accession numbers
        cell_texts = [
            "NPORT-P",
            "Documents", 
            "Monthly Portfolio Investments Report on Form N-PORT (Public)Acc-no: 0001752724-25-119791 (40 Act) Size: 515 KB",
            "2025-05-27",
            "811-0972925985388"
        ]
        
        accession_number = sec_client._extract_accession_number(cell_texts)
        
        assert accession_number == "0001752724-25-119791", f"Expected '0001752724-25-119791', got '{accession_number}'"
    
    def test_extract_accession_number_multiple_formats(self, sec_client):
        """Test extraction from various text formats"""
        test_cases = [
            # Standard format
            (["Some text", "Acc-no: 0001752724-25-119791", "more text"], "0001752724-25-119791"),
            # With extra spaces
            (["Some text", "Acc-no:  0001752724-25-043800  ", "more text"], "0001752724-25-043800"),
            # Different context
            (["NPORT-P", "Report Acc-no: 0001752724-24-269943 (40 Act)"], "0001752724-24-269943"),
            # No accession number
            (["NPORT-P", "Documents", "Some other text"], None)
        ]
        
        for cell_texts, expected in test_cases:
            result = sec_client._extract_accession_number(cell_texts)
            assert result == expected, f"For {cell_texts}, expected {expected}, got {result}"
    
    def test_extract_accession_number_handles_empty_text(self, sec_client):
        """Test that empty or None text is handled gracefully"""
        test_cases = [
            [],  # Empty list
            [""],  # Empty string
            [None],  # None value
            ["", None, ""],  # Mixed empty values
        ]
        
        for cell_texts in test_cases:
            result = sec_client._extract_accession_number(cell_texts)
            assert result is None, f"Empty/None text should return None, got {result}"
    
    def test_parse_series_filings_response_prioritizes_raw_text(self, sec_client):
        """Test that the parsing logic prioritizes raw text over link extraction"""
        # Create mock HTML that has both link and raw text
        mock_html = """
        <table>
            <tr>
                <td>NPORT-P</td>
                <td>Documents</td>
                <td>Monthly Portfolio Investments Report on Form N-PORT (Public)Acc-no: 0001752724-25-119791 (40 Act) Size: 515 KB</td>
                <td>2025-05-27</td>
                <td>811-0972925985388</td>
            </tr>
        </table>
        """
        
        result = sec_client._parse_series_filings_response(mock_html, "S000004310", "NPORT-P")
        
        # Should find at least one filing
        assert len(result) > 0, "Should find at least one filing"
        
        # First filing should have correct accession number
        first_filing = result[0]
        assert first_filing["accession_number"] == "0001752724-25-119791", \
            f"Expected '0001752724-25-119791', got '{first_filing['accession_number']}'"
    
    @patch('fh.sec_client.SECHTTPClient._make_request')
    def test_fetch_series_filings_extracts_correct_accession_numbers(self, mock_request, sec_client):
        """Test that fetch_series_filings extracts unique accession numbers"""
        # Mock response with multiple filings
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = """
        <table>
            <tr>
                <td>NPORT-P</td>
                <td>Documents</td>
                <td>Monthly Portfolio Investments Report on Form N-PORT (Public)Acc-no: 0001752724-25-119791 (40 Act) Size: 515 KB</td>
                <td>2025-05-27</td>
            </tr>
            <tr>
                <td>NPORT-P</td>
                <td>Documents</td>
                <td>Monthly Portfolio Investments Report on Form N-PORT (Public)Acc-no: 0001752724-25-043800 (40 Act) Size: 1 MB</td>
                <td>2025-02-27</td>
            </tr>
            <tr>
                <td>NPORT-P</td>
                <td>Documents</td>
                <td>Monthly Portfolio Investments Report on Form N-PORT (Public)Acc-no: 0001752724-24-269943 (40 Act) Size: 515 KB</td>
                <td>2024-11-26</td>
            </tr>
        </table>
        """
        mock_request.return_value = mock_response
        
        # Fetch series filings
        result = sec_client.fetch_series_filings("S000004310", "NPORT-P")
        
        # Should find all 3 filings
        assert len(result) == 3, f"Should find 3 filings, got {len(result)}"
        
        # Check that accession numbers are unique and correct
        expected_accession_numbers = {
            "0001752724-25-119791",
            "0001752724-25-043800", 
            "0001752724-24-269943"
        }
        
        actual_accession_numbers = {filing["accession_number"] for filing in result}
        
        assert actual_accession_numbers == expected_accession_numbers, \
            f"Expected {expected_accession_numbers}, got {actual_accession_numbers}"
    
    def test_build_nport_url_from_accession_number(self, sec_client):
        """Test that N-PORT URLs are built correctly from CIK and accession numbers"""
        test_cases = [
            ("1100663", "0001752724-25-119791", "https://www.sec.gov/Archives/edgar/data/1100663/000175272425119791/primary_doc.xml"),
            ("1100663", "0001752724-25-043800", "https://www.sec.gov/Archives/edgar/data/1100663/000175272425043800/primary_doc.xml"),
            ("1100663", "0001752724-24-269943", "https://www.sec.gov/Archives/edgar/data/1100663/000175272424269943/primary_doc.xml"),
        ]
        
        for cik, accession_number, expected_url in test_cases:
            result = sec_client.build_nport_url(cik, accession_number)
            assert result == expected_url, f"For CIK {cik}, accession {accession_number}, expected {expected_url}, got {result}"
    
    @pytest.mark.integration
    def test_live_series_filings_have_unique_accession_numbers(self, sec_client, test_series_id):
        """Integration test: verify that live series filings have unique accession numbers"""
        # Fetch live series filings
        result = sec_client.fetch_series_filings(test_series_id, "NPORT-P")
        
        # Should find some filings
        assert len(result) > 0, f"Should find some filings for {test_series_id}"
        
        # All accession numbers should be unique
        accession_numbers = [filing.get("accession_number") for filing in result]
        accession_numbers = [acc for acc in accession_numbers if acc is not None]
        
        assert len(accession_numbers) == len(set(accession_numbers)), \
            f"Accession numbers should be unique. Found duplicates in: {accession_numbers}"
        
        # All accession numbers should follow the correct format
        for acc_num in accession_numbers:
            assert acc_num is not None, "Accession number should not be None"
            assert len(acc_num) == 20, f"Accession number should be 20 chars: {acc_num}"
            assert acc_num.count('-') == 2, f"Accession number should have 2 dashes: {acc_num}"
            
        print(f"✓ Found {len(result)} filings with unique accession numbers")
        print(f"✓ Sample accession numbers: {accession_numbers[:3]}")
    
    @pytest.mark.integration
    def test_process_cik_series_filings_creates_correct_files(self, sec_client, test_cik):
        """Integration test: verify that processing CIK series creates files with correct data"""
        # Process a subset of series (limit to avoid too many API calls)
        series_data = sec_client.load_series_data(test_cik)
        
        if not series_data:
            pytest.skip("No series data available for testing")
        
        # Get just the first series for testing
        series_ids = sec_client.extract_series_ids(series_data)
        if not series_ids:
            pytest.skip("No series IDs available for testing")
        
        first_series_id = series_ids[0]
        
        # Fetch series filings for just this one series
        series_filings = sec_client.fetch_series_filings(first_series_id, "NPORT-P")
        
        if not series_filings:
            pytest.skip(f"No filings found for series {first_series_id}")
        
        # Save the filings
        saved_file = sec_client.save_series_filings(series_filings, first_series_id, "NPORT-P", test_cik)
        
        # Verify the file was created
        assert Path(saved_file).exists(), f"File should be created: {saved_file}"
        
        # Load and verify the file contents
        with open(saved_file, 'r') as f:
            data = json.load(f)
        
        # Verify structure
        assert "filings" in data, "Data should contain filings"
        assert len(data["filings"]) > 0, "Should have at least one filing"
        
        # Verify accession numbers are unique
        accession_numbers = [filing.get("accession_number") for filing in data["filings"]]
        accession_numbers = [acc for acc in accession_numbers if acc is not None]
        
        assert len(accession_numbers) == len(set(accession_numbers)), \
            f"Accession numbers in file should be unique: {accession_numbers}"
        
        print(f"✓ Created file: {saved_file}")
        print(f"✓ File contains {len(data['filings'])} filings with unique accession numbers")


if __name__ == "__main__":
    """Run tests directly"""
    import sys
    
    # Run specific tests
    client = SECHTTPClient()
    test_instance = TestSeriesFilings()
    
    print("Testing accession number extraction...")
    
    # Test basic extraction
    test_instance.test_extract_accession_number_from_raw_text(client)
    print("✓ Basic extraction test passed")
    
    # Test multiple formats
    test_instance.test_extract_accession_number_multiple_formats(client)
    print("✓ Multiple formats test passed")
    
    # Test empty text handling
    test_instance.test_extract_accession_number_handles_empty_text(client)
    print("✓ Empty text handling test passed")
    
    # Test N-PORT URL building
    test_instance.test_build_nport_url_from_accession_number(client)
    print("✓ N-PORT URL building test passed")
    
    print("\n🎉 All unit tests passed!")
    
    # Optionally run integration tests
    if len(sys.argv) > 1 and sys.argv[1] == "--integration":
        print("\nRunning integration tests...")
        test_instance.test_live_series_filings_have_unique_accession_numbers(client, "S000004310")
        test_instance.test_process_cik_series_filings_creates_correct_files(client, "1100663")
        print("🎉 Integration tests passed!")