#!/usr/bin/env python3
"""
N-PORT XML Parser for SEC EDGAR Filings

This script parses N-PORT XML files and extracts both fund-level information
and individual holdings data into pandas DataFrames.
"""

import xml.etree.ElementTree as ET
import pandas as pd
from typing import Dict, List, Any, Optional
import os
from loguru import logger


class NPortParser:
    """Parser for N-PORT XML filings"""
    
    def __init__(self, xml_file_path: str):
        """Initialize parser with XML file path"""
        self.xml_file_path = xml_file_path
        self.root = None
        self.namespaces = {
            '': 'http://www.sec.gov/edgar/nport',
            'nport': 'http://www.sec.gov/edgar/nport',
            'com': 'http://www.sec.gov/edgar/common',
            'ncom': 'http://www.sec.gov/edgar/nportcommon'
        }
        
    def load_xml(self) -> bool:
        """Load and parse the XML file"""
        try:
            tree = ET.parse(self.xml_file_path)
            self.root = tree.getroot()
            return True
        except Exception as e:
            print(f"Error loading XML file: {e}")
            return False
    
    def get_text_safe(self, element: Optional[ET.Element], default: str = "") -> str:
        """Safely get text from XML element"""
        if element is not None and element.text is not None:
            return element.text.strip()
        return default
    
    def get_fund_info(self) -> Dict[str, Any]:
        """Extract fund-level information"""
        fund_info = {}
        
        if self.root is None:
            return fund_info
        
        # General info
        gen_info = self.root.find('.//genInfo', self.namespaces)
        if gen_info is not None:
            fund_info.update({
                'fund_name': self.get_text_safe(gen_info.find('seriesName', self.namespaces)),
                'reg_name': self.get_text_safe(gen_info.find('regName', self.namespaces)),
                'series_id': self.get_text_safe(gen_info.find('seriesId', self.namespaces)),
                'reg_cik': self.get_text_safe(gen_info.find('regCik', self.namespaces)),
                'reg_lei': self.get_text_safe(gen_info.find('regLei', self.namespaces)),
                'series_lei': self.get_text_safe(gen_info.find('seriesLei', self.namespaces)),
                'report_period_end': self.get_text_safe(gen_info.find('repPdEnd', self.namespaces)),
                'report_period_date': self.get_text_safe(gen_info.find('repPdDate', self.namespaces)),
                'is_final_filing': self.get_text_safe(gen_info.find('isFinalFiling', self.namespaces))
            })
        
        # Fund financial info
        fund_info_elem = self.root.find('.//fundInfo', self.namespaces)
        if fund_info_elem is not None:
            fund_info.update({
                'total_assets': self.get_text_safe(fund_info_elem.find('totAssets', self.namespaces)),
                'total_liabilities': self.get_text_safe(fund_info_elem.find('totLiabs', self.namespaces)),
                'net_assets': self.get_text_safe(fund_info_elem.find('netAssets', self.namespaces))
            })
        
        # Header info
        header_data = self.root.find('.//headerData', self.namespaces)
        if header_data is not None:
            fund_info.update({
                'submission_type': self.get_text_safe(header_data.find('submissionType', self.namespaces)),
                'is_confidential': self.get_text_safe(header_data.find('isConfidential', self.namespaces))
            })
        
        # Series/Class info
        series_class_info = self.root.find('.//seriesClassInfo', self.namespaces)
        if series_class_info is not None:
            fund_info.update({
                'class_id': self.get_text_safe(series_class_info.find('classId', self.namespaces))
            })
        
        return fund_info
    
    def get_holdings_data(self) -> List[Dict[str, Any]]:
        """Extract individual holdings data"""
        holdings = []
        
        if self.root is None:
            return holdings
        
        # Find all investment/security entries with proper namespace
        invst_or_secs = self.root.findall('.//invstOrSec', self.namespaces)
        
        for invst in invst_or_secs:
            holding = {}
            
            # Basic security information
            holding['name'] = self.get_text_safe(invst.find('name', self.namespaces))
            holding['lei'] = self.get_text_safe(invst.find('lei', self.namespaces))
            holding['title'] = self.get_text_safe(invst.find('title', self.namespaces))
            holding['cusip'] = self.get_text_safe(invst.find('cusip', self.namespaces))
            
            # Identifiers
            isin_elem = invst.find('.//isin', self.namespaces)
            if isin_elem is not None:
                holding['isin'] = isin_elem.get('value', '')
            else:
                holding['isin'] = ''
            
            # Other identifier
            other_elem = invst.find('.//other', self.namespaces)
            if other_elem is not None:
                holding['other_id'] = other_elem.get('value', '')
                holding['other_id_desc'] = other_elem.get('otherDesc', '')
            else:
                holding['other_id'] = ''
                holding['other_id_desc'] = ''
            
            # Financial data
            holding['balance'] = self.get_text_safe(invst.find('balance', self.namespaces))
            holding['units'] = self.get_text_safe(invst.find('units', self.namespaces))
            holding['currency'] = self.get_text_safe(invst.find('curCd', self.namespaces))
            holding['value_usd'] = self.get_text_safe(invst.find('valUSD', self.namespaces))
            holding['percent_value'] = self.get_text_safe(invst.find('pctVal', self.namespaces))
            
            # Classification data
            holding['payoff_profile'] = self.get_text_safe(invst.find('payoffProfile', self.namespaces))
            holding['asset_category'] = self.get_text_safe(invst.find('assetCat', self.namespaces))
            holding['issuer_category'] = self.get_text_safe(invst.find('issuerCat', self.namespaces))
            holding['investment_country'] = self.get_text_safe(invst.find('invCountry', self.namespaces))
            holding['is_restricted_security'] = self.get_text_safe(invst.find('isRestrictedSec', self.namespaces))
            holding['fair_value_level'] = self.get_text_safe(invst.find('fairValLevel', self.namespaces))
            
            # Security lending information
            sec_lending = invst.find('securityLending', self.namespaces)
            if sec_lending is not None:
                holding['is_cash_collateral'] = self.get_text_safe(sec_lending.find('isCashCollateral', self.namespaces))
                holding['is_non_cash_collateral'] = self.get_text_safe(sec_lending.find('isNonCashCollateral', self.namespaces))
                
                loan_by_fund = sec_lending.find('loanByFundCondition', self.namespaces)
                if loan_by_fund is not None:
                    holding['is_loan_by_fund'] = loan_by_fund.get('isLoanByFund', '')
                    holding['loan_value'] = loan_by_fund.get('loanVal', '')
                else:
                    holding['is_loan_by_fund'] = ''
                    holding['loan_value'] = ''
            else:
                holding['is_cash_collateral'] = ''
                holding['is_non_cash_collateral'] = ''
                holding['is_loan_by_fund'] = ''
                holding['loan_value'] = ''
            
            holdings.append(holding)
        
        return holdings
    
    def to_dataframes(self) -> tuple[pd.DataFrame, Dict[str, Any]]:
        """Parse XML and return holdings DataFrame and fund info dict"""
        if not self.load_xml():
            return pd.DataFrame(), {}
        
        fund_info = self.get_fund_info()
        holdings_data = self.get_holdings_data()
        
        # Convert holdings to DataFrame
        holdings_df = pd.DataFrame(holdings_data)
        
        # Check for missing CUSIPs and log warnings
        if not holdings_df.empty:
            missing_cusip_mask = (holdings_df['cusip'].isna()) | (holdings_df['cusip'] == '') | (holdings_df['cusip'] == 'N/A')
            missing_count = missing_cusip_mask.sum()
            total_count = len(holdings_df)
            
            if missing_count > 0:
                logger.warning(f"Found {missing_count}/{total_count} holdings ({missing_count/total_count*100:.1f}%) with missing CUSIPs")
                
                # Log details about holdings with missing CUSIPs
                missing_holdings = holdings_df[missing_cusip_mask]
                logger.warning(f"Holdings missing CUSIPs:")
                for idx, row in missing_holdings.iterrows():
                    # Show name, value, and alternative identifiers
                    name = row.get('name', 'Unknown')[:50]  # Truncate long names
                    value = row.get('value_usd', 0)
                    # Convert value to float for formatting, handle potential string values
                    try:
                        value_num = float(value) if value else 0
                        value_str = f"${value_num:,.0f}"
                    except (ValueError, TypeError):
                        value_str = f"${str(value)}"
                    
                    isin = row.get('isin', '')
                    other_id = row.get('other_id', '')
                    logger.warning(f"  - {name} ({value_str}) - ISIN: {isin}, Other: {other_id}")
        
        # Convert numeric columns
        numeric_columns = ['balance', 'value_usd', 'percent_value', 'loan_value']
        for col in numeric_columns:
            if col in holdings_df.columns:
                holdings_df[col] = pd.to_numeric(holdings_df[col], errors='coerce')
        
        # Convert fund info numeric columns
        fund_numeric_columns = ['total_assets', 'total_liabilities', 'net_assets']
        for col in fund_numeric_columns:
            if col in fund_info:
                try:
                    fund_info[col] = float(fund_info[col]) if fund_info[col] else None
                except (ValueError, TypeError):
                    fund_info[col] = None
        
        return holdings_df, fund_info


def parse_nport_file(xml_file_path: str) -> tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Parse an N-PORT XML file and return holdings DataFrame and fund info
    
    Args:
        xml_file_path: Path to the N-PORT XML file
        
    Returns:
        tuple: (holdings_df, fund_info_dict)
    """
    parser = NPortParser(xml_file_path)
    return parser.to_dataframes()


def main():
    """Example usage"""
    xml_file = "/Users/weston/clients/westonplatter/getfundholdings-private/data/nport_1100663_S000004310_0001752724_25_119791.xml"
    
    if not os.path.exists(xml_file):
        print(f"File not found: {xml_file}")
        return
    
    print("Parsing N-PORT XML file...")
    holdings_df, fund_info = parse_nport_file(xml_file)
    
    print(f"\nFund Information:")
    for key, value in fund_info.items():
        print(f"  {key}: {value}")
    
    print(f"\nHoldings DataFrame:")
    print(f"  Total holdings: {len(holdings_df)}")
    print(f"  Columns: {list(holdings_df.columns)}")
    
    if not holdings_df.empty:
        print(f"\nFirst 5 holdings:")
        print(holdings_df[['name', 'cusip', 'value_usd', 'percent_value']].head())
        
        print(f"\nTop 10 holdings by value:")
        top_holdings = holdings_df.nlargest(10, 'value_usd')[['name', 'value_usd', 'percent_value']]
        print(top_holdings)

    import pdb; pdb.set_trace()

    x = 1


if __name__ == "__main__":
    main()