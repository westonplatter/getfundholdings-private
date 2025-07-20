#!/usr/bin/env python3
"""
Constants for the fund holdings data pipeline.

This module contains shared constants used across the pipeline, including
CIK mappings for major fund companies.
"""

# Central Index Key (CIK) mappings for major fund companies
# Format: "Company Name" -> "10-digit CIK with leading zeros"
CIK_MAP = {
    # Vanguard
    "The Vanguard Group, Inc.": "0000102909",
    "Vanguard Advisers, Inc.": "0000862084",
    "Vanguard Marketing Corporation": "0000862110",
    
    # SPDR / State Street
    "State Street Corporation": "0000093751",
    "State Street Global Advisors Trust Company": "0000934647",
    "SPDR S&P 500 ETF Trust": "0000884394",
    "State Street Global Advisors Funds Management": "0001064641",
    
    # iShares
    "iShares Trust": "1100663",
    "iShares, Inc.": "0000930667",

    # BlackRock
    "BlackRock, Inc.": "0001364742",
    "BlackRock Fund Advisors": "0001006249",
    "BlackRock Advisors LLC": "0001006250",
    "blackrock": "0001761055",
    "iShares Bitcoin Trust ETF": "0001980994",
    
    # Invesco
    "Invesco Ltd.": "0000914208",
    "Invesco Advisers, Inc.": "0000914648",
    "Invesco Capital Management LLC": "0000914649",
    "Invesco QQQ Trust, Series 1": "0001067839",
    
    # Schwab
    "Charles Schwab Corporation": "0000316709",
    "Charles Schwab Investment Management": "0001064642",
    "Schwab Strategic Trust": "0001064646",
    
    # Dimensional
    "Dimensional Fund Advisors LP": "0000874761",
    "DFA Investment Dimensions Group Inc.": "0000874762",
    
    # JPMorgan
    "JPMorgan Chase & Co.": "0000019617",
    "J.P. Morgan Investment Management Inc.": "0000895421",
    "jpmorgan": "0001485894",
    
    # VanEck
    "VanEck Associates Corporation": "0000912471",
    "Market Vectors ETF Trust": "0001345413",
    
    # ProShares
    "ProShare Advisors LLC": "0001174610",
    "ProShares Trust": "0001174612",
    "ProShares UltraPro QQQ": "0001174610",  # TQQQ
    
    # Fidelity
    "FMR LLC": "0000315066",
    "Fidelity Management & Research Company": "0000315067",
    
    # Grayscale
    "Grayscale Investments, LLC": "0001588489",
    "Grayscale Bitcoin Trust": "0001588489",
    
    # Janus Henderson
    "Janus Henderson Group plc": "0001691415",
    "Janus Capital Management LLC": "0000886982",
    
    # Simplify
    "Simplify Asset Management Inc.": "0001810747",
    
    # Defiance ETFs
    "Defiance ETFs": "0001771146",  # Defiance Daily Target 2X Long MSTR ETF (MSTX)
    
    # REX Shares
    "REX Shares": "0001771146",  # T-REX 2X Long MSTR Daily Target ETF (MSTU)
    
    # Exchange Traded Concepts
    "Exchange Traded Concepts, LLC": "0001452937",  # Bitwise Crypto Industry Innovators ETF (BITQ)
    
    # ARK ETF Trust
    "ARK ETF Trust": "0001579982",  # ARK Innovation ETF (ARKK), ARK Space Exploration & Innovation ETF (ARKX)
    
    # Tuttle Capital Management
    "Collaborative Investment Series Trust": "0001719812",  # Tuttle Capital Short Innovation ETF (SARK)
}