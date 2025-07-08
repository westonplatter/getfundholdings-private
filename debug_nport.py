#!/usr/bin/env python3
"""Debug script for N-PORT XML parsing"""

import xml.etree.ElementTree as ET

def debug_xml_structure(xml_file_path: str):
    """Debug the XML structure"""
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
        
        print(f"Root tag: {root.tag}")
        print(f"Root attributes: {root.attrib}")
        print()
        
        # Print first few child elements
        print("First level children:")
        for i, child in enumerate(root):
            print(f"  {i}: {child.tag}")
            if i >= 5:
                break
        print()
        
        # Look for formData
        form_data = root.find('.//formData')
        if form_data is not None:
            print("Found formData, children:")
            for i, child in enumerate(form_data):
                print(f"  {i}: {child.tag}")
                if i >= 10:
                    break
        print()
        
        # Look for invstOrSecs with different approaches
        print("Testing different ways to find invstOrSecs:")
        
        # Method 1: Simple find
        invst_or_secs_1 = root.find('.//invstOrSecs')
        print(f"Method 1 (.//invstOrSecs): {invst_or_secs_1}")
        
        # Method 2: Find all
        invst_or_secs_2 = root.findall('.//invstOrSecs')
        print(f"Method 2 (findall .//invstOrSecs): {len(invst_or_secs_2)} found")
        
        # Method 3: Find individual securities
        invst_or_sec_3 = root.findall('.//invstOrSec')
        print(f"Method 3 (findall .//invstOrSec): {len(invst_or_sec_3)} found")
        
        if invst_or_sec_3:
            print(f"First invstOrSec children:")
            first_security = invst_or_sec_3[0]
            for i, child in enumerate(first_security):
                print(f"  {i}: {child.tag} = {child.text}")
                if i >= 10:
                    break
        
        # Method 4: Try with explicit namespace
        namespaces = {'': 'http://www.sec.gov/edgar/nport'}
        try:
            ns_invst = root.findall('.//invstOrSec', namespaces)
            print(f"Method 4 (with namespace): {len(ns_invst)} found")
        except Exception as e:
            print(f"Method 4 error: {e}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    xml_file = "/Users/weston/clients/westonplatter/getfundholdings-private/data/nport_1100663_S000004310_0001752724_25_119791.xml"
    debug_xml_structure(xml_file)