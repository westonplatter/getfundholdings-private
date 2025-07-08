#!/usr/bin/env python3
"""Explore the full XML structure of N-PORT filing"""

import xml.etree.ElementTree as ET
from collections import defaultdict

def explore_xml_structure(xml_file_path: str):
    """Explore the complete XML structure"""
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
        
        print(f"Root tag: {root.tag}")
        print(f"Root attributes: {root.attrib}")
        print()
        
        # Use proper namespace
        namespaces = {'': 'http://www.sec.gov/edgar/nport'}
        
        # Find all unique tags in the document
        all_tags = set()
        tag_counts = defaultdict(int)
        
        def collect_tags(element):
            all_tags.add(element.tag)
            tag_counts[element.tag] += 1
            for child in element:
                collect_tags(child)
        
        collect_tags(root)
        
        print(f"Total unique tags: {len(all_tags)}")
        print("\nMost common tags:")
        for tag, count in sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:20]:
            simple_tag = tag.split('}')[-1] if '}' in tag else tag
            print(f"  {simple_tag}: {count}")
        
        print("\nLooking for holdings-related tags:")
        holdings_tags = [tag for tag in all_tags if any(keyword in tag.lower() for keyword in ['invst', 'sec', 'holding', 'portfolio'])]
        for tag in holdings_tags:
            simple_tag = tag.split('}')[-1] if '}' in tag else tag
            print(f"  {simple_tag}: {tag_counts[tag]}")
        
        # Try to find the holdings section
        print("\nSearching for holdings sections...")
        
        # Look for invstOrSecs
        invst_sections = root.findall('.//invstOrSecs', namespaces)
        print(f"Found {len(invst_sections)} invstOrSecs sections")
        
        # Look for individual securities
        securities = root.findall('.//invstOrSec', namespaces)
        print(f"Found {len(securities)} individual securities")
        
        if securities:
            print(f"\nFirst security structure:")
            first_sec = securities[0]
            for i, child in enumerate(first_sec):
                simple_tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                print(f"  {simple_tag}: {child.text[:50] if child.text else 'None'}")
                if i >= 15:
                    print("  ...")
                    break
        
        # Look for any other potential holdings containers
        print("\nSearching for other potential holdings containers...")
        form_data = root.find('.//formData', namespaces)
        if form_data is not None:
            print("FormData children:")
            for child in form_data:
                simple_tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                print(f"  {simple_tag}")
                if simple_tag in ['invstOrSecs', 'fundInfo']:
                    print(f"    -> Has {len(list(child))} children")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    xml_file = "/Users/weston/clients/westonplatter/getfundholdings-private/data/nport_1100663_S000004310_0001752724_25_119791.xml"
    explore_xml_structure(xml_file)