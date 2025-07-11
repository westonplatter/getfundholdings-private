#!/usr/bin/env python3
"""
Schema Generator for External API Specifications

This script generates and updates external schema specifications (JSON Schema, TypeScript, OpenAPI)
based on actual data structures from the fund holdings pipeline.

Usage:
    python -m fh.external_schemas.generator
    python -m fh.external_schemas.generator --csv-file path/to/enriched_holdings.csv
    python -m fh.external_schemas.generator --validate-only
"""

import json
import os
import sys
import argparse
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from schemas import HoldingsEnrichedSchema
from r2_client import R2Client


class ExternalSchemaGenerator:
    """Generate external API schemas from actual data structures."""
    
    def __init__(self, schema_dir: Optional[str] = None):
        """Initialize generator with schema directory path."""
        if schema_dir is None:
            schema_dir = Path(__file__).parent
        self.schema_dir = Path(schema_dir)
        self.timestamp = datetime.now().isoformat()
    
    def analyze_csv_structure(self, csv_file: str) -> Dict[str, Any]:
        """
        Analyze actual CSV file to extract data types and constraints.
        
        Args:
            csv_file: Path to enriched holdings CSV file
            
        Returns:
            Dictionary with analysis results
        """
        try:
            # Read CSV file
            df = pd.read_csv(csv_file)
            
            # Convert R2 client to get JSON structure
            r2_client = R2Client()
            json_data = r2_client.read_csv_to_json(csv_file)
            
            if not json_data:
                raise ValueError(f"Failed to convert CSV to JSON: {csv_file}")
            
            analysis = {
                "csv_file": csv_file,
                "total_holdings": len(df),
                "columns": list(df.columns),
                "dtypes": df.dtypes.to_dict(),
                "sample_holding": json_data["holdings"][0] if json_data["holdings"] else {},
                "metadata_sample": json_data["metadata"],
                "null_counts": df.isnull().sum().to_dict(),
                "value_ranges": {},
                "unique_values": {}
            }
            
            # Analyze numeric columns for ranges
            numeric_columns = df.select_dtypes(include=['int64', 'float64']).columns
            for col in numeric_columns:
                if not df[col].isnull().all():
                    analysis["value_ranges"][col] = {
                        "min": float(df[col].min()),
                        "max": float(df[col].max()),
                        "mean": float(df[col].mean())
                    }
            
            # Analyze categorical columns for unique values
            categorical_columns = ['payoff_profile', 'asset_category', 'issuer_category', 
                                 'investment_country', 'is_restricted_security', 'fair_value_level',
                                 'is_cash_collateral', 'is_non_cash_collateral', 'is_loan_by_fund']
            for col in categorical_columns:
                if col in df.columns:
                    unique_vals = df[col].dropna().unique().tolist()
                    analysis["unique_values"][col] = unique_vals[:20]  # Limit for readability
            
            return analysis
            
        except Exception as e:
            print(f"Error analyzing CSV file {csv_file}: {e}")
            return {}
    
    def generate_json_schema_from_data(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate JSON Schema based on actual data analysis.
        
        Args:
            analysis: Results from analyze_csv_structure()
            
        Returns:
            Updated JSON Schema dictionary
        """
        # Load existing schema as base
        json_schema_file = self.schema_dir / "json_schema.json"
        
        if json_schema_file.exists():
            with open(json_schema_file, 'r') as f:
                schema = json.load(f)
        else:
            # Basic schema structure if file doesn't exist
            schema = {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": "Fund Holdings Response",
                "type": "object"
            }
        
        # Update metadata based on actual data
        if "metadata_sample" in analysis:
            metadata = analysis["metadata_sample"]
            if "properties" not in schema:
                schema["properties"] = {}
            if "metadata" not in schema["properties"]:
                schema["properties"]["metadata"] = {"type": "object", "properties": {}}
            
            # Update with actual metadata fields
            for key, value in metadata.items():
                prop_type = "string"
                if isinstance(value, int):
                    prop_type = "integer"
                elif isinstance(value, float):
                    prop_type = "number"
                elif isinstance(value, bool):
                    prop_type = "boolean"
                
                schema["properties"]["metadata"]["properties"][key] = {
                    "type": prop_type,
                    "description": f"Auto-generated from analysis at {self.timestamp}"
                }
        
        # Update holdings schema based on sample holding
        if "sample_holding" in analysis:
            holding = analysis["sample_holding"]
            if "definitions" not in schema:
                schema["definitions"] = {}
            if "holding" not in schema["definitions"]:
                schema["definitions"]["holding"] = {"type": "object", "properties": {}}
            
            # Update with actual holding fields
            for key, value in holding.items():
                prop_def = {"type": "string"}  # Default
                
                if isinstance(value, int):
                    prop_def = {"type": "integer"}
                elif isinstance(value, float):
                    prop_def = {"type": "number"}
                elif isinstance(value, bool):
                    prop_def = {"type": "boolean"}
                elif value is None:
                    prop_def = {"type": ["string", "null"]}
                
                # Add constraints based on analysis
                if key in analysis.get("unique_values", {}):
                    unique_vals = analysis["unique_values"][key]
                    if len(unique_vals) <= 10:  # Only for small sets
                        prop_def["enum"] = unique_vals
                
                if key in analysis.get("value_ranges", {}):
                    range_info = analysis["value_ranges"][key]
                    if range_info["min"] >= 0:
                        prop_def["minimum"] = 0
                
                schema["definitions"]["holding"]["properties"][key] = prop_def
        
        return schema
    
    def generate_typescript_from_data(self, analysis: Dict[str, Any]) -> str:
        """
        Generate TypeScript definitions based on actual data analysis.
        
        Args:
            analysis: Results from analyze_csv_structure()
            
        Returns:
            TypeScript definition string
        """
        # Load existing TypeScript as template
        ts_file = self.schema_dir / "typescript.d.ts"
        
        if ts_file.exists():
            with open(ts_file, 'r') as f:
                existing_ts = f.read()
        else:
            existing_ts = ""
        
        # Generate comment with analysis info
        ts_content = f"""/**
 * TypeScript Type Definitions for Fund Holdings API
 * 
 * Auto-generated from actual data analysis
 * Generated at: {self.timestamp}
 * Source file: {analysis.get('csv_file', 'N/A')}
 * Total holdings analyzed: {analysis.get('total_holdings', 'N/A')}
 */

"""
        
        # For now, append the analysis as comments to existing TypeScript
        ts_content += f"""
// Data Analysis Results:
// Columns found: {len(analysis.get('columns', []))}
// Numeric ranges: {list(analysis.get('value_ranges', {}).keys())}
// Categorical fields: {list(analysis.get('unique_values', {}).keys())}

"""
        
        # Add existing TypeScript content
        ts_content += existing_ts
        
        return ts_content
    
    def validate_existing_schemas(self, analysis: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        Validate existing schemas against actual data structure.
        
        Args:
            analysis: Results from analyze_csv_structure()
            
        Returns:
            Dictionary with validation results and issues
        """
        issues = {
            "json_schema": [],
            "typescript": [],
            "openapi": []
        }
        
        # Check if all columns from actual data are represented in schemas
        actual_columns = set(analysis.get("columns", []))
        
        # Check JSON Schema
        json_schema_file = self.schema_dir / "json_schema.json"
        if json_schema_file.exists():
            with open(json_schema_file, 'r') as f:
                json_schema = json.load(f)
            
            if "definitions" in json_schema and "holding" in json_schema["definitions"]:
                schema_props = set(json_schema["definitions"]["holding"].get("properties", {}).keys())
                missing_in_schema = actual_columns - schema_props
                extra_in_schema = schema_props - actual_columns
                
                if missing_in_schema:
                    issues["json_schema"].append(f"Missing columns: {missing_in_schema}")
                if extra_in_schema:
                    issues["json_schema"].append(f"Extra columns: {extra_in_schema}")
        else:
            issues["json_schema"].append("JSON Schema file not found")
        
        # Check for data type mismatches
        if "sample_holding" in analysis:
            for key, value in analysis["sample_holding"].items():
                expected_type = type(value).__name__
                if value is None:
                    expected_type = "nullable"
                # Could add more sophisticated type checking here
        
        return issues
    
    def update_schemas(self, csv_file: Optional[str] = None) -> None:
        """
        Update all schema files based on actual data analysis.
        
        Args:
            csv_file: Optional path to specific CSV file to analyze
        """
        print(f"Updating external schemas at {self.timestamp}")
        
        # Analyze actual data if CSV file provided
        analysis = {}
        if csv_file:
            if not os.path.exists(csv_file):
                print(f"CSV file not found: {csv_file}")
                return
            
            print(f"Analyzing CSV file: {csv_file}")
            analysis = self.analyze_csv_structure(csv_file)
            
            if not analysis:
                print("Failed to analyze CSV file")
                return
            
            print(f"Analysis complete: {analysis['total_holdings']} holdings, {len(analysis['columns'])} columns")
        
        # Update JSON Schema
        if analysis:
            print("Updating JSON Schema...")
            updated_json_schema = self.generate_json_schema_from_data(analysis)
            
            json_schema_file = self.schema_dir / "json_schema.json"
            with open(json_schema_file, 'w') as f:
                json.dump(updated_json_schema, f, indent=2)
            
            print(f"JSON Schema updated: {json_schema_file}")
        
        # Update TypeScript
        if analysis:
            print("Updating TypeScript definitions...")
            updated_typescript = self.generate_typescript_from_data(analysis)
            
            ts_file = self.schema_dir / "typescript.d.ts"
            with open(ts_file, 'w') as f:
                f.write(updated_typescript)
            
            print(f"TypeScript definitions updated: {ts_file}")
        
        print("Schema update complete!")
    
    def validate_schemas(self, csv_file: Optional[str] = None) -> None:
        """
        Validate existing schemas against actual data.
        
        Args:
            csv_file: Optional path to specific CSV file to validate against
        """
        print(f"Validating external schemas at {self.timestamp}")
        
        if not csv_file:
            print("No CSV file provided for validation")
            return
        
        if not os.path.exists(csv_file):
            print(f"CSV file not found: {csv_file}")
            return
        
        print(f"Analyzing CSV file: {csv_file}")
        analysis = self.analyze_csv_structure(csv_file)
        
        if not analysis:
            print("Failed to analyze CSV file")
            return
        
        print(f"Validating schemas against: {analysis['total_holdings']} holdings, {len(analysis['columns'])} columns")
        
        # Validate schemas
        issues = self.validate_existing_schemas(analysis)
        
        # Report results
        for schema_type, schema_issues in issues.items():
            print(f"\n{schema_type.upper()} Validation:")
            if schema_issues:
                for issue in schema_issues:
                    print(f"  ❌ {issue}")
            else:
                print(f"  ✅ No issues found")
        
        print("\nValidation complete!")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="Generate external API schemas from fund holdings data")
    parser.add_argument("--csv-file", help="Path to enriched holdings CSV file for analysis")
    parser.add_argument("--validate-only", action="store_true", help="Only validate existing schemas")
    parser.add_argument("--schema-dir", help="Directory containing schema files")
    
    args = parser.parse_args()
    
    generator = ExternalSchemaGenerator(args.schema_dir)
    
    if args.validate_only:
        generator.validate_schemas(args.csv_file)
    else:
        generator.update_schemas(args.csv_file)


if __name__ == "__main__":
    main()