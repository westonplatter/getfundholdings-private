#!/usr/bin/env python3
"""
JSON Schema Generator for External API Specifications

This script generates JSON Schema specifications from Pandera schemas 
defined in fh/internal_schemas/. The internal schemas are the source of truth.

Usage:
    python -m fh.external_schemas.generator
    python -m fh.external_schemas.generator --output-file custom_schema.json
"""

import json
import os
import sys
import argparse
import importlib
import inspect
from datetime import datetime
from typing import Dict, Any, List, Optional, Type
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

import pandera.pandas as pa
from pandera.typing import DataFrame, Series


class JSONSchemaGenerator:
    """Generate JSON Schema from Pandera DataFrameModel schemas."""
    
    def __init__(self, output_dir: Optional[str] = None):
        """Initialize generator with output directory path."""
        if output_dir is None:
            output_dir = Path(__file__).parent
        self.output_dir = Path(output_dir)
        self.internal_schemas_dir = Path(__file__).parent.parent / "internal_schemas"
        self.timestamp = datetime.now().isoformat()
    
    def discover_schema_files(self) -> List[Path]:
        """
        Discover all Python files in internal_schemas directory that contain schemas.
        
        Returns:
            List of paths to schema files
        """
        schema_files = []
        
        for file_path in self.internal_schemas_dir.glob("*.py"):
            # Skip __init__.py
            if file_path.name == "__init__.py":
                continue
            
            # Only include files that end with "_schema.py"
            if file_path.name.endswith("_schema.py"):
                schema_files.append(file_path)
        
        return schema_files
    
    def import_schemas_from_file(self, file_path: Path) -> List[Type[pa.DataFrameModel]]:
        """
        Import all Pandera DataFrameModel schemas from a Python file.
        
        Args:
            file_path: Path to Python file containing schemas
            
        Returns:
            List of Pandera DataFrameModel classes
        """
        schemas = []
        
        # Convert file path to module name
        relative_path = file_path.relative_to(self.internal_schemas_dir.parent)
        module_name = str(relative_path.with_suffix("")).replace(os.sep, ".")
        
        try:
            # Import the module
            module = importlib.import_module(module_name)
            
            # Find all DataFrameModel classes (including inherited ones)
            for name, obj in inspect.getmembers(module, inspect.isclass):
                # Check if it's a Pandera DataFrameModel (directly or through inheritance)
                if (hasattr(obj, '__mro__') and 
                    any(base.__name__ == 'DataFrameModel' for base in obj.__mro__)):
                    # Only include classes defined in this module (not imported ones)
                    if obj.__module__ == module.__name__:
                        schemas.append(obj)
                        print(f"  Found schema: {name}")
            
        except Exception as e:
            print(f"Error importing {module_name}: {e}")
        
        return schemas
    
    def pandera_to_json_schema(self, schema_class: Type[pa.DataFrameModel]) -> Dict[str, Any]:
        """
        Convert a Pandera DataFrameModel to JSON Schema format.
        
        Args:
            schema_class: Pandera DataFrameModel class
            
        Returns:
            JSON Schema dictionary for the schema
        """
        # Get the schema instance
        schema_instance = schema_class.to_schema()
        
        properties = {}
        required_fields = []
        
        # Process each column in the schema
        for column_name, column_schema in schema_instance.columns.items():
            # Extract field information
            field_type = self._pandera_type_to_json_type(column_schema.dtype)
            
            field_def = {
                "type": field_type,
                "description": getattr(column_schema, 'description', '')
            }
            
            # Handle nullable fields
            if getattr(column_schema, 'nullable', False):
                if isinstance(field_def["type"], str):
                    field_def["type"] = [field_def["type"], "null"]
                else:
                    field_def["type"].append("null")
            else:
                required_fields.append(column_name)
            
            properties[column_name] = field_def
        
        # Build the JSON Schema
        json_schema = {
            "type": "object",
            "title": schema_class.__name__,
            "description": schema_class.__doc__ or f"Schema for {schema_class.__name__}",
            "properties": properties
        }
        
        if required_fields:
            json_schema["required"] = required_fields
        
        return json_schema
    
    def _pandera_type_to_json_type(self, pandera_type) -> str:
        """
        Convert Pandera/pandas dtype to JSON Schema type.
        
        Args:
            pandera_type: Pandera dtype
            
        Returns:
            JSON Schema type string
        """
        type_str = str(pandera_type).lower()
        
        if 'int' in type_str:
            return "integer"
        elif 'float' in type_str:
            return "number"
        elif 'bool' in type_str:
            return "boolean"
        elif 'datetime' in type_str:
            return "string"  # ISO timestamp string
        else:
            return "string"  # Default to string
    
    def generate_combined_json_schema(self) -> Dict[str, Any]:
        """
        Generate a combined JSON Schema from all internal schemas.
        
        Returns:
            Combined JSON Schema dictionary
        """
        print("Discovering schema files...")
        schema_files = self.discover_schema_files()
        print(f"Found {len(schema_files)} schema files")
        
        all_schemas = {}
        
        for file_path in schema_files:
            print(f"\nProcessing {file_path.name}...")
            schemas = self.import_schemas_from_file(file_path)
            
            for schema_class in schemas:
                schema_name = schema_class.__name__
                json_schema = self.pandera_to_json_schema(schema_class)
                all_schemas[schema_name] = json_schema
        
        # Create the combined schema
        combined_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Fund Holdings API Schemas",
            "description": f"Combined JSON Schema for all fund holdings data structures. Generated from Pandera schemas at {self.timestamp}",
            "type": "object",
            "definitions": all_schemas,
            "properties": {
                # Add top-level properties that reference the definitions
                schema_name.lower().replace("schema", ""): {
                    "$ref": f"#/definitions/{schema_name}"
                }
                for schema_name in all_schemas.keys()
            }
        }
        
        return combined_schema
    
    def generate_schema_file(self, output_file: Optional[str] = None) -> None:
        """
        Generate and save the combined JSON Schema file.
        
        Args:
            output_file: Optional custom output filename
        """
        print(f"Generating JSON Schema from internal schemas at {self.timestamp}")
        
        # Generate the combined schema
        combined_schema = self.generate_combined_json_schema()
        
        # Determine output file path
        if output_file:
            output_path = self.output_dir / output_file
        else:
            output_path = self.output_dir / "combined_schema.json"
        
        # Write the schema file
        with open(output_path, 'w') as f:
            json.dump(combined_schema, f, indent=2)
        
        print(f"\nJSON Schema generated: {output_path}")
        print(f"Total schemas included: {len(combined_schema['definitions'])}")
        
        # Print summary
        for schema_name in combined_schema['definitions'].keys():
            field_count = len(combined_schema['definitions'][schema_name].get('properties', {}))
            print(f"  - {schema_name}: {field_count} fields")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="Generate JSON Schema from internal Pandera schemas")
    parser.add_argument("--output-file", help="Custom output filename (default: combined_schema.json)")
    parser.add_argument("--output-dir", help="Directory for output file")
    
    args = parser.parse_args()
    
    generator = JSONSchemaGenerator(args.output_dir)
    generator.generate_schema_file(args.output_file)


if __name__ == "__main__":
    main()