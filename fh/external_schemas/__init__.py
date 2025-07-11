"""
External Schema Specifications for Fund Holdings Data

This package contains schema definitions for EXTERNAL consumption - JSON API contracts
used by frontend applications, third-party integrations, and public APIs.

This is distinct from fh/schemas.py which contains INTERNAL schemas for pandas/pandera
data validation within the Python pipeline.

External Schemas (this package):
- Define JSON structure for R2 storage and web APIs
- Used by frontend applications for type safety
- Define public API contracts

Internal Schemas (fh/schemas.py):
- Define pandas DataFrame validation with pandera
- Used within Python pipeline for data quality
- Not exposed to external consumers

Files:
- json_schema.json: JSON Schema specification for validation
- typescript.d.ts: TypeScript type definitions for frontend
- openapi.yaml: OpenAPI/Swagger specification for API documentation
- generator.py: Script to generate schemas from actual data structures
"""

__version__ = "1.0.0"