"""create_nport_reports_table

Revision ID: 4653ea470261
Revises: 32cefe155f65
Create Date: 2025-07-22 21:26:28.552442

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4653ea470261'
down_revision: Union[str, Sequence[str], None] = '32cefe155f65'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create sec_reports table - Generic table for all SEC reports (N-PORT, 13F, N-CSR, etc.)
    op.create_table(
        "sec_reports",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("series_id", sa.String(length=15), nullable=False),
        sa.Column("accession_number", sa.String(length=50), nullable=False),
        
        # Report identification
        sa.Column("form_type", sa.String(length=20), nullable=False),
        sa.Column("filing_date", sa.Date(), nullable=True),
        sa.Column("report_date", sa.Date(), nullable=True),
        sa.Column("public_date", sa.Date(), nullable=True),  # When data becomes public (N-PORT has 60-day delay)
        
        # Processing status tracking
        sa.Column("download_status", sa.String(length=20), nullable=False, default="pending"),
        sa.Column("processing_status", sa.String(length=20), nullable=False, default="pending"),
        
        # Flexible storage for different form types using JSONB
        sa.Column("file_paths", sa.JSON(), nullable=True),  # {"xml": "path", "csv": "path", "txt": "path"}
        sa.Column("report_metadata", sa.JSON(), nullable=True),  # Form-specific metadata
        sa.Column("raw_data", sa.JSON(), nullable=True),  # Original parsed SEC data
        
        # Standard tracking fields
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("last_processed_at", sa.DateTime(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        
        sa.PrimaryKeyConstraint("id"),
        # Note: No foreign key constraint because fund_series.series_id is not unique 
        # (Type 6 SCD allows multiple historical records per series_id)
        sa.UniqueConstraint(
            "series_id", "accession_number", "form_type",
            name="uq_sec_reports_series_accession_form"
        ),
    )
    
    # Create indexes for efficient querying
    op.create_index("ix_sec_reports_series_id", "sec_reports", ["series_id"])
    op.create_index("ix_sec_reports_accession_number", "sec_reports", ["accession_number"])
    op.create_index("ix_sec_reports_form_type", "sec_reports", ["form_type"])
    op.create_index("ix_sec_reports_filing_date", "sec_reports", ["filing_date"])
    op.create_index("ix_sec_reports_report_date", "sec_reports", ["report_date"])
    op.create_index("ix_sec_reports_download_status", "sec_reports", ["download_status"])
    op.create_index("ix_sec_reports_processing_status", "sec_reports", ["processing_status"])
    
    # Composite indexes for common query patterns
    op.create_index(
        "ix_sec_reports_series_form_date", 
        "sec_reports", 
        ["series_id", "form_type", "report_date"]
    )
    op.create_index(
        "ix_sec_reports_form_status", 
        "sec_reports", 
        ["form_type", "download_status", "processing_status"]
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("sec_reports")
