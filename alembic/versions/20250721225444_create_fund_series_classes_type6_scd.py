"""create_fund_series_classes_type6_scd

Revision ID: bc9e12f60be2
Revises: f46c34161b44
Create Date: 2025-07-21 22:54:44.209962

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "bc9e12f60be2"
down_revision: Union[str, Sequence[str], None] = "f46c34161b44"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create fund_series table - Type 6 SCD for CIK -> Series relationships
    op.create_table(
        "fund_series",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("issuer_id", sa.Integer(), nullable=False),
        sa.Column(
            "series_id", sa.String(length=15), nullable=False
        ),  # e.g., S000004310
        # Type 6 SCD fields
        sa.Column("is_current", sa.Boolean(), nullable=False, default=True),
        sa.Column("effective_date", sa.DateTime(), nullable=False),
        sa.Column("end_date", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        # Metadata
        sa.Column("source", sa.String(length=50), nullable=False, default="sec_api"),
        sa.Column("last_verified_date", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["issuer_id"],
            ["fund_issuers.id"],
        ),
        sa.UniqueConstraint(
            "series_id", "effective_date", name="uq_fund_series_id_effective"
        ),
    )

    # Indexes for Type 6 SCD queries
    op.create_index("ix_fund_series_issuer_id", "fund_series", ["issuer_id"])
    op.create_index("ix_fund_series_series_id", "fund_series", ["series_id"])
    op.create_index("ix_fund_series_current", "fund_series", ["is_current"])
    op.create_index("ix_fund_series_effective_date", "fund_series", ["effective_date"])

    # Create fund_classes table - Type 6 SCD for Series -> Class relationships and attributes
    op.create_table(
        "fund_classes",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column(
            "series_id", sa.String(length=15), nullable=False
        ),  # References fund_series.series_id
        sa.Column("class_id", sa.String(length=15), nullable=False),  # e.g., C000219740
        # Class attributes that can change over time
        sa.Column("class_name", sa.String(length=200), nullable=True),
        sa.Column("ticker", sa.String(length=20), nullable=True),
        # Type 6 SCD fields
        sa.Column("is_current", sa.Boolean(), nullable=False, default=True),
        sa.Column("effective_date", sa.DateTime(), nullable=False),
        sa.Column("end_date", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        # Metadata and change tracking
        sa.Column("source", sa.String(length=50), nullable=False, default="sec_api"),
        sa.Column("last_verified_date", sa.DateTime(), nullable=False),
        sa.Column(
            "change_reason", sa.String(length=100), nullable=True
        ),  # Why this record was created
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "class_id", "effective_date", name="uq_fund_classes_id_effective"
        ),
    )

    # Indexes for Type 6 SCD and common queries
    op.create_index("ix_fund_classes_series_id", "fund_classes", ["series_id"])
    op.create_index("ix_fund_classes_class_id", "fund_classes", ["class_id"])
    op.create_index("ix_fund_classes_current", "fund_classes", ["is_current"])
    op.create_index(
        "ix_fund_classes_effective_date", "fund_classes", ["effective_date"]
    )
    op.create_index("ix_fund_classes_ticker", "fund_classes", ["ticker"])

    # Composite index for current records lookup
    op.create_index(
        "ix_fund_classes_current_lookup", "fund_classes", ["series_id", "is_current"]
    )
    op.create_index(
        "ix_fund_series_current_lookup", "fund_series", ["issuer_id", "is_current"]
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("fund_classes")
    op.drop_table("fund_series")
