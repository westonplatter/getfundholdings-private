"""fix_series_id_column_length

Revision ID: 32cefe155f65
Revises: bc9e12f60be2
Create Date: 2025-07-21 22:59:46.598991

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "32cefe155f65"
down_revision: Union[str, Sequence[str], None] = "bc9e12f60be2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Increase series_id column length to handle longer strings from SEC data
    # Series IDs are typically S000004310 format but SEC sometimes returns longer descriptive text
    op.alter_column(
        "fund_series",
        "series_id",
        type_=sa.String(length=100),
        existing_type=sa.String(length=15),
    )

    op.alter_column(
        "fund_classes",
        "series_id",
        type_=sa.String(length=100),
        existing_type=sa.String(length=15),
    )

    # Also increase class_id column for consistency
    op.alter_column(
        "fund_classes",
        "class_id",
        type_=sa.String(length=100),
        existing_type=sa.String(length=15),
    )


def downgrade() -> None:
    """Downgrade schema."""
    # Note: This may fail if longer data exists
    op.alter_column(
        "fund_classes",
        "class_id",
        type_=sa.String(length=15),
        existing_type=sa.String(length=100),
    )

    op.alter_column(
        "fund_classes",
        "series_id",
        type_=sa.String(length=15),
        existing_type=sa.String(length=100),
    )

    op.alter_column(
        "fund_series",
        "series_id",
        type_=sa.String(length=15),
        existing_type=sa.String(length=100),
    )
