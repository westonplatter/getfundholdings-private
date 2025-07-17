"""increase_ticker_column_size

Revision ID: d01b9d7514ee
Revises: c15c72f2ced6
Create Date: 2025-07-17 00:05:16.640726

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd01b9d7514ee'
down_revision: Union[str, Sequence[str], None] = 'c15c72f2ced6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Increase ticker column size to handle longer ticker symbols."""
    # Increase ticker column size from 10 to 20 characters
    op.alter_column('security_mappings', 'ticker',
                    existing_type=sa.String(length=10),
                    type_=sa.String(length=20),
                    existing_nullable=True)


def downgrade() -> None:
    """Decrease ticker column size back to 10 characters."""
    # Note: This may fail if there are ticker symbols longer than 10 characters
    op.alter_column('security_mappings', 'ticker',
                    existing_type=sa.String(length=20),
                    type_=sa.String(length=10),
                    existing_nullable=True)
