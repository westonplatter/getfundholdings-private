"""create_cik_management_tables

Revision ID: dbd466da6c8b
Revises: d01b9d7514ee
Create Date: 2025-07-21 22:21:53.429094

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'dbd466da6c8b'
down_revision: Union[str, Sequence[str], None] = 'd01b9d7514ee'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create fund provider and issuer hierarchy to replace constants-based CIK approach."""
    
    # Create fund_providers table - parent table for grouping related CIKs
    op.create_table(
        'fund_providers',
        sa.Column('id', sa.Integer(), nullable=False, primary_key=True),
        sa.Column('provider_name', sa.String(length=100), nullable=False, unique=True),
        sa.Column('display_name', sa.String(length=100), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=False, default=True),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('NOW()')),
    )
    
    # Create indexes for fund_providers
    op.create_index('idx_fund_providers_name', 'fund_providers', ['provider_name'])
    op.create_index('idx_fund_providers_active', 'fund_providers', ['is_active'])
    
    # Create fund_issuers table - child table with CIK management
    op.create_table(
        'fund_issuers',
        sa.Column('id', sa.Integer(), nullable=False, primary_key=True),
        sa.Column('provider_id', sa.Integer(), nullable=False),
        sa.Column('cik', sa.String(length=10), nullable=False, unique=True),
        sa.Column('company_name', sa.String(length=200), nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=False, default=True),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('NOW()')),
    )
    
    # Add foreign key constraint
    op.create_foreign_key(
        'fk_fund_issuers_provider_id',
        'fund_issuers',
        'fund_providers',
        ['provider_id'],
        ['id'],
        ondelete='CASCADE'
    )
    
    # Create indexes for fund_issuers
    op.create_index('idx_fund_issuers_provider_id', 'fund_issuers', ['provider_id'])
    op.create_index('idx_fund_issuers_cik', 'fund_issuers', ['cik'])
    op.create_index('idx_fund_issuers_active', 'fund_issuers', ['is_active'])
    op.create_index('idx_fund_issuers_name', 'fund_issuers', ['company_name'])


def downgrade() -> None:
    """Drop fund provider and issuer tables."""
    # Drop tables in reverse order due to foreign key constraints
    op.drop_table('fund_issuers')
    op.drop_table('fund_providers')
