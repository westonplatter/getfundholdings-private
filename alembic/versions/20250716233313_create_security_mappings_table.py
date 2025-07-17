"""create_security_mappings_table

Revision ID: c15c72f2ced6
Revises: 
Create Date: 2025-07-16 23:33:13.797511

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c15c72f2ced6'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create security_mappings table for CUSIP/ISIN to ticker caching."""
    # Create security_mappings table
    op.create_table(
        'security_mappings',
        sa.Column('id', sa.Integer(), nullable=False, primary_key=True),
        sa.Column('identifier_type', sa.String(length=10), nullable=False),
        sa.Column('identifier_value', sa.String(length=50), nullable=False),
        sa.Column('ticker', sa.String(length=10), nullable=True),
        sa.Column('has_no_results', sa.Boolean(), nullable=False, default=False),
        sa.Column('start_date', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('end_date', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('last_fetched_date', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('NOW()')),
    )
    
    # Add check constraint for identifier_type
    op.create_check_constraint(
        'ck_security_mappings_identifier_type',
        'security_mappings',
        "identifier_type IN ('CUSIP', 'ISIN')"
    )
    
    # Create unique index for active mappings (end_date IS NULL)
    op.create_index(
        'idx_security_mappings_active',
        'security_mappings',
        ['identifier_type', 'identifier_value'],
        unique=True,
        postgresql_where=sa.text('end_date IS NULL')
    )
    
    # Create index for efficient lookups
    op.create_index(
        'idx_security_mappings_current_lookup',
        'security_mappings',
        ['identifier_type', 'identifier_value', 'end_date']
    )
    
    # Create index for cache refresh operations
    op.create_index(
        'idx_security_mappings_fetch_date',
        'security_mappings',
        ['last_fetched_date']
    )
    
    # Create index for finding negative results
    op.create_index(
        'idx_security_mappings_no_results',
        'security_mappings',
        ['has_no_results', 'last_fetched_date'],
        postgresql_where=sa.text('has_no_results = TRUE')
    )


def downgrade() -> None:
    """Drop security_mappings table and related indexes."""
    # Drop indexes first
    op.drop_index('idx_security_mappings_no_results', table_name='security_mappings')
    op.drop_index('idx_security_mappings_fetch_date', table_name='security_mappings')
    op.drop_index('idx_security_mappings_current_lookup', table_name='security_mappings')
    op.drop_index('idx_security_mappings_active', table_name='security_mappings')
    
    # Drop check constraint
    op.drop_constraint('ck_security_mappings_identifier_type', 'security_mappings', type_='check')
    
    # Drop table
    op.drop_table('security_mappings')
