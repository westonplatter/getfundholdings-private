"""add_yieldmax_cik_to_tidal_trust

Revision ID: ed539501eefd
Revises: 4653ea470261
Create Date: 2025-07-27 19:09:42.882317

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ed539501eefd'
down_revision: Union[str, Sequence[str], None] = '4653ea470261'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add YieldMax ETFs (Tidal Trust II) to fund_providers and fund_issuers tables."""
    
    # Add YieldMax/Tidal Trust II data
    connection = op.get_bind()
    
    # Insert YieldMax as a provider
    provider_result = connection.execute(
        sa.text(
            """
            INSERT INTO fund_providers (provider_name, display_name, is_active, created_at, updated_at)
            VALUES (:provider_name, :display_name, :is_active, NOW(), NOW())
            ON CONFLICT (provider_name) DO UPDATE SET updated_at = NOW()
            RETURNING id
        """
        ),
        {"provider_name": "YieldMax", "display_name": "YieldMax ETFs", "is_active": True},
    )
    
    # Get the provider_id from the result
    provider_id = provider_result.fetchone()[0]
    
    # Insert Tidal Trust II as the issuer for YieldMax ETFs
    connection.execute(
        sa.text(
            """
            INSERT INTO fund_issuers (provider_id, company_name, cik, is_active, created_at, updated_at)
            VALUES (:provider_id, :company_name, :cik, :is_active, NOW(), NOW())
            ON CONFLICT (cik) DO NOTHING
        """
        ),
        {
            "provider_id": provider_id,
            "company_name": "Tidal Trust II",
            "cik": "0001924868",
            "is_active": True,
        },
    )


def downgrade() -> None:
    """Remove YieldMax ETFs (Tidal Trust II) from the database."""
    connection = op.get_bind()
    
    # Remove the issuer first (due to foreign key constraint)
    connection.execute(
        sa.text("DELETE FROM fund_issuers WHERE cik = '0001924868'")
    )
    
    # Remove the provider if no other issuers reference it
    connection.execute(
        sa.text(
            """
            DELETE FROM fund_providers 
            WHERE provider_name = 'YieldMax' 
            AND NOT EXISTS (
                SELECT 1 FROM fund_issuers WHERE provider_id = fund_providers.id
            )
        """
        )
    )
