"""populate_cik_data_from_constants

Revision ID: f46c34161b44
Revises: dbd466da6c8b
Create Date: 2025-07-21 22:22:40.312719

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f46c34161b44'
down_revision: Union[str, Sequence[str], None] = 'dbd466da6c8b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Populate fund_providers and fund_issuers tables with data from constants.py CIK_MAP."""
    
    # Provider-to-CIK mapping from fh/constants.py - grouped by fund provider
    provider_data = {
        "Vanguard": [
            ("The Vanguard Group, Inc.", "0000102909"),
            ("Vanguard Advisers, Inc.", "0000862084"),
            ("Vanguard Marketing Corporation", "0000862110"),
        ],
        "State Street": [
            ("State Street Corporation", "0000093751"),
            ("State Street Global Advisors Trust Company", "0000934647"),
            ("SPDR S&P 500 ETF Trust", "0000884394"),
            ("State Street Global Advisors Funds Management", "0001064641"),
        ],
        "BlackRock": [
            ("iShares Trust", "1100663"),
            ("iShares, Inc.", "0000930667"),
            ("BlackRock, Inc.", "0001364742"),
            ("BlackRock Fund Advisors", "0001006249"),
            ("BlackRock Advisors LLC", "0001006250"),
            ("blackrock", "0001761055"),
            ("iShares Bitcoin Trust ETF", "0001980994"),
        ],
        "Invesco": [
            ("Invesco Ltd.", "0000914208"),
            ("Invesco Advisers, Inc.", "0000914648"),
            ("Invesco Capital Management LLC", "0000914649"),
            ("Invesco QQQ Trust, Series 1", "0001067839"),
        ],
        "Schwab": [
            ("Charles Schwab Corporation", "0000316709"),
            ("Charles Schwab Investment Management", "0001064642"),
            ("Schwab Strategic Trust", "0001064646"),
        ],
        "Dimensional": [
            ("Dimensional Fund Advisors LP", "0000874761"),
            ("DFA Investment Dimensions Group Inc.", "0000874762"),
        ],
        "JPMorgan": [
            ("JPMorgan Chase & Co.", "0000019617"),
            ("J.P. Morgan Investment Management Inc.", "0000895421"),
            ("jpmorgan", "0001485894"),
        ],
        "VanEck": [
            ("VanEck Associates Corporation", "0000912471"),
            ("Market Vectors ETF Trust", "0001345413"),
        ],
        "ProShares": [
            ("ProShare Advisors LLC", "0001174610"),
            ("ProShares Trust", "0001174612"),
            ("ProShares UltraPro QQQ", "0001174610"),  # TQQQ - duplicate CIK
        ],
        "Fidelity": [
            ("FMR LLC", "0000315066"),
            ("Fidelity Management & Research Company", "0000315067"),
        ],
        "Grayscale": [
            ("Grayscale Investments, LLC", "0001588489"),
            ("Grayscale Bitcoin Trust", "0001588489"),  # duplicate CIK
        ],
        "Janus Henderson": [
            ("Janus Henderson Group plc", "0001691415"),
            ("Janus Capital Management LLC", "0000886982"),
        ],
        "Simplify": [
            ("Simplify Asset Management Inc.", "0001810747"),
        ],
        "Defiance": [
            ("Defiance ETFs", "0001771146"),
        ],
        "REX Shares": [
            ("REX Shares", "0001771146"),  # duplicate CIK with Defiance
        ],
        "Exchange Traded Concepts": [
            ("Exchange Traded Concepts, LLC", "0001452937"),
        ],
        "ARK": [
            ("ARK ETF Trust", "0001579982"),
        ],
        "Tuttle Capital": [
            ("Collaborative Investment Series Trust", "0001719812"),
        ],
    }
    
    # Insert data using raw SQL to ensure consistent behavior
    connection = op.get_bind()
    
    for provider_name, issuers in provider_data.items():
        # First, insert or get provider
        provider_result = connection.execute(
            sa.text("""
                INSERT INTO fund_providers (provider_name, display_name, is_active, created_at, updated_at)
                VALUES (:provider_name, :provider_name, :is_active, NOW(), NOW())
                ON CONFLICT (provider_name) DO UPDATE SET updated_at = NOW()
                RETURNING id
            """),
            {
                "provider_name": provider_name,
                "is_active": True
            }
        )
        
        # Get the provider_id from the result
        provider_id = provider_result.fetchone()[0]
        
        # Insert issuers for this provider
        for company_name, cik in issuers:
            connection.execute(
                sa.text("""
                    INSERT INTO fund_issuers (provider_id, company_name, cik, is_active, created_at, updated_at)
                    VALUES (:provider_id, :company_name, :cik, :is_active, NOW(), NOW())
                    ON CONFLICT (cik) DO NOTHING
                """),
                {
                    "provider_id": provider_id,
                    "company_name": company_name,
                    "cik": cik,
                    "is_active": True
                }
            )


def downgrade() -> None:
    """Remove all data from fund_providers and fund_issuers tables."""
    # Clear all data from both tables (foreign key cascade will handle fund_issuers)
    op.execute("DELETE FROM fund_providers")
