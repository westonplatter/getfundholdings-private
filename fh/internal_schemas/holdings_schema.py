#!/usr/bin/env python3
"""
Pandera schema definitions for fund holdings data structures.
"""

from datetime import datetime
from typing import Optional

import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame, Series


class HoldingsRawSchema(pa.DataFrameModel):
    """
    Schema for raw holdings data from N-PORT filings.

    This schema defines the structure of the raw holdings CSV files that contain
    parsed N-PORT data before ticker enrichment.
    """

    # Security identification
    name: Series[str] = pa.Field(description="Security name")
    lei: Series[str] = pa.Field(nullable=True, description="Legal Entity Identifier")
    title: Series[str] = pa.Field(description="Security title/description")
    cusip: Series[str] = pa.Field(nullable=True, description="CUSIP identifier")
    isin: Series[str] = pa.Field(nullable=True, description="ISIN identifier")
    other_id: Series[str] = pa.Field(nullable=True, description="Other identifier")
    other_id_desc: Series[str] = pa.Field(
        nullable=True, description="Other identifier description"
    )

    # Position data
    balance: Series[float] = pa.Field(description="Security balance/shares")
    units: Series[str] = pa.Field(
        description="Units of measurement (e.g., 'NS' for number of shares)"
    )
    currency: Series[str] = pa.Field(description="Currency code")
    value_usd: Series[float] = pa.Field(
        description="Market value in USD (can be negative for short positions)"
    )
    percent_value: Series[float] = pa.Field(
        description="Percentage of total portfolio value (can exceed 1.0 for leveraged positions)"
    )

    # Classification data
    payoff_profile: Series[str] = pa.Field(
        description="Payoff profile (e.g., 'Long', 'Short')"
    )
    asset_category: Series[str] = pa.Field(description="Asset category code")
    issuer_category: Series[str] = pa.Field(description="Issuer category code")
    investment_country: Series[str] = pa.Field(description="Investment country code")

    # Regulatory flags
    is_restricted_security: Series[str] = pa.Field(
        description="Restricted security flag"
    )
    fair_value_level: Series[str] = pa.Field(description="Fair value level")
    is_cash_collateral: Series[str] = pa.Field(description="Cash collateral flag")
    is_non_cash_collateral: Series[str] = pa.Field(
        description="Non-cash collateral flag"
    )
    is_loan_by_fund: Series[str] = pa.Field(description="Loan by fund flag")

    # Loan data
    loan_value: Series[float] = pa.Field(
        nullable=True, description="Loan value if applicable"
    )

    # Metadata
    source_file: Series[str] = pa.Field(description="Source N-PORT XML filename")
    report_period_date: Series[str] = pa.Field(
        description="N-PORT report period date (YYYY-MM-DD)"
    )

    class Config:
        strict = True
        coerce = True


class HoldingsEnrichedSchema(HoldingsRawSchema):
    """
    Schema for enriched holdings data from N-PORT filings.

    This schema extends HoldingsRawSchema with ticker enrichment data.
    """

    # Enrichment data (inherits all fields from HoldingsRawSchema)
    ticker: Series[str] = pa.Field(
        nullable=True, description="Ticker symbol from OpenFIGI API"
    )
    enrichment_datetime: Series[datetime] = pa.Field(
        description="Datetime when ticker enrichment was performed (timezone-aware UTC)"
    )


def validate_holdings_raw(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate a raw holdings dataframe against the schema.

    Args:
        df: DataFrame to validate

    Returns:
        Validated DataFrame

    Raises:
        pandera.errors.SchemaError: If validation fails
    """
    return HoldingsRawSchema.validate(df)


def validate_holdings_enriched(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate a holdings enriched dataframe against the schema.

    Args:
        df: DataFrame to validate

    Returns:
        Validated DataFrame

    Raises:
        pandera.errors.SchemaError: If validation fails
    """
    return HoldingsEnrichedSchema.validate(df)


# Type aliases for convenience
HoldingsRawDF = DataFrame[HoldingsRawSchema]
HoldingsEnrichedDF = DataFrame[HoldingsEnrichedSchema]
