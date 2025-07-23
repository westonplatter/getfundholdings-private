#!/usr/bin/env python3
"""
Database models and services for fund holdings data pipeline.

This module provides SQLModel-based database models and services for:
- Fund provider and issuer management (CIK hierarchy)
- Security mappings cache (CUSIP/ISIN to ticker mappings from OpenFIGI API)
- SEC reports tracking (N-PORT, 13F, N-CSR, etc.)
"""

from datetime import datetime, timedelta, date
from typing import List, Optional

from loguru import logger
from sqlmodel import Field, Session, SQLModel, create_engine, select, Column, JSON


# Fund Provider and Issuer Models
class FundProvider(SQLModel, table=True):
    """Database model for fund providers (parent companies)."""

    __tablename__ = "fund_providers"

    id: Optional[int] = Field(default=None, primary_key=True)
    provider_name: str = Field(max_length=100, index=True)
    display_name: Optional[str] = Field(max_length=100, default=None)
    is_active: bool = Field(default=True, index=True)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)


class FundIssuer(SQLModel, table=True):
    """Database model for fund issuers (CIK entities)."""

    __tablename__ = "fund_issuers"

    id: Optional[int] = Field(default=None, primary_key=True)
    provider_id: int = Field(foreign_key="fund_providers.id", index=True)
    cik: str = Field(max_length=10, unique=True, index=True)
    company_name: str = Field(max_length=200, index=True)
    is_active: bool = Field(default=True, index=True)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)


# Fund Series Models with Type 6 SCD
class FundSeries(SQLModel, table=True):
    """Database model for fund series with Type 6 SCD tracking."""

    __tablename__ = "fund_series"

    id: Optional[int] = Field(default=None, primary_key=True)
    issuer_id: int = Field(foreign_key="fund_issuers.id", index=True)
    series_id: str = Field(max_length=100, index=True)  # e.g., S000004310

    # Type 6 SCD fields
    is_current: bool = Field(default=True, index=True)
    effective_date: datetime = Field(index=True)
    end_date: Optional[datetime] = Field(default=None)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

    # Metadata
    source: str = Field(max_length=50, default="sec_api")
    last_verified_date: datetime = Field(default_factory=datetime.now)


class FundClass(SQLModel, table=True):
    """Database model for fund classes with Type 6 SCD tracking."""

    __tablename__ = "fund_classes"

    id: Optional[int] = Field(default=None, primary_key=True)
    series_id: str = Field(
        max_length=100, index=True
    )  # References FundSeries.series_id
    class_id: str = Field(max_length=100, index=True)  # e.g., C000219740

    # Class attributes that can change over time
    class_name: Optional[str] = Field(max_length=200, default=None)
    ticker: Optional[str] = Field(max_length=20, default=None, index=True)

    # Type 6 SCD fields
    is_current: bool = Field(default=True, index=True)
    effective_date: datetime = Field(index=True)
    end_date: Optional[datetime] = Field(default=None)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

    # Metadata and change tracking
    source: str = Field(max_length=50, default="sec_api")
    last_verified_date: datetime = Field(default_factory=datetime.now)
    change_reason: Optional[str] = Field(max_length=100, default=None)


# SEC Reports Models
class SECReport(SQLModel, table=True):
    """Database model for SEC reports filed by funds (N-PORT, 13F, N-CSR, etc.)."""

    __tablename__ = "sec_reports"

    id: Optional[int] = Field(default=None, primary_key=True)
    series_id: str = Field(max_length=15, index=True)  # References fund_series.series_id (no FK due to Type 6 SCD)
    accession_number: str = Field(max_length=50, index=True)  # SEC accession number

    # Report identification
    form_type: str = Field(max_length=20, index=True)  # NPORT-P, 13F, N-CSR, etc.
    filing_date: Optional[date] = Field(default=None, index=True)  # Date filed with SEC
    report_date: Optional[date] = Field(default=None, index=True)  # Period end date
    public_date: Optional[date] = Field(default=None)  # When data becomes public (N-PORT has 60-day delay)

    # Processing status tracking
    download_status: str = Field(max_length=20, default="pending", index=True)  # pending, downloaded, failed
    processing_status: str = Field(max_length=20, default="pending", index=True)  # pending, processed, failed

    # Flexible storage for different form types
    file_paths: Optional[dict] = Field(default=None, sa_column=Column(JSON))  # {"xml": "path", "csv": "path", "txt": "path"}
    report_metadata: Optional[dict] = Field(default=None, sa_column=Column(JSON))  # Form-specific metadata
    raw_data: Optional[dict] = Field(default=None, sa_column=Column(JSON))  # Original parsed SEC data

    # Standard tracking fields
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    last_processed_at: Optional[datetime] = Field(default=None)
    error_message: Optional[str] = Field(default=None)


# Security Mapping Models
class SecurityMapping(SQLModel, table=True):
    """Database model for security identifier to ticker mappings."""

    __tablename__ = "security_mappings"

    id: Optional[int] = Field(default=None, primary_key=True)
    identifier_type: str = Field(max_length=10, index=True)
    identifier_value: str = Field(max_length=50, index=True)
    ticker: Optional[str] = Field(default=None, max_length=20)
    has_no_results: bool = Field(default=False)
    start_date: datetime = Field(default_factory=lambda: datetime.now())
    end_date: Optional[datetime] = Field(default=None)
    last_fetched_date: datetime = Field(default_factory=lambda: datetime.now())
    created_at: datetime = Field(default_factory=lambda: datetime.now())
    updated_at: datetime = Field(default_factory=lambda: datetime.now())


class DatabaseManager:
    """Database connection and session management."""

    def __init__(self, database_url: str):
        """Initialize database manager with connection URL."""
        # Add connection pooling and performance settings
        self.engine = create_engine(
            database_url,
            pool_size=10,  # Number of connections to maintain
            max_overflow=20,  # Additional connections when needed
            pool_pre_ping=True,  # Validate connections before use
            pool_recycle=3600,  # Recycle connections after 1 hour
            echo=False,  # Set to True for SQL debugging
        )
        # Note: We don't create tables here since we use Alembic migrations

    def get_session(self) -> Session:
        """Get a database session context manager."""
        return Session(self.engine)


class FundDataSCDService:
    """Service for Type 6 SCD operations on fund series and class data."""

    def __init__(self, db_manager: DatabaseManager):
        """Initialize service with database manager."""
        self.db_manager = db_manager

    def upsert_series_data(self, issuer_id: int, series_data: List[dict]) -> dict:
        """
        Insert or update series data using Type 6 SCD approach with batch optimization.

        Args:
            issuer_id: Fund issuer ID (from fund_issuers table)
            series_data: List of series data from SEC API

        Returns:
            Dictionary with operation statistics
        """
        stats = {
            "series_new": 0,
            "series_verified": 0,
            "series_skipped_invalid": 0,
            "classes_new": 0,
            "classes_updated": 0,
            "classes_verified": 0,
            "classes_skipped_invalid": 0,
        }

        current_time = datetime.now()

        try:
            with self.db_manager.get_session() as session:
                # Pre-validate and collect valid data
                valid_series_ids = []
                valid_class_data = []

                for series in series_data:
                    series_id = series.get("series_id")
                    if not series_id:
                        continue

                    if self._is_valid_series_id(series_id):
                        valid_series_ids.append(series_id)

                        # Collect valid class data
                        for class_data in series.get("classes", []):
                            class_id = class_data.get("class_id")
                            if class_id and self._is_valid_class_id(class_id):
                                valid_class_data.append((series_id, class_data))
                            elif class_id:
                                logger.warning(
                                    f"Skipping invalid class_id format: '{class_id}' (expected format: C000000000)"
                                )
                                stats["classes_skipped_invalid"] += 1
                    else:
                        logger.warning(
                            f"Skipping invalid series_id format: '{series_id}' (expected format: S000000000)"
                        )
                        stats["series_skipped_invalid"] += 1

                # Batch load existing series records
                existing_series_map = {}
                if valid_series_ids:
                    existing_series_stmt = select(FundSeries).where(
                        FundSeries.issuer_id == issuer_id,
                        FundSeries.series_id.in_(valid_series_ids),
                        FundSeries.is_current == True,
                    )
                    existing_series = session.exec(existing_series_stmt).all()
                    existing_series_map = {s.series_id: s for s in existing_series}

                # Batch load existing class records
                existing_class_map = {}
                if valid_class_data:
                    class_ids = [
                        class_data[1]["class_id"] for class_data in valid_class_data
                    ]
                    existing_class_stmt = select(FundClass).where(
                        FundClass.class_id.in_(class_ids), FundClass.is_current == True
                    )
                    existing_classes = session.exec(existing_class_stmt).all()
                    existing_class_map = {c.class_id: c for c in existing_classes}

                # Process series records in batch
                for series_id in valid_series_ids:
                    if series_id in existing_series_map:
                        # Update existing series
                        existing_series = existing_series_map[series_id]
                        existing_series.last_verified_date = current_time
                        existing_series.updated_at = current_time
                        session.add(existing_series)
                        stats["series_verified"] += 1
                    else:
                        # Create new series
                        new_series = FundSeries(
                            issuer_id=issuer_id,
                            series_id=series_id,
                            effective_date=current_time,
                            last_verified_date=current_time,
                        )
                        session.add(new_series)
                        stats["series_new"] += 1

                # Process class records in batch
                for series_id, class_data in valid_class_data:
                    class_id = class_data["class_id"]
                    class_name = class_data.get("class_name")
                    ticker = class_data.get("ticker")

                    if class_id in existing_class_map:
                        existing_class = existing_class_map[class_id]

                        # Check if attributes have changed
                        changed = False
                        change_reasons = []

                        if existing_class.class_name != class_name:
                            changed = True
                            change_reasons.append(
                                f"name: '{existing_class.class_name}' → '{class_name}'"
                            )

                        if existing_class.ticker != ticker:
                            changed = True
                            change_reasons.append(
                                f"ticker: '{existing_class.ticker}' → '{ticker}'"
                            )

                        if changed:
                            # End the current record
                            existing_class.is_current = False
                            existing_class.end_date = current_time
                            existing_class.updated_at = current_time
                            session.add(existing_class)

                            # Create new current record
                            new_class = FundClass(
                                series_id=series_id,
                                class_id=class_id,
                                class_name=class_name,
                                ticker=ticker,
                                effective_date=current_time,
                                last_verified_date=current_time,
                                change_reason="; ".join(change_reasons),
                            )
                            session.add(new_class)
                            stats["classes_updated"] += 1

                            logger.info(
                                f"Class {class_id} updated: {'; '.join(change_reasons)}"
                            )
                        else:
                            # No changes - just update verification date
                            existing_class.last_verified_date = current_time
                            existing_class.updated_at = current_time
                            session.add(existing_class)
                            stats["classes_verified"] += 1
                    else:
                        # New class record
                        new_class = FundClass(
                            series_id=series_id,
                            class_id=class_id,
                            class_name=class_name,
                            ticker=ticker,
                            effective_date=current_time,
                            last_verified_date=current_time,
                            change_reason="new_record",
                        )
                        session.add(new_class)
                        stats["classes_new"] += 1

                # Single commit for all operations
                session.commit()

        except Exception as e:
            logger.error(f"Failed to upsert series data for issuer {issuer_id}: {e}")
            raise

        return stats

    def _is_valid_series_id(self, series_id: str) -> bool:
        """
        Validate if a series_id looks like a proper SEC series identifier.

        Args:
            series_id: Series ID to validate

        Returns:
            True if looks valid, False otherwise
        """
        if not series_id or not isinstance(series_id, str):
            return False

        # Valid series IDs typically start with 'S' followed by 9-10 digits
        # Example: S000004310
        if series_id.startswith("S") and len(series_id) >= 10 and len(series_id) <= 15:
            # Check if the part after 'S' is numeric
            numeric_part = series_id[1:]
            return numeric_part.isdigit()

        return False

    def _is_valid_class_id(self, class_id: str) -> bool:
        """
        Validate if a class_id looks like a proper SEC class identifier.

        Args:
            class_id: Class ID to validate

        Returns:
            True if looks valid, False otherwise
        """
        if not class_id or not isinstance(class_id, str):
            return False

        # Valid class IDs typically start with 'C' followed by 9-10 digits
        # Example: C000219740
        if class_id.startswith("C") and len(class_id) >= 10 and len(class_id) <= 15:
            # Check if the part after 'C' is numeric
            numeric_part = class_id[1:]
            return numeric_part.isdigit()

        return False

    def _upsert_series_record(
        self,
        session: Session,
        issuer_id: int,
        series_id: str,
        current_time: datetime,
        stats: dict,
    ):
        """Upsert a single series record."""
        # Validate series_id format
        if not self._is_valid_series_id(series_id):
            logger.warning(
                f"Skipping invalid series_id format: '{series_id}' (expected format: S000000000)"
            )
            stats["series_skipped_invalid"] += 1
            return

        # Check if current record exists
        existing_series = session.exec(
            select(FundSeries).where(
                FundSeries.issuer_id == issuer_id,
                FundSeries.series_id == series_id,
                FundSeries.is_current == True,
            )
        ).first()

        if existing_series:
            # Series exists - just update last_verified_date
            existing_series.last_verified_date = current_time
            existing_series.updated_at = current_time
            session.add(existing_series)
            stats["series_verified"] += 1
        else:
            # New series - create record
            new_series = FundSeries(
                issuer_id=issuer_id,
                series_id=series_id,
                effective_date=current_time,
                last_verified_date=current_time,
            )
            session.add(new_series)
            stats["series_new"] += 1

    def _upsert_class_record(
        self,
        session: Session,
        series_id: str,
        class_data: dict,
        current_time: datetime,
        stats: dict,
    ):
        """Upsert a single class record with Type 6 SCD logic."""
        class_id = class_data["class_id"]
        class_name = class_data.get("class_name")
        ticker = class_data.get("ticker")

        # Validate class_id format
        if not self._is_valid_class_id(class_id):
            logger.warning(
                f"Skipping invalid class_id format: '{class_id}' (expected format: C000000000)"
            )
            stats["classes_skipped_invalid"] += 1
            return

        # Validate series_id format (should have been validated earlier, but double-check)
        if not self._is_valid_series_id(series_id):
            logger.warning(
                f"Skipping class {class_id} - invalid series_id: '{series_id}'"
            )
            stats["classes_skipped_invalid"] += 1
            return

        # Get current record
        existing_class = session.exec(
            select(FundClass).where(
                FundClass.series_id == series_id,
                FundClass.class_id == class_id,
                FundClass.is_current == True,
            )
        ).first()

        if existing_class:
            # Check if attributes have changed
            changed = False
            change_reasons = []

            if existing_class.class_name != class_name:
                changed = True
                change_reasons.append(
                    f"name: '{existing_class.class_name}' → '{class_name}'"
                )

            if existing_class.ticker != ticker:
                changed = True
                change_reasons.append(f"ticker: '{existing_class.ticker}' → '{ticker}'")

            if changed:
                # End the current record
                existing_class.is_current = False
                existing_class.end_date = current_time
                existing_class.updated_at = current_time
                session.add(existing_class)

                # Create new current record
                new_class = FundClass(
                    series_id=series_id,
                    class_id=class_id,
                    class_name=class_name,
                    ticker=ticker,
                    effective_date=current_time,
                    last_verified_date=current_time,
                    change_reason="; ".join(change_reasons),
                )
                session.add(new_class)
                stats["classes_updated"] += 1

                logger.info(f"Class {class_id} updated: {'; '.join(change_reasons)}")
            else:
                # No changes - just update verification date
                existing_class.last_verified_date = current_time
                existing_class.updated_at = current_time
                session.add(existing_class)
                stats["classes_verified"] += 1
        else:
            # New class record
            new_class = FundClass(
                series_id=series_id,
                class_id=class_id,
                class_name=class_name,
                ticker=ticker,
                effective_date=current_time,
                last_verified_date=current_time,
                change_reason="new_record",
            )
            session.add(new_class)
            stats["classes_new"] += 1

    def get_current_series_for_issuer(self, issuer_id: int) -> List[FundSeries]:
        """Get all current series for an issuer."""
        try:
            with self.db_manager.get_session() as session:
                statement = (
                    select(FundSeries)
                    .where(
                        FundSeries.issuer_id == issuer_id, FundSeries.is_current == True
                    )
                    .order_by(FundSeries.series_id)
                )

                return list(session.exec(statement).all())
        except Exception as e:
            logger.error(f"Failed to get current series for issuer {issuer_id}: {e}")
            return []

    def get_current_classes_for_series(self, series_id: str) -> List[FundClass]:
        """Get all current classes for a series."""
        try:
            with self.db_manager.get_session() as session:
                statement = (
                    select(FundClass)
                    .where(
                        FundClass.series_id == series_id, FundClass.is_current == True
                    )
                    .order_by(FundClass.class_id)
                )

                return list(session.exec(statement).all())
        except Exception as e:
            logger.error(f"Failed to get current classes for series {series_id}: {e}")
            return []

    def get_class_history(self, class_id: str) -> List[FundClass]:
        """Get full history for a class ID."""
        try:
            with self.db_manager.get_session() as session:
                statement = (
                    select(FundClass)
                    .where(FundClass.class_id == class_id)
                    .order_by(FundClass.effective_date)
                )

                return list(session.exec(statement).all())
        except Exception as e:
            logger.error(f"Failed to get class history for {class_id}: {e}")
            return []

    def get_stats(self) -> dict:
        """Get statistics about series/class data."""
        try:
            with self.db_manager.get_session() as session:
                # Count current records
                series_count = len(
                    list(
                        session.exec(
                            select(FundSeries).where(FundSeries.is_current == True)
                        ).all()
                    )
                )

                classes_count = len(
                    list(
                        session.exec(
                            select(FundClass).where(FundClass.is_current == True)
                        ).all()
                    )
                )

                # Count total historical records
                total_series = len(list(session.exec(select(FundSeries)).all()))
                total_classes = len(list(session.exec(select(FundClass)).all()))

                return {
                    "current_series": series_count,
                    "current_classes": classes_count,
                    "total_series_history": total_series,
                    "total_classes_history": total_classes,
                    "series_change_rate": (
                        (total_series - series_count) / total_series
                        if total_series > 0
                        else 0
                    ),
                    "classes_change_rate": (
                        (total_classes - classes_count) / total_classes
                        if total_classes > 0
                        else 0
                    ),
                }
        except Exception as e:
            logger.error(f"Failed to get SCD stats: {e}")
            return {}


class SecurityMappingService:
    """Service for CRUD operations on security mappings."""

    def __init__(self, db_manager: DatabaseManager):
        """Initialize service with database manager."""
        self.db_manager = db_manager

    def get_active_mapping(
        self, identifier_type: str, identifier_value: str
    ) -> Optional[SecurityMapping]:
        """
        Get active mapping for identifier.

        Args:
            identifier_type: 'CUSIP' or 'ISIN'
            identifier_value: The identifier value

        Returns:
            SecurityMapping if found and active, None otherwise
        """
        try:
            with self.db_manager.get_session() as session:
                statement = select(SecurityMapping).where(
                    SecurityMapping.identifier_type == identifier_type,
                    SecurityMapping.identifier_value == identifier_value,
                    SecurityMapping.end_date.is_(None),
                )
                return session.exec(statement).first()
        except Exception as e:
            logger.warning(
                f"Failed to get active mapping for {identifier_type} {identifier_value}: {e}"
            )
            return None

    def create_or_update_mapping(
        self,
        identifier_type: str,
        identifier_value: str,
        ticker: Optional[str],
        has_no_results: bool = False,
    ) -> Optional[SecurityMapping]:
        """
        Create new mapping or update existing one.

        Args:
            identifier_type: 'CUSIP' or 'ISIN'
            identifier_value: The identifier value
            ticker: Ticker symbol (None if not found)
            has_no_results: True if API returned no results

        Returns:
            SecurityMapping if successful, None if failed
        """
        try:
            with self.db_manager.get_session() as session:
                # Check if active mapping exists
                existing = self.get_active_mapping(identifier_type, identifier_value)

                if existing:
                    # Update existing mapping
                    existing.ticker = ticker
                    existing.has_no_results = has_no_results
                    existing.last_fetched_date = datetime.now()
                    existing.updated_at = datetime.now()
                    session.add(existing)
                    mapping = existing
                else:
                    # Create new mapping
                    mapping = SecurityMapping(
                        identifier_type=identifier_type,
                        identifier_value=identifier_value,
                        ticker=ticker,
                        has_no_results=has_no_results,
                    )
                    session.add(mapping)

                session.commit()
                session.refresh(mapping)
                return mapping

        except Exception as e:
            logger.warning(
                f"Failed to create/update mapping for {identifier_type} {identifier_value}: {e}"
            )
            return None

    def find_stale_mappings(self, max_age_days: int = 60) -> List[SecurityMapping]:
        """
        Find mappings that need refresh.

        Args:
            max_age_days: Maximum age in days before considering stale

        Returns:
            List of stale SecurityMapping objects
        """
        try:
            with self.db_manager.get_session() as session:
                cutoff_date = datetime.now() - timedelta(days=max_age_days)
                statement = select(SecurityMapping).where(
                    SecurityMapping.last_fetched_date < cutoff_date,
                    SecurityMapping.end_date.is_(None),
                )
                return list(session.exec(statement).all())
        except Exception as e:
            logger.warning(f"Failed to find stale mappings: {e}")
            return []

    def invalidate_mapping(self, identifier_type: str, identifier_value: str) -> bool:
        """
        Invalidate mapping by setting end_date.

        Args:
            identifier_type: 'CUSIP' or 'ISIN'
            identifier_value: The identifier value

        Returns:
            True if successful, False otherwise
        """
        try:
            with self.db_manager.get_session() as session:
                statement = select(SecurityMapping).where(
                    SecurityMapping.identifier_type == identifier_type,
                    SecurityMapping.identifier_value == identifier_value,
                    SecurityMapping.end_date.is_(None),
                )
                mapping = session.exec(statement).first()
                if mapping:
                    mapping.end_date = datetime.now()
                    mapping.updated_at = datetime.now()
                    session.add(mapping)
                    session.commit()
                    return True
                return False
        except Exception as e:
            logger.warning(
                f"Failed to invalidate mapping for {identifier_type} {identifier_value}: {e}"
            )
            return False

    def get_cache_stats(self) -> dict:
        """
        Get cache statistics.

        Returns:
            Dictionary with cache statistics
        """
        try:
            with self.db_manager.get_session() as session:
                # Total active mappings
                total_statement = select(SecurityMapping).where(
                    SecurityMapping.end_date.is_(None)
                )
                total = len(list(session.exec(total_statement).all()))

                # Successful mappings (has ticker)
                success_statement = select(SecurityMapping).where(
                    SecurityMapping.end_date.is_(None),
                    SecurityMapping.has_no_results == False,
                    SecurityMapping.ticker.is_not(None),
                )
                successful = len(list(session.exec(success_statement).all()))

                # Failed mappings (no results)
                failed_statement = select(SecurityMapping).where(
                    SecurityMapping.end_date.is_(None),
                    SecurityMapping.has_no_results == True,
                )
                failed = len(list(session.exec(failed_statement).all()))

                return {
                    "total_cached": total,
                    "found_cached": successful,
                    "not_found_cached": failed,
                }
        except Exception as e:
            logger.warning(f"Failed to get cache stats: {e}")
            return {"total_cached": 0, "found_cached": 0, "not_found_cached": 0}

    def clear_cache(self) -> int:
        """
        Clear all cached mappings by setting end_date.

        Returns:
            Number of mappings cleared
        """
        try:
            with self.db_manager.get_session() as session:
                statement = select(SecurityMapping).where(
                    SecurityMapping.end_date.is_(None)
                )
                mappings = list(session.exec(statement).all())

                count = 0
                for mapping in mappings:
                    mapping.end_date = datetime.now()
                    mapping.updated_at = datetime.now()
                    session.add(mapping)
                    count += 1

                session.commit()
                return count
        except Exception as e:
            logger.warning(f"Failed to clear cache: {e}")
            return 0


class SECReportService:
    """Service for CRUD operations on SEC reports."""

    def __init__(self, db_manager: DatabaseManager):
        """Initialize service with database manager."""
        self.db_manager = db_manager

    def upsert_report(
        self,
        series_id: str,
        accession_number: str,
        form_type: str,
        filing_date: Optional[date] = None,
        report_date: Optional[date] = None,
        public_date: Optional[date] = None,
        report_metadata: Optional[dict] = None,
        raw_data: Optional[dict] = None,
    ) -> Optional[SECReport]:
        """
        Create or update an SEC report record.

        Args:
            series_id: Series ID (e.g., S000004310)
            accession_number: SEC accession number
            form_type: Form type (NPORT-P, 13F, N-CSR, etc.)
            filing_date: Date filed with SEC
            report_date: Report period end date
            public_date: Date data becomes public
            report_metadata: Form-specific metadata
            raw_data: Original parsed SEC data

        Returns:
            SECReport if successful, None if failed
        """
        try:
            with self.db_manager.get_session() as session:
                # Check if report already exists
                existing = session.exec(
                    select(SECReport).where(
                        SECReport.series_id == series_id,
                        SECReport.accession_number == accession_number,
                        SECReport.form_type == form_type,
                    )
                ).first()

                current_time = datetime.now()

                if existing:
                    # Update existing report
                    existing.filing_date = filing_date or existing.filing_date
                    existing.report_date = report_date or existing.report_date
                    existing.public_date = public_date or existing.public_date
                    existing.report_metadata = report_metadata or existing.report_metadata
                    existing.raw_data = raw_data or existing.raw_data
                    existing.updated_at = current_time
                    session.add(existing)
                    report = existing
                else:
                    # Create new report
                    report = SECReport(
                        series_id=series_id,
                        accession_number=accession_number,
                        form_type=form_type,
                        filing_date=filing_date,
                        report_date=report_date,
                        public_date=public_date,
                        report_metadata=report_metadata or {},
                        raw_data=raw_data or {},
                        created_at=current_time,
                        updated_at=current_time,
                    )
                    session.add(report)

                session.commit()
                session.refresh(report)
                return report

        except Exception as e:
            logger.error(f"Failed to upsert SEC report {form_type} {accession_number}: {e}")
            return None

    def get_reports_by_series(
        self, series_id: str, form_type: Optional[str] = None
    ) -> List[SECReport]:
        """
        Get all reports for a series, optionally filtered by form type.

        Args:
            series_id: Series ID to search for
            form_type: Optional form type filter

        Returns:
            List of SECReport objects
        """
        try:
            with self.db_manager.get_session() as session:
                statement = select(SECReport).where(SECReport.series_id == series_id)
                
                if form_type:
                    statement = statement.where(SECReport.form_type == form_type)
                
                statement = statement.order_by(SECReport.report_date.desc())
                
                return list(session.exec(statement).all())
        except Exception as e:
            logger.error(f"Failed to get reports for series {series_id}: {e}")
            return []

    def get_pending_downloads(self, form_type: Optional[str] = None) -> List[SECReport]:
        """
        Get reports that need to be downloaded.

        Args:
            form_type: Optional form type filter

        Returns:
            List of SECReport objects with pending download status
        """
        try:
            with self.db_manager.get_session() as session:
                statement = select(SECReport).where(
                    SECReport.download_status == "pending"
                )
                
                if form_type:
                    statement = statement.where(SECReport.form_type == form_type)
                
                statement = statement.order_by(SECReport.filing_date.desc())
                
                return list(session.exec(statement).all())
        except Exception as e:
            logger.error(f"Failed to get pending downloads: {e}")
            return []

    def update_download_status(
        self,
        report_id: int,
        status: str,
        file_paths: Optional[dict] = None,
        error_message: Optional[str] = None,
    ) -> bool:
        """
        Update download status for a report.

        Args:
            report_id: Report ID
            status: New status (pending, downloaded, failed)
            file_paths: Optional file paths dictionary
            error_message: Optional error message

        Returns:
            True if successful, False otherwise
        """
        try:
            with self.db_manager.get_session() as session:
                report = session.get(SECReport, report_id)
                if report:
                    report.download_status = status
                    report.updated_at = datetime.now()
                    
                    if file_paths:
                        report.file_paths = file_paths
                    
                    if error_message:
                        report.error_message = error_message
                    
                    session.add(report)
                    session.commit()
                    return True
                return False
        except Exception as e:
            logger.error(f"Failed to update download status for report {report_id}: {e}")
            return False

    def update_processing_status(
        self,
        report_id: int,
        status: str,
        error_message: Optional[str] = None,
    ) -> bool:
        """
        Update processing status for a report.

        Args:
            report_id: Report ID
            status: New status (pending, processed, failed)
            error_message: Optional error message

        Returns:
            True if successful, False otherwise
        """
        try:
            with self.db_manager.get_session() as session:
                report = session.get(SECReport, report_id)
                if report:
                    report.processing_status = status
                    report.updated_at = datetime.now()
                    report.last_processed_at = datetime.now()
                    
                    if error_message:
                        report.error_message = error_message
                    
                    session.add(report)
                    session.commit()
                    return True
                return False
        except Exception as e:
            logger.error(f"Failed to update processing status for report {report_id}: {e}")
            return False

    def get_reports_stats(self) -> dict:
        """
        Get statistics about SEC reports in the database.

        Returns:
            Dictionary with report statistics
        """
        try:
            with self.db_manager.get_session() as session:
                # Total reports by form type
                all_reports = list(session.exec(select(SECReport)).all())
                
                stats = {
                    "total_reports": len(all_reports),
                    "by_form_type": {},
                    "by_download_status": {},
                    "by_processing_status": {},
                }
                
                for report in all_reports:
                    # Count by form type
                    stats["by_form_type"][report.form_type] = (
                        stats["by_form_type"].get(report.form_type, 0) + 1
                    )
                    
                    # Count by download status
                    stats["by_download_status"][report.download_status] = (
                        stats["by_download_status"].get(report.download_status, 0) + 1
                    )
                    
                    # Count by processing status
                    stats["by_processing_status"][report.processing_status] = (
                        stats["by_processing_status"].get(report.processing_status, 0) + 1
                    )
                
                return stats
        except Exception as e:
            logger.error(f"Failed to get reports stats: {e}")
            return {"total_reports": 0, "by_form_type": {}, "by_download_status": {}, "by_processing_status": {}}

    def get_latest_report_by_form(
        self, series_id: str, form_type: str
    ) -> Optional[SECReport]:
        """
        Get the latest report for a series and form type.

        Args:
            series_id: Series ID
            form_type: Form type (NPORT-P, 13F, etc.)

        Returns:
            Latest SECReport or None if not found
        """
        try:
            with self.db_manager.get_session() as session:
                statement = (
                    select(SECReport)
                    .where(
                        SECReport.series_id == series_id,
                        SECReport.form_type == form_type,
                    )
                    .order_by(SECReport.report_date.desc())
                )
                
                return session.exec(statement).first()
        except Exception as e:
            logger.error(f"Failed to get latest report for {series_id} {form_type}: {e}")
            return None
