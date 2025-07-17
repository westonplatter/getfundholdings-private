#!/usr/bin/env python3
"""
Database models and services for security mappings cache.

This module provides SQLModel-based database models and services for caching
CUSIP/ISIN to ticker mappings from the OpenFIGI API.
"""

from datetime import datetime, timedelta
from typing import List, Optional

from sqlmodel import Field, Session, SQLModel, create_engine, select
from loguru import logger


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
        self.engine = create_engine(database_url)
        # Note: We don't create tables here since we use Alembic migrations
    
    def get_session(self) -> Session:
        """Get a database session context manager."""
        return Session(self.engine)


class SecurityMappingService:
    """Service for CRUD operations on security mappings."""
    
    def __init__(self, db_manager: DatabaseManager):
        """Initialize service with database manager."""
        self.db_manager = db_manager
    
    def get_active_mapping(self, identifier_type: str, identifier_value: str) -> Optional[SecurityMapping]:
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
                    SecurityMapping.end_date.is_(None)
                )
                return session.exec(statement).first()
        except Exception as e:
            logger.warning(f"Failed to get active mapping for {identifier_type} {identifier_value}: {e}")
            return None
    
    def create_or_update_mapping(
        self, 
        identifier_type: str, 
        identifier_value: str, 
        ticker: Optional[str], 
        has_no_results: bool = False
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
                        has_no_results=has_no_results
                    )
                    session.add(mapping)
                
                session.commit()
                session.refresh(mapping)
                return mapping
                
        except Exception as e:
            logger.warning(f"Failed to create/update mapping for {identifier_type} {identifier_value}: {e}")
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
                    SecurityMapping.end_date.is_(None)
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
                    SecurityMapping.end_date.is_(None)
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
            logger.warning(f"Failed to invalidate mapping for {identifier_type} {identifier_value}: {e}")
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
                    SecurityMapping.ticker.is_not(None)
                )
                successful = len(list(session.exec(success_statement).all()))
                
                # Failed mappings (no results)
                failed_statement = select(SecurityMapping).where(
                    SecurityMapping.end_date.is_(None),
                    SecurityMapping.has_no_results == True
                )
                failed = len(list(session.exec(failed_statement).all()))
                
                return {
                    "total_cached": total,
                    "found_cached": successful,
                    "not_found_cached": failed
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