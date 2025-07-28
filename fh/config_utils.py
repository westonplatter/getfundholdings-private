#!/usr/bin/env python3
"""
Configuration utilities for the fund holdings pipeline.

This module provides centralized configuration loading and environment
management functions used across multiple components.
"""

import os
from typing import Optional

from dotenv import load_dotenv
from loguru import logger


def get_database_url_from_env() -> str:
    """
    Get database URL from environment variables with multi-environment support.
    
    Supports both development and production environments via ENVIRONMENT variable.
    Loads appropriate .env.{environment} file if ENVIRONMENT is set to 'dev' or 'prod'.
    
    Environment variable priority:
    1. SUPABASE_DATABASE_URL
    2. DATABASE_URL
    
    Returns:
        Database URL string
        
    Raises:
        ValueError: If no database URL is found in environment variables
    """
    # Load environment-specific config if ENVIRONMENT is set
    environment = os.getenv("ENVIRONMENT", "")
    if environment in ["dev", "prod"]:
        env_file = f".env.{environment}"
        if os.path.exists(env_file):
            load_dotenv(env_file, override=True)
            logger.info(f"Loaded environment config: {env_file}")
    else:
        load_dotenv()

    # Try SUPABASE_DATABASE_URL first, then fall back to DATABASE_URL
    database_url = os.getenv("SUPABASE_DATABASE_URL") or os.getenv("DATABASE_URL")

    if not database_url:
        raise ValueError(
            "No database URL found. Set SUPABASE_DATABASE_URL or DATABASE_URL environment variable."
        )

    # Fix for SQLAlchemy 2.x: replace postgres:// with postgresql://
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
        logger.debug("Converted postgres:// to postgresql:// for SQLAlchemy 2.x compatibility")

    return database_url


def load_environment_config(environment: Optional[str] = None) -> None:
    """
    Load environment-specific configuration files.
    
    Args:
        environment: Specific environment to load ('dev', 'prod'), 
                    or None to use ENVIRONMENT variable
    """
    if environment is None:
        environment = os.getenv("ENVIRONMENT", "")
    
    if environment in ["dev", "prod"]:
        env_file = f".env.{environment}"
        if os.path.exists(env_file):
            load_dotenv(env_file, override=True)
            logger.info(f"Loaded environment config: {env_file}")
        else:
            logger.warning(f"Environment config file not found: {env_file}")
    else:
        load_dotenv()
        if environment:
            logger.warning(f"Unknown environment '{environment}', loaded default .env")


def get_r2_credentials(environment: str = "prod") -> dict:
    """
    Get R2 (Cloudflare) credentials for specified environment.
    
    Args:
        environment: Environment name ('dev' or 'prod')
        
    Returns:
        Dictionary with R2 credentials
        
    Raises:
        ValueError: If required credentials are missing
    """
    load_environment_config(environment)
    
    credentials = {
        "account_id": os.getenv("R2_ACCOUNT_ID"),
        "access_key_id": os.getenv("R2_ACCESS_KEY_ID"), 
        "secret_access_key": os.getenv("R2_SECRET_ACCESS_KEY"),
        "bucket_name": os.getenv("R2_BUCKET_NAME"),
        "endpoint_url": os.getenv("R2_ENDPOINT_URL"),
    }
    
    missing_keys = [key for key, value in credentials.items() if not value]
    if missing_keys:
        raise ValueError(f"Missing R2 credentials for {environment}: {missing_keys}")
    
    return credentials


def get_openfigi_api_key() -> Optional[str]:
    """
    Get OpenFIGI API key from environment variables.
    
    Returns:
        API key string or None if not found
    """
    return os.getenv("OPENFIGI_API_KEY")


def get_sec_user_agent() -> str:
    """
    Get SEC-compliant User-Agent string from environment or use default.
    
    Returns:
        User-Agent string
    """
    return os.getenv("SEC_USER_AGENT", "GetFundHoldings.com admin@getfundholdings.com")