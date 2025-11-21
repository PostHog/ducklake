#!/usr/bin/env python3
"""
Bootstrap DuckLake catalog and create PostHog events table schema.

This script performs one-time setup:
1. Creates DuckLake catalog in PostgreSQL
2. Creates the posthog_events table with JSON columns for properties
3. Sets up partitioning if needed

Run this once before loading data.
"""

import os
import sys
from pathlib import Path

import duckdb
from dotenv import load_dotenv
from loguru import logger

# Load environment variables
load_dotenv()

# Configuration from environment
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "ducklake")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB", "ducklake")
DUCKLAKE_DATA_PATH = os.getenv("DUCKLAKE_DATA_PATH", "/tmp/ducklake_data")

# Validate required environment variables
if not POSTGRES_PASSWORD:
    logger.error("POSTGRES_PASSWORD environment variable is required")
    sys.exit(1)

# PostgreSQL connection string for DuckLake
POSTGRES_CONN = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
    f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# PostHog events table schema
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS posthog_events (
    uuid VARCHAR,
    event VARCHAR,
    properties JSON,
    timestamp TIMESTAMP,
    team_id INTEGER,
    distinct_id VARCHAR,
    elements_chain VARCHAR,
    created_at TIMESTAMP,
    person_id VARCHAR,
    person_created_at TIMESTAMP,
    person_properties JSON,
    group0_properties JSON,
    group1_properties JSON,
    group2_properties JSON,
    group3_properties JSON,
    group4_properties JSON,
    group0_created_at TIMESTAMP,
    group1_created_at TIMESTAMP,
    group2_created_at TIMESTAMP,
    group3_created_at TIMESTAMP,
    group4_created_at TIMESTAMP,
    person_mode VARCHAR
)
"""


def setup_ducklake_catalog():
    """Set up DuckLake catalog and create the PostHog events table."""
    logger.info("Starting DuckLake bootstrap process")

    # Ensure data directory exists
    data_path = Path(DUCKLAKE_DATA_PATH)
    if not str(data_path).startswith("s3://"):
        data_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created data directory: {data_path}")

    try:
        # Connect to DuckDB with DuckLake catalog
        logger.info("Connecting to DuckLake catalog")
        con = duckdb.connect()

        # Attach DuckLake catalog
        logger.info(f"Attaching DuckLake catalog with PostgreSQL at {POSTGRES_HOST}")
        con.execute(
            f"""
            ATTACH '{POSTGRES_CONN}' AS ducklake (TYPE ducklake, DATA_PATH '{DUCKLAKE_DATA_PATH}')
            """
        )

        # Switch to ducklake catalog
        con.execute("USE ducklake")

        # Create main schema if it doesn't exist
        logger.info("Creating main schema")
        con.execute("CREATE SCHEMA IF NOT EXISTS main")
        con.execute("USE main")

        # Create posthog_events table
        logger.info("Creating posthog_events table")
        con.execute(CREATE_TABLE_SQL)

        # Verify table creation
        result = con.execute(
            """
            SELECT COUNT(*) as count
            FROM information_schema.tables
            WHERE table_name = 'posthog_events'
            """
        ).fetchone()

        if result and result[0] > 0:
            logger.success("Successfully created posthog_events table")

            # Show table schema
            schema = con.execute("DESCRIBE posthog_events").fetchall()
            logger.info("Table schema:")
            for col in schema:
                logger.info(f"  {col[0]}: {col[1]}")
        else:
            logger.error("Failed to create posthog_events table")
            sys.exit(1)

        con.close()
        logger.success("DuckLake bootstrap completed successfully")

    except Exception as e:
        logger.error(f"Error during bootstrap: {e}")
        sys.exit(1)


if __name__ == "__main__":
    logger.info("PostHog DuckLake Bootstrap")
    logger.info("=" * 50)
    setup_ducklake_catalog()
