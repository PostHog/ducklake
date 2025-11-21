#!/usr/bin/env python3
"""
Load PostHog event data from S3 into DuckLake using Apache Spark.

This script reads Zstd-compressed newline-delimited JSON files from S3,
processes them with Spark, and writes to DuckLake via JDBC.

Usage:
    # Load all data
    uv run python load_posthog_to_ducklake.py

    # Load specific partition
    uv run python load_posthog_to_ducklake.py --team-id 2 --year 2025 --month 5

    # Dry run (show what would be loaded)
    uv run python load_posthog_to_ducklake.py --dry-run
"""

import argparse
import os
import sys
from datetime import datetime

from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    current_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)

# Load environment variables
load_dotenv()

# Configuration from environment
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "ducklake")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB", "ducklake")
DUCKLAKE_DATA_PATH = os.getenv("DUCKLAKE_DATA_PATH", "/tmp/ducklake_data")

S3_BUCKET = os.getenv("S3_BUCKET", "your-s3-bucket-name")
S3_PREFIX = os.getenv("S3_PREFIX", "events/")

SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "4g")
SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")
SPARK_EXECUTOR_CORES = os.getenv("SPARK_EXECUTOR_CORES", "2")

# Validate required environment variables
if not POSTGRES_PASSWORD:
    logger.error("POSTGRES_PASSWORD environment variable is required")
    sys.exit(1)

# JDBC URL for DuckLake
JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


def create_spark_session():
    """Create and configure Spark session with DuckLake and S3 support."""
    logger.info("Creating Spark session")

    spark = (
        SparkSession.builder.appName("PostHog-to-DuckLake")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
        .config("spark.executor.cores", SPARK_EXECUTOR_CORES)
        # S3 configuration
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        # Add PostgreSQL JDBC driver
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.3,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .getOrCreate()
    )

    logger.success("Spark session created successfully")
    return spark


def build_s3_path(team_id=None, year=None, month=None, day=None, hour=None):
    """
    Build S3 path based on partition parameters.

    Path format: s3a://bucket/prefix/{team_id}/{year}/{month}/{day}/{hour}/*.log.zst
    """
    path_parts = [f"s3a://{S3_BUCKET}", S3_PREFIX.rstrip("/")]

    if team_id:
        path_parts.append(str(team_id))
        if year:
            path_parts.append(str(year))
            if month:
                path_parts.append(f"{month:02d}")
                if day:
                    path_parts.append(f"{day:02d}")
                    if hour:
                        path_parts.append(f"{hour:02d}")

    s3_path = "/".join(path_parts)

    # Add wildcard pattern for files
    if hour:
        s3_path += "/*.log.zst"
    elif day:
        s3_path += "/*/*.log.zst"
    elif month:
        s3_path += "/*/*/*.log.zst"
    elif year:
        s3_path += "/*/*/*/*.log.zst"
    elif team_id:
        s3_path += "/*/*/*/*/*.log.zst"
    else:
        s3_path += "*/*/*/*/*/*.log.zst"

    return s3_path


def load_data_from_s3(spark, s3_path, dry_run=False):
    """
    Load PostHog event data from S3.

    The data is stored as Zstd-compressed newline-delimited JSON.
    """
    logger.info(f"Loading data from: {s3_path}")

    if dry_run:
        logger.info("DRY RUN: Would load data from the above path")
        return None

    try:
        # Read JSON files (Spark automatically handles .zst compression)
        df = spark.read.json(s3_path)

        row_count = df.count()
        logger.info(f"Loaded {row_count:,} events from S3")

        # Show sample
        logger.info("Sample data:")
        df.show(5, truncate=True)

        return df

    except Exception as e:
        logger.error(f"Error loading data from S3: {e}")
        raise


def transform_data(df):
    """
    Transform the data to match DuckLake schema.

    Key transformations:
    - Keep properties, person_properties, and group*_properties as JSON
    - Convert timestamp strings to TIMESTAMP type
    - Ensure all columns match the target schema
    """
    logger.info("Transforming data")

    # The schema should already mostly match, but we ensure types are correct
    # Properties columns are already JSON/struct types from Spark's JSON reader
    transformed_df = df.select(
        col("uuid"),
        col("event"),
        col("properties"),  # Keep as JSON
        to_timestamp(col("timestamp")).alias("timestamp"),
        col("team_id").cast(IntegerType()),
        col("distinct_id"),
        col("elements_chain"),
        to_timestamp(col("created_at")).alias("created_at"),
        col("person_id"),
        to_timestamp(col("person_created_at")).alias("person_created_at"),
        col("person_properties"),  # Keep as JSON
        col("group0_properties"),  # Keep as JSON
        col("group1_properties"),  # Keep as JSON
        col("group2_properties"),  # Keep as JSON
        col("group3_properties"),  # Keep as JSON
        col("group4_properties"),  # Keep as JSON
        to_timestamp(col("group0_created_at")).alias("group0_created_at"),
        to_timestamp(col("group1_created_at")).alias("group1_created_at"),
        to_timestamp(col("group2_created_at")).alias("group2_created_at"),
        to_timestamp(col("group3_created_at")).alias("group3_created_at"),
        to_timestamp(col("group4_created_at")).alias("group4_created_at"),
        col("person_mode"),
    )

    logger.success("Data transformation complete")
    return transformed_df


def write_to_ducklake(df, dry_run=False):
    """
    Write data to DuckLake via JDBC.

    DuckLake will handle storing the data as Parquet files and
    managing metadata in PostgreSQL.
    """
    logger.info("Writing data to DuckLake")

    if dry_run:
        logger.info("DRY RUN: Would write data to DuckLake")
        logger.info(f"Target table: ducklake.main.posthog_events")
        logger.info(f"Row count: {df.count():,}")
        return

    try:
        # Write to DuckLake via JDBC
        # Note: Adjust mode based on your needs:
        # - "append": Add to existing data
        # - "overwrite": Replace all data (use with caution!)
        # - "error": Fail if table exists
        df.write.format("jdbc").options(
            url=JDBC_URL,
            dbtable="ducklake.main.posthog_events",
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            driver="org.postgresql.Driver",
        ).mode("append").save()

        logger.success(f"Successfully wrote {df.count():,} events to DuckLake")

    except Exception as e:
        logger.error(f"Error writing to DuckLake: {e}")
        raise


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Load PostHog events from S3 to DuckLake using Spark"
    )
    parser.add_argument("--team-id", type=int, help="Filter by team ID")
    parser.add_argument("--year", type=int, help="Filter by year")
    parser.add_argument("--month", type=int, help="Filter by month (1-12)")
    parser.add_argument("--day", type=int, help="Filter by day (1-31)")
    parser.add_argument("--hour", type=int, help="Filter by hour (0-23)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be loaded without loading")

    args = parser.parse_args()

    logger.info("PostHog to DuckLake Data Loader")
    logger.info("=" * 50)

    # Build S3 path
    s3_path = build_s3_path(
        team_id=args.team_id,
        year=args.year,
        month=args.month,
        day=args.day,
        hour=args.hour,
    )

    # Create Spark session
    spark = create_spark_session()

    try:
        # Load data from S3
        df = load_data_from_s3(spark, s3_path, dry_run=args.dry_run)

        if df is not None:
            # Transform data
            transformed_df = transform_data(df)

            # Write to DuckLake
            write_to_ducklake(transformed_df, dry_run=args.dry_run)

        logger.success("Data loading process completed")

    except Exception as e:
        logger.error(f"Data loading failed: {e}")
        sys.exit(1)

    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
