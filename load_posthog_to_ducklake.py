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
import json
import os
import sys
from datetime import datetime

from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    to_timestamp,
)
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def load_config():
    """
    Load configuration from either AWS Secrets Manager or .env file.

    If SECRET_NAME environment variable is set, load from Secrets Manager.
    Otherwise, fall back to .env file for local development.
    """
    secret_name = os.getenv("SECRET_NAME")

    if secret_name:
        # Load from AWS Secrets Manager (for EMR Serverless)
        logger.info(f"Loading configuration from Secrets Manager: {secret_name}")
        try:
            import boto3
            region = os.getenv("AWS_REGION", "us-east-1")
            client = boto3.client("secretsmanager", region_name=region)
            response = client.get_secret_value(SecretId=secret_name)
            secret_string = response["SecretString"]

            # Try to parse as JSON (structured secret)
            try:
                secrets = json.loads(secret_string)
                # If JSON, set all key-value pairs as env vars
                for key, value in secrets.items():
                    os.environ[key] = str(value)
                logger.success(f"Configuration loaded from Secrets Manager (JSON format)")
            except json.JSONDecodeError:
                # If not JSON, assume it's just the password
                os.environ["POSTGRES_PASSWORD"] = secret_string
                logger.success(f"Password loaded from Secrets Manager (plaintext format)")

                # Also load .env for other configs
                load_dotenv()

        except Exception as e:
            logger.error(f"Failed to load secrets from Secrets Manager: {e}")
            raise
    else:
        # Load from .env file (for local development)
        load_dotenv()


# Load configuration
load_config()

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
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        # S3A timeout configurations (in milliseconds, not with 's' suffix)
        .config("spark.hadoop.fs.s3a.connection.timeout", "200000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.retry.limit", "10")
        .config("spark.hadoop.fs.s3a.retry.interval", "500")
        .config("spark.hadoop.fs.s3a.timeout.establish", "60000")
        .config("spark.hadoop.fs.s3a.timeout.socket", "200000")
        # Thread pool configuration (keepalivetime is the likely culprit for "60s" error)
        .config("spark.hadoop.fs.s3a.threads.max", "50")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")
        .config("spark.hadoop.fs.s3a.max.total.tasks", "50")
        # Multipart upload configuration (purge.age defaults to "24h" string)
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600")  # 100MB
        .config("spark.hadoop.fs.s3a.multipart.threshold", "104857600")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")  # 24h in ms
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

    Returns (path, use_recursive) tuple.
    Path format: s3a://bucket/prefix/{team_id}/{year}/{month}/{day}/{hour}/

    For broad queries (no team_id or only team_id), use directory path + recursive lookup.
    For specific queries (month/day/hour), use wildcard patterns.
    """
    path_parts = [f"s3a://{S3_BUCKET}", S3_PREFIX.rstrip("/")]
    use_recursive = False

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

    # For specific hour/day, use wildcard patterns
    # For broader queries, use directory + recursive lookup
    if hour:
        s3_path += "/*.log.zst"
    elif day:
        s3_path += "/*/*.log.zst"
    elif month:
        s3_path += "/*/*/*.log.zst"
    else:
        # For year, team_id only, or no filters: use recursive lookup
        use_recursive = True
        # Just use the directory path, no wildcards
        if not s3_path.endswith("/"):
            s3_path += "/"

    return s3_path, use_recursive


def load_data_from_s3(spark, s3_path, use_recursive=False, dry_run=False):
    """
    Load PostHog event data from S3.

    The data is stored as Zstd-compressed newline-delimited JSON.
    """
    logger.info(f"Loading data from: {s3_path}")

    if dry_run:
        logger.info("DRY RUN: Would load data from the above path")
        if use_recursive:
            logger.info("DRY RUN: Using recursive file lookup for .log.zst files")
        return None

    try:
        # Read JSON files (Spark automatically handles .zst compression)
        if use_recursive:
            # For broad queries, use recursive lookup with path glob filter
            df = (
                spark.read.option("recursiveFileLookup", "true")
                .option("pathGlobFilter", "*.log.zst")
                .json(s3_path)
            )
        else:
            # For specific paths with wildcards
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
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be loaded without loading"
    )

    args = parser.parse_args()

    logger.info("PostHog to DuckLake Data Loader")
    logger.info("=" * 50)

    # Build S3 path
    s3_path, use_recursive = build_s3_path(
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
        df = load_data_from_s3(spark, s3_path, use_recursive=use_recursive, dry_run=args.dry_run)

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
