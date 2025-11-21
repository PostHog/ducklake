# PostHog S3 to DuckLake Loader

Load petabyte-scale PostHog event data from S3 into DuckLake using Apache Spark.

## Overview

This project provides tools to efficiently load large-scale PostHog event data stored in S3 into DuckLake for fast analytical queries. DuckDB alone is too slow for petabyte-scale data, so we use Apache Spark for parallel data loading.

### Architecture

```
S3 (zstd JSON) → Spark (parallel read) → DuckLake (JDBC write) → DuckDB (query)
```

- **Source**: S3 bucket with Zstd-compressed newline-delimited JSON
- **Processing**: Apache Spark for parallel data loading
- **Storage**: DuckLake with PostgreSQL metadata + Parquet data files
- **Query**: DuckDB for fast analytical queries

### Key Design Decisions

- **JSON columns**: `properties`, `person_properties`, and `group*_properties` are stored as JSON type (not exploded), because these fields contain hundreds of dynamic keys that vary by event type
- **Partitioning**: Source data is partitioned by `{team_id}/{yyyy}/{mm}/{dd}/{hh}/*.log.zst`
- **Parallel loading**: Spark enables efficient parallel reads from S3

## Prerequisites

- Python 3.10+
- PostgreSQL (for DuckLake metadata)
- AWS credentials with S3 read access
- UV (recommended) or pip for dependency management

## Setup

### 1. Install Dependencies

Using UV (recommended):

```bash
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install -e .
```

Using pip:

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e .
```

### 2. Configure Environment

Copy the example environment file and edit it:

```bash
cp .env.example .env
```

Edit `.env` with your settings:

```bash
# PostgreSQL for DuckLake metadata
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=ducklake
POSTGRES_PASSWORD=your_secure_password
POSTGRES_DB=ducklake

# DuckLake data storage
# Local: /path/to/local/ducklake_data
# S3: s3://your-bucket/ducklake_data
DUCKLAKE_DATA_PATH=/tmp/ducklake_data

# PostHog S3 source
S3_BUCKET=your-s3-bucket-name
S3_PREFIX=events/

# AWS credentials (if not using IAM role)
# AWS_ACCESS_KEY_ID=your_key
# AWS_SECRET_ACCESS_KEY=your_secret
# AWS_REGION=us-east-1

# Spark configuration
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g
SPARK_EXECUTOR_CORES=2
```

### 3. Set Up PostgreSQL

Create a PostgreSQL database for DuckLake metadata:

```sql
CREATE DATABASE ducklake;
CREATE USER ducklake WITH PASSWORD 'your_secure_password';
GRANT ALL PRIVILEGES ON DATABASE ducklake TO ducklake;
```

Or use a managed service like Supabase.

### 4. Bootstrap DuckLake

Run the bootstrap script once to create the catalog and table schema:

```bash
uv run python bootstrap_ducklake.py
```

This will:
- Create the DuckLake catalog in PostgreSQL
- Create the `posthog_events` table with the correct schema
- Set up the data directory

## Usage

### Load All Data

Load all available PostHog events from S3:

```bash
uv run python load_posthog_to_ducklake.py
```

### Load Specific Partitions

Load data for a specific team:

```bash
uv run python load_posthog_to_ducklake.py --team-id 2
```

Load data for a specific month:

```bash
uv run python load_posthog_to_ducklake.py --team-id 2 --year 2025 --month 5
```

Load data for a specific day:

```bash
uv run python load_posthog_to_ducklake.py --team-id 2 --year 2025 --month 5 --day 15
```

Load data for a specific hour:

```bash
uv run python load_posthog_to_ducklake.py --team-id 2 --year 2025 --month 5 --day 15 --hour 14
```

### Dry Run

Preview what would be loaded without actually loading:

```bash
uv run python load_posthog_to_ducklake.py --dry-run
uv run python load_posthog_to_ducklake.py --team-id 2 --year 2025 --month 5 --dry-run
```

## Schema

The `posthog_events` table schema:

```sql
CREATE TABLE posthog_events (
    uuid VARCHAR,
    event VARCHAR,
    properties JSON,                -- Dynamic event properties
    timestamp TIMESTAMP,
    team_id INTEGER,
    distinct_id VARCHAR,
    elements_chain VARCHAR,
    created_at TIMESTAMP,
    person_id VARCHAR,
    person_created_at TIMESTAMP,
    person_properties JSON,         -- Dynamic person properties
    group0_properties JSON,         -- Dynamic group properties
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
);
```

## Querying Data

After loading data, query it using DuckDB:

```python
import duckdb

# Connect to DuckLake
con = duckdb.connect()
con.execute("""
    ATTACH 'postgresql://user:pass@host:5432/ducklake'
    AS ducklake (TYPE ducklake, DATA_PATH '/path/to/ducklake_data')
""")

# Query events
result = con.execute("""
    SELECT
        event,
        COUNT(*) as event_count,
        COUNT(DISTINCT distinct_id) as unique_users
    FROM ducklake.main.posthog_events
    WHERE team_id = 2
        AND timestamp >= '2025-05-01'
        AND timestamp < '2025-06-01'
    GROUP BY event
    ORDER BY event_count DESC
    LIMIT 10
""").fetchall()

for row in result:
    print(f"{row[0]}: {row[1]:,} events, {row[2]:,} users")
```

## Performance Tuning

### Spark Configuration

Adjust Spark settings in `.env` based on your hardware:

```bash
# For larger machines
SPARK_DRIVER_MEMORY=8g
SPARK_EXECUTOR_MEMORY=16g
SPARK_EXECUTOR_CORES=4
```

### Batch Loading

Load data incrementally by partition to avoid overwhelming your system:

```bash
# Load one day at a time
for day in {1..31}; do
    uv run python load_posthog_to_ducklake.py --team-id 2 --year 2025 --month 5 --day $day
done
```

### S3 Performance

For better S3 performance, consider:
- Using an EC2 instance in the same region as your S3 bucket
- Configuring S3A settings for faster reads
- Using AWS IAM roles instead of access keys

## Troubleshooting

### Out of Memory Errors

Increase Spark memory settings or load smaller partitions:

```bash
SPARK_DRIVER_MEMORY=16g SPARK_EXECUTOR_MEMORY=32g uv run python load_posthog_to_ducklake.py
```

### Slow S3 Reads

- Ensure you're in the same AWS region as the S3 bucket
- Check your network bandwidth
- Consider using S3 Transfer Acceleration

### JDBC Connection Issues

Verify PostgreSQL is accessible and credentials are correct:

```bash
psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB
```

## References

- [DuckLake + Spark Tutorial](https://motherduck.com/blog/spark-ducklake-getting-started/)
- [Example Repo](https://github.com/mehd-io/tutorial-spark-ducklake)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)

## License

MIT
