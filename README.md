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

## Running at Scale

For petabyte-scale data loading, running on a single machine is not practical. Use AWS managed Spark services for production workloads.

### Option 1: EMR (Recommended for Cost & Performance)

**Best for**: Large-scale loads (100GB+), cost optimization, full Spark control

**Pros:**
- Most cost-effective (60-80% cheaper with spot instances)
- Full control over Spark configuration
- Best S3 performance in same region
- Handles very long-running jobs
- Dynamic cluster scaling

**Example - Create EMR Cluster:**

```bash
aws emr create-cluster \
  --name "PostHog-DuckLake-Loader" \
  --release-label emr-7.0.0 \
  --applications Name=Spark \
  --instance-groups \
    InstanceGroupType=MASTER,InstanceType=m5.xlarge,InstanceCount=1 \
    InstanceGroupType=CORE,InstanceType=r5.4xlarge,InstanceCount=20,BidPrice=OnDemandPrice \
  --ec2-attributes KeyName=mykey,SubnetId=subnet-xxx \
  --use-default-roles \
  --log-uri s3://my-bucket/emr-logs/ \
  --bootstrap-actions Path=s3://my-bucket/bootstrap.sh
```

**Submit Job to EMR:**

```bash
# Load specific team and month
aws emr add-steps \
  --cluster-id j-xxxxx \
  --steps Type=Spark,Name="Load Team 2 - May 2025",ActionOnFailure=CONTINUE,Args=[
    --deploy-mode,cluster,
    --conf,spark.driver.memory=16g,
    --conf,spark.executor.memory=32g,
    --conf,spark.executor.cores=8,
    s3://my-bucket/load_posthog_to_ducklake.py,
    --team-id,2,
    --year,2025,
    --month,5
  ]
```

**Cost Estimate (EMR):**
- Master: 1x m5.2xlarge on-demand: ~$0.38/hr
- Workers: 20x r5.4xlarge spot (70% discount): ~$2-3/hr total
- **Total: ~$3-4/hour** for a cluster that can process TBs/hour

### Option 2: EMR Serverless (Easiest)

**Best for**: Intermittent loads, no cluster management

**Pros:**
- Fully serverless - no cluster management
- Auto-scales based on workload
- Standard Spark (not Glue's fork)
- Pay only for what you use

**Cons:**
- Slightly more expensive than EMR clusters
- Cold start time (~1-2 minutes)

**Example:**

```bash
# Create application (one time)
aws emr-serverless create-application \
  --name posthog-ducklake-loader \
  --type SPARK \
  --release-label emr-7.0.0

# Submit job
aws emr-serverless start-job-run \
  --application-id app-xxxxx \
  --execution-role-arn arn:aws:iam::xxx:role/EMRServerlessRole \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://my-bucket/load_posthog_to_ducklake.py",
      "entryPointArguments": ["--team-id", "2", "--year", "2025", "--month", "5"],
      "sparkSubmitParameters": "--conf spark.executor.memory=32g --conf spark.executor.cores=8"
    }
  }' \
  --configuration-overrides '{
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": {
        "logUri": "s3://my-bucket/emr-serverless-logs/"
      }
    }
  }'
```

### Option 3: AWS Glue

**Best for**: Small-to-medium loads (<10TB), AWS Glue ecosystem integration

**Pros:**
- Fully managed
- Integrated with Glue Data Catalog
- Simple setup

**Cons:**
- More expensive at scale
- Uses modified Spark (some configs may not work)
- Less control over Spark tuning

### Parallel Loading Strategy

For maximum throughput, load data in parallel by team and date partitions:

**Shell Script Approach:**

```bash
#!/bin/bash
# load_parallel.sh - Load multiple partitions in parallel

TEAMS=(2 5 10 42)
YEAR=2025

for team_id in "${TEAMS[@]}"; do
  for month in {1..12}; do
    # Submit each job to EMR (they'll run in parallel)
    aws emr add-steps \
      --cluster-id j-xxxxx \
      --steps Type=Spark,Name="Load Team $team_id - $YEAR-$month",ActionOnFailure=CONTINUE,Args=[
        s3://bucket/load_posthog_to_ducklake.py,
        --team-id,$team_id,
        --year,$YEAR,
        --month,$month
      ]
  done
done
```

**Python Approach (Local Orchestration):**

```python
# run_parallel_loads.py
import subprocess
import concurrent.futures
from datetime import datetime

TEAMS = [2, 5, 10, 42]  # Your team IDs
YEAR = 2025
MONTHS = range(1, 13)

def load_team_month(team_id, year, month):
    """Load data for a specific team and month."""
    cmd = [
        "uv", "run", "python", "load_posthog_to_ducklake.py",
        "--team-id", str(team_id),
        "--year", str(year),
        "--month", str(month)
    ]

    print(f"Starting: Team {team_id}, {year}-{month:02d}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode == 0:
        print(f"✓ Completed: Team {team_id}, {year}-{month:02d}")
    else:
        print(f"✗ Failed: Team {team_id}, {year}-{month:02d}")
        print(result.stderr)

    return (team_id, year, month, result.returncode)

# Run in parallel (adjust max_workers based on cluster size)
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    futures = []
    for team_id in TEAMS:
        for month in MONTHS:
            future = executor.submit(load_team_month, team_id, YEAR, month)
            futures.append(future)

    # Wait for all to complete
    results = [f.result() for f in concurrent.futures.as_completed(futures)]

# Summary
failed = [r for r in results if r[3] != 0]
print(f"\n{'='*50}")
print(f"Total: {len(results)}, Succeeded: {len(results)-len(failed)}, Failed: {len(failed)}")
```

### Cost Comparison (1 PB of data)

| Service | Configuration | Estimated Cost | Duration |
|---------|--------------|----------------|----------|
| **EMR Spot** | 1 master + 20 workers (spot) | $500-1,000 | 10-20 hrs |
| **EMR On-Demand** | 1 master + 20 workers | $2,000-3,000 | 10-20 hrs |
| **EMR Serverless** | Auto-scaled | $1,500-2,500 | 10-20 hrs |
| **AWS Glue** | DPU-based pricing | $3,000-5,000 | 15-30 hrs |

*Estimates vary based on data characteristics, cluster configuration, and region*

### Production Best Practices

1. **Incremental Loading**: Load by team_id and date partitions to handle failures gracefully
2. **Idempotency**: Use DuckLake's upsert capabilities or load to staging tables first
3. **Monitoring**: Enable CloudWatch metrics and S3/Spark logs
4. **Spot Instances**: Use spot instances for EMR workers (60-80% cost savings)
5. **Same Region**: Run EMR in the same AWS region as your S3 bucket
6. **Checkpointing**: For very large loads, consider Spark checkpointing to recover from failures

### Scheduling with Airflow/Step Functions

**Airflow DAG Example:**

```python
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG('posthog_ducklake_daily',
         default_args=default_args,
         schedule_interval='@daily',
         start_date=datetime(2025, 1, 1)) as dag:

    load_step = EmrAddStepsOperator(
        task_id='load_posthog_data',
        job_flow_id='j-xxxxx',  # Your EMR cluster ID
        steps=[{
            'Name': 'Load PostHog to DuckLake',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    's3://bucket/load_posthog_to_ducklake.py',
                    '--team-id', '{{ var.value.team_id }}',
                    '--year', '{{ ds_nodash[:4] }}',
                    '--month', '{{ ds_nodash[4:6] }}',
                    '--day', '{{ ds_nodash[6:8] }}'
                ]
            }
        }]
    )
```

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
