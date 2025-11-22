# EMR Serverless Setup Instructions

## Overview

This setup uses AWS Secrets Manager to securely store credentials. The Spark job retrieves secrets at runtime, so no credentials are in code or environment files.

## Prerequisites

1. AWS CLI configured with credentials
2. Permissions to create IAM roles, Secrets Manager secrets, and EMR Serverless applications
3. S3 bucket for scripts and logs
4. PostgreSQL database for DuckLake metadata

## Setup Steps

### 1. Run the Setup Script

```bash
./setup_emr_serverless.sh
```

This script will:
- Prompt for your S3 bucket name (where scripts and logs will be stored)
- Prompt for your Secrets Manager secret name (for PostgreSQL password)
- Upload `load_posthog_to_ducklake.py` to S3
- Create an IAM role with necessary permissions
- Verify your Secrets Manager secret exists
- Create an EMR Serverless application
- Generate a `run_emr_job.sh` helper script

### 2. Verify Secret Exists

```bash
aws secretsmanager get-secret-value --secret-id YOUR_SECRET_NAME --region us-east-1
```

### 3. Run Your First Job (Dry Run)

```bash
./run_emr_job.sh --dry-run
```

### 4. Run a Real Load Job

```bash
# Load specific team and month
./run_emr_job.sh --team-id 2 --year 2025 --month 5

# Load specific day
./run_emr_job.sh --team-id 2 --year 2025 --month 5 --day 15
```

## How Secrets Work

1. **Storage**: Credentials are stored in AWS Secrets Manager (`ducklake/postgres` secret)
2. **Access**: The IAM role grants EMR Serverless permission to read the secret
3. **Loading**: The Spark job detects `SECRET_NAME` env var and fetches credentials at startup
4. **Fallback**: If `SECRET_NAME` is not set, it falls back to `.env` file (for local development)

## Secret Format

The secret is stored as JSON:

```json
{
  "POSTGRES_HOST": "your-postgres-host",
  "POSTGRES_PORT": "5432",
  "POSTGRES_DB": "ducklake",
  "POSTGRES_USER": "your-username",
  "POSTGRES_PASSWORD": "your-password",
  "DUCKLAKE_DATA_PATH": "/path/to/ducklake/data",
  "S3_BUCKET": "posthog-clickhouse-xxxxx-us-east-1"
}
```

## Updating Secrets

If your secret is just a password (plaintext):

```bash
aws secretsmanager update-secret \
  --secret-id YOUR_SECRET_NAME \
  --secret-string 'your-new-password' \
  --region us-east-1
```

If your secret is JSON with multiple fields:

```bash
aws secretsmanager update-secret \
  --secret-id YOUR_SECRET_NAME \
  --secret-string '{
    "POSTGRES_HOST": "new-host",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "ducklake",
    "POSTGRES_USER": "user",
    "POSTGRES_PASSWORD": "newpassword",
    "DUCKLAKE_DATA_PATH": "/tmp/ducklake_data",
    "S3_BUCKET": "your-posthog-bucket"
  }' \
  --region us-east-1
```

## Monitoring Jobs

### Check Job Status

```bash
# Get application ID from emr_config.txt
APP_ID="<your-app-id>"
JOB_RUN_ID="<job-run-id-from-output>"

aws emr-serverless get-job-run \
  --application-id $APP_ID \
  --job-run-id $JOB_RUN_ID \
  --region us-east-1
```

### View Logs

```bash
# List log files
aws s3 ls s3://your-bucket/ducklake_logs/applications/$APP_ID/jobs/$JOB_RUN_ID/

# View driver stdout
aws s3 cp s3://your-bucket/ducklake_logs/applications/$APP_ID/jobs/$JOB_RUN_ID/SPARK_DRIVER/stdout.gz - | gunzip

# View driver stderr (errors)
aws s3 cp s3://your-bucket/ducklake_logs/applications/$APP_ID/jobs/$JOB_RUN_ID/SPARK_DRIVER/stderr.gz - | gunzip
```

## Cost Management

### Stop Application When Not in Use

```bash
APP_ID="<your-app-id>"
aws emr-serverless stop-application --application-id $APP_ID --region us-east-1
```

### Start Application

```bash
aws emr-serverless start-application --application-id $APP_ID --region us-east-1
```

### Check Application Status

```bash
aws emr-serverless get-application --application-id $APP_ID --region us-east-1
```

## Cleanup (Delete Everything)

```bash
# Get APP_ID from emr_config.txt or environment
APP_ID="your-app-id"

# Delete application
aws emr-serverless delete-application --application-id $APP_ID --region us-east-1

# Delete secret (optional - only if you created it for this purpose)
# aws secretsmanager delete-secret --secret-id YOUR_SECRET_NAME --force-delete-without-recovery --region us-east-1

# Delete IAM role (first remove inline policies)
aws iam delete-role-policy --role-name EMRServerlessDuckLakeRole --policy-name EMRServerlessDuckLakePolicy
aws iam delete-role --role-name EMRServerlessDuckLakeRole

# Clean up S3 (optional - removes logs and scripts)
S3_BUCKET="your-bucket"
aws s3 rm s3://$S3_BUCKET/ducklake_logs/ --recursive
aws s3 rm s3://$S3_BUCKET/ducklake_scripts/ --recursive
```

## Parallel Loading

To load multiple teams in parallel, submit multiple jobs:

```bash
# Submit jobs for different teams (they run in parallel)
./run_emr_job.sh --team-id 2 --year 2025 --month 5
./run_emr_job.sh --team-id 5 --year 2025 --month 5
./run_emr_job.sh --team-id 10 --year 2025 --month 5
```

Or use a loop:

```bash
for team_id in 2 5 10 42; do
  for month in {1..12}; do
    ./run_emr_job.sh --team-id $team_id --year 2025 --month $month
    sleep 2  # Small delay to avoid API throttling
  done
done
```

## Troubleshooting

### "Access Denied" to S3

- Verify the IAM role has permissions for both source (PostHog) and destination (warehouse) buckets
- Check that the role trust policy allows EMR Serverless to assume it

### "Secret Not Found"

- Verify secret exists: `aws secretsmanager list-secrets --region us-east-1`
- Check the secret name matches in both the setup script and the loader

### Job Fails Immediately

- Check CloudWatch logs or S3 logs for the actual error
- Verify the script was uploaded to S3 correctly
- Ensure the S3 path in `run_emr_job.sh` matches where the script was uploaded

### PostgreSQL Connection Fails

- Verify PostgreSQL is accessible from EMR Serverless (check security groups/network ACLs)
- Test credentials manually: `psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB`
- Ensure RDS/PostgreSQL allows connections from EMR Serverless subnet

## Files to Keep Local (DO NOT COMMIT)

- `setup_emr_serverless.sh` - Contains setup logic
- `run_emr_job.sh` - Generated script with your specific config
- `emr_config.txt` - Your application IDs and ARNs
- `EMR_SERVERLESS_SETUP.md` - This file
- `.env` - Local development secrets

## Files Safe to Commit

- `load_posthog_to_ducklake.py` - No secrets, reads from Secrets Manager
- `bootstrap_ducklake.py` - No secrets
- `README.md` - Public documentation
- `pyproject.toml` - Dependencies
