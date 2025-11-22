#!/bin/bash
set -e

# EMR Serverless Setup Script
# This script sets up everything needed to run the PostHog DuckLake loader on EMR Serverless

echo "PostHog DuckLake - EMR Serverless Setup"
echo "========================================"
echo ""

# Load from .env if available
if [ -f .env ]; then
    echo "Loading configuration from .env file..."
    set -a
    source .env
    set +a
    echo ""
fi

# Configuration
REGION="${AWS_REGION:-us-east-1}"
APP_NAME="posthog-ducklake-loader"
IAM_ROLE_NAME="EMRServerlessDuckLakeRole"

# Prompt for S3 bucket if not in .env
if [ -z "$S3_BUCKET" ] || [ "$S3_BUCKET" = "sick-bucket-of-data" ]; then
    echo "Enter your S3 warehouse bucket name (for scripts and logs):"
    read -r S3_BUCKET
else
    echo "Using S3 bucket from .env: $S3_BUCKET"
fi

# Prompt for secret name if not in .env
if [ -z "$SECRET_NAME" ]; then
    echo ""
    echo "Enter your AWS Secrets Manager secret name (contains PostgreSQL password):"
    echo "Example: ducklake-prod-rds-password"
    read -r SECRET_NAME
else
    echo "Using Secrets Manager secret from .env: $SECRET_NAME"
fi

SCRIPT_PATH="ducklake_scripts/load_posthog_to_ducklake.py"
LOGS_PATH="ducklake_logs/"

# Check AWS CLI
if ! command -v aws &> /dev/null; then
    echo "Error: AWS CLI is not installed"
    exit 1
fi

# Validate bucket
if ! aws s3 ls "s3://${S3_BUCKET}" --region "$REGION" &> /dev/null; then
    echo "Error: Bucket s3://${S3_BUCKET} does not exist or is not accessible"
    exit 1
fi

echo ""
echo "Configuration:"
echo "  Region: $REGION"
echo "  S3 Bucket: s3://${S3_BUCKET}"
echo "  Script Path: s3://${S3_BUCKET}/${SCRIPT_PATH}"
echo "  Logs Path: s3://${S3_BUCKET}/${LOGS_PATH}"
echo ""

# Upload script to S3
echo "Step 1: Uploading loader script to S3..."
aws s3 cp load_posthog_to_ducklake.py "s3://${S3_BUCKET}/${SCRIPT_PATH}" --region "$REGION"
echo "✓ Script uploaded"
echo ""

# Setup IAM role
echo "Step 2: Setting up IAM role..."
if aws iam get-role --role-name "$IAM_ROLE_NAME" &> /dev/null; then
    echo "✓ IAM role $IAM_ROLE_NAME already exists"
    ROLE_ARN=$(aws iam get-role --role-name "$IAM_ROLE_NAME" --query 'Role.Arn' --output text)
else
    echo "Creating IAM role..."

    # Trust policy
    cat > /tmp/trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "emr-serverless.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
EOF

    ROLE_ARN=$(aws iam create-role \
        --role-name "$IAM_ROLE_NAME" \
        --assume-role-policy-document file:///tmp/trust-policy.json \
        --query 'Role.Arn' \
        --output text)

    # Get AWS account ID for policy
    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

    # Permissions policy
    cat > /tmp/role-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::posthog-clickhouse-*/*",
        "arn:aws:s3:::posthog-clickhouse-*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::${S3_BUCKET}/*",
        "arn:aws:s3:::${S3_BUCKET}"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["secretsmanager:GetSecretValue"],
      "Resource": "arn:aws:secretsmanager:${REGION}:${ACCOUNT_ID}:secret:${SECRET_NAME}*"
    },
    {
      "Effect": "Allow",
      "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "arn:aws:logs:*:*:*"
    }
  ]
}
EOF

    aws iam put-role-policy \
        --role-name "$IAM_ROLE_NAME" \
        --policy-name "EMRServerlessDuckLakePolicy" \
        --policy-document file:///tmp/role-policy.json

    echo "✓ Created IAM role: $ROLE_ARN"
    echo "  Waiting for IAM propagation..."
    sleep 10
fi
echo ""

# Verify Secrets Manager secret exists
echo "Step 3: Verifying Secrets Manager secret..."
if aws secretsmanager describe-secret --secret-id "$SECRET_NAME" --region "$REGION" &> /dev/null 2>&1; then
    echo "✓ Secret ${SECRET_NAME} exists"

    # Show current secret structure (without revealing values)
    echo ""
    echo "Current secret contains PostgreSQL RDS password."
    echo "The loader will fetch this at runtime and combine it with other config from .env or additional secrets."
    echo ""
else
    echo "Error: Secret ${SECRET_NAME} not found"
    echo "Please ensure the secret exists or update SECRET_NAME in this script"
    exit 1
fi
echo ""

# Create EMR Serverless application
echo "Step 4: Setting up EMR Serverless application..."
APP_ID=$(aws emr-serverless list-applications \
    --region "$REGION" \
    --query "applications[?name=='${APP_NAME}' && state=='CREATED'].id" \
    --output text)

if [ -z "$APP_ID" ]; then
    echo "Creating application..."
    APP_ID=$(aws emr-serverless create-application \
        --region "$REGION" \
        --name "$APP_NAME" \
        --type SPARK \
        --release-label emr-7.0.0 \
        --auto-start-configuration enabled=true \
        --auto-stop-configuration enabled=true,idleTimeoutMinutes=15 \
        --query 'applicationId' \
        --output text)

    echo "✓ Created application: $APP_ID"
    echo "  Waiting for application to be ready..."
    sleep 5
else
    echo "✓ Using existing application: $APP_ID"
fi
echo ""

# Create run script
echo "Step 5: Creating run_emr_job.sh helper script..."
cat > run_emr_job.sh <<'SCRIPT_EOF'
#!/bin/bash
# Submit DuckLake load jobs to EMR Serverless

# Load configuration from .env file
if [ -f .env ]; then
    set -a  # automatically export all variables
    source .env
    set +a
else
    echo "Warning: .env file not found. Using environment variables."
fi

APP_ID="APP_ID_PLACEHOLDER"
REGION="REGION_PLACEHOLDER"
ROLE_ARN="ROLE_ARN_PLACEHOLDER"
S3_BUCKET="S3_BUCKET_PLACEHOLDER"
SCRIPT_PATH="SCRIPT_PATH_PLACEHOLDER"
LOGS_PATH="LOGS_PATH_PLACEHOLDER"
SECRET_NAME="SECRET_NAME_PLACEHOLDER"

# Parse arguments
ARGS=()
JOB_NAME="ducklake-load"

while [[ \$# -gt 0 ]]; do
    case \$1 in
        --team-id)
            ARGS+=("--team-id" "\$2")
            JOB_NAME="\${JOB_NAME}-team\$2"
            shift 2
            ;;
        --year)
            ARGS+=("--year" "\$2")
            JOB_NAME="\${JOB_NAME}-\$2"
            shift 2
            ;;
        --month)
            ARGS+=("--month" "\$2")
            JOB_NAME="\${JOB_NAME}-\$(printf %02d \$2)"
            shift 2
            ;;
        --day)
            ARGS+=("--day" "\$2")
            JOB_NAME="\${JOB_NAME}-\$(printf %02d \$2)"
            shift 2
            ;;
        --hour)
            ARGS+=("--hour" "\$2")
            shift 2
            ;;
        --dry-run)
            ARGS+=("--dry-run")
            JOB_NAME="\${JOB_NAME}-dryrun"
            shift
            ;;
        *)
            echo "Usage: \$0 [--team-id ID] [--year YYYY] [--month M] [--day D] [--hour H] [--dry-run]"
            exit 1
            ;;
    esac
done

# Convert args to JSON array
ARGS_JSON="["
for i in "\${!ARGS[@]}"; do
    [ \$i -gt 0 ] && ARGS_JSON+=","
    ARGS_JSON+="\"\${ARGS[\$i]}\""
done
ARGS_JSON+="]"

echo "Submitting job: \$JOB_NAME"
echo "Arguments: \${ARGS[@]}"
echo ""

# Build Spark config with environment variables from .env
SPARK_CONF="--conf spark.executor.memory=32g"
SPARK_CONF+=" --conf spark.executor.cores=8"
SPARK_CONF+=" --conf spark.driver.memory=16g"

# Pass environment variables to Spark (both driver and executor)
[ -n "\${POSTGRES_HOST}" ] && SPARK_CONF+=" --conf spark.executorEnv.POSTGRES_HOST=\${POSTGRES_HOST} --conf spark.driverEnv.POSTGRES_HOST=\${POSTGRES_HOST}"
[ -n "\${POSTGRES_PORT}" ] && SPARK_CONF+=" --conf spark.executorEnv.POSTGRES_PORT=\${POSTGRES_PORT} --conf spark.driverEnv.POSTGRES_PORT=\${POSTGRES_PORT}"
[ -n "\${POSTGRES_USER}" ] && SPARK_CONF+=" --conf spark.executorEnv.POSTGRES_USER=\${POSTGRES_USER} --conf spark.driverEnv.POSTGRES_USER=\${POSTGRES_USER}"
[ -n "\${POSTGRES_DB}" ] && SPARK_CONF+=" --conf spark.executorEnv.POSTGRES_DB=\${POSTGRES_DB} --conf spark.driverEnv.POSTGRES_DB=\${POSTGRES_DB}"
[ -n "\${DUCKLAKE_DATA_PATH}" ] && SPARK_CONF+=" --conf spark.executorEnv.DUCKLAKE_DATA_PATH=\${DUCKLAKE_DATA_PATH} --conf spark.driverEnv.DUCKLAKE_DATA_PATH=\${DUCKLAKE_DATA_PATH}"
[ -n "\${S3_BUCKET}" ] && SPARK_CONF+=" --conf spark.executorEnv.S3_BUCKET=\${S3_BUCKET} --conf spark.driverEnv.S3_BUCKET=\${S3_BUCKET}"
[ -n "\${S3_PREFIX}" ] && SPARK_CONF+=" --conf spark.executorEnv.S3_PREFIX=\${S3_PREFIX} --conf spark.driverEnv.S3_PREFIX=\${S3_PREFIX}"

# For password, use Secrets Manager
SPARK_CONF+=" --conf spark.executorEnv.SECRET_NAME=\${SECRET_NAME} --conf spark.driverEnv.SECRET_NAME=\${SECRET_NAME}"
SPARK_CONF+=" --conf spark.executorEnv.AWS_REGION=\${REGION} --conf spark.driverEnv.AWS_REGION=\${REGION}"

# Submit job
JOB_RUN_ID=\$(aws emr-serverless start-job-run \\
    --region "\$REGION" \\
    --application-id "\$APP_ID" \\
    --execution-role-arn "\$ROLE_ARN" \\
    --name "\$JOB_NAME" \\
    --job-driver "{
        \"sparkSubmit\": {
            \"entryPoint\": \"s3://\${S3_BUCKET}/\${SCRIPT_PATH}\",
            \"entryPointArguments\": \${ARGS_JSON},
            \"sparkSubmitParameters\": \"\${SPARK_CONF}\"
        }
    }" \\
    --configuration-overrides "{
        \"monitoringConfiguration\": {
            \"s3MonitoringConfiguration\": {
                \"logUri\": \"s3://\${S3_BUCKET}/\${LOGS_PATH}\"
            }
        }
    }" \\
    --query 'jobRunId' \\
    --output text)

echo "✓ Job submitted!"
echo ""
echo "Job Run ID: \$JOB_RUN_ID"
echo ""
echo "Check status:"
echo "  aws emr-serverless get-job-run --application-id \$APP_ID --job-run-id \$JOB_RUN_ID --region \$REGION"
echo ""
echo "View driver logs (once job completes):"
echo "  aws s3 cp s3://\${S3_BUCKET}/\${LOGS_PATH}applications/\$APP_ID/jobs/\$JOB_RUN_ID/SPARK_DRIVER/stdout.gz - | gunzip"
SCRIPT_EOF

# Replace placeholders
sed -i "s|APP_ID_PLACEHOLDER|$APP_ID|g" run_emr_job.sh
sed -i "s|REGION_PLACEHOLDER|$REGION|g" run_emr_job.sh
sed -i "s|ROLE_ARN_PLACEHOLDER|$ROLE_ARN|g" run_emr_job.sh
sed -i "s|S3_BUCKET_PLACEHOLDER|$S3_BUCKET|g" run_emr_job.sh
sed -i "s|SCRIPT_PATH_PLACEHOLDER|$SCRIPT_PATH|g" run_emr_job.sh
sed -i "s|LOGS_PATH_PLACEHOLDER|$LOGS_PATH|g" run_emr_job.sh
sed -i "s|SECRET_NAME_PLACEHOLDER|$SECRET_NAME|g" run_emr_job.sh

chmod +x run_emr_job.sh

echo "✓ Created run_emr_job.sh"
echo ""

# Save configuration
cat > emr_config.txt <<EOF
EMR Serverless Configuration
============================
Application ID: $APP_ID
Region: $REGION
IAM Role: $ROLE_ARN
S3 Bucket: $S3_BUCKET
Script Path: s3://${S3_BUCKET}/${SCRIPT_PATH}
Logs Path: s3://${S3_BUCKET}/${LOGS_PATH}
Secret Name: $SECRET_NAME
EOF

echo "========================================"
echo "Setup Complete! 🎉"
echo "========================================"
echo ""
echo "Configuration saved to: emr_config.txt"
echo ""
echo "Quick Start:"
echo "  # Dry run"
echo "  ./run_emr_job.sh --dry-run"
echo ""
echo "  # Load specific team and month"
echo "  ./run_emr_job.sh --team-id 2 --year 2025 --month 5"
echo ""
echo "Update secrets:"
echo "  aws secretsmanager update-secret --secret-id $SECRET_NAME --secret-string '{\"POSTGRES_HOST\":\"...\"}' --region $REGION"
echo ""
echo "Monitor application:"
echo "  aws emr-serverless get-application --application-id $APP_ID --region $REGION"
echo ""
