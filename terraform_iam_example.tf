# Terraform configuration for EMR Serverless IAM Role
# This creates the necessary IAM role and policies for the DuckLake loader

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "secrets_manager_secret_name" {
  description = "Name of the Secrets Manager secret containing PostgreSQL password"
  type        = string
  # Example: "ducklake-prod-rds-password"
}

variable "warehouse_s3_bucket" {
  description = "S3 bucket for scripts and logs"
  type        = string
  # Example: "my-warehouse-bucket"
}

variable "posthog_s3_bucket_pattern" {
  description = "Pattern for PostHog S3 buckets (supports wildcards)"
  type        = string
  default     = "posthog-clickhouse-*"
}

variable "role_name" {
  description = "Name of the IAM role for EMR Serverless"
  type        = string
  default     = "EMRServerlessDuckLakeRole"
}

# Get current AWS account ID
data "aws_caller_identity" "current" {}

# IAM Role for EMR Serverless
resource "aws_iam_role" "emr_serverless_ducklake" {
  name        = var.role_name
  description = "IAM role for EMR Serverless to run DuckLake loader jobs"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "emr-serverless.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name        = var.role_name
    Environment = "production"
    ManagedBy   = "terraform"
  }
}

# IAM Policy for S3 access (PostHog source data - read only)
resource "aws_iam_role_policy" "s3_source_access" {
  name = "S3SourceDataAccess"
  role = aws_iam_role.emr_serverless_ducklake.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.posthog_s3_bucket_pattern}",
          "arn:aws:s3:::${var.posthog_s3_bucket_pattern}/*"
        ]
      }
    ]
  })
}

# IAM Policy for S3 access (Warehouse bucket - read/write for scripts and logs)
resource "aws_iam_role_policy" "s3_warehouse_access" {
  name = "S3WarehouseAccess"
  role = aws_iam_role.emr_serverless_ducklake.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.warehouse_s3_bucket}",
          "arn:aws:s3:::${var.warehouse_s3_bucket}/*"
        ]
      }
    ]
  })
}

# IAM Policy for Secrets Manager access
resource "aws_iam_role_policy" "secrets_manager_access" {
  name = "SecretsManagerAccess"
  role = aws_iam_role.emr_serverless_ducklake.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:${var.secrets_manager_secret_name}*"
      }
    ]
  })
}

# IAM Policy for CloudWatch Logs
resource "aws_iam_role_policy" "cloudwatch_logs" {
  name = "CloudWatchLogsAccess"
  role = aws_iam_role.emr_serverless_ducklake.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/emr-serverless/*"
      }
    ]
  })
}

# Output the role ARN for use in setup script
output "emr_role_arn" {
  description = "ARN of the EMR Serverless IAM role (use this in your .env file as EMR_ROLE_ARN)"
  value       = aws_iam_role.emr_serverless_ducklake.arn
}

output "role_name" {
  description = "Name of the IAM role"
  value       = aws_iam_role.emr_serverless_ducklake.name
}
