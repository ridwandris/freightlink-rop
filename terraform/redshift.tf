# 1. IAM Role for Redshift to assume
resource "aws_iam_role" "redshift_s3_role" {
  name = "${var.project_name}-redshift-s3-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "redshift.amazonaws.com"
        }
      }
    ]
  })
}

# 2. IAM Policy to allow Redshift to Read/Write to your S3 Data Lake
resource "aws_iam_role_policy" "redshift_s3_policy" {
  name = "${var.project_name}-redshift-s3-policy"
  role = aws_iam_role.redshift_s3_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:PutObject"
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      }
    ]
  })
}

# 3. Generate a secure random password for the Redshift Admin
resource "random_password" "redshift_admin_password" {
  length           = 16
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

# 4. Redshift Serverless Namespace (The Database Metadata)
resource "aws_redshiftserverless_namespace" "freightlink" {
  namespace_name      = "${var.project_name}-namespace"
  admin_username      = "admin"
  admin_user_password = random_password.redshift_admin_password.result
  iam_roles           = [aws_iam_role.redshift_s3_role.arn]
}

# 5. Redshift Serverless Workgroup (The Compute Engine)
resource "aws_redshiftserverless_workgroup" "freightlink" {
  workgroup_name = "${var.project_name}-workgroup"
  namespace_name = aws_redshiftserverless_namespace.freightlink.namespace_name
  base_capacity  = 8 # Minimum allowed: Perfect for Free Tier / Cost Savings
  
  # CRITICAL: Must be true so your local Airflow and dbt can connect to it!
  publicly_accessible = true 
}

# 6. Outputs needed for our .env file later
output "redshift_admin_password" {
  description = "The generated password for the Redshift admin"
  value       = random_password.redshift_admin_password.result
  sensitive   = true # Hides it from the terminal by default
}

output "redshift_endpoint" {
  description = "The connection URL for Redshift"
  value       = aws_redshiftserverless_workgroup.freightlink.endpoint[0].address
}

output "redshift_iam_role_arn" {
  description = "The IAM Role ARN Redshift uses to access S3"
  value       = aws_iam_role.redshift_s3_role.arn
}