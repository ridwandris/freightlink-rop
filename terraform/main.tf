terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
  }
}

# Configure the AWS Provider
provider "aws" {
  region = var.aws_region
}

# 1. Generate a random string to ensure the S3 bucket name is globally unique
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# 2. The Core Data Lake Bucket
resource "aws_s3_bucket" "data_lake" {
  bucket        = "${var.project_name}-datalake-${random_id.bucket_suffix.hex}"
  force_destroy = true # Allows us to easily delete the bucket later to save money
}

# 3. Create the Medallion Architecture Folders inside the bucket
resource "aws_s3_object" "bronze_layer" {
  bucket       = aws_s3_bucket.data_lake.id
  key          = "bronze_raw/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "silver_layer" {
  bucket       = aws_s3_bucket.data_lake.id
  key          = "silver_cleaned/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "gold_layer" {
  bucket       = aws_s3_bucket.data_lake.id
  key          = "gold_curated/"
  content_type = "application/x-directory"
}

# 4. Output the exact bucket name to the terminal 
output "s3_bucket_name" {
  description = "The name of the S3 bucket to be used in Python scripts"
  value       = aws_s3_bucket.data_lake.bucket
}