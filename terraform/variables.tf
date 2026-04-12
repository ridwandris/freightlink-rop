variable "aws_region" {
  description = "The AWS region to deploy resources in"
  type        = string
  default     = "eu-central-1" 
}

variable "project_name" {
  description = "The core name for the project resources"
  type        = string
  default     = "freightlink-rop"
}