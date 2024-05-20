terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
}

variable "aws-credentials" {
  description = "AWS credentials"
  type = string
}

provider "aws" {
  region                   = "us-east-2"
  shared_credentials_files = [var.aws-credentials]
}


resource "aws_s3_bucket" "taxi_data_bucket" {
  bucket = "taxi-data-bucket-klr"

  force_destroy = true

  tags = {
    Name        = "My bucket"
    Environment = "Dev"
  }
}