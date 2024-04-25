terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
}

variable "aws-credentials" {
  type = string
}

provider "aws" {
  region                   = "us-east-2"
  shared_credentials_files = [var.aws-credentials]
}


resource "aws_s3_bucket" "demo-bucket" {
  bucket = "kevinr-terraform-demo-bucket"

  force_destroy = true

  tags = {
    Name        = "My bucket"
    Environment = "Dev"
  }
}

