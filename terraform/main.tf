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


resource "aws_s3_bucket" "demo-bucket" {
  bucket = "kevinr-terraform-demo-bucket"

  force_destroy = true

  tags = {
    Name        = "My bucket"
    Environment = "Dev"
  }
}

resource "aws_db_instance" "pgdatabase" {
  allocated_storage = 20
  db_name = "terraformDb"
  engine = "postgres"
  engine_version = "16.2"
  instance_class = "db.t3.micro"
  username= "root"
  password = "password"
  skip_final_snapshot = true
  vpc_security_group_ids = ["sg-037334a01b964d48a"]
}
