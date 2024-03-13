provider "aws" {
  region = "us-east-1" # Change this to your desired region
}

resource "aws_s3_bucket" "example_bucket" {
  bucket = "example-bucket-name" # Change this to your desired bucket name
  acl    = "private" # Access control list for the bucket, can be "private", "public-read", "public-read-write", "authenticated-read", "aws-exec-read", "bucket-owner-read", or "bucket-owner-full-control"
}
