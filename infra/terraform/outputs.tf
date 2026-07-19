output "raw_bucket_name" {
  description = "S3 bucket used for raw data."
  value       = aws_s3_bucket.raw.bucket
}

output "raw_bucket_arn" {
  description = "ARN of the S3 bucket used for raw data."
  value       = aws_s3_bucket.raw.arn
}