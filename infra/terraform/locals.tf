locals {
  raw_bucket_name = var.bucket_name_suffix == "" ? "${var.project_name}-${var.environment}-raw" : "${var.project_name}-${var.environment}-raw-${var.bucket_name_suffix}"

  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}