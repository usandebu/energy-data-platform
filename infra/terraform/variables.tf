variable "project_name" {
  description = "Project"
  type        = string
  default     = "energy-data-platform"
}

variable "aws_region" {
  description = "Project Region"
  type        = string
  default     = "eu-west-1"
}

variable "environment" {
  description = "environment of the project"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging or prod."
  }
}

variable "bucket_name_suffix" {
  description = "optional suffix for the bucket to make it unique"
  type        = string
  default     = ""
}