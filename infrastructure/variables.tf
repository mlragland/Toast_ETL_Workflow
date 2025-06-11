# Variables for Toast ETL Pipeline Infrastructure

variable "project_id" {
  description = "Google Cloud Project ID"
  type        = string
}

variable "region" {
  description = "Google Cloud region"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "Google Cloud zone"
  type        = string
  default     = "us-central1-a"
}

variable "environment" {
  description = "Environment name (development, staging, production)"
  type        = string
  default     = "production"
  validation {
    condition     = contains(["development", "staging", "production"], var.environment)
    error_message = "Environment must be one of: development, staging, production."
  }
}

# BigQuery Configuration
variable "bigquery_dataset_id" {
  description = "BigQuery dataset ID for Toast analytics"
  type        = string
  default     = "toast_analytics"
}

variable "bigquery_location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "US"
}

variable "table_expiration_days" {
  description = "Number of days after which tables expire (0 for no expiration)"
  type        = number
  default     = 0
}

# Storage Configuration
variable "storage_location" {
  description = "Google Cloud Storage location"
  type        = string
  default     = "US"
}

variable "raw_data_bucket_name" {
  description = "Name for raw data storage bucket"
  type        = string
  default     = "toast-raw-data"
}

variable "logs_bucket_name" {
  description = "Name for logs storage bucket"
  type        = string
  default     = "toast-etl-logs"
}

# Pub/Sub Configuration
variable "notification_topic_name" {
  description = "Pub/Sub topic name for ETL notifications"
  type        = string
  default     = "etl-pipeline-notifications"
}

# Artifact Registry Configuration
variable "artifact_registry_repository" {
  description = "Artifact Registry repository name"
  type        = string
  default     = "toast-etl-repo"
}

variable "artifact_registry_format" {
  description = "Artifact Registry format"
  type        = string
  default     = "DOCKER"
}

# Cloud Scheduler Configuration
variable "scheduler_timezone" {
  description = "Timezone for Cloud Scheduler"
  type        = string
  default     = "America/New_York"
}

variable "etl_schedule" {
  description = "Cron schedule for ETL pipeline execution"
  type        = string
  default     = "30 4 * * *"  # 4:30 AM daily
}

# Labels
variable "labels" {
  description = "Common labels to apply to all resources"
  type        = map(string)
  default = {
    project     = "toast-etl"
    managed-by  = "terraform"
    team        = "data-engineering"
  }
} 