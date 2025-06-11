# Outputs for Toast ETL Pipeline Infrastructure

# Project Information
output "project_id" {
  description = "Google Cloud Project ID"
  value       = var.project_id
}

output "region" {
  description = "Google Cloud region"
  value       = var.region
}

output "environment" {
  description = "Environment name"
  value       = var.environment
}

# Service Account
output "etl_service_account_email" {
  description = "ETL service account email"
  value       = google_service_account.etl_service_account.email
}

output "etl_service_account_key_secret" {
  description = "Secret Manager secret name containing service account key"
  value       = google_secret_manager_secret.etl_service_account_key.secret_id
  sensitive   = true
}

# BigQuery
output "bigquery_dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.toast_analytics.dataset_id
}

output "bigquery_dataset_location" {
  description = "BigQuery dataset location"
  value       = google_bigquery_dataset.toast_analytics.location
}

output "bigquery_tables" {
  description = "List of BigQuery table names"
  value = [
    google_bigquery_table.all_items_report.table_id,
    google_bigquery_table.check_details.table_id,
    google_bigquery_table.cash_entries.table_id,
    google_bigquery_table.item_selection_details.table_id,
    google_bigquery_table.kitchen_timings.table_id,
    google_bigquery_table.order_details.table_id,
    google_bigquery_table.payment_details.table_id
  ]
}

# Storage
output "raw_data_bucket_name" {
  description = "Raw data storage bucket name"
  value       = google_storage_bucket.raw_data.name
}

output "raw_data_bucket_url" {
  description = "Raw data storage bucket URL"
  value       = google_storage_bucket.raw_data.url
}

output "logs_bucket_name" {
  description = "Logs storage bucket name"
  value       = google_storage_bucket.logs.name
}

output "logs_bucket_url" {
  description = "Logs storage bucket URL"
  value       = google_storage_bucket.logs.url
}

# Pub/Sub
output "notification_topic_name" {
  description = "ETL notifications Pub/Sub topic name"
  value       = google_pubsub_topic.etl_notifications.name
}

output "notification_subscription_name" {
  description = "ETL notifications Pub/Sub subscription name"
  value       = google_pubsub_subscription.etl_notifications_sub.name
}

output "dead_letter_topic_name" {
  description = "Dead letter Pub/Sub topic name"
  value       = google_pubsub_topic.etl_dead_letter.name
}

# Connection Information for ETL Pipeline
output "etl_config" {
  description = "Configuration values for ETL pipeline"
  value = {
    project_id                  = var.project_id
    bigquery_dataset_id         = google_bigquery_dataset.toast_analytics.dataset_id
    bigquery_location           = google_bigquery_dataset.toast_analytics.location
    raw_data_bucket             = google_storage_bucket.raw_data.name
    logs_bucket                 = google_storage_bucket.logs.name
    notification_topic          = google_pubsub_topic.etl_notifications.name
    service_account_email       = google_service_account.etl_service_account.email
    service_account_key_secret  = google_secret_manager_secret.etl_service_account_key.secret_id
  }
  sensitive = true
} 