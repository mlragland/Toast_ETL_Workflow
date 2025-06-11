# Cloud Scheduler configuration for Toast ETL Pipeline
# Automates daily execution at 4:30 AM EST with retry logic

# Cloud Scheduler Job for daily ETL execution
resource "google_cloud_scheduler_job" "daily_etl" {
  name             = "toast-etl-daily-pipeline"
  description      = "Daily Toast ETL Pipeline execution at 4:30 AM EST"
  schedule         = "30 4 * * *"  # 4:30 AM daily
  time_zone        = var.scheduler_timezone
  region           = var.region
  project          = var.project_id

  # Retry configuration
  retry_config {
    retry_count          = 3
    max_retry_duration   = "3600s"  # 1 hour max retry duration
    max_backoff_duration = "600s"   # 10 minutes max backoff
    min_backoff_duration = "30s"    # 30 seconds min backoff
    max_doublings        = 4        # Exponential backoff doublings
  }

  # HTTP target configuration for Cloud Run
  http_target {
    http_method = "POST"
    uri         = "https://${google_cloud_run_service.etl_pipeline.status[0].url}/execute"
    
    # Authentication for Cloud Run
    oidc_token {
      service_account_email = google_service_account.etl_service_account.email
      audience              = "https://${google_cloud_run_service.etl_pipeline.status[0].url}"
    }

    # Request body with execution parameters
    body = base64encode(jsonencode({
      execution_date = "$${execution_date}"
      environment   = var.environment
      enable_validation = true
      quality_report   = true
      execution_id     = "$${execution_id}"
    }))

    headers = {
      "Content-Type" = "application/json"
      "X-Scheduler-Source" = "toast-etl-daily"
    }
  }

  depends_on = [
    google_project_service.required_apis,
    google_cloud_run_service.etl_pipeline,
    google_service_account.etl_service_account
  ]

  # Labels for resource management
  labels = merge(var.labels, {
    environment = var.environment
    purpose     = "daily-etl-execution"
    schedule    = "daily-4-30am"
  })
}

# Cloud Scheduler Job for weekly full validation
resource "google_cloud_scheduler_job" "weekly_validation" {
  name             = "toast-etl-weekly-validation"
  description      = "Weekly comprehensive validation and quality check"
  schedule         = "0 5 * * 1"  # 5:00 AM every Monday
  time_zone        = var.scheduler_timezone
  region           = var.region
  project          = var.project_id

  # Retry configuration
  retry_config {
    retry_count          = 2
    max_retry_duration   = "1800s"  # 30 minutes max retry
    max_backoff_duration = "300s"   # 5 minutes max backoff
    min_backoff_duration = "60s"    # 1 minute min backoff
    max_doublings        = 3
  }

  # HTTP target for weekly validation
  http_target {
    http_method = "POST"
    uri         = "https://${google_cloud_run_service.etl_pipeline.status[0].url}/validate-weekly"
    
    oidc_token {
      service_account_email = google_service_account.etl_service_account.email
      audience              = "https://${google_cloud_run_service.etl_pipeline.status[0].url}"
    }

    body = base64encode(jsonencode({
      validation_type = "comprehensive"
      date_range_days = 7
      environment     = var.environment
      deep_analysis   = true
    }))

    headers = {
      "Content-Type" = "application/json"
      "X-Scheduler-Source" = "toast-etl-weekly"
    }
  }

  depends_on = [
    google_project_service.required_apis,
    google_cloud_run_service.etl_pipeline,
    google_service_account.etl_service_account
  ]

  labels = merge(var.labels, {
    environment = var.environment
    purpose     = "weekly-validation"
    schedule    = "weekly-monday-5am"
  })
}

# Grant Cloud Scheduler permission to invoke Cloud Run
resource "google_cloud_run_service_iam_member" "scheduler_invoker" {
  location = google_cloud_run_service.etl_pipeline.location
  project  = google_cloud_run_service.etl_pipeline.project
  service  = google_cloud_run_service.etl_pipeline.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.etl_service_account.email}"

  depends_on = [
    google_cloud_run_service.etl_pipeline,
    google_service_account.etl_service_account
  ]
} 