# Google Cloud Storage resources for Toast ETL Pipeline

# Raw data storage bucket
resource "google_storage_bucket" "raw_data" {
  name     = "${var.raw_data_bucket_name}-${var.project_id}"
  location = var.storage_location
  project  = var.project_id

  # Prevent deletion if bucket contains objects
  force_destroy = false

  # Versioning configuration
  versioning {
    enabled = true
  }

  # Lifecycle management
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  # Public access prevention
  public_access_prevention = "enforced"

  # Uniform bucket-level access
  uniform_bucket_level_access = true

  labels = merge(var.labels, {
    environment = var.environment
    purpose     = "raw-data-storage"
  })

  depends_on = [google_project_service.required_apis]
}

# Logs storage bucket
resource "google_storage_bucket" "logs" {
  name     = "${var.logs_bucket_name}-${var.project_id}"
  location = var.storage_location
  project  = var.project_id

  # Allow deletion for log cleanup
  force_destroy = true

  # Lifecycle management - logs older than 30 days deleted
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }

  # Public access prevention
  public_access_prevention = "enforced"

  # Uniform bucket-level access
  uniform_bucket_level_access = true

  labels = merge(var.labels, {
    environment = var.environment
    purpose     = "logs-storage"
  })

  depends_on = [google_project_service.required_apis]
}

# Raw data subdirectories (folders)
resource "google_storage_bucket_object" "raw_folder" {
  name    = "raw/"
  content = " "
  bucket  = google_storage_bucket.raw_data.name
}

resource "google_storage_bucket_object" "processed_folder" {
  name    = "processed/"
  content = " "
  bucket  = google_storage_bucket.raw_data.name
}

resource "google_storage_bucket_object" "archive_folder" {
  name    = "archive/"
  content = " "
  bucket  = google_storage_bucket.raw_data.name
}

# IAM binding for ETL service account to access raw data bucket
resource "google_storage_bucket_iam_member" "etl_raw_data_access" {
  bucket = google_storage_bucket.raw_data.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.etl_service_account.email}"

  depends_on = [google_service_account.etl_service_account]
}

# IAM binding for ETL service account to access logs bucket
resource "google_storage_bucket_iam_member" "etl_logs_access" {
  bucket = google_storage_bucket.logs.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.etl_service_account.email}"

  depends_on = [google_service_account.etl_service_account]
} 