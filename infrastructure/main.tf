# Toast ETL Pipeline - Infrastructure as Code
# Main Terraform configuration for Google Cloud Platform resources

terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# Configure the Google Cloud Provider
provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# Enable required APIs
resource "google_project_service" "required_apis" {
  for_each = toset([
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "pubsub.googleapis.com",
    "cloudbuild.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudscheduler.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "cloudfunctions.googleapis.com",
    "run.googleapis.com"
  ])

  service = each.key
  project = var.project_id

  disable_dependent_services = true
  disable_on_destroy         = false
}

# Create service account for ETL pipeline
resource "google_service_account" "etl_service_account" {
  account_id   = "toast-etl-pipeline"
  display_name = "Toast ETL Pipeline Service Account"
  description  = "Service account for Toast ETL Pipeline operations"
  project      = var.project_id

  depends_on = [google_project_service.required_apis]
}

# Grant necessary permissions to service account
resource "google_project_iam_member" "etl_permissions" {
  for_each = toset([
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectAdmin",
    "roles/pubsub.publisher",
    "roles/pubsub.subscriber",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter"
  ])

  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.etl_service_account.email}"

  depends_on = [google_service_account.etl_service_account]
}

# Create service account key
resource "google_service_account_key" "etl_key" {
  service_account_id = google_service_account.etl_service_account.name
  public_key_type    = "TYPE_X509_PEM_FILE"
}

# Store service account key in Secret Manager
resource "google_secret_manager_secret" "etl_service_account_key" {
  secret_id = "toast-etl-service-account-key"
  project   = var.project_id

  replication {
    auto {}
  }

  depends_on = [google_project_service.required_apis]
}

resource "google_secret_manager_secret_version" "etl_service_account_key_version" {
  secret      = google_secret_manager_secret.etl_service_account_key.id
  secret_data = base64decode(google_service_account_key.etl_key.private_key)
} 