# Cloud Run service configuration for Toast ETL Pipeline
# Provides scalable, serverless execution of the containerized ETL pipeline

# Artifact Registry repository for Docker images
resource "google_artifact_registry_repository" "etl_repo" {
  location      = var.region
  project       = var.project_id
  repository_id = "toast-etl-pipeline"
  description   = "Docker repository for Toast ETL Pipeline images"
  format        = "DOCKER"

  labels = merge(var.labels, {
    environment = var.environment
    purpose     = "container-registry"
  })

  depends_on = [google_project_service.required_apis]
}

# Cloud Run service for ETL pipeline
resource "google_cloud_run_service" "etl_pipeline" {
  name     = "toast-etl-pipeline"
  location = var.region
  project  = var.project_id

  template {
    metadata {
      labels = merge(var.labels, {
        environment = var.environment
        service     = "etl-pipeline"
      })
      
      annotations = {
        "autoscaling.knative.dev/maxScale"              = "10"
        "autoscaling.knative.dev/minScale"              = "0"
        "run.googleapis.com/cpu-throttling"             = "false"
        "run.googleapis.com/execution-environment"      = "gen2"
        "run.googleapis.com/vpc-access-connector"       = var.vpc_connector_name
        "run.googleapis.com/vpc-access-egress"          = "all-traffic"
      }
    }

    spec {
      container_concurrency = 1  # Process one ETL job at a time
      timeout_seconds       = 3600  # 1 hour timeout
      service_account_name  = google_service_account.etl_service_account.email

      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.etl_repo.repository_id}/toast-etl:latest"
        
        # Resource allocation
        resources {
          limits = {
            cpu    = "2"      # 2 vCPUs
            memory = "4Gi"    # 4GB RAM
          }
          requests = {
            cpu    = "1"      # 1 vCPU minimum
            memory = "2Gi"    # 2GB RAM minimum
          }
        }

        # Environment variables
        env {
          name  = "PROJECT_ID"
          value = var.project_id
        }
        
        env {
          name  = "DATASET_ID"
          value = var.bigquery_dataset_id
        }
        
        env {
          name  = "GCS_BUCKET"
          value = google_storage_bucket.raw_data.name
        }
        
        env {
          name  = "ENVIRONMENT"
          value = var.environment
        }
        
        env {
          name  = "LOG_LEVEL"
          value = var.environment == "production" ? "INFO" : "DEBUG"
        }

        env {
          name  = "PUBSUB_TOPIC"
          value = google_pubsub_topic.etl_notifications.name
        }

        env {
          name  = "ENABLE_MONITORING"
          value = "true"
        }

        # Health check endpoint
        ports {
          container_port = 8080
        }

        # Startup probe
        startup_probe {
          http_get {
            path = "/health"
            port = 8080
          }
          initial_delay_seconds = 10
          timeout_seconds       = 5
          period_seconds        = 10
          failure_threshold     = 5
        }

        # Liveness probe
        liveness_probe {
          http_get {
            path = "/health"
            port = 8080
          }
          initial_delay_seconds = 30
          timeout_seconds       = 5
          period_seconds        = 30
          failure_threshold     = 3
        }
      }
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  autogenerate_revision_name = true

  depends_on = [
    google_project_service.required_apis,
    google_service_account.etl_service_account,
    google_artifact_registry_repository.etl_repo
  ]
}

# Cloud Run IAM policy for public access (restricted to service account)
resource "google_cloud_run_service_iam_policy" "etl_pipeline_policy" {
  location = google_cloud_run_service.etl_pipeline.location
  project  = google_cloud_run_service.etl_pipeline.project
  service  = google_cloud_run_service.etl_pipeline.name

  policy_data = data.google_iam_policy.etl_pipeline_policy.policy_data
}

# IAM policy data for Cloud Run service
data "google_iam_policy" "etl_pipeline_policy" {
  binding {
    role = "roles/run.invoker"
    members = [
      "serviceAccount:${google_service_account.etl_service_account.email}",
    ]
  }
}

# Cloud Run domain mapping (optional)
resource "google_cloud_run_domain_mapping" "etl_pipeline_domain" {
  count    = var.custom_domain != "" ? 1 : 0
  location = google_cloud_run_service.etl_pipeline.location
  name     = var.custom_domain
  project  = var.project_id

  metadata {
    namespace = var.project_id
    labels = merge(var.labels, {
      environment = var.environment
      service     = "etl-pipeline"
    })
  }

  spec {
    route_name = google_cloud_run_service.etl_pipeline.name
  }

  depends_on = [google_cloud_run_service.etl_pipeline]
} 