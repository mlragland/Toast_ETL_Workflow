# Pub/Sub resources for Toast ETL Pipeline

# ETL Pipeline notifications topic
resource "google_pubsub_topic" "etl_notifications" {
  name    = var.notification_topic_name
  project = var.project_id

  labels = merge(var.labels, {
    environment = var.environment
    purpose     = "etl-notifications"
  })

  depends_on = [google_project_service.required_apis]
}

# ETL Pipeline notifications subscription
resource "google_pubsub_subscription" "etl_notifications_sub" {
  name    = "${var.notification_topic_name}-sub"
  topic   = google_pubsub_topic.etl_notifications.name
  project = var.project_id

  # Message retention duration
  message_retention_duration = "604800s" # 7 days

  # Retry policy
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  # Dead letter policy
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.etl_dead_letter.id
    max_delivery_attempts = 5
  }

  # Acknowledgment deadline
  ack_deadline_seconds = 60

  labels = merge(var.labels, {
    environment = var.environment
    purpose     = "etl-notifications"
  })

  depends_on = [google_pubsub_topic.etl_notifications]
}

# Dead letter topic for failed messages
resource "google_pubsub_topic" "etl_dead_letter" {
  name    = "${var.notification_topic_name}-dead-letter"
  project = var.project_id

  labels = merge(var.labels, {
    environment = var.environment
    purpose     = "dead-letter-queue"
  })

  depends_on = [google_project_service.required_apis]
}

# Dead letter subscription
resource "google_pubsub_subscription" "etl_dead_letter_sub" {
  name    = "${var.notification_topic_name}-dead-letter-sub"
  topic   = google_pubsub_topic.etl_dead_letter.name
  project = var.project_id

  # Longer retention for dead letters
  message_retention_duration = "1209600s" # 14 days

  labels = merge(var.labels, {
    environment = var.environment
    purpose     = "dead-letter-queue"
  })

  depends_on = [google_pubsub_topic.etl_dead_letter]
}

# Grant ETL service account publish permissions
resource "google_pubsub_topic_iam_member" "etl_publisher" {
  topic   = google_pubsub_topic.etl_notifications.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.etl_service_account.email}"
  project = var.project_id

  depends_on = [
    google_pubsub_topic.etl_notifications,
    google_service_account.etl_service_account
  ]
}

# Grant ETL service account subscriber permissions
resource "google_pubsub_subscription_iam_member" "etl_subscriber" {
  subscription = google_pubsub_subscription.etl_notifications_sub.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.etl_service_account.email}"
  project      = var.project_id

  depends_on = [
    google_pubsub_subscription.etl_notifications_sub,
    google_service_account.etl_service_account
  ]
} 