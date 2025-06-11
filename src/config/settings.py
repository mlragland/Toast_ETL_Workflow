"""Settings configuration for Toast ETL Pipeline."""

import os
from typing import Optional
from dataclasses import dataclass


@dataclass
class Settings:
    """Main configuration class for Toast ETL Pipeline."""
    
    # SFTP Configuration
    sftp_user: str = os.getenv("SFTP_USER", "LoveExportUser")
    sftp_server: str = os.getenv("SFTP_SERVER", "s-9b0f88558b264dfda.server.transfer.us-east-1.amazonaws.com")
    sftp_path_template: str = os.getenv("SFTP_PATH_TEMPLATE", "185129/{date}/*")
    ssh_key_path: str = os.getenv("SSH_KEY_PATH", "~/.ssh/toast_ssh")
    
    # Google Cloud Configuration
    gcp_project_id: str = os.getenv("GCP_PROJECT_ID", "toast-analytics-444116")
    gcs_bucket_name: str = os.getenv("GCS_BUCKET_NAME", "toast-raw-data")
    bigquery_dataset: str = os.getenv("BIGQUERY_DATASET", "toast_analytics")
    
    # Pub/Sub Configuration
    pubsub_topic: str = os.getenv("PUBSUB_TOPIC", "etl_pipeline_notifications")
    pubsub_subscription: str = os.getenv("PUBSUB_SUBSCRIPTION", "etl_pipeline_notifications-sub")
    
    # Local Storage Configuration
    raw_local_dir: str = os.getenv("RAW_LOCAL_DIR", "/tmp/toast_raw_data/raw")
    cleaned_local_dir: str = os.getenv("CLEANED_LOCAL_DIR", "/tmp/toast_raw_data/cleaned")
    logs_dir: str = os.getenv("LOGS_DIR", "/tmp/toast_raw_data/logs")
    
    # Processing Configuration
    processing_timezone: str = os.getenv("PROCESSING_TIMEZONE", "UTC")
    max_retry_attempts: int = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))
    retry_delay_seconds: int = int(os.getenv("RETRY_DELAY_SECONDS", "60"))
    
    # Environment
    environment: str = os.getenv("ENVIRONMENT", "development")
    debug: bool = os.getenv("DEBUG", "false").lower() == "true"
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.gcp_project_id:
            raise ValueError("GCP_PROJECT_ID must be set")
        if not self.gcs_bucket_name:
            raise ValueError("GCS_BUCKET_NAME must be set")


# Global settings instance
settings = Settings() 