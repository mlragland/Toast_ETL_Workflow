
# Deployment Guide

## Infrastructure
- GCS buckets: toast-raw-data/raw and /logs
- BigQuery dataset: toast_analytics
- Artifact Registry for Docker images

## Deployment Steps
1. Build ETL Docker image
2. Push to Artifact Registry
3. Create Cloud Scheduler Job for daily trigger
4. Enable Pub/Sub and Cloud Logging
5. Deploy dashboard backend and frontend

## Rollback
- Revert to previous container tag in Cloud Run
- Disable scheduler job for hotfixes
