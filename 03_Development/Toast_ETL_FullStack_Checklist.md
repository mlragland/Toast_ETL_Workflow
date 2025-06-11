
# 🍴 Toast ETL Pipeline — Full Stack Implementation Checklist

A detailed, actionable checklist optimized for AI-native coding platforms like OpenAI Codex or Cursor AI. Each task is structured to enable inline assistance from autocomplete or code generation models.

---

## 🏗️ Provision Infrastructure
- [ ] Create Google Cloud project and enable billing.
- [ ] Enable required GCP APIs:
  - [ ] BigQuery
  - [ ] Cloud Storage
  - [ ] Pub/Sub
  - [ ] Cloud Scheduler
- [ ] Create GCS buckets:
  - [ ] `toast-raw-data/raw`
  - [ ] `toast-raw-data/logs`
- [ ] Create BigQuery dataset:
  - [ ] `toast_analytics`
- [ ] Define and create BigQuery tables:
  - [ ] all_items_report
  - [ ] check_details
  - [ ] cash_entries
  - [ ] item_selection_details
  - [ ] kitchen_timings
  - [ ] order_details
  - [ ] payment_details

---

## 🧠 Build and Test ETL Pipeline
- [ ] Refactor manual ETL script to a `main.py` orchestrator module.
  - [ ] Ensure `FILE_CONFIG` is modular and importable.
- [ ] Write unit tests using `pytest`:
  - [ ] Validate each transformation function.
  - [ ] Mock CSV inputs and schema mismatch cases.
- [ ] Containerize ETL pipeline:
  - [ ] Write `Dockerfile` with all required dependencies.
  - [ ] Build and test Docker image locally.
- [ ] Push Docker image to:
  - [ ] GCP Artifact Registry
- [ ] Manual test run:
  - [ ] Select 1-day sample data
  - [ ] Run pipeline end-to-end
  - [ ] Validate BigQuery ingestion and record counts

---

## ⏱ Automate Daily Execution
- [ ] Create Cloud Scheduler job:
  - [ ] Set trigger to 4:30AM daily
  - [ ] Point to Cloud Run or Pub/Sub trigger
- [ ] Set up Pub/Sub:
  - [ ] Topic: `etl_pipeline_notifications`
  - [ ] Subscription: `etl_pipeline_notifications-sub`
- [ ] Add retry logic in ETL:
  - [ ] Use exponential backoff for SFTP/network errors
  - [ ] Log and tag transient failures separately
- [ ] Validate job completion:
  - [ ] Monitor job logs
  - [ ] Ensure metrics/logs are generated in BigQuery and Cloud Logging

---

## 📊 Implement Monitoring and Alerts
- [ ] Integrate Cloud Logging with structured logs:
  - [ ] SFTP stage
  - [ ] Transform stage
  - [ ] GCS upload stage
  - [ ] BigQuery load stage
- [ ] Alerting:
  - [ ] GCP alert policy for failure conditions
  - [ ] Push alerts to email or Slack via Pub/Sub subscriber

---

## 🖥️ Build Dashboard UI
- [ ] Frontend Setup:
  - [ ] React app with Tailwind UI
- [ ] Backend API:
  - [ ] Flask or Firebase Functions
  - [ ] Endpoints:
    - [ ] `/runs` – recent ETL run metadata
    - [ ] `/metrics` – file-level metrics
    - [ ] `/backfill` – trigger bulk re-ingestion
- [ ] BigQuery integration via backend
- [ ] Host dashboard:
  - [ ] Firebase Hosting
  - [ ] or Cloud Run + static assets

---

## 🕰️ Add Historical Backfill Support
- [ ] CLI-based tool:
  - [ ] Accept `--date` or `--range` to select files
  - [ ] Validate file presence and deduplicate
- [ ] UI-based backfill:
  - [ ] Calendar or input form
  - [ ] Backend triggers batch re-processing
- [ ] Log all runs in `etl_run_log`

---

## ✅ Testing and Finalization
- [ ] Load 7 days of real Toast data for QA
- [ ] Conduct UAT with analytics and data teams
- [ ] Optimize:
  - [ ] BigQuery partitioning
  - [ ] GCS-to-BQ load throughput
- [ ] Final steps:
  - [ ] Write deployment & support documentation
  - [ ] Handoff or training with internal ops team

---
