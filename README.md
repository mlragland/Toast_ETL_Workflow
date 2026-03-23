# Toast ETL Pipeline

**Automated data pipeline for LOV3 Houston**  
Ingests daily Toast POS data from SFTP and loads to BigQuery

---

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Cloud          │     │                  │     │                 │
│  Scheduler      │────▶│   Cloud Run      │────▶│   BigQuery      │
│  (8 AM CST)     │     │   ETL Service    │     │   toast_raw     │
└─────────────────┘     └────────┬─────────┘     └─────────────────┘
                                 │
                                 │ SFTP
                                 ▼
                        ┌─────────────────┐
                        │  Toast SFTP     │
                        │  (AWS Transfer) │
                        └─────────────────┘
                                 │
                                 │ Alert on 
                                 │ failure
                                 ▼
                        ┌─────────────────┐
                        │  Slack/Email    │
                        └─────────────────┘
```

---

## Features

| Feature | Description |
|---------|-------------|
| **Daily automation** | Cloud Scheduler triggers at 8 AM CST |
| **Incremental loads** | Delete-then-append by processing_date |
| **Schema validation** | Detects new/removed columns in source |
| **Error handling** | Retries, alerts, detailed logging |
| **Backfill support** | Fill historical data gaps |
| **Idempotent** | Safe to re-run for same date |

---

## Files Processed

| File | BigQuery Table | Primary Key | Records (typical) |
|------|----------------|-------------|-------------------|
| OrderDetails.csv | OrderDetails_raw | order_id | ~40/day |
| CheckDetails.csv | CheckDetails_raw | check_id | ~50/day |
| PaymentDetails.csv | PaymentDetails_raw | payment_id | ~60/day |
| ItemSelectionDetails.csv | ItemSelectionDetails_raw | item_selection_id | ~400/day |
| AllItemsReport.csv | AllItemsReport_raw | master_id, location | ~500 |
| CashEntries.csv | CashEntries_raw | entry_id | ~10/day |
| KitchenTimings.csv | KitchenTimings_raw | order_id, station | ~100/day |

---

## API Endpoints

### Health Check
```bash
GET /
```

### Run Pipeline (Single Date)
```bash
POST /run
Content-Type: application/json

{
  "processing_date": "20250129",  // optional, defaults to yesterday
  "backfill_days": 0              // optional
}
```

### Backfill Date Range
```bash
POST /backfill
Content-Type: application/json

{
  "start_date": "20251010",
  "end_date": "20250129"
}
```

### Table Status
```bash
GET /status/{table_name}
```

---

## Deployment

### Prerequisites
- GCP project with billing enabled
- `gcloud` CLI authenticated
- SFTP private key for Toast

### Quick Deploy
```bash
chmod +x deploy.sh
./deploy.sh
```

### Manual Steps

1. **Add SFTP key to Secret Manager**
```bash
gcloud secrets versions add toast-sftp-private-key \
    --data-file=/path/to/your/private_key
```

2. **Configure Slack alerts (optional)**
```bash
gcloud run services update toast-etl-pipeline \
    --update-env-vars SLACK_WEBHOOK_URL=https://hooks.slack.com/...
```

3. **Test the pipeline**
```bash
# Get auth token
TOKEN=$(gcloud auth print-identity-token)

# Run for specific date
curl -X POST \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"processing_date": "20250129"}' \
    https://toast-etl-pipeline-xxxxx-uc.a.run.app/run
```

---

## Backfilling Historical Data

Your BigQuery tables are stale since October 10, 2025. To catch up:

```bash
# Backfill ~3.5 months of data
curl -X POST \
    -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
    -H "Content-Type: application/json" \
    -d '{"start_date": "20251011", "end_date": "20250129"}' \
    https://toast-etl-pipeline-xxxxx-uc.a.run.app/backfill
```

**Note:** This will take several minutes to process ~110 days of data.

---

## Slack Notifications

The pipeline sends alerts to Slack on every run (success or failure).

### Alert Format

**Success:**
```
✅ Pipeline Run Complete
• Run ID: run_20260201_abc123
• Date Processed: 2026-01-31
• Status: SUCCESS
• Duration: 45.2s
• Files: 7 processed, 0 failed
• Total Rows: 2,910
```

**Failure:**
```
❌ Pipeline Run Complete
• Run ID: run_20260201_abc123
• Date Processed: 2026-01-31
• Status: FAILED
• Duration: 12.5s
• Files: 3 processed, 4 failed
• Total Rows: 850

Errors:
• Failed to download OrderDetails.csv: Connection timeout
• CheckDetails.csv: Schema validation failed
```

### Setup Slack Webhook

1. Go to https://api.slack.com/apps
2. Click **"Create New App"** → **"From scratch"**
3. Name it (e.g., "Toast ETL Alerts") and select your workspace
4. Click **"Incoming Webhooks"** in the left sidebar
5. Toggle **"Activate Incoming Webhooks"** to ON
6. Click **"Add New Webhook to Workspace"**
7. Select the channel for alerts (e.g., `#toast-alerts`)
8. Copy the Webhook URL

### Configure Cloud Run

```bash
gcloud run services update toast-etl-pipeline \
  --region=us-central1 \
  --update-env-vars="SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T.../B.../xxx"
```

### Test Notification

```bash
TOKEN=$(gcloud auth print-identity-token)
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"processing_date": "20260131"}' \
  https://toast-etl-pipeline-720125651862.us-central1.run.app/run
```

---

## Monitoring

### Cloud Logging
```bash
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=toast-etl-pipeline" \
    --limit=50 \
    --format="table(timestamp,textPayload)"
```

### Check Last Run
```bash
gcloud scheduler jobs describe toast-etl-daily \
    --location=us-central1 \
    --format="yaml(state,lastAttemptTime,scheduleTime)"
```

### BigQuery Data Freshness
```sql
SELECT 
    table_name,
    MAX(processing_date) as latest_date,
    COUNT(*) as total_rows
FROM `toast-analytics-444116.toast_raw.INFORMATION_SCHEMA.TABLES` t
JOIN (
    SELECT 'OrderDetails_raw' as table_name, MAX(processing_date) as processing_date FROM `toast_raw.OrderDetails_raw`
    UNION ALL
    SELECT 'CheckDetails_raw', MAX(processing_date) FROM `toast_raw.CheckDetails_raw`
    -- etc
)
GROUP BY 1
```

---

## Troubleshooting

### "No files found for date"
Toast typically uploads files by 5 AM CST. If missing:
- Check Toast admin for data export status
- Verify SFTP connectivity

### "Schema changes detected"
Pipeline logs new/removed columns but continues processing. Review changes:
```bash
gcloud logging read "textPayload:\"Schema changes detected\"" --limit=10
```

### Authentication errors
Ensure secrets are accessible:
```bash
gcloud secrets versions access latest --secret=toast-sftp-private-key | head -c 50
```

---

## Cost Estimate

| Resource | Monthly Cost |
|----------|--------------|
| Cloud Run | ~$5 (minimal invocations) |
| Cloud Scheduler | Free (1 job) |
| BigQuery Storage | ~$0.50 (100MB) |
| BigQuery Queries | ~$1-5 (depends on usage) |
| **Total** | **~$7-12/month** |

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| GCP_PROJECT | GCP project ID | toast-analytics-444116 |
| BQ_DATASET | BigQuery dataset | toast_raw |
| SFTP_HOST | Toast SFTP host | (AWS Transfer) |
| SFTP_USER | SFTP username | lov3houston |
| SLACK_WEBHOOK_URL | Slack alerts | (none) |
| ALERT_EMAIL | Email alerts | maurice@lov3houston.com |

---

## Support

Pipeline maintained by LOV3 Houston operations team.  
For issues, check Cloud Logging first, then contact tech support.
