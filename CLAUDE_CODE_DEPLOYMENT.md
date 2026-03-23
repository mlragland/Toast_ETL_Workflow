# Deploying Toast ETL Pipeline with Claude Code

This guide walks you through deploying the Toast data pipeline using Claude Code on your local machine.

---

## Prerequisites

Before starting, ensure you have:

1. **Claude Code installed**
   ```bash
   # If not installed:
   npm install -g @anthropic-ai/claude-code
   ```

2. **Google Cloud SDK installed and authenticated**
   ```bash
   # Check if installed
   gcloud --version
   
   # If not installed, download from:
   # https://cloud.google.com/sdk/docs/install
   
   # Authenticate
   gcloud auth login
   gcloud auth application-default login
   ```

3. **Your Toast SFTP private key file** (the .pem file you use to connect to Toast SFTP)

---

## Step 1: Set Up Project Directory

Open your terminal and run:

```bash
# Create project directory
mkdir -p ~/projects/toast-pipeline
cd ~/projects/toast-pipeline

# Download the pipeline files (from Claude's output) or create them:
# Option A: If you downloaded the zip
unzip ~/Downloads/toast-pipeline.zip -d .

# Option B: Start Claude Code and have it create the files
claude
```

---

## Step 2: Start Claude Code Session

```bash
cd ~/projects/toast-pipeline
claude
```

Once Claude Code starts, you can give it commands like a conversation.

---

## Step 3: Initial Setup Commands

Copy and paste this into Claude Code:

```
I need to deploy the Toast ETL pipeline to Google Cloud. Here's what I need:

1. First, verify I have the required tools:
   - gcloud CLI installed and authenticated
   - Docker installed (for local testing)

2. Set my GCP project:
   gcloud config set project toast-analytics-444116

3. Check if required APIs are enabled
```

Claude Code will run these checks and report status.

---

## Step 4: Add SFTP Key to Secret Manager

Tell Claude Code:

```
I need to add my Toast SFTP private key to Secret Manager. 
My key file is at: /path/to/your/toast-sftp-key.pem

Please:
1. Create the secret if it doesn't exist
2. Add the key as a new version
3. Verify it was stored correctly
```

**Replace `/path/to/your/toast-sftp-key.pem` with your actual key path.**

Claude Code will execute:
```bash
gcloud secrets create toast-sftp-private-key --replication-policy="automatic" 2>/dev/null || true
gcloud secrets versions add toast-sftp-private-key --data-file=/path/to/your/toast-sftp-key.pem
gcloud secrets versions access latest --secret=toast-sftp-private-key | head -c 50
```

---

## Step 5: Deploy to Cloud Run

Tell Claude Code:

```
Deploy the Toast ETL pipeline to Cloud Run:

1. Build the Docker container
2. Push to Google Container Registry  
3. Deploy to Cloud Run with:
   - 2GB memory
   - 2 CPUs
   - 10 minute timeout
   - No public access (authenticated only)
4. Show me the service URL when done
```

Claude Code will execute the build and deployment:
```bash
gcloud builds submit --tag gcr.io/toast-analytics-444116/toast-etl-pipeline

gcloud run deploy toast-etl-pipeline \
    --image gcr.io/toast-analytics-444116/toast-etl-pipeline \
    --platform managed \
    --region us-central1 \
    --memory 2Gi \
    --cpu 2 \
    --timeout 600 \
    --set-env-vars "GCP_PROJECT=toast-analytics-444116,BQ_DATASET=toast_raw" \
    --no-allow-unauthenticated
```

---

## Step 6: Set Up IAM Permissions

Tell Claude Code:

```
Set up the IAM permissions for the pipeline:

1. Create a service account for Cloud Scheduler
2. Grant it permission to invoke the Cloud Run service
3. Grant the Cloud Run service account access to:
   - BigQuery (data editor + job user)
   - Secret Manager (to read the SFTP key)
```

---

## Step 7: Create Cloud Scheduler Job

Tell Claude Code:

```
Create a Cloud Scheduler job that:
- Runs daily at 6 AM Central Time
- Calls the /run endpoint on the Cloud Run service
- Uses OIDC authentication
- Has 3 retries on failure
```

Claude Code will create the scheduler:
```bash
gcloud scheduler jobs create http toast-etl-daily \
    --location=us-central1 \
    --schedule="0 6 * * *" \
    --time-zone="America/Chicago" \
    --uri="https://toast-etl-pipeline-XXXXX-uc.a.run.app/run" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --oidc-service-account-email="toast-etl-scheduler@toast-analytics-444116.iam.gserviceaccount.com" \
    --message-body='{}' \
    --attempt-deadline="600s"
```

---

## Step 8: Test the Pipeline

Tell Claude Code:

```
Test the pipeline by:
1. First checking the health endpoint
2. Then running it for yesterday's data
3. Show me the response
```

Claude Code will:
```bash
# Get auth token
TOKEN=$(gcloud auth print-identity-token)

# Health check
curl -s -H "Authorization: Bearer $TOKEN" "https://YOUR-URL/" | jq .

# Run pipeline
curl -s -X POST \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{}' \
    "https://YOUR-URL/run" | jq .
```

---

## Step 9: Backfill Historical Data

Your BigQuery data stopped in October 2025. Tell Claude Code:

```
Backfill the missing Toast data from October 11, 2025 through yesterday.
This will take a while - show me progress as it runs.
```

Claude Code will execute:
```bash
curl -X POST \
    -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
    -H "Content-Type: application/json" \
    -d '{"start_date": "20251011", "end_date": "20250129"}' \
    "https://YOUR-URL/backfill"
```

**Note:** This processes ~110 days of data and may take 10-15 minutes.

---

## Step 10: (Optional) Add Slack Alerts

If you want Slack notifications:

```
Add a Slack webhook for pipeline alerts:
https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

Claude Code will update the Cloud Run environment:
```bash
gcloud run services update toast-etl-pipeline \
    --region=us-central1 \
    --update-env-vars "SLACK_WEBHOOK_URL=https://hooks.slack.com/services/..."
```

---

## Verification Checklist

After deployment, verify everything is working:

| Check | Command |
|-------|---------|
| Cloud Run service exists | `gcloud run services list` |
| Scheduler job exists | `gcloud scheduler jobs list --location=us-central1` |
| Secret is accessible | `gcloud secrets versions list toast-sftp-private-key` |
| BigQuery tables updated | Check in BigQuery Console |

---

## Common Claude Code Commands

Once deployed, you can use Claude Code for ongoing management:

```
# Check pipeline status
"Show me the last 5 Cloud Run logs for toast-etl-pipeline"

# Manually trigger a run
"Run the Toast pipeline for January 28, 2025"

# Check data freshness
"Query BigQuery to show the latest processing_date in each toast_raw table"

# Debug issues
"Show me any errors in the toast-etl-pipeline logs from today"
```

---

## Troubleshooting

### "Permission denied" errors
```
Grant my user account the required IAM roles for this project
```

### "Secret not found" errors
```
Verify the toast-sftp-private-key secret exists and has a valid version
```

### "SFTP connection failed"
```
Test the SFTP connection manually to verify credentials work
```

### Pipeline runs but no data loads
```
Check the Cloud Run logs for the most recent run and show me any errors
```

---

## Cost Summary

| Resource | Monthly Cost |
|----------|--------------|
| Cloud Run | ~$5 |
| Cloud Scheduler | Free |
| BigQuery Storage | ~$0.50 |
| BigQuery Queries | ~$2-5 |
| Secret Manager | ~$0.06 |
| **Total** | **~$8-12/month** |

---

## Next Steps

After successful deployment:

1. **Monitor first few automated runs** - Check Cloud Logging the morning after deployment
2. **Set up BigQuery scheduled queries** - Create daily summary views for LOV3 analytics
3. **Connect to Looker/Data Studio** - Build dashboards on toast_raw tables
4. **Add additional alerting** - Set up budget alerts and uptime checks
