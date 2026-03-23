#!/bin/bash
# Toast ETL Pipeline - Deployment Script
# LOV3 Houston - Toast Analytics

set -e

# Configuration
PROJECT_ID="toast-analytics-444116"
REGION="us-central1"
SERVICE_NAME="toast-etl-pipeline"
DATASET_ID="toast_raw"
SERVICE_ACCOUNT_NAME="toast-etl-scheduler"
SLACK_WEBHOOK_URL="${SLACK_WEBHOOK_URL:?Set SLACK_WEBHOOK_URL env var before deploying}"

echo "=========================================="
echo "Toast ETL Pipeline Deployment"
echo "=========================================="

# Ensure we're using the right project
gcloud config set project $PROJECT_ID

# ==========================================
# 1. CREATE SERVICE ACCOUNT
# ==========================================
echo ""
echo "[1/7] Creating service account..."

# Create scheduler service account
gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME \
    --display-name="Toast ETL Scheduler" \
    --description="Service account for Cloud Scheduler to invoke Cloud Run" \
    2>/dev/null || echo "Service account already exists"

# Get the full email
SA_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# ==========================================
# 2. SET UP SECRET MANAGER
# ==========================================
echo ""
echo "[2/7] Setting up Secret Manager..."

# Create secret for SFTP private key (you'll need to add the actual key)
gcloud secrets create toast-sftp-private-key \
    --replication-policy="automatic" \
    2>/dev/null || echo "Secret already exists"

# Create secret for SendGrid API key
gcloud secrets create sendgrid-api-key \
    --replication-policy="automatic" \
    2>/dev/null || echo "Secret already exists"

echo ""
echo "⚠️  IMPORTANT: Add your SFTP private key to Secret Manager:"
echo "   gcloud secrets versions add toast-sftp-private-key --data-file=/path/to/your/private_key"
echo ""
echo "⚠️  IMPORTANT: Add your SendGrid API key to Secret Manager:"
echo "   echo 'SG.your-api-key' | gcloud secrets versions add sendgrid-api-key --data-file=-"
echo ""

# Get the project number for the compute service account
PROJECT_NUMBER=$(gcloud projects describe ${PROJECT_ID} --format='value(projectNumber)')

# Grant Cloud Run access to secrets
gcloud secrets add-iam-policy-binding toast-sftp-private-key \
    --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
    --role="roles/secretmanager.secretAccessor"

gcloud secrets add-iam-policy-binding sendgrid-api-key \
    --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
    --role="roles/secretmanager.secretAccessor"

# ==========================================
# 3. ENABLE REQUIRED APIs
# ==========================================
echo ""
echo "[3/7] Enabling required APIs..."

gcloud services enable \
    run.googleapis.com \
    cloudbuild.googleapis.com \
    cloudscheduler.googleapis.com \
    secretmanager.googleapis.com \
    bigquery.googleapis.com

# ==========================================
# 4. BUILD AND DEPLOY CLOUD RUN
# ==========================================
echo ""
echo "[4/7] Building and deploying to Cloud Run..."

# Build the container
gcloud builds submit --tag gcr.io/${PROJECT_ID}/${SERVICE_NAME}

# Deploy to Cloud Run
gcloud run deploy $SERVICE_NAME \
    --image gcr.io/${PROJECT_ID}/${SERVICE_NAME} \
    --platform managed \
    --region $REGION \
    --memory 2Gi \
    --cpu 2 \
    --timeout 600 \
    --min-instances 0 \
    --max-instances 3 \
    --set-env-vars "GCP_PROJECT=${PROJECT_ID},BQ_DATASET=${DATASET_ID},SLACK_WEBHOOK_URL=${SLACK_WEBHOOK_URL}" \
    --no-allow-unauthenticated

# Get the Cloud Run URL
CLOUD_RUN_URL=$(gcloud run services describe $SERVICE_NAME --region $REGION --format='value(status.url)')
echo "Cloud Run URL: $CLOUD_RUN_URL"

# ==========================================
# 5. GRANT IAM PERMISSIONS
# ==========================================
echo ""
echo "[5/7] Configuring IAM permissions..."

# Allow scheduler to invoke Cloud Run
gcloud run services add-iam-policy-binding $SERVICE_NAME \
    --region=$REGION \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/run.invoker"

# Allow public access (for /bank-review dashboard)
gcloud run services add-iam-policy-binding $SERVICE_NAME \
    --region=$REGION \
    --member="allUsers" \
    --role="roles/run.invoker"

# Grant BigQuery permissions to Cloud Run service account
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
    --role="roles/bigquery.jobUser"

# ==========================================
# 6. CREATE CLOUD SCHEDULER JOB
# ==========================================
echo ""
echo "[6/7] Creating Cloud Scheduler job..."

# Delete existing job if it exists
gcloud scheduler jobs delete toast-etl-daily --location=$REGION --quiet 2>/dev/null || true

# Create daily scheduler job (6 AM CST = 12:00 UTC)
gcloud scheduler jobs create http toast-etl-daily \
    --location=$REGION \
    --schedule="0 12 * * *" \
    --time-zone="America/Chicago" \
    --uri="${CLOUD_RUN_URL}/run" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --oidc-service-account-email="${SA_EMAIL}" \
    --message-body='{"processing_date": null, "backfill_days": 0}' \
    --attempt-deadline="600s"

echo "Scheduler job created: toast-etl-daily (runs at 8 AM CST daily)"

# Delete existing weekly report job if it exists
gcloud scheduler jobs delete toast-weekly-report --location=$REGION --quiet 2>/dev/null || true

# Create weekly report scheduler job (10 AM CST on Tuesday = 16:00 UTC)
gcloud scheduler jobs create http toast-weekly-report \
    --location=$REGION \
    --schedule="0 10 * * 2" \
    --time-zone="America/Chicago" \
    --uri="${CLOUD_RUN_URL}/weekly-report" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --oidc-service-account-email="${SA_EMAIL}" \
    --message-body='{}' \
    --attempt-deadline="300s"

echo "Scheduler job created: toast-weekly-report (runs at 10 AM CST every Tuesday)"

# ==========================================
# 7. TEST THE DEPLOYMENT
# ==========================================
echo ""
echo "[7/7] Testing deployment..."

# Get auth token
TOKEN=$(gcloud auth print-identity-token)

# Test health endpoint
echo "Testing health endpoint..."
curl -s -H "Authorization: Bearer $TOKEN" "${CLOUD_RUN_URL}/" | jq .

echo ""
echo "=========================================="
echo "✅ Deployment Complete!"
echo "=========================================="
echo ""
echo "Cloud Run Service: $CLOUD_RUN_URL"
echo "Scheduler Jobs:"
echo "  - toast-etl-daily (8 AM CST daily)"
echo "  - toast-weekly-report (10 AM CST every Tuesday)"
echo ""
echo "Next Steps:"
echo "1. Add SFTP private key to Secret Manager:"
echo "   gcloud secrets versions add toast-sftp-private-key --data-file=/path/to/key"
echo ""
echo "2. Add SendGrid API key to Secret Manager:"
echo "   echo 'SG.your-api-key' | gcloud secrets versions add sendgrid-api-key --data-file=-"
echo ""
echo "3. (Optional) Add Slack webhook for alerts:"
echo "   gcloud run services update $SERVICE_NAME --update-env-vars SLACK_WEBHOOK_URL=your_webhook"
echo ""
echo "4. Test the pipeline manually:"
echo "   curl -X POST -H 'Authorization: Bearer \$(gcloud auth print-identity-token)' \\"
echo "        -H 'Content-Type: application/json' \\"
echo "        -d '{\"processing_date\": \"20250129\"}' \\"
echo "        ${CLOUD_RUN_URL}/run"
echo ""
echo "5. Test the weekly report:"
echo "   curl -X POST -H 'Authorization: Bearer \$(gcloud auth print-identity-token)' \\"
echo "        -H 'Content-Type: application/json' \\"
echo "        ${CLOUD_RUN_URL}/weekly-report"
echo ""
echo "6. Backfill historical data:"
echo "   curl -X POST -H 'Authorization: Bearer \$(gcloud auth print-identity-token)' \\"
echo "        -H 'Content-Type: application/json' \\"
echo "        -d '{\"start_date\": \"20251010\", \"end_date\": \"20250129\"}' \\"
echo "        ${CLOUD_RUN_URL}/backfill"
echo ""
