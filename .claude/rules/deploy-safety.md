# Deploy Safety Rules — Toast ETL Pipeline

## Deploy Pattern
```bash
# 1. Verify locally first
python -c "from main import app; print('OK')"

# 2. Deploy to Cloud Run
SLACK_WEBHOOK_URL="..." ./deploy.sh
```
deploy.sh runs `gcloud builds submit` (builds container in cloud) then `gcloud run deploy`.

## Rollback
Cloud Run keeps previous revisions. If a deploy breaks something:
```bash
# List recent revisions
gcloud run revisions list --service=toast-etl-pipeline --region=us-central1 --limit=5

# Rollback to previous revision
gcloud run services update-traffic toast-etl-pipeline \
    --to-revisions=PREVIOUS_REVISION_NAME=100 --region=us-central1
```

## Post-Deploy Verification
```bash
# Check the service is healthy
curl -s https://toast-etl-pipeline-t3di7qky4q-uc.a.run.app/ | python -c "import json,sys; print(json.load(sys.stdin)['status'])"

# Check a dashboard renders
curl -s https://toast-etl-pipeline-t3di7qky4q-uc.a.run.app/bank-review | head -1
```

## Dockerfile
- `COPY *.py .` — all Python modules in the root are deployed
- Gunicorn serves `main:app` with 1 worker, 8 threads, 300s timeout
- If you add a subdirectory (e.g., `utils/`), update the COPY command

## NEVER Do
- Deploy without running the import check first
- Push secrets to GitHub (push protection will block it anyway)
- Change `main:app` entry point without updating the Dockerfile CMD
- Delete Cloud Run revisions — they're your rollback safety net
