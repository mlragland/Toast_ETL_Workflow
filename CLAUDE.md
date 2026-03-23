# Toast ETL Pipeline

## What This Is
Flask app on Cloud Run for LOV3 Houston restaurant. Two functions:
1. **Daily ETL** — Toast POS SFTP → BigQuery (7 CSV file types, runs 6 AM CST via Cloud Scheduler)
2. **Financial dashboards** — 14 self-contained HTML pages + REST APIs for bank transaction categorization, P&L, labor, loyalty, budget tracking, etc.

## Module Structure
```
main.py              → Flask app entry point (registers 4 blueprints)
config.py            → All constants, env vars, business rules, schemas
models.py            → Dataclasses (PipelineResult, PipelineRunSummary, BankUploadResult)
services.py          → Business logic (BofACSVParser, BankCategoryManager, CheckRegisterSync,
                        SecretManager, ToastSFTPClient, SchemaValidator, DataTransformer,
                        BigQueryLoader, AlertManager)
pipeline.py          → ToastPipeline orchestrator (SFTP → transform → BigQuery)
weekly_report.py     → WeeklyReportGenerator (email reports with inline HTML)
dashboards.py        → 14 HTML dashboard generators (pure string functions, no imports)
routes_etl.py        → Blueprint: /, /run, /backfill, /status/<table>, /weekly-report
routes_bank.py       → Blueprint: /upload-bank-csv, /bank-categories, /api/bank-transactions/*
routes_dashboards.py → Blueprint: 14 GET dashboard routes (thin wrappers)
routes_analytics.py  → Blueprint: all POST /api/* analytics endpoints
```

## Key Business Rules (in config.py)
- **Business day cutoff:** 4 AM. Revenue at 1 AM Saturday = Friday's business day.
- **Gratuity split:** House retains 35%, 65% passes through to staff. Tips are 100% staff.
- **True labor:** Gross labor debits minus tip/gratuity pass-through.
- **paid_date is STRING** in PaymentDetails_raw — always `CAST(paid_date AS DATETIME)`.
- **Category hierarchy:** `{N}. {Section}/{Subcategory}` format. E.g. `5. Operating Expenses (OPEX)/Permits & Licenses`.
- **Toast ACH patterns:** `DES:DEP` = daily deposit (Revenue), `DES:EOM` = month-end (Revenue), `DES:REF` = refund (OPEX/POS).

## Common Commands

```bash
# Deploy to Cloud Run
SLACK_WEBHOOK_URL="..." ./deploy.sh

# Run locally
pip install -r requirements.txt
python main.py
# → http://localhost:8080/

# Check Cloud Run logs
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=toast-etl-pipeline" --limit=20

# Manual ETL run
TOKEN=$(gcloud auth print-identity-token)
curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d '{"processing_date": "20250129"}' \
    https://toast-etl-pipeline-t3di7qky4q-uc.a.run.app/run
```

## Code Style
- Python 3.11, type hints on all functions, dataclasses for structured data
- Gunicorn entry point: `main:app`
- Logging to stdout (Cloud Logging picks it up)
- No circular imports — dependency flow: config/models → services → pipeline/weekly_report → routes_* → main

## GCP Environment
- **Project:** toast-analytics-444116 | **Region:** us-central1 | **Dataset:** toast_raw
- SFTP key and SendGrid API key stored in Secret Manager
- Cloud Scheduler triggers `/run` daily at 6 AM CST

## Gotchas
- Bank CSV upload is idempotent by file hash — re-uploading the same file is a no-op
- Nav bar HTML is duplicated across all 14 dashboards in dashboards.py (future: extract shared helper)
- `EVENT_VENDOR_MAP` in config.py maps vendors to specific weekly events — first match wins, so more specific keywords must come before general ones
- Check register syncs from Google Sheet on every `/upload-bank-csv` call; use `/api/reconcile-checks` to re-categorize without re-uploading
