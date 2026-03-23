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

## Security Rules
- **Never commit secrets.** API keys, webhook URLs, SFTP keys, Sheet IDs, and service account emails go in Secret Manager or env vars — never in source files. deploy.sh reads `SLACK_WEBHOOK_URL` from env.
- **SQL injection risk.** Several routes in routes_analytics.py build BigQuery SQL with f-strings using user-supplied date params. Always validate date inputs (`YYYY-MM-DD` format) before interpolating. Prefer parameterized queries for any new endpoints.
- **No DELETE/DROP in BigQuery.** Bank transaction deletion uses row-level DML (`DELETE WHERE`), not table drops. Never add `DROP TABLE` or `DELETE` without a `WHERE` clause.
- **Public endpoint.** Cloud Run allows `allUsers` invoker access (for dashboards). Auth-sensitive routes (`/run`, `/backfill`, `/weekly-report`) check for OIDC tokens. Do not add routes that mutate data without auth.

## Verification (before every deploy)
```bash
# 1. Import check — catches broken imports instantly
python -c "from main import app; print('OK')"

# 2. Smoke test — run locally, hit key endpoints
python main.py &
curl -s http://localhost:8080/ | python -c "import json,sys; print(json.load(sys.stdin)['status'])"
curl -s http://localhost:8080/bank-review | head -1  # should be <!DOCTYPE html>
kill %1

# 3. After deploying — verify Cloud Run revision is serving
gcloud run services describe toast-etl-pipeline --region=us-central1 --format='value(status.url)'
```
No test suite exists yet. The import check + smoke test is the minimum gate before deploying.

## Error Recovery
- **Bad deploy:** Cloud Run keeps previous revisions. Rollback: `gcloud run services update-traffic toast-etl-pipeline --to-revisions=PREVIOUS_REVISION=100 --region=us-central1`
- **Broken ETL run:** Safe to re-run for the same date — data is idempotent (deduplication by primary key + processing_date).
- **Bad bank categorization:** Re-upload the same CSV (idempotent by hash) or call `/api/reconcile-checks` to re-categorize checks. Manual fixes via `/api/bank-transactions/categorize`.
- **Check CLAUDE.md after structural changes.** If you add/move modules, update the Module Structure section so future sessions have accurate context.

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
- Several analytics routes create a new `bigquery.Client()` per request — acceptable for Cloud Run but worth noting for future optimization
