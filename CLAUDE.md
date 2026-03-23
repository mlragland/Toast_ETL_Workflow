# Toast ETL Pipeline — Claude Code Context

## What This Is
Flask app on Cloud Run for LOV3 Houston restaurant. Two functions:
1. **Daily ETL** — Toast POS SFTP → BigQuery (7 CSV file types, runs 6 AM CST via Cloud Scheduler)
2. **Financial dashboards** — 14 self-contained HTML pages + REST APIs for bank transaction categorization, P&L, labor, loyalty, budget tracking, etc.

**Owner:** Maurice Ragland | **Venue:** LOV3|HTX, Houston

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
- **Category hierarchy:** `{N}. {Section}/{Subcategory}` format.

## Deploy Commands
See `.claude/rules/deploy-safety.md` for full details.

```bash
# Standard deploy
SLACK_WEBHOOK_URL="..." ./deploy.sh

# Run locally
pip install -r requirements.txt && python main.py
```

## Code Style
- Python 3.11, type hints, dataclasses, logging to stdout
- Gunicorn entry point: `main:app`
- No circular imports — config/models → services → pipeline/weekly_report → routes_* → main

## GCP Environment
- **Project:** toast-analytics-444116 | **Region:** us-central1 | **Dataset:** toast_raw
- SFTP key and SendGrid API key in Secret Manager
- Cloud Scheduler triggers `/run` daily at 6 AM CST
