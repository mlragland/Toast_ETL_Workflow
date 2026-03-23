# ETL Pipeline Rules

## Daily Toast Ingestion
- Pipeline runs at 6 AM CST via Cloud Scheduler hitting `/run`
- Toast files available on SFTP by ~5 AM CST — 7 CSV types
- Data is idempotent — safe to re-run for the same date (dedup by primary key + processing_date)
- Schema changes are logged but don't block processing

## Bank CSV Upload
- Upload is idempotent by file hash — re-uploading the same file is a no-op
- Check register syncs from Google Sheet automatically on every `/upload-bank-csv` call
- To re-categorize checks without re-uploading: `POST /api/reconcile-checks`
- `_categorize()` detects Toast ACH patterns BEFORE keyword matching — order matters

## BigQuery Patterns
- `paid_date` is STRING in PaymentDetails_raw — ALWAYS use `CAST(paid_date AS DATETIME)` before datetime ops
- Business day SQL uses `BUSINESS_DAY_SQL.format(dt_col=...)` from config.py — 4 AM cutoff
- Several analytics routes create a new `bigquery.Client()` per request — acceptable for Cloud Run cold starts
- `EVENT_VENDOR_MAP` keys are matched case-insensitively, first match wins — specific keywords before general ones

## Backfill
```bash
TOKEN=$(gcloud auth print-identity-token)
curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d '{"start_date": "20251011", "end_date": "20250129"}' \
    https://toast-etl-pipeline-t3di7qky4q-uc.a.run.app/backfill
```
