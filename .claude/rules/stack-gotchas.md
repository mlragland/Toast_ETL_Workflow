# Stack Gotchas — Lessons Rolled Forward From Prior Projects

<!-- Every line here cost real debugging time in a previous project. Sources: -->
<!-- sms_blast_system, lov3synch, Toast_ETL_Workflow, event-landing-sites. -->
<!-- Next.js / Tailwind / Supabase sections omitted — not this stack. -->

## SQL / Python workers
- `CAST(:param AS type)`, never `:param::type`, in SQLAlchemy `text()` queries.
- Raw SQL INSERTs must list ALL NOT NULL columns — model defaults don't apply outside the ORM.
- NULL in boolean OR logic needs `COALESCE(col, FALSE)`.
- asyncpg forbids concurrent operations on one session — sequence DB calls; `asyncio.gather` only for non-DB work.
- Python ≤3.10 `datetime.fromisoformat()` chokes on `+0000` offsets — use `dateutil.parser.isoparse()` (Toast API timestamps trigger this).
- UUID primary keys (`uuid4`) over integer IDs for anything distributed or externally ingested.
- CSV column names differ between vendor docs and real exports — verify with `head -1` before writing parsers.
- BigQuery `paid_date` in `PaymentDetails_raw` is STRING — always `CAST(paid_date AS DATETIME)` before datetime ops.
- BigQuery `bigquery.ScalarQueryParameter` for ALL date/user-supplied params — never f-string interpolation.

## External APIs / messaging
- NEVER send real SMS/email/Slack messages without explicit user confirmation — test mode / test channel first.
- NEVER guess API field names — read the handler + response schema and show the exact shape before writing consumers.
- NEVER type secrets from memory — read the live value from Secret Manager, curl-verify it works, then use it.
- Opt-in/opt-out state is owned by provider webhooks (Twilio/Mailchimp) — never mutate it directly.
- GET endpoints must never mutate — Slack/iMessage link-preview crawlers fire GETs. Confirmation page on GET, mutation on POST.
- User-facing timestamps in the user's timezone (America/Chicago for LOV3 work), never UTC.
- Google OAuth refresh tokens rotate — persist the new token immediately when returned.
- Silent HTTP bridges (Apps Script, fire-and-forget webhooks) report 0% errors even when SSL/DNS breaks. Monitor RECEIVER-side staleness, not sender telemetry (5-day silent SR gap in Jun 2026 caused by this).
- Resend + SendGrid: SendGrid API key expired 2026-04-04; new reports use Resend via `gratuity_report.py:send_email` pattern. Don't add new SendGrid callers.

## Cloud Run / GCP
- Cloud Run keeps previous revisions as the rollback safety net — never delete them.
- Env var changes require a redeploy to take effect (no hot reload).
- `gcloud run deploy` without `--set-env-vars` preserves existing env — safer than re-running full `deploy.sh` for code-only changes.
- Cloud Build in `us-central1` billed per-second — a full container rebuild is ~90-120s.
- Cloud Scheduler timezones are IANA (`America/Chicago`), not `CST` — the latter silently falls back to UTC.
- `gcloud builds submit` bundles the local working directory INCLUDING untracked files. A deploy that works on your machine can silently break on a clean checkout because the bundled untracked file is missing. Always commit any file imported by production code before deploying.
- Module-level imports of optional deps (openpyxl, reportlab, weasyprint) crash Cloud Run cold start even for routes that never use them. Add the dep to requirements.txt OR lazy-import inside the functions that need it.
