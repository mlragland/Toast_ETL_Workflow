# Toast ETL Pipeline — Claude Code Context

## What This Is
Flask app on Cloud Run for LOV3 Houston restaurant. Four functions:
1. **Daily ETL** — Toast POS SFTP → BigQuery (7 CSV file types, runs 6 AM CST via Cloud Scheduler)
2. **Financial dashboards** — 14 self-contained HTML pages + REST APIs for bank transaction categorization, P&L, labor, loyalty, budget tracking, etc.
3. **Weekly Report** — Automated weekly performance report sent via Slack (primary) to #lov3-leader-report. Fallback to email via SendGrid. Triggered every Tuesday 10 AM CST by Cloud Scheduler.
4. **SBA Financial Statements** — Standalone scripts generating lender-grade P&L statements from Toast POS + BofA data

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
weekly_report.py     → WeeklyReportGenerator (Slack primary, email fallback)
dashboards.py        → 14 HTML dashboard generators (pure string functions, no imports)
routes_etl.py        → Blueprint: /, /run, /backfill, /status/<table>, /weekly-report
routes_bank.py       → Blueprint: /upload-bank-csv, /bank-categories, /api/bank-transactions/*
routes_dashboards.py → Blueprint: 14 GET dashboard routes (thin wrappers)
routes_analytics.py  → Blueprint: all POST /api/* analytics endpoints
```

### Standalone Scripts (not deployed to Cloud Run)
```
sba_financial_statements.py      → SBA P&L generator (Toast POS revenue + BofA expenses → Excel)
toast_api_backfill.py            → Backfill BQ gaps via Toast REST API (orders/checks/payments)
toast_labor_pull.py              → Pull clock-in/out time entries from Toast Labor API
backfill_allitems_from_api.py    → Reconstruct AllItemsReport from Orders API for missing dates
```

### Documentation
```
SBA_FINANCIAL_STATEMENT_METHODOLOGY.md   → SBA P&L: data sources, hookah treatment, reconciliation
SBA_2025_FOLLOWUP_ITEMS.md               → Balance sheet inputs, tax recon, data gap inventory
PROMOTER_PAYOUT_METHODOLOGY.md           → /promoter-payout calc logic, order_date rule, category bucketing
```

## Key Business Rules (in config.py)
- **Business day cutoff:** 4 AM. Revenue at 1 AM Saturday = Friday's business day.
- **Service Charge (not gratuity):** 20% mandatory on every check. Legally a service charge under IRS Revenue Ruling 2012-18.
  - Waitstaff/Bartender checks: 70% to staff, 30% to LOV3
  - Bottle Manager station checks: 50% to staff, 50% to LOV3
  - Voluntary tips: 100% to staff always
- **SBA presentation:** Gross revenue (tips + full service charge + cash undeposited + sales tax in revenue). Gross labor (no pass-through deduction). EBITDA = Revenue - ALL expenses.
- **paid_date is STRING** in PaymentDetails_raw — always `CAST(paid_date AS DATETIME)`.
- **transaction_date is DATE** in BankTransactions_raw — use DATE params, not STRING.
- **Category hierarchy:** `{N}. {Section}/{Subcategory}` format.

## Hookah Revenue — Three Phases
1. **Mar 2024 – Mar 2025:** In-house POS sales (`sales_category = 'Hookah'`). Already in `net_sales`.
2. **Apr 2025:** $20K reclass from Predictive Insights Jan 2024 payment (hardcoded in `HOOKAH_RECLASS`).
3. **May 2025 – present:** Predictive Insights LLC monthly bank deposits (additive revenue, not in Toast POS).
- **Dec 2025:** $15K reclass. Also in `HOOKAH_RECLASS`.
- **Mar 2026:** $16,400 reclass. Also in `HOOKAH_RECLASS`.

## Service Charge & Staff Compensation Model
- 20% service charge on every check (legally a service charge, NOT gratuity)
- Bottle Manager is a POS station, not a person — rings walk-in bottle orders and pools revenue to waitstaff
- Composite tracked staff hourly rate: **$57.07/hr** (Q1 2026, 36 waitstaff/bartenders)
- Estimated true rate with cash tips: **$66-74/hr**
- LOV3 service charge retention generates ~$376K/year

## Weekly Report Delivery
- **Primary:** Slack webhook to #lov3-leader-report channel (ID: C0AU9S12362)
- **Fallback:** SendGrid email (currently expired as of Apr 4, 2026 — not renewed)
- **Schedule:** Cloud Scheduler `toast-weekly-report`, Tuesdays 10 AM CST
- **Env vars:** `SLACK_WEBHOOK_URL` (alerts), `SLACK_REPORT_WEBHOOK` (weekly report channel)
- SendGrid import is now optional — won't break if not installed

## Toast REST API
- **Auth:** OAuth2 via Secret Manager (`toast-api-client-id`, `toast-api-client-secret`, `toast-restaurant-guid`)
- **Scopes:** cashmgmt:read, config:read, kitchen:read, labor:read, menus:read, orders:read, restaurants:read, stock:read
- **Key endpoints:** `/orders/v2/orders`, `/labor/v1/timeEntries`, `/labor/v1/employees`, `/labor/v1/jobs`
- **Rate limits:** 5 req/sec ordersBulk, 20 req/sec general, 10K per 15 min
- **NOT available:** feedback:read (would need to request from Toast for guest feedback data)

## Deploy Commands
See `.claude/rules/deploy-safety.md` for full details.

```bash
# Standard deploy (requires Slack webhook)
SLACK_WEBHOOK_URL="..." ./deploy.sh

# Deploy with separate report channel webhook
SLACK_WEBHOOK_URL="..." SLACK_REPORT_WEBHOOK="..." ./deploy.sh

# Run locally
pip install -r requirements.txt && python main.py

# Generate SBA financial statements
pip install openpyxl && python sba_financial_statements.py

# Pull labor time entries
python toast_labor_pull.py --date 20250823
python toast_labor_pull.py --start 20250601 --end 20250630 --csv

# Backfill AllItemsReport from API
python backfill_allitems_from_api.py --dry-run
python backfill_allitems_from_api.py --date 20240510
python backfill_allitems_from_api.py --limit 10
```

## Code Style
- Python 3.11, type hints, dataclasses, logging to stdout
- Gunicorn entry point: `main:app`
- No circular imports — config/models → services → pipeline/weekly_report → routes_* → main

## GCP Environment
- **Project:** toast-analytics-444116 | **Region:** us-central1 | **Dataset:** toast_raw
- SFTP key in Secret Manager
- SendGrid API key in Secret Manager (expired Apr 4, 2026 — not renewed)
- Toast API credentials in Secret Manager
- Cloud Scheduler triggers `/run` daily at 6 AM CST, `/weekly-report` Tuesdays 10 AM CST

## BigQuery Tables
| Table | Source | Coverage |
|-------|--------|----------|
| OrderDetails_raw | SFTP + API backfill | Mar 2024 – present (~97%) |
| ItemSelectionDetails_raw | SFTP + CSV backfill | Mar 2024 – present (~97%) |
| CheckDetails_raw | SFTP + API backfill | Mar 2024 – present (~97%) |
| PaymentDetails_raw | SFTP + API backfill | Mar 2024 – present (~96%) |
| CashEntries_raw | SFTP + API backfill | Mar 2024 – present (~96%) |
| KitchenTimings_raw | SFTP | Mar 2024 – present (~96%) |
| AllItemsReport_raw | SFTP + API reconstruction | Mar 2024 – present (~95%) |
| BankTransactions_raw | BofA CSV upload | 2024 – present |
| BankCategoryRules | Manual | Expense categorization rules |
| CheckRegister | Google Sheets sync | Check reconciliation |

AllItemsReport was reconstructed from Toast Orders API for Mar-Nov 2024 (14,436 rows, 173 dates). 9 dates confirmed as closures (zero orders). Metrics and hierarchy fields are accurate; `parent_id` may not match SFTP export exactly.

## LOV3 Operations
- **Open:** Wednesday – Sunday | **Closed:** Monday, Tuesday
- **Managers:** Anthony Winn (Tony), Tiffany Loving — show on cash register, not always on POS orders
- **Security:** Lewis Security Services (external vendor, clocks into Toast Labor API but not POS)
- **Bussers:** Clock into Toast Labor API, paid via handwritten checks
- **Hosts, Runners, Barbacks, Cooks, Dishwashers:** All clock into Toast Labor API
- **Hookah operator:** Predictive Insights LLC (monthly bank deposits since May 2025)
- **Bottle Manager:** POS station (not a person) for walk-in bottle orders — revenue pooled to waitstaff

## SBA Loan Package Status
- **2025 P&L:** Generated — `LOV3_HTX_Financial_Statements_SBA.xlsx`
- **Q1 2026 Interim P&L:** Generated — `LOV3_HTX_Q1_2026_PL.xlsx`
- **Source of truth (2025):** `/Predictive_Models/PMG/VIC3/SBA Loan Artifacts/LOV3 P&L Analysis/LOV3_SBA_PL_Package_Updated.xlsx`
- **Business plan:** `/Predictive_Models/PMG/VIC3/SBA Loan Artifacts/VIC3 Business Plan/VIC3_SBA_Business_Plan_Full Narrative_Final.pdf`
- **Outstanding:** Balance sheet (needs inputs), tax reconciliation ($59K legal order needs clarification), CY 2025 tax return status
- **Follow-up items:** `SBA_2025_FOLLOWUP_ITEMS.md`

## External Systems (NOT in this codebase)
- **SevenRooms:** Reservation data stored in PostgreSQL (`sms_blast` DB) via SMS blast app at `/Dropbox/Developer/sms_blast_system/`. Requires Docker to access. Has `booked_by` field for attribution.
- **Twilio:** SMS marketing via sms_blast_system
- **Mailchimp:** Email marketing (separate subscription)
- **Slack:** Workspace `lov3htx.slack.com`, channels include #lov3-leader-report (C0AU9S12362)
