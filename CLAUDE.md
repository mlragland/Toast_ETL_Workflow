# Toast ETL Pipeline - Claude Code Context

## Project Overview
This is the Toast SFTP to BigQuery ETL pipeline for LOV3 Houston restaurant. It ingests daily POS data from Toast's SFTP server and loads it to BigQuery for analytics.

## Key Information

### GCP Configuration
- **Project ID:** toast-analytics-444116
- **Region:** us-central1
- **Dataset:** toast_raw
- **Service Name:** toast-etl-pipeline

### SFTP Configuration
- **Host:** s-93c7f3e3febd45809.server.transfer.us-east-1.amazonaws.com
- **Port:** 22
- **User:** lov3houston
- **Key Secret:** toast-sftp-private-key (in Secret Manager)

### Files Processed Daily
| File | BigQuery Table |
|------|----------------|
| OrderDetails.csv | OrderDetails_raw |
| CheckDetails.csv | CheckDetails_raw |
| PaymentDetails.csv | PaymentDetails_raw |
| ItemSelectionDetails.csv | ItemSelectionDetails_raw |
| AllItemsReport.csv | AllItemsReport_raw |
| CashEntries.csv | CashEntries_raw |
| KitchenTimings.csv | KitchenTimings_raw |

### Bank of America Integration (Manual Upload)
| BigQuery Table | Description |
|----------------|-------------|
| BankTransactions_raw | Uploaded BofA CSV transactions with auto-categorization |
| BankCategoryRules | Vendor keyword -> expense category mapping rules |
| CheckRegister | Google Sheet check register (check_number → payee mapping) |

### Bank API Endpoints
- **`POST /upload-bank-csv`** - Upload BofA CSV (multipart file). Auto-categorizes and loads to BigQuery. Idempotent by file hash.
- **`GET /bank-categories`** - List all auto-categorization rules.
- **`POST /bank-categories`** - Add/update/delete rules (`{"action":"upsert","keyword":"SYSCO","category":"COGS/Food"}` or `{"action":"delete","keyword":"SYSCO"}`).
- **`POST /profit-summary`** - P&L combining Toast revenue + bank expenses. Body: `{"start_date":"2025-01-01","end_date":"2025-01-31"}`.
- **`POST /comprehensive-analysis`** - Full financial analysis with monthly P&L, revenue by business day, hourly profile. Same body format as profit-summary.
- **`POST /sync-check-register`** - Sync Google Sheet check register → BigQuery `CheckRegister` table. Returns `{"rows_synced": N}`.
- **`POST /upload-check-register`** - Upload check register CSV (fallback). Multipart file with columns: `check_number`, `payee`, and optionally `category`, `amount`, `memo`.
- **`POST /api/cash-recon`** - Cash reconciliation: POS collections vs bank deposits. Body: `{"start_date":"2025-09-01","end_date":"2026-02-28"}`. Returns monthly credit card and cash recon with cumulative diffs, status badges, and alerts.
- **`POST /api/menu-mix`** - Menu mix / item analysis from ItemSelectionDetails. Body: `{"start_date":"2025-12-01","end_date":"2026-02-27"}`. Returns top 20 items, category breakdown, service period performance, day-of-week and hourly profiles.
- **`POST /api/events-calendar`** - Events calendar with weekly revenue overlay. Body: `{"year": 2026}`. Returns events, weekly revenue (current + prior year), top 20 revenue weeks with event tagging, upcoming events with historical context, KPIs, and computed insights.
- **`POST /api/server-performance`** - Server rankings and performance from OrderDetails + PaymentDetails. Body: `{"start_date":"2025-12-01","end_date":"2026-02-27"}`. Returns server leaderboard (revenue, orders, avg check, guests, tips, discounts), per-server DOW and hourly breakdowns, KPIs.
- **`POST /api/kitchen-speed`** - Kitchen fulfillment speed from KitchenTimings_raw. Body: `{"start_date":"2025-12-01","end_date":"2026-02-27"}`. Returns station performance (avg/median/min/max fulfillment times), hourly speed profile, cook leaderboard, weekly trend, KPIs.
- **`POST /api/labor-analysis`** - Labor cost analysis: weekly/monthly true labor vs revenue, vendor breakdown. Body: `{"start_date":"2025-09-01","end_date":"2026-02-27"}`. Returns weekly labor trend with labor %, monthly summary with prime cost %, labor vendor breakdown, KPIs. Uses LOV3 gratuity split for true labor calculation.
- **`POST /api/reconcile-checks`** - Re-categorize uncategorized Check transactions using current register. Syncs register from Google Sheet, re-runs categorization for all `Check XXXX` rows still marked Uncategorized. Returns reconciled count + details. No body required.
- **`POST /api/menu-engineering`** - Menu engineering matrix from ItemSelectionDetails. Body: `{"start_date":"2025-12-01","end_date":"2026-02-27"}`. Classifies items as Stars/Plowhorses/Puzzles/Dogs using popularity (qty vs avg) and profitability (revenue per item vs avg). Returns item classifications, category breakdown, matrix thresholds, KPIs.
- **`POST /api/customer-loyalty`** - Guest intelligence: card-based RFM segmentation & analytics. Body: `{"start_date":"2025-06-01","end_date":"2026-02-28"}`. Uses PaymentDetails_raw card last-4 + card_type as guest proxy. 4 BQ queries: card-level RFM aggregates, monthly guest flow (new vs returning), DOW+hourly patterns by frequency tier, contact enrichment (CheckDetails + PaymentDetails join). Python: segment assignment (Champions/Loyal/Regulars/Returning/New/At Risk/Dormant), revenue concentration (top 5/10/20/50%), frequency distribution bands, tip analysis, phone cleanup. Returns segments, concentration, freq_distribution, monthly trend, timing patterns, top 50 repeat guests, enriched contacts with email/phone.
- **`GET /api/guest-export`** - CSV export of enriched guest contacts for SevenRooms CRM import. Params: `?start_date=2025-06-01&end_date=2026-02-28`. Returns downloadable CSV with columns: first_name, last_name, email, phone, visits, total_spend, avg_check, first_visit, last_visit, segment, tags. Tags include segment name + spend tier + visit tier. Phone numbers cleaned (stripped .0, filtered dummy 555s, formatted +1XXXXXXXXXX).
- **`POST /api/budget`** - Budget tracker: actual vs target spending for 15% profit margin goal. Body: `{"month": "2026-03"}` (defaults to current month). Returns per-category budget (COGS/Labor/Marketing/OPEX) with target %, actual %, variance, status, top vendors; 12-month trend; path-to-15% recommendations with priority-ranked savings; insights sorted by severity.
- **`POST /api/event-roi`** - Event ROI analysis for 6 recurring weekly events. Body: `{"start_date":"2025-09-01","end_date":"2026-02-28"}`. Revenue by DOW (business-day aware, PaymentDetails_raw). Direct costs via `EVENT_VENDOR_MAP` vendor-to-event mapping. Shared costs (social media, flyers, ads) allocated by revenue share. Returns per-event revenue, costs, ROI%, margin%, monthly trend, unattributed vendors, insights. Monday is dark ($0 baseline).

### LOV3 Bank Accounts (BofA)
| Last 4 | Account | Notes |
|--------|---------|-------|
| 9121 | LOV3 Cash Account | POS cash deposited here first, then transferred to operating. Inbound transfers from 9121 = deposited cash. |
| 9439 | PMG Artist Account | Outbound transfers for artist/entertainment payments. |
| 0227 | Eddie Jasper Personal | Used for initial payroll payments (mostly outbound). |
| 4243 | Lewis Security Services | Weekly security contractor payments ($3K-$6K each). |
| 4115 | BAED Corporation | Small occasional transfers. |
| 8306 | JL Watkins LLC | Rare transfers. |
| 8949 | (Unknown) | Active mid-2024 only, mostly closed out. |

### Check Register Integration
- **Google Sheet ID:** `1IAquzS-GES3A7-Cxj1ICbdg3fcSJF8BGttId-NPviIY` (sheet: `check_register_master`)
- **Service account:** `720125651862-compute@developer.gserviceaccount.com` (needs Viewer share on the sheet)
- On each `/upload-bank-csv` call, the check register is synced from the Google Sheet automatically.
- Bank transactions matching `Check XXXX` are looked up in the register to resolve the payee.
- The payee is then run through keyword rules; `category_source` is set to `"check_register"`.
- If no keyword rule matches, `vendor_normalized` is still set to the payee name (better than "Check 1234").

### Dashboard Pages (shared nav bar)
- **`GET /bank-review`** - Interactive HTML dashboard for reviewing/categorizing bank transactions. Self-contained (no external deps).
- **`GET /pnl`** - P&L summary dashboard. Date range picker → calls `POST /profit-summary`. Shows KPI cards, revenue/expense breakdown, cash control, profitability metrics with percentage bars.
- **`GET /analysis`** - Comprehensive analysis dashboard. Date range picker → calls `POST /comprehensive-analysis`. Shows monthly P&L table (sortable columns), revenue by day-of-week (inline bar chart), hourly revenue profile (inline bar chart).
- **`GET /cash-recon`** - Cash reconciliation dashboard. Compares POS credit/cash collections to bank deposits (Citizens settlements, Toast DEP/EOM, counter credits). Shows KPI cards, alerts for $0-deposit months, credit card recon table with status badges, cash gap tracking, and POS status breakdown (CAPTURED vs AUTHORIZED).
- **`GET /menu-mix`** - Menu mix / item analysis dashboard. Top 20 items by revenue, sales category breakdown, service period (daypart) performance, day-of-week with peak highlighting, hourly revenue profile. Uses ItemSelectionDetails_raw with business day logic.
- **`GET /servers`** - Server performance dashboard. Server leaderboard ranked by revenue with inline bars, click-to-expand DOW + hourly detail, discount analysis, tip analysis. Uses OrderDetails_raw + PaymentDetails_raw. Dark theme with emerald green gradient.
- **`GET /kitchen`** - Kitchen speed dashboard. Station performance sorted fastest-first with fulfillment time bars, hourly speed profile, cook leaderboard, weekly trend. Uses KitchenTimings_raw. Dark theme with amber gradient.
- **`GET /labor`** - Labor analysis dashboard. Weekly labor trend with color-coded labor % bars (Lean <25%, Target 25-35%, High >35%) and 30% target line, monthly summary with prime cost %, labor vendor breakdown. Uses OrderDetails_raw + BankTransactions_raw. Dark theme with blue gradient.
- **`GET /menu-eng`** - Menu engineering dashboard. BCG-style matrix classifying items as Stars/Plowhorses/Puzzles/Dogs. 2x2 matrix summary cards, filterable item table with column sorting, category breakdown. Uses ItemSelectionDetails_raw. Dark theme with purple gradient.
- **`GET /events`** - Events & promotional calendar dashboard. Year toggle (2025/2026), 6-month calendar grid with colored event dots and peak-week gold stripes, upcoming events table with historical revenue context, top 20 revenue weeks with event overlap tagging, and computed insights/intel cards. Uses `LOV3_EVENTS` constant + PaymentDetails_raw weekly revenue.
- **`GET /loyalty`** - Guest Intelligence dashboard. Card-based RFM segmentation with 7 guest segments (Champions/Loyal/Regulars/Returning/New/At Risk/Dormant). KPIs: unique guests, repeat rate, repeat revenue %, avg visits, avg spend, revenue/guest, at-risk count. Revenue concentration (power law). Visit frequency distribution. Monthly guest trend. DOW + hourly timing patterns. Top 50 repeat guests. Contact database with email/phone (enriched from CheckDetails), CSV export button for SevenRooms CRM import. Marketing campaign playbook with per-segment recommendations, reachable contact counts, and channel suggestions. SevenRooms integration guide.
- **`GET /kpi-benchmarks`** - KPI Benchmarking dashboard. Tracks 14 metrics against industry benchmarks for upscale nightlife venues. MTD/YTD toggle with instant switching. Scorecard grid with green/yellow/red status, prior-period deltas with directional arrows. Financial health gauge bars (COGS%, Labor%, Prime Cost%, Net Margin%, Marketing%, OPEX%). Operational efficiency cards (Avg Check, Orders/Day, Void Rate, Discount Rate, Rev/Labor Hr). Guest intelligence (Repeat %, Repeat Revenue %, At-Risk %). 6-month trend sparklines with benchmark reference lines. Expandable benchmark legend with sources. Uses OrderDetails_raw + BankTransactions_raw + PaymentDetails_raw. Dark theme with indigo gradient.
- **`POST /api/kpi-benchmarks`** - KPI benchmarking API. Body: `{"start_date":"2026-03-01","end_date":"2026-03-31"}`. Returns scorecard with 14 metrics (value, prior period value, delta, green/yellow/red status), financial/operational/guest sections, 6-month trend arrays, benchmark definitions. Auto-computes prior period (MTD → prior month, YTD → prior year).
- **`GET /budget`** - Budget Tracker dashboard. Monthly spending performance vs 15% profit margin target. Month selector, margin status banner, 4 budget category cards (COGS/Labor/Marketing/OPEX) with actual vs target %, variance, status badges, top vendors. P&L waterfall chart. 12-month trend table with margin bar chart. Path-to-15% recommendation cards ranked by savings impact. Top 30 vendors table. Insights sorted by severity. Emerald gradient theme.
- **`GET /event-roi`** - Event ROI dashboard. Per-event profitability for 6 recurring weekly events (Tue Bingo, Wed Live Music, Thu Happiest Hour, Fri 106, Sat RNB, Sun Brunch). Date range picker, KPI summary cards, event ROI cards with margin/cost breakdown, revenue vs cost bar chart, monthly margin trend table, collapsible cost breakdown per event with vendor detail, unattributed vendors table, insights. Amber/orange gradient theme.

### Bank Transaction Review API
- **`GET /api/bank-transactions`** - Paginated transaction API. Params: `status` (uncategorized/categorized/all), `limit`, `offset`, `sort`, `search`, `date_from`, `date_to`.
- **`POST /api/bank-transactions/categorize`** - Bulk-update categories. Body: `{"updates":[{"transaction_date":"...","description":"...","amount":-100,"new_category":"COGS/Food","vendor_normalized":"Sysco","create_rule":true,"rule_keyword":"SYSCO"}]}`.
- **`POST /api/bank-transactions/delete`** - Delete transactions by composite key. Body: `{"deletes":[{"transaction_date":"...","description":"...","amount":-100}]}`. Dashboard includes checkbox selection + "Delete Selected" button.

### LOV3 Business Assumptions (codified in `main.py`)
These constants are defined at the top of `main.py` and used by all report/analysis endpoints:
- **Business day cutoff:** 4 AM. A "business day" runs 4:00 AM → 3:59 AM. Revenue at 1 AM Saturday = Friday's business day.
- **Gratuity split:** House retains 35%, 65% passes through to staff. Tips are 100% staff.
- **True labor:** Gross labor debits minus tip/gratuity pass-through (bank labor includes pass-through payouts).
- **Unreconciled cash:** Toast cash collected (PaymentDetails) minus bank cash deposits (BankTransactions counter credits).
- **Category hierarchy:** `{N}. {Section}/{Subcategory}` format (post-audit, Feb 2026). E.g. `5. Operating Expenses (OPEX)/Permits & Licenses`.
- **Wire vendor parsing:** `_extract_wire_vendor()` parses BNF: (outbound) / ORIG: (inbound) from wire descriptions.
- **paid_date is STRING** in PaymentDetails_raw — must `CAST(paid_date AS DATETIME)` before datetime operations.
- **Toast ACH transaction detection:** `_categorize()` detects Toast-specific patterns before keyword matching: `DES:DEP` = daily credit card deposit (Revenue), `DES:EOM` = end-of-month adjustment (Revenue), `DES:REF` = fee refund (OPEX/POS), `Toast, Inc DES:Toast` = platform fee (OPEX/POS), `TOAST, INC. DES:YYYYMMDD` = monthly settlement (Revenue).
- **Check reconciliation:** After uploading a bank CSV, if check register entries are added later, call `POST /api/reconcile-checks` to re-categorize uncategorized checks without re-uploading the CSV.
- **LOV3_EVENTS constant:** Hardcoded list of Houston-area events, holidays, and LOV3 dates (2025-2026). Categories: holiday, conference, cultural, lov3, sports. Used by `/events` dashboard and `/api/events-calendar` endpoint.

## Common Commands

### Deploy
```bash
./deploy.sh
```

### Test locally
```bash
pip install -r requirements.txt
python main.py
# Then: curl http://localhost:8080/
```

### Check logs
```bash
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=toast-etl-pipeline" --limit=20
```

### Manual pipeline run
```bash
TOKEN=$(gcloud auth print-identity-token)
curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d '{"processing_date": "20250129"}' \
    https://toast-etl-pipeline-XXXXX-uc.a.run.app/run
```

### Backfill data
```bash
curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d '{"start_date": "20251011", "end_date": "20250129"}' \
    https://toast-etl-pipeline-XXXXX-uc.a.run.app/backfill
```

## Code Style
- Python 3.11+
- Type hints on all functions
- Dataclasses for structured data
- Logging to stdout (Cloud Logging picks it up)

## Important Notes
- Pipeline runs daily at 6 AM CST via Cloud Scheduler
- Data is idempotent - safe to re-run for same date
- Toast files available on SFTP by ~5 AM CST
- Schema changes are logged but don't block processing
