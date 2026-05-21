# LOV3 Houston — Promoter Payout Calculator Methodology

## Purpose

Replace the manual `Other Promoter Days.xlsx` template with a Flask dashboard that auto-pulls Toast sales, applies the same formulas as the spreadsheet, persists every payout to BigQuery, and prints a spreadsheet-style summary. This document captures every data source, business rule, and edge case so the calculation can be re-derived by any agent, auditor, or new operator in the future.

## Quick Start

```bash
# Local
python main.py
# → open http://localhost:8080/promoter-payout

# Production (after deploy)
# → https://toast-etl-pipeline-t3di7qky4q-uc.a.run.app/promoter-payout?key=$DASHBOARD_KEY
```

Workflow:
1. Pick **Event Date**, **Time Start**, **Time End** (e.g. 23:00 → 02:00 — next-day rollover handled automatically)
2. Click **"Pull Sales from Toast"** → form auto-fills with Net Liquor / Food / Shisha / Tips / Auto-Grat
3. Enter expenses (Security / Hostess / Entertainment / Marketing / Other) and Promoter %
4. Verify the live-computed payout, **Save** to BigQuery, **Print** for the promoter

---

## Data Sources

| Source | Use |
|--------|-----|
| `toast_raw.ItemSelectionDetails_raw` | Net sales by category (Liquor, Food, Shisha) for the time window |
| `toast_raw.PaymentDetails_raw` | CC Tips and CC Auto-Gratuity (informational only — not in payout calc) |
| `toast_raw.PromoterPayouts_raw` | Persistent history of every saved payout |

**Why these:** the original Toast Sales Summary report uses the same underlying data. We match it by filtering with the same boundaries.

---

## The Critical Time-Window Rule

**Filter by `order_date`, NOT `sent_date` or `paid_date`.**

This is the single most important rule. Toast's Sales Summary UI ("Custom hours" filter) groups items by **when the order was opened**, not when items were sent to the kitchen or when the payment was captured.

| Column | What it represents | Use it for promoter payouts? |
|---|---|---|
| `order_date` | Time the order was first opened | ✅ Yes — matches Toast UI |
| `sent_date` | Time the item was sent to the kitchen | ❌ No — drifts from order open by 5–30 min |
| `paid_date` | Time the payment was captured | ❌ No — drifts even further; sometimes after close |

**Audit example (5/14/2026, 23:00–02:00):**

| Field | Toast UI | order_date filter | sent_date filter |
|---|---:|---:|---:|
| Net Liquor (incl NA Bev) | $4,788.70 | **$4,788.70** ✓ | $5,496 (over by $700+) |
| Net Food | $344.60 | **$344.60** ✓ | $403.60 (over by $59) |
| CC Tips | $489.81 | **$489.81** ✓ | $869.52 (over by $380) |

### Dual date format (both columns)

`order_date`, `sent_date`, and `paid_date` are all stored as STRING with **two different formats** depending on ingestion era:

- `"YYYY-MM-DD HH:MM:SS"` — current SFTP ETL (2025+ ingestion)
- `"M/D/YY h:MM AM/PM"` — 2024 backfill via Toast API

Always coalesce both parses:

```sql
COALESCE(
  SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', order_date),
  SAFE.PARSE_DATETIME('%m/%d/%y %I:%M %p', order_date)
) AS order_dt
```

### Next-day rollover

Events typically run "11 PM – 2 AM" (3-hour window crossing midnight). The calculator resolves this in `_parse_event_window()`:

```python
if end_dt <= start_dt:
    end_dt = end_dt + timedelta(days=1)
```

So `event_date=2026-05-14, time_start=23:00, time_end=02:00` →
`start_dt = 2026-05-14T23:00:00`, `end_dt = 2026-05-15T02:00:00`.

The `processing_date` filter spans both calendar dates accordingly (`@proc_start` = start.date(), `@proc_end` = end.date()) so we don't miss late-night items that landed in the next day's partition.

---

## Sales Category Bucketing

`ItemSelectionDetails_raw.sales_category` is a free-text Toast field. We bucket each row into one of `liquor`, `food`, `shisha`, or `other`:

| Bucket | Match rules (case-insensitive substring on `sales_category`) |
|---|---|
| `liquor` | `liquor`, `beer`, `wine`, `bottle`, `cocktail`, `spirits`, `na beverage`, `n/a beverage` |
| `food` | `food`, `kitchen`, `appetizer`, `entree`, `dessert`, `brunch` |
| `shisha` | `hookah`, `shisha` |
| `other` | anything else (surfaced in the UI as a warning so the user knows it wasn't auto-classified) |

### Why NA Beverage is in `liquor`

Sossity's manual spreadsheets always sum `Liquor + Bottled Beer + Wine + NA Beverage` into the single "Net Liquor" line. NA Beverage at LOV3 is bar-side revenue (mixers, sodas, water sold separately, juices) so it's treated the same as alcohol revenue for promoter purposes.

**Spreadsheet proof (5/14/2026):**
```
Toast UI breakdown:           Spreadsheet "Net Liquor":
  Liquor       $4,385.50      $4,788.70
  Bottled Beer    $34.00      ───────
  Wine            $86.00      = Liquor + Beer + Wine + NA Beverage
  NA Beverage    $279.20        ($4,784.70, ~$4 rounding diff)
```

### Void exclusion

Voided items are excluded via `(voided IS NULL OR LOWER(voided) != 'true')`. `voided` is a STRING column (`'true'` / `'false'`) in BigQuery.

### Discount/comp handling

`net_price` already reflects discounts (`gross_price − discount = net_price`). Fully comped items have `net_price = 0` and naturally contribute zero to the sum. No special handling needed.

---

## Payout Formulas (from the spreadsheet template)

These mirror the formulas in `Other Promoter Days.xlsx` cell-for-cell. They run both client-side (live UI) and server-side (on save) — server-side is authoritative for the BigQuery row.

```
Gross Sales        = Net Liquor + Net Food + Net Shisha
COGS Adjustment    = Net Liquor × Liquor COGS %  +  Net Food × Food COGS %
Mixed Beverage Tax = Net Liquor × Mixed Bev Tax %
Net Sales          = Gross Sales − COGS Adjustment − Mixed Beverage Tax
Total Expenses     = Security + Hostess + Entertainment + Marketing + Other
Net Profit         = Net Sales − Total Expenses
Promoter Payout    = Net Profit × Promoter %
```

### Default rates (overridable per event)

| Rate | Default | Stored in |
|---|---|---|
| Liquor COGS % | 18% | `config.DEFAULT_LIQUOR_COGS_PCT` |
| Food COGS % | 25% | `config.DEFAULT_FOOD_COGS_PCT` |
| Mixed Beverage Tax % | 6.7% | `config.DEFAULT_MIXED_BEV_TAX_PCT` (Texas mixed beverage gross receipts tax) |
| Promoter % | 15% | `config.DEFAULT_PROMOTER_PCT` |

### Worked example (matches the 5.27.24 sheet to the cent)

```
Inputs:  Net Liquor $7,525.15, Net Food $1,195.50, Net Shisha $0
         Security $1,375.60, Hostess $412.50, others $0, Promoter 15%

Gross Sales        = 7,525.15 + 1,195.50 + 0          = $8,720.65
COGS Adjustment    = 7,525.15 × 0.18 + 1,195.50 × 0.25 = $1,653.40
Mixed Beverage Tax = 7,525.15 × 0.067                  =   $504.19
Net Sales          = 8,720.65 − 1,653.40 − 504.19      = $6,563.06
Total Expenses     = 1,375.60 + 412.50 + 0 + 0 + 0     = $1,788.10
Net Profit         = 6,563.06 − 1,788.10               = $4,774.96
Promoter Payout    = 4,774.96 × 0.15                   =   $716.24
```

This matches the spreadsheet `B30 = $4,774.96`, `F30 = $716.24` exactly.

### Worked example (matches the 5/14/2026 Toast pull)

```
Inputs:  Net Liquor $4,788.70 (Liquor+Beer+Wine+NA Bev for 23:00–02:00 by order_date)
         Net Food $344.60, Net Shisha $0
         All expenses $0 (audit run), Promoter 20%

Gross Sales        = 5,133.30
COGS Adjustment    = 4,788.70 × 0.18 + 344.60 × 0.25   =   $948.12
Mixed Beverage Tax = 4,788.70 × 0.067                  =   $320.84
Net Sales          = 5,133.30 − 948.12 − 320.84        = $3,864.34
Net Profit         = 3,864.34
Promoter Payout    = 3,864.34 × 0.20                   =   $772.87
```

Matches the 5/14/2026 spreadsheet `B30 = $3,864.34`, `B16 = $772.87`.

---

## What is *not* in the payout calc

| Field | Pulled from Toast? | Used in payout? |
|---|---|---|
| CC Tips | Yes (informational) | **No** — tips are 100% staff property; not part of net profit |
| CC Auto-Gratuity | Yes (informational) | **No** — 70/30 split tracked separately in service charge model |
| Cover Revenue | Manual entry | **No** by default (no inline JS sum) — kept in BQ row for future ROI analysis |
| Guest Count | Manual entry | No — informational only |

If the user wants cover revenue in the payout base, change the JS in `_promoter_payout_html()` to add `coverRev` to `gross` (one-line edit).

---

## Persistence — `toast_raw.PromoterPayouts_raw`

One row per saved payout. The `computed_*` columns are written by the backend (not derived as a view) so a saved row is always reproducible even if the formula logic later changes.

### Schema location

- BigQuery: `toast-analytics-444116.toast_raw.PromoterPayouts_raw` (37 fields)
- DDL reference: `_promoter_payout_schema.json` in the repo root
- Re-create:
  ```bash
  bq mk --table toast-analytics-444116:toast_raw.PromoterPayouts_raw _promoter_payout_schema.json
  ```

### Server-side recompute (authoritative)

`_compute_payout_totals()` in `routes_analytics.py` re-runs every formula server-side on save. The client never gets to set `computed_*` values — even if a user POSTs malicious totals, the backend overwrites them.

### Update flow

Saving with an existing `payout_id` deletes the old row first, then re-inserts. (BigQuery streaming-buffer caveat: rows freshly inserted via `insert_rows_json` cannot be deleted by DML for ~30–90 minutes. If you save twice in rapid succession the first row may briefly survive.)

---

## How to Reproduce a Past Audit

To re-verify any saved payout against fresh Toast data:

```bash
# 1. Pull the saved row
bq query --use_legacy_sql=false \
  'SELECT * FROM toast_raw.PromoterPayouts_raw WHERE payout_id = "<hex>"'

# 2. Re-pull the sales for the same window
curl -X POST http://localhost:8080/api/promoter-payout/fetch-sales \
  -H "Content-Type: application/json" \
  -d '{"event_date":"YYYY-MM-DD","time_start":"HH:MM","time_end":"HH:MM"}'

# 3. Compare net_liquor / net_food / net_shisha against the saved row.
# Any drift indicates: late-arriving items, refunds posted after save, or schema change.
```

If you need to re-derive a payout from scratch (no calculator):

```sql
WITH parsed AS (
  SELECT COALESCE(
    SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', order_date),
    SAFE.PARSE_DATETIME('%m/%d/%y %I:%M %p', order_date)
  ) AS order_dt,
  sales_category, net_price, voided
  FROM `toast-analytics-444116.toast_raw.ItemSelectionDetails_raw`
  WHERE processing_date BETWEEN DATE(@start_dt) AND DATE(@end_dt)
)
SELECT
  ROUND(SUM(IF(REGEXP_CONTAINS(LOWER(sales_category),
    r'liquor|beer|wine|bottle|cocktail|spirits|na beverage|n/a beverage'),
    net_price, 0)), 2) AS net_liquor,
  ROUND(SUM(IF(REGEXP_CONTAINS(LOWER(sales_category),
    r'food|kitchen|appetizer|entree|dessert|brunch'),
    net_price, 0)), 2) AS net_food,
  ROUND(SUM(IF(REGEXP_CONTAINS(LOWER(sales_category),
    r'hookah|shisha'),
    net_price, 0)), 2) AS net_shisha
FROM parsed
WHERE order_dt BETWEEN @start_dt AND @end_dt
  AND (voided IS NULL OR LOWER(voided) != 'true');
```

Then apply the seven formulas from "Payout Formulas" above. Result must match `computed_*` columns from the saved row to within ~$4 (timestamp-edge variance).

---

## Code Locations

| What | File | Symbol |
|---|---|---|
| Time-window parsing + rollover | `routes_analytics.py` | `_parse_event_window()` |
| Category bucketing rules | `routes_analytics.py` | `_LIQUOR_HINTS`, `_FOOD_HINTS`, `_SHISHA_HINTS`, `_bucket_sales_category()` |
| Server-side payout math | `routes_analytics.py` | `_compute_payout_totals()` |
| Sales auto-pull endpoint | `routes_analytics.py` | `api_promoter_payout_fetch_sales()` |
| Save endpoint | `routes_analytics.py` | `api_promoter_payout_save()` |
| History endpoint | `routes_analytics.py` | `api_promoter_payout_history()` |
| HTML / CSS / JS page | `dashboards.py` | `_promoter_payout_html()` |
| Default rates | `config.py` | `DEFAULT_*_PCT` constants |
| Route registration | `routes_dashboards.py` | `GET /promoter-payout` |
| BQ schema | `_promoter_payout_schema.json` | — |

---

## Known Quirks

1. **~$4 rounding tolerance** vs. Toast UI on the liquor column. Source is edge timestamps — items opened a few seconds before/after the window boundary. Don't chase this.
2. **CC Tips/Grat may differ from Toast Revenue Summary's "Gratuity" line.** Toast's top-level "Gratuity" can include extra service charges (valet, bottle service surcharges) not in the `PaymentDetails.gratuity` column. Our number matches Toast's *Service Charge Summary* section ($1,029.30 for the 5/14 example), not the *Revenue Summary* section ($1,629.30). Both are "correct" for different definitions.
3. **Auto-grat is never $0** for a normal night — LOV3 charges 20% on every check. If a manual spreadsheet shows $0 in that field, it's almost certainly a data-entry skip, not a true zero.
4. **CC Tips on the historical 5/14 spreadsheet ($320.84) is a typo** — that's the same value as the Mixed Beverage Tax line. Real tips were $489.81.
5. **Streaming buffer prevents immediate re-delete.** Saving and trying to re-save (or delete) within ~30–90 minutes can fail with a "rows in the streaming buffer" error. Just wait it out.

---

## When to Update This Doc

- Schema change (new column added to `PromoterPayouts_raw`)
- New default rate (e.g., Texas changes the mixed beverage tax)
- New category appears in Toast that doesn't bucket cleanly (add it to `_LIQUOR_HINTS` / `_FOOD_HINTS` / `_SHISHA_HINTS`)
- Toast changes their report aggregation behavior (re-run the order_date vs sent_date audit)
- A user-visible business rule changes (e.g., NA Beverage moves out of Liquor)
