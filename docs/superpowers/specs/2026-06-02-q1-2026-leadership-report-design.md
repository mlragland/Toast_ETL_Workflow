# Q1 2026 Leadership Financial Report — Design

**Date:** 2026-06-02
**Owner:** Maurice Ragland
**Status:** Approved, ready for plan

## Goal

Produce a comprehensive Q1 2026 financial analysis report for LOV3|HTX leadership that is also suitable for inclusion in the SBA lender package. The report must be available as:

1. A live interactive HTML dashboard inside the Flask app (`/q1-report`)
2. A live Markdown export (`/q1-report.md`)
3. A standalone PDF generated locally via a separate script (`q1_report_pdf.py`)

## Non-Goals

- Real-time refresh / streaming updates (BigQuery on each page load is fine)
- Authentication beyond existing dashboard pattern (`/q1-report` is public like other dashboards)
- Scheduled delivery (Slack/email) — this is read-on-demand, not pushed
- Server-side PDF generation in Cloud Run (avoided to keep image lean)
- Forecasting models or Q2/Q3 projections — forward look is qualitative bullets only

## Comparison Frame

Every numeric in the report is shown across three comparison axes:

| Column | Period |
|---|---|
| Q1 2026 | Jan 1 – Mar 31, 2026 |
| Q4 2025 | Oct 1 – Dec 31, 2025 (sequential quarter, % chg) |
| Q1 2025 | Jan 1 – Mar 31, 2025 (YoY, % chg) |
| Jan 2026 / Feb 2026 / Mar 2026 | intra-quarter monthly trend |

## Sections

The report contains seven sections, each rendered identically across HTML, Markdown, and PDF outputs.

### A. Revenue Analysis
- POS gross sales (`OrderDetails_raw` + `PaymentDetails_raw`)
- Service charge breakdown (Waitstaff/Bartender 70/30, Bottle Manager 50/50)
- Voluntary tips (100% to staff)
- Hookah revenue — three phases per `CLAUDE.md`:
  - In-house POS hookah category (Mar 2024 – Mar 2025) — already in net_sales
  - Hardcoded reclasses from `config.HOOKAH_RECLASS` (Apr 2025 $20K, Dec 2025 $15K, Mar 2026 $16,400)
  - Predictive Insights LLC bank deposits (May 2025 – present, additive)
- Sales mix by category (Food / Liquor / Beer / Wine / NA Bev / Hookah)

### B. Cost Structure
- Labor (Toast Labor API time entries × wage rates)
  - Composite tracked rate: $57.07/hr (Q1 2026 baseline)
  - Labor % of revenue
- COGS (BankTransactions categorized as Food / Beverage / Hookah supply)
- Operating expenses by `BankCategoryRules` bucket
- Hookah margin (Predictive Insights revenue − attributable supply costs)

### C. Profitability
- Gross profit = Revenue − COGS
- EBITDA = Revenue − ALL operating expenses (SBA presentation: no tip pass-through deduction)
- EBITDA margin %
- Net income (after interest/depreciation if applicable)
- Trend lines per axis (sequential, YoY, monthly)

### D. Operational KPIs
- Covers = distinct checks (`CheckDetails_raw`)
- Average check
- Revenue per labor hour
- Business days operated (Wed–Sun only per `CLAUDE.md`)
- Revenue per business day
- Service speed (KitchenTimings_raw fulfilled durations)

### E. Staff Performance Summary
- **Bartenders:** revenue attributed via `KitchenTimings_raw.fulfilled_by` (service-well drinks) — NOT POS sales alone (per memory `feedback_bartender_evaluation.md`)
- **Servers:** revenue includes Bottle Manager tab name parsing to credit walk-in bottle revenue back to the booking server (per memory `feedback_server_evaluation.md`)
- Top 5 in each category by attributed revenue
- Hours worked and effective hourly contribution

### F. Cash Flow & Bank Reconciliation
- Total deposits (Toast settlement + Predictive Insights hookah)
- Total expenses by top-level category
- Top 10 vendors by spend
- Vendor concentration warning (any vendor > 15% of opex)

### G. Forward Look
- 3–5 qualitative bullets pulled from:
  - `SBA_2025_FOLLOWUP_ITEMS.md` (open items, capital needs)
  - Q2-to-date computed run rate (Apr/May 2026 if data exists at generation time)
  - Static manager-supplied narrative (editable string in `q1_report.py`)

## Architecture

### Components

```
q1_report.py
├── @dataclass RevenueSection         # A
├── @dataclass CostSection            # B
├── @dataclass ProfitabilitySection   # C
├── @dataclass KPISection             # D
├── @dataclass StaffSection           # E
├── @dataclass CashFlowSection        # F
├── @dataclass ForwardLookSection     # G
├── @dataclass Q1ReportData           # composes all 7 sections
└── class Q1ReportGenerator
    ├── fetch() -> Q1ReportData       # one BQ batch
    ├── render_html(data) -> str      # full HTML page string
    └── render_markdown(data) -> str  # markdown string
```

```
routes_dashboards.py  (modified)
├── GET /q1-report      -> HTML
├── GET /q1-report.md   -> Markdown (text/markdown)

routes_etl.py  (modified — already imports auth decorators)
└── (no change; PDF generation lives in standalone script)
```

```
q1_report_pdf.py  (new, standalone — NOT deployed to Cloud Run)
├── imports Q1ReportGenerator
├── renders markdown
├── pipes through weasyprint (or pandoc) locally
└── writes LOV3_HTX_Q1_2026_Leadership_Report.pdf
```

```
config.py  (modified)
├── Q1_2026_START / Q1_2026_END
├── Q4_2025_START / Q4_2025_END
├── Q1_2025_START / Q1_2025_END
└── Q1_REPORT_FORWARD_LOOK  (multi-line string, editable narrative)
```

### Data Flow

1. User hits `/q1-report` (or `.md`)
2. Route handler instantiates `Q1ReportGenerator(bq_client)`
3. `.fetch()` runs ~10 parameterized BigQuery queries (one per section, sub-queries for each comparison axis), assembles into `Q1ReportData`
4. Route picks renderer (`render_html` or `render_markdown`) and returns response
5. Generated timestamp included in header: `Generated: 2026-06-02 14:32:11 CST`

### Reused Code

| Component | Source | Why |
|---|---|---|
| `HOOKAH_RECLASS` | `config.py` | Three-phase hookah methodology |
| `BUSINESS_DAY_SQL` | `config.py` | 4 AM cutoff for revenue attribution |
| `EVENT_VENDOR_MAP` | `config.py` | Event/vendor categorization |
| Revenue + cost queries | `sba_financial_statements.py` | Already SBA-grade; port the SQL |
| BigQuery client pattern | `routes_analytics.py` | Cold-start friendly |
| Dashboard HTML scaffolding | `dashboards.py` | Pure-string functions, matches existing pattern |
| Bottle Manager parsing | `routes_analytics.py` server attribution endpoint | Don't reinvent |
| KitchenTimings fulfilled_by | `routes_analytics.py` bartender attribution endpoint | Don't reinvent |

## Error Handling

- BigQuery query timeout: 60s per query. On timeout, log + render the affected section with `⚠️ Data unavailable — retry generation` and continue rendering the rest.
- Missing comparison period (e.g., Q1 2025 has gap dates): show `n/a` in comparison cells; never break layout.
- Date parameter safety: all dates passed via `bigquery.ScalarQueryParameter` (per `.claude/rules/guardrails.md` — no f-string SQL).
- HTML escaping for staff names and vendor names in tables.

## Testing

- **Smoke test** (`tests/test_smoke.py`): add `test_q1_report_returns_200_with_title` — asserts response.status_code == 200 and `"Q1 2026 Financial Analysis"` in body.
- **Unit test** (`tests/test_q1_report.py` new): `test_compute_pct_change` covers normal, zero-prior (returns `None`), negative-to-positive.
- **Manual verification before declaring done:**
  1. `python -c "from main import app; print('OK')"` (import check per `.claude/rules/guardrails.md`)
  2. Run Flask locally, hit `/q1-report` — eyeball Revenue total against `LOV3_HTX_Q1_2026_PL.xlsx`
  3. Hit `/q1-report.md` — verify markdown renders cleanly in a preview
  4. Run `python q1_report_pdf.py` — open the resulting PDF, scan for layout breaks

## Deploy Considerations

- No new heavy dependencies added to Cloud Run (`weasyprint` is in `q1_report_pdf.py` only — gated behind `if __name__ == "__main__"`, never imported by `main.py`)
- Run `python -c "from main import app; print('OK')"` before `./deploy.sh` per `deploy-safety.md`
- Existing Cloud Run public-access pattern applies to `/q1-report*` routes

## Open Questions / Followups

None — all questions resolved during brainstorming. Manager-supplied forward-look narrative will be drafted during implementation and committed alongside the code.
