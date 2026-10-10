# 2025 Adjusted Balance Sheet (K-1 Prep) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `adjusted_balance_sheet_2025.py` which reconciles Toast POS 2025 revenue to Texas Comptroller filings (MB Sales Tax, MB Gross Receipts, Sales & Use), produces an accrual-basis adjusted balance sheet at 12/31/2025, an Excel workbook, and a leadership report PDF — plus the reusable prompt doc `PROMPT_2025_ADJUSTED_BALANCE_SHEET.md`.

**Architecture:** Single standalone script (follows `sba_financial_statements.py` pattern — NOT deployed to Cloud Run). WebFile histories transcribed as constants (source PDFs are single-page scans). Pure computation functions at module level so `tests/test_adjusted_bs_math.py` can import them. BigQuery pulled once; results pasted into clearly-marked `BQ_RESULTS` constants so the script reruns without network access.

**Tech Stack:** Python 3.11, openpyxl (Excel), reportlab 4.5.1 (PDF — follow `q1_report_pdf.py` style), `bq query` CLI for data pulls (project `toast-analytics-444116`, dataset `toast_raw`).

**Spec:** `docs/superpowers/specs/2026-10-10-2025-adjusted-balance-sheet-design.md`

---

## File Structure

- Create: `adjusted_balance_sheet_2025.py` — constants (tax filings, 2024 Schedule L, BQ results), pure math (implied receipts, period liabilities, taxes payable, P&I), balance sheet builder, Excel writer, PDF writer, `main()`
- Create: `tests/test_adjusted_bs_math.py` — unit tests for the pure math only
- Create: `PROMPT_2025_ADJUSTED_BALANCE_SHEET.md` — reusable prompt/methodology doc
- Outputs: `LOV3_HTX_2025_Adjusted_Balance_Sheet.xlsx`, `LOV3_2025_Adjusted_BS_Leadership_Report.pdf` → repo root, then copied to `/Users/maurice_mac/Library/CloudStorage/Dropbox/Predictive_Models/PMG/Financial Analysis/2025/2025 Adjusted Balance Sheet/`

---

## Canonical data (transcribed 2026-10-10 from the WebFile PDFs — single source of truth)

Transcribed from the three single-page scans in the `2025 Adjusted Balance Sheet` Dropbox folder. `struck=True` = row crossed out on the scan (superseded). Format: `(txn_type, filed_date, ref, period, period_end, amount, struck)`.

### MB Gross Receipts Tax (6.7% — LOV3 expense)
```python
MB_GR = [
    ("return",  "2026-04-01", "9126075761",  "2602", "2026-02-28",  3419.98, False),
    ("payment", "2026-04-01", "9126075760",  "2602", "2026-02-28",  3419.98, False),
    ("payment", "2026-03-02", "6126094241",  "2601", "2026-01-31",  4116.39, False),
    ("return",  "2026-02-17", "4826143348",  "2601", "2026-01-31",  3920.37, False),
    ("amended", "2026-02-06", "3726120074",  "2512", "2025-12-31",  3587.85, False),
    ("payment", "2026-02-06", "3726120072",  "2512", "2025-12-31",  3587.85, False),
    ("return",  "2026-02-06", "3726119366",  "2512", "2025-12-31",   701.02, False),
    ("payment", "2026-02-06", "3726119365",  "2512", "2025-12-31",   701.02, False),
    ("payment", "2026-01-05", "526339152",   "2511", "2025-11-30",  4373.80, False),
    ("return",  "2025-12-19", "35325127769", "2511", "2025-11-30",  4165.52, False),
    ("return",  "2025-11-20", "32425146675", "2510", "2025-10-31",  5341.24, False),
    ("payment", "2025-11-20", "32425146674", "2510", "2025-10-31",  5341.24, False),
    ("return",  "2025-11-05", "30925135176", "2509", "2025-09-30",  4181.52, False),
    ("payment", "2025-11-05", "30925135175", "2509", "2025-09-30",  4181.52, False),
    ("payment", "2025-10-14", "28725130171", "2508", "2025-08-31", 23246.41, False),
    ("payment", "2025-10-02", "27525090710", "2508", "2025-08-31", 23216.41, False),
    ("return",  "2025-09-15", "25825124464", "2508", "2025-08-31", 22110.87, False),
    ("payment", "2025-08-21", "23325085354", "2507", "2025-07-31", 17951.91, False),
    ("return",  "2025-08-13", "22525102187", "2507", "2025-07-31", 17097.06, False),
    ("payment", "2025-08-12", "22425142171", "2506", "2025-06-30",  1956.22, False),
    ("return",  "2025-07-21", "20225324335", "2506", "2025-06-30",  1863.07, False),
    ("payment", "2025-06-23", "17425085654", "2504", "2025-04-30",  3756.59, False),
    ("amended", "2025-06-23", "17425084411", "2505", "2025-05-31",  2564.18, False),
    ("payment", "2025-06-23", "17425084410", "2505", "2025-05-31",  2564.18, False),
    ("return",  "2025-06-20", "17125071276", "2505", "2025-05-31", 21389.68, True),
    ("return",  "2025-05-30", "15025136131", "2504", "2025-04-30",  3588.11, False),
    ("payment", "2025-05-30", "15025136130", "2504", "2025-04-30",  3588.11, True),
    ("payment", "2025-04-30", "12025116949", "2503", "2025-03-31",  3529.25, False),
    ("return",  "2025-04-21", "11125251608", "2503", "2025-03-31",  3361.19, False),
    ("return",  "2025-03-13", "7225149828",  "2502", "2025-02-28",  4058.66, False),
    ("payment", "2025-03-13", "7225149827",  "2502", "2025-02-28",  4058.66, False),
    ("return",  "2025-03-13", "7225149578",  "2501", "2025-01-31",  4615.58, False),
    ("payment", "2025-03-13", "7225149577",  "2501", "2025-01-31",  4615.58, False),
]
```
(2024 periods 2406–2412 exist on the scan but are out of scope; 2024 return already filed.)

### MB Sales Tax (8.25% — collected from customers, pass-through liability)
```python
MB_SALES = [
    ("return",  "2026-04-01", "9126076170",  "2602", "2026-02-28",  3952.00, False),
    ("payment", "2026-04-01", "9126076168",  "2602", "2026-02-28",  4199.60, False),
    ("payment", "2026-03-02", "6126094999",  "2601", "2026-01-31",  5068.69, False),
    ("return",  "2026-02-17", "4826143745",  "2601", "2026-01-31",  4827.32, False),
    ("return",  "2026-02-06", "3726120390",  "2512", "2025-12-31",  4970.96, False),
    ("payment", "2026-02-06", "3726120388",  "2512", "2025-12-31",  5269.51, False),
    ("payment", "2026-01-05", "526340231",   "2511", "2025-11-30",  5385.65, False),
    ("return",  "2025-12-19", "35325128140", "2511", "2025-11-30",  5129.19, False),
    ("return",  "2025-11-20", "32425150461", "2510", "2025-10-31",  6576.90, False),
    ("payment", "2025-11-20", "32425150460", "2510", "2025-10-31",  6576.90, False),
    ("return",  "2025-11-05", "30925135687", "2509", "2025-09-30",  4845.06, False),
    ("payment", "2025-11-05", "30925135686", "2509", "2025-09-30",  5137.31, False),
    ("payment", "2025-09-30", "27325092222", "2508", "2025-08-31", 28587.37, False),
    ("return",  "2025-09-15", "25825124671", "2508", "2025-08-31", 27226.07, False),
    ("payment", "2025-08-21", "23325085815", "2507", "2025-07-31", 22104.97, False),
    ("return",  "2025-08-13", "22525102340", "2507", "2025-07-31", 21052.35, False),
    ("payment", "2025-08-12", "22425142890", "2506", "2025-06-30",  2408.78, False),
    ("return",  "2025-07-21", "20225324610", "2506", "2025-06-30",  2294.08, False),
    ("amended", "2025-06-23", "17425087959", "2505", "2025-05-31",  3007.04, False),
    ("payment", "2025-06-23", "17425087957", "2505", "2025-05-31",  3157.39, False),
    ("payment", "2025-06-23", "17425087180", "2504", "2025-04-30",  4614.09, False),
    ("return",  "2025-06-20", "17125071687", "2505", "2025-05-31", 26338.04, True),
    ("return",  "2025-05-30", "15025136190", "2504", "2025-04-30",  4149.17, False),
    ("payment", "2025-05-30", "15025136189", "2504", "2025-04-30",  4406.63, True),
    ("payment", "2025-04-30", "12025118236", "2503", "2025-03-31",  4345.72, False),
    ("payment", "2025-04-30", "12025117513", "2502", "2025-02-28",  5497.36, False),
    ("return",  "2025-04-21", "11125251960", "2503", "2025-03-31",  4138.78, False),
    ("return",  "2025-03-20", "7925179319",  "2502", "2025-02-28",  4997.60, False),
    ("return",  "2025-03-13", "7225149986",  "2501", "2025-01-31",  5354.09, False),
    ("payment", "2025-03-13", "7225149985",  "2501", "2025-01-31",  5671.79, False),
]
```
Note: 2505 original return $26,338.04 is treated as struck (amended 3 days later; payments match the amended figure, not the original).

### Sales & Use Tax (8.25% — collected on non-MB taxable sales; quarterly 251/252 then monthly from 2507)
```python
SALES_USE = [
    ("return",  "2026-04-01", "9126076486",  "2602", "2026-02-28",  6368.59, False),
    ("payment", "2026-04-01", "9126076719",  "2602", "2026-02-28",  6737.02, False),
    ("payment", "2026-03-02", "6126095491",  "2601", "2026-01-31",  5899.25, False),
    ("return",  "2026-02-17", "4826142811",  "2601", "2026-01-31",  5618.33, False),
    ("return",  "2026-02-06", "3726120587",  "2512", "2025-12-31",  4990.67, False),
    ("payment", "2026-02-06", "3726120697",  "2512", "2025-12-31",  5290.20, False),
    ("payment", "2026-01-05", "526341357",   "2511", "2025-11-30",  6207.89, False),
    ("return",  "2025-12-19", "35325128673", "2511", "2025-11-30",  5912.28, False),
    ("payment", "2025-11-20", "32425145332", "2509", "2025-09-30",  1907.98, False),
    ("return",  "2025-11-20", "32425135596", "2510", "2025-10-31",  5901.14, False),
    ("payment", "2025-11-20", "32425136237", "2510", "2025-10-31",  5871.64, False),
    ("return",  "2025-11-05", "30925154472", "2509", "2025-09-30",  1661.80, False),
    ("payment", "2025-11-05", "30925155322", "2509", "2025-09-30",  1794.89, True),
    ("payment", "2025-09-30", "27325090952", "2508", "2025-08-31",  5801.11, False),
    ("return",  "2025-09-15", "25825125009", "2508", "2025-08-31",  5524.86, False),
    ("payment", "2025-08-21", "23325086493", "2507", "2025-07-31",  5719.42, False),
    ("return",  "2025-08-13", "22525104873", "2507", "2025-07-31",  5447.06, False),
    ("payment", "2025-08-12", "22425143730", "252",  "2025-06-30",  5455.22, False),
    ("return",  "2025-07-21", "20225325832", "252",  "2025-06-30",  5195.44, False),
    ("payment", "2025-04-30", "12025119803", "251",  "2025-03-31",  4522.34, False),
    ("return",  "2025-04-21", "11125255690", "251",  "2025-03-31",  4306.99, False),
    ("return",  "2025-02-04", "3525233010",  "244",  "2024-12-31",  7507.42, False),
    ("payment", "2025-02-04", "3525233050",  "244",  "2024-12-31",  7932.79, False),
]
```
Note: period `244` is Q4-2024 (liability belongs to 2024; the Feb-2025 payment is a 2025 cash outflow only). Periods `251`/`252` are Q1/Q2 2025.

### 2024 Schedule L ending balances (= 1/1/2025 beginning balances)
```python
BEGINNING_BALANCES = {
    "cash": 72435, "accounts_receivable": 373592, "inventory": 32191,
    "depreciable_assets": 80900, "accumulated_depreciation": 9278,
    "total_assets": 549839, "total_liabilities": 0, "partners_capital": 549839,
}
PARTNERS = {"Maurice Ragland": 0.15, "Eddiele B Jasper": None, "Derwin James": None}  # 2024 K-1 shows only Maurice's 15% capital; others flagged ACCOUNTANT INPUT
```

### Tax rates / decision rules
```python
MB_SALES_RATE = 0.0825   # collected on mixed-beverage sales
MB_GR_RATE    = 0.067    # LOV3-paid on the same MB receipts base
SU_RATE       = 0.0825   # Houston combined state+local rate on non-MB taxable sales
```
- **Period liability rule:** liability = sum of non-struck `return`/`amended` amounts per period. A struck original is superseded by its amended return. A non-struck original coexisting with an amended (MB GR 2512: $701.02 + $3,587.85, both separately paid) is additive — flag these periods.
- **P&I rule:** per-period `payments − liability` (when positive) = penalties/interest expense; when negative = underpayment/credit, flagged.
- **Taxes payable @ 12/31/2025:** liabilities for periods ending ≤ 2025-12-31 whose payments occurred after 2025-12-31 (i.e., 2511 + 2512 across all three types).

---

### Task 1: Pure math module with tests

**Files:**
- Create: `adjusted_balance_sheet_2025.py` (constants above + functions below)
- Test: `tests/test_adjusted_bs_math.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_adjusted_bs_math.py
from adjusted_balance_sheet_2025 import (
    period_liabilities, taxes_payable_at_yearend, penalties_and_interest,
    implied_receipts, MB_GR, MB_SALES, SALES_USE,
)

def test_period_liability_amended_supersedes_struck_original():
    liabs = period_liabilities(MB_GR)
    assert liabs["2505"] == 2564.18          # struck 21,389.68 ignored

def test_period_liability_additive_when_original_not_struck():
    liabs = period_liabilities(MB_GR)
    assert liabs["2512"] == 701.02 + 3587.85  # both paid → additive, flagged

def test_mb_sales_2505_uses_amended():
    liabs = period_liabilities(MB_SALES)
    assert liabs["2505"] == 3007.04

def test_taxes_payable_at_yearend_is_2511_plus_2512():
    tp = taxes_payable_at_yearend(MB_GR)
    assert tp == round(4165.52 + 701.02 + 3587.85, 2)

def test_penalties_and_interest_positive_delta():
    pi = penalties_and_interest(MB_SALES)
    assert pi["2501"] == round(5671.79 - 5354.09, 2)   # 317.70

def test_implied_receipts_divides_by_rate():
    liabs = {"2501": 8250.0}
    assert implied_receipts(liabs, 0.0825)["2501"] == 100000.0

def test_sales_use_excludes_2024_q4_from_2025():
    liabs = period_liabilities(SALES_USE, year=2025)
    assert "244" not in liabs and "251" in liabs
```

- [ ] **Step 2: Run tests, verify failure** — `pytest tests/test_adjusted_bs_math.py -v` → FAIL (module/functions missing)

- [ ] **Step 3: Implement**

```python
# adjusted_balance_sheet_2025.py (functions section)
from collections import defaultdict
from datetime import date

def _period_year(period_end: str) -> int:
    return int(period_end[:4])

def period_liabilities(rows, year=2025):
    """Filed liability per period: sum of non-struck return/amended rows."""
    out = defaultdict(float)
    for txn, filed, ref, period, pend, amount, struck in rows:
        if txn in ("return", "amended") and not struck and _period_year(pend) == year:
            out[period] += amount
    return {p: round(v, 2) for p, v in out.items()}

def period_payments(rows, year=2025):
    out = defaultdict(float)
    for txn, filed, ref, period, pend, amount, struck in rows:
        if txn == "payment" and not struck and _period_year(pend) == year:
            out[period] += amount
    return {p: round(v, 2) for p, v in out.items()}

def taxes_payable_at_yearend(rows, asof="2025-12-31"):
    """Liabilities for periods ending <= asof with no payment on or before asof."""
    liabs = period_liabilities(rows, year=int(asof[:4]))
    paid_by_asof = defaultdict(float)
    for txn, filed, ref, period, pend, amount, struck in rows:
        if txn == "payment" and not struck and filed <= asof:
            paid_by_asof[period] += amount
    return round(sum(v for p, v in liabs.items() if paid_by_asof[p] < v - 0.01), 2)

def penalties_and_interest(rows, year=2025):
    """Per-period payments minus liability (positive = P&I expense)."""
    liabs, pays = period_liabilities(rows, year), period_payments(rows, year)
    return {p: round(pays.get(p, 0) - liabs[p], 2) for p in liabs if abs(pays.get(p, 0) - liabs[p]) > 0.01}

def implied_receipts(liabs: dict, rate: float) -> dict:
    return {p: round(v / rate, 2) for p, v in liabs.items()}
```

- [ ] **Step 4: Run tests, verify pass** — `pytest tests/test_adjusted_bs_math.py -v` → 7 passed
- [ ] **Step 5: Verify app untouched** — `python -c "from main import app; print('OK')"` → OK
- [ ] **Step 6: Commit** — `git add adjusted_balance_sheet_2025.py tests/test_adjusted_bs_math.py && git commit -m "feat(k1-prep): WebFile tax data + reconciliation math for 2025 adjusted balance sheet"`

---

### Task 2: BigQuery pulls — Toast 2025 revenue, cash, legal order

**Files:** Modify: `adjusted_balance_sheet_2025.py` (add `BQ_RESULTS` constants)

- [ ] **Step 1: Discover actual sales categories** (never guess buckets)

```bash
bq query --use_legacy_sql=false --format=csv 'SELECT COALESCE(sales_category,"(none)") c, ROUND(SUM(CAST(net_price AS FLOAT64)),2) rev FROM `toast-analytics-444116.toast_raw.ItemSelectionDetails_raw` WHERE DATE(order_date) BETWEEN "2024-12-31" AND "2026-01-02" GROUP BY c ORDER BY rev DESC'
```
(If `order_date` is not the right datetime column, check schema first: `bq show --schema toast_raw.ItemSelectionDetails_raw`.)

- [ ] **Step 2: Monthly revenue by MB vs non-MB bucket, business-day basis (4 AM cutoff)**

Use `BUSINESS_DAY_SQL` logic from `config.py` (DATE_SUB 1 day when hour < 4). MB bucket = alcohol categories found in Step 1 (expect: Liquor, Wine, Bottled Beer, similar); non-MB = Food, NA Beverage, retail/merch, etc. Run one query grouping by `FORMAT_DATE('%Y-%m', business_day)` × bucket for business days 2025-01-01..2025-12-31.

- [ ] **Step 3: Cash at 12/31/2025** — `bq show --schema toast_raw.BankTransactions_raw`; if a running-balance column exists, take the latest 2025 balance; else reconstruct (sum of signed amounts through 2025-12-31) or fall back to `Financial Analysis/2025/BofA_statement...xlsx`; else mark `ACCOUNTANT INPUT`.

- [ ] **Step 4: Locate the $59,175 legal order** —

```bash
bq query --use_legacy_sql=false --format=csv 'SELECT transaction_date, description, amount FROM `toast-analytics-444116.toast_raw.BankTransactions_raw` WHERE UPPER(description) LIKE "%LEGAL ORDER%" OR UPPER(description) LIKE "%LTS%" ORDER BY transaction_date'
```

- [ ] **Step 5: 2025 owner distributions + any contributions** — reuse the P&L's Owner Draws number ($231,907 per `SBA_2025_FOLLOWUP_ITEMS.md`); re-derive from `BankTransactions_raw` categories if available.

- [ ] **Step 6: Paste results into `BQ_RESULTS` dict constant** in the script with a `# Pulled YYYY-MM-DD — rerun queries in PROMPT doc to refresh` comment. Commit: `git commit -m "data(k1-prep): 2025 Toast revenue, cash, legal-order BQ snapshots"`

---

### Task 3: $59,175 verification function

**Files:** Modify: `adjusted_balance_sheet_2025.py`; Test: `tests/test_adjusted_bs_math.py`

- [ ] **Step 1: Failing test**

```python
def test_legal_order_match_finds_no_single_or_combo_match_in_window():
    from adjusted_balance_sheet_2025 import match_legal_order
    result = match_legal_order(59175.00, window=("2025-07-25", "2025-09-25"))
    # expected based on transcription: no single payment or same-day combo equals 59,175
    assert result["matched"] is False
```

- [ ] **Step 2: Implement** — `match_legal_order(amount, window)` scans all payments in MB_GR + MB_SALES + SALES_USE within the window; checks singles, same-day sums per account, and same-day sums across accounts against `amount` ± $1. Returns `{"matched": bool, "candidates": [...]}` listing nearest combos.
- [ ] **Step 3: Run tests** → pass. If the test's expectation is wrong (a combo DOES match), the classification flips to "tax remittance" — update the test to assert the actual finding and record it in Assumptions & Flags.
- [ ] **Step 4: Commit** — `git commit -m "feat(k1-prep): legal-order matcher for \$59,175 classification"`

---

### Task 4: Balance sheet builder with tie check

**Files:** Modify: `adjusted_balance_sheet_2025.py`; Test: `tests/test_adjusted_bs_math.py`

- [ ] **Step 1: Failing test**

```python
def test_balance_sheet_ties():
    from adjusted_balance_sheet_2025 import build_balance_sheet
    bs = build_balance_sheet()
    assert abs(bs["total_assets"] - (bs["total_liabilities"] + bs["total_equity"])) < 1.0

def test_taxes_payable_line_is_sum_of_three_accounts():
    from adjusted_balance_sheet_2025 import build_balance_sheet, taxes_payable_at_yearend, MB_GR, MB_SALES, SALES_USE
    bs = build_balance_sheet()
    expected = sum(taxes_payable_at_yearend(r) for r in (MB_GR, MB_SALES, SALES_USE))
    assert bs["liabilities"]["taxes_payable"] == round(expected, 2)
```

- [ ] **Step 2: Implement `build_balance_sheet()`** — assets (cash from BQ_RESULTS, A/R, inventory carried or ACCOUNTANT INPUT, fixed assets net of 2025 depreciation ACCOUNTANT INPUT, other assets incl. probable Comptroller bond if Task 3 unmatched), liabilities (taxes payable, AP/accrued ACCOUNTANT INPUT = 0 placeholder with flag), equity roll-forward (beginning capital + contributions − distributions ± adjusted 2025 net income). Any unavoidable gap goes to a named line `"unreconciled_difference"` — surfaced, never hidden. Each ACCOUNTANT INPUT line carries a `flag` string.
- [ ] **Step 3: Run full test file** → all pass. **Step 4: Commit** — `git commit -m "feat(k1-prep): accrual balance sheet builder with tie check"`

---

### Task 5: Excel workbook + leadership report PDF

**Files:** Modify: `adjusted_balance_sheet_2025.py` (writer functions + `main()`)

- [ ] **Step 1: Excel writer** (openpyxl, lazy-imported inside function per stack-gotchas). Tabs:
  1. **Balance Sheet** — 12/31/2024 vs 12/31/2025 columns, ACCOUNTANT INPUT cells highlighted yellow
  2. **Tax Reconciliation** — per tax type × period: filed liability, payments, P&I, implied receipts; annual totals; MB Sales vs MB GR base cross-check column
  3. **Revenue Adjustments** — Toast book revenue vs filed implied receipts by month/bucket, variance, recommended adjustment entries (debit/credit style, plain English)
  4. **Assumptions & Flags** — all six spec flags + anything discovered in Tasks 2–3
- [ ] **Step 2: PDF writer** — reportlab, follow `q1_report_pdf.py` layout conventions. Sections: headline variance; the three taxes explained (one paragraph each); year-end taxes payable; adjustments summary; open items for BAED. Timestamps/dates in America/Chicago.
- [ ] **Step 3: Run `python adjusted_balance_sheet_2025.py`** — generates both files in repo root; open and eyeball totals vs the Tax Reconciliation tab.
- [ ] **Step 4: Verify** — `pytest tests/test_adjusted_bs_math.py -v` all pass; `python -c "from main import app; print('OK')"` → OK.
- [ ] **Step 5: Commit** — `git commit -m "feat(k1-prep): Excel workbook + leadership PDF generation"`

---

### Task 6: Prompt doc, copy outputs, wrap up

**Files:** Create: `PROMPT_2025_ADJUSTED_BALANCE_SHEET.md`

- [ ] **Step 1: Write the prompt doc.** Self-contained prompt a fresh Claude Code session could execute: context (K-1/1065 prep, BAED), data source paths (the three WebFile PDFs, BQ tables, 2024 1065), the four locked decisions (Comptroller anchor, accrual, bond hypothesis, Approach-1 script), the tax math and decision rules from this plan's "Canonical data" section, required outputs, and the rerun procedure (`python adjusted_balance_sheet_2025.py` after updating BQ_RESULTS / accountant inputs).
- [ ] **Step 2: Copy outputs** to `/Users/maurice_mac/Library/CloudStorage/Dropbox/Predictive_Models/PMG/Financial Analysis/2025/2025 Adjusted Balance Sheet/` (keep repo copies).
- [ ] **Step 3: Final verification** — rerun full `pytest tests/ -v` (no regressions), import check.
- [ ] **Step 4: Commit** — `git add PROMPT_2025_ADJUSTED_BALANCE_SHEET.md && git commit -m "docs(k1-prep): reusable prompt for 2025 adjusted balance sheet"`
- [ ] **Step 5: Report to Maurice** — summary of filed-vs-book variance, the $59,175 finding, taxes payable, and the explicit ACCOUNTANT INPUT list. No emails/Slack sent.
