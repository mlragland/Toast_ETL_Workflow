# PROMPT: 2025 Adjusted Balance Sheet — K-1 Prep (LOV3 Restaurant & Lounge LLC)

> Use this document to orient a fresh Claude Code session for reproducing or
> updating `adjusted_balance_sheet_2025.py`. Read it entirely before touching
> any code.

---

## 1. Context

**Entity:** LOV3 Restaurant & Lounge LLC (dba LOV3|HTX), Houston TX
**Purpose:** Accrual-basis 12/31/2025 balance sheet for K-1 preparation
**Accountant:** Sanchez Burnside, BAED Corporation
**Partners:** Maurice Ragland (15% capital, confirmed via 2024 K-1); Eddiele B Jasper and Derwin James Jr (ownership % unknown — ACCOUNTANT INPUT)
**Basis:** Accrual (consistent with the 2024 Form 1065 and accepted SBA P&L)

This is a **standalone script**, NOT part of the Flask/Cloud Run app. It has no Flask imports and produces two output files in the repo root.

---

## 2. Output Files

| File | Description |
|------|-------------|
| `LOV3_HTX_2025_Adjusted_Balance_Sheet.xlsx` | 4-tab workbook (Tab 1–4 below) |
| `LOV3_2025_Adjusted_Balance_Sheet_Leadership_Report.pdf` | 6-page leadership PDF |

Both are regenerated each run and are **untracked** (not committed to git). Copy to Dropbox after each run — see Section 8.

### Excel Tabs
1. **Balance Sheet** — Comparative 12/31/2024 (Schedule L) vs 12/31/2025; ACCOUNTANT INPUT cells highlighted yellow; named plug `unreconciled_difference`
2. **Tax Reconciliation** — Per-period liability, payments, P&I, implied receipts for all three TX Comptroller accounts; MB Sales vs MB GR cross-check
3. **Revenue Adjustments** — Monthly Toast book vs filed implied receipts; 9 variance driver notes
4. **Assumptions & Flags** — 6 spec flags + all `build_balance_sheet()` flags; color-coded by severity

### PDF Sections
- Cover: KPI table, unreconciled difference explanation
- Section 1: Revenue variance (annual + monthly detail + key drivers)
- Section 2: Three TX tax accounts explained + year-end taxes payable breakdown
- Section 3: Recommended accounting entries (6 adjustments)
- Section 4: Open items for BAED (10 ACCOUNTANT INPUT placeholders)
- Section 5: All balance sheet flags (ACCOUNTANT INPUT / WARNING / NOTE)

---

## 3. Data Sources

### Texas Comptroller WebFile PDFs (primary anchor)
```
/Users/maurice_mac/Library/CloudStorage/Dropbox/Predictive_Models/PMG/Financial Analysis/2025/2025 Adjusted Balance Sheet/
  MB Gross Receipts Tax_20260403121740.pdf   ← 6.7% LOV3 expense
  MB Sales Tax.pdf                            ← 8.25% collected from customers
  Sales and Use Tax.pdf                       ← 8.25% collected on non-MB sales
```
All constants in `MB_GR`, `MB_SALES`, `SALES_USE` were transcribed from these PDFs on 2026-10-10. `struck=True` rows are crossed out on the scans (superseded by later amended returns).

### BigQuery (BigQuery project: `toast-analytics-444116`, dataset: `toast_raw`)
- All revenue figures are from `ItemSelectionDetails_raw`, **net of voids** (`voided != 'true'`), business-day basis (`processing_date`), 4 AM CT cutoff
- Bank data from `BankTransactions_raw` (single BofA checking account)
- Results are hardcoded in `BQ_RESULTS` — re-pull via the SQL in `BQ_QUERIES` when refreshing

### 2024 Form 1065 / Schedule L
- Beginning balances hardcoded in `BEGINNING_BALANCES`
- Source: SBA Loan Artifacts folder in Dropbox
- 2024 ending A/R: $373,592 (unusually large — ACCOUNTANT INPUT)

---

## 4. Four Locked Decisions

### Decision 1: Comptroller Anchor
The Texas Comptroller WebFile filings are the **source of truth** for tax liabilities and implied receipts. Toast POS revenue is compared against Comptroller-implied receipts (`filed_liability ÷ rate`), never the other way around.

### Decision 2: Accrual Basis
All items recognized when earned/incurred, not when cash moves. Periods 2511 (Nov 2025) and 2512 (Dec 2025) are liabilities at 12/31/2025 even though payments were filed in Jan/Feb 2026.

### Decision 3: $59,175 Legal Order — RESOLVED, NET ZERO
A $59,175 bank debit on 2025-08-25 (`Legal Order, LTS PXXXXX5000391`) was **fully reversed on 2025-08-29**. Net = $0. No balance sheet asset or liability. If this was booked as OPEX, that entry must be reversed. `match_legal_order()` corroborates: no WebFile payment matches the amount.

### Decision 4: Standalone Script
`adjusted_balance_sheet_2025.py` does NOT import from `main.py`, `config.py`, `services.py`, or any Flask module. All pure math is at module level so `tests/test_adjusted_bs_math.py` imports it without triggering BigQuery, Flask, or openpyxl/reportlab.

---

## 5. Tax Math and Decision Rules

### Three Taxes

| Tax | Rate | Who Pays | P&L Treatment |
|-----|------|----------|---------------|
| MB Gross Receipts (MB GR) | 6.7% | LOV3 | OPEX — Taxes & Licenses; NEVER net from revenue |
| MB Sales Tax (MB Sales) | 8.25% | Customer (collected by LOV3) | Pass-through liability; not revenue, not expense |
| Sales & Use Tax (S&U) | 8.25% | Customer (collected by LOV3) | Pass-through liability; not revenue, not expense |

**MB bucket:** Liquor, Wine, Bottled Beer (TX § 183 — Mixed Beverage)
**NON-MB bucket:** Food, NA Beverage, Hookah, Door, (none)/NULL (TX S&U § 151)

### Period Code Format
- Monthly (standard): `YYMM` — e.g., `2511` = Nov 2025, `2512` = Dec 2025
- Quarterly (S&U H1 2025 only): `YYQ` — e.g., `251` = Q1 2025 (Jan–Mar), `252` = Q2 2025 (Apr–Jun)
- Prior year carry-over: `244` = Q4 2024 (liability is 2024; Jan 2025 payment is a 2025 cash outflow only)

### Struck vs Amended Decision Rules
1. `struck=True` rows are **always excluded** from all calculations
2. When a period has only an amended return (original struck), the amended amount is the sole liability
3. When a period has a non-struck original AND an amended return (both non-struck), they are **additive** (e.g., MB GR period 2512: 701.02 + 3587.85 = 4288.87)

### Timely-Filer Discount
- **Applies ONLY to S&U** (Tax Code § 151.423): 0.5% of filed liability when return + payment are on time
- **MB taxes are INELIGIBLE** (Tax Code § 183 — no equivalent discount provision)
- If a period's payment shortfall is within `TIMELY_FILER_TOLERANCE` ($0.02) of 0.5% × liability, the shortfall is treated as a settled discount — not counted in taxes payable
- Always pass `discount_eligible=True` for `SALES_USE`, `discount_eligible=False` for `MB_GR` and `MB_SALES`

### Taxes Payable at 12/31/2025
$29,457.49 total (periods 2511 + 2512 across all three accounts; payments filed Jan/Feb 2026):
- MB GR: 4165.52 + 701.02 + 3587.85 = 8454.39
- MB Sales: 10,100.15
- S&U: 10,902.95 (after 0.5% timely-filer discount on any qualifying periods)

---

## 6. Key Findings

### Revenue Verification
- Toast 2025 full-year (net of voids): MB $3,336,676.50 | NON-MB $914,285.37 | Grand $4,250,961.87
- Voided 2025 items: $176,422.30 (MB $159,144.23 + NON-MB $17,278.07) — tracked separately, not in revenue
- Cross-check: $4,250,961.87 + $176,422.30 = $4,427,384.17 (matches prior gross)

### Comptroller Validation Finding (Critical)
**Jul–Aug 2025 (periods 2507–2508) filed MB base matches Toast POS to the dollar (variance <$6)** — this validates the comparison method.

**Probable under-filing 10 of 12 months:** Jan–Jun AND Sep–Dec filed liabilities imply only ~$28–80K/mo vs Toast $230–350K/mo. The ~$2.18M MB base and ~$442K non-MB base discrepancy is consistent with the struck/amended 2504–2505 returns and a mid-2025 Comptroller correction that did not persist after August. **Refer to BAED; consider amended Comptroller returns. ACCOUNTANT REVIEW REQUIRED before any revenue adjustment is booked.**

### $59,175 Legal Order
Confirmed net zero — see Decision 3 above.

### Owner Distributions
BQ bank-category total 2025: $225,496.37 (Owner Draws/Transfers $172,406.51 + Discretionary $43,676.41 + Travel $9,413.45). Prior SBA P&L showed $231,907 — delta ~$6,411 likely in uncategorized rows or miscategorized credit card payments. **ACCOUNTANT INPUT: reconcile before filing K-1s.**

---

## 7. Balance Sheet Open Items (ACCOUNTANT INPUT Placeholders)

All of the following are set to $0 or a prior-year carry-forward in the current script. **Do not invent numbers — fill these in only when the accountant confirms them.**

| Item | Current Placeholder | Action |
|------|--------------------|----|
| Cash at 12/31/2025 | $0 | Use Dec-2025 BofA statement closing balance |
| Net Income 2025 | $0 | From `LOV3_HTX_Financial_Statements_SBA.xlsx` |
| 2025 Depreciation | $0 | From fixed-asset schedule |
| Accounts Payable | $0 | Confirm vendor invoices outstanding 12/31/2025 |
| Accrued Payroll | $0 | Confirm pay-period cutoff vs 12/31/2025 |
| Capital Contributions 2025 | $0 | From bank records and partner agreements |
| Accounts Receivable 2025 | $373,592 (2024) | Confirm: collect, write-off, or reclassify |
| Inventory 2025 | $32,191 (2024) | Physical count at 12/31/2025 |
| Partner Ownership % | Maurice 15% only | Eddiele B Jasper and Derwin James from LLC op agreement |

The `unreconciled_difference` plug ($123,604.88 with current placeholders) forces the sheet to balance; it reaches $0 when all ACCOUNTANT INPUT items are resolved.

---

## 8. Rerun Procedure

### Step 1: Install dependencies (if needed)
```bash
pip install openpyxl reportlab
```

### Step 2: (Optional) Refresh BigQuery snapshots
Update the constants in `BQ_RESULTS` by running the SQL in `BQ_QUERIES` against BigQuery:
```bash
bq query --nouse_legacy_sql '<paste query from BQ_QUERIES>'
```
Paste results back into the appropriate keys in `BQ_RESULTS`. Always use `voided IS NULL OR voided = 'false'` to keep revenue net of voids (consistent with the accepted SBA P&L basis).

### Step 3: Fill ACCOUNTANT INPUT constants
When the accountant supplies confirmed values, update the corresponding constants and placeholders in `build_balance_sheet()`:
- `cash` (line ~723): replace `0.0` with confirmed BofA closing balance
- `depreciation_2025` (line ~754): replace `0.0` with fixed-asset schedule figure
- `net_income_2025` (line ~876): replace `0.0` with accrual-basis P&L net income
- `accounts_payable` (line ~817): replace `0.0` with confirmed AP
- `accrued_payroll` (line ~824): replace `0.0` with confirmed payroll accrual
- `contributions_2025` (line ~853): replace `0.0` with confirmed contributions

### Step 4: Generate outputs
```bash
cd /Users/maurice_mac/Library/CloudStorage/Dropbox/Developer/Toast_ETL_Workflow
python adjusted_balance_sheet_2025.py
```

### Step 5: Verify with tests
```bash
pytest tests/test_adjusted_bs_math.py --noconftest -v
```
All 25 tests must pass before treating output as final.

### Step 6: Copy to Dropbox
```bash
DEST="/Users/maurice_mac/Library/CloudStorage/Dropbox/Predictive_Models/PMG/Financial Analysis/2025/2025 Adjusted Balance Sheet"
cp LOV3_HTX_2025_Adjusted_Balance_Sheet.xlsx "$DEST/"
cp LOV3_2025_Adjusted_Balance_Sheet_Leadership_Report.pdf "$DEST/"
ls -lh "$DEST/"
```

---

## 9. What NOT to Do

- NEVER commit `LOV3_HTX_2025_Adjusted_Balance_Sheet.xlsx` or `LOV3_2025_Adjusted_Balance_Sheet_Leadership_Report.pdf` to git (they are untracked, regenerated each run)
- NEVER restate numbers accepted in prior lender submissions (Lender Consistency Rule) — refinements go in Assumptions & Flags, not the primary statement
- NEVER call the MB under-filing finding "confirmed fraud" — frame as "pattern signal under review" until BAED closes the investigation
- NEVER adjust `BEGINNING_BALANCES` without a confirmed 2024 Schedule L source — those numbers come from the 2024 Form 1065 filed return
- NEVER net MB GR (6.7%) from revenue — it is LOV3's own operating expense and belongs in Taxes & Licenses
- NEVER mark collected taxes (MB Sales 8.25%, S&U 8.25%) as revenue — they are pass-through liabilities

---

## 10. Related Files

| File | Purpose |
|------|---------|
| `adjusted_balance_sheet_2025.py` | The script — all constants, math, and Excel/PDF writers |
| `tests/test_adjusted_bs_math.py` | 25 unit tests for pure math functions |
| `SBA_2025_FOLLOWUP_ITEMS.md` | Balance sheet inputs, tax recon, data gap inventory |
| `SBA_FINANCIAL_STATEMENT_METHODOLOGY.md` | SBA P&L methodology reference |
| Dropbox `LOV3_HTX_Financial_Statements_SBA.xlsx` | Source of 2025 net income for ACCOUNTANT INPUT |
| Dropbox `SBA Loan Artifacts/LOV3 P&L Analysis/LOV3_SBA_PL_Package_Updated.xlsx` | Accepted SBA P&L package |
