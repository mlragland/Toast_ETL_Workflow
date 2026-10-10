# 2025 Adjusted Balance Sheet for K-1 Preparation — Design

**Date:** 2026-10-10
**Owner:** Maurice Ragland
**Purpose:** Produce a tax-reconciled, adjusted 12/31/2025 balance sheet (plus revenue/cost adjustment recommendations and a leadership report) so BAED Corporation (Sanchez Burnside) can complete the 2025 Form 1065, Schedule L, and partner K-1s.

## Decisions (confirmed with Maurice)

1. **Deliverable form:** Write the methodology as a reusable prompt document AND execute it now (Approach C from brainstorming).
2. **Anchor:** Texas Comptroller WebFile filings are the source of truth. Toast-derived revenue is adjusted to match filed taxable receipts; variances become reclasses or flagged adjustments.
3. **$59,175 Legal Order (Aug 25, 2025):** Verify against WebFile payment histories. Maurice recalls the Comptroller may have required a **security bond** to secure future payments. If the amount matches no filed period → classify as probable Comptroller security bond → **Other Assets** (refundable deposit), flagged for accountant confirmation. If it matches filings → tax remittance.
4. **Basis:** Accrual (matches the 2024 Form 1065, which checked "Accrual").
5. **Implementation:** Approach 1 — standalone generator script following the `sba_financial_statements.py` pattern.

## Data Sources

| Source | Location | Use |
|---|---|---|
| MB Gross Receipts Tax history | `Dropbox/Predictive_Models/PMG/Financial Analysis/2025/2025 Adjusted Balance Sheet/MB Gross Receipts Tax_20260403121740.pdf` | 6.7% tax on MB gross receipts — **LOV3's expense** (not collected from customers) |
| MB Sales Tax history | same folder, `MB Sales Tax.pdf` | 8.25% collected from customers on mixed beverages — **liability/pass-through** |
| Sales & Use Tax history | same folder, `Sales and Use Tax.pdf` | 8.25% on non-MB taxable sales (food/NA/merch) — collected, liability |
| Toast POS 2025 sales | BigQuery `toast_raw` (ItemSelectionDetails_raw / PaymentDetails_raw / OrderDetails_raw) | Book revenue by category (liquor/beer/wine vs food/NA) |
| 2024 Form 1065 | `SBA Loan Artifacts/2024 LOV3 Tax Returns/2024 1065 w Schedules.pdf` | Beginning balances + accounting method + prior-year treatment |
| 2024 K-1s | `SBA Loan Artifacts/2024 K1 LOV3/` | Partner structure, capital account continuity |
| BofA transactions | BigQuery `BankTransactions_raw` | Cash at 12/31/2025; locate the $59,175 legal order |
| SBA follow-up checklist | `SBA_2025_FOLLOWUP_ITEMS.md` | Open balance-sheet inputs |

All three WebFile PDFs are single-page scans; data is transcribed once into script constants (auditable against the PDFs).

### 2024 Schedule L — ending balances (= 1/1/2025 beginning balances)

| Line | Amount |
|---|---|
| Cash | $72,435 |
| Accounts receivable | $373,592 |
| Inventory | $32,191 |
| Depreciable assets / accum. dep. | $80,900 / $(9,278) → net $71,622 |
| **Total assets** | **$549,839** |
| Liabilities | $0 (none reported) |
| Partners' capital | $549,839 (M-2: $585,000 contributed − $35,161 loss) |

Partners: Maurice Ragland (15% capital per K-1), Eddiele B Jasper (partnership representative), Derwin James. Preparer: Sanchez Burnside, BAED Corporation.

## Components

### 1. `PROMPT_2025_ADJUSTED_BALANCE_SHEET.md` (repo root)
The user-requested prompt document. Self-contained instructions that a fresh Claude Code session could follow to reproduce or re-run the analysis: data source paths, tax math, anchor rule, accrual rule, open flags, output spec.

### 2. `adjusted_balance_sheet_2025.py` (repo root, standalone — not deployed)
Sections:
- **TAX_FILINGS constants** — transcribed WebFile rows: `(tax_type, period, period_end, txn_type, txn_date, amount, reference)`. CY2025 = periods 2501–2512 for MB taxes; S&U = quarterly 251/252 + monthly 2507–2512. Amended returns supersede originals for the same period.
- **Implied receipts calc** — MB taxable receipts = MB Sales Tax returns ÷ 0.0825; cross-check vs MB GR returns ÷ 0.067 (both tax the same base; divergence is itself a flag). S&U taxable sales = S&U returns ÷ 0.0825 (note: Houston combined rate 8.25%).
- **Toast comparison** — BigQuery 2025 net sales by sales category (business-day basis, 4 AM cutoff via `BUSINESS_DAY_SQL`), liquor/beer/wine vs food/NA buckets, compared month-by-month to implied receipts.
- **Adjustment engine** — produces journal-style adjustment recommendations:
  - Revenue side: adjust book revenue to filed taxable receipts; classify variance drivers (comps, voids, service-charge exclusion, cash not rung, timing).
  - Cost side: MB Gross Receipts tax (6.7%) booked as LOV3 operating expense (Taxes & Licenses), NOT netted from revenue; collected MB Sales / S&U taxes excluded from revenue (pass-through liability).
  - $59,175: match single payments and same-day combination sums across all three tax accounts near 2025-08-25; if unmatched → Other Assets "TX Comptroller Security Bond (unconfirmed)".
- **Balance sheet builder (12/31/2025, accrual)** —
  - Beginning balances from 2024 Schedule L.
  - Cash: BofA balance at 12/31/2025 from bank data (or flagged input).
  - Taxes payable: period 2511 + 2512 return amounts across all three tax types (paid Jan/Feb 2026).
  - Inventory / fixed assets / AP / accrued payroll: carry known values, else explicit `ACCOUNTANT INPUT` markers.
  - Equity roll-forward: beginning capital + 2025 contributions − 2025 distributions (P&L shows $231,907 owner draws) ± 2025 net income (from adjusted P&L figures).
  - Balance check: assets = liabilities + equity; any plug is surfaced as a named reconciling item, never hidden.
- **Outputs** —
  - `LOV3_HTX_2025_Adjusted_Balance_Sheet.xlsx` with tabs: Balance Sheet, Tax Reconciliation, Revenue Adjustments, Assumptions & Flags.
  - Leadership report PDF (plain-language summary: what changed, why, what the accountant still needs).
  - Both written to the `2025 Adjusted Balance Sheet` Dropbox folder (and repo copies).

### 3. Leadership report contents
- Headline: filed-vs-book revenue variance and its causes.
- The three tax types explained in one paragraph each (who pays, who collects).
- Balance sheet summary with taxes payable at year-end.
- Open items for the accountant (explicit list).
- Framing follows the Lender Consistency rule: refinements and uncertainties live in Assumptions & Flags, not silently merged into the primary statement.

## Known flags to carry into Assumptions & Flags

1. **MB GR step change:** periods 2503–2506 filed ~$1.9–4.1K/mo; 2507+ filed ~$17–23K/mo, with amended/struck-through returns for 2504–2505. Pattern suggests a mid-2025 Comptroller correction/audit event — consistent with the bond recollection. Quantify the under-filed early-2025 months vs Toast data.
2. **$59,175 legal order** classification (bond vs levy) — pending verification + accountant confirmation.
3. **2024 Schedule L reported zero liabilities** — if 2025 shows taxes payable (it will, under accrual), note the presentation change for the accountant.
4. **2024 K-1 allocation:** Maurice's K-1 carries the full $(35,161) loss but 15% capital — flag allocation consistency question for 2025 K-1s.
5. **$3,249 sales tax variance** from prior reconciliation — restate using the now-complete WebFile data.
6. **A/R $373,592 at 12/31/2024** — large for a cash-and-card restaurant; likely Toast settlement receivable or related-party; accountant to confirm 2025 treatment.

## Error handling / verification

- Script is pure standalone (no Flask import); verify with a direct run.
- Cross-check: MB Sales base vs MB GR base must agree within rounding per month — report divergence, don't average silently.
- Balance sheet must tie (assets = L + E) or fail loudly with the gap amount.
- Excel totals recomputed from components, not hardcoded.
- No emails/Slack sent. Files written locally only.

## Out of scope

- Filing anything with the Comptroller or IRS.
- Regenerating the 2025 P&L (exists; adjustments reference it).
- 2026 periods (2601/2602) — excluded from the 12/31/2025 statement except as subsequent-event context for the Jan/Feb 2026 payments of 2511/2512 liabilities.
