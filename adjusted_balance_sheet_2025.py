"""
LOV3 HTX — 2025 Adjusted Balance Sheet (K-1 Prep)

Tax-filing constants transcribed 2026-10-10 from the Texas Comptroller WebFile PDF
histories located in:
  Dropbox/Predictive_Models/PMG/Financial Analysis/2025/2025 Adjusted Balance Sheet/

Three single-page scans cover MB Gross Receipts Tax, MB Sales Tax, and Sales & Use Tax.
struck=True rows are crossed out on the scan (superseded by a later amended return).

Pure math functions at module level so tests/test_adjusted_bs_math.py can import them
without triggering any BigQuery, Flask, or optional-dependency imports.
"""

from collections import defaultdict
from itertools import combinations

# ---------------------------------------------------------------------------
# Tax rates
# ---------------------------------------------------------------------------

MB_SALES_RATE = 0.0825   # collected on mixed-beverage sales
MB_GR_RATE    = 0.067    # LOV3-paid on the same MB receipts base
SU_RATE       = 0.0825   # Houston combined state+local rate on non-MB taxable sales

# ---------------------------------------------------------------------------
# MB Gross Receipts Tax (6.7% — LOV3 expense)
# Tuple format: (txn_type, filed_date, ref, period, period_end, amount, struck)
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# MB Sales Tax (8.25% — collected from customers, pass-through liability)
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Sales & Use Tax (8.25% — collected on non-MB taxable sales)
# Periods 251/252 are quarterly (Q1/Q2 2025); monthly from 2507.
# Period 244 is Q4-2024: liability belongs to 2024; Feb-2025 payment is a
# 2025 cash outflow only — excluded from year=2025 liability calculations.
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# 2024 Schedule L ending balances (= 1/1/2025 beginning balances)
# ---------------------------------------------------------------------------

BEGINNING_BALANCES = {
    "cash": 72435, "accounts_receivable": 373592, "inventory": 32191,
    "depreciable_assets": 80900, "accumulated_depreciation": 9278,
    "total_assets": 549839, "total_liabilities": 0, "partners_capital": 549839,
}

PARTNERS = {
    "Maurice Ragland": 0.15,
    "Eddiele B Jasper": None,
    "Derwin James": None,
}  # 2024 K-1 shows only Maurice's 15% capital; others flagged ACCOUNTANT INPUT

# ---------------------------------------------------------------------------
# Pure math functions
# ---------------------------------------------------------------------------


def _period_year(period_end: str) -> int:
    return int(period_end[:4])


def period_liabilities(rows: list, year: int = 2025) -> dict:
    """Filed liability per period: sum of non-struck return/amended rows."""
    out: defaultdict = defaultdict(float)
    for txn, filed, ref, period, pend, amount, struck in rows:
        if txn in ("return", "amended") and not struck and _period_year(pend) == year:
            out[period] += amount
    return {p: round(v, 2) for p, v in out.items()}


def period_payments(rows: list, year: int = 2025) -> dict:
    out: defaultdict = defaultdict(float)
    for txn, filed, ref, period, pend, amount, struck in rows:
        if txn == "payment" and not struck and _period_year(pend) == year:
            out[period] += amount
    return {p: round(v, 2) for p, v in out.items()}


def taxes_payable_at_yearend(rows: list, asof: str = "2025-12-31") -> float:
    """Liabilities for periods ending <= asof with no full payment on or before asof."""
    liabs = period_liabilities(rows, year=int(asof[:4]))
    paid_by_asof: defaultdict = defaultdict(float)
    for txn, filed, ref, period, pend, amount, struck in rows:
        if txn == "payment" and not struck and filed <= asof:
            paid_by_asof[period] += amount
    return round(sum(v for p, v in liabs.items() if paid_by_asof[p] < v - 0.01), 2)


def penalties_and_interest(rows: list, year: int = 2025) -> dict:
    """Per-period payments minus liability (positive = P&I expense)."""
    liabs = period_liabilities(rows, year)
    pays = period_payments(rows, year)
    return {
        p: round(pays.get(p, 0) - liabs[p], 2)
        for p in liabs
        if abs(pays.get(p, 0) - liabs[p]) > 0.01
    }


def implied_receipts(liabs: dict, rate: float) -> dict:
    return {p: round(v / rate, 2) for p, v in liabs.items()}


def match_legal_order(
    amount: float = 59175.00,
    window: tuple = ("2025-07-01", "2025-09-30"),
    tolerance: float = 0.01,
    rows: list | None = None,
    reversal_rows: list | None = None,
) -> dict:
    """Search WebFile payment rows for a single payment or same-day combination
    (across MB_GR, MB_SALES, SALES_USE) equal to `amount` within `window`.

    Parameters
    ----------
    amount : float
        Dollar amount to search for (default $59,175.00).
    window : tuple[str, str]
        Inclusive ISO date window ``("YYYY-MM-DD", "YYYY-MM-DD")``.
    tolerance : float
        Maximum absolute difference allowed for a match (default 0.01).
    rows : list | None
        Override the combined WebFile rows used for matching.  When None the
        three module-level constants (MB_GR, MB_SALES, SALES_USE) are combined.
        Each row must be a 7-tuple: (txn_type, filed_date, ref, period,
        period_end, amount, struck).
    reversal_rows : list | None
        Override the BankTransactions rows scanned for a positive reversal
        matching ``amount``.  Each row must be a 3-tuple:
        ``(date_str, description, amount)``.  When None defaults to
        ``BQ_RESULTS.get("legal_order_rows", [])``.

    Returns
    -------
    dict
        ``{"matched": bool, "matches": list, "classification": str}``

        *matches* — each entry is a list of payment rows (as dicts with an
        added ``tax_type`` key) whose combined amount equals ``amount`` within
        ``tolerance``.  An entry with one element is a single-payment match;
        an entry with two or more elements is a same-day combination.

        *classification* — one of three strings depending on outcome:
        - matched → "tax remittance — matches WebFile payment(s); reclassify
          from legal order to tax payment"
        - not matched + reversal found → "temporary hold, fully reversed
          <reversal_date>, net zero — no balance sheet impact; remove from
          OPEX if booked as expense"
        - not matched + no reversal → "TX Comptroller Security Bond
          (unconfirmed) — Other Assets, flag for accountant"
    """
    window_start, window_end = window

    # Build the candidate pool from the three constants (or injected rows).
    if rows is None:
        tagged: list[tuple] = (
            [(r, "MB_GR") for r in MB_GR]
            + [(r, "MB_SALES") for r in MB_SALES]
            + [(r, "SALES_USE") for r in SALES_USE]
        )
    else:
        # Injected rows: tag with a generic label.
        tagged = [(r, "injected") for r in rows]

    # Filter to qualifying payment rows inside the window.
    candidates: list[dict] = []
    for row, tax_type in tagged:
        txn_type, filed_date, ref, period, period_end, row_amount, struck = row
        if (
            "payment" in txn_type.lower()
            and not struck
            and window_start <= filed_date <= window_end
        ):
            candidates.append({
                "txn_type": txn_type,
                "filed_date": filed_date,
                "ref": ref,
                "period": period,
                "period_end": period_end,
                "amount": row_amount,
                "struck": struck,
                "tax_type": tax_type,
            })

    # Group candidates by filed_date for same-day combination checks.
    by_date: dict[str, list[dict]] = defaultdict(list)
    for c in candidates:
        by_date[c["filed_date"]].append(c)

    matches: list[list[dict]] = []

    for date_str, day_rows in sorted(by_date.items()):
        # Check single payments.
        for row in day_rows:
            if abs(row["amount"] - amount) <= tolerance:
                matches.append([row])

        # Check all same-day subsets of size 2..n (single rows already caught above).
        for r in range(2, len(day_rows) + 1):
            for combo in combinations(day_rows, r):
                combo_sum = sum(c["amount"] for c in combo)
                if abs(combo_sum - amount) <= tolerance:
                    matches.append(list(combo))

    if matches:
        classification = (
            "tax remittance — matches WebFile payment(s); "
            "reclassify from legal order to tax payment"
        )
        return {"matched": True, "matches": matches, "classification": classification}

    # Not matched — check for the reversal row in BQ_RESULTS (or injected rows).
    _reversal_pool = (
        reversal_rows if reversal_rows is not None
        else BQ_RESULTS.get("legal_order_rows", [])
    )
    reversal_date: str | None = None
    for row_date, _, row_amount in _reversal_pool:
        if row_amount > 0 and abs(row_amount - amount) <= tolerance:
            reversal_date = row_date
            break

    if reversal_date is not None:
        classification = (
            f"temporary hold, fully reversed {reversal_date}, net zero — "
            "no balance sheet impact; remove from OPEX if booked as expense"
        )
    else:
        classification = (
            "TX Comptroller Security Bond (unconfirmed) — "
            "Other Assets, flag for accountant"
        )

    return {"matched": False, "matches": [], "classification": classification}


# ---------------------------------------------------------------
# Reproducibility — SQL used to generate the snapshots below.
# Project: toast-analytics-444116  Dataset: toast_raw
# All queries use processing_date (DATE) which already encodes
# the 4 AM CT business-day cutoff stored at ingest time.
# Run with: bq query --nouse_legacy_sql '<sql>'
# ---------------------------------------------------------------
BQ_QUERIES = {
    # Step 1 — enumerate every sales_category and its 2025 full-year net_price.
    # MB bucket = Liquor, Wine, Bottled Beer.  NON_MB = everything else.
    "sales_category_classification": """
SELECT
    sales_category,
    ROUND(SUM(CAST(net_price AS FLOAT64)), 2) AS total_net_price,
    COUNT(*) AS row_count
FROM `toast-analytics-444116.toast_raw.ItemSelectionDetails_raw`
WHERE processing_date BETWEEN '2025-01-01' AND '2025-12-31'
    AND (voided IS NULL OR voided = 'false')
GROUP BY sales_category
ORDER BY total_net_price DESC
""",

    # Step 2 — monthly MB vs NON_MB revenue (business-day basis, net_price sum).
    # Also breaks out individual categories to match the per-category comments below.
    # NULL-bucketing fix: NON_MB uses an OR IS NULL guard so NULL-category rows
    # land in NON_MB instead of being silently dropped by NOT IN (NULL NOT IN → NULL).
    "monthly_revenue_2025": """
SELECT
    FORMAT_DATE('%Y-%m', processing_date) AS month,
    ROUND(SUM(CASE WHEN sales_category IN ('Liquor', 'Wine', 'Bottled Beer')
                   THEN CAST(net_price AS FLOAT64) ELSE 0 END), 2) AS MB_rev,
    ROUND(SUM(CASE WHEN sales_category NOT IN ('Liquor', 'Wine', 'Bottled Beer')
                        OR sales_category IS NULL
                   THEN CAST(net_price AS FLOAT64) ELSE 0 END), 2) AS NON_MB_rev,
    ROUND(SUM(CASE WHEN sales_category = 'Liquor'       THEN CAST(net_price AS FLOAT64) ELSE 0 END), 2) AS liquor_rev,
    ROUND(SUM(CASE WHEN sales_category = 'Wine'         THEN CAST(net_price AS FLOAT64) ELSE 0 END), 2) AS wine_rev,
    ROUND(SUM(CASE WHEN sales_category = 'Bottled Beer' THEN CAST(net_price AS FLOAT64) ELSE 0 END), 2) AS beer_rev,
    ROUND(SUM(CASE WHEN sales_category = 'Food'         THEN CAST(net_price AS FLOAT64) ELSE 0 END), 2) AS food_rev,
    ROUND(SUM(CASE WHEN sales_category = 'NA Beverage'  THEN CAST(net_price AS FLOAT64) ELSE 0 END), 2) AS na_bev_rev,
    ROUND(SUM(CASE WHEN sales_category = 'Hookah'       THEN CAST(net_price AS FLOAT64) ELSE 0 END), 2) AS hookah_rev,
    ROUND(SUM(CASE WHEN sales_category IN ('(none)', 'Door')
                        OR sales_category IS NULL
                   THEN CAST(net_price AS FLOAT64) ELSE 0 END), 2) AS other_rev
FROM `toast-analytics-444116.toast_raw.ItemSelectionDetails_raw`
WHERE processing_date BETWEEN '2025-01-01' AND '2025-12-31'
    AND (voided IS NULL OR voided = 'false')
GROUP BY month
ORDER BY month
""",

    # Step 6 — monthly voided MB / NON_MB for 2025.
    # Retained as a revenue-variance driver: new_grand + voided_annual == old_gross.
    # Same NULL-bucketing guard as monthly_revenue_2025.
    "voided_items_2025": """
SELECT
    FORMAT_DATE('%Y-%m', processing_date) AS month,
    ROUND(SUM(CASE WHEN sales_category IN ('Liquor', 'Wine', 'Bottled Beer')
                   THEN CAST(net_price AS FLOAT64) ELSE 0 END), 2) AS voided_MB,
    ROUND(SUM(CASE WHEN sales_category NOT IN ('Liquor', 'Wine', 'Bottled Beer')
                        OR sales_category IS NULL
                   THEN CAST(net_price AS FLOAT64) ELSE 0 END), 2) AS voided_NON_MB,
    ROUND(SUM(CAST(net_price AS FLOAT64)), 2) AS voided_total
FROM `toast-analytics-444116.toast_raw.ItemSelectionDetails_raw`
WHERE processing_date BETWEEN '2025-01-01' AND '2025-12-31'
    AND voided = 'true'
GROUP BY month
ORDER BY month
""",

    # Step 3 — cash position at year-end.
    # Returns the last running_balance row on 2025-12-31 (intraday low after
    # payroll wire; use BofA Dec-2025 statement closing balance instead).
    "cash_balance_20251231": """
SELECT
    transaction_date,
    description,
    amount,
    running_balance
FROM `toast-analytics-444116.toast_raw.BankTransactions_raw`
WHERE transaction_date = '2025-12-31'
ORDER BY running_balance ASC
LIMIT 5
""",

    # Step 4 — legal order debit/reversal rows in Aug 2025.
    # Searches for ABS(amount) matching ±59,175 in the Aug 2025 window.
    "legal_order_rows": """
SELECT
    transaction_date,
    description,
    amount
FROM `toast-analytics-444116.toast_raw.BankTransactions_raw`
WHERE transaction_date BETWEEN '2025-08-01' AND '2025-08-31'
    AND ABS(amount) = 59175
ORDER BY transaction_date
""",

    # Step 5 — 2025 owner-attributable spend from BankTransactions_raw.
    # Matches the three owner-labeled categories in the LOV3 numbered taxonomy.
    "owner_distributions_2025": """
SELECT
    category,
    COUNT(*) AS txn_count,
    ROUND(SUM(amount), 2) AS total_amount
FROM `toast-analytics-444116.toast_raw.BankTransactions_raw`
WHERE transaction_date BETWEEN '2025-01-01' AND '2025-12-31'
    AND (
        REGEXP_CONTAINS(LOWER(category), r'owner draws')
        OR REGEXP_CONTAINS(LOWER(category), r'owner discretionary')
        OR REGEXP_CONTAINS(LOWER(category), r'owner travel')
    )
GROUP BY category
ORDER BY total_amount
""",
}

# ---------------------------------------------------------------
# BigQuery snapshots — re-pulled 2026-10-10 (corrected run).
# Rerun the SQL in BQ_QUERIES above to refresh these snapshots.
#
# IMPORTANT — SNAPSHOTS ARE NET OF VOIDS
# All revenue figures below exclude voided items (voided = 'true').
# This is consistent with sba_financial_statements.py (lines 164/188/226/501)
# and the accepted SBA P&L basis filed with the lender.
#
# Prior snapshot (before this correction) erroneously INCLUDED $176,422.30 of
# voided 2025 items, inflating MB by $159,144.23 and NON_MB by $17,278.07.
# Voided totals are now separately tracked in BQ_RESULTS["voided_items_2025"]
# as a revenue-variance driver.  Cross-check: new_grand + voided_annual =
# 4,250,961.87 + 176,422.30 = 4,427,384.17 (old gross, exact match).
# ---------------------------------------------------------------
BQ_RESULTS = {
    # Step 1 — sales category classification
    # MB bucket = alcohol (Texas Mixed Beverage tax applies)
    # NON_MB = food, NA beverage, hookah, door, uncategorized / NULL category
    # No ambiguous categories — each was clearly identifiable.
    # Revenue figures below are 2025 full-year, NET OF VOIDS (business-day basis, net_price sum).
    "sales_category_classification": {
        "mb":      ["Liquor", "Wine", "Bottled Beer"],
        "non_mb":  ["Food", "NA Beverage", "Hookah", "(none)", "Door", "NULL"],
        "ambiguous": [],
    },
    # Per-category full-year 2025 revenue (net_price, business-day basis, NET OF VOIDS):
    #   Liquor       3,286,827.30  (prior snapshot erroneously showed 3,445,320.28 — includes voids)
    #   Food           672,441.54  (prior: 683,866.09)
    #   NA Beverage    137,162.03  (prior: 139,134.03)
    #   Hookah         102,673.09  (in-house POS only; Predictive Insights deposits are in BankTransactions_raw; prior: 106,434.61)
    #   Wine            28,167.30  (prior: 28,587.30)
    #   Bottled Beer    21,681.90  (prior: 21,913.15)
    #   (none)/NULL      1,368.71  (unchanged — no voided rows in NULL category)
    #   Door               640.00  (prior: 760.00)
    # MB total: 3,336,676.50  |  NON_MB total: 914,285.37  |  Grand: 4,250,961.87

    # Step 2 — monthly revenue by bucket (business-day basis, 2025-01 through 2025-12, NET OF VOIDS)
    "monthly_revenue_2025": {
        "2025-01": {"MB": 282350.20, "NON_MB":  93219.66},
        "2025-02": {"MB": 251502.09, "NON_MB": 100471.71},
        "2025-03": {"MB": 347544.06, "NON_MB": 124950.24},
        "2025-04": {"MB": 272526.60, "NON_MB":  65968.80},
        "2025-05": {"MB": 318686.60, "NON_MB":  66570.58},
        "2025-06": {"MB": 229306.47, "NON_MB":  62974.61},
        "2025-07": {"MB": 255180.27, "NON_MB":  66025.17},
        "2025-08": {"MB": 330018.62, "NON_MB":  66940.68},
        "2025-09": {"MB": 247467.78, "NON_MB":  60013.70},
        "2025-10": {"MB": 279621.50, "NON_MB":  71634.09},
        "2025-11": {"MB": 261649.13, "NON_MB":  75022.94},
        "2025-12": {"MB": 260823.18, "NON_MB":  60493.19},
    },
    # MB annual total: 3,336,676.50  |  NON_MB annual total: 914,285.37
    # Grand total POS net_price (net of voids): 4,250,961.87
    # Gross POS net_price (including voids):    4,427,384.17  (= new grand + voided_items_2025 annual)

    # Step 6 — voided items 2025 by month (revenue-variance driver; retained for audit trail)
    # Cross-check: voided_annual_total 176,422.30 + new grand 4,250,961.87 = 4,427,384.17 (exact)
    "voided_items_2025": {
        "2025-01": {"voided_MB":  14540.96, "voided_NON_MB":  2498.70, "voided_total":  17039.66},
        "2025-02": {"voided_MB":  11998.41, "voided_NON_MB":  4274.14, "voided_total":  16272.55},
        "2025-03": {"voided_MB":  15182.11, "voided_NON_MB":  2250.68, "voided_total":  17432.79},
        "2025-04": {"voided_MB":  11090.68, "voided_NON_MB":   658.25, "voided_total":  11748.93},
        "2025-05": {"voided_MB":  18585.70, "voided_NON_MB":   695.00, "voided_total":  19280.70},
        "2025-06": {"voided_MB":  10246.90, "voided_NON_MB":   644.00, "voided_total":  10890.90},
        "2025-07": {"voided_MB":   9788.47, "voided_NON_MB":   865.95, "voided_total":  10654.42},
        "2025-08": {"voided_MB":  11577.27, "voided_NON_MB":  1160.75, "voided_total":  12738.02},
        "2025-09": {"voided_MB":  14309.70, "voided_NON_MB":   503.00, "voided_total":  14812.70},
        "2025-10": {"voided_MB":  12696.14, "voided_NON_MB":  1588.60, "voided_total":  14284.74},
        "2025-11": {"voided_MB":  12559.52, "voided_NON_MB":  1432.00, "voided_total":  13991.52},
        "2025-12": {"voided_MB":  16568.37, "voided_NON_MB":   707.00, "voided_total":  17275.37},
    },
    # Voided annual: MB 159,144.23 | NON_MB 17,278.07 | Total 176,422.30

    # Step 3 — cash at 12/31/2025
    # Source: BankTransactions_raw stmt-14.csv (BofA CSV upload, single checking account).
    # running_balance column exists and is per-transaction in statement order.
    # Last (lowest) running_balance on 2025-12-31 = -$473.97 due to two $10 overdraft fees
    # posting after a $71,212.57 payroll wire left the account at $873.51.
    # The account balance just before OD fees was ~$873.51.
    # This is UNRELIABLE as a balance-sheet cash figure: it reflects an intraday low after a
    # large payroll wire on 12/31; does not include any savings or secondary accounts.
    # Plaid live sync began 2026-07-30 so it cannot backfill the 12/31/2025 closing balance.
    # ACCOUNTANT INPUT required — use BofA Dec 2025 statement closing balance instead.
    "cash_balance_20251231": None,
    "cash_balance_note": (
        "BQ stmt-14.csv shows -$473.97 at end of 12/31 due to $10 OD fees after $71K payroll wire. "
        "Intraday balance pre-wire was ~$74,082.61; post-wire before OD fees was ~$873.51. "
        "Use Dec-2025 BofA statement closing balance for the balance sheet. ACCOUNTANT INPUT."
    ),

    # Step 4 — legal order rows (ALL matches for LEGAL ORDER or LTS in description)
    # The $59,175 debit was REVERSED on 2025-08-29 — net impact to the account is $0.
    "legal_order_rows": [
        ("2025-08-25", "Legal Order, LTS PXXXXX5000391",        -59175.00),
        ("2025-08-29", "Legal Order Reversal, LTS PXXXXX5000391", 59175.00),
    ],
    "legal_order_note": (
        "Debit of $59,175 on 2025-08-25 was fully reversed on 2025-08-29 (net = $0). "
        "No other legal-order or LTS rows found in BankTransactions_raw. "
        "Confirm with BofA statements that reversal cleared; do not show as a 2025 expense."
    ),

    # Step 5 — 2025 owner distributions from BankTransactions_raw categories
    # Category column is present; query grouped by category for transaction_date 2025-01-01 to 2025-12-31.
    # Owner-related categories:
    #   Owner Draws / Transfers:          -172,406.51  (108 transactions)
    #   Owner Discretionary Expenses:      -43,676.41  ( 77 transactions)
    #   Owner Travel:                       -9,413.45  ( 30 transactions)
    # Combined owner-attributable:        -225,496.37
    # Prior P&L showed $231,907 Owner Draws — difference of ~$6,411 likely in
    # uncategorized rows (3 rows, -$1,740.77) and possible mis-bucket in Credit Card
    # Payments (-$48,759.49, 20 rows) or Internal Account Transfer (+$156,731.51).
    "owner_distributions_2025": {
        "Owner Draws / Transfers":       -172406.51,
        "Owner Discretionary Expenses":   -43676.41,
        "Owner Travel":                    -9413.45,
        "combined_owner_attributable":   -225496.37,
    },
    "owner_distributions_note": (
        "BQ bank category total for owner-attributable spend = $225,496.37. "
        "Prior P&L showed $231,907 — delta of ~$6,411. "
        "Possible gap: uncategorized rows ($1,740.77) and potential owner transactions "
        "miscategorized as Credit Card Payments ($48,759.49) or Admin & Office ($23,416.95). "
        "ACCOUNTANT INPUT: reconcile to K-1 distribution amount before filing."
    ),
}

