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

# Texas Comptroller timely-filing discount: 0.5% of the tax due when paid on time.
# Applies ONLY to Sales & Use Tax (Tax Code § 151.423 / Comptroller Rule 3.286).
# Mixed Beverage taxes (Tax Code § 183) are NOT eligible for this discount.
TIMELY_FILER_DISCOUNT_RATE = 0.005
# Maximum absolute difference between a period's remaining shortfall and the
# expected 0.5% discount amount before we stop treating it as a timely-filer discount.
TIMELY_FILER_TOLERANCE = 0.02

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

# Shared narrative for the MB step-change finding — used in Tab 3 variance drivers,
# Tab 4 SPEC FLAG 1, and the PDF Page 2 drivers section.  Keep per-site headings/short
# labels local; only the body text lives here.
MB_STEP_CHANGE_NARRATIVE = (
    "Jul–Aug 2025 (periods 2507–2508) filed MB base matches Toast POS to the dollar "
    "(variance <$6), validating the comparison method. "
    "Under-filed MB GR periods: 2501–2506 (liabilities $1,863–$4,616/mo) AND "
    "2509–2512 (~$4.2–5.3K/mo); only 2507–2508 (~$17–22K/mo) match Toast. "
    "Amended/struck returns for 2504–2505 are consistent with a mid-2025 Comptroller "
    "correction that did NOT persist after August. "
    "~$2.18M MB base and ~$442K non-MB base probable under-filing across 10 of 12 months. "
    "Refer to BAED and consider amended Comptroller returns. "
    "ACCOUNTANT REVIEW REQUIRED before any revenue adjustment is booked."
)

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


def taxes_payable_at_yearend(
    rows: list,
    asof: str = "2025-12-31",
    discount_eligible: bool = False,
) -> float:
    """Return the net unpaid tax liability for all periods whose period_end <= asof,
    after crediting payments made on or before asof.

    For each in-scope period the remaining balance is:
        max(0, liability - payments_made_by_asof)

    This correctly handles partial prepayments (prior implementation returned
    the full liability when any payment remained outstanding).

    Generalisation: periods are included when period_end <= asof (not restricted
    to a single calendar year), so the function is correct for mid-year asof dates.
    Payments with filed_date <= asof are credited regardless of which year's
    period they belong to.

    Timely-filer discount settlement (discount_eligible=True only):
        Texas Tax Code § 151.423 grants a 0.5% discount on Sales & Use Tax when
        the return and payment are filed on time.  Mixed Beverage taxes (§ 183)
        are NOT eligible.  When ``discount_eligible=True``, any period where
        (a) at least one payment was made by asof, (b) a positive shortfall
        remains, and (c) that shortfall is within ``TIMELY_FILER_TOLERANCE`` of
        0.5% of the period liability is treated as fully settled — the shortfall
        is the earned discount, not an unpaid balance.  Pass
        ``discount_eligible=False`` (the default) for MB_GR and MB_SALES rows.
    """
    balances = _period_balances(rows, asof)

    total = 0.0
    for p, (liability, paid) in balances.items():
        remaining = liability - paid
        if discount_eligible and paid > 0 and remaining > 0:
            if _is_timely_discount(liability, remaining):
                # Settled via timely-filer discount — remainder is $0
                continue
        total += max(0.0, remaining)
    return round(total, 2)


def timely_filer_discounts(
    rows: list,
    asof: str = "2025-12-31",
    discount_eligible: bool = False,
) -> dict:
    """Return a mapping of {period: discount_amount} for every period whose shortfall
    was recognised as a Texas Comptroller timely-filing discount (0.5% of liability).

    Only periods with at least one payment on or before asof, a positive remaining
    shortfall, and a shortfall within ``TIMELY_FILER_TOLERANCE`` of 0.5% × liability
    qualify.  Pass ``discount_eligible=True`` only for Sales & Use Tax rows (Tax Code
    § 151.423); Mixed Beverage taxes (§ 183) are NOT eligible — pass False (default)
    for MB_GR and MB_SALES rows.  Used by build_balance_sheet() to surface discount
    amounts in the audit flags.
    """
    if not discount_eligible:
        return {}

    balances = _period_balances(rows, asof)

    discounts: dict = {}
    for p, (liability, paid) in balances.items():
        remaining = liability - paid
        if paid > 0 and remaining > 0 and _is_timely_discount(liability, remaining):
            discounts[p] = round(remaining, 2)
    return discounts


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


# ---------------------------------------------------------------------------
# Shared private helpers (used by taxes_payable_at_yearend and
# timely_filer_discounts to avoid copy-pasting the grouping logic)
# ---------------------------------------------------------------------------

def _period_balances(rows: list, asof: str) -> dict:
    """Return ``{period: (liability, paid_by_asof)}`` for every period whose
    ``period_end <= asof``.  Struck rows are always excluded.

    *liability* — sum of non-struck return/amended rows for that period.
    *paid_by_asof* — sum of non-struck payment rows whose ``filed_date <= asof``.
    """
    liabs: defaultdict = defaultdict(float)
    paid: defaultdict = defaultdict(float)
    for txn, filed, ref, period, pend, amount, struck in rows:
        if struck:
            continue
        if txn in ("return", "amended") and pend <= asof:
            liabs[period] += amount
        elif txn == "payment" and filed <= asof:
            paid[period] += amount
    return {p: (round(v, 2), round(paid[p], 2)) for p, v in liabs.items()}


def _is_timely_discount(liability: float, shortfall: float) -> bool:
    """Return True when *shortfall* is within ``TIMELY_FILER_TOLERANCE`` of the
    Texas Comptroller 0.5% timely-filing discount on *liability*."""
    expected = round(liability * TIMELY_FILER_DISCOUNT_RATE, 2)
    return abs(shortfall - expected) <= TIMELY_FILER_TOLERANCE


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


# ---------------------------------------------------------------------------
# Balance Sheet Builder
# ---------------------------------------------------------------------------

def build_balance_sheet(asof: str = "2025-12-31") -> dict:
    """Build an accrual-basis balance sheet as of `asof` (default 2025-12-31).

    All lines that require external confirmation are marked ACCOUNTANT INPUT and
    carry a human-readable flag in bs["flags"].  Any resulting gap between
    total_assets and total_liabilities + total_equity is surfaced in
    bs["unreconciled_difference"] — never hidden.

    Returns
    -------
    dict with keys:
        as_of, basis, assets, liabilities, equity,
        total_assets, total_liabilities, total_equity,
        unreconciled_difference, flags
    """
    flags: list[str] = []

    # ------------------------------------------------------------------
    # ASSETS
    # ------------------------------------------------------------------

    # Cash — BQ stmt-14.csv shows an intraday low on 12/31 after payroll wire;
    # value is unreliable.  Accountant must supply Dec-2025 BofA closing balance.
    cash = 0.0
    flags.append(
        "ACCOUNTANT INPUT — cash: BQ shows -$473.97 (intraday low after $71K "
        "payroll wire on 12/31/2025); use Dec-2025 BofA statement closing balance. "
        "Placeholder: $0."
    )

    # Accounts Receivable — carrying 2024 Schedule L ending balance forward.
    # Large for a cash-and-card restaurant; likely Toast settlement receivable
    # or related-party balance.  Accountant must confirm 2025 treatment.
    accounts_receivable = BEGINNING_BALANCES["accounts_receivable"]  # 373,592
    flags.append(
        "ACCOUNTANT INPUT — accounts_receivable: carrying 2024 Schedule L ending "
        f"balance of ${accounts_receivable:,.2f} forward. "
        "Unusually large for a cash-and-card restaurant; likely Toast settlement "
        "receivable or related-party balance. Confirm 2025 year-end treatment."
    )

    # Inventory — carrying 2024 Schedule L ending balance forward.
    # Physical count at 12/31/2025 required.
    inventory = BEGINNING_BALANCES["inventory"]  # 32,191
    flags.append(
        "ACCOUNTANT INPUT — inventory: carrying 2024 Schedule L ending balance of "
        f"${inventory:,.2f} forward. Confirm with 12/31/2025 physical count."
    )

    # Fixed Assets — 2024 Schedule L gross amount is definitive.
    # Accumulated Depreciation: 2024 ending = $9,278; 2025 depreciation expense is
    # required from the fixed-asset schedule to compute the 12/31/2025 balance.
    gross_fixed_assets = BEGINNING_BALANCES["depreciable_assets"]  # 80,900
    accum_dep_prior = BEGINNING_BALANCES["accumulated_depreciation"]  # 9,278
    depreciation_2025 = 0.0  # ACCOUNTANT INPUT
    accumulated_depreciation = accum_dep_prior + depreciation_2025
    net_fixed_assets = gross_fixed_assets - accumulated_depreciation
    flags.append(
        "ACCOUNTANT INPUT — depreciation_2025: 2025 depreciation expense required "
        "from fixed-asset schedule. Placeholder $0 used; accumulated depreciation "
        f"carried at 2024 ending balance of ${accum_dep_prior:,.2f}."
    )

    # Legal Order / Bond — $59,175 debit on 2025-08-25 was fully reversed
    # 2025-08-29 (net = $0).  No balance sheet asset or liability is created.
    # Finding: Task 3 confirmed via match_legal_order() — classification
    # "temporary hold, fully reversed 2025-08-29, net zero — no balance sheet impact".
    flags.append(
        "NOTE — legal order $59,175: debit 2025-08-25 fully reversed 2025-08-29 "
        "(net = $0). No bond asset or liability on this balance sheet. "
        "Remove from OPEX if previously booked as an expense."
    )

    assets = {
        "cash":                    round(cash, 2),
        "accounts_receivable":     round(accounts_receivable, 2),
        "inventory":               round(inventory, 2),
        "gross_fixed_assets":      round(gross_fixed_assets, 2),
        "accumulated_depreciation": round(accumulated_depreciation, 2),
        "net_fixed_assets":        round(net_fixed_assets, 2),
    }
    total_assets = round(
        assets["cash"]
        + assets["accounts_receivable"]
        + assets["inventory"]
        + assets["net_fixed_assets"],
        2,
    )

    # ------------------------------------------------------------------
    # LIABILITIES
    # ------------------------------------------------------------------

    # Taxes Payable — sum of unpaid period-end liabilities across all three
    # Texas Comptroller tax accounts (MB GR, MB Sales, Sales & Use).
    # Periods 2511 and 2512 had payments filed in Jan/Feb 2026 (after 12/31/2025),
    # so those balances remain outstanding at year-end.
    # NOTE: timely-filer discount (§ 151.423) applies ONLY to Sales & Use Tax;
    # MB taxes (§ 183) are ineligible — discount_eligible=False for MB_GR and MB_SALES.
    taxes_payable = round(
        taxes_payable_at_yearend(MB_GR,    asof=asof, discount_eligible=False)
        + taxes_payable_at_yearend(MB_SALES,  asof=asof, discount_eligible=False)
        + taxes_payable_at_yearend(SALES_USE, asof=asof, discount_eligible=True),
        2,
    )

    # Timely-filer discount audit flag: surface any periods treated as settled via
    # the Texas Comptroller 0.5% timely-filing discount (not counted in taxes_payable).
    _su_discounts = timely_filer_discounts(SALES_USE, asof=asof, discount_eligible=True)
    if _su_discounts:
        for _period, _disc_amt in _su_discounts.items():
            flags.append(
                f"NOTE — S&U period {_period} timely-filer discount (0.5%): "
                f"${_disc_amt:.2f} shortfall treated as settled, not payable "
                "(Texas Comptroller timely-filing discount per Tax Code § 151.423)."
            )

    # Accounts Payable — no AP sub-ledger available; placeholder 0.
    accounts_payable = 0.0
    flags.append(
        "ACCOUNTANT INPUT — accounts_payable: no AP sub-ledger available. "
        "Placeholder $0. Confirm with vendor invoices outstanding at 12/31/2025."
    )

    # Accrued Payroll — no payroll accrual data available; placeholder 0.
    accrued_payroll = 0.0
    flags.append(
        "ACCOUNTANT INPUT — accrued_payroll: no payroll accrual data available. "
        "Placeholder $0. Confirm with pay-period cutoff at 12/31/2025."
    )

    # Presentation note: 2024 Schedule L reported zero liabilities; taxes_payable
    # here represents a change in presentation driven by accrual-basis recognition.
    flags.append(
        "NOTE — presentation change: 2024 Schedule L reported $0 total liabilities. "
        "This 2025 balance sheet introduces taxes_payable under accrual-basis recognition "
        "for periods 2511 (Nov 2025) and 2512 (Dec 2025) paid in Jan/Feb 2026."
    )

    liabilities = {
        "taxes_payable":   round(taxes_payable, 2),
        "accounts_payable": round(accounts_payable, 2),
        "accrued_payroll":  round(accrued_payroll, 2),
    }
    total_liabilities = round(sum(liabilities.values()), 2)

    # ------------------------------------------------------------------
    # EQUITY (Partners' Capital roll-forward)
    # ------------------------------------------------------------------

    # Beginning partners' capital = 2024 Schedule L ending balance.
    beginning_capital = BEGINNING_BALANCES["partners_capital"]  # 549,839

    # 2025 contributions — no capital contribution records available; placeholder 0.
    contributions_2025 = 0.0
    flags.append(
        "ACCOUNTANT INPUT — contributions_2025: no capital contribution records "
        "available. Placeholder $0. Confirm with bank records and partner agreements."
    )

    # 2025 distributions — BQ bank category total for owner-attributable spend.
    # BQ figure: $225,496.37 (Owner Draws/Transfers + Discretionary + Travel).
    # Prior SBA P&L showed $231,907; delta ~$6,411.  Both figures stated here;
    # accountant must reconcile to K-1 distribution amount before filing.
    distributions_2025 = abs(BQ_RESULTS["owner_distributions_2025"]["combined_owner_attributable"])
    flags.append(
        f"NOTE — distributions_2025: BQ bank-category total = ${distributions_2025:,.2f}. "
        "Prior SBA P&L showed $231,907 Owner Draws (delta ~$6,411). "
        "Possible gap: uncategorized rows ($1,740.77) or transactions miscategorized "
        "as Credit Card Payments / Admin & Office. "
        f"Using BQ figure (${distributions_2025:,.2f}) as the working value; "
        "reconcile to K-1 distribution amount before filing."
    )

    # 2025 net income — placeholder 0; reference the adjusted P&L / SBA package.
    # Do not invent a number here; the accrual-basis P&L holds the book figure.
    net_income_2025 = 0.0
    flags.append(
        "ACCOUNTANT INPUT — net_income_2025: placeholder $0. "
        "Reference the adjusted P&L / SBA package (LOV3_HTX_Financial_Statements_SBA.xlsx) "
        "for the book net income figure. Insert here before filing K-1s."
    )

    # Partner ownership — Maurice Ragland 15% confirmed via 2024 K-1.
    # Remaining 85% (Eddiele B Jasper and Derwin James) split unknown.
    flags.append(
        "ACCOUNTANT INPUT — partner_ownership: Maurice Ragland = 15% (from 2024 K-1). "
        "Ownership percentages for Eddiele B Jasper and Derwin James are unknown. "
        "Confirm from the LLC operating agreement before allocating K-1 income/loss."
    )

    ending_capital = round(
        beginning_capital + contributions_2025 - distributions_2025 + net_income_2025,
        2,
    )

    equity = {
        "beginning_partners_capital": round(beginning_capital, 2),
        "contributions_2025":         round(contributions_2025, 2),
        "distributions_2025":         round(-distributions_2025, 2),  # negative = reduction
        "net_income_2025":            round(net_income_2025, 2),
        "ending_partners_capital":    round(ending_capital, 2),
    }
    total_equity = round(ending_capital, 2)

    # ------------------------------------------------------------------
    # TIE CHECK — any gap becomes a named unreconciled_difference plug
    #
    # unreconciled_difference = total_assets - total_liabilities - pre-plug equity
    # Adding this plug to total_equity makes the sheet tie BY CONSTRUCTION.
    # When all ACCOUNTANT INPUT placeholders are resolved this should reach $0.
    # ------------------------------------------------------------------

    pre_plug_equity = total_equity  # ending_capital before the plug
    unreconciled_difference = round(
        total_assets - (total_liabilities + pre_plug_equity), 2
    )

    if abs(unreconciled_difference) > 0.01:
        flags.append(
            f"WARNING — unreconciled_difference = ${unreconciled_difference:,.2f}. "
            "This plug is included in total_equity to force the balance sheet to tie. "
            "Resolve ACCOUNTANT INPUT placeholders (cash, depreciation_2025, "
            "net_income_2025, accounts_payable, accrued_payroll) to eliminate this gap."
        )

    # Add the plug to the equity section so the sheet ties by construction.
    equity["unreconciled_difference_plug"] = unreconciled_difference
    total_equity = round(pre_plug_equity + unreconciled_difference, 2)

    return {
        "as_of":                   asof,
        "basis":                   "accrual",
        "assets":                  assets,
        "liabilities":             liabilities,
        "equity":                  equity,
        "total_assets":            total_assets,
        "total_liabilities":       total_liabilities,
        "total_equity":            total_equity,
        "unreconciled_difference": unreconciled_difference,
        "flags":                   flags,
    }


# ---------------------------------------------------------------------------
# Helper: period code → human label, e.g. "2511" → "Nov 2025"
# ---------------------------------------------------------------------------

_MONTH_NAMES = {
    "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
    "05": "May", "06": "Jun", "07": "Jul", "08": "Aug",
    "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec",
}


def _period_label(period: str) -> str:
    """Convert a Comptroller period code to a human label.

    Standard monthly codes (4-digit YYMM):
        "2511" → "Nov 2025"
    Quarterly S&U shorthand (3-digit YYQ):
        "251" → "Q1 2025 (Jan–Mar)"
        "252" → "Q2 2025 (Apr–Jun)"
    2024 carry-over:
        "244" → "Q4 2024 (Oct–Dec)"
    """
    if len(period) == 4:
        yy = period[:2]
        mm = period[2:]
        year = f"20{yy}"
        month = _MONTH_NAMES.get(mm, mm)
        return f"{month} {year}"
    if len(period) == 3:
        yy = period[:2]
        q = period[2]
        year = f"20{yy}"
        quarter_labels = {
            "1": "Q1 (Jan–Mar)",
            "2": "Q2 (Apr–Jun)",
            "3": "Q3 (Jul–Sep)",
            "4": "Q4 (Oct–Dec)",
        }
        return f"{quarter_labels.get(q, 'Q' + q)} {year}"
    return period


def _monthly_variance_table() -> list:
    """Return per-month variance rows comparing Toast book revenue to filed
    implied receipts.

    Each row is a dict with keys:
        month, toast_mb, toast_non_mb, toast_total,
        filed_mb_liab, filed_non_mb_liab,
        implied_mb_receipts, implied_non_mb_receipts,
        var_mb, var_non_mb
    where var_* = toast_book − implied_receipts (positive = book > filed).

    Toast uses 4 AM CT business-day cutoff; Comptroller periods end on the
    last calendar day of the month.  Small timing differences are expected.
    """
    mb_liabs = period_liabilities(MB_GR)          # keyed by period code e.g. "2501"
    mb_sales_liabs = period_liabilities(MB_SALES)  # same base → cross-check
    su_liabs = period_liabilities(SALES_USE)

    rows = []
    for month_key in sorted(BQ_RESULTS["monthly_revenue_2025"]):
        # month_key = "2025-01" etc.
        yy = month_key[2:4]    # "25"
        mm = month_key[5:7]    # "01"
        period = f"{yy}{mm}"   # "2501"

        toast_mb = BQ_RESULTS["monthly_revenue_2025"][month_key]["MB"]
        toast_non_mb = BQ_RESULTS["monthly_revenue_2025"][month_key]["NON_MB"]
        toast_total = round(toast_mb + toast_non_mb, 2)

        # Filed liabilities (may be quarterly for S&U in H1 2025)
        filed_mb_gr = mb_liabs.get(period, 0.0)
        filed_mb_sales = mb_sales_liabs.get(period, 0.0)

        # S&U: Q1=251 covers Jan/Feb/Mar; Q2=252 covers Apr/May/Jun
        # Distribute quarterly amounts equally across the 3 months in the quarter
        if mm in ("01", "02", "03"):
            su_period = f"{yy}1"  # "251"
            su_quarter_liab = su_liabs.get(su_period, 0.0)
            filed_su = round(su_quarter_liab / 3, 2)
        elif mm in ("04", "05", "06"):
            su_period = f"{yy}2"  # "252"
            su_quarter_liab = su_liabs.get(su_period, 0.0)
            filed_su = round(su_quarter_liab / 3, 2)
        else:
            filed_su = su_liabs.get(period, 0.0)

        # Implied receipts from each tax
        impl_mb_gr = round(filed_mb_gr / MB_GR_RATE, 2) if filed_mb_gr else 0.0
        impl_mb_sales = round(filed_mb_sales / MB_SALES_RATE, 2) if filed_mb_sales else 0.0
        impl_su = round(filed_su / SU_RATE, 2) if filed_su else 0.0

        var_mb = round(toast_mb - impl_mb_gr, 2)
        var_non_mb = round(toast_non_mb - impl_su, 2)

        rows.append({
            "month": month_key,
            "label": f"{_MONTH_NAMES[mm]} 2025",
            "toast_mb": toast_mb,
            "toast_non_mb": toast_non_mb,
            "toast_total": toast_total,
            "filed_mb_gr_liab": filed_mb_gr,
            "filed_mb_sales_liab": filed_mb_sales,
            "filed_su_liab": filed_su,
            "implied_mb_gr_receipts": impl_mb_gr,
            "implied_mb_sales_receipts": impl_mb_sales,
            "implied_su_receipts": impl_su,
            "var_mb": var_mb,
            "var_non_mb": var_non_mb,
        })
    return rows


# ---------------------------------------------------------------------------
# Excel writer (Tab 1–4)
# ---------------------------------------------------------------------------

XLSX_PATH = "LOV3_HTX_2025_Adjusted_Balance_Sheet.xlsx"

# Colours
_YELLOW = "FFFF00"
_HEADER_FILL = "1A1A1A"   # near-black
_ALT_FILL    = "F5F5F0"   # light cream
_GOLD_FILL   = "B8956A"   # section header gold
_RED_FILL    = "FDECEA"


def write_excel(bs: dict) -> str:
    """Write the four-tab workbook.  openpyxl is imported lazily here.

    Returns the output path.
    """
    try:
        from openpyxl import Workbook
        from openpyxl.styles import (
            Alignment, Border, Font, PatternFill, Side,
        )
        from openpyxl.utils import get_column_letter
    except ImportError:
        import sys
        print("openpyxl not installed. Run: pip install openpyxl", file=sys.stderr)
        sys.exit(1)

    wb = Workbook()

    # ------------------------------------------------------------------ helpers
    def _hdr_font(bold=True, color="FFFFFF", size=10):
        return Font(name="Calibri", bold=bold, color=color, size=size)

    def _body_font(bold=False, color="000000", size=9.5):
        return Font(name="Calibri", bold=bold, color=color, size=size)

    def _fill(hex_color):
        return PatternFill("solid", fgColor=hex_color)

    def _bottom_border():
        med = Side(border_style="medium", color="1A1A1A")
        return Border(bottom=med)

    def _currency(ws, row, col, value, is_input=False):
        """Write a currency value; highlight yellow if ACCOUNTANT INPUT."""
        cell = ws.cell(row=row, column=col, value=value)
        cell.number_format = '"$"#,##0.00'
        cell.alignment = Alignment(horizontal="right")
        cell.font = _body_font()
        if is_input:
            cell.fill = _fill(_YELLOW)
        return cell

    def _label(ws, row, col, text, bold=False, indent=0, italic=False):
        cell = ws.cell(row=row, column=col, value=text)
        cell.font = Font(name="Calibri", bold=bold, size=9.5, italic=italic)
        if indent:
            cell.alignment = Alignment(horizontal="left", indent=indent)
        return cell

    def _section_hdr(ws, row, text, ncols=4, color=_GOLD_FILL):
        """Merge + fill a section header row."""
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=ncols)
        cell = ws.cell(row=row, column=1, value=text)
        cell.font = Font(name="Calibri", bold=True, color="FFFFFF", size=10)
        cell.fill = _fill(color)
        cell.alignment = Alignment(horizontal="left", vertical="center", indent=1)

    def _col_hdr(ws, row, col, text):
        cell = ws.cell(row=row, column=col, value=text)
        cell.font = _hdr_font()
        cell.fill = _fill(_HEADER_FILL)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        return cell

    def _total_row(ws, row, label, val_2024, val_2025, ncols=4, bold=True):
        cell_l = ws.cell(row=row, column=1, value=label)
        cell_l.font = Font(name="Calibri", bold=bold, size=9.5)
        cell_l.border = _bottom_border()
        for col, val in ((2, val_2024), (3, val_2025)):
            cell = ws.cell(row=row, column=col, value=val)
            cell.number_format = '"$"#,##0.00'
            cell.alignment = Alignment(horizontal="right")
            cell.font = Font(name="Calibri", bold=bold, size=9.5)
            cell.border = _bottom_border()

    # ===================================================================
    # TAB 1 — Balance Sheet
    # ===================================================================
    ws1 = wb.active
    ws1.title = "Balance Sheet"

    # Title block
    ws1.merge_cells("A1:C1")
    ws1["A1"] = "LOV3 Restaurant & Lounge LLC — Adjusted Balance Sheet (K-1 Prep)"
    ws1["A1"].font = Font(name="Calibri", bold=True, size=14)
    ws1.merge_cells("A2:C2")
    ws1["A2"] = "Accrual Basis  |  Prepared for Sanchez Burnside / BAED Corporation"
    ws1["A2"].font = Font(name="Calibri", size=9.5, italic=True, color="555555")
    ws1.merge_cells("A3:C3")
    ws1["A3"] = "ACCOUNTANT INPUT cells highlighted yellow — do not present as final until resolved"
    ws1["A3"].font = Font(name="Calibri", size=9, italic=True, color="AA0000")

    # Column headers
    r = 5
    _col_hdr(ws1, r, 1, "Account")
    _col_hdr(ws1, r, 2, "12/31/2024\n(Schedule L)")
    _col_hdr(ws1, r, 3, "12/31/2025\n(This Workbook)")

    # Freeze pane at row 6
    ws1.freeze_panes = "A6"

    bb = BEGINNING_BALANCES
    assets = bs["assets"]
    liabilities = bs["liabilities"]
    equity = bs["equity"]

    r = 6
    # ---- ASSETS ----
    _section_hdr(ws1, r, "ASSETS", ncols=3)
    r += 1

    rows_assets = [
        ("Cash",                    bb["cash"],                  assets["cash"],                  True),
        ("Accounts Receivable",     bb["accounts_receivable"],   assets["accounts_receivable"],   True),
        ("Inventory",               bb["inventory"],             assets["inventory"],             True),
        ("Gross Fixed Assets",      bb["depreciable_assets"],    assets["gross_fixed_assets"],    False),
        ("Less: Accum. Depreciation", bb["accumulated_depreciation"], assets["accumulated_depreciation"], True),
        ("Net Fixed Assets",        bb["depreciable_assets"] - bb["accumulated_depreciation"],
                                     assets["net_fixed_assets"],  False),
    ]
    for label, v24, v25, is_input in rows_assets:
        bold_row = label.startswith("Net Fixed")
        _label(ws1, r, 1, label, bold=bold_row, indent=1)
        _currency(ws1, r, 2, v24)
        _currency(ws1, r, 3, v25, is_input=(is_input and label not in ("Net Fixed Assets",)))
        r += 1

    _total_row(ws1, r, "TOTAL ASSETS",
               bb["total_assets"],
               bs["total_assets"])
    r += 2

    # ---- LIABILITIES ----
    _section_hdr(ws1, r, "LIABILITIES", ncols=3)
    r += 1

    rows_liab = [
        ("Taxes Payable (TX Comptroller)",   0.0,  liabilities["taxes_payable"],   False),
        ("Accounts Payable",                 0.0,  liabilities["accounts_payable"], True),
        ("Accrued Payroll",                  0.0,  liabilities["accrued_payroll"],  True),
    ]
    for label, v24, v25, is_input in rows_liab:
        _label(ws1, r, 1, label, indent=1)
        _currency(ws1, r, 2, v24)
        _currency(ws1, r, 3, v25, is_input=is_input)
        r += 1

    _total_row(ws1, r, "TOTAL LIABILITIES", 0.0, bs["total_liabilities"])
    r += 2

    # ---- EQUITY ----
    _section_hdr(ws1, r, "PARTNERS' CAPITAL", ncols=3)
    r += 1

    rows_eq = [
        ("Beginning Partners' Capital (1/1/2025)", bb["partners_capital"],
         equity["beginning_partners_capital"], False),
        ("+ Capital Contributions 2025",   0.0,  equity["contributions_2025"],  True),
        ("- Distributions 2025",           0.0,  equity["distributions_2025"],  False),
        ("+ Net Income 2025",              0.0,  equity["net_income_2025"],      True),
        ("Ending Partners' Capital (pre-plug)", 0.0,
         equity["ending_partners_capital"], False),
    ]
    for label, v24, v25, is_input in rows_eq:
        bold_row = "Ending" in label or "Beginning" in label
        _label(ws1, r, 1, label, bold=bold_row, indent=1)
        _currency(ws1, r, 2, v24 if v24 != 0.0 else None)
        _currency(ws1, r, 3, v25, is_input=is_input)
        r += 1

    # Plug row
    plug = equity["unreconciled_difference_plug"]
    plug_label = "Unreconciled Difference (plug — resolve ACCOUNTANT INPUTs)"
    _label(ws1, r, 1, plug_label, indent=1, italic=True)
    ws1.cell(row=r, column=2, value=None)
    cell_plug = ws1.cell(row=r, column=3, value=plug)
    cell_plug.number_format = '"$"#,##0.00'
    cell_plug.alignment = Alignment(horizontal="right")
    cell_plug.font = Font(name="Calibri", italic=True, color="AA0000", size=9.5)
    r += 1

    _total_row(ws1, r, "TOTAL EQUITY",
               bb["partners_capital"],
               bs["total_equity"])
    r += 2

    # Tie-check row
    _total_row(ws1, r, "TOTAL LIABILITIES + EQUITY",
               bb["total_assets"],
               round(bs["total_liabilities"] + bs["total_equity"], 2))
    r += 2

    # Note
    note_cell = ws1.cell(row=r, column=1,
        value="* Cells highlighted yellow require accountant input before this balance sheet is final.")
    note_cell.font = Font(name="Calibri", size=8.5, italic=True, color="AA0000")
    ws1.merge_cells(start_row=r, start_column=1, end_row=r, end_column=3)

    # Column widths
    ws1.column_dimensions["A"].width = 48
    ws1.column_dimensions["B"].width = 22
    ws1.column_dimensions["C"].width = 22

    # ===================================================================
    # TAB 2 — Tax Reconciliation
    # ===================================================================
    ws2 = wb.create_sheet("Tax Reconciliation")

    ws2.merge_cells("A1:H1")
    ws2["A1"] = "Texas Comptroller Tax Reconciliation — 2025 (All three accounts)"
    ws2["A1"].font = Font(name="Calibri", bold=True, size=13)
    ws2.merge_cells("A2:H2")
    ws2["A2"] = (
        "Period codes: YYMM (monthly) or YYQ (quarterly).  "
        "Struck rows excluded from all totals.  "
        "MB Sales ÷ 0.0825 vs MB GR ÷ 0.067 — same alcohol base, "
        "divergence flagged in rightmost column."
    )
    ws2["A2"].font = Font(name="Calibri", size=8.5, italic=True, color="555555")

    def _tax_recon_section(ws, start_row: int, tax_label: str, rows: list,
                           rate: float) -> int:
        """Write one tax type's reconciliation block. Returns next available row."""
        r = start_row
        # Section header
        ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=8)
        cell = ws.cell(row=r, column=1,
                       value=f"{tax_label}  (rate: {rate*100:.3g}%)")
        cell.font = Font(name="Calibri", bold=True, color="FFFFFF", size=10)
        cell.fill = _fill(_HEADER_FILL)
        r += 1

        # Sub-header
        headers = [
            "Period Code", "Period Name", "Filed Date", "Type",
            "Filed Liability ($)", "Payments Made ($)",
            "Penalties & Interest ($)", "Implied Receipts ($)",
        ]
        for c, h in enumerate(headers, 1):
            cell = ws.cell(row=r, column=c, value=h)
            cell.font = _hdr_font(size=8.5)
            cell.fill = _fill("444444")
            cell.alignment = Alignment(horizontal="center", wrap_text=True, vertical="center")
        r += 1

        # Data rows — per period
        liabs = period_liabilities(rows)
        pays  = period_payments(rows)
        pi    = penalties_and_interest(rows)
        impl  = implied_receipts(liabs, rate)

        # Get unique periods in calendar order
        periods_seen = sorted({row[3] for row in rows if not row[6] and row[0] in ("return", "amended")
                                and _period_year(row[4]) == 2025})

        for i, period in enumerate(periods_seen):
            fill_alt = _fill(_ALT_FILL) if i % 2 else None
            pname = _period_label(period)

            # Find the latest relevant filed_date for this period
            relevant = [row[1] for row in rows
                        if row[3] == period and not row[6] and row[0] in ("return", "amended")]
            filed_date = max(relevant) if relevant else "—"
            txn_types = list({row[0] for row in rows
                              if row[3] == period and not row[6]
                              and row[0] in ("return", "amended")})
            txn_type_str = "/".join(txn_types)

            liab_val = liabs.get(period, 0.0)
            pay_val  = pays.get(period, 0.0)
            pi_val   = pi.get(period, 0.0)
            impl_val = impl.get(period, 0.0)

            row_data = [period, pname, filed_date, txn_type_str,
                        liab_val, pay_val, pi_val, impl_val]

            for c, val in enumerate(row_data, 1):
                cell = ws.cell(row=r, column=c, value=val)
                if c in (5, 6, 7, 8):
                    cell.number_format = '"$"#,##0.00'
                    cell.alignment = Alignment(horizontal="right")
                    # Highlight P&I > 0 in amber
                    if c == 7 and val > 0.01:
                        cell.fill = _fill("FFE8A0")
                else:
                    cell.alignment = Alignment(horizontal="left")
                cell.font = _body_font(size=9)
                if fill_alt:
                    cell.fill = fill_alt
            r += 1

        # Annual totals row
        annual_liab  = sum(liabs.values())
        annual_pay   = sum(pays.values())
        annual_pi    = sum(pi.values())
        annual_impl  = sum(impl.values())

        cell_l = ws.cell(row=r, column=1, value="ANNUAL TOTAL 2025")
        cell_l.font = Font(name="Calibri", bold=True, size=9.5)
        cell_l.border = _bottom_border()
        ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=4)
        for c, val in enumerate([annual_liab, annual_pay, annual_pi, annual_impl], 5):
            cell = ws.cell(row=r, column=c, value=round(val, 2))
            cell.number_format = '"$"#,##0.00'
            cell.alignment = Alignment(horizontal="right")
            cell.font = Font(name="Calibri", bold=True, size=9.5)
            cell.border = _bottom_border()

        return r + 2

    # Write all three tax types
    r = 4
    r = _tax_recon_section(ws2, r, "MB Gross Receipts Tax (6.7% — LOV3 expense)", MB_GR, MB_GR_RATE)
    r = _tax_recon_section(ws2, r, "MB Sales Tax (8.25% — collected from customers, pass-through liability)", MB_SALES, MB_SALES_RATE)
    r = _tax_recon_section(ws2, r, "Sales & Use Tax (8.25% — collected on non-MB taxable sales; S&U only eligible for 0.5% timely-filer discount)", SALES_USE, SU_RATE)

    # Cross-check section: MB Sales vs MB GR implied receipts base
    ws2.merge_cells(start_row=r, start_column=1, end_row=r, end_column=8)
    hdr = ws2.cell(row=r, column=1,
                   value="MB BASE CROSS-CHECK: MB Sales ÷ 0.0825  vs  MB GR ÷ 0.067  (same alcohol base — divergence is a flag)")
    hdr.font = Font(name="Calibri", bold=True, color="FFFFFF", size=10)
    hdr.fill = _fill(_GOLD_FILL)
    r += 1

    xhdr = ["Period Code", "Period Name",
            "MB Sales Implied Receipts ($)", "MB GR Implied Receipts ($)",
            "Divergence ($)", "Divergence (%)"]
    for c, h in enumerate(xhdr, 1):
        cell = ws2.cell(row=r, column=c, value=h)
        cell.font = _hdr_font(size=8.5)
        cell.fill = _fill("444444")
        cell.alignment = Alignment(horizontal="center", wrap_text=True, vertical="center")
    r += 1

    mb_sales_liabs = period_liabilities(MB_SALES)
    mb_gr_liabs    = period_liabilities(MB_GR)
    all_periods = sorted(set(list(mb_sales_liabs.keys()) + list(mb_gr_liabs.keys())))

    for i, period in enumerate(all_periods):
        impl_sales = round(mb_sales_liabs.get(period, 0.0) / MB_SALES_RATE, 2)
        impl_gr    = round(mb_gr_liabs.get(period, 0.0)    / MB_GR_RATE,    2)
        diverge    = round(impl_sales - impl_gr, 2)
        diverge_pct = round((diverge / impl_gr * 100) if impl_gr else 0.0, 1)

        fill_alt = _fill(_ALT_FILL) if i % 2 else None
        row_data = [period, _period_label(period), impl_sales, impl_gr, diverge, diverge_pct]
        for c, val in enumerate(row_data, 1):
            cell = ws2.cell(row=r, column=c, value=val)
            if c in (3, 4, 5):
                cell.number_format = '"$"#,##0.00'
                cell.alignment = Alignment(horizontal="right")
            elif c == 6:
                cell.number_format = '0.0"%"'
                cell.alignment = Alignment(horizontal="right")
                # Flag large divergences
                if abs(diverge_pct) > 5:
                    cell.fill = _fill(_RED_FILL)
            else:
                cell.alignment = Alignment(horizontal="left")
            cell.font = _body_font(size=9)
            if fill_alt and c != 6:
                cell.fill = fill_alt
        r += 1

    # Column widths for ws2
    for col, w in zip("ABCDEFGH", [12, 22, 14, 14, 14, 14, 14, 20]):
        ws2.column_dimensions[get_column_letter(ord(col) - 64)].width = w

    # ===================================================================
    # TAB 3 — Revenue Adjustments
    # ===================================================================
    ws3 = wb.create_sheet("Revenue Adjustments")

    ws3.merge_cells("A1:J1")
    ws3["A1"] = "Revenue Adjustments — Toast Book vs Filed Implied Receipts, by Month/Bucket"
    ws3["A1"].font = Font(name="Calibri", bold=True, size=13)
    ws3.merge_cells("A2:J2")
    ws3["A2"] = (
        "Implied receipts = Filed Liability ÷ Rate.  "
        "MB bucket: Liquor, Wine, Bottled Beer (TX § 183 — LOV3's MB tax is 6.7% expense; collected MB Sales 8.25% is pass-through).  "
        "NON_MB: Food, NA Bev, Hookah, Door, (none)/NULL (TX S&U 8.25%).  "
        "Toast excludes voided items ($176,422.30 annual); filed receipts cover net taxable sales."
    )
    ws3["A2"].font = Font(name="Calibri", size=8.5, italic=True, color="555555")

    r = 4
    rev_headers = [
        "Month", "Toast MB ($)", "Toast NON-MB ($)", "Toast Total ($)",
        "Filed MB GR Liab ($)", "MB GR Implied Receipts ($)",
        "Filed S&U Liab ($)",   "S&U Implied Receipts ($)",
        "Var MB (Book−Filed) ($)", "Var NON-MB (Book−Filed) ($)",
    ]
    for c, h in enumerate(rev_headers, 1):
        cell = ws3.cell(row=r, column=c, value=h)
        cell.font = _hdr_font(size=8.5)
        cell.fill = _fill(_HEADER_FILL)
        cell.alignment = Alignment(horizontal="center", wrap_text=True, vertical="center")
    r += 1

    var_rows = _monthly_variance_table()
    annual_toast_mb = annual_toast_non_mb = annual_toast_total = 0.0
    annual_impl_mb  = annual_impl_non_mb  = 0.0
    annual_filed_mb_gr = annual_filed_su  = 0.0
    annual_var_mb   = annual_var_non_mb   = 0.0

    for i, row in enumerate(var_rows):
        fill_alt = _fill(_ALT_FILL) if i % 2 else None
        vals = [
            row["label"],
            row["toast_mb"],
            row["toast_non_mb"],
            row["toast_total"],
            row["filed_mb_gr_liab"],
            row["implied_mb_gr_receipts"],
            row["filed_su_liab"],
            row["implied_su_receipts"],
            row["var_mb"],
            row["var_non_mb"],
        ]
        for c, val in enumerate(vals, 1):
            cell = ws3.cell(row=r, column=c, value=val)
            if c == 1:
                cell.alignment = Alignment(horizontal="left")
            else:
                cell.number_format = '"$"#,##0.00'
                cell.alignment = Alignment(horizontal="right")
                # Highlight large variances red
                if c in (9, 10) and isinstance(val, float) and abs(val) > 20000:
                    cell.fill = _fill(_RED_FILL)
                elif fill_alt:
                    cell.fill = fill_alt
            cell.font = _body_font(size=9)
        r += 1

        annual_toast_mb      += row["toast_mb"]
        annual_toast_non_mb  += row["toast_non_mb"]
        annual_toast_total   += row["toast_total"]
        annual_filed_mb_gr   += row["filed_mb_gr_liab"]
        annual_impl_mb       += row["implied_mb_gr_receipts"]
        annual_filed_su      += row["filed_su_liab"]
        annual_impl_non_mb   += row["implied_su_receipts"]
        annual_var_mb        += row["var_mb"]
        annual_var_non_mb    += row["var_non_mb"]

    # Annual totals
    annual_vals = [
        "ANNUAL 2025",
        round(annual_toast_mb, 2),
        round(annual_toast_non_mb, 2),
        round(annual_toast_total, 2),
        round(annual_filed_mb_gr, 2),
        round(annual_impl_mb, 2),
        round(annual_filed_su, 2),
        round(annual_impl_non_mb, 2),
        round(annual_var_mb, 2),
        round(annual_var_non_mb, 2),
    ]
    for c, val in enumerate(annual_vals, 1):
        cell = ws3.cell(row=r, column=c, value=val)
        if c == 1:
            cell.alignment = Alignment(horizontal="left")
        else:
            cell.number_format = '"$"#,##0.00'
            cell.alignment = Alignment(horizontal="right")
        cell.font = Font(name="Calibri", bold=True, size=9.5)
        cell.border = _bottom_border()
    r += 2

    # Variance drivers note block
    _section_hdr(ws3, r, "VARIANCE DRIVERS & RECOMMENDED ADJUSTMENTS", ncols=10, color=_GOLD_FILL)
    r += 1

    drivers = [
        ("1. Toast basis (net of voids)",
         "Toast BQ snapshot excludes voided items ($176,422.30 annual: MB $159,144 + NON-MB $17,278). "
         "Filed receipts are also net of voided sales. Voids are not a variance driver — basis match is correct."),
        ("2. Voided items — audit note",
         "Void total $176,422.30 = 4.0% of gross POS. Cross-check: prior gross $4,427,384 − voids = $4,250,962 (matches BQ net). "
         "No adjustment needed but the void rate should be reviewed annually for IRS/Comptroller consistency."),
        ("3. Service charge exclusion",
         "LOV3 levies a mandatory 20% service charge. Under IRS Rev. Rul. 2012-18 this is NOT gratuity — it is LOV3's revenue. "
         "Service charges ARE included in net_price (MB or NON-MB bucket based on the parent item). "
         "No adjustment needed; confirm with accountant that Comptroller receipts base also includes service charges."),
        ("4. S&U quarterly vs monthly timing",
         "S&U filed quarterly (Q1=period 251; Q2=period 252) then monthly from Jul 2025. "
         "This tab distributes Q1/Q2 S&U evenly across 3 months for comparability. "
         "No adjustment — this is a presentation choice for this variance analysis only."),
        ("5. MB step change (Flag 1)", MB_STEP_CHANGE_NARRATIVE),
        ("6. $59,175 legal order (RESOLVED)",
         "Debit 2025-08-25 reversed 2025-08-29, net = $0. "
         "RECOMMENDATION: if this was booked as OPEX in the 2025 P&L, reverse the entry. No balance sheet impact."),
        ("7. MB GR treatment",
         "MB Gross Receipts Tax (6.7%) is LOV3's own operating expense — book as Taxes & Licenses. "
         "Do NOT net from revenue. Collected MB Sales (8.25%) is a pass-through liability, NOT revenue."),
        ("8. Cash not rung / off-POS revenue",
         "Predictive Insights LLC monthly hookah deposits (May 2025+) are BankTransactions_raw entries, "
         "not POS net_price. These are additive revenue not captured in Toast MB or NON-MB buckets. "
         "Accountant: confirm Comptroller treatment of hookah revenue (MB? NON-MB? Exempt?)."),
        ("9. Timing — 4 AM CT business-day cutoff",
         "Toast records revenue using a 4 AM CT cutoff (1 AM Saturday = Friday business). "
         "Comptroller periods end on the calendar last day of the month. "
         "Near month-end sessions may shift across periods. Variances <1% of monthly revenue are immaterial."),
    ]
    for title, detail in drivers:
        cell_t = ws3.cell(row=r, column=1, value=title)
        cell_t.font = Font(name="Calibri", bold=True, size=9.5)
        ws3.merge_cells(start_row=r, start_column=2, end_row=r, end_column=10)
        cell_d = ws3.cell(row=r, column=2, value=detail)
        cell_d.font = _body_font(size=9)
        cell_d.alignment = Alignment(wrap_text=True, vertical="top")
        ws3.row_dimensions[r].height = 28
        r += 1

    # Column widths ws3
    ws3.column_dimensions["A"].width = 18
    for col_letter in "BCDEFGHIJ":
        ws3.column_dimensions[col_letter].width = 18
    ws3.column_dimensions["B"].width = 60  # detail notes

    # ===================================================================
    # TAB 4 — Assumptions & Flags
    # ===================================================================
    ws4 = wb.create_sheet("Assumptions & Flags")

    ws4.merge_cells("A1:C1")
    ws4["A1"] = "Assumptions & Flags — LOV3 HTX 2025 Adjusted Balance Sheet"
    ws4["A1"].font = Font(name="Calibri", bold=True, size=14)
    ws4.merge_cells("A2:C2")
    ws4["A2"] = (
        "All items marked ACCOUNTANT INPUT must be resolved before this sheet is presented as final. "
        "Items marked NOTE are informational; items marked WARNING require immediate attention."
    )
    ws4["A2"].font = Font(name="Calibri", size=9, italic=True, color="AA0000")

    r = 4
    _col_hdr(ws4, r, 1, "#")
    _col_hdr(ws4, r, 2, "Category")
    _col_hdr(ws4, r, 3, "Flag / Assumption Text")
    r += 1

    ws4.freeze_panes = "A5"

    # The six spec-required flags first, then the flags from build_balance_sheet()
    spec_flags = [
        ("SPEC FLAG 1",
         "MB GR Step Change — Probable Under-Filing 10 of 12 Months",
         MB_STEP_CHANGE_NARRATIVE),
        ("SPEC FLAG 2",
         "$59,175 Classification — RESOLVED",
         "TX Comptroller legal hold debit of $59,175 on 2025-08-25 was FULLY REVERSED on 2025-08-29. "
         "Net = $0. No balance sheet asset, liability, or OPEX. "
         "If this was booked as an expense in the 2025 P&L, that entry must be reversed before filing."),
        ("SPEC FLAG 3",
         "2024 Liability Presentation Change",
         f"2024 Schedule L reported $0 total liabilities. "
         f"This 2025 balance sheet introduces ${bs['liabilities']['taxes_payable']:,.2f} taxes payable "
         f"(accrual-basis recognition for periods 2511/2512 paid Jan/Feb 2026). "
         "This is not an error — it is an accounting policy change. "
         "Accountant: document the change in the return notes / footnote."),
        ("SPEC FLAG 4",
         "2024 K-1 Allocation Consistency",
         "2024 K-1 shows Maurice Ragland at 15% ownership but allocated 100% of the $(35,161) loss. "
         "For 2025 K-1s: confirm ownership percentages for all three partners "
         "(Maurice 15% confirmed; Eddiele B Jasper and Derwin James unknown) "
         "and apply consistent allocation methodology per the LLC operating agreement."),
        ("SPEC FLAG 5",
         "Sales Tax Variance — Restate with Complete Data",
         "Prior analysis cited a $3,249 sales tax variance. "
         "This workbook uses the complete 2025 WebFile data (all three accounts, all periods). "
         "Restate the variance using the Revenue Adjustments tab (Tab 3) annual totals, "
         "which show annualized MB GR implied receipts vs Toast MB POS and "
         "S&U implied receipts vs Toast NON-MB POS."),
        ("SPEC FLAG 6",
         "Accounts Receivable — $373,592",
         "The 2024 Schedule L ending A/R balance of $373,592 is unusually large for a "
         "cash-and-card restaurant. "
         "Possible explanations: Toast settlement receivable, related-party advances, or prior-year "
         "bookkeeping artifact. Accountant: confirm 2025 treatment (write-off, reclassify, or carry). "
         "This figure is carried forward as the 2025 A/R placeholder in this workbook."),
    ]

    all_flags_for_tab = spec_flags[:]
    # Add build_balance_sheet flags
    for i, flag_text in enumerate(bs["flags"], 1):
        # Categorise
        if flag_text.startswith("ACCOUNTANT INPUT"):
            cat = "ACCOUNTANT INPUT"
        elif flag_text.startswith("WARNING"):
            cat = "WARNING"
        elif flag_text.startswith("NOTE"):
            cat = "NOTE"
        else:
            cat = "NOTE"
        all_flags_for_tab.append((f"BS FLAG {i}", cat, flag_text))

    category_colors = {
        "ACCOUNTANT INPUT": "FFF3CD",  # amber
        "WARNING":          "F8D7DA",  # red
        "NOTE":             "D1ECF1",  # blue
        "SPEC FLAG 1":      "E8F5E9",
        "SPEC FLAG 2":      "E8F5E9",
        "SPEC FLAG 3":      "E8F5E9",
        "SPEC FLAG 4":      "E8F5E9",
        "SPEC FLAG 5":      "E8F5E9",
        "SPEC FLAG 6":      "E8F5E9",
    }

    for i, flag_tuple in enumerate(all_flags_for_tab):
        num_label, cat_label, text = flag_tuple
        fill_hex = category_colors.get(num_label, category_colors.get(cat_label, "FFFFFF"))

        cell_n = ws4.cell(row=r, column=1, value=num_label)
        cell_n.font = Font(name="Calibri", bold=True, size=9)
        cell_n.fill = _fill(fill_hex)
        cell_n.alignment = Alignment(vertical="top")

        cell_c = ws4.cell(row=r, column=2, value=cat_label)
        cell_c.font = Font(name="Calibri", bold=True, size=9)
        cell_c.fill = _fill(fill_hex)
        cell_c.alignment = Alignment(vertical="top")

        cell_t = ws4.cell(row=r, column=3, value=text)
        cell_t.font = _body_font(size=9)
        cell_t.alignment = Alignment(wrap_text=True, vertical="top")
        cell_t.fill = _fill(fill_hex)

        ws4.row_dimensions[r].height = max(40, min(120, len(text) // 4))
        r += 1

    ws4.column_dimensions["A"].width = 18
    ws4.column_dimensions["B"].width = 26
    ws4.column_dimensions["C"].width = 90

    wb.save(XLSX_PATH)
    return XLSX_PATH


# ---------------------------------------------------------------------------
# PDF writer — reportlab (lazy import)
# ---------------------------------------------------------------------------

PDF_PATH = "LOV3_2025_Adjusted_Balance_Sheet_Leadership_Report.pdf"


def write_pdf(bs: dict) -> str:
    """Write the leadership-audience PDF.  reportlab is imported lazily.

    Returns the output path.
    """
    try:
        from reportlab.lib import colors as rl_colors
        from reportlab.lib.pagesizes import LETTER
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import inch
        from reportlab.platypus import (
            BaseDocTemplate, Frame, KeepTogether, PageBreak, PageTemplate,
            Paragraph, Spacer, Table, TableStyle,
        )
    except ImportError:
        import sys
        print("reportlab not installed. Run: pip install reportlab", file=sys.stderr)
        sys.exit(1)
    from datetime import datetime
    import zoneinfo

    # ── Design tokens (matches comp_report_pdf.py)
    BLACK    = rl_colors.HexColor("#111111")
    INK      = rl_colors.HexColor("#1a1a1a")
    GOLD     = rl_colors.HexColor("#B8956A")
    BONE     = rl_colors.HexColor("#6a6a6a")
    CREAM    = rl_colors.HexColor("#f9f6f0")
    RULE     = rl_colors.HexColor("#e5e5e5")
    AMBER    = rl_colors.HexColor("#D4A24C")
    RED      = rl_colors.HexColor("#C97064")
    BLUE     = rl_colors.HexColor("#5A8CBF")

    _ss = getSampleStyleSheet()

    STYLE_H1 = ParagraphStyle(
        "h1", parent=_ss["Heading1"], fontSize=22, leading=28,
        textColor=BLACK, spaceAfter=8, fontName="Helvetica-Bold",
    )
    STYLE_H2 = ParagraphStyle(
        "h2", parent=_ss["Heading2"], fontSize=13, leading=16,
        textColor=BLACK, spaceBefore=14, spaceAfter=6, fontName="Helvetica-Bold",
    )
    STYLE_EYEBROW = ParagraphStyle(
        "eyebrow", parent=_ss["BodyText"], fontSize=8, leading=10,
        textColor=GOLD, fontName="Helvetica-Bold", spaceAfter=2,
    )
    STYLE_BODY = ParagraphStyle(
        "body", parent=_ss["BodyText"], fontSize=9.5, leading=13,
        textColor=INK, spaceAfter=5,
    )
    STYLE_SMALL = ParagraphStyle(
        "small", parent=_ss["BodyText"], fontSize=8, leading=10, textColor=BONE,
    )
    STYLE_LEAD = ParagraphStyle(
        "lead", parent=_ss["BodyText"], fontSize=10.5, leading=14,
        textColor=INK, spaceAfter=8, fontName="Helvetica",
    )
    STYLE_NOTE = ParagraphStyle(
        "note", parent=_ss["BodyText"], fontSize=8.5, leading=11,
        textColor=BONE, spaceAfter=4, leftIndent=12,
    )

    TABLE_HDR = TableStyle([
        ("FONTNAME",    (0, 0), (-1,  0), "Helvetica-Bold"),
        ("FONTSIZE",    (0, 0), (-1, -1), 8.5),
        ("BACKGROUND",  (0, 0), (-1,  0), BLACK),
        ("TEXTCOLOR",   (0, 0), (-1,  0), rl_colors.white),
        ("ALIGN",       (1, 0), (-1, -1), "RIGHT"),
        ("ALIGN",       (0, 0), (0,  -1), "LEFT"),
        ("VALIGN",      (0, 0), (-1, -1), "MIDDLE"),
        ("BOTTOMPADDING", (0, 0), (-1,  0), 5),
        ("TOPPADDING",    (0, 0), (-1,  0), 5),
        ("BOTTOMPADDING", (0, 1), (-1, -1), 3),
        ("TOPPADDING",    (0, 1), (-1, -1), 3),
        ("LINEBELOW",   (0, 0), (-1, 0), 1, BLACK),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [rl_colors.white, CREAM]),
    ])

    def _money(v):
        if v is None:
            return "—"
        sign = "(" if v < 0 else ""
        end  = ")" if v < 0 else ""
        return f"{sign}${abs(v):,.0f}{end}"

    def _pf(v):
        return f"${v:,.2f}"

    # ── Document setup
    doc = BaseDocTemplate(
        PDF_PATH,
        pagesize=LETTER,
        rightMargin=0.75 * inch,
        leftMargin=0.75 * inch,
        topMargin=0.75 * inch,
        bottomMargin=0.75 * inch,
    )

    page_w, page_h = LETTER
    body_w = page_w - 1.5 * inch

    def _page_template(canvas_obj, doc_obj):
        canvas_obj.saveState()
        # Footer
        canvas_obj.setFont("Helvetica", 7)
        canvas_obj.setFillColor(BONE)
        canvas_obj.drawString(
            0.75 * inch, 0.45 * inch,
            "LOV3 Restaurant & Lounge LLC — 2025 Adjusted Balance Sheet | CONFIDENTIAL — BAED Corporation / Sanchez Burnside",
        )
        canvas_obj.drawRightString(
            page_w - 0.75 * inch, 0.45 * inch,
            f"Page {doc_obj.page}",
        )
        # Top rule
        canvas_obj.setStrokeColor(GOLD)
        canvas_obj.setLineWidth(1.5)
        canvas_obj.line(0.75 * inch, page_h - 0.55 * inch,
                        page_w - 0.75 * inch, page_h - 0.55 * inch)
        canvas_obj.restoreState()

    frame = Frame(
        0.75 * inch, 0.75 * inch,
        body_w, page_h - 1.5 * inch,
        id="normal",
    )
    doc.addPageTemplates([PageTemplate(id="main", frames=[frame], onPage=_page_template)])

    story = []

    # ── Helper to format a two-column table
    def _two_col_table(left_header, right_header, rows, col_widths=None):
        if col_widths is None:
            col_widths = [body_w * 0.55, body_w * 0.45]
        data = [[left_header, right_header]] + rows
        tbl = Table(data, colWidths=col_widths)
        tbl.setStyle(TABLE_HDR)
        return tbl

    now_ct = datetime.now(tz=zoneinfo.ZoneInfo("America/Chicago"))
    ts = now_ct.strftime("%B %d, %Y  %I:%M %p CT")

    # ==================================================================
    # PAGE 1 — Cover
    # ==================================================================
    story += [
        Spacer(1, 0.5 * inch),
        Paragraph("LOV3 Restaurant & Lounge LLC", STYLE_EYEBROW),
        Paragraph("2025 Adjusted Balance Sheet", STYLE_H1),
        Paragraph("K-1 Preparation Package — Sanchez Burnside / BAED Corporation", STYLE_H2),
        Spacer(1, 0.15 * inch),
        Paragraph(
            f"<b>As of:</b> December 31, 2025  &nbsp;|&nbsp; "
            f"<b>Basis:</b> Accrual  &nbsp;|&nbsp;  "
            f"<b>Generated:</b> {ts}",
            STYLE_BODY,
        ),
        Spacer(1, 0.15 * inch),
        Paragraph(
            "<b>IMPORTANT:</b> This document contains estimates and placeholders. "
            "Cells and items labeled <b>ACCOUNTANT INPUT</b> have not been confirmed and "
            "must be resolved before this balance sheet is presented as final or filed with any return. "
            "Refinements and uncertainties are presented explicitly in Section 5 (Open Items).",
            ParagraphStyle("warn", parent=STYLE_BODY, textColor=RED, spaceAfter=10),
        ),
        Spacer(1, 0.15 * inch),
    ]

    # Cover summary KPI table
    kpi_rows = [
        ["Total Assets (with placeholders)",     _money(bs["total_assets"])],
        ["Total Liabilities",                    _money(bs["total_liabilities"])],
        ["  Taxes Payable (TX Comptroller)",     _money(bs["liabilities"]["taxes_payable"])],
        ["Total Partners' Capital",              _money(bs["total_equity"])],
        ["Unreconciled Difference (plug)",       _money(bs["unreconciled_difference"])],
        ["Beginning Partners' Capital (1/1/25)", _money(BEGINNING_BALANCES["partners_capital"])],
        ["Distributions 2025 (BQ bank data)",   _money(-abs(BQ_RESULTS["owner_distributions_2025"]["combined_owner_attributable"]))],
    ]
    tbl = _two_col_table("Balance Sheet Line", "Amount", kpi_rows,
                         col_widths=[body_w * 0.65, body_w * 0.35])
    story.append(tbl)
    story.append(Spacer(1, 0.15 * inch))
    story.append(Paragraph(
        f"The unreconciled difference ({_money(bs['unreconciled_difference'])}) is a plug "
        "that forces the sheet to balance. "
        "It will collapse to $0 when cash, net income 2025, depreciation, AP, and accrued payroll "
        "are supplied by the accountant.",
        STYLE_NOTE,
    ))
    story.append(PageBreak())

    # ==================================================================
    # PAGE 2 — Revenue Variance Summary
    # ==================================================================
    story += [
        Paragraph("SECTION 1", STYLE_EYEBROW),
        Paragraph("Revenue: Filed vs Book — Headline Variance", STYLE_H1),
        Spacer(1, 0.1 * inch),
        Paragraph(
            "Toast POS net_price (net of voids) vs Texas Comptroller implied receipts (filed liability ÷ rate). "
            "Two buckets: <b>MB</b> (Mixed Beverage — Liquor, Wine, Bottled Beer) and "
            "<b>NON-MB</b> (Food, NA Beverage, Hookah POS, Door, uncategorized). "
            "Numbers are business-day basis (4 AM CT cutoff). "
            "The Comptroller uses calendar-month periods; small timing differences are expected and immaterial.",
            STYLE_LEAD,
        ),
    ]

    var_rows = _monthly_variance_table()
    annual_toast_mb  = sum(r["toast_mb"]    for r in var_rows)
    annual_toast_non = sum(r["toast_non_mb"] for r in var_rows)
    annual_impl_mb   = sum(r["implied_mb_gr_receipts"]  for r in var_rows)
    annual_impl_non  = sum(r["implied_su_receipts"]      for r in var_rows)
    annual_var_mb    = round(annual_toast_mb  - annual_impl_mb,  2)
    annual_var_non   = round(annual_toast_non - annual_impl_non, 2)

    story.append(Paragraph("Annual Totals", STYLE_H2))
    annual_summary = [
        ["Bucket", "Toast Book ($)", "Filed Implied Receipts ($)", "Variance (Book − Filed) ($)"],
        ["MB (alcohol)", _pf(annual_toast_mb),  _pf(annual_impl_mb),  _pf(annual_var_mb)],
        ["NON-MB",       _pf(annual_toast_non), _pf(annual_impl_non), _pf(annual_var_non)],
        ["TOTAL",
         _pf(annual_toast_mb + annual_toast_non),
         _pf(annual_impl_mb + annual_impl_non),
         _pf(annual_var_mb + annual_var_non)],
    ]
    tbl_annual = Table(annual_summary,
                       colWidths=[body_w * 0.28, body_w * 0.24, body_w * 0.24, body_w * 0.24])
    tbl_annual.setStyle(TABLE_HDR)
    story.append(tbl_annual)
    story.append(Spacer(1, 0.12 * inch))

    story.append(Paragraph("Monthly Detail", STYLE_H2))
    monthly_hdr = ["Month", "Toast MB ($)", "MB Implied ($)", "Var MB ($)",
                   "Toast NON-MB ($)", "NON-MB Implied ($)", "Var NON-MB ($)"]
    monthly_data = [monthly_hdr] + [
        [r["label"],
         _pf(r["toast_mb"]), _pf(r["implied_mb_gr_receipts"]), _pf(r["var_mb"]),
         _pf(r["toast_non_mb"]), _pf(r["implied_su_receipts"]), _pf(r["var_non_mb"])]
        for r in var_rows
    ]
    col_w = body_w / 7
    tbl_monthly = Table(monthly_data, colWidths=[col_w] * 7)
    tbl_monthly.setStyle(TABLE_HDR)
    story.append(tbl_monthly)
    story.append(Spacer(1, 0.1 * inch))

    story.append(Paragraph(
        "<b>Key drivers of variance (where quantified):</b>",
        STYLE_BODY,
    ))
    drivers_text = [
        ("Voided items",
         "$176,422.30 in 2025 voided POS items (MB $159,144 + NON-MB $17,278). "
         "Both the Toast BQ snapshot and the Comptroller receipts base exclude voids — no net variance from this."),
        ("Quarterly S&U distribution",
         "S&U was filed quarterly for H1 2025 (Q1 = period 251, Q2 = period 252). "
         "Monthly variances in Jan–Jun reflect equal distribution of the quarterly filing across 3 months. "
         "The annual total is precise."),
        ("MB step change — probable under-filing", MB_STEP_CHANGE_NARRATIVE),
        ("Service charges",
         "Mandatory 20% service charge is LOV3 revenue (IRS Rev. Rul. 2012-18, not gratuity). "
         "Included in net_price and likely included in Comptroller receipts. No adjustment needed."),
        ("Hookah (Predictive Insights)",
         "Monthly bank deposits from Predictive Insights LLC (hookah operator, May 2025+) "
         "do not appear in Toast net_price. Accountant must confirm Comptroller treatment."),
        ("Timing",
         "4 AM CT business-day cutoff shifts late-night sessions. Near-month-end variances < 1% of "
         "monthly revenue are immaterial."),
    ]
    for title, detail in drivers_text:
        story.append(Paragraph(f"<b>{title}:</b> {detail}", STYLE_BODY))

    story.append(PageBreak())

    # ==================================================================
    # PAGE 3 — Three Taxes Explained
    # ==================================================================
    story += [
        Paragraph("SECTION 2", STYLE_EYEBROW),
        Paragraph("The Three Texas Comptroller Tax Accounts", STYLE_H1),
        Spacer(1, 0.1 * inch),
    ]

    tax_explanations = [
        (
            "1. Mixed Beverage Gross Receipts Tax (MB GR) — 6.7%",
            f"LOV3's own operating expense on alcohol sales. The Comptroller charges LOV3 6.7% of "
            f"gross MB receipts (Liquor, Wine, Bottled Beer). This is a cost-of-doing-business tax, "
            f"like a franchise fee on alcohol revenue. It is NOT collected from customers. "
            f"Book as Taxes & Licenses in OPEX — never net from revenue. "
            f"2025 annual liability: {_pf(sum(period_liabilities(MB_GR).values()))}.",
        ),
        (
            "2. Mixed Beverage Sales Tax (MB Sales) — 8.25%",
            f"Collected from customers on every MB sale and remitted to the Comptroller. "
            f"This is a pass-through liability — it never touches P&L as revenue or expense. "
            f"At any month-end, amounts collected but not yet remitted sit on the balance sheet "
            f"as a current liability (Sales Tax Payable). "
            f"2025 annual liability: {_pf(sum(period_liabilities(MB_SALES).values()))}.",
        ),
        (
            "3. Sales & Use Tax (S&U) — 8.25%",
            f"Collected from customers on non-MB taxable sales (Food, NA Beverage, Door cover charges, etc.). "
            f"Same pass-through structure as MB Sales Tax. "
            f"Houston combined state + local rate = 8.25%. Filed quarterly for H1 2025 "
            f"(period 251 = Q1 Jan–Mar; period 252 = Q2 Apr–Jun), then monthly from Jul 2025. "
            f"Texas Tax Code § 151.423 grants LOV3 a 0.5% timely-filing discount when the return "
            f"and payment are filed on time — this discount does NOT apply to MB taxes. "
            f"2025 annual liability: {_pf(sum(period_liabilities(SALES_USE, year=2025).values()))}.",
        ),
    ]
    for title, body in tax_explanations:
        story.append(Paragraph(title, STYLE_H2))
        story.append(Paragraph(body, STYLE_BODY))

    # Year-end taxes payable breakdown
    tp_mbgr  = taxes_payable_at_yearend(MB_GR,    discount_eligible=False)
    tp_mbs   = taxes_payable_at_yearend(MB_SALES,  discount_eligible=False)
    tp_su    = taxes_payable_at_yearend(SALES_USE, discount_eligible=True)

    story.append(Paragraph("Year-End Taxes Payable (12/31/2025)", STYLE_H2))
    story.append(Paragraph(
        "These are tax liabilities that accrued in 2025 (periods 2511 = Nov 2025 and "
        "2512 = Dec 2025) but whose payments were filed in January/February 2026. "
        "Under accrual accounting, they are a current liability at 12/31/2025.",
        STYLE_BODY,
    ))
    tp_table = [
        ["Tax Account",         "Periods Outstanding", "Amount Payable"],
        ["MB Gross Receipts",   "Nov 2025 (2511), Dec 2025 (2512)", _pf(tp_mbgr)],
        ["MB Sales Tax",        "Nov 2025 (2511), Dec 2025 (2512)", _pf(tp_mbs)],
        ["Sales & Use Tax",     "Nov 2025 (2511), Dec 2025 (2512)", _pf(tp_su)],
        ["TOTAL TAXES PAYABLE", "",                                 _pf(tp_mbgr + tp_mbs + tp_su)],
    ]
    tbl_tp = Table(tp_table, colWidths=[body_w * 0.38, body_w * 0.38, body_w * 0.24])
    tbl_tp.setStyle(TABLE_HDR)
    story.append(tbl_tp)
    story.append(Spacer(1, 0.1 * inch))
    story.append(Paragraph(
        f"Note: S&U period 2510 (Oct 2025) recognized a $29.50 timely-filer discount "
        f"(0.5% of {_pf(period_liabilities(SALES_USE).get('2510', 0.0))} liability per Tax Code § 151.423). "
        f"This $29.50 shortfall is settled — not counted in taxes payable.",
        STYLE_NOTE,
    ))

    story.append(PageBreak())

    # ==================================================================
    # PAGE 4 — Adjustments Summary
    # ==================================================================
    story += [
        Paragraph("SECTION 3", STYLE_EYEBROW),
        Paragraph("Adjustments Summary", STYLE_H1),
        Spacer(1, 0.1 * inch),
        Paragraph(
            "Recommended accounting entries to bring the 2025 books in line with this analysis. "
            "These are advisory — the CPA of record (Sanchez Burnside / BAED Corporation) should "
            "review and approve before any entries are posted.",
            STYLE_LEAD,
        ),
    ]

    adj_rows = [
        ["#", "Debit Account", "Credit Account", "Approximate Amount", "Description"],
        ["1", "Taxes & Licenses (OPEX)",  "MB Gross Receipts Tax Payable",
         _pf(sum(period_liabilities(MB_GR).values())),
         "Accrue 2025 MB GR expense if not already in P&L"],
        ["2", "MB Sales Tax Payable",      "Cash / Bank",
         "As remitted",
         "Reclassify MB Sales Tax from revenue to liability (pass-through)"],
        ["3", "Sales & Use Tax Payable",   "Cash / Bank",
         "As remitted",
         "Reclassify S&U Tax from revenue to liability (pass-through)"],
        ["4", "Prior OPEX / Legal Expense", "Cash / Reversal",
         _pf(59175.0),
         "Reverse $59,175 legal hold expense if booked (hold was reversed 2025-08-29, net = $0)"],
        ["5", "Cash / Receivable",          "Hookah Revenue",
         "TBD",
         "Record Predictive Insights LLC monthly hookah deposits as additive revenue (not in Toast POS)"],
        ["6", "Owner Distributions",        "Cash / Partners' Capital",
         _pf(abs(BQ_RESULTS["owner_distributions_2025"]["combined_owner_attributable"])),
         f"Confirm BQ bank-category total ({_pf(abs(BQ_RESULTS['owner_distributions_2025']['combined_owner_attributable']))}) reconciles to K-1 distributions before filing"],
    ]
    tbl_adj = Table(adj_rows,
                    colWidths=[body_w * 0.04, body_w * 0.20, body_w * 0.20,
                               body_w * 0.16, body_w * 0.40])
    tbl_adj.setStyle(TABLE_HDR)
    story.append(tbl_adj)
    story.append(PageBreak())

    # ==================================================================
    # PAGE 5 — Open Items for BAED (ACCOUNTANT INPUT list)
    # ==================================================================
    story += [
        Paragraph("SECTION 4", STYLE_EYEBROW),
        Paragraph("Open Items for BAED Corporation / Sanchez Burnside", STYLE_H1),
        Spacer(1, 0.1 * inch),
        Paragraph(
            "The following items are placeholders in this balance sheet. "
            "All must be resolved before the 2025 balance sheet is final and the 1065 / K-1s are filed.",
            STYLE_LEAD,
        ),
    ]

    open_items = [
        ["Item", "Placeholder Used", "Data Source / Action Required"],
        ["Cash at 12/31/2025",
         "$0",
         "BQ shows -$473.97 (intraday low after $71K payroll wire). "
         "Use Dec-2025 BofA statement closing balance. Request from BofA online portal."],
        ["2025 Depreciation",
         "$0 (no add'l dep.)",
         "Fixed-asset schedule required. 2024 accum. dep. = $9,278 carried forward."],
        ["Net Income 2025",
         "$0",
         "Reference LOV3_HTX_Financial_Statements_SBA.xlsx (adjusted P&L). "
         "Insert 2025 accrual-basis book income before filing K-1s."],
        ["Accounts Payable",
         "$0",
         "No AP sub-ledger available. Confirm outstanding vendor invoices at 12/31/2025."],
        ["Accrued Payroll",
         "$0",
         "No payroll accrual data. Confirm pay-period cutoff vs 12/31/2025."],
        ["Capital Contributions 2025",
         "$0",
         "No contribution records. Confirm with bank records and partner agreements."],
        ["Accounts Receivable 2025",
         "$373,592 (2024 balance)",
         "Large A/R for a card restaurant. Confirm treatment: collect, write off, or reclassify."],
        ["Inventory 2025",
         "$32,191 (2024 balance)",
         "Physical count at 12/31/2025 required."],
        ["Partner Ownership %",
         "Maurice 15% (confirmed)",
         "Eddiele B Jasper and Derwin James % unknown. "
         "Confirm from LLC operating agreement before K-1 income allocation."],
        ["Owner Distributions — reconciliation",
         "$225,496 (BQ bank)",
         "Prior SBA P&L showed $231,907 (delta ~$6,411). "
         "Reconcile to K-1 distribution amount; check Credit Card Payments / Admin & Office buckets."],
    ]
    tbl_open = Table(open_items,
                     colWidths=[body_w * 0.25, body_w * 0.20, body_w * 0.55])
    tbl_open.setStyle(TABLE_HDR)
    story.append(tbl_open)

    story.append(PageBreak())

    # ==================================================================
    # PAGE 6 — Flags (from build_balance_sheet)
    # ==================================================================
    story += [
        Paragraph("SECTION 5", STYLE_EYEBROW),
        Paragraph("Balance Sheet Flags & Assumptions", STYLE_H1),
        Spacer(1, 0.1 * inch),
        Paragraph(
            "These flags are generated directly from the balance sheet computation. "
            "Items marked ACCOUNTANT INPUT are unresolved placeholders. "
            "Items marked NOTE are informational audit trails.",
            STYLE_LEAD,
        ),
    ]

    for i, flag_text in enumerate(bs["flags"], 1):
        if flag_text.startswith("ACCOUNTANT INPUT"):
            color = AMBER
            prefix = "ACCOUNTANT INPUT"
        elif flag_text.startswith("WARNING"):
            color = RED
            prefix = "WARNING"
        else:
            color = BLUE
            prefix = "NOTE"

        story.append(Paragraph(
            f'<font color="{color.hexval()}">[{i}] <b>{prefix}:</b></font> {flag_text}',
            STYLE_BODY,
        ))

    story.append(Spacer(1, 0.3 * inch))
    story.append(Paragraph(
        f"This report was generated from adjusted_balance_sheet_2025.py on {ts}. "
        "Source data: Texas Comptroller WebFile PDFs (transcribed 2026-10-10), "
        "BigQuery toast_raw dataset (net of voids, business-day basis), "
        "and 2024 Schedule L (Form 1065).",
        STYLE_SMALL,
    ))

    doc.build(story)
    return PDF_PATH


# ---------------------------------------------------------------------------
# main() — build balance sheet, write both files, print summary
# ---------------------------------------------------------------------------

def main() -> None:
    """Build balance sheet + write Excel workbook and leadership PDF."""
    import os

    print("=" * 68)
    print("LOV3 HTX — 2025 Adjusted Balance Sheet (K-1 Prep)")
    print("=" * 68)

    bs = build_balance_sheet()

    # ── Key totals
    print(f"\nKey Totals (accrual basis, 12/31/2025)")
    print(f"  Total Assets              : {bs['total_assets']:>14,.2f}")
    print(f"  Total Liabilities         : {bs['total_liabilities']:>14,.2f}")
    print(f"    Taxes Payable (TX)      : {bs['liabilities']['taxes_payable']:>14,.2f}")
    print(f"  Total Equity              : {bs['total_equity']:>14,.2f}")
    print(f"  Unreconciled Difference   : {bs['unreconciled_difference']:>14,.2f}  ← resolve ACCOUNTANT INPUTs")
    print(f"  Balance check (A = L+E)   : "
          f"{'OK' if abs(bs['total_assets'] - bs['total_liabilities'] - bs['total_equity']) < 0.01 else 'FAIL'}")
    print(f"  Flags                     : {len(bs['flags'])} items")

    # ── Tax payable breakdown
    tp_mbgr = taxes_payable_at_yearend(MB_GR,    discount_eligible=False)
    tp_mbs  = taxes_payable_at_yearend(MB_SALES,  discount_eligible=False)
    tp_su   = taxes_payable_at_yearend(SALES_USE, discount_eligible=True)
    print(f"\nTaxes Payable at 12/31/2025")
    print(f"  MB Gross Receipts (6.7%)  : {tp_mbgr:>14,.2f}")
    print(f"  MB Sales Tax (8.25%)      : {tp_mbs:>14,.2f}")
    print(f"  Sales & Use Tax (8.25%)   : {tp_su:>14,.2f}")
    print(f"  TOTAL                     : {tp_mbgr + tp_mbs + tp_su:>14,.2f}")

    # ── Annual variance quick-check
    var_rows = _monthly_variance_table()
    annual_toast_mb  = round(sum(r["toast_mb"]    for r in var_rows), 2)
    annual_toast_non = round(sum(r["toast_non_mb"] for r in var_rows), 2)
    annual_impl_mb   = round(sum(r["implied_mb_gr_receipts"]  for r in var_rows), 2)
    annual_impl_non  = round(sum(r["implied_su_receipts"]      for r in var_rows), 2)
    annual_var_mb    = round(annual_toast_mb  - annual_impl_mb,  2)
    annual_var_non   = round(annual_toast_non - annual_impl_non, 2)
    print(f"\nRevenue Variance (Toast Book vs Filed Implied Receipts)")
    print(f"  MB  — Toast: {annual_toast_mb:>14,.2f}  |  Filed Implied: {annual_impl_mb:>14,.2f}  |  Var: {annual_var_mb:>+14,.2f}")
    print(f"  NON — Toast: {annual_toast_non:>14,.2f}  |  Filed Implied: {annual_impl_non:>14,.2f}  |  Var: {annual_var_non:>+14,.2f}")

    # ── Write Excel
    print(f"\nWriting Excel workbook ...")
    xlsx_path = write_excel(bs)
    xlsx_size = os.path.getsize(xlsx_path)
    print(f"  {xlsx_path}  ({xlsx_size:,} bytes)")

    # ── Write PDF
    print(f"\nWriting leadership PDF ...")
    pdf_path = write_pdf(bs)
    pdf_size = os.path.getsize(pdf_path)
    print(f"  {pdf_path}  ({pdf_size:,} bytes)")

    print(f"\nDone. Open files with:")
    print(f"  open \"{xlsx_path}\"")
    print(f"  open \"{pdf_path}\"")
    print("=" * 68)


if __name__ == "__main__":
    main()

