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
