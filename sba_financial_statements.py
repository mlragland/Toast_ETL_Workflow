"""
SBA Loan Financial Statement Generator — LOV3|HTX

Generates a professional Excel workbook with:
  Sheet 1: 2025 Year-End P&L (Jan–Dec monthly + YTD)
  Sheet 2: Feb 2026 Interim P&L (Jan–Feb monthly + YTD)

Usage:
  pip install openpyxl google-cloud-bigquery
  python sba_financial_statements.py
"""

import calendar
import logging
from datetime import date
from typing import Dict, List, Tuple

from google.cloud import bigquery
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from config import (
    DATASET_ID,
    GRAT_PASSTHROUGH_PCT,
    GRAT_RETAIN_PCT,
    PROJECT_ID,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

# ── Styles ───────────────────────────────────────────────────────────────────
TITLE_FONT = Font(name="Calibri", size=14, bold=True)
SUBTITLE_FONT = Font(name="Calibri", size=11, bold=False, italic=True)
SECTION_FONT = Font(name="Calibri", size=11, bold=True)
SECTION_FILL = PatternFill(start_color="D9E2F3", end_color="D9E2F3", fill_type="solid")
TOTAL_FONT = Font(name="Calibri", size=11, bold=True)
HEADER_FONT = Font(name="Calibri", size=10, bold=True)
HEADER_FILL = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
HEADER_FONT_WHITE = Font(name="Calibri", size=10, bold=True, color="FFFFFF")
CURRENCY_FMT = '$#,##0'
PCT_FMT = '0.0%'
THIN_TOP = Border(top=Side(style="thin"))
THIN_BOTTOM = Border(bottom=Side(style="thin"))
DOUBLE_BOTTOM = Border(bottom=Side(style="double"), top=Side(style="thin"))
RED_FONT = Font(name="Calibri", size=10, color="FF0000")

# ── P&L Line Item Definitions ────────────────────────────────────────────────
# Each tuple: (label, key, indent, line_type)
# line_type: "section", "item", "total", "subtotal", "blank", "net_income"

PNL_STRUCTURE = [
    ("REVENUE", None, 0, "section"),
    ("Food Sales", "food_rev", 1, "item"),
    ("Liquor / Beverage Sales", "liquor_rev", 1, "item"),
    ("Hookah Sales", "hookah_rev", 1, "item"),
    ("Other Revenue", "other_rev", 1, "item"),
    ("Gratuity Collected (20% auto)", "gratuity", 1, "item"),
    ("Tips Collected (voluntary)", "tips", 1, "item"),
    ("Cash Sales (undeposited)", "cash_undeposited", 1, "item"),
    ("Sales Tax Collected", "sales_tax", 1, "item"),
    ("TOTAL OPERATING REVENUE", "total_revenue", 0, "total"),
    ("", None, 0, "blank"),
    ("COST OF GOODS SOLD", None, 0, "section"),
    ("Food COGS", "food_cogs", 1, "item"),
    ("Liquor / Beverage COGS", "liquor_cogs", 1, "item"),
    ("Supplies & Smallwares", "supplies_cogs", 1, "item"),
    ("TOTAL COGS", "total_cogs", 0, "total"),
    ("", None, 0, "blank"),
    ("GROSS PROFIT", "gross_profit", 0, "total"),
    ("  Gross Margin %", "gross_margin_pct", 0, "pct_line"),
    ("", None, 0, "blank"),
    ("LABOR", None, 0, "section"),
    ("Payroll & Wages", "labor_gross", 1, "item"),
    ("  Labor % of Revenue", "labor_pct", 0, "pct_line"),
    ("", None, 0, "blank"),
    ("MARKETING & ENTERTAINMENT", None, 0, "section"),
    ("Entertainment", "mkt_entertainment", 1, "item"),
    ("Promoter Payout", "mkt_promoter", 1, "item"),
    ("PMG / Artist Booking", "mkt_artist", 1, "item"),
    ("Social Media", "mkt_social", 1, "item"),
    ("Flyers & Print", "mkt_flyers", 1, "item"),
    ("Event Expense", "mkt_event", 1, "item"),
    ("Pay-Per-View", "mkt_ppv", 1, "item"),
    ("TOTAL MARKETING", "total_marketing", 0, "total"),
    ("", None, 0, "blank"),
    ("OPERATING EXPENSES", None, 0, "section"),
    ("Rent & CAM", "opex_rent", 1, "item"),
    ("Taxes", "opex_taxes", 1, "item"),
    ("Security", "opex_security", 1, "item"),
    ("Insurance", "opex_insurance", 1, "item"),
    ("Bussers & Cleaners", "opex_bussers", 1, "item"),
    ("Contract Labor", "opex_contract_labor", 1, "item"),
    ("Janitorial Services", "opex_cleaning", 1, "item"),
    ("Utilities", "opex_utilities", 1, "item"),
    ("POS & Technology Fees", "opex_pos_tech", 1, "item"),
    ("Software & Subscriptions", "opex_software", 1, "item"),
    ("Phone & Internet", "opex_phone", 1, "item"),
    ("Professional Services", "opex_professional", 1, "item"),
    ("Permits & Licenses", "opex_permits", 1, "item"),
    ("Bank Fees", "opex_bank_fees", 1, "item"),
    ("Penalties & Fees", "opex_penalties", 1, "item"),
    ("Admin & Office", "opex_admin", 1, "item"),
    ("Lighting & Sound", "opex_lighting", 1, "item"),
    ("Other / Uncategorized", "opex_other", 1, "item"),
    ("TOTAL OPERATING EXPENSES", "total_opex", 0, "total"),
    ("", None, 0, "blank"),
    ("G&A / CORPORATE", None, 0, "section"),
    ("Owner Draws / Transfers", "ga_owner_draws", 1, "item"),
    ("Owner Discretionary", "ga_discretionary", 1, "item"),
    ("Personal Meals", "ga_meals", 1, "item"),
    ("Transportation", "ga_transportation", 1, "item"),
    ("Travel & Lodging", "ga_travel", 1, "item"),
    ("Credit Card Payments", "ga_credit_card", 1, "item"),
    ("Competitive Research", "ga_competitive", 1, "item"),
    ("Other G&A", "ga_other", 1, "item"),
    ("TOTAL G&A", "total_ga", 0, "total"),
    ("", None, 0, "blank"),
    ("FACILITY & TENANT IMPROVEMENTS", None, 0, "section"),
    ("Construction Build-Out", "cap_construction", 1, "item"),
    ("Capital Equipment", "cap_equipment", 1, "item"),
    ("Repairs & Maintenance", "cap_repairs", 1, "item"),
    ("TOTAL FACILITY & TI", "total_capex", 0, "total"),
    ("", None, 0, "blank"),
    ("TOTAL EXPENSES", "total_all_expenses", 0, "total"),
    ("", None, 0, "blank"),
    ("EBITDA", "ebitda", 0, "net_income"),
    ("  EBITDA Margin %", "ebitda_pct", 0, "pct_line"),
    ("", None, 0, "blank"),
    ("Note: Revenue includes tip & gratuity pass-through to staff", "pass_through_memo", 0, "memo"),
]


# ── BigQuery Queries ─────────────────────────────────────────────────────────

def _make_date_config(start: str, end: str) -> bigquery.QueryJobConfig:
    """Parameterized query config for Toast tables (DATE type)."""
    return bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("start_date", "DATE", start),
        bigquery.ScalarQueryParameter("end_date", "DATE", end),
    ])


def _make_string_config(start: str, end: str) -> bigquery.QueryJobConfig:
    """Parameterized query config for BankTransactions (STRING dates)."""
    return bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("start_date", "STRING", start),
        bigquery.ScalarQueryParameter("end_date", "STRING", end),
    ])


def query_monthly_revenue(client: bigquery.Client, start: str, end: str) -> Dict[str, Dict]:
    """Monthly net_sales, tips, gratuity from OrderDetails_raw."""
    q = f"""
    SELECT
        FORMAT_DATE('%Y-%m', processing_date) AS month,
        COALESCE(SUM(amount), 0) AS net_sales,
        COALESCE(SUM(tip), 0) AS tips,
        COALESCE(SUM(gratuity), 0) AS gratuity,
        COUNT(DISTINCT order_id) AS order_count
    FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
    WHERE processing_date BETWEEN @start_date AND @end_date
        AND (voided IS NULL OR voided = 'false')
    GROUP BY month ORDER BY month
    """
    rows = client.query(q, job_config=_make_date_config(start, end)).result()
    return {
        r.month: {
            "net_sales": float(r.net_sales or 0),
            "tips": float(r.tips or 0),
            "gratuity": float(r.gratuity or 0),
            "order_count": int(r.order_count or 0),
        }
        for r in rows
    }


def query_revenue_by_category(client: bigquery.Client, start: str, end: str) -> Dict[str, Dict]:
    """Monthly food vs liquor revenue from ItemSelectionDetails_raw."""
    q = f"""
    SELECT
        FORMAT_DATE('%Y-%m', processing_date) AS month,
        COALESCE(SUM(CASE WHEN sales_category = 'Food' THEN CAST(net_price AS FLOAT64) ELSE 0 END), 0) AS food_rev,
        COALESCE(SUM(CASE WHEN sales_category = 'Liquor' THEN CAST(net_price AS FLOAT64) ELSE 0 END), 0) AS liquor_rev
    FROM `{PROJECT_ID}.{DATASET_ID}.ItemSelectionDetails_raw`
    WHERE processing_date BETWEEN @start_date AND @end_date
        AND (voided IS NULL OR voided = 'false')
    GROUP BY month ORDER BY month
    """
    rows = client.query(q, job_config=_make_date_config(start, end)).result()
    return {
        r.month: {
            "food_rev": float(r.food_rev or 0),
            "liquor_rev": float(r.liquor_rev or 0),
        }
        for r in rows
    }


def query_hookah_revenue_bank(client: bigquery.Client, start: str, end: str) -> Dict[str, float]:
    """Monthly hookah revenue from bank deposits (Predictive Insights, May 2025+)."""
    q = f"""
    SELECT
        FORMAT_DATE('%Y-%m', transaction_date) AS month,
        COALESCE(SUM(amount), 0) AS hookah_rev
    FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
    WHERE transaction_date BETWEEN @start_date AND @end_date
        AND LOWER(category) LIKE '%hookah sales%'
        AND amount > 0
    GROUP BY month ORDER BY month
    """
    rows = client.query(q, job_config=_make_date_config(start, end)).result()
    return {r.month: float(r.hookah_rev or 0) for r in rows}


def query_hookah_revenue_pos(client: bigquery.Client, start: str, end: str) -> Dict[str, float]:
    """Monthly hookah revenue from Toast POS (in-house, through Mar 2025)."""
    q = f"""
    SELECT
        FORMAT_DATE('%Y-%m', processing_date) AS month,
        COALESCE(SUM(CAST(net_price AS FLOAT64)), 0) AS hookah_rev
    FROM `{PROJECT_ID}.{DATASET_ID}.ItemSelectionDetails_raw`
    WHERE processing_date BETWEEN @start_date AND @end_date
        AND sales_category = 'Hookah'
        AND (voided IS NULL OR voided = 'false')
    GROUP BY month ORDER BY month
    """
    rows = client.query(q, job_config=_make_date_config(start, end)).result()
    return {r.month: float(r.hookah_rev or 0) for r in rows}


# Predictive Insights $20K from Jan 2024 reclassed to Apr 2025 hookah revenue
HOOKAH_RECLASS = {"2025-04": 20_000.00, "2025-12": 15_000.00, "2026-03": 16_400.00}


def query_expenses_by_category(client: bigquery.Client, start: str, end: str) -> Dict[str, Dict[str, float]]:
    """Monthly expenses grouped by bank category."""
    q = f"""
    SELECT
        FORMAT_DATE('%Y-%m', transaction_date) AS month,
        category,
        ROUND(SUM(abs_amount), 2) AS total
    FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
    WHERE transaction_date BETWEEN @start_date AND @end_date
        AND transaction_type = 'debit'
    GROUP BY month, category
    ORDER BY month, total DESC
    """
    rows = client.query(q, job_config=_make_date_config(start, end)).result()
    result: Dict[str, Dict[str, float]] = {}
    for r in rows:
        if r.month not in result:
            result[r.month] = {}
        result[r.month][r.category] = float(r.total or 0)
    return result


def query_sales_tax(client: bigquery.Client, start: str, end: str) -> Dict[str, float]:
    """Monthly sales tax collected from OrderDetails_raw."""
    q = f"""
    SELECT
        FORMAT_DATE('%Y-%m', processing_date) AS month,
        COALESCE(SUM(tax), 0) AS sales_tax
    FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
    WHERE processing_date BETWEEN @start_date AND @end_date
        AND (voided IS NULL OR voided = 'false')
    GROUP BY month ORDER BY month
    """
    rows = client.query(q, job_config=_make_date_config(start, end)).result()
    return {r.month: float(r.sales_tax or 0) for r in rows}


def query_cash_undeposited(client: bigquery.Client, start: str, end: str) -> Dict[str, float]:
    """Monthly undeposited cash = Toast POS cash collected minus bank cash deposits."""
    # Cash collected at POS
    q_collected = f"""
    SELECT
        FORMAT_DATE('%Y-%m', processing_date) AS month,
        COALESCE(SUM(CASE WHEN payment_type = 'Cash' OR payment_type LIKE '%CASH%'
                     THEN total ELSE 0 END), 0) AS cash_collected
    FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
    WHERE processing_date BETWEEN @start_date AND @end_date
    GROUP BY month ORDER BY month
    """
    collected = {r.month: float(r.cash_collected or 0)
                 for r in client.query(q_collected, job_config=_make_date_config(start, end)).result()}

    # Cash deposited at bank
    q_deposited = f"""
    SELECT
        FORMAT_DATE('%Y-%m', transaction_date) AS month,
        COALESCE(SUM(abs_amount), 0) AS cash_deposited
    FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
    WHERE transaction_date BETWEEN @start_date AND @end_date
        AND transaction_type = 'credit'
        AND (LOWER(category) LIKE '%cash deposit%'
             OR LOWER(category) LIKE '%cash account transfer%'
             OR LOWER(description) LIKE '%counter credit%')
    GROUP BY month ORDER BY month
    """
    deposited = {r.month: float(r.cash_deposited or 0)
                 for r in client.query(q_deposited, job_config=_make_date_config(start, end)).result()}

    # Undeposited = collected - deposited
    all_months = sorted(set(list(collected.keys()) + list(deposited.keys())))
    return {m: round(collected.get(m, 0) - deposited.get(m, 0), 2) for m in all_months}


# ── Data Assembly ────────────────────────────────────────────────────────────

def _sum_matching(cats: Dict[str, float], keywords: List[str]) -> float:
    """Sum expense categories whose name contains any of the keywords (case-insensitive)."""
    return sum(v for k, v in cats.items()
               if any(kw.lower() in k.lower() for kw in keywords))


def compute_pnl_for_month(
    rev: Dict, rev_cat: Dict, hookah_bank: float, hookah_pos: float,
    sales_tax: float, cash_undeposited: float, expenses: Dict[str, float]
) -> Dict[str, float]:
    """Compute all P&L line items for a single month.

    Revenue sourced from Toast POS (full picture):
      net_sales, tips, gratuity, sales_tax, cash_undeposited, hookah
    Expenses sourced from Bank of America (what hits the bank):
      debits by category
    """
    net_sales = rev.get("net_sales", 0)
    tips = rev.get("tips", 0)
    gratuity = rev.get("gratuity", 0)
    food_rev = rev_cat.get("food_rev", 0)
    liquor_rev = rev_cat.get("liquor_rev", 0)

    # Total hookah = POS (already in net_sales) + bank deposits (additive)
    hookah_total = round(hookah_pos + hookah_bank, 2)

    pass_through = round(tips + gratuity * GRAT_PASSTHROUGH_PCT, 2)
    # POS hookah is already in net_sales, so subtract it from other_rev to avoid double-count
    other_rev = round(max(net_sales - food_rev - liquor_rev - hookah_pos, 0), 2)
    # Total Operating Revenue: Toast POS full picture + bank hookah deposits
    # net_sales + tips + gratuity + cash_undeposited + sales_tax + hookah_bank
    total_revenue = round(
        net_sales + tips + gratuity + cash_undeposited + sales_tax + hookah_bank, 2
    )
    rev_denom = total_revenue if total_revenue > 0 else 1

    # COGS (includes Shisha COGS)
    food_cogs = _sum_matching(expenses, ["food cogs"])
    liquor_cogs = _sum_matching(expenses, ["liquor cogs", "shisha cogs"])
    supplies_cogs = _sum_matching(expenses, ["supplies & equipment", "supplies & smallwares"])
    total_cogs = round(food_cogs + liquor_cogs + supplies_cogs, 2)

    gross_profit = round(total_revenue - total_cogs, 2)

    # Labor (gross — includes tip pass-through, bonuses per business plan methodology)
    labor_gross = _sum_matching(expenses, ["3. labor", "labor cost", "payroll",
                                            "tip pass-through", "employee bonus"])

    # Marketing (includes PMG Artist, PPV)
    mkt_entertainment = _sum_matching(expenses, ["entertainment"])
    mkt_promoter = _sum_matching(expenses, ["promoter"])
    mkt_social = _sum_matching(expenses, ["social media"])
    mkt_flyers = _sum_matching(expenses, ["flyer", "digital ads", "print", "event flyer"])
    mkt_event = _sum_matching(expenses, ["event expense"])
    mkt_artist = _sum_matching(expenses, ["pmg artist", "artist booking"])
    mkt_ppv = _sum_matching(expenses, ["pay-per-view"])
    total_marketing = round(
        mkt_entertainment + mkt_promoter + mkt_social + mkt_flyers
        + mkt_event + mkt_artist + mkt_ppv, 2
    )

    # OPEX (includes uniforms, legal fees, chargebacks)
    opex_rent = _sum_matching(expenses, ["rent", "cam", "property tax"])
    opex_taxes = _sum_matching(expenses, ["5. operating expenses (opex)/taxes"])
    opex_security = _sum_matching(expenses, ["security"])
    opex_insurance = _sum_matching(expenses, ["insurance"])
    opex_bussers = _sum_matching(expenses, ["bussers & cleaners"])
    opex_contract_labor = _sum_matching(expenses, ["contract labor"])
    opex_cleaning = _sum_matching(expenses, ["janitorial services", "cleaning", "janitorial"])
    opex_utilities = _sum_matching(expenses, ["electric", "gas", "energy"])
    opex_pos_tech = _sum_matching(expenses, ["pos", "technology fee"])
    opex_software = _sum_matching(expenses, ["software", "subscription"])
    opex_phone = _sum_matching(expenses, ["phone", "internet"])
    opex_professional = _sum_matching(expenses, ["professional service", "legal", "accounting",
                                                   "consulting"])
    opex_permits = _sum_matching(expenses, ["permit", "license"])
    opex_bank_fees = _sum_matching(expenses, ["bank fee", "service charge"])
    opex_penalties = _sum_matching(expenses, ["penalty", "fine", "late fee"])
    opex_admin = _sum_matching(expenses, ["admin & office", "uniform"])
    opex_lighting = _sum_matching(expenses, ["lighting", "sound", "av"])
    opex_other = _sum_matching(expenses, ["chargeback", "adjustment",
                                           "other income/expense",
                                           "other/uncategorized", "uncategorized"])
    total_opex = round(
        opex_rent + opex_taxes + opex_security + opex_insurance + opex_bussers
        + opex_contract_labor + opex_cleaning + opex_utilities + opex_pos_tech
        + opex_software + opex_phone + opex_professional + opex_permits
        + opex_bank_fees + opex_penalties + opex_admin + opex_lighting
        + opex_other, 2
    )

    total_expenses_operating = round(total_cogs + labor_gross + total_marketing + total_opex, 2)

    # Facility / CapEx
    cap_construction = _sum_matching(expenses, ["construction", "build out"])
    cap_equipment = _sum_matching(expenses, ["capital equipment"])
    cap_repairs = _sum_matching(expenses, ["repair", "maintenance"])
    total_capex = round(cap_construction + cap_equipment + cap_repairs, 2)

    # G&A / Owner's Compensation (includes internal account transfers)
    ga_owner_draws = _sum_matching(expenses, ["owner draws"])
    ga_discretionary = _sum_matching(expenses, ["owner discretionary"])
    ga_meals = _sum_matching(expenses, ["personal meals"])
    ga_transportation = _sum_matching(expenses, ["6. general & administrative / corporate/transportation"])
    ga_travel = _sum_matching(expenses, ["owner travel", "travel & entertainment", "travel & lodging"])
    ga_competitive = _sum_matching(expenses, ["competitive research"])
    ga_credit_card = _sum_matching(expenses, ["credit card payments"])
    ga_other = _sum_matching(expenses, ["equity injection", "non-transaction",
                                         "operating account credit", "cash withdrawal",
                                         "internal account transfer"])
    total_ga = round(
        ga_owner_draws + ga_discretionary + ga_meals + ga_transportation
        + ga_travel + ga_competitive + ga_credit_card + ga_other, 2
    )

    total_all_expenses = round(total_expenses_operating + total_ga + total_capex, 2)
    ebitda = round(total_revenue - total_all_expenses, 2)

    # Memo: pass-through amount for disclosure note
    pass_through_memo = pass_through

    return {
        "food_rev": food_rev,
        "liquor_rev": liquor_rev,
        "hookah_rev": hookah_total,
        "other_rev": other_rev,
        "gratuity": gratuity,
        "tips": tips,
        "cash_undeposited": cash_undeposited,
        "sales_tax": sales_tax,
        "total_revenue": total_revenue,
        "food_cogs": food_cogs,
        "liquor_cogs": liquor_cogs,
        "supplies_cogs": supplies_cogs,
        "total_cogs": total_cogs,
        "gross_profit": gross_profit,
        "gross_margin_pct": round(gross_profit / rev_denom, 4),
        "labor_gross": labor_gross,
        "labor_pct": round(labor_gross / rev_denom, 4),
        "mkt_entertainment": mkt_entertainment,
        "mkt_promoter": mkt_promoter,
        "mkt_artist": mkt_artist,
        "mkt_social": mkt_social,
        "mkt_flyers": mkt_flyers,
        "mkt_event": mkt_event,
        "mkt_ppv": mkt_ppv,
        "total_marketing": total_marketing,
        "opex_rent": opex_rent,
        "opex_taxes": opex_taxes,
        "opex_security": opex_security,
        "opex_insurance": opex_insurance,
        "opex_bussers": opex_bussers,
        "opex_contract_labor": opex_contract_labor,
        "opex_cleaning": opex_cleaning,
        "opex_utilities": opex_utilities,
        "opex_pos_tech": opex_pos_tech,
        "opex_software": opex_software,
        "opex_phone": opex_phone,
        "opex_professional": opex_professional,
        "opex_permits": opex_permits,
        "opex_bank_fees": opex_bank_fees,
        "opex_penalties": opex_penalties,
        "opex_admin": opex_admin,
        "opex_lighting": opex_lighting,
        "opex_other": opex_other,
        "total_opex": total_opex,
        "total_expenses_operating": total_expenses_operating,
        "cap_construction": cap_construction,
        "cap_equipment": cap_equipment,
        "cap_repairs": cap_repairs,
        "total_capex": total_capex,
        "ga_owner_draws": ga_owner_draws,
        "ga_discretionary": ga_discretionary,
        "ga_meals": ga_meals,
        "ga_transportation": ga_transportation,
        "ga_travel": ga_travel,
        "ga_competitive": ga_competitive,
        "ga_credit_card": ga_credit_card,
        "ga_other": ga_other,
        "total_ga": total_ga,
        "total_all_expenses": total_all_expenses,
        "ebitda": ebitda,
        "ebitda_pct": round(ebitda / rev_denom, 4),
        "pass_through_memo": pass_through_memo,
    }


def sum_monthly_data(monthly: Dict[str, Dict[str, float]]) -> Dict[str, float]:
    """Sum all monthly P&L dicts into a YTD total."""
    ytd: Dict[str, float] = {}
    for month_data in monthly.values():
        for key, val in month_data.items():
            ytd[key] = ytd.get(key, 0) + val
    # Recompute percentages from YTD totals
    rev = ytd.get("total_revenue", 0) or 1
    ytd["gross_margin_pct"] = round(ytd.get("gross_profit", 0) / rev, 4)
    ytd["labor_pct"] = round(ytd.get("labor_gross", 0) / rev, 4)
    ytd["ebitda_pct"] = round(ytd.get("ebitda", 0) / rev, 4)
    return ytd


def query_period(client: bigquery.Client, start: str, end: str) -> Tuple[List[str], Dict[str, Dict], Dict[str, float]]:
    """Run all queries for a period and return (months, monthly_data, ytd_data).

    Revenue: Toast POS (full picture) + bank hookah deposits
    Expenses: Bank of America debits (CC transactions + limited cash deposits)
    """
    log.info(f"Querying period {start} to {end}...")

    rev = query_monthly_revenue(client, start, end)
    rev_cat = query_revenue_by_category(client, start, end)
    hookah_bank = query_hookah_revenue_bank(client, start, end)
    hookah_pos = query_hookah_revenue_pos(client, start, end)
    sales_tax = query_sales_tax(client, start, end)
    cash_undeposited = query_cash_undeposited(client, start, end)
    expenses = query_expenses_by_category(client, start, end)

    # Apply hookah reclass (Predictive Insights $20K from Jan 2024 → Apr 2025)
    for m, amt in HOOKAH_RECLASS.items():
        if start <= m <= end:
            hookah_bank[m] = hookah_bank.get(m, 0) + amt

    all_months = sorted(set(
        list(rev.keys()) + list(rev_cat.keys())
        + list(hookah_bank.keys()) + list(hookah_pos.keys())
        + list(sales_tax.keys()) + list(cash_undeposited.keys())
        + list(expenses.keys())
    ))

    monthly_data: Dict[str, Dict[str, float]] = {}
    for m in all_months:
        monthly_data[m] = compute_pnl_for_month(
            rev.get(m, {}),
            rev_cat.get(m, {}),
            hookah_bank.get(m, 0),
            hookah_pos.get(m, 0),
            sales_tax.get(m, 0),
            cash_undeposited.get(m, 0),
            expenses.get(m, {}),
        )

    ytd = sum_monthly_data(monthly_data)
    log.info(f"  {len(all_months)} months loaded, YTD revenue: ${ytd.get('total_revenue', 0):,.0f}")
    return all_months, monthly_data, ytd


# ── Excel Writer ─────────────────────────────────────────────────────────────

def _month_label(m: str) -> str:
    """Convert '2025-01' to 'Jan 2025'."""
    parts = m.split("-")
    return f"{calendar.month_abbr[int(parts[1])]} {parts[0]}"


def write_pnl_sheet(
    wb: Workbook,
    sheet_name: str,
    period_label: str,
    months: List[str],
    monthly_data: Dict[str, Dict[str, float]],
    ytd_data: Dict[str, float],
):
    """Write one P&L sheet to the workbook."""
    ws = wb.create_sheet(title=sheet_name)
    num_months = len(months)
    ytd_col = num_months + 2  # col B..B+n-1 = months, then YTD
    pct_col = ytd_col + 1     # % of Revenue
    last_col = pct_col

    # Column widths
    ws.column_dimensions["A"].width = 42
    for i in range(2, last_col + 1):
        ws.column_dimensions[get_column_letter(i)].width = 15

    # ── Header block ──
    row = 1
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=last_col)
    c = ws.cell(row=row, column=1, value="LOV3|HTX")
    c.font = TITLE_FONT
    c.alignment = Alignment(horizontal="center")

    row = 2
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=last_col)
    c = ws.cell(row=row, column=1, value="Income Statement (Profit & Loss)")
    c.font = Font(name="Calibri", size=12, bold=True)
    c.alignment = Alignment(horizontal="center")

    row = 3
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=last_col)
    c = ws.cell(row=row, column=1, value=period_label)
    c.font = SUBTITLE_FONT
    c.alignment = Alignment(horizontal="center")

    row = 4
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=last_col)
    c = ws.cell(row=row, column=1, value=f"Prepared: {date.today().strftime('%B %d, %Y')}")
    c.font = Font(name="Calibri", size=9, italic=True, color="666666")
    c.alignment = Alignment(horizontal="center")

    # ── Column headers ──
    row = 6
    ws.cell(row=row, column=1, value="").font = HEADER_FONT
    for i, m in enumerate(months):
        c = ws.cell(row=row, column=i + 2, value=_month_label(m))
        c.font = HEADER_FONT_WHITE
        c.fill = HEADER_FILL
        c.alignment = Alignment(horizontal="center")
    c = ws.cell(row=row, column=ytd_col, value="YTD Total")
    c.font = HEADER_FONT_WHITE
    c.fill = HEADER_FILL
    c.alignment = Alignment(horizontal="center")
    c = ws.cell(row=row, column=pct_col, value="% of Rev")
    c.font = HEADER_FONT_WHITE
    c.fill = HEADER_FILL
    c.alignment = Alignment(horizontal="center")

    # ── P&L rows ──
    row = 7
    for label, key, indent, line_type in PNL_STRUCTURE:
        if line_type == "blank":
            row += 1
            continue

        # Label cell
        display_label = ("  " * indent + label) if indent else label
        c = ws.cell(row=row, column=1, value=display_label)

        if line_type == "section":
            c.font = SECTION_FONT
            for col in range(1, last_col + 1):
                ws.cell(row=row, column=col).fill = SECTION_FILL
            row += 1
            continue

        if line_type == "memo":
            # Memo note with pass-through amount in YTD column
            c.font = Font(name="Calibri", size=9, italic=True, color="666666")
            ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=ytd_col - 1)
            if key:
                ytd_val = ytd_data.get(key, 0)
                cell = ws.cell(row=row, column=ytd_col, value=round(ytd_val, 2))
                cell.number_format = CURRENCY_FMT
                cell.font = Font(name="Calibri", size=9, italic=True, color="666666")
            row += 1
            continue

        # Data cells
        if key:
            is_pct = line_type == "pct_line"
            is_credit = line_type == "credit"

            for i, m in enumerate(months):
                val = monthly_data.get(m, {}).get(key, 0)
                cell = ws.cell(row=row, column=i + 2)
                if is_pct:
                    cell.value = val
                    cell.number_format = PCT_FMT
                elif is_credit and val > 0:
                    cell.value = -val
                    cell.number_format = CURRENCY_FMT
                    cell.font = RED_FONT
                else:
                    cell.value = round(val, 2)
                    cell.number_format = CURRENCY_FMT
                    if val < 0:
                        cell.font = RED_FONT

            # YTD
            ytd_val = ytd_data.get(key, 0)
            cell = ws.cell(row=row, column=ytd_col)
            if is_pct:
                cell.value = ytd_val
                cell.number_format = PCT_FMT
            elif is_credit and ytd_val > 0:
                cell.value = -ytd_val
                cell.number_format = CURRENCY_FMT
                cell.font = RED_FONT
            else:
                cell.value = round(ytd_val, 2)
                cell.number_format = CURRENCY_FMT
                if ytd_val < 0:
                    cell.font = RED_FONT

            # % of Revenue
            if not is_pct:
                rev = ytd_data.get("total_revenue", 0) or 1
                pct = ytd_val / rev if not is_credit else -ytd_val / rev
                cell = ws.cell(row=row, column=pct_col)
                cell.value = abs(pct) if is_credit else pct
                cell.number_format = PCT_FMT

        # Formatting by line type
        if line_type in ("total", "subtotal"):
            c.font = TOTAL_FONT
            for col in range(1, last_col + 1):
                ws.cell(row=row, column=col).border = THIN_TOP
            c.font = TOTAL_FONT
        elif line_type == "net_income":
            c.font = Font(name="Calibri", size=12, bold=True)
            for col in range(1, last_col + 1):
                ws.cell(row=row, column=col).border = DOUBLE_BOTTOM
                ws.cell(row=row, column=col).font = Font(name="Calibri", size=11, bold=True)
            c.font = Font(name="Calibri", size=12, bold=True)

        row += 1

    # Print settings
    ws.sheet_properties.pageSetUpPr = None
    ws.print_area = f"A1:{get_column_letter(last_col)}{row}"
    log.info(f"  Sheet '{sheet_name}' written ({row} rows)")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    client = bigquery.Client(project=PROJECT_ID)

    # Period 1: 2025 Year-End
    months_2025, data_2025, ytd_2025 = query_period(client, "2025-01-01", "2025-12-31")

    # Period 2: Jan–Mar 2026 Interim
    months_2026, data_2026, ytd_2026 = query_period(client, "2026-01-01", "2026-03-31")

    wb = Workbook()
    wb.remove(wb.active)  # remove default sheet

    write_pnl_sheet(
        wb, "2025 Year-End P&L",
        "For the Year Ended December 31, 2025",
        months_2025, data_2025, ytd_2025,
    )
    write_pnl_sheet(
        wb, "Mar 2026 Interim P&L",
        "For the Three Months Ended March 31, 2026",
        months_2026, data_2026, ytd_2026,
    )

    filename = "LOV3_HTX_Financial_Statements_SBA.xlsx"
    wb.save(filename)
    log.info(f"Saved: {filename}")

    # Standalone Q1 2026 file
    wb2 = Workbook()
    wb2.remove(wb2.active)
    write_pnl_sheet(
        wb2, "Q1 2026 P&L",
        "For the Three Months Ended March 31, 2026",
        months_2026, data_2026, ytd_2026,
    )
    q1_filename = "LOV3_HTX_Q1_2026_PL.xlsx"
    wb2.save(q1_filename)
    log.info(f"Saved: {q1_filename}")

    print(f"\nDone! Files saved:")
    print(f"  {filename}")
    print(f"  {q1_filename}")


if __name__ == "__main__":
    main()
