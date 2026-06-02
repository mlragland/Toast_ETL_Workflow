"""Q1 2026 Leadership Financial Report generator.

Fetches all required data in a single BigQuery batch, holds it in typed
dataclasses, and renders to HTML or Markdown from the same data structure.
PDF generation lives in the standalone q1_report_pdf.py script — NOT here.
"""

from __future__ import annotations

import html
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from google.cloud import bigquery

from config import (
    Q1_2026_START, Q1_2026_END,
    Q4_2025_START, Q4_2025_END,
    Q1_2025_START, Q1_2025_END,
    Q1_2026_MONTHS,
    Q1_REPORT_FORWARD_LOOK,
    VENDOR_CONCENTRATION_THRESHOLD,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Dataclasses — one per section of the report
# ---------------------------------------------------------------------

@dataclass
class PeriodMetrics:
    """Numeric metrics for a single period (Q1 2026, Q4 2025, or Q1 2025)."""
    label: str
    gross_revenue: float = 0.0
    pos_revenue: float = 0.0
    service_charge: float = 0.0
    voluntary_tips: float = 0.0
    hookah_pos: float = 0.0
    hookah_bank: float = 0.0
    hookah_reclass: float = 0.0
    cogs: float = 0.0
    labor: float = 0.0
    opex: float = 0.0
    ebitda: float = 0.0
    covers: int = 0
    avg_check: float = 0.0
    business_days: int = 0
    labor_hours: float = 0.0


@dataclass
class RevenueSection:
    q1_2026: PeriodMetrics
    q4_2025: PeriodMetrics
    q1_2025: PeriodMetrics
    monthly: Dict[str, PeriodMetrics] = field(default_factory=dict)
    category_mix: Dict[str, float] = field(default_factory=dict)


@dataclass
class CostSection:
    q1_2026: PeriodMetrics
    q4_2025: PeriodMetrics
    q1_2025: PeriodMetrics
    opex_by_category: Dict[str, float] = field(default_factory=dict)
    labor_pct_revenue: float = 0.0


@dataclass
class ProfitabilitySection:
    q1_2026: PeriodMetrics
    q4_2025: PeriodMetrics
    q1_2025: PeriodMetrics
    ebitda_margin_q1_2026: float = 0.0
    ebitda_margin_q4_2025: float = 0.0
    ebitda_margin_q1_2025: float = 0.0


@dataclass
class KPISection:
    q1_2026: PeriodMetrics
    q4_2025: PeriodMetrics
    q1_2025: PeriodMetrics
    revenue_per_labor_hour_q1: float = 0.0
    revenue_per_business_day_q1: float = 0.0


@dataclass
class StaffPerformer:
    name: str
    attributed_revenue: float
    hours_worked: float = 0.0


@dataclass
class StaffSection:
    top_bartenders: List[StaffPerformer] = field(default_factory=list)
    top_servers: List[StaffPerformer] = field(default_factory=list)


@dataclass
class VendorSpend:
    name: str
    spend: float
    pct_of_opex: float


@dataclass
class CashFlowSection:
    total_deposits: float = 0.0
    total_expenses: float = 0.0
    by_category: Dict[str, float] = field(default_factory=dict)
    top_vendors: List[VendorSpend] = field(default_factory=list)
    concentration_warnings: List[str] = field(default_factory=list)


@dataclass
class ForwardLookSection:
    bullets: List[str] = field(default_factory=list)


@dataclass
class Q1ReportData:
    generated_at: str
    revenue: RevenueSection
    costs: CostSection
    profitability: ProfitabilitySection
    kpis: KPISection
    staff: StaffSection
    cashflow: CashFlowSection
    forward: ForwardLookSection


# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------

def compute_pct_change(current: float, prior: float) -> Optional[float]:
    """Percent change from prior to current. Returns None if prior is zero."""
    if prior == 0:
        return None
    return ((current - prior) / prior) * 100.0


def safe_div(num: float, denom: float) -> float:
    """Division that returns 0.0 when denominator is zero."""
    if denom == 0:
        return 0.0
    return num / denom


# ---------------------------------------------------------------------
# Generator (skeleton — sections filled in by later tasks)
# ---------------------------------------------------------------------

class Q1ReportGenerator:
    """Fetches Q1 report data from BigQuery and renders to HTML / Markdown."""

    def __init__(self, client: bigquery.Client):
        self.client = client

    def fetch(self) -> Q1ReportData:
        """Run all section queries and assemble Q1ReportData. Filled in Task 8."""
        raise NotImplementedError("Implemented in Task 8")

    def render_html(self, data: Q1ReportData) -> str:
        """Render full HTML page. Filled in Task 10."""
        raise NotImplementedError("Implemented in Task 10")

    def render_markdown(self, data: Q1ReportData) -> str:
        """Render markdown. Filled in Task 9."""
        raise NotImplementedError("Implemented in Task 9")

    def _fetch_period_revenue_raw(self, start: str, end: str) -> dict:
        """Run revenue queries for a single period, returning raw totals dict.

        Reuses query helpers from sba_financial_statements.py.
        Hookah_reclass is applied AFTER summing — see config.HOOKAH_RECLASS.
        """
        from sba_financial_statements import (
            query_monthly_revenue, query_hookah_revenue_bank,
            query_hookah_revenue_pos, sum_monthly_data,
            HOOKAH_RECLASS,
        )

        monthly = query_monthly_revenue(self.client, start, end)
        totals = sum_monthly_data(monthly)
        hookah_pos = sum(query_hookah_revenue_pos(self.client, start, end).values())
        hookah_bank = sum(query_hookah_revenue_bank(self.client, start, end).values())

        # Apply HOOKAH_RECLASS overrides that fall within the period
        reclass = 0.0
        for ym, amt in HOOKAH_RECLASS.items():
            ym_start = ym.replace("-", "") + "01"
            if start <= ym_start <= end:
                reclass += amt

        return {
            "gross_revenue": totals.get("net_sales", 0.0)
                + totals.get("service_charge", 0.0)
                + totals.get("voluntary_tips", 0.0)
                + hookah_bank + reclass,
            "pos_revenue": totals.get("net_sales", 0.0),
            "service_charge": totals.get("service_charge", 0.0),
            "voluntary_tips": totals.get("voluntary_tips", 0.0),
            "hookah_pos": hookah_pos,
            "hookah_bank": hookah_bank,
            "hookah_reclass": reclass,
        }

    def _make_period_metrics(self, label: str, raw: dict) -> PeriodMetrics:
        return PeriodMetrics(
            label=label,
            gross_revenue=raw["gross_revenue"],
            pos_revenue=raw["pos_revenue"],
            service_charge=raw["service_charge"],
            voluntary_tips=raw["voluntary_tips"],
            hookah_pos=raw["hookah_pos"],
            hookah_bank=raw["hookah_bank"],
            hookah_reclass=raw["hookah_reclass"],
        )

    def _fetch_revenue(self) -> RevenueSection:
        q1_raw = self._fetch_period_revenue_raw(Q1_2026_START, Q1_2026_END)
        q4_raw = self._fetch_period_revenue_raw(Q4_2025_START, Q4_2025_END)
        prior_q1_raw = self._fetch_period_revenue_raw(Q1_2025_START, Q1_2025_END)

        monthly = {}
        for ym in Q1_2026_MONTHS:
            ym_start = ym.replace("-", "") + "01"
            # last day of month — quick lookup table for Q1
            last_day = {"2026-01": "31", "2026-02": "28", "2026-03": "31"}[ym]
            ym_end = ym.replace("-", "") + last_day
            m_raw = self._fetch_period_revenue_raw(ym_start, ym_end)
            monthly[ym] = self._make_period_metrics(ym, m_raw)

        # Category mix from Q1 2026 only
        from sba_financial_statements import query_revenue_by_category
        cat_data = query_revenue_by_category(self.client, Q1_2026_START, Q1_2026_END)
        category_mix = {}
        for m_data in cat_data.values():
            for cat, amt in m_data.items():
                category_mix[cat] = category_mix.get(cat, 0.0) + amt

        return RevenueSection(
            q1_2026=self._make_period_metrics("Q1 2026", q1_raw),
            q4_2025=self._make_period_metrics("Q4 2025", q4_raw),
            q1_2025=self._make_period_metrics("Q1 2025", prior_q1_raw),
            monthly=monthly,
            category_mix=category_mix,
        )

    def _fetch_period_costs_raw(self, start: str, end: str) -> dict:
        """Total cost buckets for a period."""
        from sba_financial_statements import query_expenses_by_category

        expenses = query_expenses_by_category(self.client, start, end)
        # query_expenses_by_category returns {month: {category: amount}}
        totals = {}
        for m_data in expenses.values():
            for cat, amt in m_data.items():
                totals[cat] = totals.get(cat, 0.0) + amt

        cogs = (totals.get("Food", 0.0) + totals.get("Beverage", 0.0)
                + totals.get("Liquor", 0.0))
        labor = totals.get("Labor", 0.0) + totals.get("Payroll", 0.0)
        opex = sum(totals.values()) - cogs - labor

        return {"cogs": cogs, "labor": labor, "opex": opex}

    def _fetch_opex_by_category(self, start: str, end: str) -> Dict[str, float]:
        from sba_financial_statements import query_expenses_by_category
        expenses = query_expenses_by_category(self.client, start, end)
        totals = {}
        for m_data in expenses.values():
            for cat, amt in m_data.items():
                totals[cat] = totals.get(cat, 0.0) + amt
        # Strip COGS/labor buckets — those have dedicated lines
        for k in ("Food", "Beverage", "Liquor", "Labor", "Payroll"):
            totals.pop(k, None)
        return totals

    def _make_cost_metrics(self, label: str, raw: dict, gross_revenue: float) -> PeriodMetrics:
        return PeriodMetrics(
            label=label,
            gross_revenue=gross_revenue,
            cogs=raw["cogs"],
            labor=raw["labor"],
            opex=raw["opex"],
        )

    def _fetch_costs(self, revenue: RevenueSection) -> CostSection:
        q1 = self._fetch_period_costs_raw(Q1_2026_START, Q1_2026_END)
        q4 = self._fetch_period_costs_raw(Q4_2025_START, Q4_2025_END)
        prior = self._fetch_period_costs_raw(Q1_2025_START, Q1_2025_END)
        opex_cats = self._fetch_opex_by_category(Q1_2026_START, Q1_2026_END)

        labor_pct = safe_div(q1["labor"], revenue.q1_2026.gross_revenue) * 100.0

        return CostSection(
            q1_2026=self._make_cost_metrics("Q1 2026", q1, revenue.q1_2026.gross_revenue),
            q4_2025=self._make_cost_metrics("Q4 2025", q4, revenue.q4_2025.gross_revenue),
            q1_2025=self._make_cost_metrics("Q1 2025", prior, revenue.q1_2025.gross_revenue),
            opex_by_category=opex_cats,
            labor_pct_revenue=labor_pct,
        )

    def _build_profitability(self, revenue: RevenueSection, costs: CostSection) -> ProfitabilitySection:
        def ebitda(rev: PeriodMetrics, c: PeriodMetrics) -> float:
            return rev.gross_revenue - c.cogs - c.labor - c.opex

        q1 = PeriodMetrics(
            label="Q1 2026",
            gross_revenue=revenue.q1_2026.gross_revenue,
            cogs=costs.q1_2026.cogs, labor=costs.q1_2026.labor, opex=costs.q1_2026.opex,
            ebitda=ebitda(revenue.q1_2026, costs.q1_2026),
        )
        q4 = PeriodMetrics(
            label="Q4 2025",
            gross_revenue=revenue.q4_2025.gross_revenue,
            cogs=costs.q4_2025.cogs, labor=costs.q4_2025.labor, opex=costs.q4_2025.opex,
            ebitda=ebitda(revenue.q4_2025, costs.q4_2025),
        )
        prior = PeriodMetrics(
            label="Q1 2025",
            gross_revenue=revenue.q1_2025.gross_revenue,
            cogs=costs.q1_2025.cogs, labor=costs.q1_2025.labor, opex=costs.q1_2025.opex,
            ebitda=ebitda(revenue.q1_2025, costs.q1_2025),
        )
        return ProfitabilitySection(
            q1_2026=q1, q4_2025=q4, q1_2025=prior,
            ebitda_margin_q1_2026=safe_div(q1.ebitda, q1.gross_revenue) * 100.0,
            ebitda_margin_q4_2025=safe_div(q4.ebitda, q4.gross_revenue) * 100.0,
            ebitda_margin_q1_2025=safe_div(prior.ebitda, prior.gross_revenue) * 100.0,
        )

    def _fetch_period_kpis_raw(self, start: str, end: str) -> dict:
        """Operational KPIs for a single period."""
        from config import BUSINESS_DAY_SQL

        sql = f"""
        WITH checks AS (
          SELECT DISTINCT check_guid, total_amount, opened_date
          FROM `toast-analytics-444116.toast_raw.CheckDetails_raw`
          WHERE DATE({BUSINESS_DAY_SQL.format(dt_col='opened_date')}) BETWEEN @start AND @end
        ),
        days AS (
          SELECT COUNT(DISTINCT DATE({BUSINESS_DAY_SQL.format(dt_col='opened_date')})) AS biz_days
          FROM `toast-analytics-444116.toast_raw.CheckDetails_raw`
          WHERE DATE({BUSINESS_DAY_SQL.format(dt_col='opened_date')}) BETWEEN @start AND @end
        )
        SELECT
          (SELECT COUNT(*) FROM checks) AS covers,
          (SELECT AVG(total_amount) FROM checks) AS avg_check,
          (SELECT biz_days FROM days) AS business_days
        """
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start", "DATE",
                f"{start[:4]}-{start[4:6]}-{start[6:]}"),
            bigquery.ScalarQueryParameter("end", "DATE",
                f"{end[:4]}-{end[4:6]}-{end[6:]}"),
        ])
        row = next(self.client.query(sql, job_config=job_config).result(), None)
        if row is None:
            return {"covers": 0, "avg_check": 0.0, "business_days": 0, "labor_hours": 0.0}

        # Labor hours from labor table if it exists; tolerate absence
        labor_hours = 0.0
        try:
            lh_sql = """
            SELECT SUM(TIMESTAMP_DIFF(out_date, in_date, MINUTE) / 60.0) AS hours
            FROM `toast-analytics-444116.toast_raw.LaborTimeEntries_raw`
            WHERE DATE(in_date) BETWEEN @start AND @end
            """
            lh_row = next(self.client.query(lh_sql, job_config=job_config).result(), None)
            if lh_row and lh_row["hours"] is not None:
                labor_hours = float(lh_row["hours"])
        except Exception as e:
            log.warning("Labor hours query failed (table may not exist): %s", e)

        return {
            "covers": int(row["covers"] or 0),
            "avg_check": float(row["avg_check"] or 0.0),
            "business_days": int(row["business_days"] or 0),
            "labor_hours": labor_hours,
        }

    def _fetch_kpis(self, revenue: RevenueSection) -> KPISection:
        q1 = self._fetch_period_kpis_raw(Q1_2026_START, Q1_2026_END)
        q4 = self._fetch_period_kpis_raw(Q4_2025_START, Q4_2025_END)
        prior = self._fetch_period_kpis_raw(Q1_2025_START, Q1_2025_END)

        def _kpi_metrics(label, raw, rev_gross):
            return PeriodMetrics(
                label=label,
                gross_revenue=rev_gross,
                covers=raw["covers"],
                avg_check=raw["avg_check"],
                business_days=raw["business_days"],
                labor_hours=raw["labor_hours"],
            )

        gross_q1 = revenue.q1_2026.gross_revenue
        return KPISection(
            q1_2026=_kpi_metrics("Q1 2026", q1, gross_q1),
            q4_2025=_kpi_metrics("Q4 2025", q4, revenue.q4_2025.gross_revenue),
            q1_2025=_kpi_metrics("Q1 2025", prior, revenue.q1_2025.gross_revenue),
            revenue_per_business_day_q1=safe_div(gross_q1, q1["business_days"]),
            revenue_per_labor_hour_q1=safe_div(gross_q1, q1["labor_hours"]),
        )

    def _query_bartender_revenue(self, start: str, end: str) -> List[dict]:
        """Bartender attribution via KitchenTimings fulfilled_by — NOT POS sales alone."""
        from config import BUSINESS_DAY_SQL
        sql = f"""
        SELECT
          fulfilled_by AS name,
          COUNT(DISTINCT check_guid) AS items,
          SUM(net_price) AS revenue
        FROM `toast-analytics-444116.toast_raw.KitchenTimings_raw`
        WHERE DATE({BUSINESS_DAY_SQL.format(dt_col='sent_date')}) BETWEEN @start AND @end
          AND fulfilled_by IS NOT NULL
        GROUP BY fulfilled_by
        ORDER BY revenue DESC
        LIMIT 10
        """
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start", "DATE",
                f"{start[:4]}-{start[4:6]}-{start[6:]}"),
            bigquery.ScalarQueryParameter("end", "DATE",
                f"{end[:4]}-{end[4:6]}-{end[6:]}"),
        ])
        try:
            return [{"name": r["name"], "revenue": float(r["revenue"] or 0), "hours": 0.0}
                    for r in self.client.query(sql, job_config=job_config).result()]
        except Exception as e:
            log.warning("Bartender query failed: %s", e)
            return []

    def _query_server_revenue(self, start: str, end: str) -> List[dict]:
        """Server attribution including Bottle Manager tab name parsing.

        Bottle Manager is a POS station, not a person. When a check is rung
        under 'Bottle Manager' but the tab name contains a server name pattern
        (e.g., 'BM-Maria' or 'Maria-BM'), credit the revenue back to that server.
        """
        from config import BUSINESS_DAY_SQL
        sql = f"""
        WITH base AS (
          SELECT
            CASE
              WHEN LOWER(server_name) LIKE '%bottle manager%'
                THEN REGEXP_EXTRACT(tab_name, r'(?i)([A-Za-z]+)')
              ELSE server_name
            END AS attributed_name,
            total_amount
          FROM `toast-analytics-444116.toast_raw.CheckDetails_raw`
          WHERE DATE({BUSINESS_DAY_SQL.format(dt_col='opened_date')}) BETWEEN @start AND @end
        )
        SELECT attributed_name AS name, SUM(total_amount) AS revenue
        FROM base
        WHERE attributed_name IS NOT NULL
        GROUP BY attributed_name
        ORDER BY revenue DESC
        LIMIT 10
        """
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start", "DATE",
                f"{start[:4]}-{start[4:6]}-{start[6:]}"),
            bigquery.ScalarQueryParameter("end", "DATE",
                f"{end[:4]}-{end[4:6]}-{end[6:]}"),
        ])
        try:
            return [{"name": r["name"], "revenue": float(r["revenue"] or 0), "hours": 0.0}
                    for r in self.client.query(sql, job_config=job_config).result()]
        except Exception as e:
            log.warning("Server query failed: %s", e)
            return []

    def _fetch_staff(self) -> StaffSection:
        bartenders = self._query_bartender_revenue(Q1_2026_START, Q1_2026_END)
        servers = self._query_server_revenue(Q1_2026_START, Q1_2026_END)
        bartenders_sorted = sorted(bartenders, key=lambda x: x["revenue"], reverse=True)[:5]
        servers_sorted = sorted(servers, key=lambda x: x["revenue"], reverse=True)[:5]
        return StaffSection(
            top_bartenders=[StaffPerformer(b["name"], b["revenue"], b["hours"]) for b in bartenders_sorted],
            top_servers=[StaffPerformer(s["name"], s["revenue"], s["hours"]) for s in servers_sorted],
        )

    def _query_total_deposits(self, start: str, end: str) -> float:
        sql = """
        SELECT COALESCE(SUM(amount), 0) AS total
        FROM `toast-analytics-444116.toast_raw.BankTransactions_raw`
        WHERE transaction_date BETWEEN @start AND @end
          AND amount > 0
        """
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start", "DATE",
                f"{start[:4]}-{start[4:6]}-{start[6:]}"),
            bigquery.ScalarQueryParameter("end", "DATE",
                f"{end[:4]}-{end[4:6]}-{end[6:]}"),
        ])
        try:
            row = next(self.client.query(sql, job_config=job_config).result(), None)
            return float(row["total"] or 0) if row else 0.0
        except Exception as e:
            log.warning("Deposits query failed: %s", e)
            return 0.0

    def _query_expenses_for_cashflow(self, start: str, end: str) -> Dict[str, float]:
        sql = """
        SELECT
          COALESCE(category, 'Uncategorized') AS category,
          SUM(ABS(amount)) AS spend
        FROM `toast-analytics-444116.toast_raw.BankTransactions_raw`
        WHERE transaction_date BETWEEN @start AND @end
          AND amount < 0
        GROUP BY category
        ORDER BY spend DESC
        """
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start", "DATE",
                f"{start[:4]}-{start[4:6]}-{start[6:]}"),
            bigquery.ScalarQueryParameter("end", "DATE",
                f"{end[:4]}-{end[4:6]}-{end[6:]}"),
        ])
        try:
            return {r["category"]: float(r["spend"] or 0)
                    for r in self.client.query(sql, job_config=job_config).result()}
        except Exception as e:
            log.warning("Expenses query failed: %s", e)
            return {}

    def _query_top_vendors(self, start: str, end: str) -> List[dict]:
        sql = """
        SELECT
          COALESCE(vendor_name, description) AS vendor,
          SUM(ABS(amount)) AS spend
        FROM `toast-analytics-444116.toast_raw.BankTransactions_raw`
        WHERE transaction_date BETWEEN @start AND @end
          AND amount < 0
        GROUP BY vendor
        ORDER BY spend DESC
        LIMIT 10
        """
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start", "DATE",
                f"{start[:4]}-{start[4:6]}-{start[6:]}"),
            bigquery.ScalarQueryParameter("end", "DATE",
                f"{end[:4]}-{end[4:6]}-{end[6:]}"),
        ])
        try:
            return [{"vendor": r["vendor"] or "Unknown", "spend": float(r["spend"] or 0)}
                    for r in self.client.query(sql, job_config=job_config).result()]
        except Exception as e:
            log.warning("Top vendors query failed: %s", e)
            return []

    def _fetch_cashflow(self) -> CashFlowSection:
        deposits = self._query_total_deposits(Q1_2026_START, Q1_2026_END)
        by_cat = self._query_expenses_for_cashflow(Q1_2026_START, Q1_2026_END)
        vendors = self._query_top_vendors(Q1_2026_START, Q1_2026_END)
        total_exp = sum(by_cat.values())

        vendor_objs = []
        warnings = []
        for v in vendors:
            pct = safe_div(v["spend"], total_exp)
            vendor_objs.append(VendorSpend(name=v["vendor"], spend=v["spend"], pct_of_opex=pct))
            if pct > VENDOR_CONCENTRATION_THRESHOLD:
                warnings.append(f"{v['vendor']} represents {pct*100:.1f}% of opex (threshold: {VENDOR_CONCENTRATION_THRESHOLD*100:.0f}%)")

        return CashFlowSection(
            total_deposits=deposits,
            total_expenses=total_exp,
            by_category=by_cat,
            top_vendors=vendor_objs,
            concentration_warnings=warnings,
        )
