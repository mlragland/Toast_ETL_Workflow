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


def _to_iso_date(yyyymmdd: str) -> str:
    """Convert YYYYMMDD config strings to YYYY-MM-DD for BigQuery DATE params."""
    return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}"


# ---------------------------------------------------------------------
# Generator (skeleton — sections filled in by later tasks)
# ---------------------------------------------------------------------

class Q1ReportGenerator:
    """Fetches Q1 report data from BigQuery and renders to HTML / Markdown."""

    def __init__(self, client: bigquery.Client):
        self.client = client

    def fetch(self) -> Q1ReportData:
        log.info("Q1 report: fetching revenue section")
        revenue = self._fetch_revenue()
        log.info("Q1 report: fetching cost section")
        costs = self._fetch_costs(revenue)
        profitability = self._build_profitability(revenue, costs)
        log.info("Q1 report: fetching KPI section")
        kpis = self._fetch_kpis(revenue)
        log.info("Q1 report: fetching staff section")
        staff = self._fetch_staff()
        log.info("Q1 report: fetching cashflow section")
        cashflow = self._fetch_cashflow()
        forward = ForwardLookSection(bullets=list(Q1_REPORT_FORWARD_LOOK))

        return Q1ReportData(
            generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S CST"),
            revenue=revenue,
            costs=costs,
            profitability=profitability,
            kpis=kpis,
            staff=staff,
            cashflow=cashflow,
            forward=forward,
        )

    def render_html(self, data: Q1ReportData) -> str:
        """Render the full report as a standalone editorial-luxury HTML page."""
        return (
            _PAGE_OPEN
            + _STYLES
            + "</head><body>"
            + _hero_html(data)
            + '<main class="page">'
            + _section_revenue(data.revenue)
            + _section_costs(data.costs, data.revenue)
            + _section_profitability(data.profitability)
            + _section_kpis(data.kpis)
            + _section_staff(data.staff)
            + _section_cashflow(data.cashflow)
            + _section_forward(data.forward)
            + "</main>"
            + _footer_html(data)
            + "</body></html>"
        )

    def render_markdown(self, data: Q1ReportData) -> str:
        """Render the full report as markdown."""
        lines: List[str] = []
        lines.append("# LOV3|HTX — Q1 2026 Leadership Financial Report")
        lines.append("")
        lines.append(f"**Generated:** {data.generated_at}")
        lines.append("")
        lines.append("**Period:** January 1 – March 31, 2026")
        lines.append("")

        # ------ A. Revenue ------
        lines.append("## A. Revenue Analysis")
        lines.append("")
        r = data.revenue
        lines.append("| Metric | Q1 2026 | Q4 2025 | QoQ % | Q1 2025 | YoY % |")
        lines.append("|---|---:|---:|---:|---:|---:|")
        for name, attr in [
            ("Gross Revenue", "gross_revenue"),
            ("POS Net Sales", "pos_revenue"),
            ("Service Charge (20%)", "service_charge"),
            ("Voluntary Tips", "voluntary_tips"),
            ("Hookah (POS)", "hookah_pos"),
            ("Hookah (Bank deposits)", "hookah_bank"),
            ("Hookah (Reclass)", "hookah_reclass"),
        ]:
            lines.append(self._md_row(name, attr, r.q1_2026, r.q4_2025, r.q1_2025))
        lines.append("")
        lines.append("**Intra-quarter monthly trend:**")
        lines.append("")
        lines.append("| Month | Gross Revenue |")
        lines.append("|---|---:|")
        for ym, m in r.monthly.items():
            lines.append(f"| {ym} | ${m.gross_revenue:,.0f} |")
        lines.append("")
        lines.append("**Sales mix (Q1 2026):**")
        lines.append("")
        lines.append("| Category | Amount | Share |")
        lines.append("|---|---:|---:|")
        total_mix = sum(r.category_mix.values()) or 1
        for cat, amt in sorted(r.category_mix.items(), key=lambda x: -x[1]):
            lines.append(f"| {cat} | ${amt:,.0f} | {amt/total_mix*100:.1f}% |")
        lines.append("")

        # ------ B. Cost Structure ------
        lines.append("## B. Cost Structure")
        lines.append("")
        c = data.costs
        lines.append("| Metric | Q1 2026 | Q4 2025 | QoQ % | Q1 2025 | YoY % |")
        lines.append("|---|---:|---:|---:|---:|---:|")
        for name, attr in [("COGS", "cogs"), ("Labor", "labor"), ("Opex (other)", "opex")]:
            lines.append(self._md_row(name, attr, c.q1_2026, c.q4_2025, c.q1_2025))
        lines.append("")
        lines.append(f"**Labor as % of revenue (Q1 2026):** {c.labor_pct_revenue:.1f}%")
        lines.append("")
        lines.append("**Opex by category (Q1 2026):**")
        lines.append("")
        lines.append("| Category | Amount |")
        lines.append("|---|---:|")
        for cat, amt in sorted(c.opex_by_category.items(), key=lambda x: -x[1]):
            lines.append(f"| {cat} | ${amt:,.0f} |")
        lines.append("")

        # ------ C. Profitability ------
        lines.append("## C. Profitability")
        lines.append("")
        p = data.profitability
        lines.append("| Metric | Q1 2026 | Q4 2025 | QoQ % | Q1 2025 | YoY % |")
        lines.append("|---|---:|---:|---:|---:|---:|")
        lines.append(self._md_row("EBITDA", "ebitda", p.q1_2026, p.q4_2025, p.q1_2025))
        lines.append(f"| EBITDA Margin | {p.ebitda_margin_q1_2026:.1f}% | {p.ebitda_margin_q4_2025:.1f}% | — | {p.ebitda_margin_q1_2025:.1f}% | — |")
        lines.append("")

        # ------ D. KPIs ------
        lines.append("## D. Operational KPIs")
        lines.append("")
        k = data.kpis
        lines.append("| Metric | Q1 2026 | Q4 2025 | Q1 2025 |")
        lines.append("|---|---:|---:|---:|")
        lines.append(f"| Covers | {k.q1_2026.covers:,} | {k.q4_2025.covers:,} | {k.q1_2025.covers:,} |")
        lines.append(f"| Average Check | ${k.q1_2026.avg_check:,.2f} | ${k.q4_2025.avg_check:,.2f} | ${k.q1_2025.avg_check:,.2f} |")
        lines.append(f"| Business Days | {k.q1_2026.business_days} | {k.q4_2025.business_days} | {k.q1_2025.business_days} |")
        lines.append(f"| Labor Hours | {k.q1_2026.labor_hours:,.0f} | {k.q4_2025.labor_hours:,.0f} | {k.q1_2025.labor_hours:,.0f} |")
        lines.append(f"| Revenue / Business Day | ${k.revenue_per_business_day_q1:,.0f} | — | — |")
        lines.append(f"| Revenue / Labor Hour | ${k.revenue_per_labor_hour_q1:,.2f} | — | — |")
        lines.append("")

        # ------ E. Staff Performance ------
        lines.append("## E. Staff Performance")
        lines.append("")
        s = data.staff
        lines.append("**Top Bartenders (attribution via KitchenTimings fulfilled_by — service-well drinks):**")
        lines.append("")
        lines.append("| Rank | Name | Attributed Revenue |")
        lines.append("|---|---|---:|")
        for i, b in enumerate(s.top_bartenders, 1):
            lines.append(f"| {i} | {b.name} | ${b.attributed_revenue:,.0f} |")
        lines.append("")
        lines.append("**Top Servers (includes Bottle Manager walk-in attribution):**")
        lines.append("")
        lines.append("| Rank | Name | Attributed Revenue |")
        lines.append("|---|---|---:|")
        for i, srv in enumerate(s.top_servers, 1):
            lines.append(f"| {i} | {srv.name} | ${srv.attributed_revenue:,.0f} |")
        lines.append("")

        # ------ F. Cash Flow ------
        lines.append("## F. Cash Flow & Bank Reconciliation")
        lines.append("")
        cf = data.cashflow
        lines.append(f"- **Total Deposits (Q1 2026):** ${cf.total_deposits:,.0f}")
        lines.append(f"- **Total Expenses (Q1 2026):** ${cf.total_expenses:,.0f}")
        lines.append("")
        lines.append("**Expense buckets:**")
        lines.append("")
        lines.append("| Category | Amount |")
        lines.append("|---|---:|")
        for cat, amt in sorted(cf.by_category.items(), key=lambda x: -x[1]):
            lines.append(f"| {cat} | ${amt:,.0f} |")
        lines.append("")
        lines.append("**Top 10 vendors:**")
        lines.append("")
        lines.append("| Vendor | Spend | % of Opex |")
        lines.append("|---|---:|---:|")
        for v in cf.top_vendors:
            lines.append(f"| {v.name} | ${v.spend:,.0f} | {v.pct_of_opex*100:.1f}% |")
        lines.append("")
        if cf.concentration_warnings:
            lines.append("**⚠️ Concentration warnings:**")
            for w in cf.concentration_warnings:
                lines.append(f"- {w}")
            lines.append("")

        # ------ G. Forward Look ------
        lines.append("## G. Forward Look")
        lines.append("")
        for b in data.forward.bullets:
            lines.append(f"- {b}")
        lines.append("")
        return "\n".join(lines)

    def _md_row(self, label: str, attr: str, q1: PeriodMetrics, q4: PeriodMetrics, prior: PeriodMetrics) -> str:
        cur = getattr(q1, attr)
        q4v = getattr(q4, attr)
        prior_v = getattr(prior, attr)
        qoq = compute_pct_change(cur, q4v)
        yoy = compute_pct_change(cur, prior_v)
        qoq_s = f"{qoq:+.1f}%" if qoq is not None else "n/a"
        yoy_s = f"{yoy:+.1f}%" if yoy is not None else "n/a"
        return f"| {label} | ${cur:,.0f} | ${q4v:,.0f} | {qoq_s} | ${prior_v:,.0f} | {yoy_s} |"

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

        iso_start, iso_end = _to_iso_date(start), _to_iso_date(end)
        monthly = query_monthly_revenue(self.client, iso_start, iso_end)
        totals = sum_monthly_data(monthly)
        hookah_pos = sum(query_hookah_revenue_pos(self.client, iso_start, iso_end).values())
        hookah_bank = sum(query_hookah_revenue_bank(self.client, iso_start, iso_end).values())

        # Apply HOOKAH_RECLASS overrides that fall within the period
        reclass = 0.0
        for ym, amt in HOOKAH_RECLASS.items():
            ym_start = ym.replace("-", "") + "01"
            if start <= ym_start <= end:
                reclass += amt

        # query_monthly_revenue returns `gratuity` (20% service charge) and `tips` (voluntary)
        service_charge = totals.get("gratuity", 0.0)
        voluntary_tips = totals.get("tips", 0.0)
        return {
            "gross_revenue": totals.get("net_sales", 0.0)
                + service_charge + voluntary_tips
                + hookah_bank + reclass,
            "pos_revenue": totals.get("net_sales", 0.0),
            "service_charge": service_charge,
            "voluntary_tips": voluntary_tips,
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

        # Category mix from Q1 2026 only — relabel SBA keys to friendly names
        from sba_financial_statements import query_revenue_by_category
        cat_data = query_revenue_by_category(self.client, _to_iso_date(Q1_2026_START), _to_iso_date(Q1_2026_END))
        cat_labels = {"food_rev": "Food", "liquor_rev": "Liquor"}
        category_mix = {}
        for m_data in cat_data.values():
            for cat, amt in m_data.items():
                label = cat_labels.get(cat, cat)
                category_mix[label] = category_mix.get(label, 0.0) + amt

        return RevenueSection(
            q1_2026=self._make_period_metrics("Q1 2026", q1_raw),
            q4_2025=self._make_period_metrics("Q4 2025", q4_raw),
            q1_2025=self._make_period_metrics("Q1 2025", prior_q1_raw),
            monthly=monthly,
            category_mix=category_mix,
        )

    @staticmethod
    def _classify_category(cat: str) -> str:
        """Classify SBA-format `{N}. {Section}/{Subcategory}` into cogs/labor/opex bucket."""
        c = cat.lower()
        if c.startswith("2.") or "cost of goods sold" in c or "cogs" in c:
            return "cogs"
        if c.startswith("3.") or "labor cost" in c or "payroll" in c:
            return "labor"
        return "opex"

    def _fetch_period_costs_raw(self, start: str, end: str) -> dict:
        """Total cost buckets for a period.

        BankCategoryRules uses SBA-format category names like
        "2. Cost of Goods Sold/Liquor COGS" and "3. Labor Cost (Includes
        Grat + Tips)/Employee Payroll (FOH, BOH, Salaries & Taxes)".
        Classify by leading section number.
        """
        from sba_financial_statements import query_expenses_by_category

        expenses = query_expenses_by_category(self.client, _to_iso_date(start), _to_iso_date(end))
        totals = {"cogs": 0.0, "labor": 0.0, "opex": 0.0}
        for m_data in expenses.values():
            for cat, amt in m_data.items():
                bucket = self._classify_category(cat)
                totals[bucket] += amt
        return totals

    def _fetch_opex_by_category(self, start: str, end: str) -> Dict[str, float]:
        from sba_financial_statements import query_expenses_by_category
        expenses = query_expenses_by_category(self.client, _to_iso_date(start), _to_iso_date(end))
        opex: Dict[str, float] = {}
        for m_data in expenses.values():
            for cat, amt in m_data.items():
                if self._classify_category(cat) == "opex":
                    opex[cat] = opex.get(cat, 0.0) + amt
        return opex

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
        """Operational KPIs for a single period.

        opened_date in CheckDetails_raw is STRING (MM/DD/YY) — use PARSE_DATE.
        Skips the 4 AM business-day cutoff (not meaningful without hour info).
        """
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start", "DATE", _to_iso_date(start)),
            bigquery.ScalarQueryParameter("end", "DATE", _to_iso_date(end)),
        ])

        covers = 0
        avg_check = 0.0
        business_days = 0
        try:
            sql = """
            WITH checks AS (
              SELECT DISTINCT check_id, total, processing_date
              FROM `toast-analytics-444116.toast_raw.CheckDetails_raw`
              WHERE processing_date BETWEEN @start AND @end
            )
            SELECT
              COUNT(*) AS covers,
              AVG(total) AS avg_check,
              COUNT(DISTINCT processing_date) AS business_days
            FROM checks
            """
            row = next(self.client.query(sql, job_config=job_config).result(), None)
            if row is not None:
                covers = int(row["covers"] or 0)
                avg_check = float(row["avg_check"] or 0.0)
                business_days = int(row["business_days"] or 0)
        except Exception as e:
            log.warning("KPI query failed for %s..%s: %s", start, end, e)

        labor_hours = 0.0
        try:
            lh_sql = """
            SELECT SUM(COALESCE(regular_hours, 0) + COALESCE(overtime_hours, 0)) AS hours
            FROM `toast-analytics-444116.toast_raw.LaborTimeEntries_raw`
            WHERE processing_date BETWEEN @start AND @end
              AND (deleted IS NULL OR deleted = FALSE)
            """
            lh_row = next(self.client.query(lh_sql, job_config=job_config).result(), None)
            if lh_row and lh_row["hours"] is not None:
                labor_hours = float(lh_row["hours"])
        except Exception as e:
            log.warning("Labor hours query failed: %s", e)

        return {
            "covers": covers,
            "avg_check": avg_check,
            "business_days": business_days,
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
        """Bartender contribution via KitchenTimings fulfilled_by — items fulfilled.

        KitchenTimings has no revenue column. We report items fulfilled, which
        is the proxy for service-well contribution (per memory feedback).
        Revenue per bartender requires LaborTimeEntries non_cash_sales (their
        rung-up sales) which is separate.
        """
        sql = """
        SELECT
          fulfilled_by AS name,
          COUNT(*) AS items_fulfilled
        FROM `toast-analytics-444116.toast_raw.KitchenTimings_raw`
        WHERE processing_date BETWEEN @start AND @end
          AND fulfilled_by IS NOT NULL
          AND fulfilled_by != ''
        GROUP BY fulfilled_by
        ORDER BY items_fulfilled DESC
        LIMIT 10
        """
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start", "DATE", _to_iso_date(start)),
            bigquery.ScalarQueryParameter("end", "DATE", _to_iso_date(end)),
        ])
        try:
            return [{"name": r["name"], "revenue": float(r["items_fulfilled"] or 0), "hours": 0.0}
                    for r in self.client.query(sql, job_config=job_config).result()]
        except Exception as e:
            log.warning("Bartender query failed: %s", e)
            return []

    def _query_server_revenue(self, start: str, end: str) -> List[dict]:
        """Server attribution by check total.

        CheckDetails_raw has no tab_name column, so Bottle Manager walk-in
        revenue cannot be re-attributed at this query layer — those checks
        will show up under the literal 'Bottle Manager' server name.
        """
        sql = """
        SELECT
          server AS name,
          SUM(total) AS revenue
        FROM `toast-analytics-444116.toast_raw.CheckDetails_raw`
        WHERE processing_date BETWEEN @start AND @end
          AND server IS NOT NULL
          AND server != ''
        GROUP BY server
        ORDER BY revenue DESC
        LIMIT 10
        """
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start", "DATE", _to_iso_date(start)),
            bigquery.ScalarQueryParameter("end", "DATE", _to_iso_date(end)),
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
          COALESCE(vendor_normalized, description) AS vendor,
          SUM(ABS(amount)) AS spend
        FROM `toast-analytics-444116.toast_raw.BankTransactions_raw`
        WHERE transaction_date BETWEEN @start AND @end
          AND amount < 0
        GROUP BY vendor
        ORDER BY spend DESC
        LIMIT 10
        """
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start", "DATE", _to_iso_date(start)),
            bigquery.ScalarQueryParameter("end", "DATE", _to_iso_date(end)),
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


# =====================================================================
# Editorial-luxury HTML renderer for /q1-report
# Dark theme, Fraunces + Newsreader + JetBrains Mono, champagne accents.
# =====================================================================

_PAGE_OPEN = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>LOV3 / Houston — Q1 2026 Financial Review</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Fraunces:ital,opsz,wght@0,9..144,300;0,9..144,400;0,9..144,500;0,9..144,600;0,9..144,700;1,9..144,300;1,9..144,400;1,9..144,500&family=Newsreader:ital,opsz,wght@0,6..72,300;0,6..72,400;0,6..72,500;1,6..72,300;1,6..72,400&family=JetBrains+Mono:wght@300;400;500;600&display=swap" rel="stylesheet">
"""

_STYLES = """<style>
:root {
  --ink: #0A0A0F;
  --surface: #14141C;
  --surface-2: #1C1C26;
  --rule: #2A2A35;
  --rule-soft: #1E1E29;
  --gold: #D4A574;
  --gold-bright: #E8C896;
  --gold-dim: #8B6F47;
  --ivory: #F5F1E8;
  --bone: #A8A39B;
  --muted: #6B6660;
  --positive: #7FB069;
  --negative: #C97064;
  --display: 'Fraunces', 'Cormorant Garamond', Georgia, serif;
  --body: 'Newsreader', 'Source Serif Pro', Georgia, serif;
  --mono: 'JetBrains Mono', 'IBM Plex Mono', ui-monospace, monospace;
}
*, *::before, *::after { box-sizing: border-box; }
html, body { margin: 0; padding: 0; background: var(--ink); color: var(--ivory); }
body {
  font-family: var(--body);
  font-weight: 300;
  font-size: 17px;
  line-height: 1.55;
  -webkit-font-smoothing: antialiased;
  background:
    radial-gradient(ellipse at top, rgba(212,165,116,0.04), transparent 50%),
    radial-gradient(ellipse at bottom right, rgba(212,165,116,0.025), transparent 60%),
    var(--ink);
  background-attachment: fixed;
  min-height: 100vh;
}
::selection { background: var(--gold); color: var(--ink); }

/* ── Hero ─────────────────────────────────────────────────────────── */
.hero {
  max-width: 1200px;
  margin: 0 auto;
  padding: 6rem 3rem 3rem;
  border-bottom: 1px solid var(--rule);
  position: relative;
}
.hero::after {
  content: "";
  position: absolute;
  bottom: -1px; left: 3rem;
  width: 88px; height: 1px;
  background: var(--gold);
}
.eyebrow {
  font-family: var(--mono);
  font-size: 11px;
  font-weight: 500;
  letter-spacing: 0.32em;
  text-transform: uppercase;
  color: var(--gold);
  margin: 0 0 2.5rem;
  display: flex;
  align-items: center;
  gap: 1rem;
}
.eyebrow::after {
  content: "";
  flex: 1;
  height: 1px;
  background: linear-gradient(to right, var(--gold-dim), transparent);
  max-width: 200px;
}
.venue {
  font-family: var(--display);
  font-style: italic;
  font-weight: 400;
  font-size: clamp(1.5rem, 2vw, 2rem);
  color: var(--bone);
  margin: 0 0 0.5rem;
  letter-spacing: 0.02em;
}
.title {
  font-family: var(--display);
  font-weight: 300;
  font-size: clamp(3rem, 7vw, 5.5rem);
  line-height: 0.96;
  letter-spacing: -0.025em;
  margin: 0 0 2rem;
  color: var(--ivory);
}
.title em {
  font-style: italic;
  font-weight: 400;
  color: var(--gold);
}
.dek {
  font-family: var(--body);
  font-weight: 300;
  font-size: 1.25rem;
  line-height: 1.5;
  color: var(--bone);
  max-width: 620px;
  margin: 0 0 3rem;
  font-style: italic;
}
.hero-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 3rem;
  font-family: var(--mono);
  font-size: 11px;
  letter-spacing: 0.16em;
  text-transform: uppercase;
  color: var(--muted);
}
.hero-meta strong {
  display: block;
  font-weight: 500;
  color: var(--ivory);
  margin-top: 0.3rem;
  letter-spacing: 0.08em;
  font-size: 13px;
}

/* ── Layout ──────────────────────────────────────────────────────── */
.page { max-width: 1200px; margin: 0 auto; padding: 0 3rem 5rem; }
section.report {
  padding: 5rem 0 4rem;
  border-bottom: 1px solid var(--rule);
}
section.report:last-of-type { border-bottom: none; }
.section-head {
  display: grid;
  grid-template-columns: 200px 1fr;
  gap: 3rem;
  margin-bottom: 3rem;
  align-items: baseline;
}
.section-tag {
  font-family: var(--mono);
  font-size: 11px;
  letter-spacing: 0.32em;
  text-transform: uppercase;
  color: var(--gold);
}
.section-title {
  font-family: var(--display);
  font-weight: 300;
  font-size: clamp(1.8rem, 3.5vw, 2.75rem);
  line-height: 1.05;
  letter-spacing: -0.015em;
  color: var(--ivory);
  margin: 0;
}
.section-title em { font-style: italic; color: var(--gold); }

/* ── KPI Tiles ───────────────────────────────────────────────────── */
.tiles {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 0;
  border-top: 1px solid var(--rule);
  border-left: 1px solid var(--rule);
  margin: 0 0 3rem;
}
.tile {
  padding: 1.75rem 1.5rem 1.5rem;
  border-right: 1px solid var(--rule);
  border-bottom: 1px solid var(--rule);
  position: relative;
  transition: background 200ms ease;
  background: var(--surface);
}
.tile:hover { background: var(--surface-2); }
.tile-label {
  font-family: var(--mono);
  font-size: 10px;
  letter-spacing: 0.24em;
  text-transform: uppercase;
  color: var(--bone);
  margin: 0 0 1.25rem;
}
.tile-value {
  font-family: var(--display);
  font-weight: 300;
  font-size: 2.4rem;
  line-height: 1;
  letter-spacing: -0.02em;
  color: var(--ivory);
  margin: 0 0 0.5rem;
  font-variant-numeric: tabular-nums;
}
.tile-sub {
  font-family: var(--mono);
  font-size: 11px;
  letter-spacing: 0.04em;
  color: var(--muted);
  margin-top: 0.5rem;
}
.delta {
  display: inline-flex;
  align-items: center;
  gap: 0.35rem;
  font-family: var(--mono);
  font-size: 11px;
  font-weight: 500;
  letter-spacing: 0.04em;
  padding: 0.15rem 0;
}
.delta.up { color: var(--positive); }
.delta.down { color: var(--negative); }
.delta.flat { color: var(--muted); }

/* ── Data table ──────────────────────────────────────────────────── */
.t {
  width: 100%;
  border-collapse: collapse;
  font-family: var(--mono);
  font-size: 13px;
  margin: 0 0 2rem;
}
.t thead th {
  font-family: var(--mono);
  font-weight: 500;
  font-size: 10px;
  letter-spacing: 0.24em;
  text-transform: uppercase;
  color: var(--bone);
  text-align: right;
  padding: 1rem 1rem;
  border-bottom: 1px solid var(--gold-dim);
}
.t thead th:first-child { text-align: left; color: var(--gold); }
.t tbody td {
  padding: 0.85rem 1rem;
  border-bottom: 1px solid var(--rule-soft);
  text-align: right;
  color: var(--ivory);
  font-variant-numeric: tabular-nums;
  font-weight: 400;
}
.t tbody td:first-child {
  text-align: left;
  font-family: var(--body);
  font-style: italic;
  font-weight: 400;
  font-size: 15px;
  color: var(--bone);
  letter-spacing: 0.01em;
}
.t tbody tr:hover td { background: var(--surface); }
.t tbody tr:last-child td { border-bottom: none; }
.t .pos { color: var(--positive); }
.t .neg { color: var(--negative); }

.minicaps {
  font-family: var(--mono);
  font-size: 10px;
  letter-spacing: 0.24em;
  text-transform: uppercase;
  color: var(--gold);
  margin: 2.5rem 0 1rem;
}

/* ── Two-column ─────────────────────────────────────────────────── */
.two-col {
  display: grid;
  grid-template-columns: 1.2fr 1fr;
  gap: 3rem;
  margin: 0 0 3rem;
}
@media (max-width: 900px) {
  .two-col { grid-template-columns: 1fr; }
  .section-head { grid-template-columns: 1fr; gap: 1rem; }
}

/* ── EBITDA hero ────────────────────────────────────────────────── */
.ebitda-hero {
  display: grid;
  grid-template-columns: 1.4fr 1fr;
  gap: 4rem;
  align-items: center;
  padding: 3rem 0;
  border-top: 1px solid var(--gold-dim);
  border-bottom: 1px solid var(--gold-dim);
}
.ebitda-num {
  font-family: var(--display);
  font-weight: 300;
  font-size: clamp(4rem, 9vw, 7rem);
  line-height: 0.92;
  color: var(--gold);
  font-variant-numeric: tabular-nums;
  letter-spacing: -0.03em;
  margin: 0;
}
.ebitda-label {
  font-family: var(--mono);
  font-size: 11px;
  letter-spacing: 0.32em;
  text-transform: uppercase;
  color: var(--bone);
  margin: 0 0 0.5rem;
}
.ebitda-margin {
  font-family: var(--display);
  font-style: italic;
  font-weight: 400;
  font-size: 1.5rem;
  color: var(--ivory);
  margin: 1.5rem 0 0;
}
.ebitda-side .delta { font-size: 14px; }

/* ── Staff leaderboard ──────────────────────────────────────────── */
.leader {
  display: grid;
  grid-template-columns: 50px 1fr auto;
  align-items: baseline;
  gap: 1.5rem;
  padding: 1.25rem 0;
  border-bottom: 1px solid var(--rule-soft);
}
.leader:last-child { border-bottom: none; }
.leader-rank {
  font-family: var(--display);
  font-style: italic;
  font-weight: 400;
  font-size: 1.75rem;
  color: var(--gold);
}
.leader-name {
  font-family: var(--body);
  font-weight: 400;
  font-size: 1.05rem;
  color: var(--ivory);
}
.leader-value {
  font-family: var(--mono);
  font-weight: 500;
  font-size: 14px;
  color: var(--ivory);
  font-variant-numeric: tabular-nums;
}
.leader-unit {
  font-family: var(--mono);
  font-size: 10px;
  letter-spacing: 0.16em;
  text-transform: uppercase;
  color: var(--muted);
  margin-left: 0.5rem;
}
.leader.top .leader-rank { color: var(--gold-bright); font-size: 2rem; }
.leader.top .leader-name { color: var(--gold); }

/* ── Chart bars ─────────────────────────────────────────────────── */
.bars {
  display: flex;
  align-items: flex-end;
  gap: 0.75rem;
  height: 240px;
  padding: 2rem 0 0;
  border-bottom: 1px solid var(--rule);
}
.bar-col {
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 0.75rem;
  height: 100%;
}
.bar-val {
  font-family: var(--mono);
  font-size: 11px;
  font-weight: 500;
  letter-spacing: 0.04em;
  color: var(--ivory);
  font-variant-numeric: tabular-nums;
}
.bar-shaft {
  width: 100%;
  background: linear-gradient(to top, var(--gold-dim), var(--gold));
  border-top: 2px solid var(--gold-bright);
  animation: rise 800ms cubic-bezier(0.22, 1, 0.36, 1) backwards;
}
@keyframes rise { from { height: 0 !important; } }
.bar-label {
  font-family: var(--mono);
  font-size: 10px;
  letter-spacing: 0.16em;
  text-transform: uppercase;
  color: var(--bone);
  margin-top: 0.5rem;
}

/* ── Horizontal bar list ────────────────────────────────────────── */
.hbars { margin: 0; padding: 0; list-style: none; }
.hbar {
  padding: 0.9rem 0;
  border-bottom: 1px solid var(--rule-soft);
}
.hbar:last-child { border-bottom: none; }
.hbar-head {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  margin-bottom: 0.5rem;
  gap: 1rem;
}
.hbar-name {
  font-family: var(--body);
  font-style: italic;
  font-size: 14px;
  color: var(--bone);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.hbar-value {
  font-family: var(--mono);
  font-weight: 500;
  font-size: 13px;
  color: var(--ivory);
  font-variant-numeric: tabular-nums;
  flex-shrink: 0;
}
.hbar-track {
  height: 3px;
  background: var(--rule);
  overflow: hidden;
}
.hbar-fill {
  height: 100%;
  background: linear-gradient(to right, var(--gold-dim), var(--gold));
  animation: extend 800ms cubic-bezier(0.22, 1, 0.36, 1) backwards;
}
@keyframes extend { from { width: 0 !important; } }

/* ── Donut ──────────────────────────────────────────────────────── */
.donut-wrap { display: flex; align-items: center; gap: 2.5rem; }
.donut { width: 200px; height: 200px; flex-shrink: 0; transform: rotate(-90deg); }
.donut-center {
  position: relative;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
}
.donut-legend { margin: 0; padding: 0; list-style: none; flex: 1; }
.donut-legend li {
  display: grid;
  grid-template-columns: 14px 1fr auto auto;
  align-items: baseline;
  gap: 0.75rem;
  padding: 0.6rem 0;
  border-bottom: 1px solid var(--rule-soft);
}
.donut-legend .swatch { width: 14px; height: 14px; }
.donut-legend .name {
  font-family: var(--body);
  font-style: italic;
  font-size: 15px;
  color: var(--ivory);
}
.donut-legend .amt {
  font-family: var(--mono);
  font-size: 12px;
  color: var(--bone);
  font-variant-numeric: tabular-nums;
}
.donut-legend .pct {
  font-family: var(--mono);
  font-size: 12px;
  font-weight: 500;
  color: var(--gold);
  font-variant-numeric: tabular-nums;
  margin-left: 0.5rem;
}

/* ── Warnings ───────────────────────────────────────────────────── */
.warn {
  padding: 1.5rem 1.75rem;
  border-left: 2px solid var(--gold);
  background: rgba(212, 165, 116, 0.04);
  margin: 2rem 0;
}
.warn-tag {
  font-family: var(--mono);
  font-size: 10px;
  letter-spacing: 0.32em;
  text-transform: uppercase;
  color: var(--gold);
  margin: 0 0 0.5rem;
}
.warn-text {
  font-family: var(--body);
  font-style: italic;
  color: var(--ivory);
  margin: 0;
  font-size: 15px;
}

/* ── Forward look (editorial) ───────────────────────────────────── */
.forward { margin: 0; padding: 0; list-style: none; }
.forward li {
  display: grid;
  grid-template-columns: 60px 1fr;
  gap: 2rem;
  padding: 2rem 0;
  border-bottom: 1px solid var(--rule-soft);
}
.forward li:last-child { border-bottom: none; }
.forward .roman {
  font-family: var(--display);
  font-style: italic;
  font-weight: 400;
  font-size: 1.5rem;
  color: var(--gold);
  letter-spacing: 0.02em;
}
.forward .text {
  font-family: var(--body);
  font-weight: 300;
  font-size: 1.15rem;
  line-height: 1.55;
  color: var(--ivory);
}

/* ── Footer ─────────────────────────────────────────────────────── */
footer.colophon {
  max-width: 1200px;
  margin: 0 auto;
  padding: 4rem 3rem 5rem;
  border-top: 1px solid var(--rule);
  font-family: var(--mono);
  font-size: 11px;
  letter-spacing: 0.16em;
  text-transform: uppercase;
  color: var(--muted);
  display: flex;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 1rem;
}
footer.colophon strong { color: var(--gold); font-weight: 500; }

/* ── Staggered entrance ─────────────────────────────────────────── */
section.report { animation: fadein 600ms ease-out backwards; }
section.report:nth-of-type(1) { animation-delay: 80ms; }
section.report:nth-of-type(2) { animation-delay: 160ms; }
section.report:nth-of-type(3) { animation-delay: 240ms; }
section.report:nth-of-type(4) { animation-delay: 320ms; }
section.report:nth-of-type(5) { animation-delay: 400ms; }
section.report:nth-of-type(6) { animation-delay: 480ms; }
section.report:nth-of-type(7) { animation-delay: 560ms; }
.hero { animation: fadein 700ms ease-out; }
@keyframes fadein {
  from { opacity: 0; transform: translateY(20px); }
  to { opacity: 1; transform: translateY(0); }
}

/* ── Print ──────────────────────────────────────────────────────── */
@media print {
  body { background: white; color: black; }
  .hero, section.report, footer.colophon { animation: none; }
}
</style>"""


# ── helpers ──────────────────────────────────────────────────────────

def _fmt_money(v: float, dec: int = 0) -> str:
    sign = "-" if v < 0 else ""
    return f"{sign}${abs(v):,.{dec}f}"


def _fmt_pct(v: Optional[float]) -> str:
    if v is None:
        return "n/a"
    return f"{v:+.1f}%"


def _delta_html(current: float, prior: float, label: str = "vs") -> str:
    pct = compute_pct_change(current, prior)
    if pct is None:
        return f'<span class="delta flat">— {label}</span>'
    cls = "up" if pct > 0 else ("down" if pct < 0 else "flat")
    arrow = "↑" if pct > 0 else ("↓" if pct < 0 else "→")
    return f'<span class="delta {cls}">{arrow} {pct:+.1f}% {label}</span>'


def _delta_cell(current: float, prior: float) -> str:
    pct = compute_pct_change(current, prior)
    if pct is None:
        return '<td class="muted">—</td>'
    cls = "pos" if pct > 0 else ("neg" if pct < 0 else "")
    return f'<td class="{cls}">{pct:+.1f}%</td>'


# ── hero / footer ────────────────────────────────────────────────────

def _hero_html(data: Q1ReportData) -> str:
    rev = data.revenue.q1_2026.gross_revenue
    prior_rev = data.revenue.q1_2025.gross_revenue
    yoy = compute_pct_change(rev, prior_rev)
    yoy_word = "growth" if (yoy or 0) > 0 else ("decline" if (yoy or 0) < 0 else "movement")
    dek = (
        f"Quarterly revenue of {_fmt_money(rev)} represents a "
        f"{abs(yoy or 0):.1f}% {yoy_word} year-over-year, "
        f"with EBITDA of {_fmt_money(data.profitability.q1_2026.ebitda)} "
        f"({data.profitability.ebitda_margin_q1_2026:.1f}% margin) "
        f"reflecting operational discipline through the quarter."
    )
    return f"""
<header class="hero">
  <p class="eyebrow">Q1 · 2026 · Leadership Review</p>
  <p class="venue">LOV3 — Houston</p>
  <h1 class="title">Quarterly <em>Financial</em><br>Review</h1>
  <p class="dek">{html.escape(dek)}</p>
  <div class="hero-meta">
    <div>Period<strong>Jan 1 – Mar 31, 2026</strong></div>
    <div>Comparison<strong>Q4 2025 · Q1 2025</strong></div>
    <div>Generated<strong>{html.escape(data.generated_at)}</strong></div>
    <div>Source<strong>Toast POS · BofA · Predictive Insights</strong></div>
  </div>
</header>
"""


def _footer_html(data: Q1ReportData) -> str:
    return f"""
<footer class="colophon">
  <div>LOV3 / Houston · Quarterly Financial Review</div>
  <div>Data sourced from BigQuery · <strong>toast_raw</strong></div>
  <div>Generated {html.escape(data.generated_at)}</div>
</footer>
"""


# ── sections ─────────────────────────────────────────────────────────

def _section_revenue(rev: RevenueSection) -> str:
    q1, q4, prior = rev.q1_2026, rev.q4_2025, rev.q1_2025

    tiles = f"""
    <div class="tiles">
      <div class="tile">
        <p class="tile-label">Gross Revenue</p>
        <p class="tile-value">{_fmt_money(q1.gross_revenue)}</p>
        {_delta_html(q1.gross_revenue, prior.gross_revenue, "yoy")}
      </div>
      <div class="tile">
        <p class="tile-label">POS Net Sales</p>
        <p class="tile-value">{_fmt_money(q1.pos_revenue)}</p>
        {_delta_html(q1.pos_revenue, prior.pos_revenue, "yoy")}
      </div>
      <div class="tile">
        <p class="tile-label">Service Charge · 20%</p>
        <p class="tile-value">{_fmt_money(q1.service_charge)}</p>
        {_delta_html(q1.service_charge, q4.service_charge, "qoq")}
      </div>
      <div class="tile">
        <p class="tile-label">Hookah · Total</p>
        <p class="tile-value">{_fmt_money(q1.hookah_pos + q1.hookah_bank + q1.hookah_reclass)}</p>
        <p class="tile-sub">Bank {_fmt_money(q1.hookah_bank)} · Reclass {_fmt_money(q1.hookah_reclass)}</p>
      </div>
    </div>
    """

    months = list(rev.monthly.items())
    max_m = max((m.gross_revenue for _, m in months), default=1)
    bars = "".join(
        f"""<div class="bar-col">
          <div class="bar-val">{_fmt_money(m.gross_revenue)}</div>
          <div class="bar-shaft" style="height:{(m.gross_revenue / max_m * 100):.1f}%"></div>
          <div class="bar-label">{ym}</div>
        </div>"""
        for ym, m in months
    )

    total_mix = sum(rev.category_mix.values()) or 1
    sorted_mix = sorted(rev.category_mix.items(), key=lambda x: -x[1])
    donut_segments = ""
    legend_items = ""
    swatches = ["var(--gold)", "var(--ivory)", "var(--gold-dim)", "var(--bone)"]
    offset = 0.0
    circumference = 2 * 3.14159 * 70
    for i, (cat, amt) in enumerate(sorted_mix[:4]):
        pct = amt / total_mix
        color = swatches[i % len(swatches)]
        dash = pct * circumference
        donut_segments += (
            f'<circle r="70" cx="100" cy="100" fill="transparent" '
            f'stroke="{color}" stroke-width="22" '
            f'stroke-dasharray="{dash:.2f} {circumference - dash:.2f}" '
            f'stroke-dashoffset="-{offset:.2f}"></circle>'
        )
        offset += dash
        legend_items += (
            f'<li><span class="swatch" style="background:{color}"></span>'
            f'<span class="name">{html.escape(cat)}</span>'
            f'<span class="amt">{_fmt_money(amt)}</span>'
            f'<span class="pct">{pct*100:.1f}%</span></li>'
        )

    table_rows = ""
    for label, attr in [
        ("Gross Revenue", "gross_revenue"),
        ("POS Net Sales", "pos_revenue"),
        ("Service Charge", "service_charge"),
        ("Voluntary Tips", "voluntary_tips"),
        ("Hookah · POS", "hookah_pos"),
        ("Hookah · Bank Deposits", "hookah_bank"),
        ("Hookah · Reclass", "hookah_reclass"),
    ]:
        cur = getattr(q1, attr)
        q4v = getattr(q4, attr)
        priorv = getattr(prior, attr)
        table_rows += (
            f"<tr><td>{label}</td>"
            f"<td>{_fmt_money(cur)}</td>"
            f"<td>{_fmt_money(q4v)}</td>"
            f"{_delta_cell(cur, q4v)}"
            f"<td>{_fmt_money(priorv)}</td>"
            f"{_delta_cell(cur, priorv)}</tr>"
        )

    return f"""
<section class="report">
  <div class="section-head">
    <p class="section-tag">A — Revenue</p>
    <h2 class="section-title">A quarter of <em>compositional</em> revenue —<br>POS, service charge, and hookah deposits.</h2>
  </div>
  {tiles}
  <div class="two-col">
    <div>
      <p class="minicaps">Monthly Trend</p>
      <div class="bars">{bars}</div>
    </div>
    <div>
      <p class="minicaps">Sales Mix · Q1 2026</p>
      <div class="donut-wrap">
        <svg class="donut" viewBox="0 0 200 200">{donut_segments}</svg>
        <ul class="donut-legend">{legend_items}</ul>
      </div>
    </div>
  </div>
  <p class="minicaps">Full Comparison</p>
  <table class="t">
    <thead><tr>
      <th>Metric</th><th>Q1 2026</th><th>Q4 2025</th><th>QoQ</th><th>Q1 2025</th><th>YoY</th>
    </tr></thead>
    <tbody>{table_rows}</tbody>
  </table>
</section>
"""


def _section_costs(costs: CostSection, rev: RevenueSection) -> str:
    q1, q4, prior = costs.q1_2026, costs.q4_2025, costs.q1_2025

    tiles = f"""
    <div class="tiles">
      <div class="tile">
        <p class="tile-label">Cost of Goods Sold</p>
        <p class="tile-value">{_fmt_money(q1.cogs)}</p>
        {_delta_html(q1.cogs, prior.cogs, "yoy")}
      </div>
      <div class="tile">
        <p class="tile-label">Labor</p>
        <p class="tile-value">{_fmt_money(q1.labor)}</p>
        <p class="tile-sub">{costs.labor_pct_revenue:.1f}% of revenue</p>
      </div>
      <div class="tile">
        <p class="tile-label">Operating Expenses</p>
        <p class="tile-value">{_fmt_money(q1.opex)}</p>
        {_delta_html(q1.opex, prior.opex, "yoy")}
      </div>
      <div class="tile">
        <p class="tile-label">Total Costs</p>
        <p class="tile-value">{_fmt_money(q1.cogs + q1.labor + q1.opex)}</p>
        {_delta_html(q1.cogs + q1.labor + q1.opex, prior.cogs + prior.labor + prior.opex, "yoy")}
      </div>
    </div>
    """

    top_opex = sorted(costs.opex_by_category.items(), key=lambda x: -x[1])[:10]
    max_o = top_opex[0][1] if top_opex else 1
    hbars = "".join(
        f"""<li class="hbar">
          <div class="hbar-head">
            <span class="hbar-name">{html.escape(cat.split('/')[-1] if '/' in cat else cat)}</span>
            <span class="hbar-value">{_fmt_money(amt)}</span>
          </div>
          <div class="hbar-track"><div class="hbar-fill" style="width:{(amt/max_o)*100:.1f}%"></div></div>
        </li>"""
        for cat, amt in top_opex
    )

    return f"""
<section class="report">
  <div class="section-head">
    <p class="section-tag">B — Cost Structure</p>
    <h2 class="section-title">Where the <em>money goes</em> —<br>three buckets, ten line items.</h2>
  </div>
  {tiles}
  <p class="minicaps">Top 10 Operating Expense Lines · Q1 2026</p>
  <ul class="hbars">{hbars}</ul>
</section>
"""


def _section_profitability(p: ProfitabilitySection) -> str:
    q1, q4, prior = p.q1_2026, p.q4_2025, p.q1_2025

    return f"""
<section class="report">
  <div class="section-head">
    <p class="section-tag">C — Profitability</p>
    <h2 class="section-title">EBITDA <em>recovery</em> —<br>sequential and year-over-year.</h2>
  </div>
  <div class="ebitda-hero">
    <div>
      <p class="ebitda-label">EBITDA · Q1 2026</p>
      <p class="ebitda-num">{_fmt_money(q1.ebitda)}</p>
      <p class="ebitda-margin">{p.ebitda_margin_q1_2026:.1f}% margin on gross revenue</p>
    </div>
    <div class="ebitda-side">
      <table class="t">
        <thead><tr><th>Period</th><th>EBITDA</th><th>Margin</th></tr></thead>
        <tbody>
          <tr><td>Q1 2026</td><td>{_fmt_money(q1.ebitda)}</td><td>{p.ebitda_margin_q1_2026:.1f}%</td></tr>
          <tr><td>Q4 2025</td><td>{_fmt_money(q4.ebitda)}</td><td>{p.ebitda_margin_q4_2025:.1f}%</td></tr>
          <tr><td>Q1 2025</td><td>{_fmt_money(prior.ebitda)}</td><td>{p.ebitda_margin_q1_2025:.1f}%</td></tr>
        </tbody>
      </table>
      <div style="margin-top:1.5rem;display:flex;gap:1.5rem;flex-wrap:wrap;">
        {_delta_html(q1.ebitda, q4.ebitda, "qoq")}
        {_delta_html(q1.ebitda, prior.ebitda, "yoy")}
      </div>
    </div>
  </div>
</section>
"""


def _section_kpis(k: KPISection) -> str:
    q1, q4, prior = k.q1_2026, k.q4_2025, k.q1_2025

    tiles = f"""
    <div class="tiles">
      <div class="tile">
        <p class="tile-label">Covers</p>
        <p class="tile-value">{q1.covers:,}</p>
        {_delta_html(q1.covers, prior.covers, "yoy")}
      </div>
      <div class="tile">
        <p class="tile-label">Average Check</p>
        <p class="tile-value">${q1.avg_check:,.2f}</p>
        {_delta_html(q1.avg_check, prior.avg_check, "yoy")}
      </div>
      <div class="tile">
        <p class="tile-label">Business Days</p>
        <p class="tile-value">{q1.business_days}</p>
        <p class="tile-sub">vs {prior.business_days} a year ago</p>
      </div>
      <div class="tile">
        <p class="tile-label">Labor Hours</p>
        <p class="tile-value">{q1.labor_hours:,.0f}</p>
        {_delta_html(q1.labor_hours, prior.labor_hours, "yoy")}
      </div>
    </div>
    <div class="tiles">
      <div class="tile">
        <p class="tile-label">Revenue / Business Day</p>
        <p class="tile-value">${k.revenue_per_business_day_q1:,.0f}</p>
      </div>
      <div class="tile">
        <p class="tile-label">Revenue / Labor Hour</p>
        <p class="tile-value">${k.revenue_per_labor_hour_q1:,.2f}</p>
      </div>
    </div>
    """

    return f"""
<section class="report">
  <div class="section-head">
    <p class="section-tag">D — Operations</p>
    <h2 class="section-title">The <em>throughput</em> story —<br>covers, checks, hours, days.</h2>
  </div>
  {tiles}
</section>
"""


def _section_staff(s: StaffSection) -> str:
    def _row(rank: int, name: str, value: str, unit: str) -> str:
        cls = "leader top" if rank == 1 else "leader"
        roman = ["I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X"][rank - 1]
        return (
            f'<div class="{cls}">'
            f'<span class="leader-rank">{roman}</span>'
            f'<span class="leader-name">{html.escape(name)}</span>'
            f'<span><span class="leader-value">{value}</span><span class="leader-unit">{unit}</span></span>'
            f"</div>"
        )

    bartenders = "".join(
        _row(i + 1, b.name, f"{int(b.attributed_revenue):,}", "items")
        for i, b in enumerate(s.top_bartenders)
    ) or '<p style="color:var(--muted);font-style:italic;">No bartender data available.</p>'

    servers = "".join(
        _row(i + 1, srv.name, _fmt_money(srv.attributed_revenue), "rev")
        for i, srv in enumerate(s.top_servers)
    ) or '<p style="color:var(--muted);font-style:italic;">No server data available.</p>'

    return f"""
<section class="report">
  <div class="section-head">
    <p class="section-tag">E — Staff</p>
    <h2 class="section-title">Hand on the <em>service well</em>, name on the check.</h2>
  </div>
  <div class="two-col">
    <div>
      <p class="minicaps">Top Bartenders · Items Fulfilled (KitchenTimings)</p>
      {bartenders}
    </div>
    <div>
      <p class="minicaps">Top Servers · Check Revenue</p>
      {servers}
    </div>
  </div>
</section>
"""


def _section_cashflow(cf: CashFlowSection) -> str:
    deposits_pct = safe_div(cf.total_deposits, max(cf.total_deposits, cf.total_expenses)) * 100
    expenses_pct = safe_div(cf.total_expenses, max(cf.total_deposits, cf.total_expenses)) * 100

    flow = f"""
    <div class="tiles">
      <div class="tile">
        <p class="tile-label">Total Deposits</p>
        <p class="tile-value">{_fmt_money(cf.total_deposits)}</p>
      </div>
      <div class="tile">
        <p class="tile-label">Total Expenses</p>
        <p class="tile-value">{_fmt_money(cf.total_expenses)}</p>
      </div>
      <div class="tile">
        <p class="tile-label">Net Cash Movement</p>
        <p class="tile-value">{_fmt_money(cf.total_deposits - cf.total_expenses)}</p>
      </div>
    </div>
    """

    vendors = sorted(cf.top_vendors, key=lambda v: -v.spend)[:10]
    max_v = vendors[0].spend if vendors else 1
    vendor_bars = "".join(
        f"""<li class="hbar">
          <div class="hbar-head">
            <span class="hbar-name">{html.escape(v.name)}</span>
            <span class="hbar-value">{_fmt_money(v.spend)} · {v.pct_of_opex*100:.1f}%</span>
          </div>
          <div class="hbar-track"><div class="hbar-fill" style="width:{(v.spend/max_v)*100:.1f}%"></div></div>
        </li>"""
        for v in vendors
    )

    warnings_html = ""
    if cf.concentration_warnings:
        warnings_html = "".join(
            f'<div class="warn"><p class="warn-tag">Concentration · Warning</p><p class="warn-text">{html.escape(w)}</p></div>'
            for w in cf.concentration_warnings
        )

    return f"""
<section class="report">
  <div class="section-head">
    <p class="section-tag">F — Cash &amp; Vendors</p>
    <h2 class="section-title">Money <em>in</em>, money <em>out</em>,<br>and who's getting most of it.</h2>
  </div>
  {flow}
  {warnings_html}
  <p class="minicaps">Top 10 Vendors · Q1 2026</p>
  <ul class="hbars">{vendor_bars}</ul>
</section>
"""


def _section_forward(f: ForwardLookSection) -> str:
    romans = ["I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X"]
    items = "".join(
        f'<li><span class="roman">{romans[i] if i < len(romans) else str(i+1)}</span>'
        f'<span class="text">{html.escape(b)}</span></li>'
        for i, b in enumerate(f.bullets)
    )
    return f"""
<section class="report">
  <div class="section-head">
    <p class="section-tag">G — Forward Look</p>
    <h2 class="section-title">What <em>next</em>.</h2>
  </div>
  <ol class="forward">{items}</ol>
</section>
"""
