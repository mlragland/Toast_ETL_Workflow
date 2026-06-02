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
        """Render the full report as a standalone HTML page.

        Strategy: reuse render_markdown to build the body content, then wrap
        in minimal HTML scaffolding with basic CSS. Tables become real tables
        through a simple markdown-table -> HTML-table converter inline.
        """
        md = self.render_markdown(data)
        body = self._markdown_to_html(md)
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>LOV3|HTX Q1 2026 Financial Analysis</title>
<style>
  body {{ font-family: -apple-system, system-ui, sans-serif; max-width: 1100px;
         margin: 2em auto; padding: 0 1em; color: #222; line-height: 1.5; }}
  h1 {{ border-bottom: 3px solid #333; padding-bottom: 0.3em; }}
  h2 {{ margin-top: 2em; border-bottom: 1px solid #ccc; padding-bottom: 0.2em; }}
  table {{ border-collapse: collapse; margin: 1em 0; }}
  th, td {{ border: 1px solid #ddd; padding: 0.4em 0.8em; text-align: left; }}
  th {{ background: #f4f4f4; }}
  td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  .warn {{ background: #fff8dc; padding: 0.5em; border-left: 4px solid #d4a017; }}
  .meta {{ color: #666; font-size: 0.9em; }}
  ul li {{ margin: 0.3em 0; }}
</style>
</head>
<body>
{body}
<hr>
<p class="meta">Generated by Q1ReportGenerator. Data sourced from BigQuery toast-analytics-444116.toast_raw.</p>
</body>
</html>
"""

    def _markdown_to_html(self, md: str) -> str:
        """Minimal markdown -> HTML converter for the subset used by render_markdown."""
        out_lines: List[str] = []
        in_table = False
        table_rows: List[List[str]] = []
        in_list = False

        def flush_table():
            nonlocal table_rows
            if not table_rows:
                return ""
            header = table_rows[0]
            # row 2 is the alignment line in our format ("---") — skip it
            body_rows = table_rows[2:] if len(table_rows) > 2 else []
            out = ["<table>", "<thead><tr>"]
            for h in header:
                out.append(f"<th>{html.escape(h.strip())}</th>")
            out.append("</tr></thead><tbody>")
            for r in body_rows:
                out.append("<tr>")
                for cell in r:
                    cell_s = cell.strip()
                    css = ' class="num"' if cell_s.startswith("$") or cell_s.endswith("%") else ""
                    out.append(f"<td{css}>{html.escape(cell_s)}</td>")
                out.append("</tr>")
            out.append("</tbody></table>")
            table_rows = []
            return "\n".join(out)

        for line in md.splitlines():
            if line.startswith("| "):
                in_table = True
                cells = [c for c in line.strip().strip("|").split("|")]
                table_rows.append(cells)
                continue
            if in_table:
                out_lines.append(flush_table())
                in_table = False

            if line.startswith("# "):
                out_lines.append(f"<h1>{html.escape(line[2:])}</h1>")
            elif line.startswith("## "):
                out_lines.append(f"<h2>{html.escape(line[3:])}</h2>")
            elif line.startswith("- "):
                if not in_list:
                    out_lines.append("<ul>")
                    in_list = True
                out_lines.append(f"<li>{html.escape(line[2:])}</li>")
            else:
                if in_list:
                    out_lines.append("</ul>")
                    in_list = False
                if line.startswith("**") and line.endswith("**"):
                    out_lines.append(f"<p><strong>{html.escape(line[2:-2])}</strong></p>")
                elif line.strip():
                    out_lines.append(f"<p>{html.escape(line)}</p>")
        if in_table:
            out_lines.append(flush_table())
        if in_list:
            out_lines.append("</ul>")
        return "\n".join(out_lines)

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
