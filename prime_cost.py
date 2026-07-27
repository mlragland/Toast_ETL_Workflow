"""Prime Cost dashboard — the #1 hospitality KPI.

Computes (Food COGS + Liquor COGS + Labor) / Gross Revenue on a monthly
rolling basis using the SAME data sources already wired into
sba_financial_statements.py. No hardcoded dates — the report auto-extends
to future months as new data arrives.

Prime Cost is the industry-standard operator health metric. Targets:
    <55%   Excellent
    55-60% Good
    60-65% Elevated
    >65%   Investigate

Labor bucket includes tip pass-through per SBA methodology
(`3. Labor Cost (Includes Grat + Tips)`). We surface both the raw Prime %
AND a "Real Prime %" that estimates labor net of tip pass-through.
"""

from __future__ import annotations

import html
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

from google.cloud import bigquery

from config import ALERT_WEBHOOK_URL, PROJECT_ID, DATASET_ID

logger = logging.getLogger(__name__)

# Same thresholds Danny Meyer / USHG operators use publicly.
EXCELLENT_THRESHOLD = 55.0
GOOD_THRESHOLD = 60.0
ELEVATED_THRESHOLD = 65.0

# Alert threshold for the automated Slack notification (Sprint follow-up).
ALERT_PRIME_PCT = 62.0

# Cost bucket classifiers — match the BankCategoryRules `{N}. {Section}/...` hierarchy.
_COGS_LIQUOR_KEYWORDS = ("liquor cogs",)
_COGS_FOOD_KEYWORDS = ("food cogs",)


@dataclass
class PrimeCostMonth:
    """One month's Prime Cost breakdown."""
    month: str  # 'YYYY-MM'
    gross_revenue: float = 0.0
    net_sales: float = 0.0
    gratuity: float = 0.0
    tips: float = 0.0
    hookah_reclass: float = 0.0
    hookah_bank: float = 0.0

    liquor_cogs: float = 0.0
    food_cogs: float = 0.0
    labor_total: float = 0.0  # Includes tip/grat pass-through per SBA
    labor_ex_tips: float = 0.0  # Estimated "real" labor (labor - grat - tips)

    @property
    def cogs_total(self) -> float:
        return self.liquor_cogs + self.food_cogs

    @property
    def prime_cost(self) -> float:
        return self.cogs_total + self.labor_total

    @property
    def prime_cost_real(self) -> float:
        return self.cogs_total + self.labor_ex_tips

    @property
    def prime_pct(self) -> float:
        return 100.0 * self.prime_cost / self.gross_revenue if self.gross_revenue > 0 else 0.0

    @property
    def prime_pct_real(self) -> float:
        return 100.0 * self.prime_cost_real / self.gross_revenue if self.gross_revenue > 0 else 0.0

    @property
    def liquor_cogs_pct(self) -> float:
        return 100.0 * self.liquor_cogs / self.gross_revenue if self.gross_revenue > 0 else 0.0

    @property
    def food_cogs_pct(self) -> float:
        return 100.0 * self.food_cogs / self.gross_revenue if self.gross_revenue > 0 else 0.0

    @property
    def labor_pct(self) -> float:
        return 100.0 * self.labor_total / self.gross_revenue if self.gross_revenue > 0 else 0.0

    def grade(self) -> Tuple[str, str]:
        """(label, css_class) for the Prime %."""
        p = self.prime_pct
        if p < EXCELLENT_THRESHOLD:
            return ("Excellent", "grade-excellent")
        if p < GOOD_THRESHOLD:
            return ("Good", "grade-good")
        if p < ELEVATED_THRESHOLD:
            return ("Elevated", "grade-warn")
        return ("Investigate", "grade-alert")


# ── Month range generation ───────────────────────────────────────────

def trailing_month_range(months_back: int = 12,
                          today: Optional[date] = None) -> List[Tuple[str, str, str]]:
    """Return [(label, start_iso, end_iso), ...] for the trailing N complete months.

    The current (partial) month is EXCLUDED so month-over-month comparisons stay
    apples-to-apples. To include the current month-to-date use `current_month_partial`.
    """
    today = today or date.today()
    # Start from the last day of the previous month
    end_month_year = today.year
    end_month = today.month - 1
    if end_month == 0:
        end_month = 12
        end_month_year -= 1

    out: List[Tuple[str, str, str]] = []
    for i in range(months_back):
        m = end_month - i
        y = end_month_year
        while m <= 0:
            m += 12
            y -= 1
        first = date(y, m, 1)
        # Last day of month
        if m == 12:
            next_first = date(y + 1, 1, 1)
        else:
            next_first = date(y, m + 1, 1)
        last = next_first - timedelta(days=1)
        out.append((f"{y:04d}-{m:02d}", first.isoformat(), last.isoformat()))

    out.reverse()
    return out


def current_month_partial(today: Optional[date] = None) -> Tuple[str, str, str]:
    """(label, first-of-month, today) — the partial current month."""
    today = today or date.today()
    first = date(today.year, today.month, 1)
    return (f"{today.year:04d}-{today.month:02d}", first.isoformat(), today.isoformat())


def rolling_30_day_window(today: Optional[date] = None) -> Tuple[str, str, str]:
    """('30d', start, end) for the trailing 30 days ending yesterday."""
    today = today or date.today()
    end = today - timedelta(days=1)
    start = end - timedelta(days=29)
    return ("trailing_30d", start.isoformat(), end.isoformat())


# ── Cost classifiers ────────────────────────────────────────────────

def classify_expense_bucket(cat: str) -> Optional[str]:
    """Return 'liquor_cogs', 'food_cogs', 'labor', or None."""
    if not cat:
        return None
    c = cat.lower()
    if c.startswith("2."):
        if any(kw in c for kw in _COGS_LIQUOR_KEYWORDS):
            return "liquor_cogs"
        if any(kw in c for kw in _COGS_FOOD_KEYWORDS):
            return "food_cogs"
        return "other_cogs"  # non-food/liquor COGS (packaging, supplies) — excluded from prime
    if c.startswith("3.") or "labor cost" in c or "payroll" in c:
        return "labor"
    return None


# ── Data fetcher ────────────────────────────────────────────────────

class PrimeCostCalculator:
    """Compute Prime Cost from Toast POS + BofA data (reuses SBA pipeline)."""

    def __init__(self, bq_client: Optional[bigquery.Client] = None) -> None:
        self.bq = bq_client or bigquery.Client(project=PROJECT_ID)
        # Late-import to avoid the paramiko chain at module import time
        from sba_financial_statements import (
            HOOKAH_RECLASS,
            query_expenses_by_category,
            query_hookah_revenue_bank,
            query_monthly_revenue,
        )
        self._q_revenue = query_monthly_revenue
        self._q_expenses = query_expenses_by_category
        self._q_hookah_bank = query_hookah_revenue_bank
        self._hookah_reclass = HOOKAH_RECLASS

    def compute_month(self, label: str, start: str, end: str) -> PrimeCostMonth:
        """Compute a single period's Prime Cost bucket."""
        rev = self._q_revenue(self.bq, start, end)
        exp = self._q_expenses(self.bq, start, end)
        hookah_bank = self._q_hookah_bank(self.bq, start, end)

        # Aggregate revenue across the period (may span >1 month keys)
        net_sales = sum(m.get("net_sales", 0.0) for m in rev.values())
        gratuity = sum(m.get("gratuity", 0.0) for m in rev.values())
        tips = sum(m.get("tips", 0.0) for m in rev.values())
        hookah_bank_total = sum(hookah_bank.values())
        # Reclass applies to the specific YYYY-MM keys
        reclass = 0.0
        for k, amt in self._hookah_reclass.items():
            if k in rev or k == label:
                # Only apply if that YM overlaps this window
                y, m = k.split("-")
                first = f"{y}-{m}-01"
                if start <= first <= end:
                    reclass += amt

        gross_revenue = net_sales + gratuity + tips + hookah_bank_total + reclass

        # Aggregate costs
        liquor_cogs = 0.0
        food_cogs = 0.0
        labor_total = 0.0
        for _month, cats in exp.items():
            for cat_name, amt in cats.items():
                bucket = classify_expense_bucket(cat_name)
                if bucket == "liquor_cogs":
                    liquor_cogs += amt
                elif bucket == "food_cogs":
                    food_cogs += amt
                elif bucket == "labor":
                    labor_total += amt

        # Estimate real labor by subtracting the tip/grat pass-through.
        # This is a defensible approximation — actual split requires payroll-level data.
        labor_ex_tips = max(0.0, labor_total - (gratuity + tips))

        return PrimeCostMonth(
            month=label,
            gross_revenue=gross_revenue,
            net_sales=net_sales,
            gratuity=gratuity,
            tips=tips,
            hookah_reclass=reclass,
            hookah_bank=hookah_bank_total,
            liquor_cogs=liquor_cogs,
            food_cogs=food_cogs,
            labor_total=labor_total,
            labor_ex_tips=labor_ex_tips,
        )

    def compute_trailing_months(self, months_back: int = 12) -> List[PrimeCostMonth]:
        return [self.compute_month(*t) for t in trailing_month_range(months_back)]

    def compute_partial_current_month(self) -> PrimeCostMonth:
        return self.compute_month(*current_month_partial())

    def compute_rolling_30d(self) -> PrimeCostMonth:
        return self.compute_month(*rolling_30_day_window())


# ── Slack alert ─────────────────────────────────────────────────────

def build_slack_message(rolling_30d: PrimeCostMonth,
                        last_month: PrimeCostMonth,
                        trailing_avg_pct: float) -> Tuple[str, bool]:
    """Build the Slack alert message. Returns (msg, is_error)."""
    grade_label, _ = rolling_30d.grade()
    is_alert = rolling_30d.prime_pct >= ALERT_PRIME_PCT

    if is_alert:
        icon = "🔴" if rolling_30d.prime_pct >= ELEVATED_THRESHOLD else "🟡"
    else:
        icon = "✅"

    trend_arrow = "↗" if rolling_30d.prime_pct > trailing_avg_pct else "↘"

    msg = f"{icon} *LOV3 Prime Cost — {date.today().strftime('%a %b %-d, %Y')}*\n\n"
    msg += (
        f"*Rolling 30-day Prime Cost:* *{rolling_30d.prime_pct:.1f}%* ({grade_label})\n"
        f"• Real Prime % (ex-tip pass-through): {rolling_30d.prime_pct_real:.1f}%\n"
        f"• Trend {trend_arrow} vs 12-mo avg of {trailing_avg_pct:.1f}%\n\n"
        f"*Breakdown (last 30 days):*\n"
        f"• Gross Revenue: ${rolling_30d.gross_revenue:,.0f}\n"
        f"• Liquor COGS: ${rolling_30d.liquor_cogs:,.0f} ({rolling_30d.liquor_cogs_pct:.1f}%)\n"
        f"• Food COGS: ${rolling_30d.food_cogs:,.0f} ({rolling_30d.food_cogs_pct:.1f}%)\n"
        f"• Labor: ${rolling_30d.labor_total:,.0f} ({rolling_30d.labor_pct:.1f}%)\n\n"
        f"*Last complete month* ({last_month.month}): "
        f"Prime {last_month.prime_pct:.1f}% ({last_month.grade()[0]})\n"
    )

    if is_alert:
        msg += (
            f"\n⚠️ *Prime Cost above the {ALERT_PRIME_PCT:.0f}% alert threshold.* "
            f"Review controllable costs — most leverage sits in labor scheduling. "
            f"See /prime-cost for the full breakdown."
        )
    else:
        msg += "\nAll clear. See /prime-cost for the full 12-month trend."

    return msg, is_alert


def send_prime_cost_slack_report(calc: Optional["PrimeCostCalculator"] = None) -> Dict:
    """Compute the rolling Prime Cost and post a Slack summary.

    Reuses the existing AlertManager wiring (SLACK_WEBHOOK_URL env var).
    Called by Cloud Scheduler weekly + on demand via /api/prime-cost-alert.
    """
    from services import AlertManager

    calc = calc or PrimeCostCalculator()
    rolling_30d = calc.compute_rolling_30d()
    trailing = calc.compute_trailing_months(months_back=12)
    last_month = trailing[-1] if trailing else rolling_30d
    trailing_avg = (
        sum(m.prime_pct for m in trailing) / len(trailing) if trailing else 0.0
    )

    msg, is_error = build_slack_message(rolling_30d, last_month, trailing_avg)
    alert = AlertManager(slack_webhook=ALERT_WEBHOOK_URL)
    alert.send_slack_alert(msg, is_error=is_error)

    return {
        "status": "success",
        "prime_pct_30d": round(rolling_30d.prime_pct, 2),
        "prime_pct_real_30d": round(rolling_30d.prime_pct_real, 2),
        "trailing_avg_pct": round(trailing_avg, 2),
        "last_month": last_month.month,
        "last_month_prime_pct": round(last_month.prime_pct, 2),
        "alerted": is_error,
        "grade": rolling_30d.grade()[0],
    }


# ── HTML rendering ──────────────────────────────────────────────────

def _money(v: float) -> str:
    if v is None:
        return "—"
    sign = "-" if v < 0 else ""
    return f"{sign}${abs(v):,.0f}"


def _pct(v: float) -> str:
    return f"{v:.1f}%" if v else "—"


def render_html(rolling_30d: PrimeCostMonth,
                current_partial: PrimeCostMonth,
                trailing: List[PrimeCostMonth]) -> str:
    """Editorial-luxury dark UI, same shell as /q1-report."""
    from design_system import page_shell

    trailing_avg_pct = (
        sum(m.prime_pct for m in trailing) / len(trailing) if trailing else 0.0
    )
    trailing_avg_real = (
        sum(m.prime_pct_real for m in trailing) / len(trailing) if trailing else 0.0
    )

    def _row(m: PrimeCostMonth, highlight: bool = False) -> str:
        grade_label, grade_class = m.grade()
        row_class = "row-highlight" if highlight else ""
        return f"""
        <tr class="{row_class}">
          <td class="mono">{html.escape(m.month)}</td>
          <td class="num">{_money(m.gross_revenue)}</td>
          <td class="num">{_money(m.liquor_cogs)}</td>
          <td class="num">{_money(m.food_cogs)}</td>
          <td class="num">{_money(m.labor_total)}</td>
          <td class="num strong">{_money(m.prime_cost)}</td>
          <td class="num strong {grade_class}">{_pct(m.prime_pct)}</td>
          <td class="num">{_pct(m.prime_pct_real)}</td>
          <td><span class="badge {grade_class}">{grade_label}</span></td>
        </tr>"""

    headline_grade_label, headline_grade_class = rolling_30d.grade()

    trailing_rows = "".join(_row(m) for m in trailing)

    body = f"""
<div class="hero">
  <div class="eyebrow">LOV3 · Houston · Financial Discipline</div>
  <h1>Prime Cost</h1>
  <p class="lede">
    The industry's #1 operator KPI — Food + Liquor COGS + Labor as a share of gross revenue.
    Target under 60% for full-service hospitality.
  </p>
</div>

<div class="section">
  <h2>Where you sit today</h2>
  <div class="grid-3">
    <div class="stat-card {headline_grade_class}">
      <div class="stat-label">Trailing 30 days</div>
      <div class="stat-value">{_pct(rolling_30d.prime_pct)}</div>
      <div class="stat-sub">
        Real (ex-tip pass-through): {_pct(rolling_30d.prime_pct_real)}<br/>
        Grade: <strong>{headline_grade_label}</strong>
      </div>
    </div>

    <div class="stat-card">
      <div class="stat-label">Trailing-12-month avg</div>
      <div class="stat-value">{_pct(trailing_avg_pct)}</div>
      <div class="stat-sub">Real avg: {_pct(trailing_avg_real)}</div>
    </div>

    <div class="stat-card">
      <div class="stat-label">Current month (partial)</div>
      <div class="stat-value">{_pct(current_partial.prime_pct)}</div>
      <div class="stat-sub">{html.escape(current_partial.month)} · month-to-date</div>
    </div>
  </div>
</div>

<div class="section">
  <h2>Trailing 12 months</h2>
  <p class="section-note">
    Each row uses the SBA financial-statement components already wired into
    <code>sba_financial_statements.py</code>. Prime Cost includes the SBA labor bucket
    which contains gratuity + tip pass-through. "Real Prime %" estimates labor net of
    those pass-throughs to make LOV3 comparable to industry benchmarks (28-32% labor).
  </p>
  <table class="report-table">
    <thead>
      <tr>
        <th>Month</th>
        <th class="num">Gross Rev</th>
        <th class="num">Liquor COGS</th>
        <th class="num">Food COGS</th>
        <th class="num">Labor</th>
        <th class="num">Prime Cost $</th>
        <th class="num">Prime %</th>
        <th class="num">Real %</th>
        <th>Grade</th>
      </tr>
    </thead>
    <tbody>
      {trailing_rows}
    </tbody>
  </table>

  <div class="grade-legend">
    <span class="badge grade-excellent">Excellent &lt; {EXCELLENT_THRESHOLD:.0f}%</span>
    <span class="badge grade-good">Good {EXCELLENT_THRESHOLD:.0f}-{GOOD_THRESHOLD:.0f}%</span>
    <span class="badge grade-warn">Elevated {GOOD_THRESHOLD:.0f}-{ELEVATED_THRESHOLD:.0f}%</span>
    <span class="badge grade-alert">Investigate &gt; {ELEVATED_THRESHOLD:.0f}%</span>
  </div>
</div>

<div class="section">
  <h2>Anomaly flags</h2>
  <p class="section-note">
    Months with Prime % more than 5 points above the trailing average — worth a controller review.
  </p>
  <ul class="flag-list">
    {"".join(f'<li><strong>{html.escape(m.month)}</strong> · Prime <strong>{m.prime_pct:.1f}%</strong> ({m.prime_pct - trailing_avg_pct:+.1f} vs avg) — Labor ${m.labor_total:,.0f} on ${m.gross_revenue:,.0f} revenue</li>' for m in trailing if m.prime_pct > trailing_avg_pct + 5)}
  </ul>
</div>
"""

    extra_css = """
      .hero { padding: 4rem 0 2rem; }
      .eyebrow { text-transform: uppercase; letter-spacing: 0.28em;
                 color: var(--gold); font-family: var(--mono); font-size: 11px;
                 font-weight: 500; margin-bottom: 2rem; }
      .hero h1 { font-family: var(--display); font-size: clamp(3rem, 6vw, 5rem);
                 font-weight: 400; letter-spacing: -0.02em; margin: 0.25em 0; }
      .lede { color: var(--bone); font-size: 1.05rem; max-width: 640px; }
      .section { padding: 3rem 0; border-top: 1px solid var(--rule); }
      .section h2 { font-family: var(--display); font-size: 1.5rem; font-weight: 400;
                    letter-spacing: -0.01em; margin-bottom: 1.5rem; }
      .section-note { color: var(--bone); margin-bottom: 1.5rem; font-size: 0.95rem; }
      .section-note code { color: var(--gold); background: var(--surface); padding: 2px 6px;
                          border-radius: 4px; font-family: var(--mono); font-size: 0.85em; }
      .grid-3 { display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; }
      .stat-card { background: var(--surface); padding: 1.5rem; border-radius: 8px;
                   border: 1px solid var(--rule); }
      .stat-label { color: var(--muted); font-size: 0.85rem; text-transform: uppercase;
                    letter-spacing: 0.12em; margin-bottom: 0.5rem; }
      .stat-value { font-family: var(--display); font-size: 3rem; font-weight: 400;
                    letter-spacing: -0.02em; margin: 0.25rem 0; }
      .stat-sub { color: var(--bone); font-size: 0.9rem; }
      .report-table { width: 100%; border-collapse: collapse; font-family: var(--mono);
                      font-size: 0.9rem; }
      .report-table th { text-align: left; padding: 0.75rem; color: var(--muted);
                         font-weight: 500; font-size: 0.75rem; text-transform: uppercase;
                         letter-spacing: 0.1em; border-bottom: 1px solid var(--rule); }
      .report-table th.num, .report-table td.num { text-align: right; }
      .report-table td { padding: 0.75rem; border-bottom: 1px solid var(--rule-soft); }
      .report-table td.strong { font-weight: 500; color: var(--ivory); }
      .grade-excellent { color: var(--positive); }
      .grade-good { color: var(--positive); opacity: 0.85; }
      .grade-warn { color: var(--gold); }
      .grade-alert { color: var(--negative); }
      .stat-card.grade-alert { border-color: var(--negative); }
      .stat-card.grade-warn { border-color: var(--gold); }
      .badge { display: inline-block; padding: 0.25rem 0.75rem; border-radius: 999px;
               font-size: 0.75rem; font-weight: 500; letter-spacing: 0.05em;
               background: var(--surface); border: 1px solid var(--rule); margin-right: 0.5rem; }
      .grade-legend { margin-top: 1rem; }
      .flag-list { list-style: none; padding: 0; }
      .flag-list li { padding: 0.75rem 1rem; background: var(--surface); border-radius: 6px;
                      border-left: 3px solid var(--negative); margin-bottom: 0.5rem;
                      color: var(--bone); }
      .flag-list li strong { color: var(--ivory); }
      .row-highlight { background: var(--surface); }
      .mono { font-family: var(--mono); color: var(--gold); }
    """

    return page_shell(
        title="Prime Cost — LOV3 Houston",
        body_html=body,
        extra_css=extra_css,
        active_path="/prime-cost",
    )
