"""LOV3 Comp Report v2 — Executive-Ready PDF Template.

Incorporates all 4 executive committee review findings (CFO/COO/LP/Board Chair):
  Page 1 — Cover (wordmark, distribution, confidentiality, version)
  Page 2 — Executive Summary (narrative verdict + KPI grid + Money at Risk)
  Page 3 — Financial Discipline (dollar variance + trend context)
  Page 4 — Loss Prevention Audit (post-payment voids, self-approval flags)
  Page 5 — Bottle Manager Ledger (direct answer to CEO's Q)
  Page 6 — Named Scorecards (Ashley/Tiffany/Tony/Daja waterfall)
  Page 7 — Birthday Reconciliation vs SR (pre-reg only per policy)
  Page 8 — Promoter Recap (prose, named POC)
  Page 9 — Return-to-Green Plan (3-week roadmap w/ $ recovery)
  Page 10 — Appendices (A-E labeled)

Design system: editorial-luxury dark-on-ivory. Colored pill badges replace
emoji glyphs. Every action names an owner + deadline. Retail + cost dual view.

Uses:
  - comp_analytics.CompAnalytics — the core comp period
  - birthday_reconciliation.BirthdayReconciliation — birthday audit
  - PaymentDetails_raw for LP audit (self-approval + post-close voids)
"""

from __future__ import annotations

import base64
import io
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests
from google.cloud import bigquery, secretmanager
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
from reportlab.platypus import (
    BaseDocTemplate, Frame, KeepTogether, PageBreak, PageTemplate,
    Paragraph, Spacer, Table, TableStyle,
)

from comp_analytics import (
    AVG_TIER_1_RETAIL, BAR_LEADS, LOV3_OPERATING_DAYS, MANAGERS,
    MANAGER_DISC_TARGET_PCT, PROMOTER_CAPS, RECOVERY_TARGET_PCT,
    UNCATEGORIZED_ALERT_THRESHOLD, CompAnalytics, CompPeriod,
    last_completed_week, prior_week,
)
from birthday_reconciliation import (
    BirthdayReconciliation, BirthdayReconciliationResult,
    DELIVERED_ATTRIBUTED, DELIVERED_COMPED, DELIVERED_COST_BASIS,
    DELIVERED_OFF_BOOK, NOT_DELIVERED, NOT_ELIGIBLE, NO_PROGRAM, SUB_MINIMUM,
)
from config import PROJECT_ID, DATASET_ID

logger = logging.getLogger(__name__)

RESEND_ENDPOINT = "https://api.resend.com/emails"
RESEND_FROM = "LOV3 Analytics <reports@lov3htx.com>"

# ── Design System ────────────────────────────────────────────────────

BLACK = colors.HexColor("#111111")
INK = colors.HexColor("#1a1a1a")
GOLD = colors.HexColor("#B8956A")
BONE = colors.HexColor("#6a6a6a")
CREAM = colors.HexColor("#f9f6f0")
RULE = colors.HexColor("#e5e5e5")

# Status colors
RED = colors.HexColor("#C97064")
GREEN = colors.HexColor("#7FB069")
AMBER = colors.HexColor("#D4A24C")
BLUE = colors.HexColor("#5A8CBF")

# Typography
_ss = getSampleStyleSheet()

STYLE_H1 = ParagraphStyle(
    "h1", parent=_ss["Heading1"], fontSize=26, leading=32,
    textColor=BLACK, spaceAfter=8, fontName="Helvetica-Bold",
)
STYLE_H2 = ParagraphStyle(
    "h2", parent=_ss["Heading2"], fontSize=14, leading=17,
    textColor=BLACK, spaceBefore=14, spaceAfter=6, fontName="Helvetica-Bold",
)
STYLE_H3 = ParagraphStyle(
    "h3", parent=_ss["Heading3"], fontSize=11, leading=14,
    textColor=BLACK, spaceBefore=8, spaceAfter=4, fontName="Helvetica-Bold",
)
STYLE_EYEBROW = ParagraphStyle(
    "eyebrow", parent=_ss["BodyText"], fontSize=8, leading=10,
    textColor=GOLD, fontName="Helvetica-Bold",
    spaceAfter=2,
)
STYLE_BODY = ParagraphStyle(
    "body", parent=_ss["BodyText"], fontSize=9.5, leading=13,
    textColor=INK, spaceAfter=5,
)
STYLE_LEAD = ParagraphStyle(
    "lead", parent=_ss["BodyText"], fontSize=11, leading=15,
    textColor=INK, spaceAfter=8, fontName="Helvetica",
)
STYLE_SMALL = ParagraphStyle(
    "small", parent=_ss["BodyText"], fontSize=8, leading=10, textColor=BONE,
)
STYLE_CENTER = ParagraphStyle(
    "center", parent=STYLE_BODY, alignment=TA_CENTER,
)
STYLE_ACTION = ParagraphStyle(
    "action", parent=_ss["BodyText"], fontSize=9.5, leading=13,
    textColor=INK, spaceAfter=4, leftIndent=12, bulletIndent=0,
)

TABLE_STYLE_STANDARD = TableStyle([
    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
    ("FONTSIZE", (0, 0), (-1, -1), 8.5),
    ("BACKGROUND", (0, 0), (-1, 0), BLACK),
    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
    ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
    ("ALIGN", (0, 0), (0, -1), "LEFT"),
    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
    ("TOPPADDING", (0, 0), (-1, 0), 6),
    ("BOTTOMPADDING", (0, 1), (-1, -1), 4),
    ("TOPPADDING", (0, 1), (-1, -1), 4),
    ("LINEBELOW", (0, 0), (-1, 0), 1, BLACK),
    ("LINEBELOW", (0, -1), (-1, -1), 0.5, RULE),
    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, CREAM]),
])


# ── Formatting helpers ───────────────────────────────────────────────


def _money(v: Optional[float]) -> str:
    if v is None:
        return "—"
    sign = "-" if v < 0 else ""
    return f"{sign}${abs(v):,.0f}"


def _pct(v: float) -> str:
    return f"{v:.2f}%"


def _grade_color(label: str) -> colors.Color:
    return {"On Target": GREEN, "Watch": AMBER, "Investigate": RED}.get(label, BLACK)


def _pill(text: str, color: colors.Color, size: int = 8) -> Paragraph:
    """Return a colored 'pill' badge as a Paragraph with background style hack."""
    return Paragraph(
        f'<font color="{color.hexval()}" size="{size}"><b>● {text}</b></font>',
        STYLE_SMALL,
    )


# Table cell style for wrapped-string cells (used in Action Plan + RTG)
STYLE_CELL = ParagraphStyle(
    "cell", parent=_ss["BodyText"], fontSize=8, leading=10,
    textColor=INK, spaceAfter=0, alignment=TA_LEFT,
)


def _wrap(text: str) -> Paragraph:
    """Wrap a raw string in a Paragraph so ReportLab can flow it inside a table cell."""
    if not isinstance(text, str):
        return text
    # Basic HTML escape to avoid ReportLab parser errors
    import html
    escaped = html.escape(text)
    return Paragraph(escaped, STYLE_CELL)


def _wrap_row(row: List) -> List:
    """Wrap every string cell in a Paragraph. First-column bold for header rows."""
    return [_wrap(c) if isinstance(c, str) else c for c in row]


# ── Sprint B constants — COGS map + daypart bins ────────────────────

# Blended COGS % by Toast sales_category — sourced from LOV3 P&L (2025 SBA).
# Refresh quarterly. VIC3 will need its own map.
COGS_PCT_BY_CATEGORY = {
    "Liquor": 0.22,          # 22% cost of goods
    "Champagne": 0.24,
    "Beer": 0.30,
    "Bottled Beer": 0.32,
    "Wine": 0.28,
    "Food": 0.30,            # 30% cost of goods
    "Brunch Food": 0.32,
    "NA Beverage": 0.15,     # low COGS (water, soft drinks)
    "Modifier": 0.10,
    "Other": 0.25,           # default
}
_DEFAULT_COGS_PCT = 0.25     # if sales_category unknown

# Daypart bins for bottle-service venue analysis
# Pre-11p: build-up · Peak: bottle service prime · Late: close-out
DAYPART_BINS = [
    ("Pre-11p",  17, 23),  # 5pm - 10:59pm
    ("Peak (11p-1a)", 23, 25),  # 11pm - 12:59am (24=00:00 next day, we use hour%24)
    ("Late (1a-close)", 1, 3),  # 1am - 2:59am
]


def _cogs_pct(sales_category: Optional[str]) -> float:
    if not sales_category:
        return _DEFAULT_COGS_PCT
    return COGS_PCT_BY_CATEGORY.get(sales_category, _DEFAULT_COGS_PCT)


# ── Sprint A dataclasses (bad-behavior + LP-B collusion signals) ─────


@dataclass
class ApproverServerPair:
    """Repeat approver→server void pair (LP-B structural collusion signal)."""
    approver: str
    server: str
    void_user: str
    events: int
    total_dollars: float
    void_user_is_service_account: bool


@dataclass
class ReasonItemMismatch:
    """Reason code doesn't match the item comped (Spillage on bottle etc.)."""
    processing_date: str
    server: str
    tab_name: str
    menu_item: str
    sales_category: str
    reason: str
    amount: float


@dataclass
class ManagerBehaviorMetrics:
    """The 6 bad-behavior detectors per named manager (COO synthesis).

    Adds shift-normalized KPIs from Sprint B (Labor API integration): shifts_worked
    is measured from LaborTimeEntries where available, with fallback to
    CheckDetails.server-distinct-processing_date. Managers who don't clock in
    via Toast (Tiffany, Tony historically) use the fallback.
    """
    name: str
    self_approval_count: int = 0
    self_approval_dollars: float = 0.0
    round_dollar_ratio: float = 0.0
    round_dollar_events: int = 0
    late_shift_ratio: float = 0.0
    late_shift_dollars: float = 0.0
    reason_specificity_pct: float = 0.0
    retail_vs_owner_mix_pct: float = 0.0
    trend_vs_4wk_median_pct: float = 0.0
    # Sprint B — shift normalization
    shifts_worked: int = 0
    shifts_source: str = "labor"  # "labor" | "checks_fallback" | "none"
    discretionary_comp: float = 0.0  # snapshot of scorecard $ for the ratio
    comp_count: int = 0

    @property
    def discretionary_per_shift(self) -> float:
        return self.discretionary_comp / self.shifts_worked if self.shifts_worked else 0.0

    @property
    def comps_per_shift(self) -> float:
        return self.comp_count / self.shifts_worked if self.shifts_worked else 0.0


# ── LP audit data fetch (self-approval + post-close voids) ───────────


@dataclass
class LPVoidRecord:
    processing_date: str
    check_id: str
    server: str
    amount: float
    payment_type: str
    void_user: str
    void_approver: str
    paid_date: str
    void_date: str
    is_self_approved: bool
    is_cash: bool
    time_to_void_secs: Optional[int]


def fetch_lp_voids(bq: bigquery.Client, start: str, end: str) -> List[LPVoidRecord]:
    q = f"""
    SELECT
      processing_date,
      CAST(check_id AS STRING) AS check_id,
      server,
      SAFE_CAST(amount AS FLOAT64) AS amount,
      payment_type,
      COALESCE(void_user, '') AS void_user,
      COALESCE(void_approver, '') AS void_approver,
      COALESCE(paid_date, '') AS paid_date,
      COALESCE(void_date, '') AS void_date
    FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
    WHERE processing_date BETWEEN @start AND @end
      AND void_user IS NOT NULL AND void_user != ''
    ORDER BY SAFE_CAST(amount AS FLOAT64) DESC
    """
    job = bq.query(q, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("start", "DATE", start),
        bigquery.ScalarQueryParameter("end", "DATE", end),
    ]))
    out: List[LPVoidRecord] = []
    for row in job.result():
        # Self-approval detection: server name matches void_user (heuristic)
        server_low = (row.server or "").lower().strip()
        vuser_low = (row.void_user or "").lower().strip()
        is_self = server_low and vuser_low and (server_low == vuser_low
                                                 or server_low.split()[0] in vuser_low
                                                 or vuser_low.split()[0] in server_low)
        is_cash = "cash" in (row.payment_type or "").lower()
        # Time-to-void seconds if both timestamps parseable
        secs = None
        try:
            if row.paid_date and row.void_date:
                p = datetime.strptime(row.paid_date, "%m/%d/%y %I:%M %p")
                v = datetime.strptime(row.void_date, "%m/%d/%y %I:%M %p")
                secs = int((v - p).total_seconds())
        except (ValueError, TypeError):
            pass
        out.append(LPVoidRecord(
            processing_date=str(row.processing_date),
            check_id=row.check_id,
            server=row.server or "—",
            amount=float(row.amount or 0.0),
            payment_type=row.payment_type or "—",
            void_user=row.void_user,
            void_approver=row.void_approver,
            paid_date=row.paid_date,
            void_date=row.void_date,
            is_self_approved=bool(is_self),
            is_cash=is_cash,
            time_to_void_secs=secs,
        ))
    return out


# ── Sprint A fetch helpers ───────────────────────────────────────────


import re
_UUID_EMAIL_RE = re.compile(
    r"^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}",
    re.IGNORECASE,
)


def _is_service_account(void_user: str) -> bool:
    """UUID email or example.com = synthetic void_user (P0 flag)."""
    if not void_user:
        return False
    vu = void_user.lower().strip()
    if "example.com" in vu or "@test" in vu:
        return True
    if _UUID_EMAIL_RE.match(vu):
        return True
    return False


def fetch_approver_pairs(bq: bigquery.Client, ref_date: str,
                          lookback_days: int = 90) -> List[ApproverServerPair]:
    """Trailing-N-day repeat approver→server void pairs (LP-B collusion)."""
    q = f"""
    SELECT
      COALESCE(void_approver, '') AS approver,
      COALESCE(server, '') AS server,
      COALESCE(void_user, '') AS void_user,
      COUNT(*) AS events,
      ROUND(SUM(SAFE_CAST(amount AS FLOAT64)), 0) AS dollars
    FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
    WHERE processing_date BETWEEN
        DATE_SUB(@ref_date, INTERVAL {lookback_days} DAY) AND @ref_date
      AND void_user IS NOT NULL AND void_user != ''
    GROUP BY approver, server, void_user
    HAVING events >= 3 OR dollars >= 500
    ORDER BY dollars DESC
    LIMIT 20
    """
    job = bq.query(q, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("ref_date", "DATE", ref_date),
    ]))
    out: List[ApproverServerPair] = []
    for row in job.result():
        vu = row.void_user or ""
        out.append(ApproverServerPair(
            approver=row.approver or "—",
            server=row.server or "—",
            void_user=vu,
            events=int(row.events),
            total_dollars=float(row.dollars or 0.0),
            void_user_is_service_account=_is_service_account(vu),
        ))
    return out


def fetch_reason_item_mismatches(bq: bigquery.Client,
                                   start: str, end: str
                                   ) -> List[ReasonItemMismatch]:
    """Reason codes incompatible with the item (Spillage on a bottled item, etc.)."""
    q = f"""
    SELECT
      cd.processing_date,
      cd.server,
      cd.reason_of_discount,
      SAFE_CAST(cd.discount AS FLOAT64) AS discount,
      isd.menu_item,
      isd.sales_category,
      isd.tab_name
    FROM `{PROJECT_ID}.{DATASET_ID}.CheckDetails_raw` cd
    JOIN `{PROJECT_ID}.{DATASET_ID}.ItemSelectionDetails_raw` isd
      ON CAST(cd.check_id AS STRING) = CAST(isd.check_id AS STRING)
      AND cd.processing_date = isd.processing_date
    WHERE cd.processing_date BETWEEN @start AND @end
      AND SAFE_CAST(cd.discount AS FLOAT64) >= 75
      AND (LOWER(cd.reason_of_discount) LIKE '%spillage%'
           OR LOWER(cd.reason_of_discount) LIKE '%food quality%'
           OR LOWER(cd.reason_of_discount) LIKE '%didn%t like%'
           OR LOWER(cd.reason_of_discount) LIKE '%remake%')
      AND (
        UPPER(isd.menu_item) LIKE 'BTL %'
        OR UPPER(isd.menu_item) LIKE 'OWNER %'
        OR UPPER(isd.menu_item) LIKE '$%'
        OR (LOWER(isd.sales_category) LIKE '%liquor%'
            AND SAFE_CAST(isd.gross_price AS FLOAT64) >= 100)
      )
    ORDER BY SAFE_CAST(cd.discount AS FLOAT64) DESC
    LIMIT 20
    """
    job = bq.query(q, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("start", "DATE", start),
        bigquery.ScalarQueryParameter("end", "DATE", end),
    ]))
    out: List[ReasonItemMismatch] = []
    for row in job.result():
        out.append(ReasonItemMismatch(
            processing_date=str(row.processing_date),
            server=row.server or "—",
            tab_name=row.tab_name or "",
            menu_item=row.menu_item or "",
            sales_category=row.sales_category or "",
            reason=row.reason_of_discount or "",
            amount=float(row.discount or 0.0),
        ))
    return out


def _fetch_shifts_worked(bq: bigquery.Client, mgr_name: str,
                          start: str, end: str) -> Tuple[int, str]:
    """Return (shifts_worked, source_label). Multi-signal union detection.

    Managers who don't clock in via Toast (Tiffany, Tony historically) leave
    other traces. Union of DISTINCT processing_dates across:
      1. LaborTimeEntries — clocked in with ≥1 regular hour
      2. CashEntries.employee / employee_2 — cash drawer activity
      3. PaymentDetails.void_approver — approved a void (=definitively on-site)
      4. PaymentDetails.void_user — voided a payment
      5. CheckDetails.server — ring-in activity
      6. OrderDetails.server — order-of-record

    Source label reflects which signals fired. If Labor found, "labor";
    if only floor-activity found, "floor_activity"; if none, "none".
    """
    # Toast sometimes stores the void_approver as "RestaurantUser [id=..., user
    # email = tonywinn@lov3htx.com]" — need substring match on first+last name
    # AND concatenated email-form ("tonywinn", "tiffanyloving") to catch these.
    parts = [p.lower() for p in mgr_name.split() if len(p) >= 3]
    concat_name = "".join(parts) if len(parts) >= 2 else ""

    q = f"""
    WITH signals AS (
      SELECT processing_date, 'labor' AS src
      FROM `{PROJECT_ID}.{DATASET_ID}.LaborTimeEntries_raw`
      WHERE processing_date BETWEEN @start AND @end
        AND LOWER(employee_name) = LOWER(@name)
        AND regular_hours >= 1
      UNION DISTINCT
      SELECT processing_date, 'cash'
      FROM `{PROJECT_ID}.{DATASET_ID}.CashEntries_raw`
      WHERE processing_date BETWEEN @start AND @end
        AND (LOWER(employee) = LOWER(@name) OR LOWER(employee_2) = LOWER(@name))
      UNION DISTINCT
      SELECT processing_date, 'void_appr'
      FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
      WHERE processing_date BETWEEN @start AND @end
        AND (LOWER(void_approver) = LOWER(@name)
             OR (@concat != '' AND LOWER(void_approver) LIKE CONCAT('%', @concat, '@%'))
             OR (@last != '' AND LOWER(void_approver) LIKE CONCAT('%', @last, '@%')))
      UNION DISTINCT
      SELECT processing_date, 'void_user'
      FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
      WHERE processing_date BETWEEN @start AND @end
        AND (LOWER(void_user) = LOWER(@name)
             OR (@concat != '' AND LOWER(void_user) LIKE CONCAT('%', @concat, '@%')))
      UNION DISTINCT
      SELECT processing_date, 'server'
      FROM `{PROJECT_ID}.{DATASET_ID}.CheckDetails_raw`
      WHERE processing_date BETWEEN @start AND @end
        AND LOWER(server) = LOWER(@name)
      UNION DISTINCT
      SELECT processing_date, 'order_srv'
      FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
      WHERE processing_date BETWEEN @start AND @end
        AND LOWER(server) = LOWER(@name)
    )
    SELECT
      COUNT(DISTINCT processing_date) AS shifts,
      STRING_AGG(DISTINCT src ORDER BY src) AS sources
    FROM signals
    """
    job = bq.query(q, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("start", "DATE", start),
        bigquery.ScalarQueryParameter("end", "DATE", end),
        bigquery.ScalarQueryParameter("name", "STRING", mgr_name),
        bigquery.ScalarQueryParameter("concat", "STRING", concat_name),
        bigquery.ScalarQueryParameter("last", "STRING", parts[-1] if parts else ""),
    ]))
    for row in job.result():
        s = int(row.shifts or 0)
        sources = row.sources or ""
        if s == 0:
            return 0, "none"
        # Determine primary source label
        if "labor" in sources:
            return s, "labor"
        return s, "floor_activity"
    return 0, "none"


def fetch_manager_behavior(bq: bigquery.Client, cur: CompPeriod,
                            mgr_name: str) -> ManagerBehaviorMetrics:
    """Compute the 6 bad-behavior detectors for one manager (this week)."""
    m = ManagerBehaviorMetrics(name=mgr_name)

    # Discretionary-only, this manager, this week
    mgr_comps = [it for it in cur.item_log
                 if it.server == mgr_name and it.discount > 0]
    if not mgr_comps:
        return m

    total_events = len(mgr_comps)
    total_dollars = sum(it.discount for it in mgr_comps)

    # 1. Round-$ ratio
    round_events = [it for it in mgr_comps
                    if it.discount >= 100 and abs(it.discount - round(it.discount)) < 0.001
                    and int(round(it.discount)) % 100 == 0]
    m.round_dollar_events = len(round_events)
    m.round_dollar_ratio = 100.0 * len(round_events) / max(total_events, 1)

    # 2. Late-shift (opened between 11pm and 2:30am) — best-effort via reason string
    # Item log doesn't carry opened_time; approximate via processing_date + time hint later.
    # For now, use whether any comp on this manager falls in the "late" bucket
    # from CheckDetails. Query below.
    # (implemented via _fetch_late_shift_totals below in main flow)

    # 3. Reason-code specificity — % NOT using generic "Manager Comp - Check/Item (100.00%)"
    def _is_generic(reason: str) -> bool:
        if not reason:
            return True
        r = reason.lower().replace("(100.00%)", "").replace("(100%)", "").strip(", ").strip()
        return r in ("manager comp - check", "manager comp - item",
                     "manager comp - check ,",
                     "manager comp - item, manager comp - item")
    specific_events = [it for it in mgr_comps if it.reason and not _is_generic(it.reason)]
    m.reason_specificity_pct = 100.0 * len(specific_events) / max(total_events, 1)

    # 4. Retail-vs-OWNER-SKU comp mix (% of $ that used retail SKU when an OWNER equivalent exists)
    from comp_analytics import OWNER_SKU_TO_RETAIL
    retail_names = set(OWNER_SKU_TO_RETAIL.values())
    retail_when_owner_exists = sum(it.discount for it in mgr_comps
                                    if it.menu_item in retail_names)
    m.retail_vs_owner_mix_pct = 100.0 * retail_when_owner_exists / max(total_dollars, 1)

    # 5. Self-approval — best-effort via PaymentDetails; query per-manager
    q = f"""
    SELECT
      COUNT(*) AS events,
      ROUND(SUM(SAFE_CAST(amount AS FLOAT64)), 0) AS dollars
    FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
    WHERE processing_date BETWEEN @start AND @end
      AND void_user IS NOT NULL AND void_user != ''
      AND (LOWER(server) = LOWER(@name) OR LOWER(server) LIKE CONCAT('%', LOWER(@first), '%'))
      AND (LOWER(void_user) = LOWER(@name) OR LOWER(void_user) LIKE CONCAT('%', LOWER(@first), '%'))
    """
    first = mgr_name.split()[0] if mgr_name else ""
    job = bq.query(q, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("start", "DATE", cur.start),
        bigquery.ScalarQueryParameter("end", "DATE", cur.end),
        bigquery.ScalarQueryParameter("name", "STRING", mgr_name),
        bigquery.ScalarQueryParameter("first", "STRING", first),
    ]))
    for row in job.result():
        m.self_approval_count = int(row.events or 0)
        m.self_approval_dollars = float(row.dollars or 0.0)
        break

    # 6. Late-shift ratio (11pm-2:30am) via CheckDetails opened_time
    q2 = f"""
    SELECT
      ROUND(SUM(CASE WHEN
        (SAFE.PARSE_TIME('%I:%M %p', opened_time) >= TIME '23:00:00'
         OR SAFE.PARSE_TIME('%I:%M %p', opened_time) < TIME '02:30:00')
        THEN SAFE_CAST(discount AS FLOAT64) ELSE 0 END), 0) AS late_dollars,
      ROUND(SUM(SAFE_CAST(discount AS FLOAT64)), 0) AS total_dollars
    FROM `{PROJECT_ID}.{DATASET_ID}.CheckDetails_raw`
    WHERE processing_date BETWEEN @start AND @end
      AND server = @name
      AND SAFE_CAST(discount AS FLOAT64) > 0
    """
    job2 = bq.query(q2, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("start", "DATE", cur.start),
        bigquery.ScalarQueryParameter("end", "DATE", cur.end),
        bigquery.ScalarQueryParameter("name", "STRING", mgr_name),
    ]))
    for row in job2.result():
        late = float(row.late_dollars or 0.0)
        tot = float(row.total_dollars or 0.0)
        m.late_shift_dollars = late
        m.late_shift_ratio = 100.0 * late / max(tot, 1)
        break

    # 7. 4-week trend delta — this-week $ vs manager's own 4-wk rolling median
    q3 = f"""
    SELECT SUM(SAFE_CAST(discount AS FLOAT64)) / 4.0 AS avg_wk
    FROM `{PROJECT_ID}.{DATASET_ID}.CheckDetails_raw`
    WHERE processing_date BETWEEN
        DATE_SUB(@start, INTERVAL 28 DAY) AND DATE_SUB(@start, INTERVAL 1 DAY)
      AND server = @name
      AND SAFE_CAST(discount AS FLOAT64) > 0
    """
    job3 = bq.query(q3, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("start", "DATE", cur.start),
        bigquery.ScalarQueryParameter("name", "STRING", mgr_name),
    ]))
    for row in job3.result():
        base = float(row.avg_wk or 0.0)
        if base > 0:
            m.trend_vs_4wk_median_pct = 100.0 * (total_dollars - base) / base
        break

    # 8. Sprint B — shifts worked (Labor API with CheckDetails fallback)
    m.shifts_worked, m.shifts_source = _fetch_shifts_worked(
        bq, mgr_name, cur.start, cur.end,
    )
    # Snapshot the scorecard $ for the per-shift ratio
    s = cur.named_scorecards.get(mgr_name)
    if s:
        m.discretionary_comp = s.discretionary_comp
        m.comp_count = s.comp_count

    return m


# ── Sprint B dataclasses + fetchers ──────────────────────────────────


@dataclass
class DaypartComp:
    label: str
    comp_dollars: float = 0.0
    comp_count: int = 0
    net_sales: float = 0.0

    @property
    def comp_pct(self) -> float:
        return 100.0 * self.comp_dollars / self.net_sales if self.net_sales else 0.0


@dataclass
class TrendSnapshot:
    """One period's headline metrics for trend line."""
    label: str
    net_sales: float
    total_comp: float
    manager_disc_pct: float
    recovery_pct: float

    @property
    def blended_pct(self) -> float:
        return 100.0 * self.total_comp / self.net_sales if self.net_sales else 0.0


def fetch_daypart_split(bq: bigquery.Client, start: str, end: str
                         ) -> List[DaypartComp]:
    """Comp $ + count + net-sales base per daypart bin (Pre-11p / Peak / Late)."""
    q = f"""
    WITH comps AS (
      SELECT
        SAFE_CAST(discount AS FLOAT64) AS discount,
        SAFE.PARSE_TIME('%I:%M %p', opened_time) AS ot
      FROM `{PROJECT_ID}.{DATASET_ID}.CheckDetails_raw`
      WHERE processing_date BETWEEN @start AND @end
        AND SAFE_CAST(discount AS FLOAT64) > 0
        AND opened_time IS NOT NULL AND opened_time != ''
    ),
    comp_bins AS (
      SELECT
        CASE
          WHEN EXTRACT(HOUR FROM ot) BETWEEN 17 AND 22 THEN 'Pre-11p'
          WHEN EXTRACT(HOUR FROM ot) = 23 OR EXTRACT(HOUR FROM ot) = 0 THEN 'Peak (11p-1a)'
          WHEN EXTRACT(HOUR FROM ot) BETWEEN 1 AND 3 THEN 'Late (1a-close)'
          ELSE 'Other'
        END AS daypart,
        discount
      FROM comps
      WHERE ot IS NOT NULL
    ),
    comp_agg AS (
      SELECT daypart,
             ROUND(SUM(discount), 0) AS comp_dollars,
             COUNT(*) AS comp_count
      FROM comp_bins
      GROUP BY daypart
    ),
    orders AS (
      -- OrderDetails.opened is a full datetime string 'YYYY-MM-DD HH:MM:SS'
      SELECT
        EXTRACT(HOUR FROM SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', opened)) AS h,
        SAFE_CAST(amount AS FLOAT64) AS amount
      FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
      WHERE processing_date BETWEEN @start AND @end
        AND opened IS NOT NULL AND opened != ''
    ),
    order_bins AS (
      SELECT
        CASE
          WHEN h BETWEEN 17 AND 22 THEN 'Pre-11p'
          WHEN h = 23 OR h = 0 THEN 'Peak (11p-1a)'
          WHEN h BETWEEN 1 AND 3 THEN 'Late (1a-close)'
          ELSE 'Other'
        END AS daypart,
        amount
      FROM orders
      WHERE h IS NOT NULL
    ),
    net_agg AS (
      SELECT daypart, ROUND(SUM(amount), 0) AS net_sales
      FROM order_bins
      GROUP BY daypart
    )
    SELECT
      COALESCE(c.daypart, n.daypart) AS daypart,
      COALESCE(c.comp_dollars, 0) AS comp_dollars,
      COALESCE(c.comp_count, 0) AS comp_count,
      COALESCE(n.net_sales, 0) AS net_sales
    FROM comp_agg c
    FULL OUTER JOIN net_agg n USING (daypart)
    """
    job = bq.query(q, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("start", "DATE", start),
        bigquery.ScalarQueryParameter("end", "DATE", end),
    ]))
    seen = {}
    for row in job.result():
        seen[row.daypart] = DaypartComp(
            label=row.daypart,
            comp_dollars=float(row.comp_dollars or 0.0),
            comp_count=int(row.comp_count),
            net_sales=float(row.net_sales or 0.0),
        )
    # Return in canonical order
    order = ["Pre-11p", "Peak (11p-1a)", "Late (1a-close)", "Other"]
    return [seen.get(l, DaypartComp(label=l)) for l in order if l in seen or l != "Other"]


def fetch_4wk_trend(analytics: CompAnalytics,
                     end_date: str, weeks_back: int = 4
                     ) -> List[TrendSnapshot]:
    """4-week rolling trend on Blended, Manager Disc, Recovery."""
    snapshots: List[TrendSnapshot] = []
    end_d = date.fromisoformat(end_date)
    for w in range(weeks_back - 1, -1, -1):
        # Each week ends on Sunday. weeks_back=4 means current + 3 prior.
        wk_end = end_d - timedelta(days=7 * w)
        wk_start = wk_end - timedelta(days=6)
        label = f"{wk_start.strftime('%b %-d')}-{wk_end.strftime('%-d')}"
        p = analytics.compute_period(label, wk_start.isoformat(), wk_end.isoformat())
        snapshots.append(TrendSnapshot(
            label=label,
            net_sales=p.net_sales,
            total_comp=p.total_comp,
            manager_disc_pct=p.manager_disc_pct,
            recovery_pct=p.recovery_pct,
        ))
    return snapshots


def compute_cogs_drag(cur: CompPeriod) -> Tuple[float, float, Dict[str, float]]:
    """Compute EBITDA drag from comps.

    Returns (retail_comp_$, cogs_impact_$, by_category_dict).
    retail_comp_$ = total discretionary + recovery + uncategorized (comp $ we lost as revenue)
    cogs_impact_$ = sum of item_comp × COGS % (actual margin lost)
    """
    retail_total = 0.0
    cogs_impact = 0.0
    by_category: Dict[str, float] = {}
    for it in cur.item_log:
        if it.discount <= 0:
            continue
        # Skip owner_discretion + programmatic (not discretionary)
        if it.bucket in ("owner_discretion", "programmatic_birthday",
                         "programmatic_promoter", "programmatic_marketing",
                         "programmatic_standing"):
            continue
        retail_total += it.discount
        pct = _cogs_pct(it.sales_category)
        drag = it.discount * pct
        cogs_impact += drag
        cat = it.sales_category or "Other"
        by_category[cat] = by_category.get(cat, 0.0) + drag
    return retail_total, cogs_impact, by_category


# ── Bottle Manager audit ─────────────────────────────────────────────


@dataclass
class BMAudit:
    total_comps: int
    total_dollars: float
    owner_sku_rings: int
    owner_sku_dollars: float
    voids_over_100: int
    voids_dollars: float
    top_items: List[Tuple[str, int, float, str]]  # (item, count, $, tab)


def fetch_bottle_manager_audit(bq: bigquery.Client, start: str, end: str) -> BMAudit:
    q = f"""
    SELECT
      menu_item,
      tab_name,
      SAFE_CAST(gross_price AS FLOAT64) AS gross_price,
      SAFE_CAST(discount AS FLOAT64) AS discount,
      voided
    FROM `{PROJECT_ID}.{DATASET_ID}.ItemSelectionDetails_raw`
    WHERE processing_date BETWEEN @start AND @end
      AND server = 'Bottle Manager'
      AND (SAFE_CAST(discount AS FLOAT64) > 0 OR LOWER(voided) = 'true')
    """
    job = bq.query(q, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("start", "DATE", start),
        bigquery.ScalarQueryParameter("end", "DATE", end),
    ]))
    total_comps = 0
    total_dollars = 0.0
    owner_rings = 0
    owner_dollars = 0.0
    voids_100 = 0
    voids_dollars = 0.0
    item_agg: Dict[Tuple[str, str], List[float]] = {}

    for row in job.result():
        item = row.menu_item or ""
        tab = row.tab_name or ""
        gp = float(row.gross_price or 0.0)
        disc = float(row.discount or 0.0)
        voided = str(row.voided or "").lower() == "true"

        if disc > 0:
            total_comps += 1
            total_dollars += disc
            key = (item, tab)
            item_agg.setdefault(key, [0, 0.0])
            item_agg[key][0] += 1
            item_agg[key][1] += disc
        if voided and gp >= 100:
            voids_100 += 1
            voids_dollars += gp
        if item.upper().startswith("OWNER "):
            owner_rings += 1
            owner_dollars += gp if disc == 0 else disc

    # Top 5 items
    top_items = sorted(
        [(item, cnt_dol[0], cnt_dol[1], tab) for (item, tab), cnt_dol in item_agg.items()],
        key=lambda x: -x[2],
    )[:5]

    return BMAudit(
        total_comps=total_comps,
        total_dollars=total_dollars,
        owner_sku_rings=owner_rings,
        owner_sku_dollars=owner_dollars,
        voids_over_100=voids_100,
        voids_dollars=voids_dollars,
        top_items=top_items,
    )


# ── Page builders ────────────────────────────────────────────────────


def _footer(canv: canvas.Canvas, doc, version: str = "v8.0"):
    canv.saveState()
    canv.setFont("Helvetica", 7)
    canv.setFillColor(BONE)
    footer = f"LOV3|HTX Confidential · Comp Report {version} · Page {canv.getPageNumber()}"
    canv.drawString(0.55 * inch, 0.35 * inch, footer)
    canv.drawRightString(LETTER[0] - 0.55 * inch, 0.35 * inch,
                         datetime.now().strftime("%B %-d, %Y · %-I:%M %p CT"))
    canv.restoreState()


def build_cover(cur: CompPeriod) -> List:
    """Page 1 — Cover with wordmark, distribution, confidentiality."""
    # Dedicated cover style with generous leading so wordmark doesn't collide
    # with the eyebrow text below.
    cover_wordmark_style = ParagraphStyle(
        "cover_wordmark", parent=STYLE_CENTER,
        fontSize=42, leading=52, fontName="Helvetica-Bold",
        spaceAfter=6,
    )
    cover_eyebrow_style = ParagraphStyle(
        "cover_eyebrow", parent=STYLE_CENTER,
        fontSize=9, leading=12, textColor=GOLD, fontName="Helvetica-Bold",
        spaceBefore=8, spaceAfter=0,
    )
    cover_title_style = ParagraphStyle(
        "cover_title", parent=STYLE_CENTER,
        fontSize=24, leading=30, textColor=BLACK, fontName="Helvetica-Bold",
        spaceBefore=0, spaceAfter=8,
    )
    cover_period_style = ParagraphStyle(
        "cover_period", parent=STYLE_CENTER,
        fontSize=14, leading=18, textColor=BONE,
    )

    story = []
    story.append(Spacer(1, 1.5 * inch))
    # Wordmark — dedicated leading prevents overlap with eyebrow below
    story.append(Paragraph(
        '<font color="#111111"><b>LOV3</b></font>'
        '<font color="#B8956A"><b>|</b></font>'
        '<font color="#111111"><b>HTX</b></font>',
        cover_wordmark_style,
    ))
    # Explicit spacer so eyebrow drops clearly below the wordmark
    story.append(Spacer(1, 0.35 * inch))
    story.append(Paragraph(
        "LEADERSHIP · COMP DISCIPLINE · WEEKLY",
        cover_eyebrow_style,
    ))
    story.append(Spacer(1, 0.55 * inch))
    story.append(Paragraph("Weekly Comp Discipline Report", cover_title_style))
    story.append(Spacer(1, 0.15 * inch))
    story.append(Paragraph(
        f"Week of {cur.label} · {cur.start} to {cur.end}",
        cover_period_style,
    ))

    story.append(Spacer(1, 1.4 * inch))
    # Distribution
    dist_data = [
        ["DISTRIBUTION", ""],
        ["Owners", "Maurice Ragland · Eddie · Derwin"],
        ["Managers", "Tiffany Loving · Anthony Winn · Dajah Bishop"],
        ["Bar Lead", "Ashley Baines"],
        ["Generated", datetime.now().strftime("%B %-d, %Y at %-I:%M %p Central")],
        ["Document version", "v8.0 · Policy rev 2026-07-29"],
        ["Classification", "CONFIDENTIAL — For Leadership Only"],
    ]
    t = Table(dist_data, colWidths=[1.7 * inch, 4.5 * inch])
    t.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 8),
        ("TEXTCOLOR", (0, 0), (-1, 0), GOLD),
        ("BACKGROUND", (0, 0), (-1, 0), colors.transparent),
        ("FONTSIZE", (0, 1), (-1, -1), 9.5),
        ("FONTNAME", (0, 1), (0, -1), "Helvetica-Bold"),
        ("TEXTCOLOR", (0, 1), (0, -1), BLACK),
        ("TEXTCOLOR", (1, 1), (1, -1), INK),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        # Explicit gap between label column and value column
        ("RIGHTPADDING", (0, 0), (0, -1), 16),
        ("LEFTPADDING", (1, 0), (1, -1), 12),
        ("LINEBELOW", (0, 0), (-1, 0), 0.5, GOLD),
    ]))
    story.append(t)
    story.append(PageBreak())
    return story


def build_exec_summary(cur: CompPeriod, prev: CompPeriod, tier2_foregone: float,
                      lp_voids: List[LPVoidRecord]) -> List:
    """Page 2 — Executive Summary: narrative verdict + KPI grid + Money at Risk."""
    story = []
    story.append(Paragraph("LOV3 · Weekly Comp Discipline", STYLE_EYEBROW))
    story.append(Paragraph(f"Executive Summary — Week of {cur.label}", STYLE_H1))
    story.append(Spacer(1, 0.05 * inch))

    # ── 3-line narrative verdict ──
    grade_label, _ = cur.grade()
    disc_label, _ = cur.discretionary_grade()
    mgr_label, _ = cur.manager_disc_grade()

    # Root cause narrative — pick the loudest
    if cur.manager_disc_pct > MANAGER_DISC_TARGET_PCT * 2:
        root = "Manager discretionary comps are running high; reason-code discipline needs immediate attention."
    elif tier2_foregone > 5000:
        root = f"${tier2_foregone:,.0f} foregone on Tier 2 bottles — house-comped without cost recovery."
    elif any(f.is_self_approved and f.is_cash for f in lp_voids):
        cash_self_voids = sum(f.amount for f in lp_voids if f.is_self_approved and f.is_cash)
        root = f"${cash_self_voids:,.0f} in self-approved cash voids — post-payment leak requires drawer audit."
    elif cur.uncategorized_dollars > UNCATEGORIZED_ALERT_THRESHOLD:
        root = f"${cur.uncategorized_dollars:,.0f} in uncategorized ring-ins — reason codes needed."
    else:
        root = "Metrics within tolerance week-over-week."

    wow_delta = cur.total_pct - prev.total_pct
    direction = "up" if wow_delta > 0 else "down"

    verdict = (
        f"<b>This week</b> LOV3 comped <b>{_money(cur.total_comp)}</b> — "
        f"<b>{_pct(cur.total_pct)}</b> of ${cur.net_sales:,.0f} net sales — "
        f"{'above' if cur.total_pct > 4 else 'within'} the 4% target "
        f"({direction} {abs(wow_delta):.2f}pp vs prior week's {_pct(prev.total_pct)}). "
        f"Overall discipline: <b>{grade_label}</b>. "
        f"<br/><br/>"
        f"<b>Root cause:</b> {root}"
    )
    story.append(Paragraph(verdict, STYLE_LEAD))
    story.append(Spacer(1, 0.1 * inch))

    # ── KPI pill badge grid ──
    story.append(Paragraph("HEADLINE KPIs", STYLE_H2))

    def _variance_dollars(actual_pct: float, target_pct: float, base: float) -> float:
        return (actual_pct - target_pct) / 100.0 * base

    kpi_rows = [
        ["Metric", "Actual", "Target", "Variance $", "Status"],
        ["Blended comp %", _pct(cur.total_pct), "<4.00%",
         _money(_variance_dollars(cur.total_pct, 4.0, cur.net_sales)),
         grade_label],
        ["Manager Discretionary %", _pct(cur.manager_disc_pct), "≤1.00%",
         _money(_variance_dollars(cur.manager_disc_pct, 1.0, cur.net_sales)),
         mgr_label],
        ["Recovery %", _pct(cur.recovery_pct), "≤0.50%",
         _money(_variance_dollars(cur.recovery_pct, 0.5, cur.net_sales)),
         "On Target" if cur.recovery_pct <= 0.5 else "Watch"],
        ["Uncategorized $", _money(cur.uncategorized_dollars), "$0",
         _money(cur.uncategorized_dollars),
         "On Target" if cur.uncategorized_dollars <= UNCATEGORIZED_ALERT_THRESHOLD else "Investigate"],
    ]
    kpi_tbl = Table(kpi_rows, colWidths=[1.9 * inch, 1.1 * inch, 1.0 * inch, 1.4 * inch, 1.5 * inch])
    kpi_tbl.setStyle(TABLE_STYLE_STANDARD)
    # Color the Status column
    for i, row in enumerate(kpi_rows[1:], 1):
        color = _grade_color(row[4])
        kpi_tbl.setStyle(TableStyle([
            ("TEXTCOLOR", (4, i), (4, i), color),
            ("FONTNAME", (4, i), (4, i), "Helvetica-Bold"),
        ]))
    story.append(kpi_tbl)

    # ── Money at Risk callout ──
    story.append(Spacer(1, 0.15 * inch))
    story.append(Paragraph("MONEY AT RISK", STYLE_H2))

    cap_breaches = [c for c in cur.promoter_caps if c.is_over_cap]
    external_clawback = sum(c.excess_bottles * 0.8 * AVG_TIER_1_RETAIL
                            for c in cap_breaches if c.cap_type == "external")
    cash_void_dollars = sum(f.amount for f in lp_voids if f.is_cash)

    risk_rows = [["Item", "Detail", "Impact $"]]
    if tier2_foregone > 0:
        tier2_count = sum(m.house_comped_count for m in cur.tier_2_movements.values())
        risk_rows.append([
            "Tier 2 house-comped bottles",
            f"{tier2_count} bottles · retail value not recovered",
            _money(tier2_foregone),
        ])
    if cap_breaches:
        summary = ", ".join(f"{c.event} +{c.excess_bottles}" for c in cap_breaches)
        risk_rows.append([
            f"Promoter cap breach ({len(cap_breaches)})",
            summary,
            f"~{_money(external_clawback)} clawback" if external_clawback else "in-house spend",
        ])
    if cur.uncategorized_dollars > 500:
        risk_rows.append([
            "Uncategorized ring-ins",
            "Reason code missing — cannot categorize",
            _money(cur.uncategorized_dollars),
        ])

    # Sprint B: COGS-drag row (actual EBITDA impact vs headline retail $)
    retail_c, cogs_drag, _ = compute_cogs_drag(cur)
    if cogs_drag > 0:
        risk_rows.append([
            "COGS drag (EBITDA impact)",
            f"Retail comp ${retail_c:,.0f} × blended COGS %",
            _money(cogs_drag),
        ])
    if cash_void_dollars > 0:
        risk_rows.append([
            "🚨 Post-payment cash voids",
            f"{len([f for f in lp_voids if f.is_cash])} voids — see LP Audit",
            _money(cash_void_dollars),
        ])
    if len(risk_rows) == 1:
        risk_rows.append(["✓ No material risk", "All indicators within tolerance", "$0"])

    rt = Table(risk_rows, colWidths=[2.2 * inch, 3.2 * inch, 1.5 * inch])
    rt.setStyle(TABLE_STYLE_STANDARD)
    if len(risk_rows) > 1:
        rt.setStyle(TableStyle([("TEXTCOLOR", (2, 1), (2, -1), RED)]))
    story.append(rt)

    story.append(PageBreak())
    return story


def build_lp_audit(lp_voids: List[LPVoidRecord],
                    approver_pairs: List[ApproverServerPair],
                    reason_mismatches: List[ReasonItemMismatch]) -> List:
    """Page 3 — Loss Prevention Audit. Split into LP-A skim vectors + LP-B collusion."""
    story = []
    story.append(Paragraph("Loss Prevention", STYLE_EYEBROW))
    story.append(Paragraph("Loss Prevention Audit", STYLE_H1))
    story.append(Paragraph(
        "Split into <b>LP-A: Real-Time Skim Vectors</b> (this week) and "
        "<b>LP-B: Structural Collusion</b> (trailing 90 days). Post-hoc "
        "manager approvals do not satisfy the pre-void gate control. "
        "Anonymous void_users (UUID / example.com emails) are P0.",
        STYLE_BODY,
    ))

    # ── Category tally ──
    same_minute = [f for f in lp_voids
                   if f.time_to_void_secs is not None and f.time_to_void_secs < 120]
    self_approved = [f for f in lp_voids if f.is_self_approved]
    cash_voids = [f for f in lp_voids if f.is_cash]
    anon_voiders = [f for f in lp_voids if _is_service_account(f.void_user)]

    story.append(Spacer(1, 0.08 * inch))
    story.append(Paragraph("SUMMARY", STYLE_H3))
    sum_rows = [
        ["Category", "Count", "$ Amount", "Severity"],
        ["Total post-payment voids (this wk)", str(len(lp_voids)),
         _money(sum(f.amount for f in lp_voids)), "—"],
        ["Cash voids", str(len(cash_voids)),
         _money(sum(f.amount for f in cash_voids)), "🔴 P0"],
        ["Self-approved voids", str(len(self_approved)),
         _money(sum(f.amount for f in self_approved)), "🔴 P0"],
        ["Same-minute voids (<120s)", str(len(same_minute)),
         _money(sum(f.amount for f in same_minute)), "🚨 URGENT"],
        ["Anonymous void_users (service account)", str(len(anon_voiders)),
         _money(sum(f.amount for f in anon_voiders)), "🚨 P0"],
        ["Repeat approver-server pairs (90d)", str(len(approver_pairs)),
         _money(sum(p.total_dollars for p in approver_pairs)), "🔴 P0"],
        ["Reason-vs-item mismatches", str(len(reason_mismatches)),
         _money(sum(r.amount for r in reason_mismatches)), "🚨 FRAUD"],
    ]
    st = Table(sum_rows, colWidths=[3.0 * inch, 0.8 * inch, 1.5 * inch, 1.5 * inch])
    st.setStyle(TABLE_STYLE_STANDARD)
    st.setStyle(TableStyle([("TEXTCOLOR", (3, 2), (3, -1), RED),
                             ("FONTNAME", (3, 2), (3, -1), "Helvetica-Bold")]))
    story.append(st)

    # ── LP-A · Top 5 largest voids sorted by $ (not time filter) ──
    if lp_voids:
        story.append(Spacer(1, 0.12 * inch))
        story.append(Paragraph(
            "LP-A · TOP 5 LARGEST VOIDS THIS WEEK (sorted by $)",
            STYLE_H3,
        ))
        top5 = sorted(lp_voids, key=lambda f: -f.amount)[:5]
        top_rows = [["Date", "Server", "Amount", "Tender", "Void By", "Approver",
                     "Time-to-Void", "Flags"]]
        for f in top5:
            flags = []
            if f.is_cash: flags.append("CASH")
            if f.is_self_approved: flags.append("SELF")
            if f.time_to_void_secs is not None and f.time_to_void_secs < 120: flags.append("<2m")
            if _is_service_account(f.void_user): flags.append("ANON")
            ttv = f"{f.time_to_void_secs}s" if f.time_to_void_secs is not None else "—"
            top_rows.append([
                f.processing_date, f.server[:14], _money(f.amount),
                f.payment_type[:8], f.void_user[:14],
                f.void_approver[:14] if f.void_approver else "—",
                ttv, " · ".join(flags),
            ])
        tt = Table(top_rows, colWidths=[0.75 * inch, 1.15 * inch, 0.75 * inch,
                                          0.65 * inch, 1.05 * inch, 1.0 * inch,
                                          0.75 * inch, 1.15 * inch])
        tt.setStyle(TABLE_STYLE_STANDARD)
        tt.setStyle(TableStyle([("TEXTCOLOR", (2, 1), (2, -1), RED),
                                 ("FONTNAME", (2, 1), (2, -1), "Helvetica-Bold")]))
        story.append(tt)

    # ── LP-A · Reason-vs-item mismatches (Spillage on bottle = fraud pattern) ──
    if reason_mismatches:
        story.append(Spacer(1, 0.12 * inch))
        story.append(Paragraph(
            "LP-A · REASON-CODE ABUSE — Spillage / Food Quality on Bottled Items",
            STYLE_H3,
        ))
        story.append(Paragraph(
            "<font color='#C97064'><b>You cannot spill a sealed bottle.</b></font> "
            "These rings used 'Spillage' or similar reasons on bottled liquor or "
            "OWNER SKUs. Classic 'steal the bottle, ring the comp' pattern — "
            "requires camera pull + inventory tie-out.",
            STYLE_BODY,
        ))
        rm_rows = [["Date", "Server", "Item", "Reason", "Amount"]]
        for r in reason_mismatches[:10]:
            rm_rows.append([
                r.processing_date, r.server[:14], r.menu_item[:22],
                r.reason[:24], _money(r.amount),
            ])
        rmt = Table(rm_rows, colWidths=[0.9 * inch, 1.2 * inch, 2.0 * inch,
                                          2.1 * inch, 1.0 * inch])
        rmt.setStyle(TABLE_STYLE_STANDARD)
        rmt.setStyle(TableStyle([("TEXTCOLOR", (4, 1), (4, -1), RED)]))
        story.append(rmt)

    story.append(PageBreak())

    # ── LP-B · Structural Collusion (trailing 90 days) ──
    story.append(Paragraph("Loss Prevention (continued)", STYLE_EYEBROW))
    story.append(Paragraph("LP-B · Structural Collusion (90-Day Lookback)", STYLE_H1))
    story.append(Paragraph(
        "Repeat approver→server void pairs across the trailing 90 days. "
        "A pattern of one manager always approving the same server's voids is "
        "the industry #1 collusion signal. Service-account void_users (UUID or "
        "example.com) are unauthorized voiders and require immediate POS config "
        "review.",
        STYLE_BODY,
    ))

    if approver_pairs:
        story.append(Spacer(1, 0.1 * inch))
        story.append(Paragraph("APPROVER → SERVER CONCENTRATION MATRIX", STYLE_H3))
        ap_rows = [["Approver (Manager)", "Server", "Void User", "Events", "$", "Anon?"]]
        for p in approver_pairs[:12]:
            ap_rows.append([
                p.approver[:20], p.server[:18], p.void_user[:22],
                str(p.events), _money(p.total_dollars),
                "🚨 YES" if p.void_user_is_service_account else "—",
            ])
        apt = Table(ap_rows, colWidths=[1.7 * inch, 1.5 * inch, 1.8 * inch,
                                          0.6 * inch, 0.8 * inch, 0.8 * inch])
        apt.setStyle(TABLE_STYLE_STANDARD)
        # Red highlight for service-account rows
        for i, p in enumerate(approver_pairs[:12], 1):
            if p.void_user_is_service_account:
                apt.setStyle(TableStyle([
                    ("BACKGROUND", (0, i), (-1, i), colors.HexColor("#fbe4e0")),
                    ("FONTNAME", (5, i), (5, i), "Helvetica-Bold"),
                    ("TEXTCOLOR", (5, i), (5, i), RED),
                ]))
        story.append(apt)

        story.append(Spacer(1, 0.1 * inch))
        top_pair = approver_pairs[0] if approver_pairs else None
        if top_pair:
            story.append(Paragraph(
                f"<b>Top pair:</b> {top_pair.approver} → {top_pair.server} "
                f"= {top_pair.events} events / {_money(top_pair.total_dollars)} in 90 days. "
                f"Review whether these voids reflect legitimate manager gate-keeping "
                f"or a rubber-stamp pattern. Camera pull + shift-schedule cross-reference "
                f"recommended for pairs ≥ 5 events.",
                STYLE_BODY,
            ))
    else:
        story.append(Paragraph("No repeat pairs detected in 90d — clean.", STYLE_BODY))

    # Anonymous voider callout
    anon_pairs = [p for p in approver_pairs if p.void_user_is_service_account]
    if anon_pairs:
        story.append(Spacer(1, 0.12 * inch))
        story.append(Paragraph(
            "🚨 ANONYMOUS VOIDER — POS Configuration Emergency",
            STYLE_H3,
        ))
        anon_total = sum(p.total_dollars for p in anon_pairs)
        story.append(Paragraph(
            f"<font color='#C97064'><b>{len(anon_pairs)} approver pair(s) involve "
            f"a service-account void_user ({_money(anon_total)} in 90 days).</b></font> "
            f"UUID / example.com email addresses in Toast void_user field indicate "
            f"either a shared login credential or a POS misconfiguration. Neither is "
            f"acceptable. Coordinate with Toast rep within 48 hours to identify the "
            f"human behind these voids and enable proper per-user login.",
            STYLE_BODY,
        ))

    story.append(PageBreak())
    return story


def build_bottle_manager_ledger(bm_audit: BMAudit, cur: CompPeriod) -> List:
    """Page 4 — Bottle Manager Ledger (direct answer to CEO's Q)."""
    story = []
    story.append(Paragraph("Bottle Manager", STYLE_EYEBROW))
    story.append(Paragraph("Bottle Manager Ledger", STYLE_H1))
    story.append(Paragraph(
        "Bottle Manager is a pooling station, not a person. This section itemizes "
        "every comp, void, and OWNER-SKU ring rung under 'Bottle Manager' for the "
        "reporting week. OWNER-SKU rings are owner-personal or owner-discretionary "
        "activity per §12 of the comp policy — not birthday-package delivery.",
        STYLE_BODY,
    ))

    bm = cur.bottle_manager
    story.append(Spacer(1, 0.1 * inch))
    story.append(Paragraph("SUMMARY", STYLE_H3))
    sum_rows = [
        ["Metric", "Value"],
        ["Total comps rung", str(bm.comp_count)],
        ["Total comp $", _money(bm.total_comp)],
        ["Voided items ≥ $100", f"{bm_audit.voids_over_100} ({_money(bm_audit.voids_dollars)})"],
        ["OWNER-SKU rings (owner personal/discretionary)",
         f"{bm_audit.owner_sku_rings} ({_money(bm_audit.owner_sku_dollars)})"],
        ["Owner bucket $", _money(bm.owner_comp)],
        ["Programmatic bucket $", _money(bm.programmatic_comp)],
        ["Discretionary bucket $", _money(bm.discretionary_comp)],
    ]
    st = Table(sum_rows, colWidths=[3.8 * inch, 3.4 * inch])
    st.setStyle(TABLE_STYLE_STANDARD)
    story.append(st)

    # Top items on bottle manager
    if bm_audit.top_items:
        story.append(Spacer(1, 0.12 * inch))
        story.append(Paragraph("TOP COMPED ITEMS BY $", STYLE_H3))
        item_rows = [["Item", "Rings", "$", "Sample Tab"]]
        for item, cnt, dol, tab in bm_audit.top_items:
            item_rows.append([item[:35], str(cnt), _money(dol), (tab or "—")[:24]])
        it = Table(item_rows, colWidths=[2.6 * inch, 0.7 * inch, 1.0 * inch, 2.9 * inch])
        it.setStyle(TABLE_STYLE_STANDARD)
        story.append(it)

    story.append(PageBreak())
    return story


def build_manager_scorecard(cur: CompPeriod, prev: CompPeriod,
                              behaviors: Dict[str, ManagerBehaviorMetrics]) -> List:
    """Page 5 — Manager Performance (Tiffany · Tony · Daja) with peer benchmarks."""
    story = []
    story.append(Paragraph("Manager Performance", STYLE_EYEBROW))
    story.append(Paragraph("Manager Scorecard — Tiffany · Tony · Daja · Ashley", STYLE_H1))
    story.append(Paragraph(
        "Peer-benchmarked KPIs for all four managers (Ashley merged into "
        "manager category based on duties — she approves Manager Comp checks "
        "up to $488 per audit). Metrics normalized per week; peer median shown "
        "for context. Diversity = number of distinct comp buckets touched; "
        "1/4 means every comp landed in a single bucket (usually 'Manager Comp') "
        "— training signal per policy §11.",
        STYLE_BODY,
    ))

    # Compute peer stats for benchmarking — only include managers who ACTUALLY
    # showed activity this week (comp_count > 0). Otherwise peer median is 0
    # because inactive managers with 0 comps drag the median to zero.
    all_mgr_scores = [cur.named_scorecards.get(n) for n in MANAGERS
                      if cur.named_scorecards.get(n)]
    active_scores = [s for s in all_mgr_scores if s.comp_count > 0]
    peer_source = active_scores if active_scores else all_mgr_scores
    peer_disc = [s.discretionary_comp for s in peer_source]
    peer_disc_median = sorted(peer_disc)[len(peer_disc)//2] if peer_disc else 0.0
    peer_diversity_median = (
        sorted([s.reason_code_diversity for s in peer_source])[len(peer_source)//2]
        if peer_source else 0
    )
    # Note transparency
    peer_note = (f"(median of {len(peer_source)} active manager{'s' if len(peer_source) != 1 else ''}"
                 + (" this week)" if active_scores else " — inactive weeks included, median = 0)"))

    # ── Peer Benchmark table ──
    story.append(Spacer(1, 0.1 * inch))
    story.append(Paragraph(f"PEER BENCHMARKS {peer_note}", STYLE_H3))
    # Compute peer median discretionary $/shift — active managers only
    per_shift_vals = [b.discretionary_per_shift
                       for b in behaviors.values()
                       if b.shifts_worked > 0 and b.discretionary_comp > 0]
    peer_per_shift_median = (sorted(per_shift_vals)[len(per_shift_vals)//2]
                              if per_shift_vals else 0.0)
    bench_rows = [
        ["Metric", "Individual Target", "Peer Median This Week"],
        ["Manager Discretionary $ per week", "≤ $500", _money(peer_disc_median)],
        ["Manager Discretionary $ per shift", "≤ $100 / shift", _money(peer_per_shift_median)],
        ["Reason-code diversity", "≥ 3 of 4 buckets", f"{peer_diversity_median} / 4"],
        ["Recovery approvals per week", "target driven by kitchen quality", "—"],
        ["Owner comp button use", "audit-trail only, no cap", "—"],
    ]
    bt = Table(bench_rows, colWidths=[3.0 * inch, 2.2 * inch, 2.0 * inch])
    bt.setStyle(TABLE_STYLE_STANDARD)
    story.append(bt)

    # ── Individual manager KPIs ──
    story.append(Spacer(1, 0.12 * inch))
    story.append(Paragraph("INDIVIDUAL SCORECARDS (shift-normalized)", STYLE_H3))
    mgr_rows = [["Manager", "Shifts", "Comps", "Discret $",
                 "Discret $/Shift", "Diversity", "vs Peer Median"]]
    for name in MANAGERS:
        s = cur.named_scorecards.get(name)
        b = behaviors.get(name)
        if not s:
            continue
        delta = s.discretionary_comp - peer_disc_median
        delta_str = f"+${delta:,.0f}" if delta > 0 else f"−${abs(delta):,.0f}"
        shifts_str = str(b.shifts_worked) if b else "—"
        if b and b.shifts_source == "floor_activity":
            shifts_str += "*"
        per_shift = f"${b.discretionary_per_shift:,.0f}" if b and b.shifts_worked else "—"
        mgr_rows.append([
            s.name, shifts_str, str(s.comp_count),
            _money(s.discretionary_comp),
            per_shift,
            f"{s.reason_code_diversity} / 4",
            delta_str,
        ])
    mt = Table(mgr_rows, colWidths=[1.5 * inch, 0.7 * inch, 0.7 * inch, 1.0 * inch,
                                     1.2 * inch, 0.9 * inch, 1.2 * inch])
    mt.setStyle(TABLE_STYLE_STANDARD)
    story.append(mt)
    # Footnote for fallback source
    if any(b and b.shifts_source == "floor_activity" for b in behaviors.values()):
        story.append(Paragraph(
            "<i>*Shifts inferred from floor activity (CashEntries + void approvals + "
            "check server + order server) — manager did not clock in via Toast Labor. "
            "Multi-signal union of processing dates; any one signal = shift worked.</i>",
            STYLE_SMALL,
        ))

    # ── Bad-Behavior Detectors (COO Sprint A) ──
    story.append(Spacer(1, 0.12 * inch))
    story.append(Paragraph(
        "BAD-BEHAVIOR DETECTORS (COO diagnostics)",
        STYLE_H3,
    ))
    story.append(Paragraph(
        "Six signals designed to expose bad manager practices before they scale "
        "to VIC3. Any 🔴 flag is a training conversation this week.",
        STYLE_SMALL,
    ))
    story.append(Spacer(1, 0.05 * inch))
    bhv_rows = [["Manager", "Self-Appr", "Round-$", "Late-Shift %",
                  "Rsn-Code Spec.", "Retail vs OWNR", "vs 4wk Med."]]
    for name in MANAGERS:
        b = behaviors.get(name)
        if not b:
            bhv_rows.append([name, "—", "—", "—", "—", "—", "—"])
            continue
        # Format each cell with red flag if concerning
        sa_str = (f"{b.self_approval_count} ({_money(b.self_approval_dollars)})"
                  if b.self_approval_count > 0 else "0")
        round_str = (f"{b.round_dollar_events} · {b.round_dollar_ratio:.0f}%"
                     if b.round_dollar_events > 0 else "0")
        late_str = (f"{b.late_shift_ratio:.0f}% ({_money(b.late_shift_dollars)})"
                    if b.late_shift_ratio > 0 else "0%")
        spec_str = f"{b.reason_specificity_pct:.0f}%"
        retail_str = f"{b.retail_vs_owner_mix_pct:.0f}%"
        trend_str = (f"{b.trend_vs_4wk_median_pct:+.0f}%"
                     if abs(b.trend_vs_4wk_median_pct) > 1 else "flat")
        bhv_rows.append([name[:16], sa_str, round_str, late_str,
                          spec_str, retail_str, trend_str])
    bt = Table(bhv_rows, colWidths=[1.4 * inch, 1.1 * inch, 0.85 * inch, 1.1 * inch,
                                      1.0 * inch, 0.85 * inch, 1.0 * inch])
    bt.setStyle(TABLE_STYLE_STANDARD)
    # Red highlight for concerning cells
    for i, name in enumerate(MANAGERS, 1):
        b = behaviors.get(name)
        if not b:
            continue
        if b.self_approval_count > 0:
            bt.setStyle(TableStyle([("TEXTCOLOR", (1, i), (1, i), RED),
                                     ("FONTNAME", (1, i), (1, i), "Helvetica-Bold")]))
        if b.round_dollar_ratio > 30:
            bt.setStyle(TableStyle([("TEXTCOLOR", (2, i), (2, i), RED)]))
        if b.late_shift_ratio > 60:
            bt.setStyle(TableStyle([("TEXTCOLOR", (3, i), (3, i), RED)]))
        if b.reason_specificity_pct < 30:
            bt.setStyle(TableStyle([("TEXTCOLOR", (4, i), (4, i), RED)]))
        if b.trend_vs_4wk_median_pct > 50:
            bt.setStyle(TableStyle([("TEXTCOLOR", (6, i), (6, i), RED)]))
    story.append(bt)
    story.append(Paragraph(
        "<i>Legend:</i> Self-Appr = void_user matches server. Round-$ = comps ≥$100 "
        "ending in .00 (goodwill guess pattern). Late-Shift % = $ rung between "
        "11pm-2:30am (end-of-shift dump). Rsn-Code Spec. = % using non-generic "
        "reasons. Retail vs OWNR = % using retail SKU when OWNER equivalent exists. "
        "vs 4wk Med. = this week vs manager's 4-week rolling average.",
        STYLE_SMALL,
    ))

    # ── Manager assignments ──
    story.append(Spacer(1, 0.12 * inch))
    story.append(Paragraph("THIS WEEK'S ASSIGNMENTS", STYLE_H3))
    for name in MANAGERS:
        s = cur.named_scorecards.get(name)
        if not s:
            continue
        top_items = sorted(
            [it for it in cur.item_log if it.server == name and it.discount > 0],
            key=lambda x: -x.discount,
        )[:3]
        if s.discretionary_comp >= 500:
            hint = f"Review top 3 discretionary comps"
            if top_items:
                hint += ": " + ", ".join(
                    f"#{it.check_id[-6:]} ({it.menu_item[:14]}) ${it.discount:.0f}"
                    for it in top_items
                )
            hint += " · reply by Fri EOD"
        elif s.reason_code_diversity <= 1 and s.comp_count >= 5:
            hint = "Diversify reason codes — attend §11 training Wed pre-shift"
        elif s.comp_count == 0:
            hint = "No comps this week — no action"
        else:
            hint = "On track — maintain reason-code discipline"
        story.append(Paragraph(f"<b>{name}:</b> {hint}", STYLE_ACTION))

    # ── Waterfall ──
    story.append(Spacer(1, 0.15 * inch))
    story.append(Paragraph("MANAGER DISCRETIONARY WATERFALL", STYLE_H3))
    named_disc = sum(s.discretionary_comp for s in all_mgr_scores)
    bar_lead_disc = sum(s.discretionary_comp for s in cur.named_scorecards.values()
                        if s.role == "Bar Lead")
    bm_disc = cur.bottle_manager.discretionary_comp
    uncateg = cur.uncategorized_dollars
    other = max(0, cur.by_bucket.get("discretionary_manager", 0.0)
                - named_disc - bar_lead_disc - bm_disc)

    w_rows = [
        ["Source", "$", "% of Mgr Disc"],
        ["Named Managers (Tiffany · Tony · Daja)", _money(named_disc), ""],
        ["Bar Lead (Ashley)", _money(bar_lead_disc), ""],
        ["Bottle Manager pooling station", _money(bm_disc), ""],
        ["Other (unattributed servers)", _money(other), ""],
        ["+ Uncategorized ring-ins", _money(uncateg), ""],
        ["Total Discretionary + Uncategorized", "", ""],
    ]
    total_wat = named_disc + bar_lead_disc + bm_disc + other + uncateg
    for i in range(1, 6):
        if total_wat > 0:
            v = [named_disc, bar_lead_disc, bm_disc, other, uncateg][i-1]
            w_rows[i][2] = f"{100*v/total_wat:.1f}%"
    w_rows[6][1] = _money(total_wat)
    w_rows[6][2] = "100.0%"
    wt = Table(w_rows, colWidths=[3.8 * inch, 1.4 * inch, 1.8 * inch])
    wt.setStyle(TABLE_STYLE_STANDARD)
    wt.setStyle(TableStyle([
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ("BACKGROUND", (0, -1), (-1, -1), CREAM),
        ("LINEABOVE", (0, -1), (-1, -1), 1, BLACK),
    ]))
    story.append(wt)

    # ── "Other" servers breakdown (transparency about the waterfall row) ──
    if other > 0:
        # Aggregate Manager Discretionary $ from servers NOT in MANAGERS
        # and NOT Bottle Manager
        other_rows_agg: Dict[str, float] = {}
        for it in cur.item_log:
            if (it.bucket == "discretionary_manager"
                and it.server
                and it.server not in MANAGERS
                and it.server != "Bottle Manager"):
                other_rows_agg[it.server] = other_rows_agg.get(it.server, 0.0) + it.discount
        story.append(Spacer(1, 0.1 * inch))
        story.append(Paragraph(
            "'OTHER (UNATTRIBUTED SERVERS)' — WHO'S RUNGING MANAGER COMPS?",
            STYLE_H3,
        ))
        story.append(Paragraph(
            "Servers who are NOT in the manager cohort but rang comps that "
            "landed in Manager Discretionary (typically because a manager approved "
            "at check level while the server appears as the ring-of-record).",
            STYLE_SMALL,
        ))
        top_other = sorted(other_rows_agg.items(), key=lambda kv: -kv[1])[:8]
        oth_rows = [["Server", "Discretionary $ rung"]]
        for name, amt in top_other:
            oth_rows.append([name, _money(amt)])
        if not top_other:
            oth_rows.append(["(none)", "$0"])
        ot = Table(oth_rows, colWidths=[3.6 * inch, 2.0 * inch])
        ot.setStyle(TABLE_STYLE_STANDARD)
        story.append(ot)

    # ── Uncategorized attribution (who's ringing Open $/%?) ──
    if uncateg > 0:
        uncateg_agg: Dict[str, float] = {}
        for row in [r for r in cur.flagged
                     if r.bucket in ("uncategorized_open_dollar", "uncategorized_open_pct")]:
            if row.server:
                uncateg_agg[row.server] = uncateg_agg.get(row.server, 0.0) + row.amount
        story.append(Spacer(1, 0.1 * inch))
        story.append(Paragraph(
            "UNCATEGORIZED RING-IN ATTRIBUTION — WHO NEEDS REASON-CODE TRAINING?",
            STYLE_H3,
        ))
        story.append(Paragraph(
            "Servers whose Open $ / Open % ring-ins account for this week's "
            "uncategorized total. Direct training targets.",
            STYLE_SMALL,
        ))
        top_unc = sorted(uncateg_agg.items(), key=lambda kv: -kv[1])[:8]
        unc_rows = [["Server", "Uncategorized $"]]
        for name, amt in top_unc:
            unc_rows.append([name, _money(amt)])
        if not top_unc:
            unc_rows.append(["(none)", "$0"])
        ut = Table(unc_rows, colWidths=[3.6 * inch, 2.0 * inch])
        ut.setStyle(TABLE_STYLE_STANDARD)
        ut.setStyle(TableStyle([("TEXTCOLOR", (1, 1), (1, -1), RED)]))
        story.append(ut)

    story.append(PageBreak())
    return story


def build_daypart_page(cur: CompPeriod, dayparts: List[DaypartComp]) -> List:
    """Sprint B — Comps by Daypart (bottle-service nightlife KPI)."""
    story = []
    story.append(Paragraph("Sprint B · Daypart Analysis", STYLE_EYEBROW))
    story.append(Paragraph("Comps by Daypart", STYLE_H1))
    story.append(Paragraph(
        "Nightlife industry expectation: 65%+ of comp $ concentrates in the "
        "11pm-1am peak. A blended weekly % masks where the real leakage lives. "
        "Late (1a-close) window with elevated comp % = end-of-shift dumping "
        "pattern (per LP audit).",
        STYLE_BODY,
    ))
    story.append(Spacer(1, 0.1 * inch))

    dp_rows = [["Daypart", "Comp $", "Comp Count", "Net Sales", "Comp %"]]
    total_comp = sum(d.comp_dollars for d in dayparts)
    for d in dayparts:
        share = 100.0 * d.comp_dollars / total_comp if total_comp else 0.0
        dp_rows.append([
            d.label,
            f"{_money(d.comp_dollars)} ({share:.0f}%)",
            str(d.comp_count),
            _money(d.net_sales),
            f"{d.comp_pct:.2f}%",
        ])
    dt = Table(dp_rows, colWidths=[2.0 * inch, 1.6 * inch, 1.1 * inch,
                                     1.3 * inch, 1.0 * inch])
    dt.setStyle(TABLE_STYLE_STANDARD)
    story.append(dt)

    # Peak concentration check
    peak = next((d for d in dayparts if d.label == "Peak (11p-1a)"), None)
    late = next((d for d in dayparts if d.label == "Late (1a-close)"), None)
    if peak and total_comp:
        peak_share = 100.0 * peak.comp_dollars / total_comp
        story.append(Spacer(1, 0.12 * inch))
        story.append(Paragraph(
            f"<b>Peak concentration:</b> {peak_share:.0f}% "
            f"({'on target' if peak_share >= 60 else 'below the ≥65% industry benchmark for bottle-service venues'})",
            STYLE_BODY,
        ))
    if late and total_comp:
        late_share = 100.0 * late.comp_dollars / total_comp
        color = RED if late_share > 20 else BLACK
        story.append(Paragraph(
            f"<font color='{color.hexval()}'><b>Late (1a-close) share:</b> "
            f"{late_share:.0f}% — {'elevated · end-of-shift dumping likely' if late_share > 20 else 'within tolerance'}</font>",
            STYLE_BODY,
        ))

    story.append(PageBreak())
    return story


def build_trend_page(snapshots: List[TrendSnapshot]) -> List:
    """Sprint B — 4-week rolling trend on 3 headline metrics."""
    story = []
    story.append(Paragraph("Sprint B · Trend Analysis", STYLE_EYEBROW))
    story.append(Paragraph("4-Week Rolling Trend", STYLE_H1))
    story.append(Paragraph(
        "Snapshot vs trajectory. A single-week grade doesn't tell you whether "
        "discipline is drifting or recovering. Below: the last 4 completed weeks "
        "for blended comp %, manager discretionary %, and recovery %. Delta "
        "columns show week-over-week movement.",
        STYLE_BODY,
    ))
    story.append(Spacer(1, 0.1 * inch))

    tr_rows = [["Week", "Net Sales", "Total Comp", "Blended %",
                "Mgr Disc %", "Recovery %"]]
    for s in snapshots:
        tr_rows.append([
            s.label,
            _money(s.net_sales),
            _money(s.total_comp),
            f"{s.blended_pct:.2f}%",
            f"{s.manager_disc_pct:.2f}%",
            f"{s.recovery_pct:.2f}%",
        ])
    tt = Table(tr_rows, colWidths=[1.5 * inch, 1.3 * inch, 1.2 * inch, 1.0 * inch,
                                     1.1 * inch, 1.1 * inch])
    tt.setStyle(TABLE_STYLE_STANDARD)
    story.append(tt)

    # Trend commentary
    if len(snapshots) >= 2:
        story.append(Spacer(1, 0.12 * inch))
        blend_delta = snapshots[-1].blended_pct - snapshots[0].blended_pct
        mgr_delta = snapshots[-1].manager_disc_pct - snapshots[0].manager_disc_pct
        direction_b = "improving" if blend_delta < 0 else "worsening"
        direction_m = "improving" if mgr_delta < 0 else "worsening"
        story.append(Paragraph(
            f"<b>4-week trajectory:</b> Blended {direction_b} by "
            f"{abs(blend_delta):.2f}pp · Manager Discretionary {direction_m} by "
            f"{abs(mgr_delta):.2f}pp.",
            STYLE_BODY,
        ))
        # 4-wk average
        avg_blend = sum(s.blended_pct for s in snapshots) / len(snapshots)
        avg_mgr = sum(s.manager_disc_pct for s in snapshots) / len(snapshots)
        story.append(Paragraph(
            f"<b>4-week averages:</b> Blended {avg_blend:.2f}% · "
            f"Manager Discretionary {avg_mgr:.2f}%",
            STYLE_BODY,
        ))

    story.append(PageBreak())
    return story


def build_bar_lead_scorecard(cur: CompPeriod, prev: CompPeriod,
                              recon: BirthdayReconciliationResult) -> List:
    """Page 6 — Bar Lead Scorecard (Ashley) — own KPI category."""
    story = []
    story.append(Paragraph("Bar Lead Performance", STYLE_EYEBROW))
    story.append(Paragraph("Bar Lead Scorecard — Ashley Baines", STYLE_H1))
    story.append(Paragraph(
        "Ashley is tracked in her own performance category — her volume profile "
        "is legitimately different from floor managers (bar spillage recovery, "
        "birthday-package champagne delivery, high item-ring volume). Metrics "
        "here are peer-of-one; benchmarks are her own 4-week trend and role-"
        "specific targets.",
        STYLE_BODY,
    ))

    a = cur.named_scorecards.get("Ashley Baines")
    a_prev = prev.named_scorecards.get("Ashley Baines")

    # Bar Lead specific metrics for the current week
    ashley_items = [it for it in cur.item_log if it.server == "Ashley Baines"
                    and it.discount > 0]
    # Recovery = tab starts with Spill/Bug/Broke
    recovery_items = [it for it in ashley_items
                     if it.tab_name and any(
                         it.tab_name.lower().startswith(kw) or "broke" in it.tab_name.lower()
                         for kw in ("spill", "bug", "bottle broke")
                     )]
    # Birthday delivery items (OWNER champagne on bday tabs)
    bday_champagne = [it for it in ashley_items
                      if it.tab_name and ("bday" in it.tab_name.lower()
                                          or "birthday" in it.tab_name.lower())
                      and any(kw in it.menu_item.lower()
                              for kw in ("moet", "bellaire", "wycliff"))]

    story.append(Spacer(1, 0.1 * inch))
    story.append(Paragraph("BAR LEAD KPIs", STYLE_H3))
    kpi_rows = [
        ["Metric", "This Week", "Prior Week", "Role Target"],
        ["Total comp count",
         str(a.comp_count if a else 0),
         str(a_prev.comp_count if a_prev else 0),
         "role-driven"],
        ["Discretionary $",
         _money(a.discretionary_comp if a else 0),
         _money(a_prev.discretionary_comp if a_prev else 0),
         "≤ $500 / week"],
        ["Recovery items (Spill / Bug / Broke)",
         f"{len(recovery_items)} items · {_money(sum(i.discount for i in recovery_items))}",
         "—",
         "≤ 20 items · ≤ $200"],
        ["Birthday champagne rings (Fri / Sat)",
         f"{len(bday_champagne)} bottles · {_money(sum(i.gross_price for i in bday_champagne))}",
         "—",
         "1 per eligible pre-reg party"],
        ["Reason-code diversity",
         f"{a.reason_code_diversity if a else 0} / 4",
         f"{a_prev.reason_code_diversity if a_prev else 0} / 4",
         "≥ 3 / 4 buckets"],
    ]
    kt = Table(kpi_rows, colWidths=[2.6 * inch, 1.8 * inch, 1.2 * inch, 1.6 * inch])
    kt.setStyle(TABLE_STYLE_STANDARD)
    story.append(kt)

    # ── Birthday delivery ratio (Ashley-specific) ──
    fri_sat_pre_reg = [r for r in recon.eligible_reservations
                       if date.fromisoformat(r.date).weekday() in (4, 5)]
    story.append(Spacer(1, 0.12 * inch))
    story.append(Paragraph("BIRTHDAY DELIVERY RATIO (Fri / Sat)", STYLE_H3))
    story.append(Paragraph(
        f"Fri/Sat pre-registered parties this week: <b>{len(fri_sat_pre_reg)}</b> · "
        f"Ashley's OWNER champagne rings on bday tabs: <b>{len(bday_champagne)}</b> · "
        f"Coverage: <b>"
        f"{100 * len(bday_champagne) / max(len(fri_sat_pre_reg), 1):.0f}%"
        f"</b> (target 100% via Bday-F/S-{{Name}} convention)",
        STYLE_BODY,
    ))

    # ── Assignment ──
    story.append(Spacer(1, 0.12 * inch))
    story.append(Paragraph("THIS WEEK'S ASSIGNMENT", STYLE_H3))
    if a and a.discretionary_comp >= 500:
        assign = "Review top 3 discretionary comps · reply by Fri EOD"
    elif len(recovery_items) > 20:
        assign = "Recovery volume elevated — root-cause with kitchen"
    elif not bday_champagne and len(fri_sat_pre_reg) > 0:
        assign = "Adopt Bday-F/S-{FirstName} tab naming so delivery is attributable"
    else:
        assign = "On track — continue current pattern"
    story.append(Paragraph(f"<b>Ashley:</b> {assign}", STYLE_ACTION))

    story.append(PageBreak())
    return story


def build_birthday_page(recon: BirthdayReconciliationResult) -> List:
    """Page 6 — Birthday Reconciliation vs SR."""
    story = []
    story.append(Paragraph("Birthday Package Compliance", STYLE_EYEBROW))
    story.append(Paragraph("Birthday Reconciliation vs SevenRooms", STYLE_H1))
    story.append(Paragraph(
        "<b>Eligibility rule:</b> Only reservations with 'Birthday Dinner Package' "
        "in SR notes are entitled to complimentary champagne per policy §9. "
        "Birthday-tagged only reservations have no comp obligation. "
        "<b>Day-of-week program:</b> Wed=Bellaire (comped), Fri/Sat=OWNER Moet (cost basis), "
        "Sun=Bellaire (retail). Mon/Tue/Thu have no package program.",
        STYLE_BODY,
    ))

    story.append(Spacer(1, 0.1 * inch))
    story.append(Paragraph("SUMMARY", STYLE_H3))
    pre_reg = sum(1 for r in recon.reservations if r.is_pre_registered)
    eligible = len(recon.eligible_reservations)
    delivered = [r for r in recon.eligible_reservations
                 if r.status in (DELIVERED_ATTRIBUTED, DELIVERED_COMPED,
                                 DELIVERED_COST_BASIS, DELIVERED_OFF_BOOK)]
    failures = [r for r in recon.reservations if r.status == NOT_DELIVERED]

    sum_rows = [
        ["Metric", "Value"],
        ["Total birthday-related SR reservations", str(len(recon.reservations))],
        ["Pre-registered for Birthday Dinner Package", str(pre_reg)],
        ["Eligible for delivery obligation this week", str(eligible)],
        ["Delivered per program", str(len(delivered))],
        ["Policy failures 🔴", str(len(failures))],
        ["Delivery rate", f"{recon.delivery_rate:.1f}%"],
    ]
    st = Table(sum_rows, colWidths=[3.8 * inch, 3.4 * inch])
    st.setStyle(TABLE_STYLE_STANDARD)
    if failures:
        st.setStyle(TableStyle([("TEXTCOLOR", (1, 5), (1, 5), RED),
                                ("FONTNAME", (1, 5), (1, 5), "Helvetica-Bold")]))
    story.append(st)

    # Policy failures detail
    if failures:
        story.append(Spacer(1, 0.12 * inch))
        story.append(Paragraph("POLICY FAILURES — pre-reg parties with no champagne", STYLE_H3))
        f_rows = [["Date", "Day", "Guest", "Party", "Spend"]]
        for r in failures[:12]:
            f_rows.append([
                r.date,
                date.fromisoformat(r.date).strftime("%a"),
                r.guest_label[:22],
                str(r.party_size),
                _money(r.total_spend),
            ])
        ft = Table(f_rows, colWidths=[1.1 * inch, 0.7 * inch, 2.6 * inch, 0.6 * inch, 1.0 * inch])
        ft.setStyle(TABLE_STYLE_STANDARD)
        ft.setStyle(TableStyle([("TEXTCOLOR", (4, 1), (4, -1), RED)]))
        story.append(ft)

    # Manager end-of-shift reminder
    story.append(Spacer(1, 0.12 * inch))
    story.append(Paragraph("END-OF-SHIFT MANAGER RITUAL", STYLE_H3))
    story.append(Paragraph(
        "At close each Wed/Fri/Sat/Sun: (1) pull SR pre-reg birthday list; "
        "(2) search Toast for <b>Bday-{TodayLetter}-</b> tabs; (3) 1:1 match every "
        "pre-reg guest; (4) note any missed guest with reason; (5) post to "
        "<b>#lov3-leader-report</b>: <i>'Sat Bday: 5/7 delivered. Missed: Sarai "
        "(declined), Chelsea (left early)'</i>. See TAB_NAMING_STANDARDS.md §10.2.",
        STYLE_BODY,
    ))

    story.append(PageBreak())
    return story


def build_best_practices(cur: CompPeriod,
                          recon: BirthdayReconciliationResult) -> List:
    """Page 8 — Best Practices identified from data analysis (staff-facing)."""
    story = []
    story.append(Paragraph("Learning From Excellence", STYLE_EYEBROW))
    story.append(Paragraph("Best Practices — Staff Highlights", STYLE_H1))
    story.append(Paragraph(
        "Patterns identified during audit that other staff should emulate. "
        "Each practice is tied to the manager or server who demonstrated it. "
        "Weekly recognition of these builds the operating culture we want.",
        STYLE_BODY,
    ))

    story.append(Spacer(1, 0.1 * inch))
    story.append(Paragraph("PATTERNS TO EMULATE", STYLE_H3))

    practices = [
        {
            "who": "Michelle Rojas & Jordyn Aiken",
            "what": "Wednesday Birthday Package delivery — textbook execution",
            "detail": "Rings the birthday party's Bellaire Rose BTL FULLY COMPED "
                      "($169) on a properly-named 'Wednesday Bday Package- E1/E2' "
                      "tab. This is the intended policy pattern (§9.1 Birthday "
                      "Package). Verified in the data multiple times over 90 days."
        },
        {
            "who": "Tiffany Loving",
            "what": "Owner Tasting tab naming — correct attribution",
            "detail": "Uses 'Lalo Tasting' tab format when hosting owner tasting "
                      "events. Reconciler cleanly routes to Owner Discretionary "
                      "bucket without ambiguity. Sets the standard for §3 Owner "
                      "Tasting convention in TAB_NAMING_STANDARDS.md."
        },
        {
            "who": "Dajah Bishop",
            "what": "Reason-code diversity — cleanest of the manager cohort",
            "detail": "Uses Owner Comp, Birthday, and Manager Comp reason codes "
                      "in their proper contexts (3/4 buckets over 90 days). "
                      "Contrast: Tony's 1/4 (all Manager Comp) reflects category "
                      "collapse — the drift the policy §11 wants to prevent."
        },
        {
            "who": "Laiba Ejaz",
            "what": "Wycliffe host-stand welcome tracking",
            "detail": "Rings OWNER WYCLIFF BTL on 'Door' tabs when pouring the "
                      "waiting-guest welcome champagne. Only server consistently "
                      "capturing this program per policy §10, making Wycliffe "
                      "inventory pull traceable."
        },
        {
            "who": "Bottle Manager (station discipline)",
            "what": "Owner tab attribution — clear naming",
            "detail": "Uses 'Per Maurice', 'Maurice E9', 'Owner Maurice' tab "
                      "conventions when running owner-designated tabs. "
                      "Reconciler correctly attributes to Owner Personal without "
                      "ambiguity (§3.1)."
        },
    ]

    # Insert a "this week" callout if any manager beat the peer median
    top_this_week = None
    for name in MANAGERS + BAR_LEADS:
        s = cur.named_scorecards.get(name)
        if not s:
            continue
        if s.reason_code_diversity >= 3 and s.comp_count > 0 and s.discretionary_comp < 500:
            top_this_week = name
            break
    if top_this_week:
        practices.insert(0, {
            "who": top_this_week + " (this week)",
            "what": "Clean discipline this reporting week",
            "detail": (
                f"Ran with high reason-code diversity + discretionary comps under "
                f"target this week. Sustained performance qualifies for quarterly "
                f"recognition per policy §11.3."
            ),
        })

    for p in practices:
        story.append(Paragraph(
            f"<b>{p['who']}</b> · <i>{p['what']}</i>",
            STYLE_ACTION,
        ))
        story.append(Paragraph(p["detail"], STYLE_SMALL))
        story.append(Spacer(1, 0.06 * inch))

    story.append(PageBreak())
    return story


def build_risks_opportunities(cur: CompPeriod,
                                recon: BirthdayReconciliationResult,
                                lp_voids: List[LPVoidRecord],
                                bm_audit: BMAudit) -> List:
    """Page 9 — Risks & Opportunities identified from analysis."""
    story = []
    story.append(Paragraph("Forward Look", STYLE_EYEBROW))
    story.append(Paragraph("Risks & Opportunities", STYLE_H1))
    story.append(Paragraph(
        "Systemic risks (leakage / fraud / non-compliance) and opportunities "
        "(revenue recovery / process improvement) surfaced during the audit. "
        "Each item is either monitored, in progress, or awaiting decision.",
        STYLE_BODY,
    ))

    # Compute observations dynamically
    tier2_foregone = sum(m.foregone_revenue for m in cur.tier_2_movements.values())
    cash_voids = [f for f in lp_voids if f.is_cash]
    self_approved = [f for f in lp_voids if f.is_self_approved]
    failures = [r for r in recon.reservations if r.status == NOT_DELIVERED]

    risks = []
    opportunities = []

    if cash_voids or self_approved:
        risks.append((
            "Post-payment cash/self-approved voids",
            f"{len(cash_voids)} cash voids · {len(self_approved)} self-approved. "
            f"Highest-loss hospitality fraud vector. Manager gate-keeping not "
            f"enforced at time-of-void."
        ))
    if tier2_foregone > 3000:
        risks.append((
            "Tier 2 bottle leakage",
            f"${tier2_foregone:,.0f} in retail value moved with no cost recovery. "
            f"OWNER-SKU rings used to substitute for retail sales."
        ))
    if failures:
        risks.append((
            "Birthday-package policy failures",
            f"{len(failures)} pre-registered parties this week received no "
            f"champagne despite meeting the $200 minimum on a program day."
        ))
    if cur.uncategorized_dollars > UNCATEGORIZED_ALERT_THRESHOLD:
        risks.append((
            "Uncategorized ring-in discipline",
            f"${cur.uncategorized_dollars:,.0f} in Open $/% ring-ins with no "
            f"reason code — cannot classify or audit."
        ))
    if bm_audit.owner_sku_rings > 5:
        risks.append((
            "Bottle Manager station anonymity",
            f"{bm_audit.owner_sku_rings} OWNER-SKU rings under the pooling "
            f"station this week ({_money(bm_audit.owner_sku_dollars)}). "
            f"Personal accountability blocked until BM role is formalized."
        ))
    for cap in cur.promoter_caps:
        if cap.is_over_cap:
            risks.append((
                f"Promoter cap breach — {cap.event}",
                f"{cap.excess_bottles} bottle(s) over cap. "
                f"{'Clawback billable' if cap.cap_type == 'external' else 'In-house excess spend'}."
            ))

    # Opportunities
    opportunities.append((
        "Adopt Bday-{D}-{Name} tab naming",
        "Deterministic 1:1 reconciliation, no inventory double-count. Ends the "
        "'off-book speculative' delivery bucket. Adds ~5 min to end-of-shift "
        "close. Ready to roll out this week."
    ))
    opportunities.append((
        "Dedicated Birthday Champagne SKU",
        "Distinguishes birthday-package delivery from OWNER-SKU owner-discretionary "
        "consumption at the ring-in level. Removes the classifier's tab-name "
        "dependency. Coordinate with Toast rep."
    ))
    opportunities.append((
        "Replicate Michelle's Wed pattern to Fri/Sat/Sun",
        "Standardize the Bellaire-fully-comped delivery Michelle uses on Wed to "
        "the other program days. Simpler reconciliation and matches policy §9.1 "
        "'complimentary champagne bottle' language."
    ))
    opportunities.append((
        "PIN-lock Owner Comp button",
        "Ownership retains audit visibility on every use. Pending §16.1 leadership "
        "decision."
    ))
    opportunities.append((
        "Formalize Bottle Manager as named role",
        "Convert pooling station to scheduled role with named assignments. Preserves "
        "pooling economics while unlocking accountability. Pending §16.3 decision."
    ))
    if bm_audit.voids_over_100 > 0:
        opportunities.append((
            "Bottle void root-cause investigation",
            f"{bm_audit.voids_over_100} high-value bottle voids "
            f"({_money(bm_audit.voids_dollars)}) at the Bottle Manager station "
            f"this week. Common root causes: ring-in errors, guest changed order, "
            f"breakage. Establish a void-reason POS prompt."
        ))

    # Render Risks
    story.append(Spacer(1, 0.1 * inch))
    story.append(Paragraph("RISKS", STYLE_H3))
    if risks:
        for title, detail in risks:
            story.append(Paragraph(
                f"<font color='#C97064'><b>▲ {title}</b></font>",
                STYLE_ACTION,
            ))
            story.append(Paragraph(detail, STYLE_SMALL))
            story.append(Spacer(1, 0.05 * inch))
    else:
        story.append(Paragraph("✓ No material risks surfaced this week.", STYLE_BODY))

    # Render Opportunities
    story.append(Spacer(1, 0.1 * inch))
    story.append(Paragraph("OPPORTUNITIES", STYLE_H3))
    for title, detail in opportunities:
        story.append(Paragraph(
            f"<font color='#7FB069'><b>◆ {title}</b></font>",
            STYLE_ACTION,
        ))
        story.append(Paragraph(detail, STYLE_SMALL))
        story.append(Spacer(1, 0.05 * inch))

    story.append(PageBreak())
    return story


def build_action_plan(cur: CompPeriod,
                       recon: BirthdayReconciliationResult,
                       lp_voids: List[LPVoidRecord]) -> List:
    """Page 10 — 30/60/90-day Action Plan with owners + success criteria."""
    story = []
    story.append(Paragraph("Roadmap", STYLE_EYEBROW))
    story.append(Paragraph("Action Plan — 30 / 60 / 90 Days", STYLE_H1))
    story.append(Paragraph(
        "Roadmap to close discipline gaps and lock in best practices. Each action "
        "carries an owner, deadline, and success criterion measurable in the "
        "weekly report. Tracked at the Tuesday leadership meeting.",
        STYLE_BODY,
    ))

    # ── 30 DAYS ──
    story.append(Spacer(1, 0.12 * inch))
    story.append(Paragraph("NEXT 30 DAYS — Immediate", STYLE_H3))
    d30 = [
        ["Action", "Owner", "Success Criterion", "Deadline"],
        ["Roll out Bday-{D}-{Name} tab naming convention",
         "Ashley + all managers", "≥ 50% adoption on Fri/Sat", "2026-08-06 pre-shift"],
        ["Same-day cash-drawer review + camera pull for flagged voids",
         "Ownership", "Report to LP file", "24 hours after flag"],
        ["Train Tony on reason-code diversity (§11)",
         "Maurice + Tony", "Diversity ≥ 3/4 in weekly report", "2026-08-05"],
        ["Investigate Sat 06-20 & 07-18 shift roster (5+ birthday policy failures each)",
         "Ashley + shift managers", "Root cause documented", "2026-08-08"],
        ["Replicate Michelle's Wed Bellaire pattern to other days",
         "Ashley (Bar Lead)", "1 fully-comped Bellaire per Fri/Sat pre-reg party",
         "2026-08-06"],
        ["Enforce reason codes on Open $ / Open % ring-ins",
         "All managers", "Uncategorized $ = $0 in weekly report", "2026-08-13"],
    ]
    if any(f.is_self_approved for f in lp_voids):
        d30.append([
            "Implement pre-void manager PIN gate at POS",
            "Maurice + Toast rep",
            "Zero self-approved voids in weekly report",
            "2026-08-13",
        ])
    d30_wrapped = [d30[0]] + [_wrap_row(r) for r in d30[1:]]
    dt = Table(d30_wrapped, colWidths=[2.3 * inch, 1.5 * inch, 2.2 * inch, 1.0 * inch])
    dt.setStyle(TABLE_STYLE_STANDARD)
    story.append(dt)

    # ── 60 DAYS ──
    story.append(Spacer(1, 0.15 * inch))
    story.append(Paragraph("NEXT 60 DAYS — Structural", STYLE_H3))
    d60 = [
        ["Action", "Owner", "Success Criterion", "Deadline"],
        ["Deploy dedicated Birthday Champagne SKU family",
         "Maurice + Toast rep",
         "SKU-based birthday detection (no tab-name dependency)",
         "2026-08-30"],
        ["Formalize Bottle Manager into named role with schedule",
         "Ownership + Ops",
         "Every BM shift has a named operator on the roster",
         "2026-09-01"],
        ["PIN-lock Owner Comp button (§16.1 decision)",
         "Ownership",
         "Slack notification on every Owner Comp use", "2026-08-20"],
        ["Wycliffe host-stand systematic ring-in enforcement",
         "Front-of-house + host stand",
         "Wycliffe bottle pull matches Toast rings weekly", "2026-08-27"],
        ["Manager quarterly recognition ceremony (policy §11.3)",
         "Ownership",
         "Top-diversity manager named at Sept leadership meeting",
         "2026-09-15"],
    ]
    d60_wrapped = [d60[0]] + [_wrap_row(r) for r in d60[1:]]
    d6t = Table(d60_wrapped, colWidths=[2.3 * inch, 1.5 * inch, 2.2 * inch, 1.0 * inch])
    d6t.setStyle(TABLE_STYLE_STANDARD)
    story.append(d6t)

    # ── 90 DAYS ──
    story.append(Spacer(1, 0.15 * inch))
    story.append(Paragraph("NEXT 90 DAYS — Strategic", STYLE_H3))
    d90 = [
        ["Action", "Owner", "Success Criterion", "Deadline"],
        ["Migrate comp policy compliance into automated Slack alerts",
         "Maurice + Analytics",
         "Real-time alerts on policy breaches", "2026-10-01"],
        ["Complete VIP Comp bucket definition + guest list (§16.2)",
         "Ownership",
         "VIP monthly cap defined + tracked", "2026-10-15"],
        ["Establish quarterly leadership policy review cadence",
         "Ownership + Managers",
         "First formal review complete", "2026-10-30"],
        ["Bar Lead pattern documentation for training onboarding",
         "Ashley + Maurice",
         "Playbook signed off; used in next bar hire",
         "2026-10-30"],
        ["Roll out staff training program from TAB_NAMING_STANDARDS.md",
         "All managers",
         "100% shift-meeting attendance tracked", "2026-11-15"],
    ]
    d90_wrapped = [d90[0]] + [_wrap_row(r) for r in d90[1:]]
    d9t = Table(d90_wrapped, colWidths=[2.3 * inch, 1.5 * inch, 2.2 * inch, 1.0 * inch])
    d9t.setStyle(TABLE_STYLE_STANDARD)
    story.append(d9t)

    story.append(PageBreak())
    return story


def build_promoter_recap(cur: CompPeriod) -> List:
    """Page 7 — Promoter Recap (prose per COO)."""
    story = []
    story.append(Paragraph("Promoter Discipline", STYLE_EYEBROW))
    story.append(Paragraph("Promoter Cap Recap", STYLE_H1))
    story.append(Paragraph(
        "Each promoter event's actual bottle count versus the per-event cap. "
        "External promoter overages incur 80% clawback against next payout; "
        "in-house event overages log as excess promotional spend.",
        STYLE_BODY,
    ))
    story.append(Paragraph(
        "<b>Attribution rule (interim until tab-naming standard rolls out):</b> "
        "Any comped bottle that is (a) rung on the event's day of week, "
        "(b) NOT on an owner tab (Maurice / Eddie / Derwin), and "
        "(c) NOT on a birthday tab (<code>Bday</code> / <code>Birthday</code>) "
        "is counted toward that day's promoter event. Sunday defaults to "
        "Cassette (late-night bottle service) when no explicit DAE7 signal. "
        "This will tighten to explicit <code>Promoter - {Day} - {Event}</code> "
        "tab-name matching once staff adoption reaches 80%.",
        STYLE_SMALL,
    ))

    story.append(Spacer(1, 0.1 * inch))
    for cap in cur.promoter_caps:
        if cap.is_over_cap and cap.cap_type == "external":
            status = f"🔴 <b>{cap.event}</b> ({cap.poc}): {cap.bottle_count}/{cap.cap} bottles — <b>OVER by {cap.excess_bottles}</b> → ~${cap.excess_bottles * 0.8 * AVG_TIER_1_RETAIL:,.0f} clawback billed"
        elif cap.is_over_cap:
            status = f"🟡 <b>{cap.event}</b> ({cap.poc}, in-house): {cap.bottle_count}/{cap.cap} — <b>+{cap.excess_bottles} excess in-house spend</b>"
        elif cap.bottle_count == 0:
            status = f"⬜ <b>{cap.event}</b> ({cap.poc}): 0/{cap.cap} bottles this week"
        else:
            status = f"🟢 <b>{cap.event}</b> ({cap.poc}): {cap.bottle_count}/{cap.cap} bottles · clean"
        story.append(Paragraph(status, STYLE_ACTION))

    story.append(PageBreak())
    return story


def build_return_to_green(cur: CompPeriod, recon: BirthdayReconciliationResult) -> List:
    """Page 8 — Return-to-Green Plan."""
    story = []
    story.append(Paragraph("Recovery Plan", STYLE_EYEBROW))
    story.append(Paragraph("Return-to-Green Plan", STYLE_H1))
    story.append(Paragraph(
        "Where the current week landed in the red, the corrective actions with "
        "estimated $ recovery and target owner + deadline. Each item is trackable "
        "against next week's report.",
        STYLE_BODY,
    ))

    actions = []
    # Uncategorized recovery
    if cur.uncategorized_dollars > 500:
        actions.append([
            "Fix Uncategorized Ring-Ins",
            f"Enforce reason codes on {sum(cur.by_bucket_count.get(k, 0) for k in ('uncategorized_open_dollar', 'uncategorized_open_pct'))} open $/% ring-ins",
            _money(cur.uncategorized_dollars),
            "Anthony Winn",
            "Fri EOD",
        ])
    # Tier 2 review
    for name, m in sorted(cur.tier_2_movements.items(), key=lambda kv: -kv[1].foregone_revenue)[:2]:
        if m.foregone_revenue >= 500:
            actions.append([
                f"Review Tier 2 house-comps ({name})",
                f"{m.house_comped_count} bottles house-comped without cost recovery",
                _money(m.foregone_revenue),
                "Ownership",
                "Next Tue",
            ])
    # Cap breaches
    for cap in cur.promoter_caps:
        if cap.is_over_cap and cap.cap_type == "external":
            actions.append([
                f"Bill {cap.poc} clawback",
                f"{cap.event} over by {cap.excess_bottles} bottle(s)",
                _money(cap.excess_bottles * 0.8 * AVG_TIER_1_RETAIL),
                "Maurice Ragland",
                "Next payout",
            ])
    # Birthday policy failures
    failures = [r for r in recon.reservations
                if r.status == NOT_DELIVERED]
    if failures:
        actions.append([
            "Investigate birthday policy failures",
            f"{len(failures)} pre-reg parties missed champagne this week",
            "—",
            "Ashley Baines + shift managers",
            "Weds pre-shift",
        ])
    # Manager discretionary review
    for name, s in cur.named_scorecards.items():
        if s.discretionary_comp >= 500 and s.role == "Manager":
            actions.append([
                f"{name} discretionary review",
                f"${s.discretionary_comp:,.0f} discretionary this week",
                "N/A — review only",
                name,
                "Fri EOD",
            ])

    if not actions:
        actions.append(["No corrective actions this week", "Metrics within tolerance",
                       "—", "—", "—"])

    story.append(Spacer(1, 0.1 * inch))
    a_rows = [["Action", "Detail", "$ Recovery", "Owner", "Deadline"]] + \
              [_wrap_row(r) for r in actions]
    at = Table(a_rows, colWidths=[2.0 * inch, 2.6 * inch, 0.8 * inch,
                                    1.2 * inch, 0.9 * inch])
    at.setStyle(TABLE_STYLE_STANDARD)
    story.append(at)

    story.append(PageBreak())
    return story


def build_appendix(cur: CompPeriod, recon: BirthdayReconciliationResult) -> List:
    """Page 9-10 — Appendices A/B/C/D/E."""
    story = []
    story.append(Paragraph("Page 9+ · Appendices", STYLE_EYEBROW))
    story.append(Paragraph("Supporting Detail & Methodology", STYLE_H1))

    # Appendix A — Item log
    story.append(Paragraph("APPENDIX A — Item Log (top 25 by discount $)", STYLE_H2))
    log_rows = [["Date", "Server", "Item", "Tab", "$"]]
    for it in sorted(cur.item_log, key=lambda x: -x.discount)[:25]:
        log_rows.append([
            it.processing_date, (it.server or "—")[:16], it.menu_item[:24],
            (it.tab_name or "—")[:20], _money(it.discount),
        ])
    lt = Table(log_rows, colWidths=[0.9 * inch, 1.3 * inch, 2.0 * inch,
                                     1.7 * inch, 0.8 * inch])
    lt.setStyle(TABLE_STYLE_STANDARD)
    story.append(lt)

    # Appendix B — SKU/Reason Mismatches
    story.append(Spacer(1, 0.12 * inch))
    story.append(Paragraph("APPENDIX B — SKU vs Reason-Code Mismatches", STYLE_H2))
    if cur.mismatches:
        m_rows = [["Server", "Item / Signal", "Reason Bucket", "Final Bucket", "$"]]
        for m in sorted(cur.mismatches, key=lambda x: -x.amount)[:15]:
            m_rows.append([
                (m.server or "—")[:16],
                m.top_item[:24],
                m.reason_bucket,
                m.item_bucket,
                _money(m.amount),
            ])
        mt = Table(m_rows, colWidths=[1.4 * inch, 2.0 * inch, 1.4 * inch,
                                       1.4 * inch, 0.8 * inch])
        mt.setStyle(TABLE_STYLE_STANDARD)
        story.append(mt)
    else:
        story.append(Paragraph("No mismatches this week.", STYLE_BODY))

    # Appendix C — Birthday reservations detail (all statuses)
    story.append(PageBreak())
    story.append(Paragraph("APPENDIX C — Birthday Reservations (all statuses)", STYLE_H2))
    if recon.reservations:
        b_rows = [["Date", "Guest", "Party", "Spend", "Status"]]
        for r in recon.reservations[:35]:
            b_rows.append([
                r.date, r.guest_label[:22], str(r.party_size),
                _money(r.total_spend), r.status.replace("_", " ").title()[:22]
            ])
        bt = Table(b_rows, colWidths=[0.9 * inch, 2.2 * inch, 0.6 * inch,
                                       0.9 * inch, 2.1 * inch])
        bt.setStyle(TABLE_STYLE_STANDARD)
        story.append(bt)

    # Appendix D — Methodology
    story.append(Spacer(1, 0.15 * inch))
    story.append(Paragraph("APPENDIX D — Methodology & Data Sources", STYLE_H2))
    story.append(Paragraph(
        "<b>Data sources:</b> Toast POS (CheckDetails, ItemSelectionDetails, "
        "PaymentDetails via SFTP export) · SevenRooms Reservations (via API sync) "
        "· BigQuery <code>toast_raw</code> dataset."
        "<br/><br/>"
        "<b>Classification precedence:</b> Tab name → Item SKU → Reason code. "
        "OWNER-prefixed SKUs represent owner-personal or owner-discretionary "
        "consumption per policy §12; NOT birthday-package delivery."
        "<br/><br/>"
        "<b>Birthday eligibility:</b> Only reservations with 'Birthday Dinner "
        "Package' in SR notes are entitled to complimentary champagne per §9. "
        "Day-of-week program: Wed=Bellaire (comped), Fri/Sat=OWNER Moet (cost "
        "basis), Sun=Bellaire (retail). Mon/Tue/Thu have no program."
        "<br/><br/>"
        "<b>Retail prices:</b> Sourced from Toast /menus/v2/menus API, 2026-07-30 "
        "snapshot. Tier 1 &lt;$500 retail; Tier 2 ≥$500 retail. Foregone revenue = "
        "retail value moved − cost recovered."
        "<br/><br/>"
        "<b>Self-approval detection:</b> void_user matches server on the payment; "
        "post-hoc manager approval does not satisfy the pre-void gate control."
        "<br/><br/>"
        "See <code>COMP_MANAGEMENT_POLICY.md</code> and "
        "<code>TAB_NAMING_STANDARDS.md</code> for full definitions.",
        STYLE_BODY,
    ))

    return story


# ── Orchestrator ─────────────────────────────────────────────────────


def build_pdf(cur: CompPeriod, prev: CompPeriod,
              recon: BirthdayReconciliationResult,
              lp_voids: List[LPVoidRecord],
              bm_audit: BMAudit,
              approver_pairs: List[ApproverServerPair],
              reason_mismatches: List[ReasonItemMismatch],
              behaviors: Dict[str, ManagerBehaviorMetrics],
              dayparts: List[DaypartComp],
              trend: List[TrendSnapshot]) -> bytes:
    buf = io.BytesIO()
    doc = BaseDocTemplate(
        buf, pagesize=LETTER,
        leftMargin=0.55 * inch, rightMargin=0.55 * inch,
        topMargin=0.55 * inch, bottomMargin=0.7 * inch,
        title=f"LOV3 Comp Report — {cur.label}",
        author="LOV3 Analytics",
    )
    frame = Frame(doc.leftMargin, doc.bottomMargin,
                  doc.width, doc.height, showBoundary=0)
    doc.addPageTemplates([PageTemplate(id="main", frames=[frame], onPage=_footer)])

    story: List = []
    story += build_cover(cur)
    tier2_foregone = sum(m.foregone_revenue for m in cur.tier_2_movements.values())
    story += build_exec_summary(cur, prev, tier2_foregone, lp_voids)
    story += build_lp_audit(lp_voids, approver_pairs, reason_mismatches)
    story += build_bottle_manager_ledger(bm_audit, cur)
    story += build_manager_scorecard(cur, prev, behaviors)
    # Bar Lead page removed 2026-07-30 — Ashley merged into MANAGERS
    story += build_trend_page(trend)
    story += build_daypart_page(cur, dayparts)
    story += build_birthday_page(recon)
    story += build_best_practices(cur, recon)
    story += build_risks_opportunities(cur, recon, lp_voids, bm_audit)
    story += build_action_plan(cur, recon, lp_voids)
    story += build_promoter_recap(cur)
    story += build_return_to_green(cur, recon)
    story += build_appendix(cur, recon)

    doc.build(story)
    return buf.getvalue()


# ── Email delivery ───────────────────────────────────────────────────


def _get_secret(name: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    resource = f"projects/{PROJECT_ID}/secrets/{name}/versions/latest"
    return client.access_secret_version(name=resource).payload.data.decode("UTF-8").strip()


def send_pdf_email(to_email: str, subject: str, pdf_bytes: bytes,
                   pdf_filename: str, body_html: str) -> Dict:
    api_key = _get_secret("resend-api-key")
    payload = {
        "from": RESEND_FROM,
        "to": [to_email],
        "subject": subject,
        "html": body_html,
        "text": "(This message contains an HTML body and a PDF attachment.)",
        "attachments": [{
            "filename": pdf_filename,
            "content": base64.b64encode(pdf_bytes).decode("ascii"),
        }],
    }
    resp = requests.post(
        RESEND_ENDPOINT, json=payload,
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def generate_and_send(to_email: str,
                      week_start: Optional[str] = None,
                      week_end: Optional[str] = None) -> Dict:
    """Compute the reporting week + build PDF + send via Resend."""
    if week_start and week_end:
        label = f"{week_start} to {week_end}"
        s, e = week_start, week_end
    else:
        label, s, e = last_completed_week()

    analytics = CompAnalytics()
    cur = analytics.compute_period(label, s, e)
    _, ps, pe = prior_week()
    prev = analytics.compute_period("prior", ps, pe)

    recon = BirthdayReconciliation(analytics.bq).reconcile(s, e)
    lp_voids = fetch_lp_voids(analytics.bq, s, e)
    bm_audit = fetch_bottle_manager_audit(analytics.bq, s, e)
    approver_pairs = fetch_approver_pairs(analytics.bq, e, lookback_days=90)
    reason_mismatches = fetch_reason_item_mismatches(analytics.bq, s, e)
    behaviors = {name: fetch_manager_behavior(analytics.bq, cur, name)
                 for name in MANAGERS}
    dayparts = fetch_daypart_split(analytics.bq, s, e)
    trend = fetch_4wk_trend(analytics, e, weeks_back=4)

    pdf = build_pdf(cur, prev, recon, lp_voids, bm_audit,
                    approver_pairs, reason_mismatches, behaviors,
                    dayparts, trend)
    filename = f"lov3_comp_report_v2_{s}_to_{e}.pdf"

    body_html = f"""
    <div style="font-family:-apple-system,BlinkMacSystemFont,sans-serif;max-width:640px;color:#111">
    <p>Attached: LOV3 Weekly Comp Discipline Report v8.0 for the week of <b>{label}</b>.</p>
    <p><b>Verdict:</b> {cur.grade()[0]} · Blended {cur.total_pct:.2f}% (target &lt;4%)</p>
    <p><b>Money at risk:</b> {_money(sum(m.foregone_revenue for m in cur.tier_2_movements.values()))} Tier 2 foregone
    · {len([f for f in lp_voids if f.is_cash])} cash voids · {sum(1 for c in cur.promoter_caps if c.is_over_cap)} cap breach(es)</p>
    <p>Full detail in the attached PDF (10 pages). Interactive dashboard:
    <a href="https://toast-etl-pipeline-t3di7qky4q-uc.a.run.app/comps">/comps</a></p>
    <p style="color:#6a6a6a;font-size:12px">— LOV3 Analytics · Confidential — For Leadership Only</p>
    </div>
    """
    result = send_pdf_email(to_email, f"LOV3|HTX Comp Report v2 — Week of {label}",
                            pdf, filename, body_html)
    return {
        "status": "success",
        "week": label,
        "pdf_bytes": len(pdf),
        "recipient": to_email,
        "resend_id": result.get("id"),
    }


if __name__ == "__main__":
    import sys, json
    to = sys.argv[1] if len(sys.argv) > 1 else "maurice.ragland@lov3htx.com"
    result = generate_and_send(to)
    print(json.dumps(result, indent=2))
