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
    BaseDocTemplate, Frame, PageBreak, PageTemplate, Paragraph, Spacer, Table,
    TableStyle,
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


def _footer(canv: canvas.Canvas, doc, version: str = "v3.0"):
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
    story = []
    story.append(Spacer(1, 1.6 * inch))
    # Wordmark
    story.append(Paragraph(
        '<font size="42" color="#111111"><b>LOV3</b></font>'
        '<font size="42" color="#B8956A"><b>|</b></font>'
        '<font size="42" color="#111111"><b>HTX</b></font>',
        STYLE_CENTER,
    ))
    story.append(Spacer(1, 0.08 * inch))
    story.append(Paragraph(
        '<font color="#B8956A" size="9"><b>LEADERSHIP · COMP DISCIPLINE · WEEKLY</b></font>',
        STYLE_CENTER,
    ))
    story.append(Spacer(1, 0.6 * inch))
    story.append(Paragraph(
        '<font size="24"><b>Weekly Comp Discipline Report</b></font>',
        STYLE_CENTER,
    ))
    story.append(Spacer(1, 0.15 * inch))
    story.append(Paragraph(
        f'<font size="14" color="#6a6a6a">Week of {cur.label} · '
        f'{cur.start} to {cur.end}</font>',
        STYLE_CENTER,
    ))

    story.append(Spacer(1, 1.4 * inch))
    # Distribution
    dist_data = [
        ["DISTRIBUTION", ""],
        ["Owners", "Maurice Ragland · Eddie · Derwin"],
        ["Managers", "Tiffany Loving · Anthony Winn · Dajah Bishop"],
        ["Bar Lead", "Ashley Baines"],
        ["Generated", datetime.now().strftime("%B %-d, %Y at %-I:%M %p Central")],
        ["Document version", "v3.0 · Policy rev 2026-07-29"],
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


def build_lp_audit(lp_voids: List[LPVoidRecord]) -> List:
    """Page 3 (or wherever) — Loss Prevention Audit."""
    story = []
    story.append(Paragraph("Page 3 · Loss Prevention", STYLE_EYEBROW))
    story.append(Paragraph("Loss Prevention Audit", STYLE_H1))
    story.append(Paragraph(
        "Post-payment voids with self-approval detection. Cash voids and "
        "same-minute voids flagged red. Every void requires manager gate-keeping "
        "at the time of payment — post-hoc approvals do not satisfy the control.",
        STYLE_BODY,
    ))

    # Filter to actionable rows
    same_minute = [f for f in lp_voids
                   if f.time_to_void_secs is not None and f.time_to_void_secs < 120]
    self_approved = [f for f in lp_voids if f.is_self_approved]
    cash_voids = [f for f in lp_voids if f.is_cash]

    story.append(Spacer(1, 0.1 * inch))
    story.append(Paragraph("SUMMARY", STYLE_H3))
    sum_rows = [
        ["Category", "Count", "$ Amount", "Severity"],
        ["Total post-payment voids", str(len(lp_voids)),
         _money(sum(f.amount for f in lp_voids)), "—"],
        ["Cash voids (highest-risk pattern)", str(len(cash_voids)),
         _money(sum(f.amount for f in cash_voids)), "🔴 P0"],
        ["Self-approved voids", str(len(self_approved)),
         _money(sum(f.amount for f in self_approved)), "🔴 P0"],
        ["Same-minute voids (<120s)", str(len(same_minute)),
         _money(sum(f.amount for f in same_minute)), "🚨 URGENT"],
    ]
    st = Table(sum_rows, colWidths=[3.0 * inch, 0.8 * inch, 1.5 * inch, 1.5 * inch])
    st.setStyle(TABLE_STYLE_STANDARD)
    if cash_voids or self_approved or same_minute:
        st.setStyle(TableStyle([("TEXTCOLOR", (3, 2), (3, -1), RED)]))
    story.append(st)

    # Urgent alert callout
    if same_minute:
        story.append(Spacer(1, 0.12 * inch))
        story.append(Paragraph("URGENT — Same-Minute Cash Voids", STYLE_H3))
        story.append(Paragraph(
            f"<font color='#C97064'><b>Action required within 24 hours:</b></font> "
            f"same-day cash-drawer count review + camera pull for the following voids. "
            f"Same-minute voids on cash payments are hospitality's #1 skim vector.",
            STYLE_BODY,
        ))
        alert_rows = [["Date", "Server", "Amount", "Time-to-Void"]]
        for f in same_minute[:5]:
            alert_rows.append([
                f.processing_date, f.server, _money(f.amount),
                f"{f.time_to_void_secs}s"
            ])
        at = Table(alert_rows, colWidths=[1.2 * inch, 2.5 * inch, 1.5 * inch, 1.6 * inch])
        at.setStyle(TABLE_STYLE_STANDARD)
        at.setStyle(TableStyle([
            ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#fbe4e0")),
            ("TEXTCOLOR", (2, 1), (2, -1), RED),
            ("FONTNAME", (2, 1), (2, -1), "Helvetica-Bold"),
        ]))
        story.append(at)

    # Full void detail
    if lp_voids:
        story.append(Spacer(1, 0.12 * inch))
        story.append(Paragraph("VOID DETAIL", STYLE_H3))
        void_rows = [["Date", "Server", "Amount", "Tender", "Void By", "Approver", "Self?"]]
        for f in lp_voids[:15]:
            void_rows.append([
                f.processing_date,
                f.server[:18],
                _money(f.amount),
                f.payment_type[:10],
                f.void_user[:16],
                f.void_approver[:16] if f.void_approver else "—",
                "🔴" if f.is_self_approved else "—",
            ])
        vt = Table(void_rows, colWidths=[0.85 * inch, 1.4 * inch, 0.9 * inch,
                                          0.8 * inch, 1.2 * inch, 1.2 * inch, 0.55 * inch])
        vt.setStyle(TABLE_STYLE_STANDARD)
        story.append(vt)

    story.append(PageBreak())
    return story


def build_bottle_manager_ledger(bm_audit: BMAudit, cur: CompPeriod) -> List:
    """Page 4 — Bottle Manager Ledger (direct answer to CEO's Q)."""
    story = []
    story.append(Paragraph("Page 4 · Bottle Manager", STYLE_EYEBROW))
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


def build_manager_scorecard(cur: CompPeriod, prev: CompPeriod) -> List:
    """Page 5 — Manager Performance (Tiffany · Tony · Daja) with peer benchmarks."""
    story = []
    story.append(Paragraph("Page 5 · Manager Performance", STYLE_EYEBROW))
    story.append(Paragraph("Manager Scorecard — Tiffany · Tony · Daja", STYLE_H1))
    story.append(Paragraph(
        "Peer-benchmarked KPIs for the three floor managers. Metrics normalized "
        "per week; peer median shown for context. Diversity = number of distinct "
        "comp buckets touched; 1/4 means every comp landed in a single bucket "
        "(usually 'Manager Comp') — training signal per policy §11.",
        STYLE_BODY,
    ))

    # Compute peer stats for benchmarking
    mgr_scores = [cur.named_scorecards.get(n) for n in MANAGERS
                  if cur.named_scorecards.get(n)]
    peer_disc = [s.discretionary_comp for s in mgr_scores]
    peer_disc_median = sorted(peer_disc)[len(peer_disc)//2] if peer_disc else 0.0
    peer_diversity_median = (
        sorted([s.reason_code_diversity for s in mgr_scores])[len(mgr_scores)//2]
        if mgr_scores else 0
    )

    # ── Peer Benchmark table ──
    story.append(Spacer(1, 0.1 * inch))
    story.append(Paragraph("PEER BENCHMARKS", STYLE_H3))
    bench_rows = [
        ["Metric", "Individual Target", "Peer Median This Week"],
        ["Manager Discretionary $ per week", "≤ $500", _money(peer_disc_median)],
        ["Reason-code diversity", "≥ 3 of 4 buckets", f"{peer_diversity_median} / 4"],
        ["Recovery approvals per week", "target driven by kitchen quality", "—"],
        ["Owner comp button use", "audit-trail only, no cap", "—"],
    ]
    bt = Table(bench_rows, colWidths=[3.0 * inch, 2.2 * inch, 2.0 * inch])
    bt.setStyle(TABLE_STYLE_STANDARD)
    story.append(bt)

    # ── Individual manager KPIs ──
    story.append(Spacer(1, 0.12 * inch))
    story.append(Paragraph("INDIVIDUAL SCORECARDS", STYLE_H3))
    mgr_rows = [["Manager", "Comps", "Discret $", "Recovery $",
                 "Owner Rung", "Diversity", "vs Peer Median"]]
    for name in MANAGERS:
        s = cur.named_scorecards.get(name)
        if not s:
            continue
        # Delta vs peer median
        delta = s.discretionary_comp - peer_disc_median
        delta_str = f"+${delta:,.0f}" if delta > 0 else f"−${abs(delta):,.0f}"
        mgr_rows.append([
            s.name, str(s.comp_count),
            _money(s.discretionary_comp), _money(s.recovery_comp),
            _money(s.owner_comp_rung),
            f"{s.reason_code_diversity} / 4",
            delta_str,
        ])
    mt = Table(mgr_rows, colWidths=[1.5 * inch, 0.7 * inch, 1.0 * inch, 1.0 * inch,
                                     1.0 * inch, 0.9 * inch, 1.3 * inch])
    mt.setStyle(TABLE_STYLE_STANDARD)
    story.append(mt)

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
    named_disc = sum(s.discretionary_comp for s in mgr_scores)
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

    story.append(PageBreak())
    return story


def build_bar_lead_scorecard(cur: CompPeriod, prev: CompPeriod,
                              recon: BirthdayReconciliationResult) -> List:
    """Page 6 — Bar Lead Scorecard (Ashley) — own KPI category."""
    story = []
    story.append(Paragraph("Page 6 · Bar Lead Performance", STYLE_EYEBROW))
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
    story.append(Paragraph("Page 6 · Birthday Package Compliance", STYLE_EYEBROW))
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
    story.append(Paragraph("Page 8 · Learning From Excellence", STYLE_EYEBROW))
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
    story.append(Paragraph("Page 9 · Forward Look", STYLE_EYEBROW))
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
    story.append(Paragraph("Page 10 · Roadmap", STYLE_EYEBROW))
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
    dt = Table(d30, colWidths=[2.5 * inch, 1.6 * inch, 1.9 * inch, 1.2 * inch])
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
    d6t = Table(d60, colWidths=[2.5 * inch, 1.6 * inch, 1.9 * inch, 1.2 * inch])
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
    d9t = Table(d90, colWidths=[2.5 * inch, 1.6 * inch, 1.9 * inch, 1.2 * inch])
    d9t.setStyle(TABLE_STYLE_STANDARD)
    story.append(d9t)

    story.append(PageBreak())
    return story


def build_promoter_recap(cur: CompPeriod) -> List:
    """Page 7 — Promoter Recap (prose per COO)."""
    story = []
    story.append(Paragraph("Page 7 · Promoter Discipline", STYLE_EYEBROW))
    story.append(Paragraph("Promoter Cap Recap", STYLE_H1))
    story.append(Paragraph(
        "Each promoter event's actual bottle count versus the per-event cap. "
        "External promoter overages incur 80% clawback against next payout; "
        "in-house event overages log as excess promotional spend.",
        STYLE_BODY,
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
    story.append(Paragraph("Page 8 · Recovery Plan", STYLE_EYEBROW))
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
    a_rows = [["Action", "Detail", "$ Recovery", "Owner", "Deadline"]] + actions
    at = Table(a_rows, colWidths=[1.9 * inch, 2.3 * inch, 1.0 * inch,
                                    1.3 * inch, 1.0 * inch])
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
              bm_audit: BMAudit) -> bytes:
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
    story += build_lp_audit(lp_voids)
    story += build_bottle_manager_ledger(bm_audit, cur)
    story += build_manager_scorecard(cur, prev)
    story += build_bar_lead_scorecard(cur, prev, recon)
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

    pdf = build_pdf(cur, prev, recon, lp_voids, bm_audit)
    filename = f"lov3_comp_report_v2_{s}_to_{e}.pdf"

    body_html = f"""
    <div style="font-family:-apple-system,BlinkMacSystemFont,sans-serif;max-width:640px;color:#111">
    <p>Attached: LOV3 Weekly Comp Discipline Report v3.0 for the week of <b>{label}</b>.</p>
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
