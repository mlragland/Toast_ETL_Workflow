"""Send the 3-doc weekly comp bundle to LOV3 leadership.

Bundle:
    1. Weekly v8 Comp Discipline Report PDF (this week)
    2. Strategic Review v2.1 PDF (evergreen reference)
    3. POS Tab-Naming Laminate PDF (evergreen operational aid)

First sent 2026-07-31 for the week of Jul 20–26, 2026 (Resend id
19fdf3e2-04a8-45c4-b8f3-e8a28b4944b1). Kept as a reusable manual-send
template — the Tuesday automated cron sends the comp report PDF only
(see /api/comp-report in routes_analytics.py). Use this script when you
need to re-send the full 3-doc bundle to leadership out of cadence.

Usage:
    cd <repo root>
    python scripts/announcements/comp_bundle_send.py
"""
import base64
import sys
from pathlib import Path

# ── EDIT THIS ──────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parents[2]  # scripts/announcements/ → repo root
STRATEGIC_MD = REPO_ROOT / "LOV3_COMP_STRATEGIC_REVIEW.md"

RECIPIENTS = [
    "maurice.ragland@lov3htx.com",
    "Dajahbishop@lov3htx.com",
    "Eddiejasper@lov3htx.com",
    "Ashleybaines@lov3htx.com",
    "Tiffnloving@icloud.com",
    "adwinn19@gmail.com",
    "Cheffassi89@gmail.com",
]
# ───────────────────────────────────────────────────────────────────────

sys.path.insert(0, str(REPO_ROOT))

import requests

from comp_report_pdf import (
    CompAnalytics, BirthdayReconciliation, MANAGERS,
    fetch_lp_voids, fetch_bottle_manager_audit, fetch_approver_pairs,
    fetch_reason_item_mismatches, fetch_manager_behavior,
    fetch_daypart_split, fetch_4wk_trend, build_pdf as build_comp_pdf,
    last_completed_week, prior_week, _get_secret,
    RESEND_ENDPOINT, RESEND_FROM,
)
from strategic_review_pdf import build_pdf as build_strategic_pdf
from pos_laminate import build_laminate


def build_all():
    """Build all three PDFs for the last-completed week and return them."""
    analytics = CompAnalytics()
    label, s, e = last_completed_week()
    cur = analytics.compute_period(label, s, e)
    _, ps, pe = prior_week()
    prev = analytics.compute_period("prior", ps, pe)

    recon = BirthdayReconciliation(analytics.bq).reconcile(s, e)
    lp_voids = fetch_lp_voids(analytics.bq, s, e)
    bm_audit = fetch_bottle_manager_audit(analytics.bq, s, e)
    approver_pairs = fetch_approver_pairs(analytics.bq, e, lookback_days=90)
    reason_mismatches = fetch_reason_item_mismatches(analytics.bq, s, e)
    behaviors = {n: fetch_manager_behavior(analytics.bq, cur, n) for n in MANAGERS}
    dayparts = fetch_daypart_split(analytics.bq, s, e)
    trend = fetch_4wk_trend(analytics, e, weeks_back=4)

    print("Building v8 comp report...")
    comp_pdf = build_comp_pdf(cur, prev, recon, lp_voids, bm_audit,
                              approver_pairs, reason_mismatches, behaviors,
                              dayparts, trend)
    print(f"  comp v8: {len(comp_pdf):,} bytes")

    print("Building strategic review v2.1...")
    strategic_pdf = build_strategic_pdf(str(STRATEGIC_MD))
    print(f"  strategic v2.1: {len(strategic_pdf):,} bytes")

    print("Building POS laminate...")
    laminate = build_laminate()
    print(f"  laminate: {len(laminate):,} bytes")

    return cur, label, s, e, comp_pdf, strategic_pdf, laminate


def send_bundle(cur, label, s, e, comp_pdf, strategic_pdf, laminate):
    """Send the 3-attachment leadership email via Resend."""
    api_key = _get_secret("resend-api-key")

    subject = (
        f"OPEN — LOV3 Weekly Comp Report · Week of {label}, 2026 "
        f"(3 attachments)"
    )

    html = f"""
<div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
            max-width:640px;color:#111;line-height:1.55;font-size:15px">

  <h2 style="font-family:Georgia,serif;font-weight:600;font-size:20px;
             margin:0 0 4px 0">
    Weekly Comp Report — {label}, 2026
  </h2>
  <p style="color:#8a7a3d;font-size:12px;letter-spacing:1.5px;
            text-transform:uppercase;margin:0 0 20px 0">
    Action Required · 15 min · Before your next shift
  </p>

  <p><b>This week's numbers:</b></p>
  <ul style="margin:0 0 18px 0">
    <li>Blended comp: <b>{cur.total_pct:.2f}%</b> (target &lt;4%)</li>
    <li>Manager Discretionary: <b>{cur.manager_disc_pct:.2f}%</b> (target ≤1%)</li>
    <li>Recovery: <b>{cur.recovery_pct:.2f}%</b> (target ≤0.5%)</li>
  </ul>

  <p><b>What to do — 15 minutes:</b></p>
  <ol>
    <li>Open <b>LOV3_Comp_Report_v8</b> — read Executive Summary + find your scorecard</li>
    <li>Skim the Loss Prevention Audit — <b>signals, not accusations.</b> If your name appears, we'll walk through it together</li>
    <li>Action any Return-to-Green item that's on you before next Tuesday</li>
  </ol>

  <p><b>Attachments:</b></p>
  <ol>
    <li><b>LOV3_Comp_Report_v8_{s}_to_{e}.pdf</b> — this week's numbers, scorecards, LP audit, birthday reconciliation, Return-to-Green plan</li>
    <li><b>LOV3_Comp_Strategic_Review_v2.1.pdf</b> — the "why" behind the report + §9 LP Pattern Field Guide (reference this when reviewing the audit section)</li>
    <li><b>LOV3_POS_Tab_Naming_Laminate.pdf</b> — print & laminate at every POS station; drives the tab-naming standard the report measures</li>
  </ol>

  <p style="margin-top:20px"><b>Cadence reminder:</b> the report fires automatically every Tuesday 9 AM CT. Reply to Maurice with corrections or questions.</p>

  <p style="margin-top:24px">— Maurice<br>
    <span style="color:#6a6a6a;font-size:13px">LOV3 Analytics</span></p>

  <p style="color:#8a8a8a;font-size:11px;margin-top:28px;letter-spacing:.6px;
            text-transform:uppercase">
    Confidential — For Leadership Only
  </p>
</div>
"""

    text = f"""LOV3 Weekly Comp Report — {label}, 2026

This week's numbers:
- Blended comp: {cur.total_pct:.2f}% (target <4%)
- Manager Discretionary: {cur.manager_disc_pct:.2f}% (target <=1%)
- Recovery: {cur.recovery_pct:.2f}% (target <=0.5%)

What to do (15 min):
1. Open LOV3_Comp_Report_v8 — read Executive Summary + find your scorecard
2. Skim the LP Audit — signals, not accusations
3. Action any Return-to-Green item on you before next Tuesday

Attachments:
1. LOV3_Comp_Report_v8_{s}_to_{e}.pdf — this week's report
2. LOV3_Comp_Strategic_Review_v2.1.pdf — the "why" + LP Pattern Field Guide
3. LOV3_POS_Tab_Naming_Laminate.pdf — print at every POS

Fires automatically every Tuesday 9 AM CT.

— Maurice
"""

    payload = {
        "from": RESEND_FROM,
        "to": RECIPIENTS,
        "subject": subject,
        "html": html,
        "text": text,
        "attachments": [
            {"filename": f"LOV3_Comp_Report_v8_{s}_to_{e}.pdf",
             "content": base64.b64encode(comp_pdf).decode("ascii")},
            {"filename": "LOV3_Comp_Strategic_Review_v2.1.pdf",
             "content": base64.b64encode(strategic_pdf).decode("ascii")},
            {"filename": "LOV3_POS_Tab_Naming_Laminate.pdf",
             "content": base64.b64encode(laminate).decode("ascii")},
        ],
    }

    resp = requests.post(RESEND_ENDPOINT, json=payload,
                         headers={"Authorization": f"Bearer {api_key}"},
                         timeout=60)
    resp.raise_for_status()
    return resp.json().get("id")


if __name__ == "__main__":
    cur, label, s, e, comp_pdf, strategic_pdf, laminate = build_all()
    resend_id = send_bundle(cur, label, s, e, comp_pdf, strategic_pdf, laminate)
    print(f"\nSent — Resend id={resend_id}")
    print(f"To: {RECIPIENTS}")
    print(f"  v8 comp report: {len(comp_pdf):,} bytes")
    print(f"  strategic v2.1: {len(strategic_pdf):,} bytes")
    print(f"  laminate:       {len(laminate):,} bytes")
