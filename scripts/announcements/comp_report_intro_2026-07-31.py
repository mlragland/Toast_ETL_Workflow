"""LOV3 Weekly Comp Discipline Report — leadership kickoff announcement.

Sent 2026-07-31 to introduce the Tuesday 9 AM CT weekly comp discipline
report to LOV3 leadership. First automated send: Tuesday 2026-08-04.

Sent successfully — Resend id 92909ece-0428-48fd-bf2a-e0c05eb741b4.

Kept as a reference template for future leadership announcements that
introduce a new automated report. Not a scheduled job — one-shot script.

Usage:
    cd <repo root>
    python scripts/announcements/comp_report_intro_2026-07-31.py

Requires: resend-api-key in GCP Secret Manager, same account that powers
gratuity_report.py:send_email.
"""
import os
import sys
from pathlib import Path

# ── EDIT THIS ──────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parents[2]  # scripts/announcements/ → repo root

RECIPIENTS = [
    "maurice.ragland@lov3htx.com",
    "Dajahbishop@lov3htx.com",
    "Eddiejasper@lov3htx.com",
    "Ashleybaines@lov3htx.com",
    "Tiffnloving@icloud.com",
    "adwinn19@gmail.com",
    "Cheffassi89@gmail.com",
]

SUBJECT = "LOV3 Weekly Comp Discipline Report — Kicking off Tuesday, Aug 4 at 9 AM CT"
# ───────────────────────────────────────────────────────────────────────

sys.path.insert(0, str(REPO_ROOT))

import requests
from comp_report_pdf import _get_secret, RESEND_ENDPOINT, RESEND_FROM

HTML = """
<div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
            max-width:680px;color:#111;line-height:1.55;font-size:15px">

  <h2 style="font-family:Georgia,'Times New Roman',serif;font-weight:600;
             font-size:22px;letter-spacing:.5px;margin:0 0 6px 0;color:#111">
    Weekly Comp Discipline Report
  </h2>
  <p style="color:#8a7a3d;font-size:12px;letter-spacing:1.5px;
            text-transform:uppercase;margin:0 0 24px 0">
    Purpose · Cadence · How To Use It
  </p>

  <p>Team,</p>
  <p>Starting <b>Tuesday, August 4</b>, you'll receive a <b>Weekly Comp
  Discipline Report</b> in your inbox every Tuesday at 9 AM CT. This note
  explains what it is, why we built it, how it's constructed, and what
  we need from you when it lands.</p>

  <h3 style="font-family:Georgia,serif;font-size:17px;margin-top:26px">
    Why we built it</h3>
  <p>LOV3 is preparing for VIC3 (target open <b>November 2026</b>). Anything
  we can't measure at LOV3 today gets duplicated blindly at VIC3 on day one.
  Comps — free items, discounts, service recovery, birthday packages,
  owner-hosted rings — are one of the largest and least-visible variables
  in a bottle-service operation. Small percentage differences translate to
  <b>$150K–$250K annualized per venue</b>. The Weekly Comp Discipline Report
  moves us from "we think comp discipline is fine" to "here's exactly what
  our comp discipline looked like this week, per shift, per manager, per
  bucket." No opinion, no memory — just what the POS captured.</p>

  <h3 style="font-family:Georgia,serif;font-size:17px;margin-top:26px">
    How it's constructed</h3>
  <p>Every Tuesday at 9 AM CT, a Cloud Run job pulls the completed week's
  data from three sources:</p>
  <ul>
    <li><b>Toast POS</b> — orders, item selections, payments, voids,
        comp reason codes, tab names</li>
    <li><b>SevenRooms</b> — birthday and VIP reservation pre-registrations</li>
    <li><b>Bank of America</b> — expense context for the P&amp;L</li>
  </ul>
  <p>A multi-signal analysis identifies which manager was on shift (union
  of Labor time entries + cash entries + void_approver + server fields
  with email-form matching), what comps they approved, which reason codes
  they picked, how comps concentrated by daypart, by server, and by bottle
  station — then compares the week against a 4-week rolling trend and
  renders a PDF.</p>

  <h3 style="font-family:Georgia,serif;font-size:17px;margin-top:26px">
    What's in the report</h3>
  <ul>
    <li><b>Executive Summary</b> — blended comp %, Manager Discretionary %,
        Recovery %, graded against targets (&lt;4% blended, ≤1% discretionary,
        ≤0.5% recovery)</li>
    <li><b>Financial Discipline</b> — dollar variance vs prior week +
        4-week trend</li>
    <li><b>Loss Prevention Audit</b> — pattern signals (see the Strategic
        Review §9 LP Field Guide for the reference list)</li>
    <li><b>Bottle Manager Ledger</b> — accountability for the pooled station</li>
    <li><b>Named Scorecards</b> — Ashley, Tiffany, Tony, Daja individual
        snapshots with reason-code diversity and comp $ per shift</li>
    <li><b>Birthday Reconciliation</b> — SR reservations matched to Toast
        comps and the gaps</li>
    <li><b>Promoter Recap</b> — Thursday Afrikan · Fri 106 · Sat R&amp;B ·
        Sun DAE7 · Sun Cassette</li>
    <li><b>Return-to-Green Plan</b> — 3-week action list with dollar
        recovery estimates</li>
  </ul>

  <h3 style="font-family:Georgia,serif;font-size:17px;margin-top:26px">
    How to use it — 15 minutes every Tuesday</h3>
  <ol>
    <li>Open the PDF within your first hour on Tuesday.</li>
    <li>Read the Executive Summary — did we hit the three targets? If not,
        which bucket drove the miss?</li>
    <li>Skim your named scorecard — how did YOU do this week vs. peers?</li>
    <li>Look at the Loss Prevention Audit — <b>none of these are accusations.</b>
        They are shapes LP watches. If your name appears, we walk through it
        non-accusatorily.</li>
    <li>Take any Return-to-Green action item that's on you and get it done
        before next Tuesday.</li>
  </ol>
  <p><b>Every Tuesday morning huddle:</b> the report is the huddle agenda.
  Pull it up, spend 10 minutes, decide what changes this week.</p>

  <h3 style="font-family:Georgia,serif;font-size:17px;margin-top:26px">
    What we are NOT doing</h3>
  <ul>
    <li>We are <b>not accusing anyone</b>. If your name appears in an LP
        pattern, that is a signal — not a finding. We investigate to
        conclusion before drawing conclusions.</li>
    <li>Every target has a published industry basis — not arbitrary.</li>
    <li>If a metric doesn't reflect reality, reply — we fix the query,
        not the interpretation.</li>
  </ul>

  <h3 style="font-family:Georgia,serif;font-size:17px;margin-top:26px">
    The bigger picture: VIC3</h3>
  <p>Everything we harden at LOV3 between now and November 2026 becomes
  VIC3's day-one operating system. Open VIC3 with the visibility and
  discipline LOV3 has today, and VIC3's first-year numbers arrive
  dramatically cleaner. Open VIC3 without hardening, and we copy LOV3's
  data debt into a second venue.</p>
  <p>The Weekly Comp Discipline Report is one of ten controls we're
  converging on. It joins the Tab Naming Standards, the 15-code Reason
  Taxonomy, the Comp Management Policy, and the Strategic Review — all
  designed so the practices we prove at LOV3 port cleanly to VIC3.</p>

  <div style="border-top:1px solid #d4c896;margin:30px 0 18px 0"></div>

  <h3 style="font-family:Georgia,serif;font-size:17px;margin-top:0">
    Cadence</h3>
  <ul>
    <li><b>Every Tuesday, 9 AM CT</b> — PDF lands in your inbox</li>
    <li><b>First automated send: Tuesday, August 4, 2026</b></li>
    <li><b>Slack summary</b> posts simultaneously to <code>#lov3-leader-report</code></li>
    <li><b>Reply directly to Maurice</b> with questions, corrections,
        or "this metric is wrong" flags</li>
  </ul>

  <p>Any questions before Tuesday, ping me.</p>

  <p style="margin-top:26px">— Maurice<br>
  <span style="color:#6a6a6a;font-size:13px">LOV3 Analytics</span></p>

  <p style="color:#8a8a8a;font-size:11px;margin-top:32px;letter-spacing:.6px;
            text-transform:uppercase">
    Confidential — For Leadership Only
  </p>
</div>
"""

TEXT = """LOV3 Weekly Comp Discipline Report — Purpose, Cadence, How to Use

Team,

Starting Tuesday, August 4, you'll receive the Weekly Comp Discipline
Report in your inbox every Tuesday at 9 AM CT.

WHY WE BUILT IT
LOV3 is preparing for VIC3 (target open November 2026). Comps are one
of the largest, least-visible variables in a bottle-service operation
($150-250K/yr per venue impact). This report moves us from opinion
to measurement.

HOW IT'S CONSTRUCTED
Every Tuesday at 9 AM CT, a Cloud Run job pulls the completed week
from Toast POS + SevenRooms + BofA. Multi-signal analysis: manager
shifts, comps approved, reason codes, daypart, server, bottle station.
Compares against 4-week trend. Renders a PDF.

WHAT'S IN IT
- Executive Summary (blended comp %, Manager Discretionary, Recovery)
- Financial Discipline (variance + trend)
- Loss Prevention Audit (pattern signals, not accusations)
- Bottle Manager Ledger
- Named Scorecards (Ashley, Tiffany, Tony, Daja)
- Birthday Reconciliation
- Promoter Recap
- Return-to-Green Plan

HOW TO USE IT — 15 MIN EVERY TUESDAY
1. Open the PDF in your first hour
2. Read the Executive Summary
3. Skim your named scorecard
4. Check the LP Audit — none are accusations, all are signals
5. Action any Return-to-Green item on you before next Tuesday

Tuesday morning huddle: the report is the agenda.

VIC3 CONTEXT
Everything we harden at LOV3 before November 2026 becomes VIC3's
day-one operating system. This report is one of ten controls we're
converging on.

CADENCE
- Every Tuesday, 9 AM CT — PDF in your inbox
- First automated send: Tuesday, August 4, 2026
- Slack summary to #lov3-leader-report at the same time
- Reply to Maurice with questions or corrections

— Maurice
LOV3 Analytics
"""

if __name__ == "__main__":
    api_key = _get_secret("resend-api-key")
    resp = requests.post(
        RESEND_ENDPOINT,
        headers={"Authorization": f"Bearer {api_key}"},
        json={
            "from": RESEND_FROM,
            "to": RECIPIENTS,
            "subject": SUBJECT,
            "html": HTML,
            "text": TEXT,
        },
        timeout=45,
    )
    resp.raise_for_status()
    print(f"Sent — Resend id={resp.json().get('id')}")
    print(f"To: {RECIPIENTS}")
