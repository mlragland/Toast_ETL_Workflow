"""
Gratuity Breakdown Report — bi-weekly delivery to leadership.

Triggered by Cloud Scheduler every other Monday evening following a pay-period
close. Computes the pay-period gratuity breakdown from BigQuery, formats an
HTML report, and emails it to the leadership list. Optional Slack fallback if
email fails.

Pay period anchor: pay periods run Mon–Sun bi-weekly, anchored on the known
period ending Sun 2026-06-14.

Splits applied (per project_service_charge_model.md, in effect since early 2025):
  - Server: 70% employee / 30% house
  - Bartender: 75% employee / 25% house
  - Bottle Manager (Grat Pool): 50% / 50%
  - Other (managers): 0% / 100%

House target: 35% blended share.
"""

import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests
from google.cloud import bigquery

from config import PROJECT_ID, DATASET_ID
from services import SecretManager

RESEND_ENDPOINT = "https://api.resend.com/emails"
RESEND_FROM_EMAIL = "LOV3 Analytics <reports@lov3htx.com>"

logger = logging.getLogger(__name__)

# Pay-period anchor: Sun 2026-06-14 is a known pay-period end.
PAYPERIOD_END_ANCHOR = date(2026, 6, 14)

# Recipient list (project_gratuity_breakdown_report.md)
DEFAULT_RECIPIENTS = [
    "maurice.ragland@lov3htx.com",
    "eddiejasper@lov3htx.com",
    "tiffanyloving@lov3htx.com",
    "sossitytaylor@lov3htx.com",
]

# Splits — house share per bucket
HOUSE_SHARE = {
    "Server": 0.30,
    "Bartender": 0.25,
    "Bottle Manager": 0.50,
    "Other": 1.00,
}

TARGET_HOUSE_PCT = 0.35


@dataclass
class BucketRow:
    bucket: str
    net_sales: float
    grat_collected: float
    tips: float
    orders: int

    @property
    def house_total(self) -> float:
        return self.grat_collected * HOUSE_SHARE[self.bucket]

    @property
    def employee_total(self) -> float:
        return self.grat_collected * (1 - HOUSE_SHARE[self.bucket])


def latest_completed_payperiod(today: date) -> Tuple[date, date]:
    """Return (start_monday, end_sunday) for the most recently completed pay period."""
    delta = (today - PAYPERIOD_END_ANCHOR).days
    period_index = delta // 14 if delta >= 0 else -((-delta) // 14 + 1)
    end = PAYPERIOD_END_ANCHOR + timedelta(days=14 * period_index)
    if end > today:
        end -= timedelta(days=14)
    start = end - timedelta(days=13)
    return start, end


def is_payperiod_close_monday(today: date) -> bool:
    """True if today is a Monday immediately following a pay-period-end Sunday."""
    if today.weekday() != 0:  # Mon = 0
        return False
    yesterday = today - timedelta(days=1)
    # Yesterday should be a Sunday that aligns with the bi-weekly anchor
    delta = (yesterday - PAYPERIOD_END_ANCHOR).days
    return delta % 14 == 0


class GratuityReportGenerator:
    def __init__(self):
        self.bq = bigquery.Client(project=PROJECT_ID)
        self.secret_manager = SecretManager(PROJECT_ID)

    def query_buckets(self, start: date, end: date) -> List[BucketRow]:
        """Query BQ for gratuity buckets in the pay period."""
        # Use a labor lookup window covering the period + buffer for stable role mapping
        labor_start = start - timedelta(days=30)
        labor_end = end + timedelta(days=1)

        sql = f"""
        WITH labor_role AS (
          SELECT employee_name, job_title,
                 ROW_NUMBER() OVER (PARTITION BY employee_name
                                    ORDER BY SUM(IFNULL(regular_hours,0) + IFNULL(overtime_hours,0)) DESC) AS rn
          FROM `{PROJECT_ID}.{DATASET_ID}.LaborTimeEntries_raw`
          WHERE processing_date BETWEEN @labor_start AND @labor_end
            AND job_title IS NOT NULL AND job_title != ''
          GROUP BY employee_name, job_title
        ),
        role_map AS (SELECT employee_name, job_title AS primary_role FROM labor_role WHERE rn = 1),
        sales AS (
          SELECT server, amount, gratuity, tip
          FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
          WHERE processing_date BETWEEN @period_start AND @period_end
            AND (voided = 'false' OR voided IS NULL)
        )
        SELECT
          CASE
            WHEN s.server = 'Bottle Manager' THEN 'Bottle Manager'
            WHEN r.primary_role = 'Bartender' THEN 'Bartender'
            WHEN r.primary_role = 'Server' THEN 'Server'
            ELSE 'Other'
          END AS bucket,
          ROUND(SUM(s.amount),2) AS net_sales,
          ROUND(SUM(s.gratuity),2) AS grat_collected,
          ROUND(SUM(s.tip),2) AS tips,
          COUNT(*) AS orders
        FROM sales s LEFT JOIN role_map r ON r.employee_name = s.server
        GROUP BY bucket
        """

        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("labor_start", "DATE", labor_start.isoformat()),
            bigquery.ScalarQueryParameter("labor_end", "DATE", labor_end.isoformat()),
            bigquery.ScalarQueryParameter("period_start", "DATE", start.isoformat()),
            bigquery.ScalarQueryParameter("period_end", "DATE", end.isoformat()),
        ])

        rows: Dict[str, BucketRow] = {}
        for r in self.bq.query(sql, job_config=job_config).result():
            rows[r.bucket] = BucketRow(
                bucket=r.bucket,
                net_sales=float(r.net_sales or 0),
                grat_collected=float(r.grat_collected or 0),
                tips=float(r.tips or 0),
                orders=int(r.orders or 0),
            )

        ordered: List[BucketRow] = []
        for b in ["Server", "Bartender", "Bottle Manager", "Other"]:
            ordered.append(rows.get(b, BucketRow(b, 0, 0, 0, 0)))
        return ordered

    def compute_scenarios(self, buckets: List[BucketRow]) -> Dict:
        """Compute the 'Paths to 35%' scenarios."""
        b = {r.bucket: r for r in buckets}
        total_grat = sum(r.grat_collected for r in buckets)
        current_house = sum(r.house_total for r in buckets)
        target_house = total_grat * TARGET_HOUSE_PCT
        gap = target_house - current_house

        scenarios = []
        if b["Bottle Manager"].grat_collected > 0:
            new_bm = (b["Bottle Manager"].house_total + gap) / b["Bottle Manager"].grat_collected
            scenarios.append({
                "name": "Raise Bottle Manager (Grat Pool) only",
                "server_h": HOUSE_SHARE["Server"], "server_unchanged": True,
                "bart_h": HOUSE_SHARE["Bartender"], "bart_unchanged": True,
                "bm_h": new_bm, "bm_unchanged": False,
            })
        if b["Server"].grat_collected > 0:
            new_s = (b["Server"].house_total + gap) / b["Server"].grat_collected
            scenarios.append({
                "name": "Raise Server only",
                "server_h": new_s, "server_unchanged": False,
                "bart_h": HOUSE_SHARE["Bartender"], "bart_unchanged": True,
                "bm_h": HOUSE_SHARE["Bottle Manager"], "bm_unchanged": True,
            })
        if b["Bartender"].grat_collected > 0:
            new_b = (b["Bartender"].house_total + gap) / b["Bartender"].grat_collected
            scenarios.append({
                "name": "Raise Bartender only",
                "server_h": HOUSE_SHARE["Server"], "server_unchanged": True,
                "bart_h": new_b, "bart_unchanged": False,
                "bm_h": HOUSE_SHARE["Bottle Manager"], "bm_unchanged": True,
            })
        pool_3 = sum(b[k].grat_collected for k in ["Server", "Bartender", "Bottle Manager"])
        if pool_3 > 0:
            delta = gap / pool_3
            scenarios.append({
                "name": f"Raise all three equally (+{delta*100:.2f} pts)",
                "server_h": HOUSE_SHARE["Server"] + delta, "server_unchanged": False,
                "bart_h": HOUSE_SHARE["Bartender"] + delta, "bart_unchanged": False,
                "bm_h": HOUSE_SHARE["Bottle Manager"] + delta, "bm_unchanged": False,
            })

        return {
            "total_grat": total_grat,
            "current_house": current_house,
            "current_pct": current_house / total_grat if total_grat else 0,
            "target_house": target_house,
            "target_pct": TARGET_HOUSE_PCT,
            "gap": gap,
            "scenarios": scenarios,
        }

    def render_html(self, start: date, end: date, buckets: List[BucketRow], scen: Dict) -> str:
        """Render the HTML report for email body."""
        def money(v): return f"${v:,.2f}"
        def pct(v): return f"{v*100:.2f}%"

        bucket_rows = ""
        labels = {
            "Server": ("Server", "30% / 70%"),
            "Bartender": ("Bartender", "25% / 75%"),
            "Bottle Manager": ('Bottle Manager <span style="background:#1a1a1a;color:#fff;font-size:9px;padding:2px 6px;border-radius:3px;letter-spacing:0.4px;text-transform:uppercase;margin-left:4px;">Grat Pool</span> <span style="color:#777;font-size:11px;">(POS station)</span>', "50% / 50%"),
            "Other": ('Other <span style="color:#777;font-size:11px;">(managers)</span>', "100% / 0%"),
        }
        for r in buckets:
            name, split = labels[r.bucket]
            bucket_rows += f"""
            <tr>
              <td style="padding:8px 10px;border-bottom:1px solid #e3e3e3;">{name}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #e3e3e3;">{split}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #e3e3e3;text-align:right;font-variant-numeric:tabular-nums;">{money(r.net_sales)}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #e3e3e3;text-align:right;font-variant-numeric:tabular-nums;">{money(r.grat_collected)}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #e3e3e3;text-align:right;font-variant-numeric:tabular-nums;">{money(r.house_total)}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #e3e3e3;text-align:right;font-variant-numeric:tabular-nums;">{money(r.employee_total)}</td>
            </tr>"""

        total_net = sum(r.net_sales for r in buckets)
        total_grat = sum(r.grat_collected for r in buckets)
        total_house = sum(r.house_total for r in buckets)
        total_emp = sum(r.employee_total for r in buckets)
        total_tips = sum(r.tips for r in buckets)

        scenario_rows = ""
        for s in scen["scenarios"]:
            def fmt(h, unchanged):
                if unchanged:
                    return f'{pct(h)} / {pct(1-h)} <span style="color:#777;font-size:11px;">(no change)</span>'
                return f"<strong>{pct(h)} / {pct(1-h)}</strong>"
            scenario_rows += f"""
            <tr>
              <td style="padding:8px 10px;border-bottom:1px solid #e3e3e3;">{s['name']}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #e3e3e3;text-align:right;">{fmt(s['server_h'], s['server_unchanged'])}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #e3e3e3;text-align:right;">{fmt(s['bart_h'], s['bart_unchanged'])}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #e3e3e3;text-align:right;">{fmt(s['bm_h'], s['bm_unchanged'])}</td>
              <td style="padding:8px 10px;border-bottom:1px solid #e3e3e3;text-align:right;">35.00%</td>
            </tr>"""

        return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"/><title>Gratuity Breakdown — LOV3 | HTX</title></head>
<body style="font-family:-apple-system,'Helvetica Neue',Arial,sans-serif;color:#1a1a1a;max-width:900px;margin:0 auto;padding:20px;">

<h1 style="font-size:22pt;margin:0 0 4pt 0;letter-spacing:-0.5pt;">Gratuity Breakdown — LOV3 | HTX</h1>
<h2 style="font-size:13pt;color:#555;margin:0 0 18pt 0;font-weight:400;">
  Pay Period: {start.strftime('%A %b %-d')} — {end.strftime('%A %b %-d, %Y')}
</h2>

<table style="width:100%;border-collapse:collapse;font-size:11pt;margin-bottom:6pt;">
  <thead>
    <tr style="background:#1a1a1a;color:#fff;">
      <th style="padding:8px 10px;text-align:left;font-size:10pt;letter-spacing:0.3pt;">Bucket</th>
      <th style="padding:8px 10px;text-align:left;font-size:10pt;letter-spacing:0.3pt;">Split (House / Emp)</th>
      <th style="padding:8px 10px;text-align:right;font-size:10pt;letter-spacing:0.3pt;">Net Sales</th>
      <th style="padding:8px 10px;text-align:right;font-size:10pt;letter-spacing:0.3pt;">Grat Collected</th>
      <th style="padding:8px 10px;text-align:right;font-size:10pt;letter-spacing:0.3pt;">House Total</th>
      <th style="padding:8px 10px;text-align:right;font-size:10pt;letter-spacing:0.3pt;">Employee Total</th>
    </tr>
  </thead>
  <tbody>{bucket_rows}
    <tr style="background:#f3f3f3;font-weight:700;border-top:2px solid #1a1a1a;border-bottom:2px solid #1a1a1a;">
      <td style="padding:8px 10px;">TOTALS</td>
      <td style="padding:8px 10px;">—</td>
      <td style="padding:8px 10px;text-align:right;font-variant-numeric:tabular-nums;">{money(total_net)}</td>
      <td style="padding:8px 10px;text-align:right;font-variant-numeric:tabular-nums;">{money(total_grat)}</td>
      <td style="padding:8px 10px;text-align:right;font-variant-numeric:tabular-nums;">{money(total_house)}</td>
      <td style="padding:8px 10px;text-align:right;font-variant-numeric:tabular-nums;">{money(total_emp)}</td>
    </tr>
  </tbody>
</table>

<h3 style="font-size:12pt;margin:22pt 0 8pt 0;border-bottom:1px solid #ccc;padding-bottom:4pt;letter-spacing:0.4pt;text-transform:uppercase;color:#333;">House Share — Actual vs. Target</h3>
<table style="width:100%;border-collapse:separate;border-spacing:10px 0;">
  <tr>
    <td style="background:#1a1a1a;color:#fff;padding:16px 20px;border-radius:4px;width:33%;vertical-align:top;">
      <div style="font-size:26pt;font-weight:700;letter-spacing:-0.8pt;">{pct(scen['current_pct'])}</div>
      <div style="font-size:9pt;letter-spacing:0.5pt;text-transform:uppercase;color:#bbb;margin-top:4px;">
        Actual House Share<br/>{money(scen['current_house'])} of {money(scen['total_grat'])}
      </div>
    </td>
    <td style="background:#f3f3f3;border:1px solid #1a1a1a;padding:16px 20px;border-radius:4px;width:33%;vertical-align:top;">
      <div style="font-size:26pt;font-weight:700;letter-spacing:-0.8pt;">35.00%</div>
      <div style="font-size:9pt;letter-spacing:0.5pt;text-transform:uppercase;color:#555;margin-top:4px;">
        Target House Share<br/>{money(scen['target_house'])} of {money(scen['total_grat'])}
      </div>
    </td>
    <td style="background:#b45309;color:#fff;padding:16px 20px;border-radius:4px;width:33%;vertical-align:top;">
      <div style="font-size:26pt;font-weight:700;letter-spacing:-0.8pt;">{money(scen['gap'])}</div>
      <div style="font-size:9pt;letter-spacing:0.5pt;text-transform:uppercase;color:#fde68a;margin-top:4px;">
        Gap to Close<br/>Reallocate from Employee → House
      </div>
    </td>
  </tr>
</table>

<h3 style="font-size:12pt;margin:22pt 0 8pt 0;border-bottom:1px solid #ccc;padding-bottom:4pt;letter-spacing:0.4pt;text-transform:uppercase;color:#333;">Paths to 35% — Pick One</h3>
<table style="width:100%;border-collapse:collapse;font-size:10pt;">
  <thead>
    <tr style="background:#1a1a1a;color:#fff;">
      <th style="padding:8px 10px;text-align:left;">Scenario</th>
      <th style="padding:8px 10px;text-align:right;">New Server (H/E)</th>
      <th style="padding:8px 10px;text-align:right;">New Bartender (H/E)</th>
      <th style="padding:8px 10px;text-align:right;">New Bottle Mgr (H/E)</th>
      <th style="padding:8px 10px;text-align:right;">Blended House %</th>
    </tr>
  </thead>
  <tbody>{scenario_rows}</tbody>
</table>

<div style="font-size:9pt;color:#555;margin-top:18pt;line-height:1.5;">
  <strong>Notes</strong>
  <ul>
    <li>The 20% mandatory charge on every check is legally a <em>service charge</em> (IRS Rev. Rul. 2012-18), not a gratuity. LOV3 owns 100% and distributes the employee portion per the splits above.</li>
    <li>Voluntary tips (separate from service charge) for this period: <strong>{money(total_tips)}</strong>, 100% to the employee. Total cash-to-staff = {money(total_emp)} grat share + {money(total_tips)} tips = <strong>{money(total_emp + total_tips)}</strong>.</li>
    <li>Current splits (Server 70/30, Bartender 75/25, Bottle Manager 50/50) have been in effect since <strong>early 2025</strong> and remain indefinitely until a change is formally communicated to staff.</li>
    <li>"Bottle Manager" is a POS station for walk-in bottle service. The 50% employee portion is pooled to the waitstaff who actually walked in those orders (Grat Pool).</li>
    <li>Role classification uses each employee's primary job_title from the Toast Labor API for the 30 days leading up to the pay period.</li>
    <li>Gap math: {money(scen['total_grat'])} × 35% = {money(scen['target_house'])} target, minus {money(scen['current_house'])} current = {money(scen['gap'])} to reallocate.</li>
  </ul>
</div>

</body></html>"""

    def send_email(self, to_email: str, subject: str, html: str) -> bool:
        """Send an HTML email via Resend.

        Reuses the same Resend account that powers lov3synch's reports — same
        verified domain (lov3htx.com), single API key in Secret Manager.
        """
        try:
            api_key = self.secret_manager.get_secret("resend-api-key").strip()
        except Exception as e:
            logger.error(f"Could not fetch resend-api-key secret: {e}")
            return False
        try:
            payload = {
                "from": RESEND_FROM_EMAIL,
                "to": [to_email],
                "subject": subject,
                "html": html,
                "text": "(This message is best viewed as HTML.)",
            }
            resp = requests.post(
                RESEND_ENDPOINT,
                json=payload,
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=30,
            )
            if resp.status_code >= 400:
                logger.error(f"Resend {resp.status_code} for {to_email}: {resp.text[:300]}")
                return False
            data = resp.json() if resp.content else {}
            logger.info(f"Gratuity report email → {to_email} ok (id={data.get('id')})")
            return True
        except Exception as e:
            logger.error(f"Email send failed for {to_email}: {e}")
            return False

    def generate_and_send(
        self,
        period_end: Optional[str] = None,
        recipients: Optional[List[str]] = None,
    ) -> Dict:
        """Generate the report and send to all recipients.

        Args:
            period_end: optional pay-period-end Sunday as YYYYMMDD. Defaults to latest completed.
            recipients: optional list of emails. Defaults to DEFAULT_RECIPIENTS.
        """
        if period_end:
            end = datetime.strptime(period_end, "%Y%m%d").date()
            start = end - timedelta(days=13)
        else:
            start, end = latest_completed_payperiod(date.today())

        recipients = recipients or DEFAULT_RECIPIENTS

        logger.info(f"Generating gratuity report for {start} – {end}")
        buckets = self.query_buckets(start, end)
        scen = self.compute_scenarios(buckets)
        html = self.render_html(start, end, buckets, scen)

        subject = f"LOV3|HTX Gratuity Breakdown — {start.strftime('%b %-d')} to {end.strftime('%b %-d, %Y')}"

        results = []
        for r in recipients:
            ok = self.send_email(r, subject, html)
            results.append({"recipient": r, "success": ok})

        success_count = sum(1 for r in results if r["success"])
        return {
            "period_start": start.isoformat(),
            "period_end": end.isoformat(),
            "total_grat": scen["total_grat"],
            "blended_house_pct": scen["current_pct"],
            "target_pct": TARGET_HOUSE_PCT,
            "gap": scen["gap"],
            "recipients": results,
            "success_count": success_count,
            "total_recipients": len(recipients),
        }
