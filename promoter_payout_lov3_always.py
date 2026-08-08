"""LOV3 · LOV3 Always Saturday Promoter Payout — weekly automation.

Called by Cloud Scheduler every Tuesday 12 PM CT via
POST /api/promoter-payout-lov3-always-weekly.

Mirrors promoter_payout_afrikan.py but for the LOV3 Always Saturday
event (Black Swan promoter). Differences:
    - Saturday event, 12 PM – 6 PM (same day, no midnight rollover)
    - Promoter: Black Swan, Zelle 281.455.1253
    - Dropbox path: /Predictive_Models/PMG/Promoter Payouts/LOV3 ALways/
      (note the case — LOV3 ALways, with capital A — matches existing folder)
    - Filename: LOV3 Promoter Payout_{M_D}_BLKSWAN.pdf
    - Email: Maurice + Black Swan + Anno
    - SMS: Maurice + Eddie (Zelle-to-Black-Swan reminder)

Rates locked per event owner direction:
    Liquor COGS      = 9%    (half of standard 18%)
    Food COGS        = 12.5% (half of standard 25%)
    Mixed Bev Tax    = 3.35% (half of standard 6.7% TX gross receipts)
    Promoter Payout  = 20%
"""
from __future__ import annotations

import base64
import io
import json
import logging
import os
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import requests
from google.cloud import bigquery, secretmanager

logger = logging.getLogger(__name__)

# ── Rates ──────────────────────────────────────────────────────────────
LIQUOR_COGS_PCT = 0.09
FOOD_COGS_PCT = 0.125
MIXED_BEV_TAX_PCT = 0.0335
PROMOTER_PCT = 0.20

# ── Event ─────────────────────────────────────────────────────────────
EVENT_NAME = "LOV3 ALWAYS"
EVENT_WEEKDAY = 5      # Saturday (Mon=0, Sat=5)
EVENT_START_HOUR = 12  # 12 PM
EVENT_END_HOUR = 18    # 6 PM

PROMOTER_NAME = "Black Swan"
PROMOTER_FILE_TAG = "BLKSWAN"
KELVIN_ZELLE_PHONE = "281.455.1253"  # var kept named "KELVIN…" for parity; is Black Swan's

# ── Distribution ──────────────────────────────────────────────────────
# EMAIL_RECIPIENTS reads from env var LOV3_ALWAYS_EMAIL_TO (comma-separated)
# so recipients can be updated without a code redeploy. Default = Maurice only.
_DEFAULT_EMAIL_TO = "Maurice.Ragland@lov3htx.com"
EMAIL_RECIPIENTS = [
    addr.strip() for addr in
    os.environ.get("LOV3_ALWAYS_EMAIL_TO", _DEFAULT_EMAIL_TO).split(",")
    if addr.strip()
]
SMS_RECIPIENTS = [
    ("Maurice", "+15852024804"),
    ("Eddie", "+15852900519"),
]

# ── Integration endpoints ─────────────────────────────────────────────
RESEND_ENDPOINT = "https://api.resend.com/emails"
RESEND_FROM = "LOV3 Analytics <reports@lov3htx.com>"
TWILIO_ENDPOINT_TMPL = "https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json"
DROPBOX_UPLOAD_ENDPOINT = "https://content.dropboxapi.com/2/files/upload"

# ── Storage ────────────────────────────────────────────────────────────
GCS_BUCKET = "toast-analytics-444116-promoter-payouts"
GCS_PREFIX = "lov3-always"
DROPBOX_BASE_PATH = "/Predictive_Models/PMG/Promoter Payouts/LOV3 ALways"


def _colors():
    from reportlab.lib import colors
    return {
        "DARK": colors.HexColor("#1a1a1a"),
        "IVORY": colors.HexColor("#faf7f0"),
        "GOLD": colors.HexColor("#8a7a3d"),
        "GREY": colors.HexColor("#6a6a6a"),
        "RULE": colors.HexColor("#d4c896"),
    }


def _get_secret(name: str) -> Optional[str]:
    try:
        client = secretmanager.SecretManagerServiceClient()
        project = os.environ.get("GCP_PROJECT", "toast-analytics-444116")
        version = client.access_secret_version(request={
            "name": f"projects/{project}/secrets/{name}/versions/latest"
        })
        return version.payload.data.decode("utf-8").strip()
    except Exception as exc:
        logger.warning("Secret '%s' not available: %s", name, exc)
        return None


def _last_saturday(today: date) -> date:
    """Return the last completed Saturday. If today IS Saturday, returns 7d back."""
    days_back = ((today.weekday() - EVENT_WEEKDAY) % 7) or 7
    return today - timedelta(days=days_back)


def _money(v: float) -> str:
    return f"${v:,.2f}"


def _pct(v: float) -> str:
    return f"{v*100:.2f}%"


def fetch_sales(bq: bigquery.Client, start_dt: datetime, end_dt: datetime) -> Dict:
    sql = """
    WITH parsed AS (
      SELECT COALESCE(
        SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', order_date),
        SAFE.PARSE_DATETIME('%m/%d/%y %I:%M %p', order_date)
      ) AS order_dt,
      sales_category, net_price, voided
      FROM `toast-analytics-444116.toast_raw.ItemSelectionDetails_raw`
      WHERE processing_date BETWEEN DATE(@start_dt) AND DATE(@end_dt)
    )
    SELECT
      ROUND(SUM(IF(REGEXP_CONTAINS(LOWER(sales_category),
        r'liquor|beer|wine|bottle|cocktail|spirits|na beverage|n/a beverage'),
        net_price, 0)), 2) AS net_liquor,
      ROUND(SUM(IF(REGEXP_CONTAINS(LOWER(sales_category),
        r'food|kitchen|appetizer|entree|dessert|brunch'),
        net_price, 0)), 2) AS net_food,
      ROUND(SUM(IF(REGEXP_CONTAINS(LOWER(sales_category),
        r'hookah|shisha'),
        net_price, 0)), 2) AS net_shisha,
      COUNT(*) AS item_rows
    FROM parsed
    WHERE order_dt BETWEEN @start_dt AND @end_dt
      AND (voided IS NULL OR LOWER(voided) != 'true')
    """
    job = bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("start_dt", "DATETIME", start_dt.isoformat()),
        bigquery.ScalarQueryParameter("end_dt", "DATETIME", end_dt.isoformat()),
    ]))
    row = list(job)[0]
    return {
        "net_liquor": float(row.net_liquor or 0),
        "net_food": float(row.net_food or 0),
        "net_shisha": float(row.net_shisha or 0),
        "item_rows": int(row.item_rows or 0),
    }


def compute_payout(sales: Dict) -> Dict:
    nl, nf, ns = sales["net_liquor"], sales["net_food"], sales["net_shisha"]
    gross = nl + nf + ns
    liquor_cogs = nl * LIQUOR_COGS_PCT
    food_cogs = nf * FOOD_COGS_PCT
    total_cogs = liquor_cogs + food_cogs
    mixed_bev_tax = nl * MIXED_BEV_TAX_PCT
    net_sales = gross - total_cogs - mixed_bev_tax
    net_profit = net_sales
    payout = net_profit * PROMOTER_PCT
    return {
        **sales,
        "gross_sales": gross,
        "liquor_cogs": liquor_cogs,
        "food_cogs": food_cogs,
        "total_cogs": total_cogs,
        "mixed_bev_tax": mixed_bev_tax,
        "net_sales": net_sales,
        "net_profit": net_profit,
        "payout": payout,
    }


def build_pdf(event_date: date, start_dt: datetime, end_dt: datetime,
              calc: Dict) -> bytes:
    from reportlab.lib.enums import TA_LEFT
    from reportlab.lib.pagesizes import LETTER
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import inch
    from reportlab.platypus import (
        BaseDocTemplate, Frame, PageTemplate, Paragraph, Table, TableStyle,
    )
    from reportlab.lib import colors as _rl_colors

    C = _colors()
    buf = io.BytesIO()

    def footer(canv, doc):
        canv.saveState()
        canv.setFont("Helvetica", 8)
        canv.setFillColor(C["GREY"])
        canv.drawString(0.75 * inch, 0.5 * inch,
                        "LOV3 Analytics · Promoter Payout · Confidential")
        canv.drawRightString(LETTER[0] - 0.75 * inch, 0.5 * inch,
                             f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        canv.restoreState()

    doc = BaseDocTemplate(buf, pagesize=LETTER,
                          leftMargin=0.75 * inch, rightMargin=0.75 * inch,
                          topMargin=0.75 * inch, bottomMargin=0.75 * inch)
    frame = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id="normal")
    doc.addPageTemplates([PageTemplate(id="main", frames=[frame], onPage=footer)])

    styles = getSampleStyleSheet()
    wordmark = ParagraphStyle("wordmark", parent=styles["Title"],
                              fontName="Helvetica-Bold", fontSize=24,
                              textColor=C["DARK"], alignment=TA_LEFT, leading=28, spaceAfter=4)
    eyebrow = ParagraphStyle("eyebrow", parent=styles["Normal"],
                             fontName="Helvetica", fontSize=9,
                             textColor=C["GOLD"], alignment=TA_LEFT, spaceAfter=18)
    h2 = ParagraphStyle("h2", parent=styles["Heading2"],
                        fontName="Helvetica-Bold", fontSize=13,
                        textColor=C["DARK"], alignment=TA_LEFT, spaceBefore=14, spaceAfter=6)
    body = ParagraphStyle("body", parent=styles["Normal"],
                          fontName="Helvetica", fontSize=10, textColor=C["DARK"],
                          alignment=TA_LEFT, leading=14)
    note = ParagraphStyle("note", parent=styles["Normal"],
                          fontName="Helvetica-Oblique", fontSize=9, textColor=C["GREY"],
                          alignment=TA_LEFT, leading=12, spaceBefore=6)

    story = []
    story.append(Paragraph("LOV3 | HTX", wordmark))
    story.append(Paragraph(
        f"{EVENT_NAME} · SATURDAY {event_date.strftime('%B %d, %Y').upper()}",
        eyebrow))

    event_tbl = Table([
        ["Event", "LOV3 Always"],
        ["Event Date", event_date.strftime("%A, %B %d, %Y")],
        ["Time Window",
         f"{start_dt.strftime('%I:%M %p')} – {end_dt.strftime('%I:%M %p')}"],
        ["Item Rows Captured", f"{calc['item_rows']:,}"],
        ["Promoter", PROMOTER_NAME],
        ["Promoter Rate", _pct(PROMOTER_PCT)],
    ], colWidths=[1.6 * inch, 4.5 * inch])
    event_tbl.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("LINEBELOW", (0, 0), (-1, -2), 0.25, C["RULE"]),
    ]))
    story.append(event_tbl)

    story.append(Paragraph("Sales Pulled from Toast (12 PM – 6 PM window)", h2))
    sales_tbl = Table([
        ["Category", "Net Amount"],
        ["Net Liquor (incl. Beer, Wine, NA Bev)", _money(calc["net_liquor"])],
        ["Net Food", _money(calc["net_food"])],
        ["Net Shisha / Hookah", _money(calc["net_shisha"])],
        ["Gross Sales", _money(calc["gross_sales"])],
    ], colWidths=[4.0 * inch, 2.1 * inch])
    sales_tbl.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("BACKGROUND", (0, 0), (-1, 0), C["IVORY"]),
        ("ALIGN", (1, 0), (1, -1), "RIGHT"),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("LINEBELOW", (0, 0), (-1, 0), 0.5, C["DARK"]),
        ("LINEABOVE", (0, -1), (-1, -1), 0.5, C["DARK"]),
    ]))
    story.append(sales_tbl)

    story.append(Paragraph("Rate Adjustments for This Event", h2))
    story.append(Paragraph(
        "COGS and Mixed Beverage Tax applied at <b>HALF</b> of standard rates "
        "per event owner direction. Standard rates in parentheses for reference.",
        body))
    rate_tbl = Table([
        ["Rate", "This Event", "Standard"],
        ["Liquor COGS", _pct(LIQUOR_COGS_PCT), "18.00%"],
        ["Food COGS", _pct(FOOD_COGS_PCT), "25.00%"],
        ["Mixed Beverage Tax", _pct(MIXED_BEV_TAX_PCT), "6.70%"],
        ["Promoter Payout %", _pct(PROMOTER_PCT), "15.00% (default)"],
    ], colWidths=[3.0 * inch, 1.6 * inch, 1.5 * inch])
    rate_tbl.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("TEXTCOLOR", (2, 1), (2, -1), C["GREY"]),
        ("BACKGROUND", (0, 0), (-1, 0), C["IVORY"]),
        ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("LINEBELOW", (0, 0), (-1, 0), 0.5, C["DARK"]),
    ]))
    story.append(rate_tbl)

    story.append(Paragraph("Deductions", h2))
    ded_tbl = Table([
        ["Deduction", "Formula", "Amount"],
        ["Liquor COGS",
         f"{_money(calc['net_liquor'])} × {_pct(LIQUOR_COGS_PCT)}",
         _money(calc["liquor_cogs"])],
        ["Food COGS",
         f"{_money(calc['net_food'])} × {_pct(FOOD_COGS_PCT)}",
         _money(calc["food_cogs"])],
        ["Total COGS Adjustment", "", _money(calc["total_cogs"])],
        ["Mixed Beverage Tax",
         f"{_money(calc['net_liquor'])} × {_pct(MIXED_BEV_TAX_PCT)}",
         _money(calc["mixed_bev_tax"])],
        ["Total Expenses (none provided)", "", _money(0)],
    ], colWidths=[2.6 * inch, 2.5 * inch, 1.0 * inch])
    ded_tbl.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTNAME", (0, 3), (-1, 3), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("BACKGROUND", (0, 0), (-1, 0), C["IVORY"]),
        ("BACKGROUND", (0, 3), (-1, 3), _rl_colors.HexColor("#f0eddf")),
        ("ALIGN", (2, 0), (2, -1), "RIGHT"),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("LINEBELOW", (0, 0), (-1, 0), 0.5, C["DARK"]),
    ]))
    story.append(ded_tbl)

    story.append(Paragraph("Payout Calculation", h2))
    payout_tbl = Table([
        ["Line", "Amount"],
        ["Gross Sales", _money(calc["gross_sales"])],
        ["Less: Total COGS Adjustment", f"({_money(calc['total_cogs'])})"],
        ["Less: Mixed Beverage Tax", f"({_money(calc['mixed_bev_tax'])})"],
        ["Net Sales", _money(calc["net_sales"])],
        ["Less: Total Expenses", "($0.00)"],
        ["Net Profit", _money(calc["net_profit"])],
        ["Promoter Payout Rate", _pct(PROMOTER_PCT)],
        ["PROMOTER PAYOUT", _money(calc["payout"])],
    ], colWidths=[4.0 * inch, 2.1 * inch])
    payout_tbl.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ("FONTNAME", (0, 4), (-1, 4), "Helvetica-Bold"),
        ("FONTNAME", (0, 6), (-1, 6), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -2), 10),
        ("FONTSIZE", (0, -1), (-1, -1), 13),
        ("TEXTCOLOR", (0, -1), (-1, -1), C["GOLD"]),
        ("BACKGROUND", (0, 0), (-1, 0), C["IVORY"]),
        ("BACKGROUND", (0, -1), (-1, -1), _rl_colors.HexColor("#fdf6d7")),
        ("ALIGN", (1, 0), (1, -1), "RIGHT"),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, -1), (-1, -1), 10),
        ("TOPPADDING", (0, -1), (-1, -1), 10),
        ("LINEABOVE", (0, 4), (-1, 4), 0.5, C["DARK"]),
        ("LINEABOVE", (0, 6), (-1, 6), 0.5, C["DARK"]),
        ("LINEABOVE", (0, -1), (-1, -1), 1.0, C["DARK"]),
    ]))
    story.append(payout_tbl)

    story.append(Paragraph(
        "Methodology: Item sales filtered by <b>order_date</b> (not sent_date "
        "or paid_date) between the time window boundaries, excluding voided items. "
        "NA Beverage rolled into Net Liquor per LOV3 standard.",
        note))

    doc.build(story)
    return buf.getvalue()


def save_to_dropbox(pdf: bytes, event_date: date) -> Dict:
    result = {"attempted": False, "sent": False, "path": None, "error": None}
    token = _get_secret("dropbox-access-token")
    if not token:
        result["error"] = "dropbox-access-token secret not set"
        return result
    result["attempted"] = True
    folder = f"{event_date.month}_{event_date.day}"
    filename = f"LOV3 Promoter Payout_{folder}_{PROMOTER_FILE_TAG}.pdf"
    dropbox_path = f"{DROPBOX_BASE_PATH}/{folder}/{filename}"
    result["path"] = dropbox_path
    try:
        resp = requests.post(
            DROPBOX_UPLOAD_ENDPOINT,
            headers={
                "Authorization": f"Bearer {token}",
                "Dropbox-API-Arg": json.dumps({
                    "path": dropbox_path,
                    "mode": "overwrite",
                    "autorename": False,
                    "mute": True,
                }),
                "Content-Type": "application/octet-stream",
            },
            data=pdf,
            timeout=60,
        )
        resp.raise_for_status()
        result["sent"] = True
        logger.info("Dropbox: saved %s (%d bytes)", dropbox_path, len(pdf))
    except Exception as exc:
        result["error"] = str(exc)
        logger.error("Dropbox save failed: %s", exc)
    return result


def save_to_gcs(pdf: bytes, event_date: date) -> Dict:
    result = {"attempted": True, "sent": False, "uri": None, "error": None}
    from google.cloud import storage
    try:
        folder = f"{event_date.month}_{event_date.day}"
        filename = f"LOV3 Promoter Payout_{folder}_{PROMOTER_FILE_TAG}.pdf"
        blob_name = f"{GCS_PREFIX}/{folder}/{filename}"
        client = storage.Client(project="toast-analytics-444116")
        bucket = client.bucket(GCS_BUCKET)
        try:
            bucket.reload()
        except Exception:
            client.create_bucket(bucket, location="US-CENTRAL1")
        blob = bucket.blob(blob_name)
        blob.upload_from_string(pdf, content_type="application/pdf")
        result["sent"] = True
        result["uri"] = f"gs://{GCS_BUCKET}/{blob_name}"
        logger.info("GCS: saved %s (%d bytes)", result["uri"], len(pdf))
    except Exception as exc:
        result["error"] = str(exc)
        logger.error("GCS save failed: %s", exc)
    return result


def send_email(pdf: bytes, event_date: date, calc: Dict) -> Dict:
    result = {"attempted": False, "sent": False, "resend_id": None, "error": None}
    api_key = _get_secret("resend-api-key")
    if not api_key:
        result["error"] = "resend-api-key secret not set"
        return result
    if not EMAIL_RECIPIENTS:
        result["error"] = "EMAIL_RECIPIENTS empty"
        return result
    result["attempted"] = True
    folder = f"{event_date.month}_{event_date.day}"
    filename = f"LOV3 Promoter Payout_{folder}_{PROMOTER_FILE_TAG}.pdf"
    try:
        html = f"""
<div style="font-family:-apple-system,BlinkMacSystemFont,sans-serif;
            max-width:640px;color:#111;line-height:1.55;font-size:15px">
  <h2 style="font-family:Georgia,serif;font-weight:600;margin:0 0 6px 0">
    LOV3 Always · Saturday {event_date.strftime('%B %d, %Y')}
  </h2>
  <p style="color:#8a7a3d;font-size:12px;letter-spacing:1.5px;
            text-transform:uppercase;margin:0 0 20px 0">
    Promoter: {PROMOTER_NAME} · 12 PM – 6 PM Window · 20% Payout
  </p>
  <ul>
    <li>Gross Sales: <b>{_money(calc['gross_sales'])}</b></li>
    <li>Total COGS: <b>{_money(calc['total_cogs'])}</b>
      (Liquor {_money(calc['liquor_cogs'])} + Food {_money(calc['food_cogs'])})</li>
    <li>Mixed Beverage Tax: <b>{_money(calc['mixed_bev_tax'])}</b></li>
    <li>Net Sales: <b>{_money(calc['net_sales'])}</b></li>
    <li>Promoter Payout: <b style="color:#8a7a3d">{_money(calc['payout'])}</b></li>
  </ul>
  <p>Full PDF attached.</p>
  <p style="color:#6a6a6a;font-size:12px;margin-top:20px">— LOV3 Analytics</p>
</div>
"""
        text = (
            f"LOV3 Always — Saturday {event_date}\n"
            f"Promoter: {PROMOTER_NAME}\n\n"
            f"Gross Sales: {_money(calc['gross_sales'])}\n"
            f"Total COGS: {_money(calc['total_cogs'])}\n"
            f"Mixed Bev Tax: {_money(calc['mixed_bev_tax'])}\n"
            f"Net Sales: {_money(calc['net_sales'])}\n"
            f"Promoter Payout: {_money(calc['payout'])}\n"
        )
        resp = requests.post(
            RESEND_ENDPOINT,
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "from": RESEND_FROM,
                "to": EMAIL_RECIPIENTS,
                "subject": (f"LOV3 Always Promoter Payout — "
                            f"Saturday {event_date.strftime('%B %d, %Y')}"),
                "html": html,
                "text": text,
                "attachments": [{
                    "filename": filename,
                    "content": base64.b64encode(pdf).decode("ascii"),
                }],
            },
            timeout=45,
        )
        resp.raise_for_status()
        result["sent"] = True
        result["resend_id"] = resp.json().get("id")
        logger.info("Email sent — Resend id=%s to %s",
                    result["resend_id"], EMAIL_RECIPIENTS)
    except Exception as exc:
        result["error"] = str(exc)
        logger.error("Email send failed: %s", exc)
    return result


def send_sms(event_date: date, calc: Dict) -> Dict:
    result = {"attempted": False, "recipients": [], "error": None}
    account_sid = _get_secret("twilio-account-sid")
    auth_token = _get_secret("twilio-auth-token")
    messaging_service_sid = _get_secret("twilio-messaging-service-sid")
    from_number = (os.environ.get("TWILIO_FROM_NUMBER")
                   or _get_secret("twilio-from-number"))

    if not (account_sid and auth_token and (messaging_service_sid or from_number)):
        result["error"] = "Twilio not configured"
        return result

    result["attempted"] = True
    body = (
        f"Send Zelle to {PROMOTER_NAME} phone #{KELVIN_ZELLE_PHONE} for "
        f"LOV3 Always Promotion: Saturday {event_date.strftime('%m/%d/%Y')}\n\n"
        f"Net Total: {_money(calc['net_sales'])}\n"
        f"COGS: {_money(calc['total_cogs'])}\n"
        f"Mixed Bev Tax: {_money(calc['mixed_bev_tax'])}\n"
        f"Promoter Payout: {_money(calc['payout'])}"
    )

    url = TWILIO_ENDPOINT_TMPL.format(sid=account_sid)
    for name, phone in SMS_RECIPIENTS:
        row = {"name": name, "to": phone, "sent": False, "sid": None, "error": None}
        try:
            data = {"To": phone, "Body": body}
            if messaging_service_sid:
                data["MessagingServiceSid"] = messaging_service_sid
            else:
                data["From"] = from_number
            resp = requests.post(
                url, data=data, auth=(account_sid, auth_token), timeout=30,
            )
            resp.raise_for_status()
            row["sent"] = True
            row["sid"] = resp.json().get("sid")
            logger.info("SMS sent to %s (%s) — sid=%s", name, phone, row["sid"])
        except Exception as exc:
            row["error"] = str(exc)
            logger.error("SMS to %s (%s) failed: %s", name, phone, exc)
        result["recipients"].append(row)
    return result


def run_weekly(force_date: Optional[str] = None) -> Dict:
    ct = ZoneInfo("America/Chicago")
    if force_date:
        event_date = datetime.strptime(force_date, "%Y-%m-%d").date()
    else:
        today_ct = datetime.now(ct).date()
        event_date = _last_saturday(today_ct)

    start_dt = datetime.combine(event_date, datetime.min.time()).replace(
        hour=EVENT_START_HOUR)
    end_dt = datetime.combine(event_date, datetime.min.time()).replace(
        hour=EVENT_END_HOUR)

    bq = bigquery.Client(project="toast-analytics-444116")
    sales = fetch_sales(bq, start_dt, end_dt)
    calc = compute_payout(sales)

    pdf = build_pdf(event_date, start_dt, end_dt, calc)

    dropbox_res = save_to_dropbox(pdf, event_date)
    gcs_res = save_to_gcs(pdf, event_date)
    email_res = send_email(pdf, event_date, calc)
    sms_res = send_sms(event_date, calc)

    return {
        "status": "success",
        "event_name": EVENT_NAME,
        "event_date": event_date.isoformat(),
        "window": {"start": start_dt.isoformat(), "end": end_dt.isoformat()},
        "pdf_bytes": len(pdf),
        "calc": {k: round(v, 2) if isinstance(v, float) else v
                 for k, v in calc.items()},
        "dropbox": dropbox_res,
        "gcs": gcs_res,
        "email": email_res,
        "sms": sms_res,
    }
