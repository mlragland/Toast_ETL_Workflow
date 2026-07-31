"""Render LOV3_COMP_STRATEGIC_REVIEW.md into a professionally styled PDF.

Same design system as the weekly comp report — dark-on-cream, gold accent,
LOV3|HTX wordmark cover. Suitable for leadership distribution and printing.
"""

from __future__ import annotations

import base64
import io
import re
import sys
from pathlib import Path
from datetime import datetime

import requests
from google.cloud import secretmanager
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

from config import PROJECT_ID

RESEND_ENDPOINT = "https://api.resend.com/emails"
RESEND_FROM = "LOV3 Analytics <reports@lov3htx.com>"

# ── Design System (matches comp_report_pdf.py) ──────────────────────

BLACK = colors.HexColor("#111111")
INK = colors.HexColor("#1a1a1a")
GOLD = colors.HexColor("#B8956A")
BONE = colors.HexColor("#6a6a6a")
CREAM = colors.HexColor("#f9f6f0")
RULE = colors.HexColor("#e5e5e5")
RED = colors.HexColor("#C97064")
GREEN = colors.HexColor("#7FB069")
AMBER = colors.HexColor("#D4A24C")

_ss = getSampleStyleSheet()

STYLE_H1 = ParagraphStyle(
    "h1", parent=_ss["Heading1"], fontSize=22, leading=27,
    textColor=BLACK, fontName="Helvetica-Bold", spaceBefore=12, spaceAfter=10,
)
STYLE_H2 = ParagraphStyle(
    "h2", parent=_ss["Heading2"], fontSize=14, leading=18,
    textColor=BLACK, fontName="Helvetica-Bold", spaceBefore=14, spaceAfter=6,
)
STYLE_H3 = ParagraphStyle(
    "h3", parent=_ss["Heading3"], fontSize=11, leading=14,
    textColor=BLACK, fontName="Helvetica-Bold", spaceBefore=8, spaceAfter=4,
)
STYLE_BODY = ParagraphStyle(
    "body", parent=_ss["BodyText"], fontSize=10, leading=14,
    textColor=INK, spaceAfter=6, alignment=TA_LEFT,
)
STYLE_LEAD = ParagraphStyle(
    "lead", parent=_ss["BodyText"], fontSize=11, leading=16,
    textColor=INK, spaceAfter=8,
)
STYLE_LI = ParagraphStyle(
    "li", parent=STYLE_BODY, leftIndent=14, bulletIndent=0, spaceAfter=3,
)
STYLE_SMALL = ParagraphStyle(
    "small", parent=_ss["BodyText"], fontSize=8, leading=10, textColor=BONE,
)
STYLE_CENTER = ParagraphStyle(
    "center", parent=STYLE_BODY, alignment=TA_CENTER,
)
STYLE_CODE = ParagraphStyle(
    "code", parent=STYLE_BODY, fontName="Courier", fontSize=9, leading=12,
    textColor=INK,
)

TABLE_STYLE_STANDARD = TableStyle([
    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
    ("FONTSIZE", (0, 0), (-1, -1), 8.5),
    ("BACKGROUND", (0, 0), (-1, 0), BLACK),
    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
    ("ALIGN", (0, 0), (-1, -1), "LEFT"),
    ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ("TOPPADDING", (0, 0), (-1, -1), 5),
    ("LINEBELOW", (0, 0), (-1, 0), 1, BLACK),
    ("LINEBELOW", (0, -1), (-1, -1), 0.5, RULE),
    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, CREAM]),
])


def _get_secret(name: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    resource = f"projects/{PROJECT_ID}/secrets/{name}/versions/latest"
    return client.access_secret_version(name=resource).payload.data.decode("UTF-8").strip()


def _footer(canv: canvas.Canvas, doc):
    canv.saveState()
    canv.setFont("Helvetica", 7)
    canv.setFillColor(BONE)
    canv.drawString(0.55 * inch, 0.35 * inch,
                    "LOV3|HTX Confidential · Strategic Review v2.0")
    canv.drawCentredString(LETTER[0] / 2, 0.35 * inch,
                            f"Page {canv.getPageNumber()}")
    canv.drawRightString(LETTER[0] - 0.55 * inch, 0.35 * inch,
                          datetime.now().strftime("%B %-d, %Y"))
    canv.restoreState()


def _wrap(text: str) -> Paragraph:
    """Wrap raw text (escape HTML) into a Paragraph for table cells."""
    import html
    return Paragraph(html.escape(str(text)), STYLE_BODY)


# ── Markdown → ReportLab conversion ─────────────────────────────────


def _inline_md(text: str) -> str:
    """Convert inline markdown to ReportLab-compatible markup."""
    # Escape HTML entities first (but preserve markdown syntax markers)
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    # Bold
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    # Italic
    text = re.sub(r"(?<!\*)\*([^*]+?)\*(?!\*)", r"<i>\1</i>", text)
    # Inline code
    text = re.sub(r"`([^`]+)`", r'<font name="Courier" color="#B8956A">\1</font>', text)
    return text


def _parse_table(lines: list, i: int) -> tuple:
    """Parse a markdown table starting at lines[i]. Return (Table story, next i)."""
    header = [c.strip() for c in lines[i].strip().strip("|").split("|")]
    # Skip separator row
    j = i + 2
    rows = [header]
    while j < len(lines) and lines[j].strip().startswith("|"):
        cells = [c.strip() for c in lines[j].strip().strip("|").split("|")]
        rows.append(cells)
        j += 1
    # Wrap all cells in Paragraphs so they flow
    wrapped = [
        [Paragraph(_inline_md(c), STYLE_BODY) for c in row]
        for row in rows
    ]
    ncols = len(header)
    # Distribute column widths — favor first column slightly
    total_width = 7.0
    col_widths = [total_width / ncols] * ncols
    t = Table(wrapped, colWidths=[w * inch for w in col_widths], repeatRows=1)
    t.setStyle(TABLE_STYLE_STANDARD)
    return t, j


def md_to_story(md_text: str) -> list:
    """Convert markdown to a ReportLab story (list of flowables)."""
    story = []
    lines = md_text.split("\n")
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.rstrip()

        # ── Table ──
        if stripped.startswith("|") and i + 1 < len(lines) and re.match(
            r"^\s*\|[\-:\s|]+\|\s*$", lines[i + 1]
        ):
            t, next_i = _parse_table(lines, i)
            story.append(t)
            story.append(Spacer(1, 0.1 * inch))
            i = next_i
            continue

        # ── Headings ──
        if stripped.startswith("# "):
            story.append(Paragraph(_inline_md(stripped[2:]), STYLE_H1))
        elif stripped.startswith("## "):
            story.append(Paragraph(_inline_md(stripped[3:]), STYLE_H2))
        elif stripped.startswith("### "):
            story.append(Paragraph(_inline_md(stripped[4:]), STYLE_H3))
        # ── Horizontal rule ──
        elif stripped == "---":
            story.append(Spacer(1, 0.05 * inch))
            # Draw a rule as an empty single-cell table with border-top
            hr = Table([[""]], colWidths=[7.0 * inch], rowHeights=[1])
            hr.setStyle(TableStyle([("LINEABOVE", (0, 0), (-1, 0), 0.5, RULE)]))
            story.append(hr)
            story.append(Spacer(1, 0.05 * inch))
        # ── Bulleted list ──
        elif stripped.startswith("- ") or stripped.startswith("* "):
            content = _inline_md(stripped[2:])
            story.append(Paragraph(f"• {content}", STYLE_LI))
        # ── Checkbox list ──
        elif stripped.startswith("- [ ] "):
            content = _inline_md(stripped[6:])
            story.append(Paragraph(f"☐ {content}", STYLE_LI))
        # ── Numbered list ──
        elif re.match(r"^\d+\. ", stripped):
            m = re.match(r"^(\d+)\. (.*)$", stripped)
            if m:
                story.append(Paragraph(
                    f"{m.group(1)}. {_inline_md(m.group(2))}", STYLE_LI,
                ))
        # ── Blockquote ──
        elif stripped.startswith("> "):
            story.append(Paragraph(f"<i>{_inline_md(stripped[2:])}</i>", STYLE_LEAD))
        # ── Blank line ──
        elif not stripped:
            story.append(Spacer(1, 0.06 * inch))
        # ── Regular paragraph ──
        else:
            story.append(Paragraph(_inline_md(stripped), STYLE_BODY))

        i += 1
    return story


# ── Cover page ──────────────────────────────────────────────────────


def build_cover() -> list:
    """LOV3|HTX cover page for the strategic review."""
    wordmark_style = ParagraphStyle(
        "wordmark", parent=STYLE_CENTER, fontSize=42, leading=52,
        fontName="Helvetica-Bold", spaceAfter=6,
    )
    eyebrow_style = ParagraphStyle(
        "eyebrow", parent=STYLE_CENTER, fontSize=9, leading=12,
        textColor=GOLD, fontName="Helvetica-Bold",
    )
    title_style = ParagraphStyle(
        "title", parent=STYLE_CENTER, fontSize=24, leading=30,
        textColor=BLACK, fontName="Helvetica-Bold", spaceAfter=8,
    )
    subtitle_style = ParagraphStyle(
        "subtitle", parent=STYLE_CENTER, fontSize=13, leading=17, textColor=BONE,
    )

    story = []
    story.append(Spacer(1, 1.5 * inch))
    story.append(Paragraph(
        '<font color="#111111"><b>LOV3</b></font>'
        '<font color="#B8956A"><b>|</b></font>'
        '<font color="#111111"><b>HTX</b></font>',
        wordmark_style,
    ))
    story.append(Spacer(1, 0.35 * inch))
    story.append(Paragraph("STRATEGIC · COMP DISCIPLINE · LEADERSHIP", eyebrow_style))
    story.append(Spacer(1, 0.55 * inch))
    story.append(Paragraph("Comp Discipline Strategic Review", title_style))
    story.append(Paragraph("&amp; VIC3 Readiness Assessment", title_style))
    story.append(Spacer(1, 0.15 * inch))
    story.append(Paragraph(
        f"Version 2.0 · {datetime.now().strftime('%B %-d, %Y')}",
        subtitle_style,
    ))

    story.append(Spacer(1, 1.4 * inch))

    # Distribution + confidentiality
    dist = [
        ["DISTRIBUTION", ""],
        ["Owners", "Maurice Ragland · Eddie · Derwin"],
        ["Managers", "Tiffany Loving · Anthony Winn · Dajah Bishop · Ashley Baines"],
        ["Classification", "CONFIDENTIAL — For Leadership Only"],
        ["Basis", "9 domain-expert reviews · 90 days LOV3 operational data"],
        ["Version notes", "v2.0 reflects Ashley reclassified to Manager (2026-07-31)"],
    ]
    t = Table(dist, colWidths=[1.8 * inch, 4.6 * inch])
    t.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 8),
        ("TEXTCOLOR", (0, 0), (-1, 0), GOLD),
        ("FONTSIZE", (0, 1), (-1, -1), 9.5),
        ("FONTNAME", (0, 1), (0, -1), "Helvetica-Bold"),
        ("TEXTCOLOR", (0, 1), (0, -1), BLACK),
        ("TEXTCOLOR", (1, 1), (1, -1), INK),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (0, -1), 16),
        ("LEFTPADDING", (1, 0), (1, -1), 12),
        ("LINEBELOW", (0, 0), (-1, 0), 0.5, GOLD),
    ]))
    story.append(t)
    story.append(PageBreak())
    return story


# ── Orchestrator ────────────────────────────────────────────────────


def build_pdf(md_path: str) -> bytes:
    md_text = Path(md_path).read_text()
    # Strip the H1 title + preamble metadata (already handled by cover)
    # Find the first section separator "---" and drop everything before it
    parts = md_text.split("---", 1)
    body_md = parts[1] if len(parts) > 1 else md_text

    buf = io.BytesIO()
    doc = BaseDocTemplate(
        buf, pagesize=LETTER,
        leftMargin=0.6 * inch, rightMargin=0.6 * inch,
        topMargin=0.6 * inch, bottomMargin=0.75 * inch,
        title="LOV3 Comp Strategic Review v2.0",
        author="LOV3 Analytics",
    )
    frame = Frame(doc.leftMargin, doc.bottomMargin,
                  doc.width, doc.height, showBoundary=0)
    doc.addPageTemplates([PageTemplate(id="main", frames=[frame], onPage=_footer)])

    story = []
    story += build_cover()
    story += md_to_story(body_md)

    doc.build(story)
    return buf.getvalue()


def send_pdf(to_email: str, pdf_bytes: bytes) -> dict:
    api_key = _get_secret("resend-api-key")
    payload = {
        "from": RESEND_FROM,
        "to": [to_email],
        "subject": "LOV3|HTX Comp Discipline Strategic Review v2.0 — PDF",
        "html": """
        <div style="font-family:-apple-system,BlinkMacSystemFont,sans-serif;max-width:640px;color:#111">
        <p><b>Attached:</b> LOV3 Comp Discipline Strategic Review v2.0 (PDF).</p>
        <p>Updates from v1.0 (markdown):</p>
        <ul>
          <li>Ashley Baines reclassified from Bar Lead to Manager based on duties (approved $488 Manager Comp checks per audit)</li>
          <li>Peer benchmark cohort now: Tiffany · Tony · Daja · Ashley</li>
          <li>SWOT + gap analysis + VIC3 non-negotiables + 30-day action plan updated accordingly</li>
          <li>Signoff block updated</li>
        </ul>
        <p style="color:#6a6a6a;font-size:12px">Confidential — For Leadership Only</p>
        </div>
        """,
        "text": "See attachment.",
        "attachments": [{
            "filename": "LOV3_Comp_Strategic_Review_v2.pdf",
            "content": base64.b64encode(pdf_bytes).decode("ascii"),
        }],
    }
    resp = requests.post(RESEND_ENDPOINT, json=payload,
                         headers={"Authorization": f"Bearer {api_key}"},
                         timeout=30)
    resp.raise_for_status()
    return resp.json()


def main():
    md_path = "/Users/maurice_mac/Library/CloudStorage/Dropbox/Developer/Toast_ETL_Workflow/LOV3_COMP_STRATEGIC_REVIEW.md"
    pdf = build_pdf(md_path)
    out = Path("/tmp/LOV3_Comp_Strategic_Review_v2.pdf")
    out.write_bytes(pdf)
    print(f"Wrote {out} ({len(pdf):,} bytes)")
    to = sys.argv[1] if len(sys.argv) > 1 else "maurice.ragland@lov3htx.com"
    result = send_pdf(to, pdf)
    print(f"Sent — Resend id={result.get('id')}")


if __name__ == "__main__":
    main()
