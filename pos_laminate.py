"""POS Station Tab-Naming Laminate — 1-page cheat sheet for staff training.

Generates a laminatable PDF for POS stations. Distilled from
TAB_NAMING_STANDARDS.md into a scannable one-page format that servers,
managers, and bar leads can reference during shift.

Print at 8.5" × 11", laminate, post at every POS station.
"""

from __future__ import annotations

import io
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
)

BLACK = colors.HexColor("#111111")
GOLD = colors.HexColor("#B8956A")
CREAM = colors.HexColor("#f9f6f0")
RED = colors.HexColor("#C97064")
GREEN = colors.HexColor("#7FB069")

_ss = getSampleStyleSheet()
H1 = ParagraphStyle("h1", parent=_ss["Heading1"], fontSize=20, leading=24,
                     textColor=BLACK, fontName="Helvetica-Bold", alignment=TA_CENTER)
H2 = ParagraphStyle("h2", parent=_ss["Heading2"], fontSize=12, leading=15,
                     textColor=BLACK, fontName="Helvetica-Bold",
                     spaceBefore=10, spaceAfter=4)
BODY = ParagraphStyle("body", parent=_ss["BodyText"], fontSize=9.5, leading=12,
                       textColor=BLACK, spaceAfter=3)
SMALL = ParagraphStyle("small", parent=_ss["BodyText"], fontSize=8, leading=10,
                        textColor=BLACK)
CENTER = ParagraphStyle("center", parent=BODY, alignment=TA_CENTER)
GOLD_LABEL = ParagraphStyle("gold", parent=_ss["BodyText"], fontSize=8, leading=10,
                              textColor=GOLD, fontName="Helvetica-Bold",
                              alignment=TA_CENTER)

CONVENTION_TABLE_STYLE = TableStyle([
    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
    ("FONTSIZE", (0, 0), (-1, -1), 8.5),
    ("BACKGROUND", (0, 0), (-1, 0), BLACK),
    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
    ("ALIGN", (0, 0), (-1, -1), "LEFT"),
    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ("TOPPADDING", (0, 0), (-1, -1), 4),
    ("LINEBELOW", (0, 0), (-1, 0), 0.75, BLACK),
    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, CREAM]),
    ("FONTNAME", (1, 1), (1, -1), "Courier-Bold"),
])


def build_laminate() -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=LETTER,
        leftMargin=0.4 * inch, rightMargin=0.4 * inch,
        topMargin=0.4 * inch, bottomMargin=0.4 * inch,
        title="LOV3 POS Tab-Naming Cheat Sheet",
    )
    story = []

    # ── Header ──
    story.append(Paragraph(
        '<font size="26" color="#111111"><b>LOV3</b></font>'
        '<font size="26" color="#B8956A"><b>|</b></font>'
        '<font size="26" color="#111111"><b>HTX</b></font>',
        CENTER,
    ))
    story.append(Paragraph("POS TAB-NAMING · QUICK REFERENCE", GOLD_LABEL))
    story.append(Spacer(1, 0.15 * inch))

    # ── The 8 conventions ──
    rows = [
        ["Situation", "Tab format", "Example"],
        ["Birthday Package (pre-registered)",
         "Bday-{D}-{FirstName}[-{LI}]",
         "Bday-F-Kelly-A"],
        ["Promoter event",
         "Promoter - {Day} - {Event} - {POC}",
         "Promoter - Thursday - Afrikan - Kelvin"],
        ["Owner personal tab",
         "Owner - {Name}[-{Guest}]",
         "Owner - Maurice - Alex"],
        ["Owner tasting event",
         "Owner Tasting - {Item/Guest}",
         "Owner Tasting - Lalo"],
        ["VIP comp",
         "VIP - {Guest} - {Reason}",
         "VIP - Chef Torres - Industry"],
        ["Wycliffe host-stand welcome",
         "Wycliffe - Host Stand",
         "Wycliffe - Host Stand"],
        ["Recovery / spillage",
         "{Reason} - {Item}",
         "Spill - Don Julio  ·  Bug - Fries"],
        ["Distributor tasting",
         "Tasting - {Distributor}",
         "Tasting - Sazerac"],
        ["Regular guest",
         "{TableCode} - {GuestName}",
         "E12 - Sarah"],
    ]
    tbl = Table(rows, colWidths=[2.5 * inch, 2.6 * inch, 2.6 * inch])
    tbl.setStyle(CONVENTION_TABLE_STYLE)
    story.append(tbl)

    # ── DO / DON'T box ──
    story.append(Spacer(1, 0.15 * inch))

    do_dont_rows = [
        [Paragraph("<b><font color='#7FB069'>DO</font></b>", BODY),
         Paragraph("<b><font color='#C97064'>DON'T</font></b>", BODY)],
        [
            Paragraph(
                "✓ Match birthday guest first name to SR reservation<br/>"
                "✓ Use day-letter: <b>W</b>ed · <b>F</b>ri · <b>S</b>at · s<b>U</b>n<br/>"
                "✓ Add last initial if two guests share a first name<br/>"
                "✓ One tab per birthday party (no combining)<br/>"
                "✓ Ring Wycliffe pours on 'Wycliffe - Host Stand' every time<br/>"
                "✓ Use recovery reason keywords (Spill / Bug / Broke)",
                SMALL,
            ),
            Paragraph(
                "✗ Generic 'Sat Bday' / 'Fri Bday' / 'Bday' tabs<br/>"
                "✗ Multiple birthday parties on one tab<br/>"
                "✗ Ring bottles on unnamed / blank tabs (Bottle Manager)<br/>"
                "✗ Use OWNER SKUs on guest tabs (only owner-designated)<br/>"
                "✗ Skip 'Wycliffe - Host Stand' when pouring at the door<br/>"
                "✗ 'Open $' or 'Open %' comps without a reason code",
                SMALL,
            ),
        ],
    ]
    dd = Table(do_dont_rows, colWidths=[3.85 * inch, 3.85 * inch])
    dd.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("BOX", (0, 0), (0, -1), 1, GREEN),
        ("BOX", (1, 0), (1, -1), 1, RED),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))
    story.append(dd)

    # ── End-of-shift birthday check ──
    story.append(Spacer(1, 0.15 * inch))
    story.append(Paragraph("END-OF-SHIFT BIRTHDAY CHECK (5 minutes)", H2))
    ritual = [
        "1. Open the SR birthday reservation list for tonight",
        "2. Search Toast for tabs starting with <b>Bday-{TodayLetter}-</b>",
        "3. Match every pre-registered guest to a Bday tab (1-to-1)",
        "4. If a pre-reg guest has NO Bday tab: note reason (declined, closed early, etc.)",
        "5. Post to <b>#lov3-leader-report</b>:",
        "&nbsp;&nbsp;&nbsp;&nbsp;<i>\"Sat Bday check: 5/7 delivered. Missed: Sarai (declined), Chelsea (left early)\"</i>",
    ]
    for line in ritual:
        story.append(Paragraph(line, BODY))

    # ── Day-of-week birthday program ──
    story.append(Spacer(1, 0.1 * inch))
    story.append(Paragraph("BIRTHDAY PACKAGE BY DAY", H2))
    program_rows = [
        ["Day", "Champagne", "Ring Method"],
        ["Wednesday", "Bellaire Rose BTL", "Fully comped ($169)"],
        ["Friday", "OWNER MOET ROSE", "Cost basis ($47-282)"],
        ["Saturday", "OWNER MOET ROSE / ICE", "Cost basis ($47-282)"],
        ["Sunday", "Bellaire Rose BTL", "Retail or cost basis"],
        ["Mon / Tue / Thu", "— no package program —", "—"],
    ]
    pt = Table(program_rows, colWidths=[1.5 * inch, 2.5 * inch, 3.7 * inch])
    pt.setStyle(CONVENTION_TABLE_STYLE)
    story.append(pt)

    # ── Footer ──
    story.append(Spacer(1, 0.15 * inch))
    story.append(Paragraph(
        "<font size='7' color='#6a6a6a'><b>QUESTIONS?</b> "
        "Full details: <code>TAB_NAMING_STANDARDS.md</code> · "
        "<code>COMP_MANAGEMENT_POLICY.md</code> · "
        "Report tab-naming issues to shift manager. "
        "v1.0 · 2026-07-30</font>",
        CENTER,
    ))

    doc.build(story)
    return buf.getvalue()


if __name__ == "__main__":
    import sys
    from pathlib import Path
    pdf = build_laminate()
    out_path = Path(sys.argv[1] if len(sys.argv) > 1 else "/tmp/lov3_pos_laminate.pdf")
    out_path.write_bytes(pdf)
    print(f"Wrote {out_path} ({len(pdf):,} bytes)")
