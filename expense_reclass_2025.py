"""
LOV3 HTX — 2025 Expense Reclassification Schedule (K-1 prep companion)

Pulls every 2025 transaction in the gray-area bank categories and proposes a
tax classification per transaction, with the documentation needed to sustain it.
Output: LOV3_2025_Expense_Reclassification_Schedule.xlsx (2 tabs).

Buckets:
  A  DEDUCTIBLE        — reclassify to a business category (docs/1099 required)
  B  MEAL 50%          — business meal, 50% deductible (Sec. 274(n)) with who/why memo
  C  NONDEDUCTIBLE     — owner distribution (Eddie Jasper, Line 19A)
  D  PENALTY ADDBACK   — Sec. 162(f), never deductible, not a distribution
  E  CAPITALIZE        — Form 4562; Sec. 179 / bonus depreciation recovery
  F  VIC3 DEDUCTIBLE   — LOV3 is guarantor on the VIC3 construction loan
  G  CARD CHARGES      — Marketing & Ad spend + SMS blasts paid on LOV3 cards (deductible)

Owner directives applied: all bank cards (*1201, *1370) are LOV3 business cards;
nondeductible personal spend is attributed to Eddie Jasper as distributions;
ownership Eddie 45 / Derwin 40 / Maurice 15; VIC3 payments deductible per LOV3's
guarantor position on the VIC3 construction loan (owner directive 2026-10-11);
formerly-review Zelle/vehicle items owner-confirmed as LOV3 business expenses.
"""

import sys
from google.cloud import bigquery

PACKAGE_VERSION = "2.1"
PACKAGE_VERSION_DATE = "2026-10-11"

OUT_PATH = "LOV3_2025_Expense_Reclassification_Schedule.xlsx"

CATEGORIES = [
    "6. General & Administrative / Corporate/Owner Discretionary Expenses",
    "6. General & Administrative / Corporate/Personal Meals",
    "6. General & Administrative / Corporate/Competitive Research",
    "6. General & Administrative / Corporate/Transportation",
    "6. General & Administrative / Corporate/Owner Travel",
    "6. General & Administrative / Corporate/Travel & Lodging",
    "6. General & Administrative / Corporate/Travel & Entertainment",
    "6. General & Administrative / Corporate/Credit Card Payments",
    "5. Operating Expenses (OPEX)/Penalties & Fines",
    "7. Facility & Tennant Improvements/Capital Equipment Expense",
    "7. Facility & Tennant Improvements/Construction Build Out",
]

BUCKETS = {
    "A": ("DEDUCTIBLE — reclassify", 1.0),
    "B": ("MEAL 50% — Sec. 274(n)", 0.5),
    "C": ("NONDEDUCTIBLE — Eddie distribution", 0.0),
    "D": ("PENALTY ADDBACK — Sec. 162(f)", 0.0),
    "E": ("CAPITALIZE — Form 4562 (Sec. 179/bonus)", 0.0),
    "F": ("VIC3 — DEDUCTIBLE (LOV3 loan guarantor)", 1.0),
    "G": ("CARD CHARGES — DEDUCTIBLE (Marketing/Ads/SMS)", 1.0),
}


def classify(category: str, desc: str) -> tuple:
    """Return (bucket, proposed_category, docs_required)."""
    d = desc.upper()

    if "Penalties & Fines" in category:
        return ("D", "Nondeductible penalties (M-1 addback)",
                "None — city/government fines are never deductible")

    if "Credit Card Payments" in category:
        return ("G", "Marketing & Advertising / SMS blasts (owner-confirmed LOV3 expense)",
                "Owner-confirmed 2026-10-11: card charges are mostly Marketing/Ad spend and "
                "SMS blasts. Retain CRD 4314 / CRD 8594 statements to substantiate; do not "
                "also expense the underlying charges separately (double-count)")

    if "VIC3" in d or "VIC 3" in d:
        return ("F", "VIC3 loan-related cost (deductible — LOV3 guarantor)",
                "Copy of guaranty agreement on the VIC3 construction loan; BAED to confirm "
                "deduction vs. capitalization timing under the guarantor position")

    if "Capital Equipment" in category or "Construction Build Out" in category:
        return ("E", "Fixed assets / leasehold improvements",
                "Invoice per item; add to Form 4562 — Sec. 179/bonus likely recovers in full")

    if "Personal Meals" in category:
        docs = "Who attended + business purpose per charge"
        if "LOVE" in d or "LOV3" in d:
            docs += "; charged at LOV3 itself — circular, BAED to net against revenue"
        return ("B", "Business Meals (50%)", docs)

    if "Competitive Research" in category:
        if "POLEKATZ" in d:
            return ("C", "Eddie distribution", "No deductible path — Sec. 274(a) entertainment")
        if "CARIOCA" in d or "RIO" in d or "HOLLYWOO" in d:
            return ("C", "Eddie distribution",
                    "Out-of-market venue; nondeductible unless the trip itself is documented business")
        return ("B", "Business Meals (50%) — concept research",
                "Research memo per visit (menu/pricing/concept notes); meal portion only — "
                "any entertainment portion nondeductible Sec. 274(a)")

    if "Transportation" in category:
        if "DERWIN" in d:
            return ("C", "Eddie distribution (or Sec. 707(c) GP to Derwin)",
                    "Partner personal transport — BAED to classify")
        return ("A", "Artist/VIP ground transport (Marketing)",
                "Match payment date to event calendar + artist bookings; 1099-NEC to driver")

    if "Owner Travel" in category:
        if "GOL LINHA" in d:
            return ("C", "Eddie distribution", "Brazil trip — personal absent documented business purpose")
        return ("A", "Business travel (Sec. 162(a)(2))",
                "Trip purpose memo per flight (VIC3 scouting, artist booking, vendor)")

    if "Travel & Lodging" in category:
        if any(k in d for k in ("SAO PAULO", "RIO", "HIGUEY", "HARD ROCK")):
            return ("C", "Eddie distribution", "Brazil/DR — personal absent documented business purpose")
        return ("A", "Artist lodging (Marketing/Entertainment)",
                "Match hotel date to booked-artist event; booking contract naming lodging")

    if "Travel & Entertainment" in category:
        return ("A", "Business travel (Sec. 162(a)(2))",
                "Trip purpose memo; split any entertainment portion to nondeductible")

    # Owner Discretionary
    if "LOUIS VUITTON" in d or "STUBHUB" in d or "ROCKETS" in d or "TO MOM" in d:
        return ("C", "Eddie distribution",
                "No deductible path — personal goods / Sec. 274(a) entertainment tickets")
    if "FOR LOV3" in d or "FOR LOV" in d:
        return ("A", "Contract labor / event staff (1099)",
                "1099-NEC to payee; note role worked (event staff, dancer, promoter)")
    if any(k in d for k in ("BALLOO", "FREQUE", "ANTIEKS", "AMERICA'S STA")):
        return ("A", "Event decor / production supplies",
                "Receipt + event tied to purchase")
    if "ZELLE" in d:
        return ("A", "Contract labor / event staff (1099) — owner-confirmed LOV3 expense",
                "Owner-confirmed LOV3-related (2026-10-11); 1099-NEC to payee + note role worked")
    if "FIRESTONE" in d or "MOVING" in d:
        return ("A", "Business vehicle / equipment move — owner-confirmed LOV3 expense",
                "Owner-confirmed LOV3-related (2026-10-11); retain invoice")
    return ("A", "LOV3 business expense — owner-confirmed",
            "Owner-confirmed LOV3-related (2026-10-11); retain receipt + business purpose note")


def main() -> None:
    client = bigquery.Client(project="toast-analytics-444116")
    sql = """
        SELECT transaction_date, category, description, amount
        FROM toast_raw.BankTransactions_raw
        WHERE EXTRACT(YEAR FROM transaction_date) = 2025
          AND amount < 0
          AND category IN UNNEST(@cats)
        ORDER BY category, transaction_date
    """
    job = client.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("cats", "STRING", CATEGORIES)]
    ))
    rows = [(r["transaction_date"], r["category"], r["description"], float(r["amount"]))
            for r in job.result()]
    print(f"Pulled {len(rows)} transactions")

    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment
    except ImportError:
        print("openpyxl not installed. Run: pip install openpyxl", file=sys.stderr)
        sys.exit(1)

    classified = []
    for dt, cat, desc, amt in rows:
        bucket, proposed, docs = classify(cat, desc)
        classified.append((dt, cat, desc, amt, bucket, proposed, docs))

    wb = Workbook()

    FILLS = {
        "A": "D8E8D0", "B": "E8E0C8", "C": "F4B8B8", "D": "F4B8B8",
        "E": "C8D8E8", "F": "D8E8D0", "G": "E0E0E0",
    }

    # ---- Tab 1: Summary
    ws = wb.active
    ws.title = "Summary"
    ws.merge_cells("A1:E1")
    ws["A1"] = "LOV3 Restaurant & Lounge LLC — 2025 Expense Reclassification Schedule"
    ws["A1"].font = Font(bold=True, size=14)
    ws.merge_cells("A2:E2")
    ws["A2"] = (f"Companion to the 2025 Adjusted Balance Sheet v{PACKAGE_VERSION} | "
                f"{PACKAGE_VERSION_DATE} | Proposed classifications — BAED final review required. "
                "All cards are LOV3 business cards; nondeductible personal spend = "
                "Eddie Line 19A distribution per owner directive.")
    ws["A2"].font = Font(size=9.5, italic=True, color="555555")

    hdr = ["Bucket", "Treatment", "Gross ($)", "Deductible ($)", "Transactions"]
    for c, h in enumerate(hdr, 1):
        cell = ws.cell(row=4, column=c, value=h)
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill("solid", fgColor="1A1A1A")

    r = 5
    totals = {}
    for b in BUCKETS:
        sub = [x for x in classified if x[4] == b]
        gross = round(sum(x[3] for x in sub), 2)
        deductible = round(gross * BUCKETS[b][1], 2)
        totals[b] = (gross, deductible, len(sub))
        ws.cell(row=r, column=1, value=b).fill = PatternFill("solid", fgColor=FILLS[b])
        ws.cell(row=r, column=2, value=BUCKETS[b][0])
        ws.cell(row=r, column=3, value=abs(gross)).number_format = '"$"#,##0.00'
        ws.cell(row=r, column=4, value=abs(deductible)).number_format = '"$"#,##0.00'
        ws.cell(row=r, column=5, value=len(sub))
        r += 1

    gross_all = round(sum(t[0] for t in totals.values()), 2)
    deduct_all = round(sum(t[1] for t in totals.values()), 2)
    ws.cell(row=r, column=2, value="TOTAL").font = Font(bold=True)
    ws.cell(row=r, column=3, value=abs(gross_all)).number_format = '"$"#,##0.00'
    ws.cell(row=r, column=3).font = Font(bold=True)
    ws.cell(row=r, column=4, value=abs(deduct_all)).number_format = '"$"#,##0.00'
    ws.cell(row=r, column=4).font = Font(bold=True)

    r += 2
    notes = [
        "A items require the listed documentation AND 1099-NEC filings where payees are individuals.",
        "B items are 50% deductible business meals (Sec. 274(n)) with a who/why memo per charge.",
        "C/D items have no defensible deduction path — C flows to Eddie Jasper's K-1 Line 19A.",
        "E items are removed from expense but largely recovered via Sec. 179/bonus on Form 4562.",
        "F items are deductible per owner directive 2026-10-11: LOV3 is guarantor on the VIC3 "
        "construction loan. Provide BAED the guaranty agreement to confirm deduction vs. "
        "capitalization timing.",
        "G items are deductible per owner confirmation 2026-10-11: card charges are mostly "
        "Marketing & Ad spend and SMS blasts. Card statements substantiate; the underlying "
        "charges must not also be expensed separately (double-count).",
    ]
    for n in notes:
        ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=5)
        ws.cell(row=r, column=1, value=n).font = Font(size=9, italic=True)
        r += 1

    for col, w in zip("ABCDE", (8, 52, 14, 14, 13)):
        ws.column_dimensions[col].width = w

    # ---- Tab 2: Transaction detail
    ws2 = wb.create_sheet("Transactions")
    hdr2 = ["Date", "Current Category", "Description", "Amount ($)",
            "Bucket", "Proposed Classification", "Documentation Required"]
    for c, h in enumerate(hdr2, 1):
        cell = ws2.cell(row=1, column=c, value=h)
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill("solid", fgColor="1A1A1A")
        cell.alignment = Alignment(wrap_text=True, vertical="center")
    ws2.freeze_panes = "A2"

    for i, (dt, cat, desc, amt, bucket, proposed, docs) in enumerate(classified, start=2):
        ws2.cell(row=i, column=1, value=str(dt))
        ws2.cell(row=i, column=2, value=cat.split("/")[-1])
        ws2.cell(row=i, column=3, value=desc).alignment = Alignment(wrap_text=True)
        ws2.cell(row=i, column=4, value=abs(amt)).number_format = '"$"#,##0.00'
        bc = ws2.cell(row=i, column=5, value=bucket)
        bc.fill = PatternFill("solid", fgColor=FILLS[bucket])
        bc.alignment = Alignment(horizontal="center")
        ws2.cell(row=i, column=6, value=proposed).alignment = Alignment(wrap_text=True)
        ws2.cell(row=i, column=7, value=docs).alignment = Alignment(wrap_text=True)

    for col, w in zip("ABCDEFG", (11, 24, 48, 12, 8, 34, 44)):
        ws2.column_dimensions[col].width = w

    # ---- Tab 3: Cash-paid & off-bank expenses (owner directive 2026-10-11)
    # Figures derived 2026-10-11 from Toast PaymentDetails_raw / ItemSelectionDetails_raw /
    # CashEntries_raw and BankTransactions_raw (business-day adjusted, CY2025):
    #   cash collected 1,037,671.77 (sales+tax 873,785.50 + tips 163,886.27)
    #   cash back to bank 544,733 = counter credits 210,301 + cash-acct transfers 28,600
    #                               + internal transfers from CHK 0227/9121 305,831.51
    #   Toast-reported V/MC/D processing fees 87,758.40 (netted from Citizens settlements —
    #   never a bank debit, so absent from bank-based expense capture)
    CASH_BRIDGE = [
        ("Cash collected per Toast POS (sales + tax)", 873_785.50),
        ("Cash tips collected (staff pass-through, not LOV3 revenue/expense)", 163_886.27),
        ("Less: counter-credit cash deposits to BofA x5815", -210_301.00),
        ("Less: cash-account transfers to BofA (CHK 0227/9121)", -28_600.00),
        ("Less: internal transfers in from CHK 0227/9121", -305_831.51),
        ("Less: cash tips paid out to staff", -163_886.27),
        ("= Undeposited cash available for cash-paid expenses", 329_053.01),
    ]
    OFF_BANK_ENTRIES = [
        ("Cash-paid contract labor & entertainment (DJs, door staff, security, "
         "dancers — payees owner-identified)", 329_053.01,
         "Owner directive 2026-10-11: book to Entertainment / Contract Labor now; "
         "1099-NECs to be filed from owner payee roster. Split across DJ/door/"
         "security sub-categories per roster."),
        ("Card processing fees (Toast-reported V/MC/D, netted from settlements)", 87_758.40,
         "Toast PaymentDetails v_mc_d_fees, CY2025. Deductible merchant fees — "
         "not in bank debits because processors net them before settlement. "
         "Amex/Toast platform fees may add to this."),
    ]

    ws3 = wb.create_sheet("Cash-Paid & Off-Bank")
    ws3.merge_cells("A1:C1")
    ws3["A1"] = "Off-Bank Expense Additions — owner directive 2026-10-11"
    ws3["A1"].font = Font(bold=True, size=13)
    r3 = 3
    ws3.cell(row=r3, column=1, value="Cash bridge (CY2025, business-day adjusted)").font = Font(bold=True)
    r3 += 1
    for label, amt in CASH_BRIDGE:
        ws3.cell(row=r3, column=1, value=label)
        ws3.cell(row=r3, column=2, value=amt).number_format = '"$"#,##0.00'
        if label.startswith("="):
            ws3.cell(row=r3, column=1).font = Font(bold=True)
            ws3.cell(row=r3, column=2).font = Font(bold=True)
        r3 += 1

    r3 += 1
    ws3.cell(row=r3, column=1, value="Expense additions (book now; document per notes)").font = Font(bold=True)
    r3 += 1
    off_bank_total = 0.0
    for label, amt, note in OFF_BANK_ENTRIES:
        ws3.cell(row=r3, column=1, value=label).alignment = Alignment(wrap_text=True)
        ws3.cell(row=r3, column=2, value=amt).number_format = '"$"#,##0.00'
        ws3.cell(row=r3, column=3, value=note).alignment = Alignment(wrap_text=True)
        off_bank_total += amt
        r3 += 1
    ws3.cell(row=r3, column=1, value="TOTAL off-bank expense additions").font = Font(bold=True)
    ws3.cell(row=r3, column=2, value=off_bank_total).number_format = '"$"#,##0.00'
    ws3.cell(row=r3, column=2).font = Font(bold=True)

    r3 += 2
    for caveat in (
        "Cash-paid figure is derived (POS cash minus tips minus redeposits), not drawer-logged: "
        "Toast PAY_OUT entries total only $8,186 for 2025 — payouts occur after close-out. "
        "Year-end cash on hand in CHK 0227/9121 or the safe would reduce the expense figure.",
        "Owner position: payees are known; 1099-NECs to be filed subsequently. If unsubstantiated "
        "on exam, the IRS may reallocate these to nondeductible distributions.",
        "Do not double-count: the $305,831.51 of 0227/9121 transfers already fund payroll "
        "recorded through Choice Employer — only the UNDEPOSITED residual is added here.",
    ):
        ws3.merge_cells(start_row=r3, start_column=1, end_row=r3, end_column=3)
        ws3.cell(row=r3, column=1, value=caveat).font = Font(size=9, italic=True)
        r3 += 1

    for col, w in zip("ABC", (62, 16, 58)):
        ws3.column_dimensions[col].width = w

    wb.save(OUT_PATH)
    print(f"Wrote {OUT_PATH}")
    print("\nBucket totals:")
    for b, (gross, deductible, n) in totals.items():
        print(f"  {b} {BUCKETS[b][0]:<45} {abs(gross):>12,.2f}  "
              f"(deductible {abs(deductible):>12,.2f}, n={n})")
    print(f"\nOff-bank expense additions (Tab 3): {off_bank_total:,.2f}")


if __name__ == "__main__":
    main()
