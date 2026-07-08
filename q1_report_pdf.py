"""Standalone Q1 2026 leadership report PDF generator.

NOT deployed to Cloud Run — run locally when you need a PDF for the SBA
lender package or board distribution.

Usage:
    pip install weasyprint
    python q1_report_pdf.py
    # writes LOV3_HTX_Q1_2026_Leadership_Report.pdf to current directory

Skips the file if weasyprint is unavailable rather than crashing.
"""

import sys
from google.cloud import bigquery
from q1_report import Q1ReportGenerator


OUTPUT_PATH = "LOV3_HTX_Q1_2026_Leadership_Report.pdf"


def main() -> int:
    try:
        from weasyprint import HTML
    except ImportError:
        print("WeasyPrint not installed. Run: pip install weasyprint", file=sys.stderr)
        return 1

    print("Fetching Q1 2026 report data from BigQuery...")
    client = bigquery.Client(project="toast-analytics-444116")
    gen = Q1ReportGenerator(client)
    data = gen.fetch()

    print("Rendering HTML...")
    html_str = gen.render_html(data)

    print(f"Writing PDF to {OUTPUT_PATH}...")
    HTML(string=html_str).write_pdf(OUTPUT_PATH)
    print(f"Done. Open with: open {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
