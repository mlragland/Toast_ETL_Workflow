"""ETL routes: health check, pipeline run, backfill, table status, weekly report."""

import logging
from datetime import datetime

from flask import Blueprint, request, jsonify
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from config import PROJECT_ID, DATASET_ID
from pipeline import ToastPipeline
from weekly_report import WeeklyReportGenerator

logger = logging.getLogger(__name__)

bp = Blueprint("etl", __name__)


@bp.route("/", methods=["GET"])
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "service": "toast-etl-pipeline"})


@bp.route("/run", methods=["POST"])
def run_pipeline():
    """
    Trigger pipeline run

    Request body:
    {
        "processing_date": "20250129",  // optional, defaults to yesterday
        "backfill_days": 0  // optional, number of days to backfill
    }
    """
    data = request.get_json(silent=True) or {}

    processing_date = data.get("processing_date")
    backfill_days = data.get("backfill_days", 0)

    pipeline = ToastPipeline()
    summary = pipeline.run(processing_date, backfill_days)

    return jsonify({
        "run_id": summary.run_id,
        "status": summary.status,
        "processing_date": summary.processing_date,
        "files_processed": summary.files_processed,
        "files_failed": summary.files_failed,
        "total_rows": summary.total_rows,
        "duration_seconds": (summary.end_time - summary.start_time).total_seconds(),
        "errors": summary.errors
    })


@bp.route("/backfill", methods=["POST"])
def backfill():
    """
    Backfill historical data

    Request body:
    {
        "start_date": "20250101",
        "end_date": "20250129"
    }
    """
    data = request.get_json()

    if not data or "start_date" not in data or "end_date" not in data:
        return jsonify({"error": "start_date and end_date required"}), 400

    start_date = datetime.strptime(data["start_date"], "%Y%m%d")
    end_date = datetime.strptime(data["end_date"], "%Y%m%d")

    if start_date > end_date:
        return jsonify({"error": "start_date must be before end_date"}), 400

    days = (end_date - start_date).days

    pipeline = ToastPipeline()
    summary = pipeline.run(data["end_date"], backfill_days=days)

    return jsonify({
        "run_id": summary.run_id,
        "status": summary.status,
        "date_range": f"{data['start_date']} to {data['end_date']}",
        "files_processed": summary.files_processed,
        "total_rows": summary.total_rows,
        "errors": summary.errors
    })


@bp.route("/status/<table_loc>", methods=["GET"])
def table_status(table_loc: str):
    """Get status of a specific table"""
    try:
        client = bigquery.Client(project=PROJECT_ID)
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_loc}"
        table = client.get_table(table_ref)

        # Get latest processing date
        query = f"""
        SELECT MAX(processing_date) as latest_date, COUNT(*) as total_rows
        FROM `{table_ref}`
        """
        result = list(client.query(query).result())[0]

        return jsonify({
            "table": table_loc,
            "total_rows": table.num_rows,
            "size_mb": table.num_bytes / (1024 * 1024),
            "latest_processing_date": str(result.latest_date) if result.latest_date else None,
            "modified": table.modified.isoformat()
        })

    except NotFound:
        return jsonify({"error": f"Table {table_loc} not found"}), 404


@bp.route("/weekly-report", methods=["POST"])
def weekly_report():
    """
    Generate and send weekly summary report

    Request body (all optional):
    {
        "week_ending": "20250126",  // Sunday date (YYYYMMDD), defaults to last Sunday
        "to_email": "custom@email.com"  // Override recipient email
    }
    """
    data = request.get_json(silent=True) or {}

    week_ending = data.get("week_ending")
    to_email = data.get("to_email")

    try:
        generator = WeeklyReportGenerator()
        result = generator.generate_and_send_report(week_ending, to_email)

        return jsonify({
            "status": "success" if result["success"] else "failed",
            "week_start": result["week_start"],
            "week_end": result["week_end"],
            "recipient": result["recipient"],
            "summary": result["summary"]
        })

    except Exception as e:
        logger.error(f"Weekly report failed: {e}")
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500
