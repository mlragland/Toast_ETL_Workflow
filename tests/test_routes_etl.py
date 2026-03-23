"""Integration tests for ETL routes — health, run, backfill, status."""

import json
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch, MagicMock


class TestHealthEndpoint:
    """GET / health check."""

    def test_returns_healthy(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert data["status"] == "healthy"
        assert data["service"] == "toast-etl-pipeline"


class TestRunEndpoint:
    """POST /run — pipeline trigger."""

    @patch("routes_etl.ToastPipeline")
    def test_successful_run(self, mock_cls, client):
        """Successful pipeline run returns 200 with summary."""
        mock_summary = MagicMock()
        mock_summary.run_id = "test-run-1"
        mock_summary.status = "success"
        mock_summary.processing_date = "20260322"
        mock_summary.files_processed = 7
        mock_summary.files_failed = 0
        mock_summary.total_rows = 1000
        mock_summary.start_time = datetime(2026, 3, 22, 6, 0, 0)
        mock_summary.end_time = datetime(2026, 3, 22, 6, 5, 0)
        mock_summary.errors = []
        mock_cls.return_value.run.return_value = mock_summary

        resp = client.post(
            "/run",
            data=json.dumps({"processing_date": "20260322"}),
            content_type="application/json",
        )
        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert data["status"] == "success"
        assert data["files_processed"] == 7


class TestBackfillEndpoint:
    """POST /backfill — historical data backfill."""

    def test_missing_dates_returns_400(self, client):
        """Backfill without required dates returns 400."""
        resp = client.post(
            "/backfill",
            data=json.dumps({}),
            content_type="application/json",
        )
        assert resp.status_code == 400

    def test_start_after_end_returns_400(self, client):
        """start_date after end_date returns 400."""
        resp = client.post(
            "/backfill",
            data=json.dumps({"start_date": "20260301", "end_date": "20260101"}),
            content_type="application/json",
        )
        assert resp.status_code == 400


class TestStatusEndpoint:
    """GET /status/<table> — table status."""

    @patch("routes_etl.bigquery.Client")
    def test_valid_table_returns_info(self, mock_bq_class, client):
        """Known table returns row count and metadata."""
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_table.num_rows = 5000
        mock_table.num_bytes = 1024 * 1024
        mock_table.modified = datetime(2026, 3, 22, 10, 0, 0)
        mock_client.get_table.return_value = mock_table

        # Mock the query result for latest_date and total_rows
        mock_query_result = MagicMock()
        mock_query_result.result.return_value = [
            SimpleNamespace(latest_date="2026-03-22", total_rows=5000)
        ]
        mock_client.query.return_value = mock_query_result
        mock_bq_class.return_value = mock_client

        resp = client.get("/status/order_details")
        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert data["total_rows"] == 5000

    @patch("routes_etl.bigquery.Client")
    def test_unknown_table_returns_404(self, mock_bq_class, client):
        """Unknown table returns 404."""
        from google.cloud.exceptions import NotFound
        mock_client = MagicMock()
        mock_client.get_table.side_effect = NotFound("Table not found")
        mock_bq_class.return_value = mock_client

        resp = client.get("/status/nonexistent_table")
        assert resp.status_code == 404
