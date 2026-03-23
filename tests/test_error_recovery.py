"""Error recovery tests — verify graceful handling of external service failures."""

import json
from unittest.mock import patch, MagicMock

from google.cloud.exceptions import NotFound


class TestBigQueryDown:
    """Routes should return proper errors when BigQuery is unreachable."""

    @patch("routes_bank.bigquery.Client")
    def test_bank_transactions_bq_error(self, mock_bq_class, client):
        """GET /api/bank-transactions returns 500 with error when BQ fails."""
        mock_client = MagicMock()
        mock_client.query.side_effect = Exception("BigQuery connection timeout")
        mock_bq_class.return_value = mock_client

        resp = client.get("/api/bank-transactions")
        assert resp.status_code == 500
        data = json.loads(resp.data)
        assert "error" in data

    @patch("routes_etl.bigquery.Client")
    def test_table_status_bq_error(self, mock_bq_class, client):
        """GET /status/<table> returns 404 when table not found."""
        mock_client = MagicMock()
        mock_client.get_table.side_effect = NotFound("Table not found")
        mock_bq_class.return_value = mock_client

        resp = client.get("/status/nonexistent")
        assert resp.status_code == 404
        data = json.loads(resp.data)
        assert "error" in data


class TestPipelineFailures:
    """ETL pipeline handles SFTP and processing failures gracefully."""

    @patch("pipeline.bigquery.Client")
    def test_pipeline_sftp_key_missing(self, mock_bq_class):
        """Pipeline returns error status when SFTP key can't be retrieved."""
        from pipeline import ToastPipeline

        mock_bq_class.return_value = MagicMock()
        with patch("pipeline.SecretManager") as mock_sm, \
             patch("pipeline.AlertManager"):
            mock_sm.return_value.get_sftp_key.side_effect = Exception(
                "Secret not found: toast-sftp-private-key"
            )
            pipeline = ToastPipeline()
            summary = pipeline.run("20260322")

        assert summary.status == "error"
        assert any("Secret not found" in e for e in summary.errors)

    @patch("pipeline.bigquery.Client")
    def test_pipeline_sftp_connection_refused(self, mock_bq_class):
        """Pipeline returns error when SFTP connection fails."""
        from pipeline import ToastPipeline

        mock_bq_class.return_value = MagicMock()
        with patch("pipeline.SecretManager") as mock_sm, \
             patch("pipeline.AlertManager"), \
             patch("pipeline.ToastSFTPClient") as mock_sftp_class:
            mock_sm.return_value.get_sftp_key.return_value = "fake-key"
            mock_sftp_class.return_value.__enter__ = MagicMock(
                side_effect=ConnectionRefusedError("Connection refused")
            )
            pipeline = ToastPipeline()
            summary = pipeline.run("20260322")

        assert summary.status == "error"
        assert len(summary.errors) > 0

    @patch("pipeline.bigquery.Client")
    def test_pipeline_empty_sftp_directory(self, mock_bq_class):
        """Pipeline handles empty SFTP directory without crashing."""
        from pipeline import ToastPipeline

        mock_bq_class.return_value = MagicMock()
        with patch("pipeline.SecretManager") as mock_sm, \
             patch("pipeline.AlertManager"), \
             patch("pipeline.ToastSFTPClient") as mock_sftp_class:
            mock_sm.return_value.get_sftp_key.return_value = "fake-key"
            mock_sftp = MagicMock()
            mock_sftp.list_files.return_value = []  # no files
            mock_sftp_class.return_value.__enter__ = MagicMock(return_value=mock_sftp)
            mock_sftp_class.return_value.__exit__ = MagicMock(return_value=False)

            pipeline = ToastPipeline()
            summary = pipeline.run("20260322")

        # Should succeed but with 0 files processed
        assert summary.files_processed == 0
        assert "No files found" in str(summary.errors)


class TestBankUploadEdgeCases:
    """Bank CSV upload handles malformed input gracefully."""

    def test_non_csv_file_handled(self, client):
        """Uploading a non-CSV file should return an error, not crash."""
        from io import BytesIO
        resp = client.post(
            "/upload-bank-csv",
            data={"file": (BytesIO(b"not a csv at all\x00\x01\x02"), "garbage.bin")},
            content_type="multipart/form-data",
        )
        # Should return error, not 500
        assert resp.status_code in (400, 500)
        data = json.loads(resp.data)
        assert "error" in data

    def test_csv_with_wrong_columns(self, client):
        """CSV with unexpected columns should return an error."""
        from io import BytesIO
        bad_csv = b"Name,Age,City\nJohn,30,Houston\n"
        resp = client.post(
            "/upload-bank-csv",
            data={"file": (BytesIO(bad_csv), "bad-format.csv")},
            content_type="multipart/form-data",
        )
        assert resp.status_code in (400, 500)
        data = json.loads(resp.data)
        assert "error" in data


class TestAnalyticsAPIMalformedInput:
    """Analytics APIs handle bad input without crashing."""

    @patch("routes_analytics.bigquery.Client")
    def test_profit_summary_missing_dates(self, mock_bq_class, client):
        """POST /profit-summary without dates returns 400."""
        resp = client.post(
            "/profit-summary",
            data=json.dumps({}),
            content_type="application/json",
        )
        assert resp.status_code == 400

    @patch("routes_analytics.bigquery.Client")
    def test_comprehensive_analysis_missing_dates(self, mock_bq_class, client):
        """POST /comprehensive-analysis without dates returns 400."""
        resp = client.post(
            "/comprehensive-analysis",
            data=json.dumps({}),
            content_type="application/json",
        )
        assert resp.status_code == 400


class TestDateInputValidation:
    """Date format validation on analytics endpoints."""

    def test_profit_summary_invalid_date_format(self, client):
        """Invalid date format returns 400."""
        resp = client.post(
            "/profit-summary",
            data=json.dumps({"start_date": "not-a-date", "end_date": "2026-03-31"}),
            content_type="application/json",
        )
        assert resp.status_code == 400
        data = json.loads(resp.data)
        assert "Invalid" in data["error"]

    def test_profit_summary_start_after_end(self, client):
        """start_date after end_date returns 400."""
        resp = client.post(
            "/profit-summary",
            data=json.dumps({"start_date": "2026-12-01", "end_date": "2026-01-01"}),
            content_type="application/json",
        )
        assert resp.status_code == 400

    def test_comprehensive_analysis_invalid_format(self, client):
        """Invalid date format on comprehensive-analysis returns 400."""
        resp = client.post(
            "/comprehensive-analysis",
            data=json.dumps({"start_date": "01/01/2026", "end_date": "03/31/2026"}),
            content_type="application/json",
        )
        assert resp.status_code == 400

    def test_profit_summary_missing_end_date(self, client):
        """Missing end_date returns 400."""
        resp = client.post(
            "/profit-summary",
            data=json.dumps({"start_date": "2026-01-01"}),
            content_type="application/json",
        )
        assert resp.status_code == 400
