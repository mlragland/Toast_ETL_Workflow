"""Integration tests for bank routes — mocked BigQuery."""

import json
from unittest.mock import MagicMock, patch
from types import SimpleNamespace


def _mock_bq_client():
    """Create a mock BigQuery client that returns empty results by default."""
    mock_client = MagicMock()

    # Default: queries return empty result sets
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = []
    mock_client.query.return_value = mock_query_job

    return mock_client


def _make_summary_row():
    """Summary row for bank-transactions API."""
    return SimpleNamespace(
        uncategorized_count=5,
        uncategorized_total=1000.0,
        categorized_count=100,
        total_count=105,
    )


def _make_meta_row():
    """Metadata row for bank-transactions API."""
    return SimpleNamespace(
        last_upload_date="2026-03-03",
        newest_transaction_date="2026-03-02",
        oldest_transaction_date="2024-01-02",
    )


def _make_file_row():
    """Last upload file row."""
    return SimpleNamespace(source_file="stmt-18.csv", row_count=40)


def _make_txn_row():
    """Single transaction row."""
    return SimpleNamespace(
        transaction_date="2026-03-01",
        description="SYSCO FOODS",
        amount=-500.0,
        transaction_type="debit",
        category="COGS/Food",
        category_source="auto",
        vendor_normalized="Sysco",
    )


class TestBankTransactionsAPI:
    """GET /api/bank-transactions with mocked BigQuery."""

    @patch("routes_bank.bigquery.Client")
    def test_returns_json_with_expected_shape(self, mock_bq_class, client):
        """API returns JSON with summary, transactions, categories keys."""
        mock_client = _mock_bq_client()

        # Set up sequential query responses
        summary_job = MagicMock()
        summary_job.result.return_value = [_make_summary_row()]

        meta_job = MagicMock()
        meta_job.result.return_value = [_make_meta_row()]

        file_job = MagicMock()
        file_job.result.return_value = [_make_file_row()]

        cat_job = MagicMock()
        cat_job.result.return_value = [SimpleNamespace(category="COGS/Food")]

        count_job = MagicMock()
        count_job.result.return_value = [SimpleNamespace(cnt=1)]

        rows_job = MagicMock()
        rows_job.result.return_value = [_make_txn_row()]

        mock_client.query.side_effect = [
            summary_job, meta_job, file_job, cat_job, count_job, rows_job
        ]
        mock_bq_class.return_value = mock_client

        resp = client.get("/api/bank-transactions")
        assert resp.status_code == 200

        data = json.loads(resp.data)
        assert "summary" in data
        assert "transactions" in data
        assert "categories" in data
        assert data["summary"]["total_count"] == 105
        assert len(data["transactions"]) == 1
        assert data["transactions"][0]["description"] == "SYSCO FOODS"

    @patch("routes_bank.bigquery.Client")
    def test_filters_by_status(self, mock_bq_class, client):
        """Status param filters uncategorized transactions."""
        mock_client = _mock_bq_client()

        summary_job = MagicMock()
        summary_job.result.return_value = [_make_summary_row()]
        meta_job = MagicMock()
        meta_job.result.return_value = [_make_meta_row()]
        file_job = MagicMock()
        file_job.result.return_value = [_make_file_row()]
        cat_job = MagicMock()
        cat_job.result.return_value = []
        count_job = MagicMock()
        count_job.result.return_value = [SimpleNamespace(cnt=0)]
        rows_job = MagicMock()
        rows_job.result.return_value = []

        mock_client.query.side_effect = [
            summary_job, meta_job, file_job, cat_job, count_job, rows_job
        ]
        mock_bq_class.return_value = mock_client

        resp = client.get("/api/bank-transactions?status=uncategorized")
        assert resp.status_code == 200


class TestBankUploadCSV:
    """POST /upload-bank-csv error handling."""

    def test_no_file_returns_400(self, client):
        """Missing file in multipart form returns 400."""
        resp = client.post("/upload-bank-csv")
        assert resp.status_code == 400
        data = json.loads(resp.data)
        assert "error" in data

    def test_empty_filename_returns_400(self, client):
        """Empty filename returns 400."""
        from io import BytesIO
        resp = client.post(
            "/upload-bank-csv",
            data={"file": (BytesIO(b""), "")},
            content_type="multipart/form-data",
        )
        assert resp.status_code == 400


class TestBankCategorizeBulk:
    """POST /api/bank-transactions/categorize error handling."""

    def test_missing_updates_returns_400(self, client):
        """No updates array in body returns 400."""
        resp = client.post(
            "/api/bank-transactions/categorize",
            data=json.dumps({}),
            content_type="application/json",
        )
        assert resp.status_code == 400

    def test_empty_updates_returns_400(self, client):
        """Empty updates array returns 400."""
        resp = client.post(
            "/api/bank-transactions/categorize",
            data=json.dumps({"updates": []}),
            content_type="application/json",
        )
        assert resp.status_code == 400


class TestBankDeleteTransactions:
    """POST /api/bank-transactions/delete error handling."""

    def test_missing_deletes_returns_400(self, client):
        """No deletes array in body returns 400."""
        resp = client.post(
            "/api/bank-transactions/delete",
            data=json.dumps({}),
            content_type="application/json",
        )
        assert resp.status_code == 400
