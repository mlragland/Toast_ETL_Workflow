"""Unit tests for dataclasses in models.py."""

from models import PipelineResult, PipelineRunSummary, BankUploadResult


def test_pipeline_result_defaults():
    """PipelineResult creates with correct defaults."""
    result = PipelineResult(filename="test.csv", status="success")
    assert result.filename == "test.csv"
    assert result.rows_processed == 0
    assert result.schema_changes == []
    assert result.error_message == ""


def test_bank_upload_result_defaults():
    """BankUploadResult creates with correct defaults."""
    result = BankUploadResult(batch_id="b1", filename="stmt.csv", status="success")
    assert result.total_debits == 0.0
    assert result.total_credits == 0.0
    assert result.transactions_by_category == {}
    assert result.date_range == ""
