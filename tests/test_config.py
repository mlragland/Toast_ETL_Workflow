"""Unit tests for config.py — verify constants load correctly."""

from config import PROJECT_ID, DATASET_ID, FILE_CONFIGS, BUSINESS_DAY_SQL, LOV3_EVENTS


def test_project_id_has_default():
    """PROJECT_ID should have a default even without env var."""
    assert PROJECT_ID is not None
    assert len(PROJECT_ID) > 0


def test_file_configs_has_all_tables():
    """FILE_CONFIGS must define schemas for all 7 Toast CSV types."""
    expected = [
        "OrderDetails.csv", "CheckDetails.csv", "PaymentDetails.csv",
        "ItemSelectionDetails.csv", "AllItemsReport.csv",
        "CashEntries.csv", "KitchenTimings.csv",
    ]
    for filename in expected:
        assert filename in FILE_CONFIGS, f"Missing FILE_CONFIGS entry for {filename}"
        assert "table" in FILE_CONFIGS[filename], f"{filename} missing 'table' key"
        assert "column_mapping" in FILE_CONFIGS[filename], f"{filename} missing 'column_mapping'"


def test_business_day_sql_format():
    """BUSINESS_DAY_SQL.format() should produce valid SQL."""
    result = BUSINESS_DAY_SQL.format(dt_col="CAST(paid_date AS DATETIME)")
    assert "DATE_SUB" in result
    assert "paid_date" in result
    assert "HOUR" in result


def test_lov3_events_not_empty():
    """LOV3_EVENTS should have entries for both 2025 and 2026."""
    assert len(LOV3_EVENTS) > 20
    years = {e["start_date"][:4] for e in LOV3_EVENTS}
    assert "2025" in years
    assert "2026" in years
