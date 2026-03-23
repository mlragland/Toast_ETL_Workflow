"""Data validation tests — verify ETL data quality, schemas, and pipeline integrity."""

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
from types import SimpleNamespace

from config import FILE_CONFIGS
from services import DataTransformer, BigQueryLoader


# ─── FILE_CONFIGS schema validation ────────────────────────────────────────

class TestFileConfigSchemas:
    """Validate that FILE_CONFIGS entries are well-formed."""

    EXPECTED_FILES = [
        "OrderDetails.csv", "CheckDetails.csv", "PaymentDetails.csv",
        "ItemSelectionDetails.csv", "AllItemsReport.csv",
        "CashEntries.csv", "KitchenTimings.csv",
    ]

    def test_all_files_have_table(self):
        """Every file config must define a BigQuery table name."""
        for fname in self.EXPECTED_FILES:
            assert "table" in FILE_CONFIGS[fname], f"{fname} missing 'table'"
            assert FILE_CONFIGS[fname]["table"].endswith("_raw"), \
                f"{fname} table should end with '_raw', got {FILE_CONFIGS[fname]['table']}"

    def test_all_files_have_primary_key(self):
        """Every file config must define a primary key for deduplication."""
        for fname in self.EXPECTED_FILES:
            pk = FILE_CONFIGS[fname].get("primary_key", [])
            assert len(pk) > 0, f"{fname} missing primary_key"
            assert "processing_date" in pk, \
                f"{fname} primary_key should include processing_date"

    def test_all_files_have_column_mapping(self):
        """Every file config must define column mappings."""
        for fname in self.EXPECTED_FILES:
            mapping = FILE_CONFIGS[fname].get("column_mapping", {})
            assert len(mapping) > 0, f"{fname} has empty column_mapping"

    def test_column_mappings_are_snake_case(self):
        """All mapped column names should be snake_case for BigQuery."""
        for fname in self.EXPECTED_FILES:
            mapping = FILE_CONFIGS[fname]["column_mapping"]
            for original, mapped in mapping.items():
                assert mapped == mapped.lower(), \
                    f"{fname}: mapped column '{mapped}' is not lowercase"
                assert " " not in mapped, \
                    f"{fname}: mapped column '{mapped}' contains spaces"


# ─── DataTransformer output validation ──────────────────────────────────────

class TestTransformOutputQuality:
    """Validate that transformed DataFrames meet BigQuery load requirements."""

    def _make_order_df(self):
        """Create a minimal OrderDetails DataFrame matching Toast CSV format."""
        return pd.DataFrame({
            "Location": ["LOV3"],
            "Order Id": ["ORD-001"],
            "Order #": [12345],
            "Opened": ["01/15/25 06:00 PM"],
            "Server": ["John"],
            "Total": [125.50],
            "Tip": [25.00],
            "Gratuity": [20.00],
            "# of Guests": [4],
            "Revenue Center": ["Bar"],
            "Service": ["Dine In"],
        })

    def test_transform_adds_processing_date(self):
        """Transformed DataFrame must include processing_date column."""
        df = self._make_order_df()
        transformer = DataTransformer()
        config = FILE_CONFIGS["OrderDetails.csv"]
        result = transformer.transform_dataframe(df, config, "2025-01-15")
        assert "processing_date" in result.columns
        assert str(result["processing_date"].iloc[0]) == "2025-01-15"

    def test_transform_applies_column_mapping(self):
        """Column names should be mapped from Toast format to snake_case."""
        df = self._make_order_df()
        transformer = DataTransformer()
        config = FILE_CONFIGS["OrderDetails.csv"]
        result = transformer.transform_dataframe(df, config, "2025-01-15")
        # Original "Order Id" should become "order_id"
        assert "order_id" in result.columns
        assert "Order Id" not in result.columns

    def test_transform_preserves_row_count(self):
        """Transformation should not add or remove rows."""
        df = self._make_order_df()
        transformer = DataTransformer()
        config = FILE_CONFIGS["OrderDetails.csv"]
        result = transformer.transform_dataframe(df, config, "2025-01-15")
        assert len(result) == len(df)

    def test_transform_handles_empty_dataframe(self):
        """Empty DataFrame should transform without error."""
        df = pd.DataFrame()
        transformer = DataTransformer()
        config = FILE_CONFIGS["OrderDetails.csv"]
        result = transformer.transform_dataframe(df, config, "2025-01-15")
        assert len(result) == 0


# ─── Pipeline process_file validation ──────────────────────────────────────

class TestPipelineDataIntegrity:
    """Validate data flows correctly through the pipeline."""

    @patch("pipeline.bigquery.Client")
    def test_process_file_unknown_file_skipped(self, mock_bq_class):
        """Files not in FILE_CONFIGS should be skipped, not errored."""
        from pipeline import ToastPipeline

        mock_bq_class.return_value = MagicMock()
        with patch("pipeline.SecretManager"), \
             patch("pipeline.AlertManager"):
            pipeline = ToastPipeline()

        mock_sftp = MagicMock()
        result = pipeline.process_file(mock_sftp, "20260322", "UnknownFile.csv")
        assert result.status == "skipped"
        assert "No configuration" in result.error_message

    @patch("pipeline.bigquery.Client")
    def test_process_file_empty_csv_skipped(self, mock_bq_class):
        """Empty CSV files should be skipped."""
        from pipeline import ToastPipeline

        mock_bq_class.return_value = MagicMock()
        with patch("pipeline.SecretManager"), \
             patch("pipeline.AlertManager"):
            pipeline = ToastPipeline()

        mock_sftp = MagicMock()
        # Return a CSV with headers only (no data rows)
        mock_sftp.download_file.return_value = b"Location,Order Id,Order #\n"

        result = pipeline.process_file(mock_sftp, "20260322", "OrderDetails.csv")
        assert result.status == "skipped"
        assert result.rows_processed == 0


# ─── Bank CSV data validation ──────────────────────────────────────────────

class TestBankCSVDataQuality:
    """Validate bank CSV parsing produces clean data."""

    def test_parsed_amounts_are_numeric(self):
        """All amounts should be valid floats after parsing."""
        from services import BofACSVParser
        parser = BofACSVParser(category_rules=[], check_register=None)
        csv_content = (
            b"Date,Description,Amount,Running Bal.\n"
            b"01/15/2025,VENDOR A,-500.00,10000.00\n"
            b"01/16/2025,DEPOSIT,2000.00,12000.00\n"
            b"01/17/2025,VENDOR B,-123.45,11876.55\n"
        )
        df = parser.parse(csv_content, "test.csv")
        assert df["amount"].dtype in ["float64", "int64"]
        assert df["amount"].isna().sum() == 0

    def test_parsed_dates_are_valid(self):
        """All transaction dates should be valid dates."""
        from services import BofACSVParser
        parser = BofACSVParser(category_rules=[], check_register=None)
        csv_content = (
            b"Date,Description,Amount,Running Bal.\n"
            b"01/15/2025,VENDOR,-500.00,10000.00\n"
        )
        df = parser.parse(csv_content, "test.csv")
        assert df["transaction_date"].iloc[0] is not None
        # Should be parseable as a date
        date_str = str(df["transaction_date"].iloc[0])
        assert len(date_str) >= 8  # at minimum YYYY-MM-DD

    def test_every_row_has_category(self):
        """Every parsed row must have a category (even if Uncategorized)."""
        from services import BofACSVParser
        parser = BofACSVParser(category_rules=[], check_register=None)
        csv_content = (
            b"Date,Description,Amount,Running Bal.\n"
            b"01/15/2025,RANDOM VENDOR,-50.00,9950.00\n"
            b"01/16/2025,ANOTHER THING,-25.00,9925.00\n"
        )
        df = parser.parse(csv_content, "test.csv")
        assert df["category"].isna().sum() == 0
        assert df["category_source"].isna().sum() == 0

    def test_source_file_tracked(self):
        """Every row should track which source file it came from."""
        from services import BofACSVParser
        parser = BofACSVParser(category_rules=[], check_register=None)
        csv_content = (
            b"Date,Description,Amount,Running Bal.\n"
            b"01/15/2025,VENDOR,-100.00,9900.00\n"
        )
        df = parser.parse(csv_content, "stmt-19.csv")
        assert df["source_file"].iloc[0] == "stmt-19.csv"
