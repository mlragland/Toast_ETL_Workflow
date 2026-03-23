"""Unit tests for business logic in services.py — BofACSVParser and DataTransformer."""

import pytest
from services import BofACSVParser, DataTransformer


# ─── BofACSVParser._categorize() tests ──────────────────────────────────────

SAMPLE_RULES = [
    {"keyword": "SYSCO", "category": "COGS/Food", "vendor_normalized": "Sysco"},
    {"keyword": "ADP", "category": "Labor/Payroll", "vendor_normalized": "ADP"},
    {"keyword": "CENTERPOINT", "category": "Utilities", "vendor_normalized": "CenterPoint Energy"},
]


@pytest.fixture
def parser():
    """BofACSVParser with sample rules and no check register."""
    return BofACSVParser(category_rules=SAMPLE_RULES, check_register=None)


@pytest.fixture
def parser_with_register():
    """BofACSVParser with sample rules and a check register."""
    register = {
        "1234": {"payee": "Sysco Foods", "category": "COGS/Food", "vendor_normalized": "Sysco"},
        "5678": {"payee": "Unknown Vendor"},  # no category — falls through to keyword matching
    }
    return BofACSVParser(category_rules=SAMPLE_RULES, check_register=register)


class TestCategorizeToastPatterns:
    """Toast ACH transaction detection — must fire before keyword matching."""

    def test_toast_deposit(self, parser):
        cat, source, vendor = parser._categorize("TOAST DES:DEP 240115")
        assert cat == "1. Revenue/Sales Revenue"
        assert source == "auto"
        assert vendor == "Toast Deposit"

    def test_toast_eom(self, parser):
        cat, source, vendor = parser._categorize("TOAST DES:EOM")
        assert cat == "1. Revenue/Sales Revenue"
        assert vendor == "Toast EOM Adjustment"

    def test_toast_platform_fee(self, parser):
        cat, source, vendor = parser._categorize("Toast, Inc DES:Toast")
        assert cat == "5. Operating Expenses (OPEX)/POS & Technology Fees"
        assert vendor == "Toast Platform Fee"

    def test_toast_refund(self, parser):
        cat, source, vendor = parser._categorize("TOAST DES:REF")
        assert cat == "5. Operating Expenses (OPEX)/POS & Technology Fees"
        assert vendor == "Toast Refund"

    def test_toast_monthly_settlement(self, parser):
        cat, source, vendor = parser._categorize("TOAST, INC. DES:20250115")
        assert cat == "1. Revenue/Sales Revenue"
        assert vendor == "Toast Settlement"


class TestCategorizeKeywordMatching:
    """Standard keyword-based categorization."""

    def test_keyword_match_sysco(self, parser):
        cat, source, vendor = parser._categorize("SYSCO FOODS HOUSTON TX")
        assert cat == "COGS/Food"
        assert source == "auto"
        assert vendor == "Sysco"

    def test_keyword_match_adp(self, parser):
        cat, source, vendor = parser._categorize("ADP PAYROLL SERVICES")
        assert cat == "Labor/Payroll"
        assert vendor == "ADP"

    def test_no_match_returns_uncategorized(self, parser):
        cat, source, vendor = parser._categorize("RANDOM UNKNOWN MERCHANT")
        assert cat == "Uncategorized"
        assert source == "uncategorized"


class TestCategorizeCheckRegister:
    """Check register lookup — takes precedence over keyword matching."""

    def test_check_with_register_match(self, parser_with_register):
        cat, source, vendor = parser_with_register._categorize("Check 1234")
        assert cat == "COGS/Food"
        assert source == "check_register"
        assert vendor == "Sysco"

    def test_check_not_in_register(self, parser_with_register):
        cat, source, vendor = parser_with_register._categorize("Check 9999")
        assert cat == "Uncategorized"
        assert source == "uncategorized"

    def test_check_register_no_category_falls_to_keyword(self, parser_with_register):
        """Register has payee but no category — payee runs through keyword rules."""
        cat, source, vendor = parser_with_register._categorize("Check 5678")
        # "Unknown Vendor" doesn't match any keyword rule
        assert cat == "Uncategorized"
        assert source == "uncategorized"


class TestCategorizeParseCSV:
    """Test CSV parsing produces expected DataFrame shape."""

    def test_parse_minimal_csv(self, parser):
        csv_content = (
            b"Date,Description,Amount,Running Bal.\n"
            b"01/15/2025,SYSCO FOODS,-500.00,10000.00\n"
            b"01/16/2025,DEPOSIT,2000.00,12000.00\n"
        )
        df = parser.parse(csv_content, "test-stmt.csv")
        assert len(df) == 2
        assert "transaction_date" in df.columns
        assert "description" in df.columns
        assert "amount" in df.columns
        assert "category" in df.columns
        assert "category_source" in df.columns
        assert "vendor_normalized" in df.columns
        assert "source_file" in df.columns


# ─── DataTransformer tests ──────────────────────────────────────────────────

class TestDataTransformer:
    """DataTransformer static method tests."""

    def test_parse_toast_datetime_valid(self):
        result = DataTransformer.parse_toast_datetime("01/15/25 02:30 PM")
        assert result == "2025-01-15 14:30:00"

    def test_parse_toast_datetime_iso_format(self):
        result = DataTransformer.parse_toast_datetime("2025-01-15 14:30:00")
        assert result == "2025-01-15 14:30:00"

    def test_parse_toast_datetime_empty(self):
        result = DataTransformer.parse_toast_datetime("")
        assert result is None

    def test_parse_toast_datetime_unparseable(self):
        result = DataTransformer.parse_toast_datetime("not-a-date")
        assert result == "not-a-date"  # returns original string

    def test_parse_duration_valid(self):
        result = DataTransformer.parse_duration("0:45:00")
        assert result == "0:45:00"

    def test_parse_duration_empty(self):
        result = DataTransformer.parse_duration("")
        assert result is None
