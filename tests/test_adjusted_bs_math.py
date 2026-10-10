# tests/test_adjusted_bs_math.py
from adjusted_balance_sheet_2025 import (
    period_liabilities, taxes_payable_at_yearend, penalties_and_interest,
    implied_receipts, MB_GR, MB_SALES, SALES_USE,
)


def test_period_liability_amended_supersedes_struck_original():
    liabs = period_liabilities(MB_GR)
    assert liabs["2505"] == 2564.18          # struck 21,389.68 ignored


def test_period_liability_additive_when_original_not_struck():
    liabs = period_liabilities(MB_GR)
    assert liabs["2512"] == round(701.02 + 3587.85, 2)  # both paid -> additive


def test_mb_sales_2505_uses_amended():
    liabs = period_liabilities(MB_SALES)
    assert liabs["2505"] == 3007.04


def test_taxes_payable_at_yearend_is_2511_plus_2512():
    tp = taxes_payable_at_yearend(MB_GR)
    assert tp == round(4165.52 + 701.02 + 3587.85, 2)


def test_penalties_and_interest_positive_delta():
    pi = penalties_and_interest(MB_SALES)
    assert pi["2501"] == round(5671.79 - 5354.09, 2)   # 317.70


def test_implied_receipts_divides_by_rate():
    liabs = {"2501": 8250.0}
    assert implied_receipts(liabs, 0.0825)["2501"] == 100000.0


def test_sales_use_excludes_2024_q4_from_2025():
    liabs = period_liabilities(SALES_USE, year=2025)
    assert "244" not in liabs and "251" in liabs
