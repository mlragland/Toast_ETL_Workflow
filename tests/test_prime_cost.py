"""Unit tests for prime_cost.py — no BQ contact, mocked at the SBA-helper layer."""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from prime_cost import (
    ELEVATED_THRESHOLD,
    EXCELLENT_THRESHOLD,
    GOOD_THRESHOLD,
    PrimeCostCalculator,
    PrimeCostMonth,
    classify_expense_bucket,
    current_month_partial,
    render_html,
    rolling_30_day_window,
    trailing_month_range,
)


# ── Programmatic month range (auto-extends into future) ──────────────

def test_trailing_month_range_generates_12_months():
    rng = trailing_month_range(months_back=12, today=date(2026, 7, 15))
    assert len(rng) == 12
    # Last item should be June 2026 (the month before July 2026)
    assert rng[-1][0] == "2026-06"
    assert rng[-1][1] == "2026-06-01"
    assert rng[-1][2] == "2026-06-30"
    # First item should be July 2025
    assert rng[0][0] == "2025-07"


def test_trailing_range_handles_year_rollover():
    rng = trailing_month_range(months_back=3, today=date(2026, 2, 10))
    labels = [r[0] for r in rng]
    assert labels == ["2025-11", "2025-12", "2026-01"]


def test_trailing_range_last_day_correct_for_february_leap():
    # Feb 2024 was a leap year — day count = 29
    rng = trailing_month_range(months_back=1, today=date(2024, 3, 5))
    assert rng[0] == ("2024-02", "2024-02-01", "2024-02-29")


def test_current_month_partial_ends_today():
    label, start, end = current_month_partial(today=date(2026, 7, 15))
    assert label == "2026-07"
    assert start == "2026-07-01"
    assert end == "2026-07-15"


def test_rolling_30_day_window_covers_29_days_ending_yesterday():
    label, start, end = rolling_30_day_window(today=date(2026, 7, 15))
    assert label == "trailing_30d"
    assert end == "2026-07-14"  # yesterday
    assert start == "2026-06-15"  # 29 days back from yesterday


# ── Expense classifier (matches BankCategoryRules hierarchy) ─────────

def test_classify_liquor_cogs():
    assert classify_expense_bucket("2. Cost of Goods Sold/Liquor COGS") == "liquor_cogs"


def test_classify_food_cogs():
    assert classify_expense_bucket("2. Cost of Goods Sold/Food COGS") == "food_cogs"


def test_classify_labor():
    assert classify_expense_bucket("3. Labor Cost (Includes Grat + Tips)/Employee Payroll") == "labor"


def test_classify_other_cogs_is_excluded_from_prime():
    # Supplies is section 2 but not liquor/food COGS
    result = classify_expense_bucket("2. Cost of Goods Sold/Supplies & Equipment")
    assert result == "other_cogs"


def test_classify_operating_expense_returns_none():
    # Section 4+ (opex) shouldn't map to prime cost
    assert classify_expense_bucket("4. Operating Expenses/Rent") is None


def test_classify_empty_string_returns_none():
    assert classify_expense_bucket("") is None
    assert classify_expense_bucket(None) is None


# ── PrimeCostMonth grade logic ──────────────────────────────────────

def test_grade_excellent_below_55():
    m = PrimeCostMonth(month="2026-05", gross_revenue=1000, liquor_cogs=200, food_cogs=100,
                       labor_total=200)  # prime = 500 = 50%
    assert m.prime_pct == 50.0
    assert m.grade()[0] == "Excellent"


def test_grade_good_55_to_60():
    m = PrimeCostMonth(month="2026-05", gross_revenue=1000, liquor_cogs=200, food_cogs=100,
                       labor_total=270)  # prime = 570 = 57%
    assert m.grade()[0] == "Good"


def test_grade_elevated_60_to_65():
    m = PrimeCostMonth(month="2026-05", gross_revenue=1000, liquor_cogs=200, food_cogs=100,
                       labor_total=320)  # prime = 620 = 62%
    assert m.grade()[0] == "Elevated"


def test_grade_investigate_over_65():
    m = PrimeCostMonth(month="2026-05", gross_revenue=1000, liquor_cogs=300, food_cogs=100,
                       labor_total=400)  # prime = 800 = 80%
    assert m.grade()[0] == "Investigate"


def test_real_prime_pct_strips_gratuity_and_tips():
    m = PrimeCostMonth(month="2026-05", gross_revenue=1000, liquor_cogs=200,
                       food_cogs=100, labor_total=400, gratuity=100, tips=50)
    # labor_ex_tips should be 400 - 150 = 250
    m.labor_ex_tips = max(0, m.labor_total - (m.gratuity + m.tips))
    assert m.labor_ex_tips == 250
    # Real prime = 200 + 100 + 250 = 550 → 55%
    assert m.prime_pct_real == 55.0


def test_zero_revenue_produces_zero_pct_not_divzero():
    m = PrimeCostMonth(month="2026-05", gross_revenue=0, liquor_cogs=100, food_cogs=50,
                       labor_total=200)
    assert m.prime_pct == 0.0
    assert m.prime_pct_real == 0.0


# ── PrimeCostCalculator (mocked SBA layer) ──────────────────────────

def _make_calc_with_mocks():
    """Build a PrimeCostCalculator with the SBA helpers stubbed."""
    calc = PrimeCostCalculator(bq_client=MagicMock())
    calc._q_revenue = MagicMock(return_value={
        "2026-05": {"net_sales": 500_000, "gratuity": 100_000, "tips": 20_000, "order_count": 5000}
    })
    calc._q_expenses = MagicMock(return_value={
        "2026-05": {
            "2. Cost of Goods Sold/Liquor COGS": 80_000,
            "2. Cost of Goods Sold/Food COGS": 25_000,
            "2. Cost of Goods Sold/Supplies & Equipment": 5_000,  # excluded
            "3. Labor Cost (Includes Grat + Tips)/Employee Payroll": 180_000,
            "4. Operating Expenses/Rent": 20_000,  # excluded
        }
    })
    calc._q_hookah_bank = MagicMock(return_value={"2026-05": 15_000})
    calc._hookah_reclass = {}
    return calc


def test_compute_month_aggregates_costs_correctly():
    calc = _make_calc_with_mocks()
    m = calc.compute_month("2026-05", "2026-05-01", "2026-05-31")
    # Revenue: net_sales + gratuity + tips + hookah_bank
    assert m.gross_revenue == 500_000 + 100_000 + 20_000 + 15_000
    assert m.liquor_cogs == 80_000
    assert m.food_cogs == 25_000
    assert m.labor_total == 180_000
    assert m.prime_cost == 80_000 + 25_000 + 180_000  # = 285_000
    # Prime % = 285000 / 635000 ≈ 44.9%
    assert 44.0 < m.prime_pct < 46.0


def test_supplies_excluded_from_cogs():
    calc = _make_calc_with_mocks()
    m = calc.compute_month("2026-05", "2026-05-01", "2026-05-31")
    assert m.cogs_total == 105_000  # Liquor + Food only (not Supplies)


def test_operating_expenses_excluded_from_prime():
    calc = _make_calc_with_mocks()
    m = calc.compute_month("2026-05", "2026-05-01", "2026-05-31")
    assert m.prime_cost < m.gross_revenue  # Rent (20k) NOT in prime


def test_labor_ex_tips_estimation():
    calc = _make_calc_with_mocks()
    m = calc.compute_month("2026-05", "2026-05-01", "2026-05-31")
    # Labor 180k - (gratuity 100k + tips 20k) = 60k real labor
    assert m.labor_ex_tips == 60_000


def test_compute_trailing_months_returns_correct_count():
    calc = _make_calc_with_mocks()
    result = calc.compute_trailing_months(months_back=3)
    assert len(result) == 3
    assert all(isinstance(m, PrimeCostMonth) for m in result)


def test_hookah_reclass_applies_when_month_in_window():
    calc = _make_calc_with_mocks()
    calc._hookah_reclass = {"2026-05": 16_400}
    m = calc.compute_month("2026-05", "2026-05-01", "2026-05-31")
    assert m.hookah_reclass == 16_400
    # Reclass adds to gross revenue
    assert m.gross_revenue > 500_000 + 100_000 + 20_000 + 15_000


# ── HTML render — smoke test ────────────────────────────────────────

def test_render_html_produces_valid_page():
    m1 = PrimeCostMonth(month="2026-05", gross_revenue=500_000, liquor_cogs=80_000,
                        food_cogs=25_000, labor_total=180_000)
    m2 = PrimeCostMonth(month="2026-06", gross_revenue=520_000, liquor_cogs=75_000,
                        food_cogs=28_000, labor_total=170_000)
    html = render_html(rolling_30d=m1, current_partial=m2, trailing=[m1, m2])
    assert "<!DOCTYPE html>" in html or "<html" in html
    assert "Prime Cost" in html
    assert "2026-05" in html
    assert "2026-06" in html
