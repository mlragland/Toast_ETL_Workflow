"""Unit tests for q1_report utilities and section assemblers."""

import pytest

from q1_report import compute_pct_change, safe_div


def test_compute_pct_change_positive_growth():
    assert compute_pct_change(120, 100) == 20.0


def test_compute_pct_change_decline():
    assert compute_pct_change(80, 100) == -20.0


def test_compute_pct_change_zero_prior_returns_none():
    assert compute_pct_change(100, 0) is None


def test_compute_pct_change_no_change():
    assert compute_pct_change(100, 100) == 0.0


def test_compute_pct_change_negative_to_positive():
    assert compute_pct_change(50, -50) == -200.0


def test_safe_div_normal():
    assert safe_div(10, 2) == 5.0


def test_safe_div_zero_denominator():
    assert safe_div(10, 0) == 0.0


def test_safe_div_zero_numerator():
    assert safe_div(0, 10) == 0.0


from unittest.mock import MagicMock, patch
from q1_report import Q1ReportGenerator, PeriodMetrics


def _make_mock_client_for_revenue():
    """Build a mock BQ client where each query() call returns a different result set."""
    client = MagicMock()
    # Each call to client.query() returns an object whose .result() yields rows.
    # The revenue fetcher reuses sba_financial_statements query_period, so we
    # patch at that level instead.
    return client


def test_fetch_revenue_assembles_three_periods():
    gen = Q1ReportGenerator(_make_mock_client_for_revenue())
    fake_period = {
        "gross_revenue": 100_000.0,
        "pos_revenue": 80_000.0,
        "service_charge": 16_000.0,
        "voluntary_tips": 4_000.0,
        "hookah_pos": 0.0,
        "hookah_bank": 15_000.0,
        "hookah_reclass": 0.0,
    }
    with patch.object(Q1ReportGenerator, "_fetch_period_revenue_raw", return_value=fake_period):
        section = gen._fetch_revenue()
    assert section.q1_2026.gross_revenue == 100_000.0
    assert section.q4_2025.gross_revenue == 100_000.0
    assert section.q1_2025.gross_revenue == 100_000.0
    assert section.q1_2026.label == "Q1 2026"


def test_fetch_costs_computes_labor_pct():
    gen = Q1ReportGenerator(MagicMock())
    fake_costs = {"cogs": 30_000.0, "labor": 40_000.0, "opex": 20_000.0}
    fake_revenue = PeriodMetrics(label="Q1 2026", gross_revenue=200_000.0)
    fake_revenue_section = type("R", (), {
        "q1_2026": fake_revenue,
        "q4_2025": PeriodMetrics(label="Q4 2025", gross_revenue=180_000.0),
        "q1_2025": PeriodMetrics(label="Q1 2025", gross_revenue=160_000.0),
    })()
    with patch.object(Q1ReportGenerator, "_fetch_period_costs_raw", return_value=fake_costs), \
         patch.object(Q1ReportGenerator, "_fetch_opex_by_category", return_value={"Rent": 10_000.0}):
        section = gen._fetch_costs(fake_revenue_section)
    assert section.q1_2026.labor == 40_000.0
    assert section.labor_pct_revenue == pytest.approx(20.0)  # 40000 / 200000 = 20%
    assert section.opex_by_category == {"Rent": 10_000.0}


def test_fetch_profitability_computes_ebitda_margin():
    gen = Q1ReportGenerator(MagicMock())
    revenue = type("R", (), {
        "q1_2026": PeriodMetrics(label="Q1 2026", gross_revenue=200_000.0),
        "q4_2025": PeriodMetrics(label="Q4 2025", gross_revenue=180_000.0),
        "q1_2025": PeriodMetrics(label="Q1 2025", gross_revenue=160_000.0),
    })()
    costs = type("C", (), {
        "q1_2026": PeriodMetrics(label="Q1 2026", cogs=40_000, labor=60_000, opex=40_000),
        "q4_2025": PeriodMetrics(label="Q4 2025", cogs=36_000, labor=54_000, opex=36_000),
        "q1_2025": PeriodMetrics(label="Q1 2025", cogs=32_000, labor=48_000, opex=32_000),
    })()
    section = gen._build_profitability(revenue, costs)
    # 200000 - (40000+60000+40000) = 60000 EBITDA, 30% margin
    assert section.q1_2026.ebitda == 60_000.0
    assert section.ebitda_margin_q1_2026 == 30.0


def test_fetch_kpis_computes_revenue_per_business_day():
    gen = Q1ReportGenerator(MagicMock())
    revenue = type("R", (), {
        "q1_2026": PeriodMetrics(label="Q1 2026", gross_revenue=260_000.0),
        "q4_2025": PeriodMetrics(label="Q4 2025", gross_revenue=240_000.0),
        "q1_2025": PeriodMetrics(label="Q1 2025", gross_revenue=200_000.0),
    })()
    fake_kpis = {"covers": 1000, "avg_check": 130.0, "business_days": 52, "labor_hours": 2000.0}
    with patch.object(Q1ReportGenerator, "_fetch_period_kpis_raw", return_value=fake_kpis):
        section = gen._fetch_kpis(revenue)
    assert section.q1_2026.covers == 1000
    assert section.revenue_per_business_day_q1 == pytest.approx(5000.0)  # 260000/52
    assert section.revenue_per_labor_hour_q1 == pytest.approx(130.0)  # 260000/2000
