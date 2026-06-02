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
