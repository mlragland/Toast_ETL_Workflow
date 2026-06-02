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
