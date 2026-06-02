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


def test_fetch_staff_returns_top_performers_sorted():
    gen = Q1ReportGenerator(MagicMock())
    fake_bartenders = [
        {"name": "Alice", "revenue": 50_000.0, "hours": 200.0},
        {"name": "Bob", "revenue": 70_000.0, "hours": 250.0},
    ]
    fake_servers = [
        {"name": "Carol", "revenue": 60_000.0, "hours": 180.0},
    ]
    with patch.object(Q1ReportGenerator, "_query_bartender_revenue", return_value=fake_bartenders), \
         patch.object(Q1ReportGenerator, "_query_server_revenue", return_value=fake_servers):
        section = gen._fetch_staff()
    assert section.top_bartenders[0].name == "Bob"  # sorted desc
    assert section.top_bartenders[0].attributed_revenue == 70_000.0
    assert section.top_servers[0].name == "Carol"


def test_fetch_cashflow_flags_concentration_warning():
    gen = Q1ReportGenerator(MagicMock())
    fake_deposits = 300_000.0
    fake_expenses_by_cat = {"Rent": 50_000.0, "Food": 30_000.0, "Marketing": 5_000.0}
    fake_vendors = [
        {"vendor": "VendorBig", "spend": 20_000.0},
        {"vendor": "VendorSmall", "spend": 1_000.0},
    ]
    with patch.object(Q1ReportGenerator, "_query_total_deposits", return_value=fake_deposits), \
         patch.object(Q1ReportGenerator, "_query_expenses_for_cashflow", return_value=fake_expenses_by_cat), \
         patch.object(Q1ReportGenerator, "_query_top_vendors", return_value=fake_vendors):
        section = gen._fetch_cashflow()
    assert section.total_deposits == 300_000.0
    assert section.total_expenses == 85_000.0
    # VendorBig is 20000 / 85000 = 23.5% — exceeds 15% threshold
    assert any("VendorBig" in w for w in section.concentration_warnings)


def test_fetch_assembles_all_sections():
    gen = Q1ReportGenerator(MagicMock())
    fake_revenue = RevenueSection(
        q1_2026=PeriodMetrics(label="Q1 2026", gross_revenue=200_000.0),
        q4_2025=PeriodMetrics(label="Q4 2025", gross_revenue=180_000.0),
        q1_2025=PeriodMetrics(label="Q1 2025", gross_revenue=160_000.0),
    )
    fake_costs = CostSection(
        q1_2026=PeriodMetrics(label="Q1 2026", cogs=40_000, labor=60_000, opex=40_000),
        q4_2025=PeriodMetrics(label="Q4 2025"),
        q1_2025=PeriodMetrics(label="Q1 2025"),
    )
    fake_kpis = KPISection(
        q1_2026=PeriodMetrics(label="Q1 2026"),
        q4_2025=PeriodMetrics(label="Q4 2025"),
        q1_2025=PeriodMetrics(label="Q1 2025"),
    )
    fake_staff = StaffSection()
    fake_cashflow = CashFlowSection()
    with patch.object(Q1ReportGenerator, "_fetch_revenue", return_value=fake_revenue), \
         patch.object(Q1ReportGenerator, "_fetch_costs", return_value=fake_costs), \
         patch.object(Q1ReportGenerator, "_fetch_kpis", return_value=fake_kpis), \
         patch.object(Q1ReportGenerator, "_fetch_staff", return_value=fake_staff), \
         patch.object(Q1ReportGenerator, "_fetch_cashflow", return_value=fake_cashflow):
        data = gen.fetch()
    assert data.revenue.q1_2026.gross_revenue == 200_000.0
    assert data.costs.q1_2026.labor == 60_000
    assert len(data.forward.bullets) == 5
    assert data.generated_at  # non-empty string


# Import dataclasses used in this test
from q1_report import RevenueSection, CostSection, KPISection, StaffSection, CashFlowSection


def test_render_markdown_includes_all_sections():
    gen = Q1ReportGenerator(MagicMock())
    data = _build_minimal_report_data()
    md = gen.render_markdown(data)
    # Section headers
    assert "# LOV3|HTX — Q1 2026 Leadership Financial Report" in md
    assert "## A. Revenue Analysis" in md
    assert "## B. Cost Structure" in md
    assert "## C. Profitability" in md
    assert "## D. Operational KPIs" in md
    assert "## E. Staff Performance" in md
    assert "## F. Cash Flow & Bank Reconciliation" in md
    assert "## G. Forward Look" in md
    # Comparison columns shown
    assert "Q4 2025" in md
    assert "Q1 2025" in md
    # Generation timestamp
    assert "Generated:" in md


from q1_report import (
    ProfitabilitySection, StaffPerformer, VendorSpend, ForwardLookSection, Q1ReportData,
)


def _build_minimal_report_data() -> Q1ReportData:
    rev = RevenueSection(
        q1_2026=PeriodMetrics(label="Q1 2026", gross_revenue=200_000),
        q4_2025=PeriodMetrics(label="Q4 2025", gross_revenue=180_000),
        q1_2025=PeriodMetrics(label="Q1 2025", gross_revenue=160_000),
        monthly={
            "2026-01": PeriodMetrics(label="2026-01", gross_revenue=60_000),
            "2026-02": PeriodMetrics(label="2026-02", gross_revenue=65_000),
            "2026-03": PeriodMetrics(label="2026-03", gross_revenue=75_000),
        },
        category_mix={"Food": 60_000, "Liquor": 80_000, "Hookah": 15_000},
    )
    costs = CostSection(
        q1_2026=PeriodMetrics(label="Q1 2026", cogs=40_000, labor=60_000, opex=40_000),
        q4_2025=PeriodMetrics(label="Q4 2025", cogs=36_000, labor=54_000, opex=36_000),
        q1_2025=PeriodMetrics(label="Q1 2025", cogs=32_000, labor=48_000, opex=32_000),
        opex_by_category={"Rent": 20_000, "Marketing": 5_000},
        labor_pct_revenue=30.0,
    )
    profit = ProfitabilitySection(
        q1_2026=PeriodMetrics(label="Q1 2026", gross_revenue=200_000, ebitda=60_000),
        q4_2025=PeriodMetrics(label="Q4 2025", gross_revenue=180_000, ebitda=54_000),
        q1_2025=PeriodMetrics(label="Q1 2025", gross_revenue=160_000, ebitda=48_000),
        ebitda_margin_q1_2026=30.0, ebitda_margin_q4_2025=30.0, ebitda_margin_q1_2025=30.0,
    )
    kpis = KPISection(
        q1_2026=PeriodMetrics(label="Q1 2026", covers=1000, avg_check=200.0, business_days=52, labor_hours=2000.0),
        q4_2025=PeriodMetrics(label="Q4 2025", covers=950, avg_check=190.0, business_days=51, labor_hours=1900.0),
        q1_2025=PeriodMetrics(label="Q1 2025", covers=900, avg_check=178.0, business_days=50, labor_hours=1800.0),
        revenue_per_labor_hour_q1=100.0, revenue_per_business_day_q1=3846.15,
    )
    staff = StaffSection(
        top_bartenders=[StaffPerformer("Alice", 50_000.0, 200.0)],
        top_servers=[StaffPerformer("Carol", 60_000.0, 180.0)],
    )
    cashflow = CashFlowSection(
        total_deposits=300_000, total_expenses=140_000,
        by_category={"Rent": 60_000, "Food": 50_000, "Marketing": 30_000},
        top_vendors=[VendorSpend("VendorA", 40_000, 0.28)],
        concentration_warnings=["VendorA represents 28.6% of opex (threshold: 15%)"],
    )
    forward = ForwardLookSection(bullets=["item 1", "item 2"])
    return Q1ReportData(
        generated_at="2026-06-02 14:00:00 CST",
        revenue=rev, costs=costs, profitability=profit,
        kpis=kpis, staff=staff, cashflow=cashflow, forward=forward,
    )


def test_render_html_returns_complete_page():
    gen = Q1ReportGenerator(MagicMock())
    data = _build_minimal_report_data()
    out = gen.render_html(data)
    assert out.startswith("<!DOCTYPE html>")
    assert "<title>" in out
    assert "Q1 2026 Financial Analysis" in out
    assert "A. Revenue Analysis" in out
    assert "G. Forward Look" in out
    # Make sure it doesn't include the literal '{{' template artifact
    assert "{{" not in out
