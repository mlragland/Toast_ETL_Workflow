"""Smoke tests — verify the app is alive and critical routes respond."""

import json
from unittest.mock import patch

from q1_report import Q1ReportData, RevenueSection, CostSection, ProfitabilitySection
from q1_report import KPISection, StaffSection, CashFlowSection, ForwardLookSection, PeriodMetrics


def test_health_returns_200(client):
    """GET / returns healthy status JSON."""
    resp = client.get("/")
    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert data["status"] == "healthy"
    assert data["service"] == "toast-etl-pipeline"


def test_bank_review_returns_html(client):
    """GET /bank-review returns an HTML page."""
    resp = client.get("/bank-review")
    assert resp.status_code == 200
    assert resp.content_type.startswith("text/html")
    assert b"<!DOCTYPE html>" in resp.data or b"<html" in resp.data


def test_pnl_returns_html(client):
    """GET /pnl returns an HTML page."""
    resp = client.get("/pnl")
    assert resp.status_code == 200
    assert resp.content_type.startswith("text/html")
    assert b"<html" in resp.data


def test_404_on_unknown_route(client):
    """Unknown routes return 404."""
    resp = client.get("/nonexistent-route")
    assert resp.status_code == 404


def test_all_dashboards_return_html(client):
    """All 14 dashboard routes return 200 with HTML."""
    dashboard_routes = [
        "/bank-review", "/pnl", "/analysis", "/cash-recon",
        "/menu-mix", "/events", "/loyalty", "/servers",
        "/kitchen", "/labor", "/menu-eng", "/kpi-benchmarks",
        "/budget", "/event-roi",
    ]
    for route in dashboard_routes:
        resp = client.get(route)
        assert resp.status_code == 200, f"{route} returned {resp.status_code}"
        assert b"<html" in resp.data, f"{route} did not return HTML"


def _stub_q1_data():
    rev = RevenueSection(
        q1_2026=PeriodMetrics(label="Q1 2026", gross_revenue=200_000),
        q4_2025=PeriodMetrics(label="Q4 2025", gross_revenue=180_000),
        q1_2025=PeriodMetrics(label="Q1 2025", gross_revenue=160_000),
    )
    costs = CostSection(
        q1_2026=PeriodMetrics(label="Q1 2026"),
        q4_2025=PeriodMetrics(label="Q4 2025"),
        q1_2025=PeriodMetrics(label="Q1 2025"),
    )
    profit = ProfitabilitySection(
        q1_2026=PeriodMetrics(label="Q1 2026"),
        q4_2025=PeriodMetrics(label="Q4 2025"),
        q1_2025=PeriodMetrics(label="Q1 2025"),
    )
    kpis = KPISection(
        q1_2026=PeriodMetrics(label="Q1 2026"),
        q4_2025=PeriodMetrics(label="Q4 2025"),
        q1_2025=PeriodMetrics(label="Q1 2025"),
    )
    return Q1ReportData(
        generated_at="2026-06-02 14:00:00 CST",
        revenue=rev, costs=costs, profitability=profit,
        kpis=kpis, staff=StaffSection(), cashflow=CashFlowSection(),
        forward=ForwardLookSection(bullets=["item"]),
    )


def test_q1_report_html_returns_200_with_title(client):
    with patch("routes_dashboards.Q1ReportGenerator") as Gen:
        Gen.return_value.fetch.return_value = _stub_q1_data()
        Gen.return_value.render_html.return_value = "<!DOCTYPE html><title>LOV3 / Houston — Q1 2026 Financial Review</title>"
        resp = client.get("/q1-report")
    assert resp.status_code == 200
    assert b"Q1 2026 Financial Review" in resp.data


def test_q1_report_markdown_returns_200_with_header(client):
    with patch("routes_dashboards.Q1ReportGenerator") as Gen:
        Gen.return_value.fetch.return_value = _stub_q1_data()
        Gen.return_value.render_markdown.return_value = "# LOV3|HTX — Q1 2026 Leadership Financial Report"
        resp = client.get("/q1-report.md")
    assert resp.status_code == 200
    assert b"Q1 2026 Leadership" in resp.data
    assert resp.mimetype == "text/markdown"
