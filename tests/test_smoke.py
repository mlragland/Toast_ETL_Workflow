"""Smoke tests — verify the app is alive and critical routes respond."""

import json


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
