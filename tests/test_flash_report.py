"""Tests for Daily Flash Report feature."""

import json
from unittest.mock import patch, MagicMock
from types import SimpleNamespace


class TestFlashReportDashboard:
    """GET /flash — dashboard renders."""

    def test_flash_dashboard_returns_html(self, client):
        resp = client.get("/flash")
        assert resp.status_code == 200
        assert b"Daily Flash Report" in resp.data
        assert b"<html" in resp.data

    def test_flash_in_nav_bar(self, client):
        resp = client.get("/flash")
        assert b'href="/flash"' in resp.data


class TestFlashReportAPI:
    """POST /api/flash-report — JSON API."""

    def test_invalid_date_returns_400(self, client):
        resp = client.post(
            "/api/flash-report",
            data=json.dumps({"date": "not-a-date"}),
            content_type="application/json",
        )
        assert resp.status_code == 400

    @patch("flash_report.FlashReport")
    def test_returns_expected_json_shape(self, mock_fr_class, client):
        """Flash report returns expected top-level keys."""
        mock_fr = MagicMock()
        mock_fr.collect.return_value = {
            "date": "2026-03-22",
            "day_name": "Sunday",
            "display_date": "Mar 22, 2026",
            "revenue": 12450,
            "orders": 87,
            "guests": 234,
            "avg_check": 143.10,
            "tips": 2100,
            "gratuity": 1800,
            "top_servers": [{"server": "John", "revenue": 2340, "orders": 12, "tips": 400}],
            "prior_week": {"revenue": 11500, "orders": 80, "guests": 210},
            "expenses": {"cogs": 3000, "labor": 3400, "marketing": 1500, "opex": 2500},
            "cash": {"collected": 3200, "deposited": 2950, "gap": 250},
            "margins": {"cogs_pct": 24.1, "labor_pct": 27.3, "net_pct": 18.2, "adj_revenue": 12450},
        }
        mock_fr.format_json.return_value = {
            "date": "2026-03-22",
            "day_name": "Sunday",
            "revenue": 12450,
            "orders": 87,
            "guests": 234,
            "avg_check": 143.10,
            "top_servers": [{"server": "John", "revenue": 2340}],
            "comparison": {"prior_revenue": 11500, "revenue_change_pct": 8.3},
            "expenses": {"cogs": 3000, "labor": 3400},
            "margins": {"cogs_pct": 24.1, "labor_pct": 27.3, "net_pct": 18.2},
            "cash": {"collected": 3200, "deposited": 2950, "gap": 250},
        }
        mock_fr_class.return_value = mock_fr

        resp = client.post(
            "/api/flash-report",
            data=json.dumps({"date": "2026-03-22"}),
            content_type="application/json",
        )
        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert data["revenue"] == 12450
        assert "top_servers" in data
        assert "comparison" in data
        assert "margins" in data
        assert "cash" in data

    def test_empty_body_defaults_to_yesterday(self, client):
        """Empty POST body should not return 400 (defaults to yesterday)."""
        with patch("flash_report.FlashReport") as mock_cls:
            mock = MagicMock()
            mock.collect.return_value = {"date": "2026-03-22", "day_name": "Sun",
                "display_date": "Mar 22", "revenue": 0, "orders": 0, "guests": 0,
                "avg_check": 0, "tips": 0, "gratuity": 0, "top_servers": [],
                "prior_week": {}, "expenses": {}, "cash": {"collected": 0, "deposited": 0, "gap": 0},
                "margins": {"cogs_pct": 0, "labor_pct": 0, "net_pct": 0}}
            mock.format_json.return_value = {"date": "2026-03-22", "revenue": 0}
            mock_cls.return_value = mock

            resp = client.post("/api/flash-report", content_type="application/json")
            assert resp.status_code == 200


class TestFlashReportFormatting:
    """Test Slack formatting logic."""

    def test_slack_format_structure(self):
        from flash_report import FlashReport
        fr = FlashReport.__new__(FlashReport)  # skip __init__ (no BQ needed)

        data = {
            "date": "2026-03-22", "day_name": "Sunday", "display_date": "Mar 22, 2026",
            "revenue": 12450, "orders": 87, "guests": 234, "avg_check": 143.10,
            "tips": 2100, "gratuity": 1800,
            "top_servers": [
                {"server": "John", "revenue": 2340, "orders": 12, "tips": 400},
                {"server": "Maria", "revenue": 1890, "orders": 10, "tips": 350},
            ],
            "prior_week": {"revenue": 11500, "orders": 80, "guests": 210},
            "expenses": {"cogs": 3000, "labor": 3400, "marketing": 1500, "opex": 2500},
            "cash": {"collected": 3200, "deposited": 2950, "gap": 250},
            "margins": {"cogs_pct": 24.1, "labor_pct": 27.3, "net_pct": 18.2},
        }

        msg = fr.format_slack(data)
        assert "LOV3 Daily Flash" in msg
        assert "Revenue" in msg
        assert "John" in msg
        assert "COGS" in msg
        assert "Cash" in msg
