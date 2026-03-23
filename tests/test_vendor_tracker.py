"""Tests for Vendor Spend Tracker feature."""

import json
from unittest.mock import patch, MagicMock


class TestVendorDashboard:
    """GET /vendors — dashboard renders."""

    def test_vendor_dashboard_returns_html(self, client):
        resp = client.get("/vendors")
        assert resp.status_code == 200
        assert b"Vendor Spend Tracker" in resp.data

    def test_vendor_in_nav_bar(self, client):
        resp = client.get("/vendors")
        assert b'href="/vendors"' in resp.data


class TestVendorAPI:
    """POST /api/vendor-tracker — JSON API."""

    def test_missing_dates_returns_400(self, client):
        resp = client.post(
            "/api/vendor-tracker",
            data=json.dumps({}),
            content_type="application/json",
        )
        assert resp.status_code == 400

    def test_invalid_date_returns_400(self, client):
        resp = client.post(
            "/api/vendor-tracker",
            data=json.dumps({"start_date": "nope", "end_date": "2026-03-01"}),
            content_type="application/json",
        )
        assert resp.status_code == 400


class TestVendorTrackerLogic:
    """Unit tests for VendorTracker computation methods."""

    def test_concentration_empty(self):
        from vendor_tracker import VendorTracker
        vt = VendorTracker.__new__(VendorTracker)
        result = vt._compute_concentration([])
        assert result["top_5_pct"] == 0

    def test_concentration_with_data(self):
        from vendor_tracker import VendorTracker
        vt = VendorTracker.__new__(VendorTracker)
        vendors = [{"total_spend": 100 - i} for i in range(10)]
        result = vt._compute_concentration(vendors)
        assert result["top_5_pct"] > 0
        assert result["top_10_pct"] >= result["top_5_pct"]

    def test_anomaly_detection_no_trends(self):
        from vendor_tracker import VendorTracker
        vt = VendorTracker.__new__(VendorTracker)
        result = vt._detect_anomalies([])
        assert result == []

    def test_anomaly_detection_flags_spike(self):
        from vendor_tracker import VendorTracker
        vt = VendorTracker.__new__(VendorTracker)
        trends = [
            {"vendor": "Sysco", "month": "2026-01", "spend": 1000, "txn_count": 5},
            {"vendor": "Sysco", "month": "2026-02", "spend": 2000, "txn_count": 5},  # +100%
        ]
        result = vt._detect_anomalies(trends)
        assert len(result) == 1
        assert result[0]["vendor"] == "Sysco"
        assert result[0]["change_pct"] == 100.0
        assert result[0]["severity"] == "high"

    def test_anomaly_detection_ignores_small_change(self):
        from vendor_tracker import VendorTracker
        vt = VendorTracker.__new__(VendorTracker)
        trends = [
            {"vendor": "ADP", "month": "2026-01", "spend": 1000, "txn_count": 1},
            {"vendor": "ADP", "month": "2026-02", "spend": 1100, "txn_count": 1},  # +10%
        ]
        result = vt._detect_anomalies(trends)
        assert len(result) == 0

    def test_kpis_computation(self):
        from vendor_tracker import VendorTracker
        vt = VendorTracker.__new__(VendorTracker)
        data = {
            "top_vendors": [{"total_spend": 500}, {"total_spend": 300}],
            "category_breakdown": [{"section": "COGS"}, {"section": "Labor"}],
            "anomalies": [{"severity": "high"}, {"severity": "medium"}],
        }
        kpis = vt._compute_kpis(data)
        assert kpis["total_vendors"] == 2
        assert kpis["total_spend"] == 800
        assert kpis["anomaly_count"] == 2
        assert kpis["high_anomalies"] == 1
