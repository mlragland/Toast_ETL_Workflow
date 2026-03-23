"""Tests for P1 features: dashboard auth, caching, audit trail."""

import json
import os
from unittest.mock import patch


class TestDashboardAuth:
    """Dashboard key gating when DASHBOARD_KEY is set."""

    def test_health_always_accessible(self, client):
        """Health endpoint never requires key."""
        with patch("main.DASHBOARD_KEY", "secret123"):
            resp = client.get("/")
            assert resp.status_code == 200

    def test_dashboard_blocked_without_key(self, client):
        """Dashboard returns 403 without key when DASHBOARD_KEY is set."""
        with patch("main.DASHBOARD_KEY", "secret123"):
            resp = client.get("/bank-review")
            assert resp.status_code == 403

    def test_dashboard_accessible_with_query_param(self, client):
        """Dashboard accessible with ?key= parameter."""
        with patch("main.DASHBOARD_KEY", "secret123"):
            resp = client.get("/bank-review?key=secret123")
            assert resp.status_code == 200

    def test_dashboard_accessible_with_header(self, client):
        """Dashboard accessible with X-Dashboard-Key header."""
        with patch("main.DASHBOARD_KEY", "secret123"):
            resp = client.get(
                "/bank-review",
                headers={"X-Dashboard-Key": "secret123"},
            )
            assert resp.status_code == 200

    def test_dashboard_wrong_key_returns_403(self, client):
        """Wrong key returns 403."""
        with patch("main.DASHBOARD_KEY", "secret123"):
            resp = client.get("/bank-review?key=wrong")
            assert resp.status_code == 403

    def test_no_key_configured_is_public(self, client):
        """When DASHBOARD_KEY is empty, dashboards are public."""
        with patch("main.DASHBOARD_KEY", ""):
            resp = client.get("/bank-review")
            assert resp.status_code == 200


class TestAnalyticsCache:
    """In-memory cache for analytics endpoints."""

    def test_cache_helpers_work(self):
        """Cache set/get cycle returns data within TTL."""
        from routes_analytics import _cache_key, _cache_get, _cache_set

        key = _cache_key("test_endpoint", {"start": "2026-01-01"})
        assert _cache_get(key) is None  # not cached yet

        _cache_set(key, {"result": "cached_data"})
        assert _cache_get(key) == {"result": "cached_data"}

    def test_cache_key_deterministic(self):
        """Same inputs produce same cache key."""
        from routes_analytics import _cache_key

        k1 = _cache_key("ep", {"a": 1, "b": 2})
        k2 = _cache_key("ep", {"b": 2, "a": 1})
        assert k1 == k2  # sort_keys=True

    def test_cache_key_different_for_different_params(self):
        """Different inputs produce different keys."""
        from routes_analytics import _cache_key

        k1 = _cache_key("ep", {"start": "2026-01"})
        k2 = _cache_key("ep", {"start": "2026-02"})
        assert k1 != k2
