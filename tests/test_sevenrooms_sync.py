"""Unit tests for sevenrooms_sync.py — HTTP + BQ are mocked."""

import json
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from sevenrooms_sync import (
    LOOKBACK_MINUTES,
    SR_API_BASE,
    SR_PAGE_LIMIT,
    STALE_HOURS,
    SevenRoomsSync,
    TokenCache,
    _reservation_schema,
    _reservation_to_row,
)


# ── TokenCache ───────────────────────────────────────────────────────────────

def test_token_cache_returns_none_when_empty():
    assert TokenCache().get() is None


def test_token_cache_returns_token_when_fresh():
    tc = TokenCache()
    future = (datetime.now(timezone.utc) + timedelta(hours=10)).isoformat()
    tc.set("tok_abc", future)
    assert tc.get() == "tok_abc"


def test_token_cache_returns_none_when_near_expiry():
    tc = TokenCache()
    # Within the 5-minute pre-expiry buffer
    close = (datetime.now(timezone.utc) + timedelta(minutes=2)).isoformat()
    tc.set("tok_abc", close)
    assert tc.get() is None


def test_token_cache_handles_malformed_expiry():
    tc = TokenCache()
    tc.set("tok_abc", "not-a-date")
    # Should still cache the token (23h fallback)
    assert tc.get() == "tok_abc"


def test_token_cache_clear():
    tc = TokenCache()
    future = (datetime.now(timezone.utc) + timedelta(hours=10)).isoformat()
    tc.set("tok_abc", future)
    tc.clear()
    assert tc.get() is None


# ── _reservation_to_row ──────────────────────────────────────────────────────

_MIN_RES = {
    "id": "res_123",
    "venue_id": "venue_1",
    "venue_group_id": "vg_1",
    "date": "2026-07-25",
    "arrival_time": "20:00",
    "status": "CONFIRMED",
    "status_simple": "Incomplete",
    "max_guests": 4,
    "booked_by": "Sossity Taylor",
    "updated": "2026-07-25T14:30:00Z",
    "created": "2026-07-25T10:00:00Z",
    "tags": [{"name": "birthday"}],
    "pos_tickets": [{"check_id": "chk_1", "amount": 250}],
    "check_numbers": "1234, 5678",
    "total_gross_payment": 350.50,
    "is_vip": True,
}


def test_reservation_to_row_maps_core_fields():
    row = _reservation_to_row(_MIN_RES, "2026-07-25T15:00:00Z")
    assert row is not None
    assert row["id"] == "res_123"
    assert row["venue_id"] == "venue_1"
    assert row["date"] == "2026-07-25"
    assert row["max_guests"] == 4
    assert row["booked_by"] == "Sossity Taylor"
    assert row["check_numbers"] == "1234, 5678"
    assert row["total_gross_payment"] == 350.50
    assert row["is_vip"] is True


def test_reservation_to_row_stores_full_raw_json():
    row = _reservation_to_row(_MIN_RES, "2026-07-25T15:00:00Z")
    assert row["raw_json"] == json.dumps(_MIN_RES)


def test_reservation_to_row_jsonifies_nested_structures():
    row = _reservation_to_row(_MIN_RES, "2026-07-25T15:00:00Z")
    assert json.loads(row["tags_json"]) == [{"name": "birthday"}]
    assert json.loads(row["pos_tickets_json"]) == [{"check_id": "chk_1", "amount": 250}]


def test_reservation_to_row_handles_empty_nested_as_null():
    r = dict(_MIN_RES, tags=[], pos_tickets=None, custom_fields={})
    row = _reservation_to_row(r, "2026-07-25T15:00:00Z")
    assert row["tags_json"] is None
    assert row["pos_tickets_json"] is None
    assert row["custom_fields_json"] is None


def test_reservation_to_row_returns_none_when_id_missing():
    r = {k: v for k, v in _MIN_RES.items() if k != "id"}
    assert _reservation_to_row(r, "2026-07-25T15:00:00Z") is None


def test_reservation_to_row_coerces_bad_numbers_to_none():
    r = dict(_MIN_RES, max_guests="not-a-number", total_gross_payment="")
    row = _reservation_to_row(r, "2026-07-25T15:00:00Z")
    assert row["max_guests"] is None
    assert row["total_gross_payment"] is None


def test_reservation_to_row_normalizes_iso_timestamps():
    r = dict(_MIN_RES, updated="2026-07-25T14:30:00Z")
    row = _reservation_to_row(r, "2026-07-25T15:00:00Z")
    # Should preserve timezone info as +00:00
    assert row["updated"] == "2026-07-25T14:30:00+00:00"


# ── Auth ─────────────────────────────────────────────────────────────────────

def _mock_sm(client_id="cid", client_secret="sec", venue_group_id="vg_1"):
    sm = MagicMock()
    sm.get_secret.side_effect = lambda name: {
        "sevenrooms-client-id": client_id,
        "sevenrooms-client-secret": client_secret,
        "sevenrooms-venue-group-id": venue_group_id,
    }[name]
    return sm


def test_get_token_calls_auth_endpoint_with_form_body():
    srs = SevenRoomsSync(bq_client=MagicMock(), secret_manager=_mock_sm())
    future = (datetime.now(timezone.utc) + timedelta(hours=10)).isoformat()
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {"status": 200, "msg": "ok",
                              "data": {"token": "tok_xyz", "token_expiration_datetime": future}}
    with patch("sevenrooms_sync.requests.post", return_value=resp) as m:
        token = srs._get_token()
    assert token == "tok_xyz"
    m.assert_called_once()
    call_kwargs = m.call_args.kwargs
    assert call_kwargs["data"] == {"client_id": "cid", "client_secret": "sec"}
    assert call_kwargs["headers"]["Content-Type"] == "application/x-www-form-urlencoded"
    assert m.call_args.args[0] == f"{SR_API_BASE}/auth"


def test_get_token_uses_cache_on_second_call():
    srs = SevenRoomsSync(bq_client=MagicMock(), secret_manager=_mock_sm())
    future = (datetime.now(timezone.utc) + timedelta(hours=10)).isoformat()
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {"status": 200, "msg": "ok",
                              "data": {"token": "tok_xyz", "token_expiration_datetime": future}}
    with patch("sevenrooms_sync.requests.post", return_value=resp) as m:
        srs._get_token()
        srs._get_token()
    assert m.call_count == 1


def test_get_token_raises_on_non_200_status_field():
    srs = SevenRoomsSync(bq_client=MagicMock(), secret_manager=_mock_sm())
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {"status": 401, "msg": "bad creds", "data": None}
    with patch("sevenrooms_sync.requests.post", return_value=resp):
        with pytest.raises(RuntimeError, match="SR /auth"):
            srs._get_token()


# ── Fetch + pagination ───────────────────────────────────────────────────────

def _make_page(results, cursor):
    return {"status": 200, "msg": "ok",
            "data": {"results": results, "cursor": cursor, "limit": SR_PAGE_LIMIT}}


def test_fetch_reservations_paginates_until_cursor_none():
    srs = SevenRoomsSync(bq_client=MagicMock(), secret_manager=_mock_sm())
    srs._token_cache.set("tok",
        (datetime.now(timezone.utc) + timedelta(hours=10)).isoformat())

    pages = [
        _make_page([{"id": "a", "updated": "2026-07-25T10:00:00Z"}], "cur1"),
        _make_page([{"id": "b", "updated": "2026-07-25T11:00:00Z"}], "cur2"),
        _make_page([{"id": "c", "updated": "2026-07-25T12:00:00Z"}], None),
    ]
    responses = []
    for p in pages:
        r = MagicMock()
        r.status_code = 200
        r.raise_for_status.return_value = None
        r.json.return_value = p
        responses.append(r)

    with patch("sevenrooms_sync.requests.get", side_effect=responses):
        got = srs._fetch_reservations("2026-07-01T00:00:00Z", "vg_1")
    assert [r["id"] for r in got] == ["a", "b", "c"]


def test_fetch_reservations_stops_on_empty_results():
    srs = SevenRoomsSync(bq_client=MagicMock(), secret_manager=_mock_sm())
    srs._token_cache.set("tok",
        (datetime.now(timezone.utc) + timedelta(hours=10)).isoformat())
    r = MagicMock()
    r.status_code = 200
    r.raise_for_status.return_value = None
    r.json.return_value = _make_page([], "still_a_cursor_but_no_results")
    with patch("sevenrooms_sync.requests.get", return_value=r):
        got = srs._fetch_reservations("2026-07-01T00:00:00Z", "vg_1")
    assert got == []


def test_api_get_refreshes_token_on_401_and_retries_once():
    srs = SevenRoomsSync(bq_client=MagicMock(), secret_manager=_mock_sm())
    srs._token_cache.set("stale_tok",
        (datetime.now(timezone.utc) + timedelta(hours=10)).isoformat())

    r_401 = MagicMock(); r_401.status_code = 401; r_401.raise_for_status.return_value = None
    r_401.json.return_value = {}
    r_ok = MagicMock(); r_ok.status_code = 200; r_ok.raise_for_status.return_value = None
    r_ok.json.return_value = _make_page([], None)

    future = (datetime.now(timezone.utc) + timedelta(hours=10)).isoformat()
    auth_resp = MagicMock()
    auth_resp.raise_for_status.return_value = None
    auth_resp.json.return_value = {"status": 200, "msg": "ok",
                                    "data": {"token": "fresh_tok",
                                             "token_expiration_datetime": future}}

    with patch("sevenrooms_sync.requests.get", side_effect=[r_401, r_ok]) as mget, \
         patch("sevenrooms_sync.requests.post", return_value=auth_resp) as mpost:
        srs._api_get("/reservations", {"venue_group_id": "vg_1"})

    assert mget.call_count == 2
    assert mpost.call_count == 1  # only refreshed once
    # Second GET must have used the fresh token
    assert mget.call_args_list[1].kwargs["headers"]["Authorization"] == "fresh_tok"


# ── Schema is stable ─────────────────────────────────────────────────────────

def test_schema_has_id_required_and_synced_at_required():
    s = _reservation_schema()
    modes = {f.name: f.mode for f in s}
    assert modes["id"] == "REQUIRED"
    assert modes["synced_at"] == "REQUIRED"
    assert modes["raw_json"] == "REQUIRED"


def test_schema_field_count_matches_row_keys():
    schema_names = {f.name for f in _reservation_schema()}
    row = _reservation_to_row(_MIN_RES, "2026-07-25T15:00:00Z")
    assert schema_names == set(row.keys())
