"""Unit tests for plaid_sync.py — HTTP + BQ mocked."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from plaid_sync import (
    STALE_DAYS,
    PlaidSync,
    SyncSummary,
    _transaction_to_row,
)


# ── _transaction_to_row ─────────────────────────────────────────────

_PLAID_TXN = {
    "transaction_id": "abc123",
    "account_id": "acct1",
    "date": "2026-07-25",
    "authorized_date": "2026-07-25",
    "amount": 42.50,  # positive in Plaid = debit
    "iso_currency_code": "USD",
    "name": "GORDON FOOD SERV",
    "merchant_name": "Gordon Food Service",
    "original_description": "GORDON FOOD SERV DES:AR PAYMENT",
    "pending": False,
    "personal_finance_category": {"primary": "FOOD_AND_DRINK", "detailed": "FOOD_AND_DRINK_RESTAURANTS"},
}


def test_row_flips_sign_to_bofa_convention():
    r = _transaction_to_row(_PLAID_TXN, "batch1", "plaid_sync_production")
    # Plaid +42.50 debit → BofA -42.50 (money leaving account)
    assert r["amount"] == -42.50
    assert r["transaction_type"] == "debit"
    assert r["abs_amount"] == 42.50


def test_row_prefers_original_description():
    r = _transaction_to_row(_PLAID_TXN, "batch1", "plaid_sync_production")
    assert "GORDON FOOD SERV DES:AR PAYMENT" in r["description"]


def test_row_uses_detailed_category_when_present():
    r = _transaction_to_row(_PLAID_TXN, "batch1", "plaid_sync_production")
    assert r["category"] == "FOOD_AND_DRINK_RESTAURANTS"
    assert r["category_source"] == "plaid"


def test_row_falls_back_to_primary_category():
    txn = dict(_PLAID_TXN, personal_finance_category={"primary": "TRANSFER_IN", "detailed": None})
    r = _transaction_to_row(txn, "batch1", "plaid_sync_production")
    assert r["category"] == "TRANSFER_IN"


def test_row_marks_uncategorized_when_no_category():
    txn = dict(_PLAID_TXN, personal_finance_category=None)
    r = _transaction_to_row(txn, "batch1", "plaid_sync_production")
    assert r["category"] is None
    assert r["category_source"] == "uncategorized"


def test_row_returns_none_when_missing_date():
    txn = dict(_PLAID_TXN, date=None)
    assert _transaction_to_row(txn, "batch1", "plaid_sync_production") is None


def test_row_returns_none_when_missing_amount():
    txn = dict(_PLAID_TXN, amount=None)
    assert _transaction_to_row(txn, "batch1", "plaid_sync_production") is None


def test_credit_amount_positive():
    txn = dict(_PLAID_TXN, amount=-100.00)  # Plaid -100 = credit (money in)
    r = _transaction_to_row(txn, "batch1", "plaid_sync_production")
    # Should flip to +100 (BofA convention: credit = positive)
    assert r["amount"] == 100.00
    assert r["transaction_type"] == "credit"


# ── Auth ────────────────────────────────────────────────────────────

def _mock_sm(client_id="cid", prod_secret="prod_secret", sbx_secret="sbx_secret",
             access_token="access_token", item_id="item_x"):
    sm = MagicMock()
    sm.get_secret.side_effect = lambda name: {
        "plaid-client-id": client_id,
        "plaid-secret-production": prod_secret,
        "plaid-secret-sandbox": sbx_secret,
        "plaid-access-token": access_token,
        "plaid-item-id": item_id,
    }[name]
    return sm


def test_get_credentials_returns_production_by_default():
    sync = PlaidSync(bq_client=MagicMock(), secret_manager=_mock_sm(), environment="production")
    client_id, secret = sync._get_credentials()
    assert client_id == "cid"
    assert secret == "prod_secret"


def test_get_credentials_returns_sandbox_when_configured():
    sync = PlaidSync(bq_client=MagicMock(), secret_manager=_mock_sm(), environment="sandbox")
    _, secret = sync._get_credentials()
    assert secret == "sbx_secret"


# ── HTTP + pagination ───────────────────────────────────────────────

def _mock_resp(status_code, json_body):
    r = MagicMock()
    r.status_code = status_code
    r.json.return_value = json_body
    r.headers = {"content-type": "application/json"}
    return r


def test_transactions_sync_paginates_until_has_more_false():
    sync = PlaidSync(bq_client=MagicMock(), secret_manager=_mock_sm())
    pages = [
        _mock_resp(200, {"added": [{"transaction_id": "a", "date": "2026-07-01", "amount": 10, "name": "A"}],
                        "modified": [], "removed": [], "next_cursor": "cur1", "has_more": True}),
        _mock_resp(200, {"added": [{"transaction_id": "b", "date": "2026-07-02", "amount": 20, "name": "B"}],
                        "modified": [], "removed": [], "next_cursor": "cur2", "has_more": True}),
        _mock_resp(200, {"added": [{"transaction_id": "c", "date": "2026-07-03", "amount": 30, "name": "C"}],
                        "modified": [{"transaction_id": "b2", "date": "2026-07-02", "amount": 25, "name": "B2"}],
                        "removed": [{"transaction_id": "old"}], "next_cursor": "cur3", "has_more": False}),
    ]
    with patch("plaid_sync.requests.post", side_effect=pages):
        result = sync._fetch_transactions_sync("access_token", cursor="")
    assert len(result["added"]) == 3
    assert len(result["modified"]) == 1
    assert len(result["removed"]) == 1
    assert result["next_cursor"] == "cur3"


def test_transactions_sync_bubbles_up_plaid_errors():
    sync = PlaidSync(bq_client=MagicMock(), secret_manager=_mock_sm())
    err_resp = _mock_resp(400, {"error_code": "INVALID_ACCESS_TOKEN",
                                 "error_message": "Access token invalid"})
    with patch("plaid_sync.requests.post", return_value=err_resp):
        with pytest.raises(RuntimeError, match="INVALID_ACCESS_TOKEN"):
            sync._fetch_transactions_sync("bad_token", cursor="")


# ── Cursor state ────────────────────────────────────────────────────

def test_load_cursor_empty_on_first_run():
    bq = MagicMock()
    query_result = MagicMock()
    query_result.result.return_value = iter([])  # No rows
    bq.query.return_value = query_result
    bq.get_table.return_value = MagicMock()  # table exists
    sync = PlaidSync(bq_client=bq, secret_manager=_mock_sm())
    assert sync._load_cursor("item_x") == ""


def test_load_cursor_returns_saved_value():
    bq = MagicMock()
    query_result = MagicMock()
    row = MagicMock()
    row.cursor = "saved_cursor_123"
    query_result.result.return_value = iter([row])
    bq.query.return_value = query_result
    bq.get_table.return_value = MagicMock()  # exists
    sync = PlaidSync(bq_client=bq, secret_manager=_mock_sm())
    assert sync._load_cursor("item_x") == "saved_cursor_123"


# ── Link token / exchange (auth handshake) ──────────────────────────

def test_create_link_token_calls_correct_endpoint():
    sync = PlaidSync(bq_client=MagicMock(), secret_manager=_mock_sm())
    resp = _mock_resp(200, {"link_token": "link-production-abc", "expiration": "2026-07-28T06:00:00Z"})
    with patch("plaid_sync.requests.post", return_value=resp) as m:
        result = sync.create_link_token(user_id="lov3-owner")
    assert result["link_token"] == "link-production-abc"
    call_kwargs = m.call_args.kwargs
    assert "link/token/create" in m.call_args.args[0]
    body = call_kwargs["json"]
    assert body["client_name"] == "LOV3 Houston Analytics"
    assert body["products"] == ["transactions"]
    assert body["country_codes"] == ["US"]


def test_exchange_public_token_returns_access_token():
    sync = PlaidSync(bq_client=MagicMock(), secret_manager=_mock_sm())
    resp = _mock_resp(200, {"access_token": "access-production-xyz", "item_id": "item_new_1"})
    with patch("plaid_sync.requests.post", return_value=resp):
        result = sync.exchange_public_token("public-production-abc")
    assert result["access_token"] == "access-production-xyz"
    assert result["item_id"] == "item_new_1"


# ── Sync summary shape ──────────────────────────────────────────────

def test_sync_summary_to_dict_omits_error_when_success():
    s = SyncSummary(status="success", added_count=5, rows_merged=5)
    d = s.to_dict()
    assert "error" not in d
    assert d["added_count"] == 5


def test_sync_summary_to_dict_includes_error_on_failure():
    s = SyncSummary(status="error", error="something broke")
    d = s.to_dict()
    assert d["error"] == "something broke"
