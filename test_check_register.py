#!/usr/bin/env python3
"""
Test script for check register integration.

Creates a mock BofA CSV with various check scenarios, uploads it,
and verifies the categorization logic works correctly.

Usage:
    python test_check_register.py [--url URL]

Defaults to the production Cloud Run URL.
"""

import argparse
import io
import json
import sys
import tempfile

import requests

DEFAULT_URL = "https://toast-etl-pipeline-t3di7qky4q-uc.a.run.app"

# ── Test CSV ─────────────────────────────────────────────────────────────────
# Scenario breakdown:
#   1. Check 1504 — "Reginald Davis" in register, matches keyword rule "REGINALD DAVIS"
#                    → category_source="check_register", category from rule
#   2. Check 1800 — "ABC Staffing Services" in register, NO keyword rule match
#                    → category_source="uncategorized", vendor_normalized="ABC Staffing Services"
#   3. Check 9999 — NOT in register at all
#                    → category_source="uncategorized", vendor_normalized="Check 9999"
#   4. Check 1797 — "Kraftsmen Bakery" in register, no exact keyword match
#                    (rule is "KRAFTSMEN BAKING" which ≠ "KRAFTSMEN BAKERY")
#                    → category_source="uncategorized", vendor_normalized="Kraftsmen Bakery"
#   5. Non-check  — normal CHECKCARD transaction with keyword "KRAFTSMEN BAKING"
#                    → category_source="auto" (keyword matched)

TEST_CSV = """\
Date,Description,Amount,Running Bal.
01/15/2099,Check 1504,-250.00,10000.00
01/16/2099,Check 1800,-3431.28,6568.72
01/17/2099,Check 9999,-100.00,6468.72
01/18/2099,Check 1797,-488.73,5979.99
01/19/2099,CHECKCARD 0119 KRAFTSMEN BAKING HOUSTON TX,-85.00,5894.99
01/20/2099,Zelle payment to JOHN DOE Conf#ABC,500.00,6394.99
"""

EXPECTED = [
    {
        "description": "Check 1504",
        "expect_source": "check_register",
        "expect_vendor_contains": "Reginald Davis",
        "expect_category_contains": "Promoter Payout",
        "label": "Check in register + keyword rule match",
    },
    {
        "description": "Check 1800",
        "expect_source": "check_register",
        "expect_vendor_contains": "ABC Staffing",
        "expect_category_contains": "Cleaning / Janitorial",
        "label": "Check in register + keyword rule match (ABC Staffing)",
    },
    {
        "description": "Check 9999",
        "expect_source": "uncategorized",
        "expect_vendor_contains": "Check 9999",
        "expect_category_contains": "Uncategorized",
        "label": "Check NOT in register",
    },
    {
        "description": "Check 1797",
        "expect_source": "uncategorized",
        "expect_vendor_contains": "Kraftsmen Bakery",
        "expect_category_contains": "Uncategorized",
        "label": "Check in register, no keyword rule → vendor resolved",
    },
    {
        "description": "CHECKCARD 0119 KRAFTSMEN BAKING HOUSTON TX",
        "expect_source": "auto",
        "expect_vendor_contains": "",
        "expect_category_contains": "Food COGS",
        "label": "Normal non-check keyword match (KRAFTSMEN BAKING)",
    },
]


def run_tests(base_url: str) -> bool:
    """Upload test CSV and verify categorization results."""
    url = f"{base_url}/upload-bank-csv"

    print("=" * 60)
    print("Check Register Integration Test")
    print("=" * 60)

    # ── Step 1: Sync check register ─────────────────────────────────
    print("\n[1/3] Syncing check register from Google Sheet...")
    sync_resp = requests.post(f"{base_url}/sync-check-register")
    if sync_resp.status_code != 200:
        print(f"  FAIL: sync returned {sync_resp.status_code}: {sync_resp.text}")
        return False
    sync_data = sync_resp.json()
    print(f"  OK: {sync_data.get('rows_synced', '?')} rows synced")

    # ── Step 2: Upload test CSV ──────────────────────────────────────
    print("\n[2/3] Uploading test CSV with check scenarios...")
    files = {"file": ("test_checks_2099.csv", io.BytesIO(TEST_CSV.encode()), "text/csv")}
    resp = requests.post(url, files=files)

    if resp.status_code != 200:
        print(f"  FAIL: upload returned {resp.status_code}: {resp.text}")
        return False

    upload_result = resp.json()
    print(f"  OK: {upload_result.get('rows_loaded', '?')} rows loaded, "
          f"batch={upload_result.get('batch_id', '?')}")

    # ── Step 3: Query back the test rows and verify ──────────────────
    print("\n[3/3] Verifying categorization of uploaded rows...")
    txn_resp = requests.get(
        f"{base_url}/api/bank-transactions",
        params={"date_from": "2099-01-01", "date_to": "2099-12-31", "limit": 50, "status": "all"},
    )
    if txn_resp.status_code != 200:
        print(f"  FAIL: transaction query returned {txn_resp.status_code}: {txn_resp.text}")
        return False

    txns = txn_resp.json().get("transactions", [])
    # Build lookup by description
    txn_map = {t["description"]: t for t in txns}

    passed = 0
    failed = 0

    for exp in EXPECTED:
        desc = exp["description"]
        txn = txn_map.get(desc)
        label = exp["label"]

        if not txn:
            print(f"\n  FAIL [{label}]")
            print(f"    Transaction '{desc}' not found in API response")
            failed += 1
            continue

        errors = []
        actual_source = txn.get("category_source", "")
        actual_vendor = txn.get("vendor_normalized", "")
        actual_cat = txn.get("category", "")

        if actual_source != exp["expect_source"]:
            errors.append(
                f"category_source: expected '{exp['expect_source']}', got '{actual_source}'"
            )
        if exp["expect_vendor_contains"].upper() not in actual_vendor.upper():
            errors.append(
                f"vendor_normalized: expected to contain '{exp['expect_vendor_contains']}', "
                f"got '{actual_vendor}'"
            )
        if exp["expect_category_contains"].upper() not in actual_cat.upper():
            errors.append(
                f"category: expected to contain '{exp['expect_category_contains']}', "
                f"got '{actual_cat}'"
            )

        if errors:
            print(f"\n  FAIL [{label}]")
            for e in errors:
                print(f"    {e}")
            failed += 1
        else:
            print(f"  PASS [{label}]")
            print(f"    category={actual_cat}, source={actual_source}, vendor={actual_vendor}")
            passed += 1

    # ── Summary ──────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    total = passed + failed
    if failed == 0:
        print(f"ALL {total} TESTS PASSED")
    else:
        print(f"{passed}/{total} passed, {failed} FAILED")
    print("=" * 60)

    # ── Cleanup: delete the 2099 test rows ───────────────────────────
    print("\nCleaning up test transactions (date=2099)...")
    deletes = [
        {"transaction_date": txn["transaction_date"],
         "description": txn["description"],
         "amount": txn["amount"]}
        for txn in txns
        if txn.get("transaction_date", "").startswith("2099")
    ]
    if deletes:
        del_resp = requests.post(
            f"{base_url}/api/bank-transactions/delete",
            json={"deletes": deletes},
        )
        if del_resp.status_code == 200:
            print(f"  Deleted {len(deletes)} test rows")
        else:
            print(f"  Warning: cleanup failed ({del_resp.status_code}): {del_resp.text}")
    else:
        print("  No test rows to clean up")

    return failed == 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test check register integration")
    parser.add_argument("--url", default=DEFAULT_URL, help="Base URL of the service")
    args = parser.parse_args()

    success = run_tests(args.url)
    sys.exit(0 if success else 1)
