# tests/test_adjusted_bs_math.py
from adjusted_balance_sheet_2025 import (
    period_liabilities, taxes_payable_at_yearend, penalties_and_interest,
    implied_receipts, match_legal_order, MB_GR, MB_SALES, SALES_USE, BQ_RESULTS,
    build_balance_sheet, timely_filer_discounts, _period_balances, _is_timely_discount,
    TIMELY_FILER_DISCOUNT_RATE, TIMELY_FILER_TOLERANCE,
    probable_overpayments, OVERPAYMENT_THRESHOLD,
)


def test_period_liability_amended_supersedes_struck_original():
    liabs = period_liabilities(MB_GR)
    assert liabs["2505"] == 2564.18          # struck 21,389.68 ignored


def test_period_liability_additive_when_original_not_struck():
    liabs = period_liabilities(MB_GR)
    assert liabs["2512"] == round(701.02 + 3587.85, 2)  # both paid -> additive


def test_mb_sales_2505_uses_amended():
    liabs = period_liabilities(MB_SALES)
    assert liabs["2505"] == 3007.04


def test_taxes_payable_at_yearend_is_2511_plus_2512():
    tp = taxes_payable_at_yearend(MB_GR)
    assert tp == round(4165.52 + 701.02 + 3587.85, 2)


def test_penalties_and_interest_positive_delta():
    pi = penalties_and_interest(MB_SALES)
    assert pi["2501"] == round(5671.79 - 5354.09, 2)   # 317.70


def test_mb_gr_2508_duplicate_payment_is_overpayment_not_pi():
    """MB GR 2508 paid twice (23,216.41 + 23,246.41 vs 22,110.87 filed):
    the $24,351.95 excess is a probable Comptroller credit, not P&I."""
    over = probable_overpayments(MB_GR)
    assert over["2508"] == round(23216.41 + 23246.41 - 22110.87, 2)  # 24,351.95
    assert "2508" not in penalties_and_interest(MB_GR)


def test_mb_sales_2508_small_excess_stays_pi():
    """MB Sales 2508 excess (1,361.30 = 5% of 27,226.07) is plausible P&I."""
    assert penalties_and_interest(MB_SALES)["2508"] == round(28587.37 - 27226.07, 2)
    assert "2508" not in probable_overpayments(MB_SALES)


def test_overpayment_threshold_boundary_synthetic():
    rows = [
        ("return", "2025-02-20", "r1", "2501", "2025-01-31", 100.00, False),
        ("payment", "2025-02-20", "p1", "2501", "2025-01-31", 114.99, False),
        ("return", "2025-03-20", "r2", "2502", "2025-02-28", 100.00, False),
        ("payment", "2025-03-20", "p2", "2502", "2025-02-28", 220.00, False),
    ]
    assert penalties_and_interest(rows) == {"2501": 14.99}
    assert probable_overpayments(rows) == {"2502": 120.00}
    assert OVERPAYMENT_THRESHOLD == 0.15


def test_balance_sheet_flags_comptroller_credit():
    bs = build_balance_sheet()
    credit_flags = [f for f in bs["flags"] if "comptroller_credit" in f]
    assert len(credit_flags) == 1
    assert "24,351.95" in credit_flags[0]
    assert "Not booked as an asset" in credit_flags[0]


def test_implied_receipts_divides_by_rate():
    liabs = {"2501": 8250.0}
    assert implied_receipts(liabs, 0.0825)["2501"] == 100000.0


def test_sales_use_excludes_2024_q4_from_2025():
    liabs = period_liabilities(SALES_USE, year=2025)
    assert "244" not in liabs and "251" in liabs


# ---------------------------------------------------------------------------
# match_legal_order tests
# ---------------------------------------------------------------------------

def test_match_legal_order_real_data_not_matched_temporary_hold():
    """With real WebFile constants, no payment matches $59,175 — classified as
    temporary hold fully reversed."""
    result = match_legal_order()
    assert result["matched"] is False
    assert "temporary hold" in result["classification"].lower()
    assert "fully reversed" in result["classification"].lower()
    assert "net zero" in result["classification"].lower()


def test_match_legal_order_synthetic_single_payment_found():
    """Injecting a single payment row of exactly $59,175 on a date inside the
    window should return matched=True."""
    synthetic_row = ("payment", "2025-08-25", "SYNTH001", "2508", "2025-08-31", 59175.00, False)
    result = match_legal_order(rows=[synthetic_row])
    assert result["matched"] is True
    assert len(result["matches"]) >= 1
    assert "tax remittance" in result["classification"].lower()


def test_match_legal_order_synthetic_two_payment_combo():
    """Two payments on the same date whose sum equals $59,175 should be matched."""
    row_a = ("payment", "2025-08-25", "SYNTH002A", "2508", "2025-08-31", 30000.00, False)
    row_b = ("payment", "2025-08-25", "SYNTH002B", "2508", "2025-08-31", 29175.00, False)
    result = match_legal_order(rows=[row_a, row_b])
    assert result["matched"] is True
    assert any(len(m) >= 2 for m in result["matches"])
    assert "tax remittance" in result["classification"].lower()


def test_match_legal_order_no_match_no_reversal_security_bond():
    """With no WebFile match and no reversal rows, classification should be
    Security Bond / Other Assets."""
    # Use injected empty reversal pool so the test is independent of BQ_RESULTS.
    result = match_legal_order(rows=[], reversal_rows=[])
    assert result["matched"] is False
    assert result["matches"] == []
    assert "security bond" in result["classification"].lower()
    assert "other assets" in result["classification"].lower()


# ---------------------------------------------------------------------------
# taxes_payable_at_yearend — partial prepayment and generalization fixes
# ---------------------------------------------------------------------------

def test_taxes_payable_partial_prepayment_returns_remainder():
    """When a period has a partial payment by asof, the function must return
    liability - payment_made (not the full liability)."""
    # Synthetic rows: period 2511, liability 1000.00, partial payment 400.00 made
    # before asof, remaining payment 600.00 made after asof.
    rows = [
        ("return",  "2025-12-01", "R001", "2511", "2025-11-30", 1000.00, False),
        ("payment", "2025-12-15", "P001", "2511", "2025-11-30",  400.00, False),  # before asof
        ("payment", "2026-01-10", "P002", "2511", "2025-11-30",  600.00, False),  # after asof
    ]
    result = taxes_payable_at_yearend(rows, asof="2025-12-31")
    assert result == round(1000.00 - 400.00, 2)  # 600.00 still owed at year-end


def test_taxes_payable_full_prepayment_excluded():
    """A period fully paid before asof must contribute 0 to taxes_payable."""
    rows = [
        ("return",  "2025-11-01", "R001", "2510", "2025-10-31", 500.00, False),
        ("payment", "2025-11-15", "P001", "2510", "2025-10-31", 500.00, False),  # fully paid before asof
    ]
    result = taxes_payable_at_yearend(rows, asof="2025-12-31")
    assert result == 0.0


def test_taxes_payable_overpayment_floored_at_zero():
    """Overpayments (payment > liability) must floor at 0, not go negative."""
    rows = [
        ("return",  "2025-11-01", "R001", "2510", "2025-10-31", 500.00, False),
        ("payment", "2025-11-15", "P001", "2510", "2025-10-31", 550.00, False),  # overpayment
    ]
    result = taxes_payable_at_yearend(rows, asof="2025-12-31")
    assert result == 0.0


# ---------------------------------------------------------------------------
# build_balance_sheet tests
# ---------------------------------------------------------------------------

def test_balance_sheet_ties():
    """Balance sheet must balance: total_assets == total_liabilities + total_equity."""
    bs = build_balance_sheet()
    assert abs(bs["total_assets"] - (bs["total_liabilities"] + bs["total_equity"])) <= 0.01


def test_taxes_payable_line_is_sum_of_three_accounts():
    """taxes_payable in liabilities must equal the sum of taxes_payable_at_yearend
    across all three tax type rows, with discount_eligible=True only for SALES_USE."""
    bs = build_balance_sheet()
    expected = round(
        taxes_payable_at_yearend(MB_GR,    discount_eligible=False)
        + taxes_payable_at_yearend(MB_SALES,  discount_eligible=False)
        + taxes_payable_at_yearend(SALES_USE, discount_eligible=True),
        2,
    )
    assert bs["liabilities"]["taxes_payable"] == expected


def test_balance_sheet_has_required_keys():
    """Return dict must contain all required top-level keys."""
    bs = build_balance_sheet()
    for key in ("as_of", "basis", "assets", "liabilities", "equity",
                "total_assets", "total_liabilities", "total_equity",
                "unreconciled_difference", "flags"):
        assert key in bs, f"Missing key: {key}"


def test_balance_sheet_flags_is_nonempty_list():
    """Flags list must be present and non-empty (ACCOUNTANT INPUT placeholders exist)."""
    bs = build_balance_sheet()
    assert isinstance(bs["flags"], list)
    assert len(bs["flags"]) > 0


def test_balance_sheet_unreconciled_difference_surfaced():
    """unreconciled_difference must be a float (even if 0.0) — never hidden."""
    bs = build_balance_sheet()
    assert isinstance(bs["unreconciled_difference"], float)


# ---------------------------------------------------------------------------
# Timely-filer discount tests (Finding 1)
# ---------------------------------------------------------------------------

def test_timely_filer_discount_exact_half_percent_treated_as_settled():
    """A period with liability 1000.00 and payment 995.00 (exactly 0.5% short)
    must contribute 0 to taxes_payable when discount_eligible=True — the shortfall
    is the earned discount."""
    rows = [
        ("return",  "2025-11-01", "R001", "2510", "2025-10-31", 1000.00, False),
        ("payment", "2025-11-15", "P001", "2510", "2025-10-31",  995.00, False),
    ]
    result = taxes_payable_at_yearend(rows, asof="2025-12-31", discount_eligible=True)
    assert result == 0.0


def test_timely_filer_discount_one_percent_short_remains_payable():
    """A period with liability 1000.00 and payment 990.00 (1% short — not the
    timely-filer discount) must remain as a $10.00 payable."""
    rows = [
        ("return",  "2025-11-01", "R001", "2510", "2025-10-31", 1000.00, False),
        ("payment", "2025-11-15", "P001", "2510", "2025-10-31",  990.00, False),
    ]
    result = taxes_payable_at_yearend(rows, asof="2025-12-31")
    assert result == 10.00


def test_total_taxes_payable_equals_29457_49():
    """After recognising the S&U 2510 timely-filer discount, total taxes payable
    across all three accounts must equal $29,457.49
    (MB_GR 8,454.39 + MB_SALES 10,100.15 + S&U 10,902.95).
    discount_eligible=True only for SALES_USE (§ 151.423); MB taxes (§ 183) ineligible."""
    total = round(
        taxes_payable_at_yearend(MB_GR,    discount_eligible=False)
        + taxes_payable_at_yearend(MB_SALES,  discount_eligible=False)
        + taxes_payable_at_yearend(SALES_USE, discount_eligible=True),
        2,
    )
    assert total == 29457.49


def test_timely_filer_discounts_sales_use_returns_2510():
    """timely_filer_discounts(SALES_USE) must identify period 2510 as the only
    settled-via-discount period, with a discount amount of $29.50."""
    result = timely_filer_discounts(SALES_USE, discount_eligible=True)
    assert result == {"2510": 29.50}


# ---------------------------------------------------------------------------
# New guard tests (discount_eligible gating)
# ---------------------------------------------------------------------------

def test_timely_filer_discount_zero_payments_remains_payable():
    """A period with an exact 0.5% shortfall but ZERO payments by asof must NOT
    be treated as a discount settlement — there are no payments to earn the discount."""
    rows = [
        ("return",  "2025-11-01", "R001", "2510", "2025-10-31", 1000.00, False),
        # No payment row at all.
    ]
    result = taxes_payable_at_yearend(rows, asof="2025-12-31", discount_eligible=True)
    assert result == 1000.00


def test_taxes_payable_ineligible_exact_half_percent_not_settled():
    """When discount_eligible=False, an exact 0.5%-short period must NOT be settled
    — the shortfall remains a payable regardless of how close it is to the discount."""
    rows = [
        ("return",  "2025-11-01", "R001", "2510", "2025-10-31", 1000.00, False),
        ("payment", "2025-11-15", "P001", "2510", "2025-10-31",  995.00, False),  # exactly 0.5% short
    ]
    result = taxes_payable_at_yearend(rows, asof="2025-12-31", discount_eligible=False)
    assert result == 5.00  # shortfall kept as payable; not settled


# ---------------------------------------------------------------------------
# v2.1 — reconciled distributions, fixed-asset additions, service charge tie-out
# ---------------------------------------------------------------------------

from adjusted_balance_sheet_2025 import (
    RECLASS_2025, DISTRIBUTIONS_2025_RECONCILED, FIXED_ASSET_ADDITIONS_2025,
    SERVICE_CHARGE_2025, OFF_POS_REVENUE_2025, BEGINNING_BALANCES,
)


def test_reconciled_distributions_walk():
    """Reconciled = raw owner total - owner A-bucket spend + non-owner C items."""
    raw = abs(BQ_RESULTS["owner_distributions_2025"]["combined_owner_attributable"])
    expected = round(
        raw
        - (RECLASS_2025["owner_disc_deductible_A"] + RECLASS_2025["owner_travel_deductible_A"])
        + RECLASS_2025["non_owner_distribution_C"],
        2,
    )
    assert DISTRIBUTIONS_2025_RECONCILED == expected == 200268.79


def test_reclass_owner_categories_tie_to_bq_net_of_credits():
    """Owner-category gross debits (A + C) less credits must equal the BQ net totals."""
    disc_net = round(
        RECLASS_2025["owner_disc_deductible_A"] + RECLASS_2025["owner_disc_distribution_C"] - 690.00, 2)
    travel_net = round(
        RECLASS_2025["owner_travel_deductible_A"] + RECLASS_2025["owner_travel_distribution_C"] - 959.96, 2)
    assert disc_net == abs(BQ_RESULTS["owner_distributions_2025"]["Owner Discretionary Expenses"])
    assert travel_net == abs(BQ_RESULTS["owner_distributions_2025"]["Owner Travel"])
    assert round(690.00 + 959.96, 2) == RECLASS_2025["owner_category_credits"]


def test_balance_sheet_uses_reconciled_distributions():
    bs = build_balance_sheet()
    assert bs["equity"]["distributions_2025"] == -DISTRIBUTIONS_2025_RECONCILED


def test_gross_fixed_assets_includes_2025_additions():
    bs = build_balance_sheet()
    assert bs["assets"]["gross_fixed_assets"] == round(
        BEGINNING_BALANCES["depreciable_assets"] + FIXED_ASSET_ADDITIONS_2025, 2)


def test_service_charge_split_math():
    """Owner directive 2026-10-11: LOV3 split removed — 100% pass-through via payroll.
    The former operational split (70%/50%) is retained as an informational figure."""
    sc = SERVICE_CHARGE_2025
    assert sc["staff_share_passthrough"] == sc["gross_collected"]
    assert sc["house_retained"] == 0.0
    assert round(sc["waitstaff_base"] + sc["bottle_manager_base"], 2) == sc["gross_collected"]
    former_staff = round(0.70 * sc["waitstaff_base"] + 0.50 * sc["bottle_manager_base"], 2)
    assert abs(round(sc["gross_collected"] - former_staff, 2) - sc["former_house_split"]) < 0.02


def test_balance_sheet_flags_service_charge_tieout_and_off_pos_revenue():
    bs = build_balance_sheet()
    joined = " ".join(bs["flags"])
    assert "SERVICE CHARGE TIE-OUT" in joined
    assert "PASS-THROUGH" in joined
    assert "OFF-POS REVENUE" in joined
    total = round(sum(OFF_POS_REVENUE_2025.values()), 2)
    assert f"${total:,.2f}" in joined


# v2.3 — resolved inputs: inventory (BevSpot), cash (ledger chain), accrued payroll

def test_inventory_uses_bevspot_year_end_count():
    from adjusted_balance_sheet_2025 import INVENTORY_1231_2025
    bs = build_balance_sheet()
    assert INVENTORY_1231_2025 == 29377.00
    assert bs["assets"]["inventory"] == INVENTORY_1231_2025


def test_cash_uses_verified_closing_ledger_balance():
    from adjusted_balance_sheet_2025 import CASH_1231_2025
    bs = build_balance_sheet()
    assert CASH_1231_2025 == -473.97
    assert bs["assets"]["cash"] == CASH_1231_2025
    # 1/2/2026 opening chain: 28,775.07 - 29,249.04 = -473.97
    assert round(28775.07 - 29249.04, 2) == CASH_1231_2025


def test_accrued_payroll_components_sum():
    from adjusted_balance_sheet_2025 import (
        ACCRUED_PAYROLL_1231_2025, ACCRUED_PAYROLL_1231_2025_TOTAL)
    ap = ACCRUED_PAYROLL_1231_2025
    assert ACCRUED_PAYROLL_1231_2025_TOTAL == round(sum(ap.values()), 2) == 15084.56
    # FICA estimate = 7.65% of wages + svc charge + tips
    base = ap["hourly_wages"] + ap["service_charge_passthrough"] + ap["cc_tips_passthrough"]
    assert abs(ap["employer_fica_est"] - round(base * 0.0765, 2)) < 0.02
    bs = build_balance_sheet()
    assert bs["liabilities"]["accrued_payroll"] == ACCRUED_PAYROLL_1231_2025_TOTAL


def test_balance_sheet_still_ties_after_v23_inputs():
    bs = build_balance_sheet()
    a, l = bs["total_assets"], bs["total_liabilities"]
    assert round(a - l - bs["total_equity"], 2) == 0.0


# --- v2.4: accounts payable estimate (owner-directed 2026-10-11) ------------

def test_accounts_payable_estimate_components_sum():
    from adjusted_balance_sheet_2025 import (
        ACCOUNTS_PAYABLE_1231_2025, ACCOUNTS_PAYABLE_1231_2025_TOTAL)
    apay = ACCOUNTS_PAYABLE_1231_2025
    assert apay["derived_trade_ap"] == 19275.37
    assert apay["credit_cards_due"] == 30000.00
    assert apay["liquor_due"] == 20000.00
    assert ACCOUNTS_PAYABLE_1231_2025_TOTAL == round(sum(apay.values()), 2) == 69275.37
    bs = build_balance_sheet()
    assert bs["liabilities"]["accounts_payable"] == ACCOUNTS_PAYABLE_1231_2025_TOTAL


def test_balance_sheet_still_ties_after_v24_inputs():
    bs = build_balance_sheet()
    a, l = bs["total_assets"], bs["total_liabilities"]
    assert round(a - l - bs["total_equity"], 2) == 0.0
