"""Unit tests for comp_analytics.py — pure Python, no BQ contact."""

from datetime import date
from unittest.mock import MagicMock

import pytest

from comp_analytics import (
    AVG_TIER_1_RETAIL,
    BLENDED_TARGET_PCT,
    BLENDED_WATCH_PCT,
    BUCKET_LABELS,
    CompAnalytics,
    CompPeriod,
    CompRow,
    DISCRETIONARY_BUCKETS,
    DISCRETIONARY_TARGET_PCT,
    DISCRETIONARY_WATCH_PCT,
    ItemMismatch,
    OWNER_SKU_TO_RETAIL,
    PromoterCapResult,
    TIER_2_RETAIL_MAP,
    Tier2Movement,
    UNCATEGORIZED_ALERT_THRESHOLD,
    build_weekly_slack_message,
    classify_comp_item,
    classify_comp_reason,
    classify_comp_tab,
    classify_final,
    get_retail_price,
    last_completed_week,
    prior_week,
    trailing_window,
)


# ── classify_comp_reason ────────────────────────────────────────────

def test_classify_birthday():
    assert classify_comp_reason("Birthday Comp (100.00%)") == "programmatic_birthday"


def test_classify_owner_check():
    assert classify_comp_reason("Owner Comp-Check (100.00%)") == "owner_discretion"


def test_classify_owner_item():
    assert classify_comp_reason("Owner Comp-Item (100.00%)") == "owner_discretion"


def test_classify_manager_comp():
    assert classify_comp_reason("Manager Comp - Check (100.00%)") == "discretionary_manager"
    assert classify_comp_reason("Manager Comp - Item (100.00%)") == "discretionary_manager"


def test_classify_spillage_is_recovery():
    assert classify_comp_reason("Spillage/Food Quality - Item (100.00%)") == "recovery"


def test_classify_customer_didnt_like_is_recovery():
    assert classify_comp_reason("Customer Didn't Like Item (Comp) (100.00%)") == "recovery"


def test_classify_open_dollar_is_uncategorized():
    assert classify_comp_reason("Open $ Check") == "uncategorized_open_dollar"
    assert classify_comp_reason("Open $ Item") == "uncategorized_open_dollar"


def test_classify_open_pct_is_uncategorized():
    assert classify_comp_reason("Open % Check (30.00%)") == "uncategorized_open_pct"


def test_classify_employee_discount():
    assert classify_comp_reason("Employee Discount - Item (30.00%)") == "employee"


def test_classify_promoter_wins_over_manager():
    # If a reason names a promoter, prefer that over manager comp fallback
    assert classify_comp_reason("Promoter Comp - Afrikan Thursday") == "programmatic_promoter"


def test_classify_wycliffe_hits_standing():
    assert classify_comp_reason("Wycliffe Standing Comp") == "programmatic_standing"


def test_classify_concatenated_first_match_wins():
    # Multi-item reason string — order of _BUCKET_PATTERNS controls precedence.
    # "birthday" is first in the pattern list so it wins.
    r = "Birthday Comp (100.00%), Manager Comp - Item (100.00%)"
    assert classify_comp_reason(r) == "programmatic_birthday"


def test_classify_none_is_other():
    assert classify_comp_reason(None) == "other"
    assert classify_comp_reason("") == "other"


def test_classify_unknown_string_is_other():
    assert classify_comp_reason("Some Random Reason") == "other"


# ── CompRow auto-classifies on init ─────────────────────────────────

def test_comprow_auto_classifies_bucket():
    r = CompRow(processing_date="2026-07-25", check_id="c1", server="Ashley",
                reason="Birthday Comp (100.00%)", amount=100.0)
    assert r.bucket == "programmatic_birthday"


def test_comprow_preserves_explicit_bucket():
    r = CompRow(processing_date="2026-07-25", check_id="c1", server="Ashley",
                reason="Ambiguous", amount=100.0, bucket="server_outlier")
    assert r.bucket == "server_outlier"


# ── Date windows ────────────────────────────────────────────────────

def test_last_completed_week_ending_on_sunday():
    # Tuesday 2026-07-28 → last week = Mon 7/20 to Sun 7/26
    label, start, end = last_completed_week(today=date(2026, 7, 28))
    assert start == "2026-07-20"
    assert end == "2026-07-26"


def test_last_completed_week_on_sunday_is_this_week():
    # If today IS Sunday, last completed week is this same week
    label, start, end = last_completed_week(today=date(2026, 7, 26))
    assert start == "2026-07-20"
    assert end == "2026-07-26"


def test_prior_week_is_seven_days_before_last_week():
    _, cur_start, _ = last_completed_week(today=date(2026, 7, 28))
    label, start, end = prior_week(today=date(2026, 7, 28))
    assert end == "2026-07-19"  # day before last week's Monday
    assert start == "2026-07-13"


def test_trailing_window_ends_yesterday():
    label, start, end = trailing_window(days=30, today=date(2026, 7, 28))
    assert end == "2026-07-27"
    assert start == "2026-06-28"  # 29 days back from yesterday
    assert label == "trailing_30d"


# ── CompPeriod calculated fields ────────────────────────────────────

def test_period_total_pct_zero_when_no_sales():
    p = CompPeriod(label="w", start="2026-07-20", end="2026-07-26",
                   net_sales=0.0, total_comp=500.0)
    assert p.total_pct == 0.0


def test_period_total_pct_computes():
    p = CompPeriod(label="w", start="2026-07-20", end="2026-07-26",
                   net_sales=10_000.0, total_comp=500.0)
    assert p.total_pct == pytest.approx(5.0)


def test_period_discretionary_pct_only_counts_discretionary_buckets():
    p = CompPeriod(label="w", start="2026-07-20", end="2026-07-26",
                   net_sales=10_000.0, total_comp=800.0,
                   by_bucket={
                       "discretionary_manager": 100.0,
                       "recovery": 50.0,
                       "uncategorized_open_dollar": 25.0,
                       "programmatic_birthday": 500.0,  # NOT counted
                       "owner_discretion": 125.0,       # NOT counted
                   })
    # 100 + 50 + 25 = 175 → 1.75%
    assert p.discretionary_pct == pytest.approx(1.75)


def test_period_grade_on_target():
    p = CompPeriod(label="w", start="s", end="e", net_sales=10_000.0, total_comp=300.0)
    assert p.grade()[0] == "On Target"


def test_period_grade_watch():
    p = CompPeriod(label="w", start="s", end="e", net_sales=10_000.0, total_comp=500.0)
    assert p.grade()[0] == "Watch"


def test_period_grade_investigate():
    p = CompPeriod(label="w", start="s", end="e", net_sales=10_000.0, total_comp=700.0)
    assert p.grade()[0] == "Investigate"


def test_period_discretionary_grade_scales():
    p = CompPeriod(label="w", start="s", end="e", net_sales=10_000.0, total_comp=250.0,
                   by_bucket={"discretionary_manager": 250.0})
    # 2.5% discretionary → investigate
    assert p.discretionary_grade()[0] == "Investigate"


# ── Slack message shape ─────────────────────────────────────────────

def test_slack_message_fires_alert_when_above_watch():
    cur = CompPeriod(label="Jul 21-27", start="2026-07-21", end="2026-07-27",
                     net_sales=100_000.0, total_comp=7_000.0)
    prev = CompPeriod(label="Jul 14-20", start="2026-07-14", end="2026-07-20",
                      net_sales=100_000.0, total_comp=3_000.0)
    msg, is_alert = build_weekly_slack_message(cur, prev)
    assert is_alert is True
    assert "🔴" in msg


def test_slack_message_green_when_on_target():
    cur = CompPeriod(label="Jul 21-27", start="2026-07-21", end="2026-07-27",
                     net_sales=100_000.0, total_comp=2_500.0)
    prev = CompPeriod(label="Jul 14-20", start="2026-07-14", end="2026-07-20",
                      net_sales=100_000.0, total_comp=2_500.0)
    msg, is_alert = build_weekly_slack_message(cur, prev)
    assert is_alert is False
    assert "✅" in msg


def test_slack_message_flags_uncategorized_comps():
    cur = CompPeriod(label="Jul 21-27", start="2026-07-21", end="2026-07-27",
                     net_sales=100_000.0, total_comp=800.0,
                     by_bucket={"uncategorized_open_dollar": 400.0},
                     by_bucket_count={"uncategorized_open_dollar": 3})
    cur.flagged = [
        CompRow(processing_date="2026-07-22", check_id="c1", server="A",
                reason="Open $ Check", amount=200.0),
        CompRow(processing_date="2026-07-23", check_id="c2", server="B",
                reason="Open $ Item", amount=200.0),
    ]
    prev = CompPeriod(label="prev", start="s", end="e", net_sales=100_000.0, total_comp=800.0)
    msg, _ = build_weekly_slack_message(cur, prev)
    assert "uncategorized" in msg.lower() or "reason code" in msg.lower()


# ── CompAnalytics.compute_period with mocked BQ ─────────────────────

def test_compute_period_aggregates_bucket_totals():
    bq = MagicMock()

    # First query returns net_sales
    net_sales_row = MagicMock(net_sales=50_000.0)
    net_sales_job = MagicMock()
    net_sales_job.result.return_value = [net_sales_row]

    # Second query returns comp rows
    def make_row(dt, check, server, reason, amt):
        r = MagicMock()
        r.processing_date = dt
        r.check_id = check
        r.server = server
        r.reason_of_discount = reason
        r.amount = amt
        return r

    comp_rows = [
        make_row("2026-07-21", "c1", "Ashley", "Manager Comp - Check (100.00%)", 100.0),
        make_row("2026-07-22", "c2", "Ashley", "Birthday Comp (100.00%)", 50.0),
        make_row("2026-07-23", "c3", "Tony", "Owner Comp-Check (100.00%)", 200.0),
        make_row("2026-07-24", "c4", "Ashley", "Employee Discount - Item (30.00%)", 15.0),
        make_row("2026-07-25", "c5", "Kandy", "Open $ Check", 75.0),
    ]
    comp_job = MagicMock()
    comp_job.result.return_value = comp_rows

    # No item-level hints — reason codes drive classification here
    hints_job = MagicMock()
    hints_job.result.return_value = []

    totals_job = MagicMock()

    totals_job.result.return_value = []

    bq.query.side_effect = [net_sales_job, comp_job, hints_job, totals_job]

    analytics = CompAnalytics(bq_client=bq)
    period = analytics.compute_period("test_week", "2026-07-21", "2026-07-27")

    # Employee discount is EXCLUDED from totals
    assert period.total_comp == 100.0 + 50.0 + 200.0 + 75.0
    assert period.by_bucket["discretionary_manager"] == 100.0
    assert period.by_bucket["programmatic_birthday"] == 50.0
    assert period.by_bucket["owner_discretion"] == 200.0
    assert period.by_bucket["uncategorized_open_dollar"] == 75.0
    assert "employee" not in period.by_bucket

    # Server scoring
    assert period.by_server["Ashley"].total_comp == 150.0  # 100 + 50
    assert period.by_server["Ashley"].discretionary_comp == 100.0
    assert period.by_server["Ashley"].programmatic_comp == 50.0
    assert period.by_server["Tony"].owner_comp == 200.0


def test_compute_period_flags_uncategorized():
    bq = MagicMock()
    net_sales_row = MagicMock(net_sales=50_000.0)
    ns_job = MagicMock()
    ns_job.result.return_value = [net_sales_row]

    r = MagicMock()
    r.processing_date = "2026-07-21"
    r.check_id = "c1"
    r.server = "Ashley"
    r.reason_of_discount = "Open $ Item"
    r.amount = 100.0
    comp_job = MagicMock()
    comp_job.result.return_value = [r]

    hints_job = MagicMock()
    hints_job.result.return_value = []

    totals_job = MagicMock()

    totals_job.result.return_value = []

    bq.query.side_effect = [ns_job, comp_job, hints_job, totals_job]
    analytics = CompAnalytics(bq_client=bq)
    period = analytics.compute_period("test", "2026-07-21", "2026-07-27")
    assert len(period.flagged) == 1
    assert period.flagged[0].bucket == "uncategorized_open_dollar"


# ── Taxonomy sanity ─────────────────────────────────────────────────

def test_discretionary_buckets_contains_expected():
    assert "discretionary_manager" in DISCRETIONARY_BUCKETS
    assert "recovery" in DISCRETIONARY_BUCKETS
    assert "uncategorized_open_dollar" in DISCRETIONARY_BUCKETS
    # Programmatic + owner NOT in discretionary
    assert "programmatic_birthday" not in DISCRETIONARY_BUCKETS
    assert "owner_discretion" not in DISCRETIONARY_BUCKETS


# ── classify_comp_item — SKU prefix rules ───────────────────────────

def test_classify_item_owner_prefix():
    assert classify_comp_item("OWNER MOET ROSE") == "owner_discretion"
    assert classify_comp_item("OWNER DON REPO") == "owner_discretion"
    assert classify_comp_item("OWNER HENNESSY") == "owner_discretion"
    assert classify_comp_item("OWNER LALO") == "owner_discretion"


def test_classify_item_owner_wycliff_is_standing():
    # Wycliffe / OWNER WYCLIFF BTL is a standing comp arrangement, not owner
    assert classify_comp_item("OWNER WYCLIFF BTL") == "programmatic_standing"


def test_classify_item_thursday_prefix_is_promoter():
    assert classify_comp_item("Thursday Don Repo") == "programmatic_promoter"
    assert classify_comp_item("Thursday Anejo") == "programmatic_promoter"
    assert classify_comp_item("Thursday Steak Dinner") == "programmatic_promoter"
    assert classify_comp_item("Thursday .50 Cent Wings (6)") == "programmatic_promoter"


def test_classify_item_birthday_is_programmatic():
    assert classify_comp_item("BIRTHDAY SHOT (lemon drop)") == "programmatic_birthday"
    assert classify_comp_item("Birthday Dessert") == "programmatic_birthday"
    assert classify_comp_item("Birthday shot (repo tequila)") == "programmatic_birthday"


def test_classify_item_generic_returns_none():
    # Regular menu items don't carry the signal
    assert classify_comp_item("DON JULIO REPO") is None
    assert classify_comp_item("CHICKEN WINGS") is None
    assert classify_comp_item("Grilled Lamb Chops") is None


def test_classify_item_empty_returns_none():
    assert classify_comp_item(None) is None
    assert classify_comp_item("") is None


# ── classify_final — item wins over reason ──────────────────────────

def test_classify_final_item_beats_reason_when_present():
    # OWNER item rung as Manager Comp → owner wins
    assert classify_final(
        "Manager Comp - Item (100.00%)", "OWNER MOET ROSE"
    ) == "owner_discretion"


def test_classify_final_falls_back_to_reason_when_no_sku_signal():
    # Regular item + Manager Comp reason → discretionary_manager
    assert classify_final(
        "Manager Comp - Check (100.00%)", "DON JULIO REPO"
    ) == "discretionary_manager"


def test_classify_final_thursday_wins_over_manager():
    assert classify_final(
        "Manager Comp - Item (100.00%)", "Thursday Don Repo"
    ) == "programmatic_promoter"


def test_classify_final_birthday_beats_reason():
    assert classify_final(
        "Open $ Item", "BIRTHDAY SHOT (lemon drop)"
    ) == "programmatic_birthday"


# ── CompAnalytics with item hints applied ───────────────────────────

def test_compute_period_applies_sku_reclassification_and_flags_mismatch():
    bq = MagicMock()

    # 1) net_sales
    ns = MagicMock()
    ns.result.return_value = [MagicMock(net_sales=50_000.0)]

    # 2) check-level comps: Manager Comp reason on an OWNER item's check
    check_row = MagicMock()
    check_row.processing_date = "2026-07-25"
    check_row.check_id = "chk1"
    check_row.server = "Ashley Baines"
    check_row.reason_of_discount = "Manager Comp - Item (100.00%)"
    check_row.amount = 329.0
    comps_job = MagicMock()
    comps_job.result.return_value = [check_row]

    # 3) item-level hints: same check has OWNER MOET ROSE
    hint_row = MagicMock()
    hint_row.check_id = "chk1"
    hint_row.menu_item = "OWNER MOET ROSE"
    hint_row.amount = 329.0
    hint_row.tab_name = "Owner tab"
    hint_row.table_loc = "E12"
    hints_job = MagicMock()
    hints_job.result.return_value = [hint_row]

    totals_job = MagicMock()

    totals_job.result.return_value = []

    bq.query.side_effect = [ns, comps_job, hints_job, totals_job]

    analytics = CompAnalytics(bq_client=bq)
    period = analytics.compute_period("wk", "2026-07-20", "2026-07-26")

    # Reclassified into owner_discretion, NOT discretionary_manager
    assert period.by_bucket.get("owner_discretion") == 329.0
    assert "discretionary_manager" not in period.by_bucket
    # Mismatch tracked
    assert len(period.mismatches) == 1
    m = period.mismatches[0]
    assert m.reason_bucket == "discretionary_manager"
    assert m.item_bucket == "owner_discretion"
    assert m.top_item == "OWNER MOET ROSE"


def test_compute_period_bottle_manager_separated_from_by_server():
    bq = MagicMock()
    ns = MagicMock()
    ns.result.return_value = [MagicMock(net_sales=100_000.0)]

    bm_row = MagicMock()
    bm_row.processing_date = "2026-07-25"
    bm_row.check_id = "bmchk"
    bm_row.server = "Bottle Manager"
    bm_row.reason_of_discount = "Owner Comp-Check (100.00%)"
    bm_row.amount = 500.0

    reg_row = MagicMock()
    reg_row.processing_date = "2026-07-25"
    reg_row.check_id = "regchk"
    reg_row.server = "Ashley Baines"
    reg_row.reason_of_discount = "Manager Comp - Check (100.00%)"
    reg_row.amount = 100.0

    comps_job = MagicMock()
    comps_job.result.return_value = [bm_row, reg_row]
    hints_job = MagicMock()
    hints_job.result.return_value = []

    totals_job = MagicMock()

    totals_job.result.return_value = []

    bq.query.side_effect = [ns, comps_job, hints_job, totals_job]

    analytics = CompAnalytics(bq_client=bq)
    period = analytics.compute_period("wk", "2026-07-20", "2026-07-26")

    # Bottle Manager goes to its own bucket, not by_server
    assert "Bottle Manager" not in period.by_server
    assert period.bottle_manager.comp_count == 1
    assert period.bottle_manager.owner_comp == 500.0
    # Regular server tracked normally
    assert period.by_server["Ashley Baines"].discretionary_comp == 100.0


def test_compute_period_confirming_sku_does_not_flag_mismatch():
    """If reason code AND item SKU agree, no mismatch flag."""
    bq = MagicMock()
    ns = MagicMock()
    ns.result.return_value = [MagicMock(net_sales=50_000.0)]

    check_row = MagicMock()
    check_row.processing_date = "2026-07-25"
    check_row.check_id = "chk1"
    check_row.server = "Tiffany Loving"
    check_row.reason_of_discount = "Owner Comp-Item (100.00%)"
    check_row.amount = 200.0
    comps_job = MagicMock()
    comps_job.result.return_value = [check_row]

    hint_row = MagicMock()
    hint_row.check_id = "chk1"
    hint_row.menu_item = "OWNER LALO"
    hint_row.amount = 200.0
    hint_row.tab_name = "Lalo Tasting"
    hint_row.table_loc = None
    hints_job = MagicMock()
    hints_job.result.return_value = [hint_row]

    totals_job = MagicMock()

    totals_job.result.return_value = []

    bq.query.side_effect = [ns, comps_job, hints_job, totals_job]

    analytics = CompAnalytics(bq_client=bq)
    period = analytics.compute_period("wk", "2026-07-20", "2026-07-26")

    assert len(period.mismatches) == 0
    assert period.by_bucket.get("owner_discretion") == 200.0


def test_bucket_labels_cover_all_buckets():
    # Every bucket the classifier produces should have a label
    produced_buckets = {classify_comp_reason(r) for r in [
        "Birthday Comp (100.00%)",
        "Manager Comp - Check (100.00%)",
        "Owner Comp-Check (100.00%)",
        "Spillage/Food Quality - Item (100.00%)",
        "Open $ Item",
        "Open % Check (30.00%)",
        "Employee Discount - Item (30.00%)",
        "Promoter Comp",
        "Wycliffe Standing",
        None,
    ]}
    for b in produced_buckets:
        assert b in BUCKET_LABELS, f"missing label for bucket {b}"


# ── classify_comp_tab — tab-name routing (top precedence per policy §3) ──

def test_classify_tab_recovery_spill():
    assert classify_comp_tab("Spill") == "recovery"
    assert classify_comp_tab("Spill - Don Julio") == "recovery"


def test_classify_tab_recovery_bug_and_broke():
    assert classify_comp_tab("Bug in fries") == "recovery"
    assert classify_comp_tab("Bottle Broke - Makers") == "recovery"
    assert classify_comp_tab("Glass Broke") == "recovery"


def test_classify_tab_owner_personal():
    assert classify_comp_tab("Maurice") == "owner_discretion"
    assert classify_comp_tab("Maurice E9") == "owner_discretion"
    assert classify_comp_tab("Per Maurice") == "owner_discretion"
    assert classify_comp_tab("Eddie") == "owner_discretion"
    assert classify_comp_tab("Derwin") == "owner_discretion"
    # Case-insensitive
    assert classify_comp_tab("MAURICE") == "owner_discretion"


def test_classify_tab_owner_tasting():
    # Owner Tasting is an owner-discretionary event, not "personal"
    assert classify_comp_tab("Owner Tasting - Lalo") == "owner_discretion"
    assert classify_comp_tab("Tasting - Casamigos") == "programmatic_marketing"


def test_classify_tab_owner_tasting_beats_owner_personal():
    # "Maurice Owner Tasting" — the tasting signal should override personal
    assert classify_comp_tab("Maurice Owner Tasting") == "owner_discretion"


def test_classify_tab_vip():
    assert classify_comp_tab("VIP - Chef Torres - Industry") == "vip"
    assert classify_comp_tab("VIP Guest") == "vip"


def test_classify_tab_birthday():
    assert classify_comp_tab("Birthday - Jasmine - 84293") == "programmatic_birthday"
    assert classify_comp_tab("Bday party") == "programmatic_birthday"
    assert classify_comp_tab("Sat Bday Packages") == "programmatic_birthday"


def test_classify_tab_promoter_prefix():
    assert classify_comp_tab("Promoter - Thursday - Afrikan - Kelvin") == "programmatic_promoter"
    assert classify_comp_tab("Promo Thursday") == "programmatic_promoter"


def test_classify_tab_marketing():
    assert classify_comp_tab("Distributor - Sazerac") == "programmatic_marketing"
    assert classify_comp_tab("Tasting - Southern Glazer") == "programmatic_marketing"
    assert classify_comp_tab("Wycliffe Welcome") == "programmatic_marketing"


def test_classify_tab_sunday_bare_tiffany_via_promoter_cap():
    # Per policy §3.4 — bare "Tiffany" on a Sunday → Promoter (DAE7)
    # 2026-07-26 is a Sunday
    assert classify_comp_tab("Tiffany", "2026-07-26") == "programmatic_promoter"


def test_classify_tab_sunday_bare_tony_via_promoter_cap():
    # Per policy §3.5 — bare "Tony" on a Sunday → Promoter (Cassette)
    assert classify_comp_tab("Tony", "2026-07-26") == "programmatic_promoter"


def test_classify_tab_no_signal_returns_none():
    assert classify_comp_tab("Table 12") is None
    assert classify_comp_tab("Bar tab") is None
    assert classify_comp_tab(None) is None
    assert classify_comp_tab("") is None


# ── classify_final — precedence: Tab > SKU > Reason ──

def test_final_tab_wins_over_sku():
    # Promoter tab with OWNER item → promoter (tab), not owner (SKU)
    result = classify_final(
        reason="Manager Comp - Item (100.00%)",
        menu_item="OWNER MOET ROSE",
        tab_name="Promoter - Thursday - Afrikan - Kelvin",
    )
    assert result == "programmatic_promoter"


def test_final_tab_wins_recovery_over_owner_sku():
    # Spill tab with OWNER item — treats the specific incident as recovery
    result = classify_final(
        reason="Owner Comp-Item (100.00%)",
        menu_item="OWNER DON REPO",
        tab_name="Spill - Don Julio",
    )
    assert result == "recovery"


def test_final_sku_wins_over_reason_when_no_tab():
    # No tab signal — OWNER SKU should override generic Manager Comp reason
    result = classify_final(
        reason="Manager Comp - Check (100.00%)",
        menu_item="OWNER ACE",
        tab_name="Table 12",  # no signal
    )
    assert result == "owner_discretion"


def test_final_reason_fallback_when_no_tab_no_sku():
    # Generic item + generic reason → uses reason
    result = classify_final(
        reason="Manager Comp - Check (100.00%)",
        menu_item="Chicken Wings",
        tab_name=None,
    )
    assert result == "discretionary_manager"


def test_final_sunday_tiffany_tab_routes_to_promoter():
    # Sunday brunch daypart with bare "Tiffany" tab
    result = classify_final(
        reason="Owner Comp-Check (100.00%)",
        menu_item="BTL Clase Azul",
        tab_name="Tiffany",
        processing_date="2026-07-26",  # Sunday
    )
    assert result == "programmatic_promoter"


def test_final_dollar_prefix_sku_routes_to_owner():
    # $-prefix SKUs (Magnum cost-basis) should go to owner_discretion
    result = classify_final(
        reason="Manager Comp - Item (100.00%)",
        menu_item="$Ace BTL",
        tab_name=None,
    )
    assert result == "owner_discretion"


def test_final_bday_alias_routes_to_birthday():
    # "Bday" substring alias per policy §4
    result = classify_final(
        reason="Manager Comp - Check (100.00%)",
        menu_item="Bday Special",
        tab_name=None,
    )
    assert result == "programmatic_birthday"


# ── Money at Risk headline logic ────────────────────────────────────

def _period_with_tier2(house_comped: int, foregone: float) -> CompPeriod:
    p = CompPeriod(label="w", start="s", end="e",
                   net_sales=100_000.0, total_comp=1_000.0)
    p.tier_2_movements = {
        "OWNER ACE": Tier2Movement(
            menu_item="OWNER ACE",
            total_rings=house_comped,
            house_comped_count=house_comped,
            retail_value_moved=house_comped * 654.0,
            cost_recovered=0.0,
            foregone_revenue=foregone,
        )
    }
    return p


def test_money_at_risk_triggers_alert_when_cap_breach():
    cur = _period_with_tier2(house_comped=0, foregone=0.0)
    cur.promoter_caps = [
        PromoterCapResult(
            key="thu_afrikan", day="Thursday", event="Afrikan",
            poc="Kelvin", cap=2, cap_type="external",
            bottle_count=3, bottle_dollars=132.0,
        )
    ]
    prev = _period_with_tier2(0, 0.0)
    msg, is_alert = build_weekly_slack_message(cur, prev)
    assert is_alert is True
    assert "cap breach" in msg.lower() or "clawback" in msg.lower()


def test_money_at_risk_shows_foregone_in_headline():
    cur = _period_with_tier2(house_comped=3, foregone=1962.0)
    prev = _period_with_tier2(0, 0.0)
    msg, _ = build_weekly_slack_message(cur, prev)
    assert "MONEY AT RISK" in msg
    assert "1,962" in msg
    assert "foregone" in msg.lower()


def test_money_at_risk_uncategorized_over_threshold_triggers_alert():
    cur = CompPeriod(label="w", start="s", end="e",
                     net_sales=100_000.0, total_comp=UNCATEGORIZED_ALERT_THRESHOLD + 100)
    cur.by_bucket = {"uncategorized_open_dollar": UNCATEGORIZED_ALERT_THRESHOLD + 100}
    cur.by_bucket_count = {"uncategorized_open_dollar": 3}
    prev = CompPeriod(label="w", start="s", end="e",
                      net_sales=100_000.0, total_comp=0.0)
    msg, is_alert = build_weekly_slack_message(cur, prev)
    assert is_alert is True
    assert "uncategorized" in msg.lower()


def test_money_at_risk_clawback_uses_avg_tier_1_retail_constant():
    # Cap breach of 2 bottles external → clawback = 2 * 0.8 * AVG_TIER_1_RETAIL
    cur = _period_with_tier2(0, 0.0)
    cur.promoter_caps = [
        PromoterCapResult(
            key="thu_afrikan", day="Thursday", event="Afrikan",
            poc="Kelvin", cap=2, cap_type="external",
            bottle_count=4, bottle_dollars=800.0,  # +2 over cap
        )
    ]
    prev = _period_with_tier2(0, 0.0)
    msg, _ = build_weekly_slack_message(cur, prev)
    expected_clawback = 2 * 0.8 * AVG_TIER_1_RETAIL  # e.g. 480 at $300 avg
    assert f"{expected_clawback:,.0f}" in msg


# ── Retail price lookup ────────────────────────────────────────────

def test_get_retail_price_tier2_direct():
    assert get_retail_price("BTL Don Julio 1942") == 693.0
    assert get_retail_price("Ace BTL") == 654.0


def test_get_retail_price_owner_sku_resolves_to_retail():
    # OWNER ACE → Ace BTL retail
    assert get_retail_price("OWNER ACE") == 654.0
    # OWNER MOET ROSE → Moet Rose Nectar BTL
    assert get_retail_price("OWNER MOET ROSE") == 297.0
    # OWNER DON REPO → BTL Don Julio Repo
    assert get_retail_price("OWNER DON REPO") == 346.0


def test_get_retail_price_dollar_prefix_resolves():
    # $-prefix cost-basis SKUs → magnum retail
    assert get_retail_price("$1942 BTL") == 1309.0
    assert get_retail_price("$Ace BTL") == 1155.0


def test_get_retail_price_unknown_returns_none():
    assert get_retail_price("Chicken Wings") is None
    assert get_retail_price(None) is None
    assert get_retail_price("Unknown Bottle") is None


# ── Named constants ────────────────────────────────────────────────

def test_uncategorized_alert_threshold_is_positive():
    assert UNCATEGORIZED_ALERT_THRESHOLD > 0


def test_avg_tier_1_retail_below_tier2_cutoff():
    # Sanity: avg Tier 1 must be under the Tier 2 threshold ($500)
    assert AVG_TIER_1_RETAIL < 500.0


def test_owner_sku_to_retail_covers_common_owners():
    for sku in ("OWNER ACE", "OWNER DON REPO", "OWNER MOET ROSE",
                "OWNER KETEL ONE", "OWNER HENNESSY"):
        assert sku in OWNER_SKU_TO_RETAIL, f"missing {sku} in OWNER_SKU_TO_RETAIL"
