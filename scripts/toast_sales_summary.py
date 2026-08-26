#!/usr/bin/env python3
"""
Toast Sales Summary Replicator — reproduces Toast's Sales Summary UI report
from raw Orders API data, for any custom date + hour window.

Verified 2026-08-26 against the Toast web-UI export for LOV3 8/22 12-6 PM:
gross $2,032.00 · discounts $512.00 · net $1,520.00 — matched to the penny.

The critical rule: Toast's Sales Summary "Custom hours" filter picks orders
whose openedDate falls in the window, then aggregates ALL selections on
those orders (regardless of when individual items were rung up). Filtering
by sel.createdDate under-counts by ~15% for a 6-hour window because it
drops trailing items on parties that opened in-window but ordered later.

Usage:
    python scripts/toast_sales_summary.py --date 2026-08-22 --start 12:00 --end 18:00
    python scripts/toast_sales_summary.py --date 2026-08-22 --start 12:00 --end 18:00 --csv
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import requests
from dateutil import parser as dateparser

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

PROJECT_ID = "toast-analytics-444116"
TOAST_API_BASE = "https://ws-api.toasttab.com"
CT_STD_OFFSET_HOURS = -6   # CST
CT_DST_OFFSET_HOURS = -5   # CDT


def _secret(name: str) -> str:
    return subprocess.check_output(
        ["gcloud", "secrets", "versions", "access", "latest",
         "--secret", name, "--project", PROJECT_ID],
        text=True,
    ).strip()


def _is_dst_ct(d: datetime) -> bool:
    """Rough US-DST: 2nd Sunday of March through 1st Sunday of November."""
    y = d.year
    # Second Sunday of March
    march = datetime(y, 3, 1)
    dst_start = march + timedelta(days=(6 - march.weekday()) % 7 + 7)
    # First Sunday of November
    nov = datetime(y, 11, 1)
    dst_end = nov + timedelta(days=(6 - nov.weekday()) % 7)
    return dst_start <= d < dst_end


def _ct_to_utc(local: datetime) -> datetime:
    """Convert a naive Central-Time datetime to a UTC-aware datetime."""
    offset_hours = CT_DST_OFFSET_HOURS if _is_dst_ct(local) else CT_STD_OFFSET_HOURS
    return (local - timedelta(hours=offset_hours)).replace(tzinfo=timezone.utc)


def _auth() -> dict:
    r = requests.post(
        f"{TOAST_API_BASE}/authentication/v1/authentication/login",
        json={
            "clientId": _secret("toast-api-client-id"),
            "clientSecret": _secret("toast-api-client-secret"),
            "userAccessType": "TOAST_MACHINE_CLIENT",
        },
        timeout=30,
    )
    r.raise_for_status()
    token = r.json()["token"]["accessToken"]
    return {
        "Authorization": f"Bearer {token}",
        "Toast-Restaurant-External-ID": _secret("toast-restaurant-guid"),
    }


def _fetch_orders(headers: dict, business_dates: list[str]) -> list[dict]:
    """Fetch all orders for the given business dates (YYYYMMDD)."""
    all_orders: list[dict] = []
    for biz in business_dates:
        page = 1
        while True:
            r = requests.get(
                f"{TOAST_API_BASE}/orders/v2/ordersBulk",
                headers=headers,
                params={"businessDate": biz, "pageSize": 100, "page": page},
                timeout=60,
            )
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 5))
                log.warning("rate limited on businessDate=%s — sleeping %ds", biz, wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            batch = r.json()
            if not batch:
                break
            all_orders.extend(batch)
            if len(batch) < 100:
                break
            page += 1
            time.sleep(0.1)
    return all_orders


def _load_sales_category_map(headers: dict) -> dict[str, str]:
    r = requests.get(
        f"{TOAST_API_BASE}/config/v2/salesCategories",
        headers=headers, params={"pageSize": 200}, timeout=30,
    )
    r.raise_for_status()
    return {c["guid"]: c.get("name", "") for c in r.json()}


def _load_discount_map(headers: dict) -> dict[str, str]:
    """Discount GUID -> display name lookup, paginated."""
    out: dict[str, str] = {}
    page = 1
    while True:
        r = requests.get(
            f"{TOAST_API_BASE}/config/v2/discounts",
            headers=headers, params={"pageSize": 100, "page": page}, timeout=30,
        )
        if r.status_code == 404:
            break
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        for d in batch:
            out[d.get("guid", "")] = d.get("name", "")
        if len(batch) < 100:
            break
        page += 1
    return out


def _parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return dateparser.isoparse(s).astimezone(timezone.utc)
    except Exception:
        return None


def build_summary(orders: list[dict], start_utc: datetime, end_utc: datetime,
                  cat_map: dict[str, str], disc_map: dict[str, str]) -> dict[str, Any]:
    """Return a Sales-Summary-shaped dict from filtered orders."""

    # Filter orders by order.openedDate (Toast Sales Summary "Custom hours" semantics)
    in_window: list[dict] = [
        o for o in orders
        if (dt := _parse_dt(o.get("openedDate"))) and start_utc <= dt < end_utc
    ]

    # Aggregators
    net_by_cat = defaultdict(lambda: {"items": 0, "net_sales": 0.0,
                                       "discount": 0.0, "refund": 0.0,
                                       "gross_sales": 0.0, "tax": 0.0})
    net_by_hour = defaultdict(lambda: {"net_sales": 0.0, "orders": set(), "guests": 0})
    net_by_service_mode = defaultdict(lambda: {"net_sales": 0.0, "guests": 0,
                                                 "orders": set(), "payments": 0})
    net_by_dining_opt = defaultdict(lambda: {"net_sales": 0.0, "discount": 0.0,
                                              "gross_sales": 0.0, "tax": 0.0,
                                              "orders": set()})
    net_by_revenue_ctr = defaultdict(lambda: {"items": 0, "net_sales": 0.0,
                                                "discount": 0.0, "gross_sales": 0.0,
                                                "tax": 0.0})

    menu_item_discounts = defaultdict(lambda: {"count": 0, "orders": set(), "amount": 0.0})
    check_discounts = defaultdict(lambda: {"count": 0, "orders": set(), "amount": 0.0})
    service_charges = defaultdict(lambda: {"count": 0, "amount": 0.0})

    tips_total = 0.0
    tips_refunded = 0.0
    tax_taxable = 0.0
    tax_amount = 0.0

    for order in in_window:
        order_guid = order.get("guid", "")
        opened_ts = _parse_dt(order.get("openedDate"))
        # Hour bucket — convert UTC back to CT for the "Time of day" table
        opened_ct = opened_ts + timedelta(hours=CT_DST_OFFSET_HOURS if _is_dst_ct(opened_ts.replace(tzinfo=None)) else CT_STD_OFFSET_HOURS) if opened_ts else None
        hour_ct = opened_ct.hour if opened_ct else None

        n_guests = int(order.get("numberOfGuests") or 0)

        for check in order.get("checks", []) or []:
            check_guid = check.get("guid", "")
            svc_mode = check.get("serviceMode") or order.get("source") or "Unknown"
            dining_opt_ref = check.get("diningOption") or {}
            dining_opt = dining_opt_ref.get("name") or "Unknown"
            # Toast Sales Summary UI shows "Dine In" / "Bar" / etc. — the guid resolves via /config/v2/diningOptions,
            # but for our purposes the referenced name (if present) is enough. Fall back to "Unknown".

            # Payments count
            payments_count = len(check.get("payments", []) or [])
            net_by_service_mode[svc_mode]["payments"] += payments_count

            # Check-level applied discounts
            for cd in check.get("appliedDiscounts", []) or []:
                name = (cd.get("discount") or {}).get("name") or disc_map.get(
                    (cd.get("discount") or {}).get("guid", ""), cd.get("name", "Unknown")
                )
                amt = float(cd.get("discountAmount") or 0)
                check_discounts[name]["count"] += 1
                check_discounts[name]["orders"].add(order_guid)
                check_discounts[name]["amount"] += amt

            # Service charges (Toast tracks these on the check)
            for sc in check.get("appliedServiceCharges", []) or []:
                sc_ref = sc.get("serviceCharge") or {}
                name = sc_ref.get("name") or sc.get("name") or "Service Charge"
                amt = float(sc.get("chargeAmount") or 0)
                service_charges[name]["count"] += 1
                service_charges[name]["amount"] += amt

            # Payments -> tips
            for pay in check.get("payments", []) or []:
                tips_total += float(pay.get("tipAmount") or 0)
                tips_refunded += float(pay.get("refund", {}).get("tipRefundAmount") or 0) if pay.get("refund") else 0

            for sel in check.get("selections", []) or []:
                if sel.get("voided"):
                    continue
                pre = float(sel.get("preDiscountPrice") or 0)
                price = float(sel.get("price") or 0)
                discount = pre - price
                sel_tax = float(sel.get("tax") or 0)

                # Sales category
                cat_guid = (sel.get("salesCategory") or {}).get("guid", "")
                cat_name = cat_map.get(cat_guid) or "Unknown"

                b = net_by_cat[cat_name]
                b["items"] += 1
                b["net_sales"] += price
                b["discount"] += discount
                b["gross_sales"] += pre
                b["tax"] += sel_tax

                tax_amount += sel_tax
                tax_taxable += price if sel_tax > 0 else 0

                # Service mode & dining option
                net_by_service_mode[svc_mode]["net_sales"] += price
                net_by_service_mode[svc_mode]["orders"].add(order_guid)

                net_by_dining_opt[dining_opt]["net_sales"] += price
                net_by_dining_opt[dining_opt]["discount"] += discount
                net_by_dining_opt[dining_opt]["gross_sales"] += pre
                net_by_dining_opt[dining_opt]["tax"] += sel_tax
                net_by_dining_opt[dining_opt]["orders"].add(order_guid)

                # Revenue center
                rc_ref = check.get("revenueCenter") or order.get("revenueCenter") or {}
                rc_name = rc_ref.get("name") or "Unknown"
                r = net_by_revenue_ctr[rc_name]
                r["items"] += 1
                r["net_sales"] += price
                r["discount"] += discount
                r["gross_sales"] += pre
                r["tax"] += sel_tax

                # Hour bucket
                if hour_ct is not None:
                    net_by_hour[hour_ct]["net_sales"] += price
                    net_by_hour[hour_ct]["orders"].add(order_guid)

                # Item-level applied discounts
                for ad in sel.get("appliedDiscounts", []) or []:
                    name = (ad.get("discount") or {}).get("name") or disc_map.get(
                        (ad.get("discount") or {}).get("guid", ""), ad.get("name", "Unknown")
                    )
                    amt = float(ad.get("discountAmount") or 0)
                    menu_item_discounts[name]["count"] += 1
                    menu_item_discounts[name]["orders"].add(order_guid)
                    menu_item_discounts[name]["amount"] += amt

        # Guest count per hour bucket (once per order)
        if hour_ct is not None:
            net_by_hour[hour_ct]["guests"] += n_guests
        net_by_service_mode[svc_mode]["guests"] += n_guests

    # Totals
    gross_total = sum(v["gross_sales"] for v in net_by_cat.values())
    disc_total = sum(v["discount"] for v in net_by_cat.values())
    net_total = sum(v["net_sales"] for v in net_by_cat.values())

    return {
        "window_start_utc": start_utc,
        "window_end_utc": end_utc,
        "orders_in_window": len(in_window),
        "net_sales_summary": {
            "gross_sales": gross_total,
            "sales_discounts": -disc_total,
            "sales_refunds": 0.0,
            "net_sales": net_total,
        },
        "revenue_summary": {
            "net_sales": net_total,
            "gratuity": sum(v["amount"] for v in service_charges.values()),
            "tax_amount": tax_amount,
            "tips": tips_total,
            "total": net_total + sum(v["amount"] for v in service_charges.values())
                     + tax_amount + tips_total,
        },
        "sales_category_summary": dict(net_by_cat),
        "time_of_day": {h: {"net_sales": v["net_sales"], "orders": len(v["orders"]),
                            "guests": v["guests"]} for h, v in sorted(net_by_hour.items())},
        "service_mode": {m: {"net_sales": v["net_sales"], "guests": v["guests"],
                             "orders": len(v["orders"]), "payments": v["payments"]}
                         for m, v in net_by_service_mode.items()},
        "dining_options": {d: {"net_sales": v["net_sales"], "discount": v["discount"],
                                "gross_sales": v["gross_sales"], "tax": v["tax"],
                                "orders": len(v["orders"])}
                           for d, v in net_by_dining_opt.items()},
        "revenue_center": dict(net_by_revenue_ctr),
        "menu_item_discounts": {n: {"count": v["count"], "orders": len(v["orders"]),
                                     "amount": v["amount"]}
                                 for n, v in menu_item_discounts.items()},
        "check_discounts": {n: {"count": v["count"], "orders": len(v["orders"]),
                                 "amount": v["amount"]}
                            for n, v in check_discounts.items()},
        "service_charges": dict(service_charges),
        "tip_summary": {"tips_collected": tips_total,
                        "tips_refunded": tips_refunded,
                        "total_tips": tips_total - tips_refunded},
        "tax_summary": {"tax_amount": tax_amount, "taxable_amount": tax_taxable},
    }


def print_summary(s: dict) -> None:
    def fmt(x): return f"${x:>10,.2f}"

    print("\n" + "═" * 78)
    print(f"  TOAST SALES SUMMARY (API-replicated) — {s['orders_in_window']} orders in window")
    print("═" * 78)

    n = s["net_sales_summary"]
    print("\nNet sales summary")
    print(f"  Gross sales     {fmt(n['gross_sales'])}")
    print(f"  Sales discounts {fmt(n['sales_discounts'])}")
    print(f"  Sales refunds   {fmt(n['sales_refunds'])}")
    print(f"  Net sales       {fmt(n['net_sales'])}")

    r = s["revenue_summary"]
    print("\nRevenue summary")
    print(f"  Net sales   {fmt(r['net_sales'])}")
    print(f"  Gratuity    {fmt(r['gratuity'])}")
    print(f"  Tax amount  {fmt(r['tax_amount'])}")
    print(f"  Tips        {fmt(r['tips'])}")
    print(f"  Total       {fmt(r['total'])}")

    print("\nSales category summary")
    print(f"  {'Category':<15} {'Items':>6} {'Net':>12} {'Discount':>10} {'Gross':>12} {'Tax':>8}")
    for cat, v in sorted(s["sales_category_summary"].items(), key=lambda x: -x[1]["gross_sales"]):
        print(f"  {cat:<15} {v['items']:>6} {fmt(v['net_sales'])} "
              f"{fmt(v['discount'])} {fmt(v['gross_sales'])} ${v['tax']:>7,.2f}")

    print("\nTime of day")
    print(f"  {'Hour':>5} {'Net':>12} {'Orders':>7} {'Guests':>7}")
    for h in sorted(s["time_of_day"].keys()):
        v = s["time_of_day"][h]
        print(f"  {h:>5} {fmt(v['net_sales'])} {v['orders']:>7} {v['guests']:>7}")

    print("\nMenu item discounts")
    for n, v in sorted(s["menu_item_discounts"].items(), key=lambda x: -x[1]["amount"]):
        print(f"  {n:<25} count={v['count']:>3}  orders={v['orders']:>3}  amount={fmt(v['amount'])}")

    print("\nCheck discounts")
    for n, v in sorted(s["check_discounts"].items(), key=lambda x: -x[1]["amount"]):
        print(f"  {n:<25} count={v['count']:>3}  orders={v['orders']:>3}  amount={fmt(v['amount'])}")

    print("\nService charges")
    for n, v in s["service_charges"].items():
        print(f"  {n:<25} count={v['count']:>3}  amount={fmt(v['amount'])}")

    print()


def write_csv_bundle(s: dict, out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)

    with open(f"{out_dir}/net_sales_summary.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Gross sales", "Sales discounts", "Sales refunds", "Net sales"])
        n = s["net_sales_summary"]
        w.writerow([n["gross_sales"], n["sales_discounts"], n["sales_refunds"], n["net_sales"]])

    with open(f"{out_dir}/revenue_summary.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Net sales", "Gratuity", "Tax amount", "Tips", "Total"])
        r = s["revenue_summary"]
        w.writerow([r["net_sales"], r["gratuity"], r["tax_amount"], r["tips"], r["total"]])

    with open(f"{out_dir}/sales_category_summary.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Sales category", "Items", "Net sales", "Discount amount",
                    "Refund amount", "Gross sales", "Tax amount"])
        for cat, v in s["sales_category_summary"].items():
            w.writerow([cat, v["items"], v["net_sales"], v["discount"],
                        v["refund"], v["gross_sales"], v["tax"]])
        # Totals row
        n = s["net_sales_summary"]
        total_items = sum(v["items"] for v in s["sales_category_summary"].values())
        total_tax = sum(v["tax"] for v in s["sales_category_summary"].values())
        w.writerow(["Total", total_items, n["net_sales"], -n["sales_discounts"],
                    n["sales_refunds"], n["gross_sales"], total_tax])

    with open(f"{out_dir}/time_of_day.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Hour of day", "Net sales", "Total orders", "Total guests"])
        for h in sorted(s["time_of_day"]):
            v = s["time_of_day"][h]
            w.writerow([h, v["net_sales"], v["orders"], v["guests"]])

    with open(f"{out_dir}/menu_item_discounts.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Discount", "Count", "Orders", "Amount"])
        for n, v in s["menu_item_discounts"].items():
            w.writerow([n, v["count"], v["orders"], v["amount"]])

    with open(f"{out_dir}/check_discounts.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Discount", "Count", "Orders", "Amount"])
        for n, v in s["check_discounts"].items():
            w.writerow([n, v["count"], v["orders"], v["amount"]])

    with open(f"{out_dir}/service_charge_summary.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Service charge", "Count", "Amount"])
        for n, v in s["service_charges"].items():
            w.writerow([n, v["count"], v["amount"]])

    log.info("Wrote CSV bundle to %s/", out_dir)


def main():
    p = argparse.ArgumentParser(description="Replicate Toast Sales Summary from Orders API")
    p.add_argument("--date", required=True, help="Event date YYYY-MM-DD (Central Time)")
    p.add_argument("--start", required=True, help="Start time HH:MM (24h, Central Time)")
    p.add_argument("--end", required=True, help="End time HH:MM (24h, Central Time)")
    p.add_argument("--csv", action="store_true", help="Also write CSV bundle to out_dir")
    p.add_argument("--out-dir", default=None,
                   help="CSV output directory (default: toast_sales_summary_YYYYMMDD)")
    args = p.parse_args()

    # Build the CT window and convert to UTC
    d = datetime.strptime(args.date, "%Y-%m-%d")
    sh, sm = [int(x) for x in args.start.split(":")]
    eh, em = [int(x) for x in args.end.split(":")]
    start_ct = d.replace(hour=sh, minute=sm)
    end_ct = d.replace(hour=eh, minute=em)
    start_utc = _ct_to_utc(start_ct)
    end_utc = _ct_to_utc(end_ct)
    log.info("Window CT: %s → %s", start_ct, end_ct)
    log.info("Window UTC: %s → %s", start_utc.isoformat(), end_utc.isoformat())

    # Fetch orders — include the day before and after to catch business-date rollover
    biz_dates = [
        (d - timedelta(days=1)).strftime("%Y%m%d"),
        d.strftime("%Y%m%d"),
        (d + timedelta(days=1)).strftime("%Y%m%d"),
    ]
    headers = _auth()
    log.info("Loading sales category catalog + discount catalog...")
    cat_map = _load_sales_category_map(headers)
    disc_map = _load_discount_map(headers)
    log.info("  %d sales categories, %d discounts", len(cat_map), len(disc_map))

    log.info("Fetching orders for business dates: %s", biz_dates)
    orders = _fetch_orders(headers, biz_dates)
    log.info("  %d total orders", len(orders))

    summary = build_summary(orders, start_utc, end_utc, cat_map, disc_map)
    print_summary(summary)

    if args.csv:
        out = args.out_dir or f"toast_sales_summary_{args.date.replace('-','')}"
        write_csv_bundle(summary, out)


if __name__ == "__main__":
    main()
