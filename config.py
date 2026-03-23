import os
from typing import Any, Dict, List

PROJECT_ID = os.environ.get("GCP_PROJECT", "toast-analytics-444116")
DATASET_ID = os.environ.get("BQ_DATASET", "toast_raw")
SFTP_HOST = os.environ.get("SFTP_HOST", "s-9b0f88558b264dfda.server.transfer.us-east-1.amazonaws.com")
SFTP_PORT = int(os.environ.get("SFTP_PORT", 22))
SFTP_USER = os.environ.get("SFTP_USER", "LoveExportUser")
ALERT_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
ALERT_EMAIL = os.environ.get("ALERT_EMAIL", "maurice@lov3houston.com")
REPORT_EMAIL = os.environ.get("REPORT_EMAIL", "maurice.ragland@lov3htx.com")

# ─── LOV3 Business Assumptions ───────────────────────────────────────────────
# These constants codify the key business rules discovered during the financial
# audit (Jan-Feb 2026). All reports and analysis endpoints use these so results
# are consistent and repeatable.
#
# Business Day: LOV3 operates as a nightlife venue. Revenue generated after
# midnight (e.g. 1 AM Saturday) belongs to the prior business day (Friday).
# The cutoff is 4 AM: a "business day" runs from 4:00 AM to 3:59 AM.
BUSINESS_DAY_CUTOFF_HOUR = 4

# Gratuity: LOV3 charges 20% auto-gratuity on every check.
# The house retains 35%; 65% passes through to staff. Tips are 100% staff.
GRAT_RETAIN_PCT = 0.35
GRAT_PASSTHROUGH_PCT = 1.0 - GRAT_RETAIN_PCT

# SQL snippet: derive the business_date from a DATETIME/TIMESTAMP column.
# Usage: wrap a datetime column → DATE for the LOV3 business day.
#   e.g. f"{BUSINESS_DAY_SQL.format(dt_col='CAST(paid_date AS DATETIME)')}"
BUSINESS_DAY_SQL = (
    "DATE_SUB(DATE({dt_col}), "
    f"INTERVAL CASE WHEN EXTRACT(HOUR FROM {{dt_col}}) < {BUSINESS_DAY_CUTOFF_HOUR} "
    "THEN 1 ELSE 0 END DAY)"
)

# SQL snippet: day-of-week name for the business day.
BUSINESS_DOW_SQL = (
    "FORMAT_DATE('%A', " + BUSINESS_DAY_SQL + ")"
)

# ─── Check Register (Google Sheet) ──────────────────────────────────────────
CHECK_REGISTER_SHEET_ID = "1IAquzS-GES3A7-Cxj1ICbdg3fcSJF8BGttId-NPviIY"
CHECK_REGISTER_SHEET_NAME = "check_register_master"

# ─── LOV3 Events & Promotional Calendar ─────────────────────────────────────
# Hardcoded Houston-area events, holidays, and LOV3-specific dates.
# Used by /events dashboard and /api/events-calendar endpoint.
# Categories: holiday, conference, cultural, lov3, sports
LOV3_EVENTS = [
    # ── 2025 ──────────────────────────────────────────────────────────────────
    # Q1
    {"name": "Valentine's Day", "start_date": "2025-02-14", "end_date": "2025-02-14", "category": "holiday"},
    {"name": "NBA All-Star Weekend", "start_date": "2025-02-14", "end_date": "2025-02-16", "category": "sports"},
    {"name": "Mardi Gras / Galveston", "start_date": "2025-02-28", "end_date": "2025-03-04", "category": "cultural"},
    {"name": "Houston Rodeo", "start_date": "2025-03-04", "end_date": "2025-03-23", "category": "cultural"},
    {"name": "LOV3 Anniversary", "start_date": "2025-03-13", "end_date": "2025-03-14", "category": "lov3"},
    {"name": "Spring Break (HISD)", "start_date": "2025-03-10", "end_date": "2025-03-14", "category": "holiday"},
    # Q2
    {"name": "Easter", "start_date": "2025-04-20", "end_date": "2025-04-20", "category": "holiday"},
    {"name": "Cinco de Mayo", "start_date": "2025-05-03", "end_date": "2025-05-05", "category": "cultural"},
    {"name": "Memorial Day Weekend", "start_date": "2025-05-24", "end_date": "2025-05-26", "category": "holiday"},
    # Q3
    {"name": "Juneteenth", "start_date": "2025-06-19", "end_date": "2025-06-19", "category": "cultural"},
    {"name": "Fourth of July", "start_date": "2025-07-03", "end_date": "2025-07-05", "category": "holiday"},
    {"name": "Houston Restaurant Weeks", "start_date": "2025-08-01", "end_date": "2025-09-01", "category": "cultural"},
    {"name": "Labor Day Weekend", "start_date": "2025-08-30", "end_date": "2025-09-01", "category": "holiday"},
    # Q4
    {"name": "Afrotech", "start_date": "2025-10-29", "end_date": "2025-11-01", "category": "conference"},
    {"name": "TSU Homecoming", "start_date": "2025-10-24", "end_date": "2025-10-25", "category": "sports"},
    {"name": "Halloween", "start_date": "2025-10-31", "end_date": "2025-10-31", "category": "holiday"},
    {"name": "Thanksgiving", "start_date": "2025-11-27", "end_date": "2025-11-27", "category": "holiday"},
    {"name": "Black Friday Weekend", "start_date": "2025-11-28", "end_date": "2025-11-30", "category": "holiday"},
    {"name": "Christmas Eve / Christmas", "start_date": "2025-12-24", "end_date": "2025-12-25", "category": "holiday"},
    {"name": "New Year's Eve", "start_date": "2025-12-31", "end_date": "2025-12-31", "category": "holiday"},
    # ── 2026 ──────────────────────────────────────────────────────────────────
    # Q1
    {"name": "New Year's Day", "start_date": "2026-01-01", "end_date": "2026-01-01", "category": "holiday"},
    {"name": "Super Bowl LX", "start_date": "2026-02-08", "end_date": "2026-02-08", "category": "sports"},
    {"name": "Valentine's Day", "start_date": "2026-02-14", "end_date": "2026-02-14", "category": "holiday"},
    {"name": "Mardi Gras / Galveston", "start_date": "2026-02-13", "end_date": "2026-02-17", "category": "cultural"},
    {"name": "Houston Rodeo", "start_date": "2026-03-03", "end_date": "2026-03-22", "category": "cultural"},
    {"name": "LOV3 Anniversary", "start_date": "2026-03-13", "end_date": "2026-03-14", "category": "lov3"},
    {"name": "Spring Break (HISD)", "start_date": "2026-03-09", "end_date": "2026-03-13", "category": "holiday"},
    # Q2
    {"name": "Easter", "start_date": "2026-04-05", "end_date": "2026-04-05", "category": "holiday"},
    {"name": "Cinco de Mayo", "start_date": "2026-05-02", "end_date": "2026-05-05", "category": "cultural"},
    {"name": "Memorial Day Weekend", "start_date": "2026-05-23", "end_date": "2026-05-25", "category": "holiday"},
    # Q3
    {"name": "Juneteenth", "start_date": "2026-06-19", "end_date": "2026-06-19", "category": "cultural"},
    {"name": "Fourth of July", "start_date": "2026-07-03", "end_date": "2026-07-05", "category": "holiday"},
    {"name": "Houston Restaurant Weeks", "start_date": "2026-08-01", "end_date": "2026-09-01", "category": "cultural"},
    {"name": "Labor Day Weekend", "start_date": "2026-09-05", "end_date": "2026-09-07", "category": "holiday"},
    # Q4
    {"name": "Afrotech", "start_date": "2026-10-28", "end_date": "2026-10-31", "category": "conference"},
    {"name": "TSU Homecoming", "start_date": "2026-10-23", "end_date": "2026-10-24", "category": "sports"},
    {"name": "Halloween", "start_date": "2026-10-31", "end_date": "2026-10-31", "category": "holiday"},
    {"name": "Thanksgiving", "start_date": "2026-11-26", "end_date": "2026-11-26", "category": "holiday"},
    {"name": "Black Friday Weekend", "start_date": "2026-11-27", "end_date": "2026-11-29", "category": "holiday"},
    {"name": "Christmas Eve / Christmas", "start_date": "2026-12-24", "end_date": "2026-12-25", "category": "holiday"},
    {"name": "New Year's Eve", "start_date": "2026-12-31", "end_date": "2026-12-31", "category": "holiday"},
]

# File configurations with schema definitions
# Updated to match actual Toast SFTP CSV schemas (verified from 20240414-20260130)
FILE_CONFIGS = {
    "OrderDetails.csv": {
        "table": "OrderDetails_raw",
        "primary_key": ["order_id", "processing_date"],
        "date_columns": ["Opened", "Paid", "Closed"],
        "column_mapping": {
            "Location": "location",
            "Order Id": "order_id",
            "Order #": "order_number",
            "Checks": "checks",
            "Opened": "opened",
            "# of Guests": "guest_count",
            "Tab Names": "tab_names",
            "Server": "server",
            "Table": "table_loc",
            "Revenue Center": "revenue_center",
            "Dining Area": "dining_area",
            "Service": "service",
            "Dining Options": "dining_options",
            "Discount Amount": "discount_amount",
            "Amount": "amount",
            "Tax": "tax",
            "Tip": "tip",
            "Gratuity": "gratuity",
            "Total": "total",
            "Voided": "voided",
            "Paid": "paid",
            "Closed": "closed",
            "Duration (Opened to Paid)": "duration_opened_to_paid",
            "Order Source": "order_source"
        }
    },
    "CheckDetails.csv": {
        "table": "CheckDetails_raw",
        "primary_key": ["check_id", "processing_date"],
        "date_columns": ["Opened Date", "Opened Time"],
        "column_mapping": {
            "Customer Id": "customer_id",
            "Customer": "customer",
            "Customer Phone": "customer_phone",
            "Customer Email": "customer_email",
            "Location Code": "location_code",
            "Opened Date": "opened_date",
            "Opened Time": "opened_time",
            "Item Description": "item_description",
            "Server": "server",
            "Tax": "tax",
            "Tender": "tender",
            "Check Id": "check_id",
            "Check #": "check_number",
            "Total": "total",
            "Customer Family": "customer_family",
            "Table Size": "table_size",
            "Discount": "discount",
            "Reason of Discount": "reason_of_discount",
            "Link": "link"
        }
    },
    "PaymentDetails.csv": {
        "table": "PaymentDetails_raw",
        "primary_key": ["payment_id", "processing_date"],
        "date_columns": ["Paid Date", "Order Date", "Refund Date", "Void Date"],
        "column_mapping": {
            "Location": "location",
            "Payment Id": "payment_id",
            "Order Id": "order_id",
            "Order #": "order_number",
            "Paid Date": "paid_date",
            "Order Date": "order_date",
            "Check Id": "check_id",
            "Check #": "check_number",
            "Tab Name": "tab_name",
            "Server": "server",
            "Table": "table_loc",
            "Dining Area": "dining_area",
            "Service": "service",
            "Dining Option": "dining_option",
            "House Acct #": "house_acct_number",
            "Amount": "amount",
            "Tip": "tip",
            "Gratuity": "gratuity",
            "Total": "total",
            "Swiped Card Amount": "swiped_card_amount",
            "Keyed Card Amount": "keyed_card_amount",
            "Amount Tendered": "amount_tendered",
            "Refunded": "refunded",
            "Refund Date": "refund_date",
            "Refund Amount": "refund_amount",
            "Refund Tip Amount": "refund_tip_amount",
            "Void User": "void_user",
            "Void Approver": "void_approver",
            "Void Date": "void_date",
            "Status": "status",
            "Type": "payment_type",
            "Cash Drawer": "cash_drawer",
            "Card Type": "card_type",
            "Other Type": "other_type",
            "Email": "email",
            "Phone": "phone",
            "Last 4 Card Digits": "last_4_card_digits",
            "V/MC/D Fees": "v_mc_d_fees",
            "Room Info": "room_info",
            "Receipt": "receipt",
            "Source": "source",
            "Last 4 Gift Card Digits": "last_4_gift_card_digits",
            "First 5 Gift Card Digits": "first_5_gift_card_digits"
        }
    },
    "ItemSelectionDetails.csv": {
        "table": "ItemSelectionDetails_raw",
        "primary_key": ["item_selection_id", "processing_date"],
        "date_columns": ["Sent Date", "Order Date"],
        "column_mapping": {
            "Location": "location",
            "Order Id": "order_id",
            "Order #": "order_number",
            "Sent Date": "sent_date",
            "Order Date": "order_date",
            "Check Id": "check_id",
            "Server": "server",
            "Table": "table_loc",
            "Dining Area": "dining_area",
            "Service": "service",
            "Dining Option": "dining_option",
            "Item Selection Id": "item_selection_id",
            "Item Id": "item_id",
            "Master Id": "master_id",
            "SKU": "sku",
            "PLU": "plu",
            "Menu Item": "menu_item",
            "Menu Subgroup(s)": "menu_subgroups",
            "Menu Group": "menu_group",
            "Menu": "menu",
            "Sales Category": "sales_category",
            "Gross Price": "gross_price",
            "Discount": "discount",
            "Net Price": "net_price",
            "Qty": "qty",
            "Tax": "tax",
            "Void?": "voided",
            "Deferred": "deferred",
            "Tax Exempt": "tax_exempt",
            "Tax Inclusion Option": "tax_inclusion_option",
            "Dining Option Tax": "dining_option_tax",
            "Tab Name": "tab_name"
        }
    },
    "AllItemsReport.csv": {
        "table": "AllItemsReport_raw",
        "primary_key": ["master_id", "item_id", "processing_date"],
        "date_columns": [],
        "column_mapping": {
            "Master ID": "master_id",
            "Item ID": "item_id",
            "Parent ID": "parent_id",
            "Menu Name": "menu_name",
            "Menu Group": "menu_group",
            "Subgroup": "subgroup",
            "Menu Item": "menu_item",
            "Tags": "tags",
            "Avg Price": "avg_price",
            "Item Qty (incl voids)": "item_qty_incl_voids",
            "% of Ttl Qty (incl voids)": "pct_of_ttl_qty_incl_voids",
            "Gross Amount (incl voids)": "gross_amount_incl_voids",
            "% of Ttl Amt (incl voids)": "pct_of_ttl_amt_incl_voids",
            "Item Qty": "item_qty",
            "Gross Amount": "gross_amount",
            "Void Qty": "void_qty",
            "Void Amount": "void_amount",
            "Discount Amount": "discount_amount",
            "Net Amount": "net_amount",
            "# Orders": "num_orders",
            "% of Ttl # Orders": "pct_of_ttl_num_orders",
            "% Qty (Group)": "pct_qty_group",
            "% Qty (Menu)": "pct_qty_menu",
            "% Qty (All)": "pct_qty_all",
            "% Net Amt (Group)": "pct_net_amt_group",
            "% Net Amt (Menu)": "pct_net_amt_menu",
            "% Net Amt (All)": "pct_net_amt_all"
        }
    },
    "CashEntries.csv": {
        "table": "CashEntries_raw",
        "primary_key": ["entry_id", "processing_date"],
        "date_columns": ["Created Date"],
        "column_mapping": {
            "Location": "location",
            "Entry Id": "entry_id",
            "Created Date": "created_date",
            "Action": "action",
            "Amount": "amount",
            "Cash Drawer": "cash_drawer",
            "Payout Reason": "payout_reason",
            "No Sale Reason": "no_sale_reason",
            "Comment": "comment",
            "Employee": "employee",
            "Employee 2": "employee_2"
        }
    },
    "KitchenTimings.csv": {
        "table": "KitchenTimings_raw",
        "primary_key": ["id", "processing_date"],
        "date_columns": ["Check Opened", "Fired Date", "Fulfilled Date"],
        "column_mapping": {
            "Location": "location",
            "ID": "id",
            "Server": "server",
            "Check #": "check_number",
            "Table": "table_loc",
            "Check Opened": "check_opened",
            "Station": "station",
            "Expediter Level": "expediter_level",
            "Fired Date": "fired_date",
            "Fulfilled Date": "fulfilled_date",
            "Fulfillment Time": "fulfillment_time",
            "Fulfilled By": "fulfilled_by"
        }
    }
}

# Default category rules for restaurant expense auto-categorization
DEFAULT_CATEGORY_RULES: List[Dict[str, str]] = [
    # COGS / Food & Beverage
    {"keyword": "SYSCO", "category": "COGS/Food", "vendor_normalized": "Sysco"},
    {"keyword": "US FOODS", "category": "COGS/Food", "vendor_normalized": "US Foods"},
    {"keyword": "USFOODS", "category": "COGS/Food", "vendor_normalized": "US Foods"},
    {"keyword": "BEN E KEITH", "category": "COGS/Food", "vendor_normalized": "Ben E Keith"},
    {"keyword": "PERFORMANCE FOOD", "category": "COGS/Food", "vendor_normalized": "Performance Food Group"},
    {"keyword": "PFG", "category": "COGS/Food", "vendor_normalized": "Performance Food Group"},
    {"keyword": "RESTAURANT DEPOT", "category": "COGS/Food", "vendor_normalized": "Restaurant Depot"},
    {"keyword": "JETRO", "category": "COGS/Food", "vendor_normalized": "Jetro/Restaurant Depot"},
    {"keyword": "SAM'S CLUB", "category": "COGS/Food", "vendor_normalized": "Sam's Club"},
    {"keyword": "SAMS CLUB", "category": "COGS/Food", "vendor_normalized": "Sam's Club"},
    {"keyword": "COSTCO", "category": "COGS/Food", "vendor_normalized": "Costco"},
    {"keyword": "WALMART", "category": "COGS/Food", "vendor_normalized": "Walmart"},
    {"keyword": "HEB", "category": "COGS/Food", "vendor_normalized": "H-E-B"},
    {"keyword": "KROGER", "category": "COGS/Food", "vendor_normalized": "Kroger"},
    # COGS / Beverage & Alcohol
    {"keyword": "SOUTHERN GLAZER", "category": "COGS/Beverage", "vendor_normalized": "Southern Glazers"},
    {"keyword": "RNDC", "category": "COGS/Beverage", "vendor_normalized": "RNDC"},
    {"keyword": "REPUBLIC NATIONAL", "category": "COGS/Beverage", "vendor_normalized": "RNDC"},
    {"keyword": "BREAKTHRU", "category": "COGS/Beverage", "vendor_normalized": "Breakthru Beverage"},
    {"keyword": "SPEC'S", "category": "COGS/Beverage", "vendor_normalized": "Spec's"},
    {"keyword": "SPECS", "category": "COGS/Beverage", "vendor_normalized": "Spec's"},
    {"keyword": "TOTAL WINE", "category": "COGS/Beverage", "vendor_normalized": "Total Wine"},
    # Labor / Payroll
    {"keyword": "ADP", "category": "Labor/Payroll", "vendor_normalized": "ADP"},
    {"keyword": "GUSTO", "category": "Labor/Payroll", "vendor_normalized": "Gusto"},
    {"keyword": "PAYCHEX", "category": "Labor/Payroll", "vendor_normalized": "Paychex"},
    {"keyword": "PAYROLL", "category": "Labor/Payroll", "vendor_normalized": "Payroll"},
    # Rent / Occupancy
    {"keyword": "RENT", "category": "Rent/Occupancy", "vendor_normalized": "Rent"},
    {"keyword": "LEASE", "category": "Rent/Occupancy", "vendor_normalized": "Lease"},
    # Utilities
    {"keyword": "CENTERPOINT", "category": "Utilities", "vendor_normalized": "CenterPoint Energy"},
    {"keyword": "RELIANT", "category": "Utilities", "vendor_normalized": "Reliant Energy"},
    {"keyword": "TXU", "category": "Utilities", "vendor_normalized": "TXU Energy"},
    {"keyword": "CITY OF HOUSTON WATER", "category": "Utilities", "vendor_normalized": "City of Houston Water"},
    {"keyword": "AT&T", "category": "Utilities", "vendor_normalized": "AT&T"},
    {"keyword": "COMCAST", "category": "Utilities", "vendor_normalized": "Comcast"},
    {"keyword": "SPECTRUM", "category": "Utilities", "vendor_normalized": "Spectrum"},
    {"keyword": "VERIZON", "category": "Utilities", "vendor_normalized": "Verizon"},
    # Insurance
    {"keyword": "INSURANCE", "category": "Insurance", "vendor_normalized": "Insurance"},
    {"keyword": "STATE FARM", "category": "Insurance", "vendor_normalized": "State Farm"},
    {"keyword": "GEICO", "category": "Insurance", "vendor_normalized": "GEICO"},
    # Technology / POS
    {"keyword": "TOAST INC", "category": "5. Operating Expenses (OPEX)/POS & Technology Fees", "vendor_normalized": "Toast Platform Fee"},
    {"keyword": "SQUARE", "category": "Technology/POS", "vendor_normalized": "Square"},
    {"keyword": "CLOVER", "category": "Technology/POS", "vendor_normalized": "Clover"},
    {"keyword": "YELP", "category": "Marketing", "vendor_normalized": "Yelp"},
    {"keyword": "GOOGLE ADS", "category": "Marketing", "vendor_normalized": "Google Ads"},
    {"keyword": "META", "category": "Marketing", "vendor_normalized": "Meta/Facebook"},
    {"keyword": "FACEBOOK", "category": "Marketing", "vendor_normalized": "Meta/Facebook"},
    # Maintenance & Supplies
    {"keyword": "HOME DEPOT", "category": "Maintenance/Supplies", "vendor_normalized": "Home Depot"},
    {"keyword": "LOWES", "category": "Maintenance/Supplies", "vendor_normalized": "Lowe's"},
    {"keyword": "LOWE'S", "category": "Maintenance/Supplies", "vendor_normalized": "Lowe's"},
    {"keyword": "WEBSTAURANT", "category": "Maintenance/Supplies", "vendor_normalized": "WebstaurantStore"},
    {"keyword": "AMAZON", "category": "Maintenance/Supplies", "vendor_normalized": "Amazon"},
    # Taxes & Fees
    {"keyword": "IRS", "category": "Taxes/Fees", "vendor_normalized": "IRS"},
    {"keyword": "TEXAS COMPTROLLER", "category": "Taxes/Fees", "vendor_normalized": "TX Comptroller"},
    {"keyword": "TABC", "category": "Taxes/Fees", "vendor_normalized": "TABC"},
]

# ---------------------------------------------------------------------------
# Budget Tracker API
# ---------------------------------------------------------------------------
BUDGET_TARGETS = {
    "cogs": {
        "label": "Cost of Goods Sold",
        "target_pct": 25.0,
        "max_pct": 30.0,
        "keywords": ["cost of goods", "cogs"],
        "insight": "Negotiate supplier contracts, reduce waste, optimize portions",
    },
    "labor": {
        "label": "True Labor",
        "target_pct": 28.0,
        "max_pct": 33.0,
        "keywords": ["3. labor", "labor cost", "payroll"],
        "insight": "Optimize scheduling, reduce overtime, cross-train staff",
    },
    "marketing": {
        "label": "Marketing & Entertainment",
        "target_pct": 12.0,
        "max_pct": 16.0,
        "keywords": ["marketing", "promotions", "entertainment", "event"],
        "insight": "Track ROI per event/promoter, cut underperforming acts",
    },
    "opex": {
        "label": "Operating Expenses",
        "target_pct": 20.0,
        "max_pct": 25.0,
        "keywords": ["operating expenses", "opex"],
        "insight": "Audit security costs, renegotiate vendor contracts",
    },
    "ga": {
        "label": "General & Administrative",
        "target_pct": 0.1,
        "max_pct": 5.0,
        "keywords": ["6. general", "g&a"],
        "insight": "Watch item — owner discretionary, minimize where possible",
    },
    "facility": {
        "label": "Facility & Build-Out",
        "target_pct": 0.5,
        "max_pct": 5.0,
        "keywords": ["7. facility", "build-out", "construction"],
        "insight": "Capital spend — track against planned improvements",
    },
}

# Subcategory budget breakdown — each ties to a BUDGET_TARGETS parent.
# share_pct = this sub's expected share of the parent's target allocation.
# Derived from 6-month actuals (Sep 2025 – Feb 2026).
BUDGET_SUBCATEGORIES = {
    # ── COGS (parent target 25%: Liquor 16% + Food 6.5% + Supplies 2.5%) ──
    "food_cogs": {
        "parent": "cogs", "label": "Food COGS", "share_pct": 26.0,
        "keywords": ["food cogs"],
        "insight": "Audit food waste, renegotiate broadline distributor pricing",
    },
    "liquor_cogs": {
        "parent": "cogs", "label": "Liquor COGS", "share_pct": 64.0,
        "keywords": ["liquor cogs"],
        "insight": "Review pour costs, cut underperforming SKUs, negotiate volume discounts",
    },
    "supplies_equipment": {
        "parent": "cogs", "label": "Supplies & Equipment", "share_pct": 10.0,
        "keywords": ["supplies & equipment", "supplies & smallwares"],
        "insight": "Consolidate supply vendors, track equipment lifecycle",
    },
    # ── Labor (parent target 28% — True Labor tracked at top level only) ──
    "employee_payroll": {
        "parent": "labor", "label": "Employee Payroll", "share_pct": 0.0,
        "keywords": ["employee payroll"],
        "informational": True,
        "insight": "Component of True Labor — tracked at parent level",
    },
    "tip_passthrough": {
        "parent": "labor", "label": "Tip & Grat Pass-Through", "share_pct": 0.0,
        "keywords": ["tip pass"],
        "informational": True,
        "insight": "Component of True Labor — pass-through to staff, not controllable",
    },
    "employee_bonuses": {
        "parent": "labor", "label": "Employee Bonuses", "share_pct": 0.0,
        "fixed_target": 500.0,
        "keywords": ["bonus"],
        "insight": "Tie bonuses to performance metrics, review payout frequency",
    },
    # ── Marketing (parent target 5%) ──
    # ── Marketing (parent target 12%: based on Oct 2025–Feb 2026 actuals) ──
    "entertainment": {
        "parent": "marketing", "label": "Entertainment", "share_pct": 48.0,
        "keywords": ["entertainment"],
        "insight": "Track per-act ROI, cut underperforming talent",
    },
    "promoter_payout": {
        "parent": "marketing", "label": "Promoter Payout", "share_pct": 26.0,
        "keywords": ["promoter"],
        "insight": "Evaluate promoter ROI per event night, renegotiate flat fees",
    },
    "social_media": {
        "parent": "marketing", "label": "Social Media", "share_pct": 21.0,
        "keywords": ["social media"],
        "insight": "Measure engagement-to-revenue conversion, consolidate agencies",
    },
    "pmg_artist": {
        "parent": "marketing", "label": "PMG / Artist Booking", "share_pct": 0.0,
        "keywords": ["pmg artist", "artist booking"],
        "informational": True,
        "insight": "No budget allocated — track if reactivated",
    },
    "event_flyers": {
        "parent": "marketing", "label": "Flyers & Print", "share_pct": 3.0,
        "keywords": ["flyer", "digital ads", "print"],
        "insight": "Shift spend from print to digital for better tracking",
    },
    "event_expense": {
        "parent": "marketing", "label": "Event Expense", "share_pct": 2.0,
        "keywords": ["event expense"],
        "insight": "Audit per-event miscellaneous costs",
    },
    "ppv": {
        "parent": "marketing", "label": "Pay-Per-View", "share_pct": 0.0,
        "keywords": ["pay-per-view"],
        "informational": True,
        "insight": "No budget allocated — track if reactivated",
    },
    # ── OPEX (parent target 20%: based on Oct 2025–Feb 2026 actuals) ──
    "rent_cam": {
        "parent": "opex", "label": "Rent & CAM", "share_pct": 28.0,
        "keywords": ["rent", "cam", "property tax"],
        "insight": "Fixed cost — review lease terms at renewal",
    },
    "taxes": {
        "parent": "opex", "label": "Taxes", "share_pct": 18.0,
        "keywords": ["5. operating expenses (opex)/taxes"],
        "insight": "Verify tax filings, check for overpayments or credits",
    },
    "security": {
        "parent": "opex", "label": "Security", "share_pct": 14.5,
        "keywords": ["security"],
        "insight": "Audit staffing levels per night, renegotiate hourly rates",
    },
    "contract_labor": {
        "parent": "opex", "label": "Contract Labor", "share_pct": 3.0,
        "keywords": ["contract labor"],
        "insight": "Review contractor hours, consider converting to W-2 if cheaper",
    },
    "bussers_cleaners": {
        "parent": "opex", "label": "Bussers & Cleaners", "share_pct": 14.5,
        "keywords": ["bussers & cleaners"],
        "insight": "Track staffing agency costs, compare to in-house hiring",
    },
    "insurance": {
        "parent": "opex", "label": "Insurance", "share_pct": 8.5,
        "keywords": ["insurance"],
        "insight": "Shop policies annually, bundle for discounts",
    },
    "repairs": {
        "parent": "facility", "label": "Repairs & Maintenance", "share_pct": 3.5,
        "keywords": ["repair", "maintenance"],
        "insight": "Implement preventive maintenance schedule to reduce emergency repairs",
    },
    "cleaning": {
        "parent": "opex", "label": "Janitorial Services", "share_pct": 3.0,
        "keywords": ["janitorial services", "cleaning", "janitorial"],
        "insight": "Bid out cleaning contract, verify scope of work",
    },
    "utilities": {
        "parent": "opex", "label": "Utilities", "share_pct": 3.0,
        "keywords": ["electric", "gas", "energy"],
        "insight": "Audit HVAC scheduling, check for energy rebates",
    },
    "pos_tech": {
        "parent": "opex", "label": "POS & Tech Fees", "share_pct": 3.0,
        "keywords": ["pos", "technology fee"],
        "insight": "Review Toast plan, eliminate unused add-ons",
    },
    "software": {
        "parent": "opex", "label": "Software & Subscriptions", "share_pct": 1.0,
        "keywords": ["software", "subscription"],
        "insight": "Audit active subscriptions, cancel unused tools",
    },
    "phone_internet": {
        "parent": "opex", "label": "Phone & Internet", "share_pct": 0.5,
        "keywords": ["phone", "internet"],
        "insight": "Bundle telecom services for volume pricing",
    },
    "professional_svc": {
        "parent": "opex", "label": "Professional Services", "share_pct": 0.5,
        "keywords": ["professional service", "legal", "accounting"],
        "insight": "Review retainer agreements, get competitive bids",
    },
    "permits_licenses": {
        "parent": "opex", "label": "Permits & Licenses", "share_pct": 0.5,
        "keywords": ["permit", "license"],
        "insight": "Calendar renewal dates, budget for annual bumps",
    },
    "admin": {
        "parent": "opex", "label": "Admin & Office", "share_pct": 0.5,
        "keywords": ["admin & office"],
        "insight": "Minimal budget — watch item, flag any spend above target",
    },
    "lighting_sound": {
        "parent": "opex", "label": "Lighting & Sound", "share_pct": 0.5,
        "keywords": ["lighting", "sound", "av"],
        "insight": "Minimal budget — watch item, flag any spend above target",
    },
    "penalties": {
        "parent": "opex", "label": "Penalties & Fees", "share_pct": 0.5,
        "keywords": ["penalty", "fine", "late fee"],
        "insight": "Set up auto-pay to avoid late charges",
    },
    "bank_fees": {
        "parent": "opex", "label": "Bank Fees", "share_pct": 0.5,
        "keywords": ["bank fee", "service charge"],
        "insight": "Negotiate fee waivers with minimum balance",
    },
    # ── G&A (parent target 0.1%: watch items — any spend flags over) ──
    "owner_draws": {
        "parent": "ga", "label": "Owner Draws / Transfers", "share_pct": 20.0,
        "keywords": ["owner draws", "transfers"],
        "insight": "Owner draws — minimize to preserve working capital",
    },
    "owner_discretionary": {
        "parent": "ga", "label": "Owner Discretionary", "share_pct": 15.0,
        "keywords": ["owner discretionary"],
        "insight": "Owner discretionary spend — review monthly",
    },
    "personal_meals": {
        "parent": "ga", "label": "Personal Meals", "share_pct": 15.0,
        "keywords": ["personal meals"],
        "insight": "Owner meal charges — cap monthly spend",
    },
    "ga_transportation": {
        "parent": "ga", "label": "Transportation", "share_pct": 10.0,
        "keywords": ["6. general & administrative / corporate/transportation"],
        "insight": "Owner transportation — track frequency and cost",
    },
    "competitive_research": {
        "parent": "ga", "label": "Competitive Research", "share_pct": 10.0,
        "keywords": ["competitive research"],
        "insight": "Market research visits — budget quarterly",
    },
    "owner_travel": {
        "parent": "ga", "label": "Travel & Lodging", "share_pct": 10.0,
        "keywords": ["owner travel", "travel & entertainment", "travel & lodging"],
        "insight": "Owner travel — pre-approve trips, set per-trip limits",
    },
    "credit_card_payments": {
        "parent": "ga", "label": "Credit Card Payments", "share_pct": 12.5,
        "keywords": ["credit card payments"],
        "insight": "Credit card balance payments — reduce outstanding balances",
    },
    "ga_other": {
        "parent": "ga", "label": "Other G&A", "share_pct": 7.5,
        "keywords": ["equity injection", "non-transaction", "operating account credit", "cash withdrawal"],
        "insight": "Misc G&A — equity moves, account adjustments",
    },
    # ── Facility (parent target 0.5%: capital spend) ──
    "construction": {
        "parent": "facility", "label": "Construction Build-Out", "share_pct": 58.0,
        "keywords": ["construction", "build out"],
        "insight": "Track against planned renovation budget",
    },
    "capital_equipment": {
        "parent": "facility", "label": "Capital Equipment", "share_pct": 38.5,
        "keywords": ["capital equipment"],
        "insight": "Major equipment purchases — verify ROI and useful life",
    },
}

# Legacy — kept empty for drilldown endpoint backward compat
UNBUDGETED_SECTIONS = {}


# ── Event ROI Configuration ──────────────────────────────────────────────────
# LOV3 runs 6 recurring weekly events (Monday is dark/closed).
# Each maps to a BigQuery DAYOFWEEK number (1=Sunday ... 7=Saturday).
RECURRING_EVENTS = {
    "tuesday_bingo": {"label": "Tuesday Bingo", "dow_num": 3, "dow_name": "Tuesday"},
    "wednesday_live_music": {"label": "Wednesday Live Music", "dow_num": 4, "dow_name": "Wednesday"},
    "thursday_happiest_hour": {"label": "Thursday Happiest Hour", "dow_num": 5, "dow_name": "Thursday"},
    "friday_106": {"label": "106 & Friday", "dow_num": 6, "dow_name": "Friday"},
    "saturday_rnb": {"label": "Nothing But RNB", "dow_num": 7, "dow_name": "Saturday"},
    "sunday_brunch": {"label": "Recovery Brunch", "dow_num": 1, "dow_name": "Sunday"},
}

# Vendor → event attribution for direct costs (Tier 1).
# Key = case-insensitive substring matched against vendor_normalized.
# Value = event key (str) or list of keys (split evenly across nights).
# More specific keywords must appear BEFORE general ones (first match wins).
EVENT_VENDOR_MAP: Dict[str, Any] = {
    # --- Multi-night splits (50/50) ---
    "CHK 9439": ["friday_106", "saturday_rnb"],
    "Darryl": ["friday_106", "saturday_rnb"],
    "J&D Entertainment": ["friday_106", "saturday_rnb"],
    "Likeminds": ["tuesday_bingo", "sunday_brunch"],
    # --- Split-name: Bobby Bandz alias → Tue (MUST be before "Robert Aldrich") ---
    "Bobby Bandz": "tuesday_bingo",
    # --- Single-night mappings ---
    "Robert Aldrich": "friday_106",
    "Jermaine Williams": "thursday_happiest_hour",
    "Keith Jacobs": "thursday_happiest_hour",
    "Isaiah Moss": "thursday_happiest_hour",
    "Urban Social": "sunday_brunch",
    "Cause & Effect": "tuesday_bingo",
    "Underrated Co": "sunday_brunch",
    "Dennis White": "sunday_brunch",
    "Rosemore": "saturday_rnb",
    "UNIVERSAL ATTRACTIONS": "saturday_rnb",
    "Thomas Green": "tuesday_bingo",
    "Chelsea Watts": "friday_106",
    "Joie Chavis": "friday_106",
    "Brandon Latigue": "sunday_brunch",
    "JOE HAND": "saturday_rnb",
    "DJBOOF": "saturday_rnb",
    "Matthew Hayes": "friday_106",
    "Justin Roundtree": "friday_106",
    "The Play Hooky": "sunday_brunch",
    # "JIJO RAJAN": Mon (dark) — excluded, one-off
}

# Direct event cost categories — attributed to specific events via EVENT_VENDOR_MAP
DIRECT_EVENT_CATEGORIES = [
    "pmg artist booking", "entertainment", "promoter payout", "pay-per-view",
]

# Shared event cost categories — allocated proportionally by revenue share
SHARED_EVENT_CATEGORIES = [
    "social media marketing", "event flyers", "digital ads", "event expense",
]

# Labor allocation by day-of-week (from management staffing report)
# These represent the proportion of hourly staff labor per event night.
LABOR_DOW_PCT = {
    "tuesday_bingo": 0.065,
    "wednesday_live_music": 0.096,
    "thursday_happiest_hour": 0.15,
    "friday_106": 0.309,
    "saturday_rnb": 0.28,
    "sunday_brunch": 0.10,
}

# Fixed payroll components — management + 1099 are overhead, NOT allocated to events.
# Only the variable portion (total labor - fixed) is allocated by DOW%.
MGMT_SALARY_PER_PERIOD = 20_000   # management salaries per pay period
CONTRACTOR_1099_PER_PERIOD = 3_500  # 1099 contractor wages per pay period
PAY_PERIODS_PER_YEAR = 26           # bi-weekly payroll
FIXED_LABOR_MONTHLY = round((MGMT_SALARY_PER_PERIOD + CONTRACTOR_1099_PER_PERIOD) * PAY_PERIODS_PER_YEAR / 12, 2)

# Operational labor categories — security + contract staffing allocated by DOW%.
# These are vendor payments (no tip/grat pass-through), allocated directly.
OPERATIONAL_LABOR_CATEGORIES = [
    "security services", "contract labor",
]
