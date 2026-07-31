"""Comp Management analytics — Phase 1.

Categorizes Toast discount/comp activity into an operator-friendly taxonomy,
computes weekly performance vs industry benchmarks, and surfaces flagged
transactions requiring leadership review.

Ties into the leadership team's draft comp policy (2026-07-29):
  - Programmatic comps (birthday, promoter, standing) — tracked against plan
  - Discretionary comps (manager, recovery) — measured vs 1.5% net-revenue target
  - Owner-discretion comps — full audit trail
  - Uncategorized "Open $/%" ring-ins — flagged for reason-code enforcement

Reads from CheckDetails_raw (check-level comps) + OrderDetails_raw (revenue base).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

from google.cloud import bigquery

from config import ALERT_WEBHOOK_URL, PROJECT_ID, DATASET_ID

logger = logging.getLogger(__name__)

# Industry benchmarks (upscale bottle-service venue) and LOV3 policy targets.
BLENDED_TARGET_PCT = 4.0             # Total comp % of gross revenue
BLENDED_WATCH_PCT = 6.0              # Alert threshold
DISCRETIONARY_TARGET_PCT = 1.5       # Combined Manager Discretionary + Uncategorized
DISCRETIONARY_WATCH_PCT = 2.0        # Alert threshold
MANAGER_DISC_TARGET_PCT = 1.0        # Manager Discretionary only, per policy
RECOVERY_TARGET_PCT = 0.5            # Recovery bucket target
UNCATEGORIZED_TARGET_DOLLARS = 0.0   # Zero target — every comp needs a code
# Uncategorized ring-ins above this trigger a red Slack alert. Policy target
# is $0; the threshold gives operational tolerance for one-off training misses.
UNCATEGORIZED_ALERT_THRESHOLD = 500.0
# Approx avg Tier 1 bottle retail — used for external promoter clawback estimates
# in the Money at Risk headline. Refresh from Toast API when menu changes.
AVG_TIER_1_RETAIL = 300.0

# Named lists per COMP_MANAGEMENT_POLICY.md
# Ashley Baines merged into MANAGERS 2026-07-30 — her duties (approving
# Manager Comp checks up to $488) align with manager authority. She's now
# held to the same peer benchmarks as Tiffany/Tony/Daja.
MANAGERS = ["Tiffany Loving", "Anthony Winn", "Dajah Bishop", "Ashley Baines"]
BAR_LEADS = []  # reserved for future junior bar-lead roles
OWNER_TAB_KEYWORDS = ["maurice", "eddie", "derwin", "per maurice", "per eddie"]

# Tier 1 bottles (eligible for promoter comps) — retail below this threshold
TIER_1_MAX_RETAIL = 500.0

# Promoter cap schedule from Section 5 of the policy doc.
# `dow`: Python weekday number (Mon=0, Sun=6)
# `daypart`: informational for name-collision disambiguation
PROMOTER_CAPS = [
    {"key": "thu_afrikan", "dow": 3, "day": "Thursday",
     "event": "Afrikan", "poc": "Kelvin", "cap": 2, "type": "external",
     # Add other Thursday POC names + historical tab patterns
     "tab_signals": ["afrikan", "kelvin", "promo thurs", "igbo",
                     "keith", "anno", "swan"]},
    {"key": "fri_106", "dow": 4, "day": "Friday",
     "event": "106 & Friday", "poc": "In-House", "cap": 3, "type": "internal",
     "tab_signals": ["106", "106 & friday", "106&friday"]},
    {"key": "sat_nbrb", "dow": 5, "day": "Saturday",
     "event": "Nothing But R&B", "poc": "In-House", "cap": 3, "type": "internal",
     "tab_signals": ["nothing but r&b", "nbrb", "r&b", "nothing but rnb"]},
    {"key": "sun_dae7", "dow": 6, "day": "Sunday", "daypart": "brunch",
     "event": "DAE7 Brunch + Karaoke", "poc": "Tiffany", "cap": 2, "type": "external",
     # Bare "tiffany" on Sunday routes here per policy §3.4
     "tab_signals": ["dae7", "brunch", "karaoke", "tiffany"]},
    {"key": "sun_cassette", "dow": 6, "day": "Sunday", "daypart": "late_night",
     "event": "Cassette Sunday", "poc": "Tony", "cap": 2, "type": "external",
     # Bare "tony" on Sunday routes here per policy §3.5
     "tab_signals": ["cassette", "casette", "tony"]},
]

# Days of the week LOV3 operates (Tue-Sun; Mon closed)
LOV3_OPERATING_DAYS = ["Tuesday", "Wednesday", "Thursday",
                      "Friday", "Saturday", "Sunday"]
_DAY_ORDER = {d: i for i, d in enumerate(LOV3_OPERATING_DAYS)}


# Retail prices — pulled from Toast /menus/v2/menus API on 2026-07-29.
# Keys are the exact Toast menu_item name. Update when menu changes.
TIER_2_RETAIL_MAP = {
    "BTL 1942 Magnum": 1309.0,
    "BTL Clase Magnum": 1309.0,
    "Ace Magnum BTL": 1155.0,
    "Moet Magnum BTL": 924.0,
    "Ace Rose BTL": 700.0,
    "BTL Clase Azul": 693.0,
    "BTL Don Julio 1942": 693.0,
    "BTL Dusse XO": 693.0,
    "Dom P": 692.0,
    "Ace BTL": 654.0,
    "Johnnie Walker Blue Label": 500.0,
    "Glenfiddich 15 BTL": 500.0,
}

# OWNER SKU → canonical retail equivalent (name in TIER_2_RETAIL_MAP or plain retail).
# Verified from Toast /menus/v2/menus on 2026-07-29.
OWNER_SKU_TO_RETAIL = {
    # Tier 2 mappings
    "OWNER ACE": "Ace BTL",
    "OWNER ACE ROSE": "Ace Rose BTL",
    "Owner Ace Rose": "Ace Rose BTL",
    "OWNER DON 1942": "BTL Don Julio 1942",
    "OWNER CLASE AZUL": "BTL Clase Azul",
    "OWNER DUSSE XO": "BTL Dusse XO",
    "$1942 BTL": "BTL 1942 Magnum",
    "$Ace BTL": "Ace Magnum BTL",
    "$Clase Azul BTL": "BTL Clase Magnum",
    # Tier 1 mappings (below $500 retail; still tracked for foregone visibility)
    "OWNER MOET ICE": "Moet Ice BTL",
    "OWNER MOET ROSE": "Moet Rose Nectar BTL",
    "OWNER DON REPO": "BTL Don Julio Repo",
    "OWNER KETEL ONE": "BTL Ketel One",
    "OWNER LALO": "Lalo blanco BTL",
    "OWNER HENNESSY": "BTL Hennessy",
    "OWNER BELLAIRE": "Bellaire Rose BTL",
    "OWNER WYCLIFF BTL": "HOUSE CHAMPAGNE BTL WYCLIFF",
    "OWNER WYCLIFF ROSE": "HOUSE CHAMPAGNE BTL WYCLIFF",
    "OWNER DON ANEJO": "BTL Don Julio Anejo",
    "OWNER DUSSE": "BTL Dusse",
    # Champagne extras discovered 2026-07-30
    "OWNER VEUVE": "Veuve Clicquot Yellow Label BTL",
    "OWNER DOM PERIGNON": "Dom P",
    "OWNER PROSECCO ROSE": "PROSECCO ROSE",
}

# Standard retail prices for non-Tier-2 items used by Owner Contribution calc.
# All verified from Toast API 2026-07-29.
_STANDARD_RETAIL_EXTRAS = {
    "Moet Ice BTL": 216.0,
    "Moet Rose Nectar BTL": 297.0,
    "BTL Don Julio Repo": 346.0,
    "BTL Don Julio Anejo": 346.0,
    "BTL Ketel One": 200.0,
    "Lalo blanco BTL": 269.0,
    "BTL Hennessy": 269.0,
    "Bellaire Rose BTL": 169.0,
    "HOUSE CHAMPAGNE BTL WYCLIFF": 80.0,
    "BTL Dusse": 269.0,
    "Veuve Clicquot Yellow Label BTL": 231.0,
    "Dom P": 692.0,
    "PROSECCO ROSE": 14.0,
}


def is_owner_sku(menu_item: Optional[str]) -> bool:
    """True if the item is an OWNER-prefixed cost-basis SKU (OWNER or $-prefix)."""
    if not menu_item:
        return False
    m = menu_item.strip()
    return m.upper().startswith("OWNER ") or m.startswith("$")


def get_retail_price(menu_item: Optional[str]) -> Optional[float]:
    """Return the canonical retail price for an item, if known.

    For OWNER SKUs, resolves to the retail equivalent via OWNER_SKU_TO_RETAIL.
    """
    if not menu_item:
        return None
    if menu_item in TIER_2_RETAIL_MAP:
        return TIER_2_RETAIL_MAP[menu_item]
    if menu_item in _STANDARD_RETAIL_EXTRAS:
        return _STANDARD_RETAIL_EXTRAS[menu_item]
    if menu_item in OWNER_SKU_TO_RETAIL:
        retail_name = OWNER_SKU_TO_RETAIL[menu_item]
        return (TIER_2_RETAIL_MAP.get(retail_name)
                or _STANDARD_RETAIL_EXTRAS.get(retail_name))
    return None

# Ordered — earlier patterns win when a concatenated reason contains multiple.
_BUCKET_PATTERNS: List[Tuple[str, str]] = [
    ("birthday", "programmatic_birthday"),
    ("promoter", "programmatic_promoter"),
    ("wycliffe", "programmatic_standing"),
    ("owner comp", "owner_discretion"),
    ("spillage", "recovery"),
    ("food quality", "recovery"),
    ("customer didn't like", "recovery"),
    ("employee discount", "employee"),
    ("manager comp", "discretionary_manager"),
    ("open $", "uncategorized_open_dollar"),
    ("open %", "uncategorized_open_pct"),
]

BUCKET_LABELS = {
    "programmatic_birthday": "Birthday",
    "programmatic_promoter": "Promoter",
    "programmatic_standing": "Standing (Wycliffe etc.)",
    "programmatic_marketing": "Marketing (Distributor/Tasting)",
    "owner_discretion": "Owner",
    "vip": "VIP Comp",
    "recovery": "Recovery (Spillage/Quality)",
    "employee": "Employee Discount",
    "discretionary_manager": "Manager Discretionary",
    "uncategorized_open_dollar": "Open $ (Unclassified)",
    "uncategorized_open_pct": "Open % (Unclassified)",
    "other": "Other",
}

# Buckets that count against the discretionary benchmark (1.5% target).
DISCRETIONARY_BUCKETS = {"discretionary_manager", "recovery",
                         "uncategorized_open_dollar", "uncategorized_open_pct"}

# Buckets excluded from all totals — payroll compensation, not a comp.
EXCLUDED_BUCKETS = {"employee"}


def classify_comp_reason(reason: Optional[str]) -> str:
    """Route a Toast reason_of_discount string to a taxonomy bucket.

    Handles concatenated multi-item reasons ("Manager Comp - Item, Manager Comp - Item")
    by matching the first substring pattern found.
    """
    if not reason:
        return "other"
    r = reason.lower()
    for needle, bucket in _BUCKET_PATTERNS:
        if needle in r:
            return bucket
    return "other"


def classify_comp_item(menu_item: Optional[str]) -> Optional[str]:
    """Return the bucket implied by a Toast menu item's SKU prefix.

    LOV3's menu carries the intent inside the SKU name:
      OWNER MOET ROSE, OWNER DON REPO, OWNER LALO → owner_discretion
      $Ace BTL, $1942 BTL, $Clase Azul BTL         → owner_discretion (magnum cost basis)
      OWNER WYCLIFF BTL                           → programmatic_standing
      Thursday Don Repo, Thursday Anejo,          → programmatic_promoter
        Thursday Lamb Chops, Thursday Steak Dinner
      BIRTHDAY SHOT (lemon drop), Birthday Dessert → programmatic_birthday
      Bday party (any 'Bday' substring)           → programmatic_birthday

    Returns None when the SKU carries no such signal — caller falls back to
    reason-code classification.
    """
    if not menu_item:
        return None
    m = menu_item.upper().strip()
    # Birthday match (BIRTHDAY substring OR "BDAY" alias per policy §4)
    if "BIRTHDAY" in m or "BDAY" in m:
        return "programmatic_birthday"
    # Wycliffe standing comp uses SKU prefix "OWNER WYCLIFF ..."
    if "WYCLIFF" in m:
        return "programmatic_standing"
    # Owner-tab SKUs — OWNER or $ prefix (both are cost-basis mechanisms)
    if m.startswith("OWNER ") or m.startswith("$"):
        return "owner_discretion"
    # Thursday promoter SKUs prefixed "Thursday " / "THURSDAY "
    if m.startswith("THURSDAY"):
        return "programmatic_promoter"
    return None


def classify_comp_tab(tab_name: Optional[str],
                      processing_date: Optional[str] = None) -> Optional[str]:
    """Return the bucket implied by a tab name, per COMP_MANAGEMENT_POLICY.md §4.

    Precedence (first match wins):
      1. Recovery signals (Spill*, Bug*, Bottle Broke*, *Broke)
      2. Owner Personal (Maurice/Eddie/Derwin variants)
      3. Owner Tasting (Owner Tasting - *, *Owner Tasting*)
      4. VIP (VIP - *)
      5. Birthday (*Birthday*, *Bday*)
      6. Promoter (Promoter *, Promo *, or named POC + weekday match)
      7. Marketing (Tasting *, Distributor *, Wycliffe *)

    Returns None when the tab carries no signal — caller falls back to SKU/reason.
    """
    if not tab_name:
        return None
    t = tab_name.strip().lower()

    # Recovery — server tab shortcuts for spillage/breakage/quality
    if (t.startswith("spill") or t.startswith("bug") or
        t.startswith("bottle broke") or "broke" in t):
        return "recovery"

    # Owner Personal — Maurice/Eddie/Derwin (case handled by lower())
    for kw in ("maurice", "eddie", "derwin"):
        if t.startswith(kw) or t.startswith(f"per {kw}"):
            # But not "Owner Tasting - Maurice" which routes below
            if "tasting" not in t:
                return "owner_discretion"

    # Owner Tasting (owner discretionary — owner hosting a tasting event)
    # Note: bare "Tasting - X" (no "Owner" prefix) is a distributor/marketing
    # tasting and routes to programmatic_marketing below.
    if "owner tasting" in t:
        return "owner_discretion"

    # VIP Comp bucket (policy §2)
    if t.startswith("vip -") or t.startswith("vip:") or t.startswith("vip "):
        return "vip"

    # Birthday
    if "birthday" in t or "bday" in t:
        return "programmatic_birthday"

    # Promoter — explicit prefix or POC name on right weekday
    if t.startswith("promoter") or t.startswith("promo "):
        return "programmatic_promoter"

    # Marketing (distributor tastings, brand tastings, Wycliffe)
    if (t.startswith("tasting") or t.startswith("distributor")
        or "wycliff" in t):
        return "programmatic_marketing"

    # POC-name based promoter matching (Sunday Tiffany/Tony bare names, etc.)
    if processing_date:
        cap = match_promoter_cap(tab_name, processing_date)
        if cap:
            return "programmatic_promoter"

    return None


def classify_final(reason: Optional[str], menu_item: Optional[str],
                   tab_name: Optional[str] = None,
                   processing_date: Optional[str] = None) -> str:
    """Resolve the final bucket. Policy precedence: Tab > SKU > Reason (§3).

    Rationale: the tab name is closest to human intent. SKU (e.g. OWNER-prefixed)
    is deterministic but can be overridden by a promoter tab. Reason code is
    manual and error-prone — used only as final fallback.
    """
    # 1. Tab name wins if it carries a signal
    tab_bucket = classify_comp_tab(tab_name, processing_date)
    if tab_bucket:
        return tab_bucket
    # 2. Item SKU signal
    item_bucket = classify_comp_item(menu_item)
    if item_bucket:
        return item_bucket
    # 3. Reason code fallback
    return classify_comp_reason(reason)


# ── Date window helpers ─────────────────────────────────────────────


def _format_week_label(start: date, end: date) -> str:
    """Return 'Jul 21-27' when same month, 'Jul 28 - Aug 3' when crossing."""
    if start.month == end.month:
        return f"{start.strftime('%b %-d')}-{end.strftime('%-d')}"
    return f"{start.strftime('%b %-d')} - {end.strftime('%b %-d')}"


def last_completed_week(today: Optional[date] = None) -> Tuple[str, str, str]:
    """(label, start, end) — the completed Mon-Sun week ending yesterday's Sunday.

    Runs the report on the "prior Monday to Sunday" — the trading week that just
    closed. LOV3 operates Tue-Sun so this covers the full trading week naturally.
    When today IS Sunday, returns the current-week Mon-Sun (Sunday has closed).
    """
    today = today or date.today()
    # Most recent Sunday (or today if today IS Sunday)
    days_since_sun = (today.weekday() + 1) % 7
    last_sun = today - timedelta(days=days_since_sun)
    last_mon = last_sun - timedelta(days=6)
    return (_format_week_label(last_mon, last_sun),
            last_mon.isoformat(), last_sun.isoformat())


def prior_week(today: Optional[date] = None) -> Tuple[str, str, str]:
    """The week BEFORE last_completed_week, for WoW comparison."""
    _, cur_start, _ = last_completed_week(today)
    cur_start_d = date.fromisoformat(cur_start)
    prior_end = cur_start_d - timedelta(days=1)
    prior_start = prior_end - timedelta(days=6)
    return (_format_week_label(prior_start, prior_end),
            prior_start.isoformat(), prior_end.isoformat())


def trailing_window(days: int, today: Optional[date] = None) -> Tuple[str, str, str]:
    """(label, start, end) for the trailing N days ending yesterday."""
    today = today or date.today()
    end = today - timedelta(days=1)
    start = end - timedelta(days=days - 1)
    return (f"trailing_{days}d", start.isoformat(), end.isoformat())


# ── Dataclasses ─────────────────────────────────────────────────────


@dataclass
class CompRow:
    """One check's comp record from BigQuery."""
    processing_date: str
    check_id: str
    server: Optional[str]
    reason: Optional[str]
    amount: float
    bucket: str = ""

    def __post_init__(self) -> None:
        if not self.bucket:
            self.bucket = classify_comp_reason(self.reason)


@dataclass
class ServerScore:
    server: str
    total_comp: float = 0.0
    comp_count: int = 0
    discretionary_comp: float = 0.0
    programmatic_comp: float = 0.0
    owner_comp: float = 0.0
    recovery_comp: float = 0.0

    @property
    def avg_comp(self) -> float:
        return self.total_comp / self.comp_count if self.comp_count else 0.0


@dataclass
class ItemMismatch:
    """A check where item SKU signals a different bucket than the reason code."""
    check_id: str
    server: Optional[str]
    reason_bucket: str
    item_bucket: str
    top_item: str
    amount: float


@dataclass
class ItemComp:
    """One line-item comp with full metadata for the item-level log."""
    processing_date: str
    day_of_week: str
    check_id: str
    server: Optional[str]
    tab_name: Optional[str]
    table_loc: Optional[str]
    menu_item: str
    sales_category: Optional[str]
    gross_price: float
    discount: float
    bucket: str = ""
    reason: Optional[str] = None
    is_tier_2: bool = False
    protocol_flag: Optional[str] = None


@dataclass
class DailyTotal:
    """One day's comp totals (Tue-Sun)."""
    day: str  # 'Tuesday' etc.
    date_iso: str
    total: float = 0.0
    count: int = 0
    by_bucket: Dict[str, float] = field(default_factory=dict)


@dataclass
class PromoterCapResult:
    """Cap enforcement outcome for one promoter event in the period."""
    key: str
    day: str
    event: str
    poc: str
    cap: int
    cap_type: str  # "external" or "internal"
    bottle_count: int = 0
    bottle_dollars: float = 0.0
    tier_2_count: int = 0
    tier_2_dollars: float = 0.0
    sample_items: List[str] = field(default_factory=list)

    @property
    def is_over_cap(self) -> bool:
        return self.bottle_count > self.cap

    @property
    def excess_bottles(self) -> int:
        return max(0, self.bottle_count - self.cap)

    @property
    def status_icon(self) -> str:
        if self.tier_2_count > 0:
            return "🚨"
        if self.is_over_cap:
            return "🔴"
        return "🟢"


@dataclass
class OwnerLog:
    """Per-owner comp footprint (Maurice/Eddie/Derwin)."""
    name: str
    tab_count: int = 0
    total: float = 0.0
    sample_items: List[str] = field(default_factory=list)


@dataclass
class ManagerScore:
    """Scorecard row for a named manager (or bar lead)."""
    name: str
    role: str  # "Manager" | "Bar Lead"
    comp_count: int = 0
    total_comp: float = 0.0
    discretionary_comp: float = 0.0
    recovery_comp: float = 0.0
    owner_comp_rung: float = 0.0
    programmatic_comp: float = 0.0
    reason_code_diversity: int = 0  # distinct buckets touched


@dataclass
class Tier2Movement:
    """Aggregated Tier 2 bottle activity per SKU (comped OR owner-paid)."""
    menu_item: str
    total_rings: int = 0
    owner_paid_count: int = 0      # check_total > 0 AND on owner tab
    house_comped_count: int = 0    # check_total == 0
    other_paid_count: int = 0      # check_total > 0, NOT on owner tab
    cost_recovered: float = 0.0    # sum of gross_price for owner_paid checks
    retail_value_moved: float = 0.0  # total_rings × retail_price
    foregone_revenue: float = 0.0  # retail_value_moved - cost_recovered - retail_paid_by_others


@dataclass
class OwnerContribution:
    """Per-owner ledger of bottle activity attributed to their tab."""
    owner_name: str
    paid_by_owner: float = 0.0            # sum of check totals collected on owner's tab (bottles only)
    house_comped_at_cost: float = 0.0     # sum of cost-basis SKU gross_price when check total = 0
    house_comped_retail_estimate: float = 0.0  # estimated retail value of house-comped items
    tabs_paid: int = 0
    tabs_house_comped: int = 0
    items_paid: List[str] = field(default_factory=list)
    items_house_comped: List[str] = field(default_factory=list)


@dataclass
class CompPeriod:
    """One period's comp performance."""
    label: str
    start: str
    end: str
    net_sales: float = 0.0
    total_comp: float = 0.0
    by_bucket: Dict[str, float] = field(default_factory=dict)
    by_bucket_count: Dict[str, int] = field(default_factory=dict)
    by_server: Dict[str, ServerScore] = field(default_factory=dict)
    flagged: List[CompRow] = field(default_factory=list)
    # Bottle Manager tracked separately — it's a pooling station, not a person.
    bottle_manager: ServerScore = field(
        default_factory=lambda: ServerScore(server="Bottle Manager")
    )
    # Item SKU disagreements with reason codes (OWNER SKU rung as Manager Comp, etc.)
    mismatches: List[ItemMismatch] = field(default_factory=list)
    # Full item-level log for the period
    item_log: List[ItemComp] = field(default_factory=list)
    # Day-of-week rollup
    by_day: Dict[str, DailyTotal] = field(default_factory=dict)
    # Top comped items — populated by aggregation pass
    top_items: List[Tuple[str, int, float]] = field(default_factory=list)
    # Promoter cap enforcement per event
    promoter_caps: List[PromoterCapResult] = field(default_factory=list)
    # Per-owner personal comp log
    owner_logs: Dict[str, OwnerLog] = field(default_factory=dict)
    # Named manager + bar lead scorecards (Ashley, Tiffany, Tony, Daja)
    named_scorecards: Dict[str, ManagerScore] = field(default_factory=dict)
    # Tier 2 bottle movements (Ace, 1942, Clase Azul, Cristal, etc.) — comped or paid
    tier_2_movements: Dict[str, Tier2Movement] = field(default_factory=dict)
    # Per-owner contribution ledger (Maurice, Eddie, Derwin)
    owner_contributions: Dict[str, OwnerContribution] = field(default_factory=dict)

    @property
    def total_pct(self) -> float:
        return 100.0 * self.total_comp / self.net_sales if self.net_sales else 0.0

    @property
    def discretionary_total(self) -> float:
        return sum(self.by_bucket.get(b, 0.0) for b in DISCRETIONARY_BUCKETS)

    @property
    def discretionary_pct(self) -> float:
        return 100.0 * self.discretionary_total / self.net_sales if self.net_sales else 0.0

    @property
    def programmatic_total(self) -> float:
        return sum(
            v for k, v in self.by_bucket.items()
            if k.startswith("programmatic") or k == "owner_discretion"
        )

    def grade(self) -> Tuple[str, str]:
        p = self.total_pct
        if p < BLENDED_TARGET_PCT:
            return ("On Target", "grade-excellent")
        if p < BLENDED_WATCH_PCT:
            return ("Watch", "grade-warn")
        return ("Investigate", "grade-alert")

    def discretionary_grade(self) -> Tuple[str, str]:
        p = self.discretionary_pct
        # Policy target "≤1.5%" — inclusive boundary
        if p <= DISCRETIONARY_TARGET_PCT:
            return ("On Target", "grade-excellent")
        if p <= DISCRETIONARY_WATCH_PCT:
            return ("Watch", "grade-warn")
        return ("Investigate", "grade-alert")

    @property
    def recovery_pct(self) -> float:
        recovery = self.by_bucket.get("recovery", 0.0)
        return 100.0 * recovery / self.net_sales if self.net_sales else 0.0

    @property
    def manager_disc_pct(self) -> float:
        m = self.by_bucket.get("discretionary_manager", 0.0)
        return 100.0 * m / self.net_sales if self.net_sales else 0.0

    @property
    def uncategorized_dollars(self) -> float:
        return (self.by_bucket.get("uncategorized_open_dollar", 0.0)
                + self.by_bucket.get("uncategorized_open_pct", 0.0))

    def recovery_grade(self) -> Tuple[str, str]:
        p = self.recovery_pct
        if p <= RECOVERY_TARGET_PCT:
            return ("On Target", "grade-excellent")
        if p <= RECOVERY_TARGET_PCT * 2:
            return ("Watch", "grade-warn")
        return ("Investigate", "grade-alert")

    def manager_disc_grade(self) -> Tuple[str, str]:
        p = self.manager_disc_pct
        if p <= MANAGER_DISC_TARGET_PCT:
            return ("On Target", "grade-excellent")
        if p <= MANAGER_DISC_TARGET_PCT * 1.5:
            return ("Watch", "grade-warn")
        return ("Investigate", "grade-alert")


# ── Tier 2 detection + helpers ──────────────────────────────────────

_TIER_2_KEYWORDS = (
    "1942", "clase azul", "ace of spades", "cristal", "louis xiii",
    "louis roederer", "moet ice", "chandon reserve",
)


def is_tier_2_item(menu_item: Optional[str]) -> bool:
    """Return True if the item name matches a known Tier 2 (≥$500 retail) bottle.

    OWNER-prefixed variants are treated as cost-basis rings and NOT flagged
    (they're the legitimate mechanism for owner-contributed Tier 2 bottles).
    """
    if not menu_item:
        return False
    m = menu_item.upper()
    if m.startswith("OWNER "):
        return False
    m_low = menu_item.lower()
    return any(kw in m_low for kw in _TIER_2_KEYWORDS)


def is_bottle(menu_item: Optional[str], sales_category: Optional[str],
              gross_price: float) -> bool:
    """Best-effort bottle detection for promoter cap counting.

    A bottle-like comp: Liquor category AND (item name has BTL or OWNER prefix
    OR gross price >= $100).
    """
    if not menu_item:
        return False
    cat = (sales_category or "").lower()
    if "liquor" not in cat and "champagne" not in cat:
        return False
    m = menu_item.upper()
    if m.startswith("BTL ") or " BTL" in m or m.startswith("OWNER ") or m.startswith("THURSDAY "):
        return True
    return gross_price >= 100.0


def match_promoter_cap(tab_name: Optional[str], processing_date: str) -> Optional[Dict]:
    """Match a comped item to a promoter cap event via tab-name signals + weekday.

    Returns the PROMOTER_CAPS entry that matches, or None.
    """
    if not tab_name:
        return None
    tab_low = tab_name.lower()
    try:
        d = date.fromisoformat(processing_date)
    except (ValueError, TypeError):
        return None
    dow = d.weekday()
    for cap in PROMOTER_CAPS:
        if cap["dow"] != dow:
            continue
        if any(sig in tab_low for sig in cap["tab_signals"]):
            return cap
    return None


def match_owner_from_tab(tab_name: Optional[str]) -> Optional[str]:
    """Return canonical owner name if the tab belongs to one of the owners."""
    if not tab_name:
        return None
    tab_low = tab_name.lower().strip()
    for kw in OWNER_TAB_KEYWORDS:
        if tab_low.startswith(kw) or f" {kw}" in f" {tab_low}":
            if "maurice" in kw:
                return "Maurice"
            if "eddie" in kw:
                return "Eddie"
            if "derwin" in kw:
                return "Derwin"
    return None


# ── BQ queries ──────────────────────────────────────────────────────


class CompAnalytics:
    """Compute comp performance from Toast POS raw tables."""

    def __init__(self, bq_client: Optional[bigquery.Client] = None) -> None:
        self.bq = bq_client or bigquery.Client(project=PROJECT_ID)

    def _fetch_net_sales(self, start: str, end: str) -> float:
        q = f"""
        SELECT COALESCE(SUM(SAFE_CAST(amount AS FLOAT64)), 0.0) AS net_sales
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN @start AND @end
        """
        job = self.bq.query(q, job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start", "DATE", start),
                bigquery.ScalarQueryParameter("end", "DATE", end),
            ]
        ))
        for row in job.result():
            return float(row.net_sales or 0.0)
        return 0.0

    def _fetch_comps(self, start: str, end: str) -> List[CompRow]:
        q = f"""
        SELECT
          processing_date,
          check_id,
          server,
          reason_of_discount,
          SAFE_CAST(discount AS FLOAT64) AS amount
        FROM `{PROJECT_ID}.{DATASET_ID}.CheckDetails_raw`
        WHERE processing_date BETWEEN @start AND @end
          AND SAFE_CAST(discount AS FLOAT64) > 0
        """
        job = self.bq.query(q, job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start", "DATE", start),
                bigquery.ScalarQueryParameter("end", "DATE", end),
            ]
        ))
        return [
            CompRow(
                processing_date=str(row.processing_date),
                check_id=str(row.check_id) if row.check_id else "",
                server=row.server,
                reason=row.reason_of_discount,
                amount=float(row.amount or 0.0),
            )
            for row in job.result()
        ]

    def _fetch_check_totals(self, start: str, end: str) -> Dict[str, float]:
        """Return {check_id: paid_amount_excluding_tax_and_tip}.

        Definition of "collected":
          - If PaymentDetails has any row for the check → sum of PaymentDetails.amount
            (the sale amount actually collected, excl. tip/gratuity).
          - If NO PaymentDetails row → 0 (conservative). This treats open checks
            AND legitimately house-comped checks the same way. Open checks are
            rare in weekly reporting since shifts close by end of day; ad-hoc
            reports on the previous night would still see them as house-comped,
            which is the safer classification (avoids false-positive "owner paid").
        """
        q = f"""
        WITH paid AS (
          SELECT CAST(check_id AS STRING) AS check_id,
                 SUM(SAFE_CAST(amount AS FLOAT64)) AS paid
          FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
          WHERE processing_date BETWEEN @start AND @end
          GROUP BY check_id
        ),
        cd AS (
          SELECT CAST(check_id AS STRING) AS check_id
          FROM `{PROJECT_ID}.{DATASET_ID}.CheckDetails_raw`
          WHERE processing_date BETWEEN @start AND @end
        )
        SELECT cd.check_id, COALESCE(paid.paid, 0.0) AS collected
        FROM cd LEFT JOIN paid USING (check_id)
        """
        job = self.bq.query(q, job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start", "DATE", start),
                bigquery.ScalarQueryParameter("end", "DATE", end),
            ]
        ))
        return {row.check_id: float(row.collected or 0.0) for row in job.result()}

    def _fetch_item_log(self, start: str, end: str) -> List[ItemComp]:
        """Full item-level activity log for the period.

        Includes both:
          - discounted items (comps)
          - OWNER-SKU items on owner tabs (even with $0 item-level discount — check-level
            comp handled separately via check_totals)
          - Tier 2 retail items (any bottle in TIER_2_RETAIL_MAP regardless of comp status)
        """
        # Build the SQL predicate — include known Tier 2 retail SKUs and OWNER SKUs
        tier2_names = list(TIER_2_RETAIL_MAP.keys()) + list(OWNER_SKU_TO_RETAIL.keys())
        # Parameterize each name to prevent injection
        tier2_placeholders = ", ".join(f"@tier2_{i}" for i in range(len(tier2_names)))
        params = [
            bigquery.ScalarQueryParameter("start", "DATE", start),
            bigquery.ScalarQueryParameter("end", "DATE", end),
        ]
        for i, n in enumerate(tier2_names):
            params.append(bigquery.ScalarQueryParameter(f"tier2_{i}", "STRING", n))

        q = f"""
        SELECT
          processing_date,
          CAST(check_id AS STRING) AS check_id,
          server,
          menu_item,
          sales_category,
          SAFE_CAST(gross_price AS FLOAT64) AS gross_price,
          SAFE_CAST(discount AS FLOAT64) AS discount,
          tab_name,
          table_loc
        FROM `{PROJECT_ID}.{DATASET_ID}.ItemSelectionDetails_raw`
        WHERE processing_date BETWEEN @start AND @end
          AND (
            SAFE_CAST(discount AS FLOAT64) > 0
            OR menu_item IN ({tier2_placeholders})
            OR (STARTS_WITH(UPPER(menu_item), 'OWNER ')
                AND (LOWER(tab_name) LIKE '%maurice%'
                     OR LOWER(tab_name) LIKE '%eddie%'
                     OR LOWER(tab_name) LIKE '%derwin%'))
          )
        """
        job = self.bq.query(q, job_config=bigquery.QueryJobConfig(query_parameters=params))
        log: List[ItemComp] = []
        for row in job.result():
            proc_date = str(row.processing_date)
            try:
                dow = date.fromisoformat(proc_date).strftime("%A")
            except (ValueError, TypeError):
                dow = "Unknown"
            log.append(ItemComp(
                processing_date=proc_date,
                day_of_week=dow,
                check_id=str(row.check_id) if row.check_id else "",
                server=row.server,
                tab_name=row.tab_name,
                table_loc=row.table_loc,
                menu_item=row.menu_item or "",
                sales_category=row.sales_category,
                gross_price=float(row.gross_price or 0.0),
                discount=float(row.discount or 0.0),
            ))
        return log

    def _build_item_hints(self, item_log: List[ItemComp]) -> Dict[str, Dict]:
        """Aggregate per-check SKU classification hint from the item log."""
        raw: Dict[str, Dict] = {}
        for it in item_log:
            item_bucket = classify_comp_item(it.menu_item)
            if not item_bucket:
                continue
            entry = raw.setdefault(it.check_id, {
                "bucket_totals": {},
                "top_item": None,
                "top_amount": 0.0,
            })
            entry["bucket_totals"][item_bucket] = entry["bucket_totals"].get(item_bucket, 0.0) + it.discount
            if it.discount > entry["top_amount"]:
                entry["top_amount"] = it.discount
                entry["top_item"] = it.menu_item

        hints: Dict[str, Dict] = {}
        for check_id, entry in raw.items():
            winning = max(entry["bucket_totals"], key=entry["bucket_totals"].get)
            hints[check_id] = {
                "bucket": winning,
                "top_item": entry["top_item"] or "",
            }
        return hints

    def compute_period(self, label: str, start: str, end: str) -> CompPeriod:
        net_sales = self._fetch_net_sales(start, end)
        comps = self._fetch_comps(start, end)
        item_log = self._fetch_item_log(start, end)
        check_totals = self._fetch_check_totals(start, end)
        item_hints = self._build_item_hints([it for it in item_log if it.discount > 0])

        # check_id → tab_name lookup (from item log, since CheckDetails lacks tab_name).
        # Enables tab-name-first classification precedence at the check level.
        check_tab: Dict[str, str] = {}
        for it in item_log:
            if it.check_id and it.check_id not in check_tab and it.tab_name:
                check_tab[it.check_id] = it.tab_name

        period = CompPeriod(label=label, start=start, end=end, net_sales=net_sales)
        # Filter item_log for the discount-only path (existing logic uses this)
        # Keep the full item_log accessible via item_log field for new sections
        period.item_log = item_log

        for row in comps:
            reason_bucket = row.bucket  # from reason code
            hint = item_hints.get(row.check_id)
            tab_name = check_tab.get(row.check_id)

            # Apply full policy precedence: Tab > SKU > Reason (§3)
            top_item = hint["top_item"] if hint else None
            final_bucket = classify_final(row.reason, top_item, tab_name, row.processing_date)

            # Track mismatch if final differs from reason
            if final_bucket != reason_bucket:
                # Determine what overrode the reason: tab-name or SKU
                tab_bucket = classify_comp_tab(tab_name, row.processing_date) if tab_name else None
                sku_bucket = hint["bucket"] if hint else None
                override_source = (
                    f"tab-name({tab_name})" if tab_bucket else
                    f"item-SKU({top_item})" if sku_bucket else None
                )
                if override_source:
                    period.mismatches.append(ItemMismatch(
                        check_id=row.check_id,
                        server=row.server,
                        reason_bucket=reason_bucket,
                        item_bucket=final_bucket,
                        top_item=top_item or (tab_name or ""),
                        amount=row.amount,
                    ))
            row.bucket = final_bucket

            if row.bucket in EXCLUDED_BUCKETS:
                continue

            period.total_comp += row.amount
            period.by_bucket[row.bucket] = period.by_bucket.get(row.bucket, 0.0) + row.amount
            period.by_bucket_count[row.bucket] = period.by_bucket_count.get(row.bucket, 0) + 1

            # Bottle Manager gets its own scorecard — it's a pooling station.
            if row.server == "Bottle Manager":
                s = period.bottle_manager
            elif row.server:
                s = period.by_server.setdefault(row.server, ServerScore(server=row.server))
            else:
                s = None

            if s is not None:
                s.total_comp += row.amount
                s.comp_count += 1
                if row.bucket in DISCRETIONARY_BUCKETS:
                    s.discretionary_comp += row.amount
                elif row.bucket.startswith("programmatic"):
                    s.programmatic_comp += row.amount
                elif row.bucket == "owner_discretion":
                    s.owner_comp += row.amount
                elif row.bucket == "recovery":
                    s.recovery_comp += row.amount

        # Flag: uncategorized open $/% ring-ins
        for row in comps:
            if row.bucket in ("uncategorized_open_dollar", "uncategorized_open_pct"):
                period.flagged.append(row)

        # Flag: server discretionary > 150% of median
        disc_totals = [s.discretionary_comp for s in period.by_server.values()
                       if s.discretionary_comp > 0]
        if disc_totals:
            disc_totals.sort()
            median = disc_totals[len(disc_totals) // 2]
            threshold = median * 1.5
            # Floor at $250 to avoid flagging every server when median is tiny.
            for s in period.by_server.values():
                if s.discretionary_comp > threshold and s.discretionary_comp > 250:
                    period.flagged.append(CompRow(
                        processing_date=end,
                        check_id="SERVER_OUTLIER",
                        server=s.server,
                        reason=f"Discretionary ${s.discretionary_comp:,.0f} vs median ${median:,.0f}",
                        amount=s.discretionary_comp,
                        bucket="server_outlier",
                    ))

        # ── ITEM-LEVEL ENRICHMENT PASSES ────────────────────────────
        # Build check → reason lookup for the item-level pass
        check_reason: Dict[str, str] = {row.check_id: row.reason or "" for row in comps}

        # Enrich every item with bucket, tier2, protocol flags
        for it in period.item_log:
            reason = check_reason.get(it.check_id, "")
            it.reason = reason
            it.bucket = classify_final(reason, it.menu_item)
            it.is_tier_2 = is_tier_2_item(it.menu_item)
            flags = []
            if it.is_tier_2 and match_promoter_cap(it.tab_name, it.processing_date):
                flags.append("Tier 2 under promoter tab")
            if it.bucket in ("uncategorized_open_dollar", "uncategorized_open_pct"):
                flags.append("Uncategorized")
            it.protocol_flag = " · ".join(flags) if flags else None

        # Filter to items with actual comps for the aggregation passes.
        # Zero-discount items (Tier 2 rings + OWNER SKUs on owner tabs) are handled
        # by the new tier2_movements + owner_contributions passes below.
        comp_items = [it for it in period.item_log if it.discount > 0]

        # By day of week — use CHECK-LEVEL totals so amounts match blended %
        # (item-level would undercount because check-level Owner-Comps aren't
        # allocated to individual item rows in Toast)
        for row in comps:
            if row.bucket in EXCLUDED_BUCKETS:
                continue
            try:
                dow = date.fromisoformat(row.processing_date).strftime("%A")
            except (ValueError, TypeError):
                continue
            if dow not in _DAY_ORDER:
                continue
            entry = period.by_day.setdefault(
                dow, DailyTotal(day=dow, date_iso=row.processing_date),
            )
            entry.total += row.amount
            entry.count += 1
            entry.by_bucket[row.bucket] = entry.by_bucket.get(row.bucket, 0.0) + row.amount

        # Top comped items (leaderboard)
        item_agg: Dict[str, Tuple[int, float]] = {}
        for it in comp_items:
            cnt, tot = item_agg.get(it.menu_item, (0, 0.0))
            item_agg[it.menu_item] = (cnt + 1, tot + it.discount)
        period.top_items = sorted(
            [(name, cnt, tot) for name, (cnt, tot) in item_agg.items()],
            key=lambda x: x[2], reverse=True,
        )[:10]

        # Promoter cap enforcement — per event
        # Fallback rule (locked 2026-07-31): until Bday-{D}-{Name} tab-naming
        # standard rolls out, any comped bottle that is NOT on an owner tab
        # (Maurice/Eddie/Derwin) and NOT on a birthday tab counts as promoter
        # or in-house comp for that day of the week. This closes the
        # DAE7/Cassette/etc. attribution gap where servers used server-name
        # tabs (E5 Ceriah, Marcus E3) instead of promoter labels.
        for cap in PROMOTER_CAPS:
            result = PromoterCapResult(
                key=cap["key"], day=cap["day"], event=cap["event"],
                poc=cap["poc"], cap=cap["cap"], cap_type=cap["type"],
            )
            for it in comp_items:
                # Skip non-bottle items
                if not is_bottle(it.menu_item, it.sales_category, it.gross_price):
                    continue

                # Rule 1: date must fall on this event's day-of-week
                try:
                    d = date.fromisoformat(it.processing_date)
                except (ValueError, TypeError):
                    continue
                if d.weekday() != cap["dow"]:
                    continue

                # Rule 2: exclude owner-tab bottles (owner personal/discretionary)
                if match_owner_from_tab(it.tab_name):
                    continue

                # Rule 3: exclude birthday-tab bottles (birthday program)
                tab_low = (it.tab_name or "").lower()
                if "bday" in tab_low or "birthday" in tab_low:
                    continue

                # Rule 4: Sunday has TWO events — split by tab signal
                # DAE7 (brunch) vs Cassette (late night). If neither tab-signal
                # matches, use daypart heuristic: sunday morning/afternoon → DAE7,
                # evening → Cassette. If daypart unknown, assign to DAE7 (brunch).
                if cap["dow"] == 6:  # Sunday
                    matched_signal = match_promoter_cap(it.tab_name, it.processing_date)
                    if matched_signal and matched_signal["key"] != cap["key"]:
                        # Tab explicitly matches the OTHER Sunday event — skip
                        continue
                    if not matched_signal:
                        # Fallback: split by daypart (need opened_time; item log
                        # doesn't have it — use rough heuristic based on cap key)
                        # For now, assign non-labeled Sunday bottles to Cassette
                        # (late night is where most Sunday bottle activity happens).
                        if cap["key"] == "sun_dae7":
                            continue

                result.bottle_count += 1
                result.bottle_dollars += it.discount
                if it.is_tier_2:
                    result.tier_2_count += 1
                    result.tier_2_dollars += it.discount
                if len(result.sample_items) < 5:
                    result.sample_items.append(it.menu_item)
            period.promoter_caps.append(result)

        # Owner personal log (comp-only view — legacy compatibility)
        for it in comp_items:
            owner = match_owner_from_tab(it.tab_name)
            if not owner:
                continue
            log = period.owner_logs.setdefault(owner, OwnerLog(name=owner))
            log.total += it.discount
            if len(log.sample_items) < 5:
                log.sample_items.append(it.menu_item)
            log.tab_count = len({
                x.check_id for x in comp_items
                if match_owner_from_tab(x.tab_name) == owner
            })

        # ── TIER 2 BOTTLE MOVEMENTS (all rings, comped OR paid) ─────
        # Semantics fix per audit: cost_recovered = what LOV3 actually collected
        # (cost basis for OWNER SKUs when owner paid; retail for non-OWNER when
        # someone else paid). Non-owner retail sales are NOT foregone revenue.
        for it in period.item_log:
            retail = get_retail_price(it.menu_item)
            is_t2_activity = (it.menu_item in TIER_2_RETAIL_MAP
                              or it.menu_item in OWNER_SKU_TO_RETAIL
                              or it.is_tier_2)
            if not is_t2_activity:
                continue
            check_total = check_totals.get(it.check_id, 0.0)
            movement = period.tier_2_movements.setdefault(
                it.menu_item,
                Tier2Movement(menu_item=it.menu_item),
            )
            movement.total_rings += 1
            if retail:
                movement.retail_value_moved += retail

            owner_tab = match_owner_from_tab(it.tab_name) is not None
            house_comped = check_total <= 0.01  # tiny threshold guards floating point

            if house_comped:
                movement.house_comped_count += 1
                # Nothing recovered; retail moves into foregone
            elif owner_tab:
                movement.owner_paid_count += 1
                # Owner contributed the cost basis (item gross_price is cost for OWNER SKUs)
                movement.cost_recovered += it.gross_price
            else:
                movement.other_paid_count += 1
                # Guest paid at (approx) retail. Not a house loss.
                # Use retail as the recovered value so foregone nets to 0 for these.
                movement.cost_recovered += (retail if retail else it.gross_price)

        # Compute foregone revenue = retail value moved - cost recovered
        for m in period.tier_2_movements.values():
            m.foregone_revenue = max(0.0, m.retail_value_moved - m.cost_recovered)

        # ── OWNER CONTRIBUTION LEDGER ──────────────────────────────
        for it in period.item_log:
            owner = match_owner_from_tab(it.tab_name)
            if not owner:
                continue
            entry = period.owner_contributions.setdefault(
                owner, OwnerContribution(owner_name=owner),
            )
            check_total = check_totals.get(it.check_id, 0.0)
            retail = get_retail_price(it.menu_item)

            if check_total > 0:
                # Owner (or their guest) paid — capture the check's collected value
                # We attribute item-level gross_price to owner_paid
                entry.paid_by_owner += it.gross_price
                if it.menu_item not in entry.items_paid:
                    entry.items_paid.append(it.menu_item)
            else:
                # House-comped: capture cost basis + retail estimate for foregone
                entry.house_comped_at_cost += it.gross_price
                if retail:
                    entry.house_comped_retail_estimate += retail
                if it.menu_item not in entry.items_house_comped:
                    entry.items_house_comped.append(it.menu_item)

        # Count distinct paid vs house-comped checks per owner
        owner_check_status: Dict[str, Dict[str, set]] = {}
        for it in period.item_log:
            owner = match_owner_from_tab(it.tab_name)
            if not owner:
                continue
            check_total = check_totals.get(it.check_id, 0.0)
            bucket = owner_check_status.setdefault(owner, {"paid": set(), "comped": set()})
            if check_total > 0:
                bucket["paid"].add(it.check_id)
            else:
                bucket["comped"].add(it.check_id)
        for owner, status in owner_check_status.items():
            if owner in period.owner_contributions:
                period.owner_contributions[owner].tabs_paid = len(status["paid"])
                period.owner_contributions[owner].tabs_house_comped = len(status["comped"])

        # Named scorecards — Ashley, Tiffany, Tony, Daja
        def _score_for(name: str, role: str) -> ManagerScore:
            s = period.by_server.get(name)
            score = ManagerScore(name=name, role=role)
            if s:
                score.comp_count = s.comp_count
                score.total_comp = s.total_comp
                score.discretionary_comp = s.discretionary_comp
                score.recovery_comp = s.recovery_comp
                score.owner_comp_rung = s.owner_comp
                score.programmatic_comp = s.programmatic_comp
            # Diversity = count of non-zero score buckets (check-level attribution,
            # since managers rarely ring items themselves — they approve check-level)
            non_zero = 0
            if score.discretionary_comp > 0:
                non_zero += 1
            if score.recovery_comp > 0:
                non_zero += 1
            if score.owner_comp_rung > 0:
                non_zero += 1
            if score.programmatic_comp > 0:
                non_zero += 1
            score.reason_code_diversity = non_zero
            return score

        for name in MANAGERS:
            period.named_scorecards[name] = _score_for(name, "Manager")
        for name in BAR_LEADS:
            period.named_scorecards[name] = _score_for(name, "Bar Lead")

        return period

    def compute_last_week(self) -> CompPeriod:
        return self.compute_period(*last_completed_week())

    def compute_prior_week(self) -> CompPeriod:
        return self.compute_period(*prior_week())

    def compute_trailing_90d(self) -> CompPeriod:
        return self.compute_period(*trailing_window(90))


# ── Slack report ────────────────────────────────────────────────────


def build_weekly_slack_message(cur: CompPeriod, prev: CompPeriod) -> Tuple[str, bool]:
    """Comprehensive weekly Tuesday leadership comp report.

    Sections: headline · by day · top items · promoter caps · manager scorecard ·
    bottle manager · owner log · protocol flags · action items · links.
    """
    is_alert = (cur.total_pct >= BLENDED_WATCH_PCT
                or cur.discretionary_pct >= DISCRETIONARY_WATCH_PCT
                or cur.manager_disc_pct > MANAGER_DISC_TARGET_PCT
                or cur.recovery_pct > RECOVERY_TARGET_PCT * 2
                or cur.uncategorized_dollars > UNCATEGORIZED_ALERT_THRESHOLD
                or any(c.is_over_cap or c.tier_2_count > 0 for c in cur.promoter_caps))
    icon = "🔴" if is_alert else "✅"

    def _wow(cur_v: float, prev_v: float) -> str:
        if prev_v == 0:
            return ""
        delta = cur_v - prev_v
        arrow = "↑" if delta > 0 else "↓"
        return f" {arrow} {abs(delta):.2f}pp"

    def _grade_icon(label: str) -> str:
        return {"On Target": "✅", "Watch": "🟡", "Investigate": "🔴"}.get(label, "•")

    blended_label, _ = cur.grade()
    disc_label, _ = cur.discretionary_grade()
    mgr_disc_label, _ = cur.manager_disc_grade()
    recovery_label, _ = cur.recovery_grade()

    # Money at Risk — top-of-report leadership framing per agent audit
    tier2_foregone = sum(m.foregone_revenue for m in cur.tier_2_movements.values())
    tier2_house_comped_count = sum(m.house_comped_count for m in cur.tier_2_movements.values())
    cap_breaches = [c for c in cur.promoter_caps if c.is_over_cap]
    external_clawback = sum(
        c.excess_bottles * 0.8 * AVG_TIER_1_RETAIL
        for c in cap_breaches if c.cap_type == "external"
    )
    uncateg = cur.uncategorized_dollars

    money_at_risk_lines = []
    if tier2_foregone > 0:
        money_at_risk_lines.append(
            f"  • *${tier2_foregone:,.0f}* foregone on {tier2_house_comped_count} "
            f"Tier 2 house-comped bottle(s)"
        )
    if cap_breaches:
        breach_summary = ", ".join(f"{c.event} +{c.excess_bottles}" for c in cap_breaches)
        clawback_str = f" → ~${external_clawback:,.0f} clawback" if external_clawback else ""
        money_at_risk_lines.append(f"  • {len(cap_breaches)} cap breach: {breach_summary}{clawback_str}")
    if uncateg > UNCATEGORIZED_ALERT_THRESHOLD:
        money_at_risk_lines.append(
            f"  • *${uncateg:,.0f}* in uncategorized ring-ins — reason code required"
        )

    lines = [
        f"{icon} *LOV3 Comp Report — Week of {cur.label}*",
        "",
    ]
    if money_at_risk_lines:
        lines.append("════ MONEY AT RISK ════")
        lines.extend(money_at_risk_lines)
        lines.append("")
    lines.append("════ HEADLINE ════")
    lines.append(
        f"Blended:            *{cur.total_pct:.2f}%*  target <{BLENDED_TARGET_PCT:.0f}%   "
        f"{_grade_icon(blended_label)} {blended_label}{_wow(cur.total_pct, prev.total_pct)}"
    )
    lines.append(
        f"Manager Discret.:   *{cur.manager_disc_pct:.2f}%*  target ≤{MANAGER_DISC_TARGET_PCT:.1f}%   "
        f"{_grade_icon(mgr_disc_label)} {mgr_disc_label}"
    )
    lines.append(
        f"Recovery:           *{cur.recovery_pct:.2f}%*  target ≤{RECOVERY_TARGET_PCT:.1f}%   "
        f"{_grade_icon(recovery_label)} {recovery_label}"
    )
    lines.append(f"Uncategorized:      *${cur.uncategorized_dollars:,.0f}*  target $0")
    lines.append(f"Net sales base:     ${cur.net_sales:,.0f}")
    lines.append("")
    lines.append("════ BY DAY OF WEEK ════")

    if cur.by_day:
        for day in LOV3_OPERATING_DAYS:
            d = cur.by_day.get(day)
            if d:
                promoter_tag = ""
                for cap in PROMOTER_CAPS:
                    if cap["day"] == day:
                        promoter_tag = f"  ← promoter ({cap['event']})"
                        break
                lines.append(f"{day[:3]}: ${d.total:>6,.0f} ({d.count} comps){promoter_tag}")
            else:
                lines.append(f"{day[:3]}: —")
    else:
        lines.append("No item-level detail this week.")

    # ════ TOP 10 COMPED ITEMS ════
    if cur.top_items:
        lines.append("")
        lines.append("════ TOP 10 COMPED ITEMS ════")
        for i, (name, cnt, tot) in enumerate(cur.top_items, 1):
            tier2 = " 🚨 Tier 2" if is_tier_2_item(name) else ""
            lines.append(f"{i:2d}. {name} × {cnt} · ${tot:,.0f}{tier2}")

    # ════ PROMOTER CAPS ════
    lines.append("")
    lines.append("════ PROMOTER CAPS ════")
    for cap_result in cur.promoter_caps:
        icon_c = cap_result.status_icon
        excess = f" ⚠ +{cap_result.excess_bottles} excess (${cap_result.bottle_dollars:,.0f})" if cap_result.is_over_cap else ""
        tier2 = f" 🚨 {cap_result.tier_2_count} Tier 2 (${cap_result.tier_2_dollars:,.0f})" if cap_result.tier_2_count else ""
        lines.append(
            f"{icon_c} {cap_result.day[:3]} {cap_result.event} ({cap_result.poc}): "
            f"{cap_result.bottle_count}/{cap_result.cap} bottles{excess}{tier2}"
        )

    # ════ MANAGER + BAR LEAD SCORECARD ════
    lines.append("")
    lines.append("════ MANAGER SCORECARD ════")
    lines.append(f"{'Name':<22} {'Role':<10} {'Cmps':>5} {'Disc $':>8} {'Recov $':>8} {'Diversity':>10}")
    for name in MANAGERS + BAR_LEADS:
        s = cur.named_scorecards.get(name)
        if not s:
            continue
        # Diversity flag: 1 = only one reason code (bad), 4+ = diverse (good)
        div_flag = "⚠" if s.reason_code_diversity <= 1 and s.comp_count >= 5 else " "
        lines.append(
            f"{s.name:<22} {s.role:<10} {s.comp_count:>5} "
            f"${s.discretionary_comp:>6,.0f} ${s.recovery_comp:>6,.0f} "
            f"{div_flag} {s.reason_code_diversity} buckets"
        )

    # ════ BOTTLE MANAGER STATION ════
    bm = cur.bottle_manager
    if bm.comp_count > 0:
        lines.append("")
        lines.append("════ BOTTLE MANAGER (pooling station, anonymous) ════")
        lines.append(
            f"Total: {bm.comp_count} comps · ${bm.total_comp:,.0f}  |  "
            f"Owner ${bm.owner_comp:,.0f} · "
            f"Programmatic ${bm.programmatic_comp:,.0f} · "
            f"Discretionary ${bm.discretionary_comp:,.0f}"
        )
        lines.append(
            "  (Note: comps rung under Bottle Manager on promoter tabs also count in "
            "the cap table above — see attribution on `/comps`.)"
        )

    # ════ TIER 2 BOTTLE ACTIVITY ════
    if cur.tier_2_movements:
        total_retail = sum(m.retail_value_moved for m in cur.tier_2_movements.values())
        total_cost = sum(m.cost_recovered for m in cur.tier_2_movements.values())
        total_foregone = sum(m.foregone_revenue for m in cur.tier_2_movements.values())
        lines.append("")
        lines.append("════ TIER 2 BOTTLE ACTIVITY ════")
        lines.append(
            f"Total retail moved: ${total_retail:,.0f}  |  Cost recovered: ${total_cost:,.0f}  |  "
            f"Foregone: *${total_foregone:,.0f}*"
        )
        # Show each Tier 2 item
        for name, m in sorted(cur.tier_2_movements.items(),
                              key=lambda kv: -kv[1].retail_value_moved):
            splits = []
            if m.owner_paid_count:
                splits.append(f"{m.owner_paid_count} owner-paid")
            if m.house_comped_count:
                splits.append(f"{m.house_comped_count} house-comped")
            if m.other_paid_count:
                splits.append(f"{m.other_paid_count} other-paid")
            lines.append(
                f"  • {name} × {m.total_rings}  "
                f"({', '.join(splits)})  "
                f"retail ${m.retail_value_moved:,.0f}  foregone ${m.foregone_revenue:,.0f}"
            )

    # ════ OWNER PERSONAL TABS ════ (Maurice/Eddie/Derwin — paid vs house-comped)
    if cur.owner_contributions:
        lines.append("")
        lines.append("════ OWNER PERSONAL TABS ════")
        for owner_name in ["Maurice", "Eddie", "Derwin"]:
            oc = cur.owner_contributions.get(owner_name)
            if not oc or (oc.paid_by_owner == 0 and oc.house_comped_at_cost == 0):
                lines.append(f"{owner_name}: no activity")
                continue
            lines.append(f"{owner_name}:")
            if oc.paid_by_owner > 0:
                lines.append(
                    f"  💵 Paid: ${oc.paid_by_owner:,.0f} on {oc.tabs_paid} tab(s)  "
                    f"— {', '.join(oc.items_paid[:3])}"
                )
            if oc.house_comped_at_cost > 0:
                lines.append(
                    f"  🏠 House-comped: ${oc.house_comped_at_cost:,.0f} at cost on "
                    f"{oc.tabs_house_comped} tab(s) (est. retail ${oc.house_comped_retail_estimate:,.0f})"
                )

    # ════ OWNER DISCRETIONARY (non-personal) ════ — Owner Comp used on VIP guests etc.
    owner_disc = cur.by_bucket.get("owner_discretion", 0.0)
    owner_disc_cnt = cur.by_bucket_count.get("owner_discretion", 0)
    # Subtract owner-personal totals (already covered above) — best-effort estimate
    owner_personal_total = sum(
        (oc.paid_by_owner + oc.house_comped_at_cost)
        for oc in cur.owner_contributions.values()
    )
    non_personal = max(0.0, owner_disc - owner_personal_total)
    if non_personal > 0:
        lines.append("")
        lines.append("════ OWNER DISCRETIONARY (non-personal) ════")
        lines.append(
            f"Non-personal-tab owner comps: ${non_personal:,.0f} "
            f"(hosting VIPs, industry pros, chef guests)"
        )

    # ════ PROTOCOL FLAGS ════
    open_flags = [f for f in cur.flagged
                  if f.bucket in ("uncategorized_open_dollar", "uncategorized_open_pct")]
    tier2_violations = sum(1 for c in cur.promoter_caps if c.tier_2_count > 0)
    cap_breaches = sum(1 for c in cur.promoter_caps if c.is_over_cap)
    total_flags = len(open_flags) + len(cur.mismatches) + tier2_violations + cap_breaches

    lines.append("")
    lines.append(f"════ PROTOCOL FLAGS ({total_flags} items) ════")
    if open_flags:
        total_open = sum(f.amount for f in open_flags)
        lines.append(f"🚩 {len(open_flags)} uncategorized comps (${total_open:,.0f}) — reason code required")
    if cur.mismatches:
        total_mis = sum(m.amount for m in cur.mismatches)
        lines.append(f"🚩 {len(cur.mismatches)} SKU/reason mismatches (${total_mis:,.0f})")
    if cap_breaches:
        lines.append(f"🚩 {cap_breaches} promoter cap breach(es) — see cap table above")
    if tier2_violations:
        lines.append(f"🚩 {tier2_violations} Tier 2 bottle(s) rung under promoter tab")
    if not open_flags and not cur.mismatches and not cap_breaches and not tier2_violations:
        lines.append("✅ No protocol flags this week")

    # ════ ACTION ITEMS ════ (each includes check_id or tab_name for actionability)
    actions = []
    # Manager discretionary review — surface top 3 check_ids for their biggest comps
    for name, s in cur.named_scorecards.items():
        if s.discretionary_comp >= 500 and s.role == "Manager":
            top_items = sorted(
                [it for it in cur.item_log if it.server == name and it.discount > 0],
                key=lambda x: -x.discount,
            )[:3]
            hint = ""
            if top_items:
                hint = " · Top checks: " + ", ".join(
                    f"#{it.check_id[-6:]}({it.menu_item[:20]} ${it.discount:.0f})"
                    for it in top_items
                )
            actions.append(
                f"{name} elevated discretionary (${s.discretionary_comp:,.0f}) — review{hint}"
            )
    for cap in cur.promoter_caps:
        if cap.is_over_cap and cap.cap_type == "external":
            clawback = cap.excess_bottles * 0.8 * AVG_TIER_1_RETAIL
            items_hint = f" · Items: {', '.join(cap.sample_items[:3])}" if cap.sample_items else ""
            actions.append(
                f"Promoter clawback: {cap.event} ({cap.poc}) over by {cap.excess_bottles} bottle(s) "
                f"→ ~${clawback:,.0f} against next payout{items_hint}"
            )
        elif cap.is_over_cap and cap.cap_type == "internal":
            actions.append(
                f"In-house event overage: {cap.event} +{cap.excess_bottles} bottle(s) → excess promotional spend"
            )
    if open_flags:
        top_open = sorted(open_flags, key=lambda x: -x.amount)[:3]
        servers = ", ".join(sorted({f.server for f in open_flags if f.server}))
        actions.append(
            f"{len(open_flags)} uncategorized ring-ins (${sum(f.amount for f in open_flags):,.0f}) "
            f"— from {servers or 'unknown'} · biggest: "
            + ", ".join(f"#{f.check_id[-6:]} ${f.amount:.0f}" for f in top_open)
        )
    # Tier 2 house-comps that need review
    tier2_house_comped = [
        (name, m) for name, m in cur.tier_2_movements.items()
        if m.house_comped_count > 0 and m.foregone_revenue >= 500
    ]
    if tier2_house_comped:
        for name, m in sorted(tier2_house_comped, key=lambda kv: -kv[1].foregone_revenue)[:3]:
            actions.append(
                f"Tier 2 review: {name} × {m.house_comped_count} house-comped "
                f"→ ${m.foregone_revenue:,.0f} foregone"
            )

    if actions:
        lines.append("")
        lines.append("════ ACTION ITEMS ════")
        for a in actions:
            lines.append(f"• {a}")

    lines.append("")
    lines.append("Full detail: https://toast-etl-pipeline-t3di7qky4q-uc.a.run.app/comps")

    return "\n".join(lines), is_alert


def send_weekly_comp_report(analytics: Optional[CompAnalytics] = None) -> Dict:
    """Compute last completed week's comp performance and post to Slack.

    Called by Cloud Scheduler (Tue 9 AM CT) and on demand via /api/comp-report.
    """
    from services import AlertManager
    import os

    analytics = analytics or CompAnalytics()
    cur = analytics.compute_last_week()
    prev = analytics.compute_prior_week()

    msg, is_error = build_weekly_slack_message(cur, prev)
    # Prefer SLACK_REPORT_WEBHOOK (leadership channel); fall back to alerts webhook.
    webhook = os.environ.get("SLACK_REPORT_WEBHOOK") or ALERT_WEBHOOK_URL
    alert = AlertManager(slack_webhook=webhook)
    alert.send_slack_alert(msg, is_error=is_error)

    return {
        "status": "success",
        "week": cur.label,
        "net_sales": round(cur.net_sales, 2),
        "total_comp": round(cur.total_comp, 2),
        "total_pct": round(cur.total_pct, 2),
        "discretionary_pct": round(cur.discretionary_pct, 2),
        "flagged_count": len(cur.flagged),
        "alerted": is_error,
    }


# ── HTML rendering ──────────────────────────────────────────────────


def _money(v: float) -> str:
    if v is None:
        return "—"
    sign = "-" if v < 0 else ""
    return f"{sign}${abs(v):,.0f}"


def _pct(v: float) -> str:
    return f"{v:.2f}%" if v else "0.00%"


def render_html(cur: CompPeriod, prev: CompPeriod, trailing_90d: CompPeriod) -> str:
    """Editorial-luxury dark dashboard for /comps."""
    import html
    from design_system import page_shell

    grade_label, grade_class = cur.grade()
    disc_label, disc_class = cur.discretionary_grade()

    # Category rows
    cat_rows = []
    total_categorized = sum(cur.by_bucket.values()) or 1.0
    for bucket_key in ("programmatic_birthday", "programmatic_promoter",
                       "programmatic_standing", "owner_discretion",
                       "recovery", "discretionary_manager",
                       "uncategorized_open_dollar", "uncategorized_open_pct", "other"):
        amt = cur.by_bucket.get(bucket_key, 0.0)
        cnt = cur.by_bucket_count.get(bucket_key, 0)
        if amt == 0 and cnt == 0:
            continue
        share = 100.0 * amt / total_categorized
        label = BUCKET_LABELS.get(bucket_key, bucket_key)
        flag_badge = ""
        if bucket_key.startswith("uncategorized"):
            flag_badge = ' <span class="badge grade-alert">⚠ Reason code missing</span>'
        cat_rows.append(f"""
        <tr>
          <td>{html.escape(label)}{flag_badge}</td>
          <td class="num">{cnt}</td>
          <td class="num">{_money(amt)}</td>
          <td class="num">{share:.1f}%</td>
        </tr>""")

    # Server rows — sort by discretionary
    servers = sorted(cur.by_server.values(),
                     key=lambda s: s.discretionary_comp, reverse=True)
    server_rows = []
    disc_totals = [s.discretionary_comp for s in servers if s.discretionary_comp > 0]
    median = sorted(disc_totals)[len(disc_totals) // 2] if disc_totals else 0.0
    for s in servers[:20]:
        outlier = s.discretionary_comp > median * 1.5 and s.discretionary_comp > 250
        badge = ' <span class="badge grade-alert">⚠ Outlier</span>' if outlier else ""
        server_rows.append(f"""
        <tr>
          <td>{html.escape(s.server)}{badge}</td>
          <td class="num">{s.comp_count}</td>
          <td class="num">{_money(s.total_comp)}</td>
          <td class="num">{_money(s.discretionary_comp)}</td>
          <td class="num">{_money(s.programmatic_comp)}</td>
          <td class="num">{_money(s.owner_comp)}</td>
        </tr>""")

    # Flagged transactions
    flag_rows = []
    for f in cur.flagged[:25]:
        flag_rows.append(f"""
        <tr>
          <td class="mono">{html.escape(str(f.processing_date))}</td>
          <td>{html.escape(f.server or '—')}</td>
          <td>{html.escape((f.reason or '')[:80])}</td>
          <td class="num">{_money(f.amount)}</td>
          <td><span class="badge grade-warn">{BUCKET_LABELS.get(f.bucket, f.bucket)}</span></td>
        </tr>""")

    # Mismatch rows — SKU classifies differently than reason code
    mismatch_rows = []
    for m in sorted(cur.mismatches, key=lambda x: x.amount, reverse=True)[:20]:
        reason_label = BUCKET_LABELS.get(m.reason_bucket, m.reason_bucket)
        item_label = BUCKET_LABELS.get(m.item_bucket, m.item_bucket)
        mismatch_rows.append(f"""
        <tr>
          <td>{html.escape(m.server or '—')}</td>
          <td>{html.escape(m.top_item)}</td>
          <td><span class="badge grade-warn">{html.escape(reason_label)}</span></td>
          <td>→ <span class="badge grade-excellent">{html.escape(item_label)}</span></td>
          <td class="num">{_money(m.amount)}</td>
        </tr>""")

    # Named manager scorecard rows (Ashley, Tiffany, Tony, Daja)
    named_rows = []
    for name in MANAGERS + BAR_LEADS:
        s = cur.named_scorecards.get(name)
        if not s:
            continue
        div_flag = ""
        if s.reason_code_diversity <= 1 and s.comp_count >= 5:
            div_flag = ' <span class="badge grade-warn">⚠ Low diversity</span>'
        # Compare against target for role
        if s.role == "Manager":
            target_hint = "Manager Discretionary ≤1.0%"
        else:
            target_hint = "Bar Lead — own peer group"
        named_rows.append(f"""
        <tr>
          <td><strong>{html.escape(s.name)}</strong>{div_flag}<br/><span class="stat-sub">{s.role} · {target_hint}</span></td>
          <td class="num">{s.comp_count}</td>
          <td class="num">{_money(s.total_comp)}</td>
          <td class="num">{_money(s.discretionary_comp)}</td>
          <td class="num">{_money(s.recovery_comp)}</td>
          <td class="num">{_money(s.owner_comp_rung)}</td>
          <td class="num">{s.reason_code_diversity} / 4</td>
        </tr>""")

    # Day-of-week rows
    day_rows = []
    for day in LOV3_OPERATING_DAYS:
        d = cur.by_day.get(day)
        promoter_tag = ""
        for cap in PROMOTER_CAPS:
            if cap["day"] == day:
                promoter_tag = f'<span class="badge">{html.escape(cap["event"])}</span>'
                break
        if d:
            day_rows.append(f"""
            <tr>
              <td><strong>{html.escape(day)}</strong> {promoter_tag}</td>
              <td class="num">{d.count}</td>
              <td class="num">{_money(d.total)}</td>
            </tr>""")
        else:
            day_rows.append(f"""
            <tr>
              <td>{html.escape(day)} {promoter_tag}</td>
              <td class="num">—</td>
              <td class="num">—</td>
            </tr>""")

    # Top items rows
    top_item_rows = []
    for i, (name, cnt, tot) in enumerate(cur.top_items, 1):
        tier2_badge = ' <span class="badge grade-alert">🚨 Tier 2</span>' if is_tier_2_item(name) else ""
        top_item_rows.append(f"""
        <tr>
          <td class="num mono">{i}</td>
          <td>{html.escape(name)}{tier2_badge}</td>
          <td class="num">{cnt}</td>
          <td class="num">{_money(tot)}</td>
        </tr>""")

    # Promoter cap rows
    cap_rows = []
    for c in cur.promoter_caps:
        status = c.status_icon
        excess_note = ""
        if c.is_over_cap:
            excess_note = f' <span class="badge grade-alert">+{c.excess_bottles} over</span>'
        if c.tier_2_count:
            excess_note += f' <span class="badge grade-alert">🚨 {c.tier_2_count} Tier 2</span>'
        cap_rows.append(f"""
        <tr>
          <td>{status} {html.escape(c.day)}</td>
          <td>{html.escape(c.event)}</td>
          <td>{html.escape(c.poc)} <span class="stat-sub">({c.cap_type})</span></td>
          <td class="num"><strong>{c.bottle_count}</strong> / {c.cap}{excess_note}</td>
          <td class="num">{_money(c.bottle_dollars)}</td>
        </tr>""")

    # Owner log rows
    owner_rows = []
    for owner_name in ["Maurice", "Eddie", "Derwin"]:
        log = cur.owner_logs.get(owner_name)
        if log and log.total > 0:
            samples = ", ".join(html.escape(x) for x in log.sample_items[:3])
            owner_rows.append(f"""
            <tr>
              <td><strong>{owner_name}</strong></td>
              <td class="num">{log.tab_count}</td>
              <td class="num">{_money(log.total)}</td>
              <td class="stat-sub">{samples}</td>
            </tr>""")
        else:
            owner_rows.append(f"""
            <tr>
              <td>{owner_name}</td>
              <td class="num">0</td>
              <td class="num">$0</td>
              <td class="stat-sub">—</td>
            </tr>""")

    # Tier 2 bottle activity rows
    tier2_rows = []
    for name, m in sorted(cur.tier_2_movements.items(),
                          key=lambda kv: -kv[1].retail_value_moved):
        splits = []
        if m.owner_paid_count:
            splits.append(f'<span class="badge grade-excellent">{m.owner_paid_count} owner-paid</span>')
        if m.house_comped_count:
            splits.append(f'<span class="badge grade-alert">{m.house_comped_count} house-comped</span>')
        if m.other_paid_count:
            splits.append(f'<span class="badge">{m.other_paid_count} other-paid</span>')
        tier2_rows.append(f"""
        <tr>
          <td>{html.escape(name)}</td>
          <td class="num">{m.total_rings}</td>
          <td>{' '.join(splits)}</td>
          <td class="num">{_money(m.retail_value_moved)}</td>
          <td class="num">{_money(m.cost_recovered)}</td>
          <td class="num"><strong>{_money(m.foregone_revenue)}</strong></td>
        </tr>""")

    # Owner contribution ledger rows
    owner_contrib_rows = []
    for owner_name in ["Maurice", "Eddie", "Derwin"]:
        oc = cur.owner_contributions.get(owner_name)
        if not oc or (oc.paid_by_owner == 0 and oc.house_comped_at_cost == 0):
            owner_contrib_rows.append(f"""
            <tr>
              <td><strong>{owner_name}</strong></td>
              <td class="num">$0</td><td class="num">0</td>
              <td class="num">$0</td><td class="num">0</td>
              <td class="num">$0</td>
            </tr>""")
            continue
        items_paid = ", ".join(html.escape(x) for x in oc.items_paid[:3]) or "—"
        items_comped = ", ".join(html.escape(x) for x in oc.items_house_comped[:3]) or "—"
        owner_contrib_rows.append(f"""
        <tr>
          <td>
            <strong>{owner_name}</strong><br/>
            <span class="stat-sub">paid: {items_paid}<br/>comped: {items_comped}</span>
          </td>
          <td class="num">{_money(oc.paid_by_owner)}</td>
          <td class="num">{oc.tabs_paid}</td>
          <td class="num">{_money(oc.house_comped_at_cost)}</td>
          <td class="num">{oc.tabs_house_comped}</td>
          <td class="num"><strong>{_money(oc.house_comped_retail_estimate)}</strong></td>
        </tr>""")

    # Full item log rows — comprehensive detail table
    item_log_rows = []
    for it in sorted(cur.item_log, key=lambda x: (x.processing_date, -x.discount))[:100]:
        flag_badge = ""
        if it.protocol_flag:
            flag_badge = f' <span class="badge grade-alert">🚩 {html.escape(it.protocol_flag)}</span>'
        elif it.is_tier_2:
            flag_badge = ' <span class="badge grade-warn">Tier 2</span>'
        bucket_label = BUCKET_LABELS.get(it.bucket, it.bucket)
        item_log_rows.append(f"""
        <tr>
          <td class="mono">{html.escape(it.processing_date)}</td>
          <td>{html.escape(it.day_of_week[:3])}</td>
          <td>{html.escape(it.server or '—')}</td>
          <td>{html.escape(it.tab_name or '—')}</td>
          <td>{html.escape(it.menu_item)}{flag_badge}</td>
          <td>{html.escape(bucket_label)}</td>
          <td class="num">{_money(it.discount)}</td>
        </tr>""")

    # Bottle Manager scorecard block (separate — it's a pooling station, not a person)
    bm = cur.bottle_manager
    bm_block = ""
    if bm.comp_count > 0:
        bm_block = f"""
<div class="section">
  <h2>Bottle Manager — pooling station scorecard</h2>
  <p class="section-note">
    Bottle Manager is a POS station for walk-in bottle sales — anyone can ring as it,
    so the totals here belong to the station, not a person. Track it separately from
    individual servers so cross-manager comparisons stay meaningful.
  </p>
  <div class="grid-4">
    <div class="stat-card">
      <div class="stat-label">Total Comps</div>
      <div class="stat-value">{bm.comp_count}</div>
      <div class="stat-sub">${bm.total_comp:,.0f} · avg ${bm.avg_comp:,.0f}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Owner Comps</div>
      <div class="stat-value">{_money(bm.owner_comp)}</div>
      <div class="stat-sub">OWNER-prefixed SKUs</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Programmatic</div>
      <div class="stat-value">{_money(bm.programmatic_comp)}</div>
      <div class="stat-sub">Birthday / Thursday / Wycliff</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Discretionary</div>
      <div class="stat-value">{_money(bm.discretionary_comp)}</div>
      <div class="stat-sub">Manager comp + unclassified</div>
    </div>
  </div>
</div>
"""

    body = f"""
<div class="hero">
  <div class="eyebrow">LOV3 · Houston · Leadership Comp Discipline</div>
  <h1>Comp Performance</h1>
  <p class="lede">
    Weekly leadership view of comp activity vs industry benchmarks. Discretionary comps
    (manager + recovery) target ≤{DISCRETIONARY_TARGET_PCT:.1f}% of net sales.
    Blended comps target &lt;{BLENDED_TARGET_PCT:.0f}%.
  </p>
</div>

<div class="section">
  <h2>Last completed week — {html.escape(cur.label)}</h2>
  <div class="grid-4">
    <div class="stat-card {grade_class}">
      <div class="stat-label">Blended Comp %</div>
      <div class="stat-value">{_pct(cur.total_pct)}</div>
      <div class="stat-sub">
        ${cur.total_comp:,.0f} on ${cur.net_sales:,.0f}<br/>
        target &lt;{BLENDED_TARGET_PCT:.0f}% · <strong>{grade_label}</strong>
      </div>
    </div>

    <div class="stat-card {cur.manager_disc_grade()[1]}">
      <div class="stat-label">Manager Discretionary %</div>
      <div class="stat-value">{_pct(cur.manager_disc_pct)}</div>
      <div class="stat-sub">
        target ≤{MANAGER_DISC_TARGET_PCT:.1f}%<br/>
        <strong>{cur.manager_disc_grade()[0]}</strong>
      </div>
    </div>

    <div class="stat-card {cur.recovery_grade()[1]}">
      <div class="stat-label">Recovery %</div>
      <div class="stat-value">{_pct(cur.recovery_pct)}</div>
      <div class="stat-sub">
        target ≤{RECOVERY_TARGET_PCT:.1f}%<br/>
        <strong>{cur.recovery_grade()[0]}</strong>
      </div>
    </div>

    <div class="stat-card">
      <div class="stat-label">Uncategorized $</div>
      <div class="stat-value">{_money(cur.uncategorized_dollars)}</div>
      <div class="stat-sub">
        target $0<br/>
        Prior wk: {_pct(prev.total_pct)} blended
      </div>
    </div>
  </div>
</div>

<div class="section">
  <h2>By day of week</h2>
  <p class="section-note">
    LOV3 operates Tuesday through Sunday. Promoter events are labeled.
  </p>
  <table class="report-table">
    <thead>
      <tr><th>Day</th><th class="num">Comps</th><th class="num">$</th></tr>
    </thead>
    <tbody>
      {''.join(day_rows)}
    </tbody>
  </table>
</div>

<div class="section">
  <h2>Top comped items</h2>
  <p class="section-note">
    Highest-$ comped items this week. Tier 2 bottles (retail ≥$500) flagged —
    Tier 2 bottles under promoter tabs are policy violations.
  </p>
  <table class="report-table">
    <thead>
      <tr><th class="num">#</th><th>Item</th><th class="num">Times</th><th class="num">$</th></tr>
    </thead>
    <tbody>
      {''.join(top_item_rows) or '<tr><td colspan="4">No comped items this week.</td></tr>'}
    </tbody>
  </table>
</div>

<div class="section">
  <h2>Promoter cap enforcement</h2>
  <p class="section-note">
    Per-event caps from Section 5 of the comp policy. Tier 1 bottles only (retail &lt; ${TIER_1_MAX_RETAIL:.0f}).
    External promoter overages incur 80% clawback; In-House overages log as excess promotional spend.
    Note: while the tab-naming standard rolls out, the classifier depends on tab-name signals
    which are not yet consistently applied — cap counts may under-represent actual usage.
  </p>
  <table class="report-table">
    <thead>
      <tr>
        <th>Day</th>
        <th>Event</th>
        <th>Promoter (Type)</th>
        <th class="num">Bottles / Cap</th>
        <th class="num">$ Comp</th>
      </tr>
    </thead>
    <tbody>
      {''.join(cap_rows)}
    </tbody>
  </table>
</div>

<div class="section">
  <h2>Named scorecard — managers &amp; bar lead</h2>
  <p class="section-note">
    Ashley (Bar Lead) tracked in her own category. Managers ranked against
    the Manager Discretionary target. Diversity = distinct comp buckets touched
    (a manager using only "Manager Comp" for everything gets flagged).
  </p>
  <table class="report-table">
    <thead>
      <tr>
        <th>Name / Role</th>
        <th class="num">Cmps</th>
        <th class="num">Total $</th>
        <th class="num">Discret $</th>
        <th class="num">Recovery $</th>
        <th class="num">Owner $ rung</th>
        <th class="num">Diversity</th>
      </tr>
    </thead>
    <tbody>
      {''.join(named_rows)}
    </tbody>
  </table>
</div>

<div class="section">
  <h2>By category</h2>
  <p class="section-note">
    Programmatic comps (birthday, promoter, standing) are measured against plan.
    Discretionary comps are measured against the 1.5% net-sales target. Uncategorized
    <code>Open $</code>/<code>Open %</code> ring-ins should be moved to a proper reason code
    at the POS level.
  </p>
  <table class="report-table">
    <thead>
      <tr><th>Category</th><th class="num">Count</th><th class="num">$</th><th class="num">Share</th></tr>
    </thead>
    <tbody>
      {''.join(cat_rows)}
    </tbody>
  </table>
</div>

<div class="section">
  <h2>Server scorecard</h2>
  <p class="section-note">
    Ranked by discretionary comp $. Outlier badge fires when a server's discretionary
    comp is &gt;150% of the peer median AND &gt;$100 for the week.
    Bottle Manager naturally shows high totals — it's a pooling station, not a person.
  </p>
  <table class="report-table">
    <thead>
      <tr>
        <th>Server</th>
        <th class="num">Comps</th>
        <th class="num">Total $</th>
        <th class="num">Discretionary $</th>
        <th class="num">Programmatic $</th>
        <th class="num">Owner $</th>
      </tr>
    </thead>
    <tbody>
      {''.join(server_rows) or '<tr><td colspan="6">No server-attributed comps this week.</td></tr>'}
    </tbody>
  </table>
</div>

{bm_block}

<div class="section">
  <h2>Tier 2 bottle activity — retail value + foregone revenue</h2>
  <p class="section-note">
    Every ring of a Tier 2 SKU or OWNER-prefixed cost-basis SKU. Retail prices sourced from Toast menus API.
    "Foregone" = retail value moved − cost recovered (what the house didn't collect).
    OWNER SKUs resolve to their retail equivalent (e.g., <code>OWNER ACE</code> → Ace BTL retail $654).
  </p>
  <table class="report-table">
    <thead>
      <tr>
        <th>Item</th>
        <th class="num">Rings</th>
        <th>Attribution</th>
        <th class="num">Retail Moved</th>
        <th class="num">Cost Recovered</th>
        <th class="num">Foregone $</th>
      </tr>
    </thead>
    <tbody>
      {''.join(tier2_rows) or '<tr><td colspan="6">No Tier 2 activity this week.</td></tr>'}
    </tbody>
  </table>
</div>

<div class="section">
  <h2>Owner contribution ledger</h2>
  <p class="section-note">
    Distinguishes what an owner actually paid for versus what was house-comped on their tab.
    "Paid by owner" = check total &gt; 0 (Visa, cash, etc.). "House-comped" = check total = 0
    (Owner Comp-Check applied). Retail estimate uses the Toast menu retail equivalent for OWNER SKUs.
  </p>
  <table class="report-table">
    <thead>
      <tr>
        <th>Owner</th>
        <th class="num">Paid $</th>
        <th class="num">Paid Tabs</th>
        <th class="num">House-Comped $ (cost)</th>
        <th class="num">Comped Tabs</th>
        <th class="num">Est. Retail Comped</th>
      </tr>
    </thead>
    <tbody>
      {''.join(owner_contrib_rows)}
    </tbody>
  </table>
</div>

<div class="section">
  <h2>Owner log (bucket-based, legacy)</h2>
  <p class="section-note">
    Legacy view — same tabs from a comp-bucket accounting perspective. The
    Owner Contribution Ledger above is the authoritative view.
  </p>
  <table class="report-table">
    <thead>
      <tr><th>Owner</th><th class="num">Tabs</th><th class="num">Total $</th><th>Sample Items</th></tr>
    </thead>
    <tbody>
      {''.join(owner_rows)}
    </tbody>
  </table>
</div>

<div class="section">
  <h2>Full item log</h2>
  <p class="section-note">
    Every comped item this week (top 100 by discount $). Filter by column in your
    browser. Protocol flags surface Tier 2 bottles under promoter tabs and uncategorized ring-ins.
  </p>
  <table class="report-table item-log">
    <thead>
      <tr>
        <th>Date</th>
        <th>Day</th>
        <th>Server</th>
        <th>Tab Name</th>
        <th>Item</th>
        <th>Category</th>
        <th class="num">$</th>
      </tr>
    </thead>
    <tbody>
      {''.join(item_log_rows) or '<tr><td colspan="7">No item-level comps this week.</td></tr>'}
    </tbody>
  </table>
</div>

<div class="section">
  <h2>Item vs reason-code mismatches</h2>
  <p class="section-note">
    Toast menu items with <code>OWNER</code>, <code>Thursday</code>, or <code>Birthday</code>
    prefixes carry the intended category in the SKU name. These checks were rung with a
    different reason code — the SKU wins in this report, but the ring-in habits should
    be corrected at the POS level.
  </p>
  <table class="report-table">
    <thead>
      <tr><th>Server</th><th>Top Item</th><th>Reason Code Said</th><th>SKU Says</th><th class="num">$</th></tr>
    </thead>
    <tbody>
      {''.join(mismatch_rows) or '<tr><td colspan="5">No SKU/reason mismatches this week ✅</td></tr>'}
    </tbody>
  </table>
</div>

<div class="section">
  <h2>Flagged transactions</h2>
  <p class="section-note">
    Uncategorized <code>Open $</code>/<code>Open %</code> ring-ins + server outliers.
    Each of these should have a story behind it — the point of the report is to make
    that story explicit before the pattern normalizes.
  </p>
  <table class="report-table">
    <thead>
      <tr><th>Date</th><th>Server</th><th>Reason</th><th class="num">$</th><th>Flag</th></tr>
    </thead>
    <tbody>
      {''.join(flag_rows) or '<tr><td colspan="5">No flagged transactions this week ✅</td></tr>'}
    </tbody>
  </table>
</div>

<div class="section">
  <h2>Draft team policy — parameters</h2>
  <ul class="policy-list">
    <li><strong>Blended target:</strong> &lt;{BLENDED_TARGET_PCT:.0f}% of gross revenue</li>
    <li><strong>Discretionary target:</strong> ≤{DISCRETIONARY_TARGET_PCT:.1f}% of net sales</li>
    <li><strong>Promoter cap:</strong> per event AND per promoter; 80% clawback on overage</li>
    <li><strong>Owner comp:</strong> PIN + Slack notification each use</li>
    <li><strong>Reason codes:</strong> required on 100% of discretionary comps</li>
    <li><strong>Cadence:</strong> Tuesday 9 AM CT to <code>#lov3-leader-report</code>; real-time alerts to ownership</li>
  </ul>
</div>
"""

    extra_css = """
      .hero { padding: 4rem 0 2rem; }
      .eyebrow { text-transform: uppercase; letter-spacing: 0.28em;
                 color: var(--gold); font-family: var(--mono); font-size: 11px;
                 font-weight: 500; margin-bottom: 2rem; }
      .hero h1 { font-family: var(--display); font-size: clamp(3rem, 6vw, 5rem);
                 font-weight: 400; letter-spacing: -0.02em; margin: 0.25em 0; }
      .lede { color: var(--bone); font-size: 1.05rem; max-width: 640px; }
      .section { padding: 3rem 0; border-top: 1px solid var(--rule); }
      .section h2 { font-family: var(--display); font-size: 1.5rem; font-weight: 400;
                    letter-spacing: -0.01em; margin-bottom: 1.5rem; }
      .section-note { color: var(--bone); margin-bottom: 1.5rem; font-size: 0.95rem; }
      .section-note code { color: var(--gold); background: var(--surface); padding: 2px 6px;
                          border-radius: 4px; font-family: var(--mono); font-size: 0.85em; }
      .grid-3 { display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; }
      .grid-4 { display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem; }
      .stat-card { background: var(--surface); padding: 1.5rem; border-radius: 8px;
                   border: 1px solid var(--rule); }
      .stat-label { color: var(--muted); font-size: 0.85rem; text-transform: uppercase;
                    letter-spacing: 0.12em; margin-bottom: 0.5rem; }
      .stat-value { font-family: var(--display); font-size: 3rem; font-weight: 400;
                    letter-spacing: -0.02em; margin: 0.25rem 0; }
      .stat-sub { color: var(--bone); font-size: 0.9rem; }
      .stat-card.grade-alert { border-color: var(--negative); }
      .stat-card.grade-warn { border-color: var(--gold); }
      .stat-card.grade-excellent { border-color: var(--positive); }
      .report-table { width: 100%; border-collapse: collapse; font-family: var(--mono);
                      font-size: 0.9rem; }
      .report-table th { text-align: left; padding: 0.75rem; color: var(--muted);
                         font-weight: 500; font-size: 0.75rem; text-transform: uppercase;
                         letter-spacing: 0.1em; border-bottom: 1px solid var(--rule); }
      .report-table th.num, .report-table td.num { text-align: right; }
      .report-table td { padding: 0.75rem; border-bottom: 1px solid var(--rule-soft); }
      .grade-excellent { color: var(--positive); }
      .grade-warn { color: var(--gold); }
      .grade-alert { color: var(--negative); }
      .badge { display: inline-block; padding: 0.2rem 0.6rem; border-radius: 999px;
               font-size: 0.7rem; font-weight: 500; letter-spacing: 0.05em;
               background: var(--surface); border: 1px solid var(--rule); margin-left: 0.5rem; }
      .badge.grade-alert { border-color: var(--negative); color: var(--negative); }
      .badge.grade-warn { border-color: var(--gold); color: var(--gold); }
      .mono { font-family: var(--mono); color: var(--gold); }
      .policy-list { list-style: none; padding: 0; }
      .policy-list li { padding: 0.75rem 1rem; background: var(--surface); border-radius: 6px;
                        border-left: 3px solid var(--gold); margin-bottom: 0.5rem;
                        color: var(--bone); }
      .policy-list li strong { color: var(--ivory); }
    """

    return page_shell(
        title="Comp Performance — LOV3 Houston",
        body_html=body,
        extra_css=extra_css,
        active_path="/comps",
    )
