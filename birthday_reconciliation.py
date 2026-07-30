"""Birthday Package Reconciliation — SR ↔ Toast bridge.

Answers: for each SR reservation that pre-registered for the Birthday Dinner
Package, did they receive the promised complimentary champagne per policy §9?

**ELIGIBILITY RULE (locked 2026-07-30):**
Only reservations with "Birthday Dinner Package" (or equivalent) in the SR
`notes` field are eligible for the free champagne. Reservations that are
merely birthday-tagged in `tags_json` (or mentioned in loose free-text) are
NOT bound by any comp rules.

**OWNER SKU RULE (locked 2026-07-30):**
OWNER-prefixed SKUs represent bottles consumed by the owners themselves OR
allocated at their discretion — NOT birthday-package delivery. In particular,
Bottle Manager ringing OWNER champagne on non-birthday tabs is owner-personal
or owner-discretionary activity, NOT evidence of birthday delivery.

The only OWNER champagne rings that count as birthday delivery are those
that appear on tabs matching the birthday-tab pattern
(`Bday-{D}-{Name}` or legacy `Fri Bday` / `Sat Bday` etc.).

Future: LOV3 will add a dedicated "Birthday Champagne" button/SKU family
so birthday deliveries are distinguishable from owner discretion at the
SKU level. Until then, tab-name is the sole disambiguator.

Bridge mechanism: SevenRooms_Reservations_raw.check_numbers is a comma-separated
list of Toast CheckDetails.check_number values on the same date. Join gives us
the guest's actual spend, discount, and item mix.

Delivery classification per reservation:
  DELIVERED_COMPED       — champagne fully comped on the reservation's check
  DELIVERED_COST_BASIS   — OWNER-prefixed champagne rung at $0 discount on
                           reservation's check (Ashley Fri/Sat pattern)
  DELIVERED_OFF_BOOK     — SR reservation exists but no champagne on the
                           reservation check; separately a bday-tab champagne
                           on the same date matches (Ashley + Bottle Manager)
  DELIVERED_ATTRIBUTED   — tab name follows Bday-{D}-{FirstName} convention
                           and matches this SR guest (deterministic 1:1 pairing)
  SUB_MINIMUM            — pre-reg reservation with <$200 spend (no obligation)
  NOT_DELIVERED          — pre-reg reservation ≥$200 spend on program day but
                           no champagne anywhere (policy failure)
  NO_BRIDGE              — pre-reg reservation completed but check_numbers empty
  NO_PROGRAM             — pre-reg reservation but day-of-week has no program
                           (Mon, Tue, Thu — package wouldn't have been accepted)
  NOT_ELIGIBLE           — not pre-registered for the package; no rules apply

Also computes:
  - Ashley + Bottle Manager cost-basis pattern audit
  - Ghost birthday flags — birthday-tab champagne with no SR pre-reg match
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

from google.cloud import bigquery

from config import PROJECT_ID, DATASET_ID

# Pre-registration signals in SR notes field. Only these confer eligibility.
PRE_REG_KEYWORDS = (
    "birthday dinner package",
    "birthday package",
    "bday package",
    "birthday dinner",  # some hosts abbreviate
)

# Tab-naming convention for per-party bday tabs. Match patterns like:
#   "Bday-F-Kelly", "Bday-Fri-Chelsea-T", "Bday-S-Naya-N", "Bday-Sat-Taylor"
# Day letter: F(riday), S(aturday), W(ednesday), U(sun). Also accept full names.
BDAY_TAB_PATTERN = re.compile(
    r"^bday[\s\-\.]*(?:f(?:ri)?|s(?:at)?|w(?:ed)?|u(?:un)?|sun)"
    r"[\s\-\.]+(?P<first_name>[a-z]+)",
    re.IGNORECASE,
)

logger = logging.getLogger(__name__)

BIRTHDAY_PACKAGE_MIN_SPEND = 200.0  # Policy §9.2 — package only released above this

# Champagne SKU patterns for detection (case-insensitive substring match)
CHAMPAGNE_KEYWORDS = (
    "moet", "wycliff", "bellaire", "prosecco", "veuve", "cristal",
    "dom p", "ace ", " ace", "champagne",
)

# Day-of-week birthday package program (confirmed from 90-day audit 2026-07-30).
# Python weekday: Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sat=5, Sun=6
# Values are the expected champagne pattern per day:
#   ("expected_champagne_keyword", "expected_delivery_mode")
# Delivery modes: "comped" | "cost_basis" | "retail_or_cost_basis"
BIRTHDAY_PROGRAM_BY_DOW = {
    2: {"item_keyword": "bellaire", "mode": "comped",
        "label": "Wednesday — Bellaire Rose (fully comped)"},
    4: {"item_keyword": "moet", "mode": "cost_basis",
        "label": "Friday — OWNER MOET (cost basis)"},
    5: {"item_keyword": "moet", "mode": "cost_basis",
        "label": "Saturday — OWNER MOET (cost basis)"},
    6: {"item_keyword": "bellaire", "mode": "retail_or_cost_basis",
        "label": "Sunday — Bellaire Rose (retail)"},
    # Days NOT in this dict have no package program (Mon, Tue, Thu)
}


def is_champagne(menu_item: Optional[str]) -> bool:
    if not menu_item:
        return False
    m = menu_item.lower()
    return any(kw in m for kw in CHAMPAGNE_KEYWORDS)


def is_owner_prefixed(menu_item: Optional[str]) -> bool:
    if not menu_item:
        return False
    m = menu_item.upper().strip()
    return m.startswith("OWNER ") or m.startswith("$")


# ── Delivery status enum ─────────────────────────────────────────────

DELIVERED_COMPED = "delivered_comped"
DELIVERED_COST_BASIS = "delivered_cost_basis"
DELIVERED_OFF_BOOK = "delivered_off_book"
DELIVERED_ATTRIBUTED = "delivered_attributed"  # Tab-name convention matched
SUB_MINIMUM = "sub_minimum"
NOT_DELIVERED = "not_delivered"
NO_BRIDGE = "no_bridge"
NO_PROGRAM = "no_program"
NOT_ELIGIBLE = "not_eligible"  # Not pre-registered for the package
WRONG_ITEM = "wrong_item"

STATUS_LABELS = {
    DELIVERED_ATTRIBUTED: "Delivered (attributed via Bday-{D}-{Name} tab)",
    DELIVERED_COMPED: "Delivered per program (fully comped)",
    DELIVERED_COST_BASIS: "Delivered per program (OWNER cost basis)",
    DELIVERED_OFF_BOOK: "Delivered off-book (same-day bday-tab match)",
    SUB_MINIMUM: "Sub-minimum — pre-reg but <$200 spend, no obligation",
    NOT_DELIVERED: "🔴 Policy failure — pre-reg party got no champagne",
    NO_BRIDGE: "Bridge failure — SR check_numbers empty",
    NO_PROGRAM: "No program — day of week has no birthday package",
    NOT_ELIGIBLE: "Not eligible — not pre-registered for Birthday Package",
    WRONG_ITEM: "Wrong champagne for the day (review)",
}


# ── Dataclasses ──────────────────────────────────────────────────────


@dataclass
class ChampagneRing:
    """One champagne item on a check — comp OR paid."""
    processing_date: str
    server: Optional[str]
    tab_name: Optional[str]
    check_id: str
    check_number: Optional[int]
    menu_item: str
    gross_price: float
    discount: float

    @property
    def is_fully_comped(self) -> bool:
        return self.discount > 0 and abs(self.discount - self.gross_price) < 0.01

    @property
    def is_owner_cost_basis(self) -> bool:
        return is_owner_prefixed(self.menu_item) and self.discount == 0

    @property
    def is_retail_sale(self) -> bool:
        return not is_owner_prefixed(self.menu_item) and self.discount == 0


@dataclass
class BirthdayReservation:
    """One SR birthday reservation with reconciliation result."""
    date: str
    first_name: str
    last_name: str
    max_guests: int
    arrived_guests: Optional[int]
    arrival_time: Optional[str]
    check_numbers_raw: str
    check_numbers: List[int] = field(default_factory=list)
    sr_revenue: float = 0.0
    # Eligibility signal — only pre-registered parties have package obligation
    is_pre_registered: bool = False
    notes_snippet: str = ""

    # Reconciliation output
    total_spend: float = 0.0
    champagne_rings: List[ChampagneRing] = field(default_factory=list)
    attributed_tab: Optional[str] = None  # Set when Bday-{D}-{Name} tab matched
    status: str = ""
    status_reason: str = ""

    @property
    def guest_label(self) -> str:
        return f"{self.first_name.strip()} {self.last_name.strip()}".strip()

    @property
    def party_size(self) -> int:
        return self.arrived_guests or self.max_guests

    @property
    def hit_minimum(self) -> bool:
        return self.total_spend >= BIRTHDAY_PACKAGE_MIN_SPEND


@dataclass
class BirthdayReconciliationResult:
    period_start: str
    period_end: str
    reservations: List[BirthdayReservation] = field(default_factory=list)
    # All champagne rings in the window (comped OR paid); used for Ashley audit
    all_champagne_rings: List[ChampagneRing] = field(default_factory=list)
    # Champagne comps with NO SR reservation match — ghost birthdays
    ghost_birthdays: List[ChampagneRing] = field(default_factory=list)
    # Ashley-pattern audit: OWNER champagne at $0 discount on bday tabs
    cost_basis_pattern: List[ChampagneRing] = field(default_factory=list)

    @property
    def total_reservations(self) -> int:
        return len(self.reservations)

    @property
    def by_status(self) -> Dict[str, List[BirthdayReservation]]:
        out: Dict[str, List[BirthdayReservation]] = {}
        for r in self.reservations:
            out.setdefault(r.status, []).append(r)
        return out

    @property
    def eligible_reservations(self) -> List["BirthdayReservation"]:
        """Pre-registered reservations with real package obligation."""
        return [r for r in self.reservations
                if r.status not in (NOT_ELIGIBLE, SUB_MINIMUM, NO_BRIDGE, NO_PROGRAM)]

    @property
    def delivery_rate(self) -> float:
        """% of pre-registered eligible reservations that got the package."""
        eligible = self.eligible_reservations
        if not eligible:
            return 0.0
        delivered = [r for r in eligible
                     if r.status in (DELIVERED_ATTRIBUTED, DELIVERED_COMPED,
                                     DELIVERED_COST_BASIS, DELIVERED_OFF_BOOK)]
        return 100.0 * len(delivered) / len(eligible)

    @property
    def attributed_delivery_rate(self) -> float:
        """% delivered via the tab-name convention (deterministic 1:1)."""
        eligible = self.eligible_reservations
        if not eligible:
            return 0.0
        attributed = [r for r in eligible if r.status == DELIVERED_ATTRIBUTED]
        return 100.0 * len(attributed) / len(eligible)


# ── The reconciler ───────────────────────────────────────────────────


class BirthdayReconciliation:

    def __init__(self, bq_client: Optional[bigquery.Client] = None) -> None:
        self.bq = bq_client or bigquery.Client(project=PROJECT_ID)

    def _fetch_sr_birthdays(self, start: str, end: str) -> List[BirthdayReservation]:
        q = f"""
        SELECT
          date,
          COALESCE(first_name, '') AS first_name,
          COALESCE(last_name, '') AS last_name,
          COALESCE(max_guests, 0) AS max_guests,
          arrived_guests,
          arrival_time,
          COALESCE(check_numbers, '') AS check_numbers,
          COALESCE(total_payment, 0.0) AS sr_revenue,
          COALESCE(notes, '') AS notes
        FROM `{PROJECT_ID}.{DATASET_ID}.SevenRooms_Reservations_raw`
        WHERE date BETWEEN @start AND @end
          AND LOWER(status_simple) IN ('complete', 'seated')
          AND (LOWER(tags_json) LIKE '%birthday%'
               OR LOWER(tags_json) LIKE '%bday%'
               OR LOWER(notes) LIKE '%birthday%'
               OR LOWER(client_requests) LIKE '%birthday%')
        ORDER BY date, first_name
        """
        job = self.bq.query(q, job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start", "DATE", start),
                bigquery.ScalarQueryParameter("end", "DATE", end),
            ]
        ))
        out: List[BirthdayReservation] = []
        for row in job.result():
            checks: List[int] = []
            if row.check_numbers:
                for part in str(row.check_numbers).split(","):
                    p = part.strip()
                    if p.isdigit():
                        checks.append(int(p))
            notes_str = str(row.notes or "")
            notes_low = notes_str.lower()
            pre_reg = any(kw in notes_low for kw in PRE_REG_KEYWORDS)
            out.append(BirthdayReservation(
                date=str(row.date),
                first_name=row.first_name,
                last_name=row.last_name,
                max_guests=int(row.max_guests or 0),
                arrived_guests=row.arrived_guests,
                arrival_time=row.arrival_time,
                check_numbers_raw=str(row.check_numbers),
                check_numbers=checks,
                sr_revenue=float(row.sr_revenue or 0.0),
                is_pre_registered=pre_reg,
                notes_snippet=notes_str[:120],
            ))
        return out

    def _fetch_check_totals_by_number(self, start: str, end: str) -> Dict[Tuple[str, int], float]:
        """(processing_date, check_number) → total_paid (amount, excl. tax/tip)."""
        q = f"""
        WITH paid AS (
          SELECT
            processing_date,
            CAST(check_id AS STRING) AS check_id,
            SUM(SAFE_CAST(amount AS FLOAT64)) AS paid
          FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
          WHERE processing_date BETWEEN @start AND @end
          GROUP BY processing_date, check_id
        )
        SELECT
          cd.processing_date,
          SAFE_CAST(cd.check_number AS INT64) AS check_number,
          COALESCE(SUM(paid.paid), 0.0) AS collected
        FROM `{PROJECT_ID}.{DATASET_ID}.CheckDetails_raw` cd
        LEFT JOIN paid ON paid.check_id = CAST(cd.check_id AS STRING)
                     AND paid.processing_date = cd.processing_date
        WHERE cd.processing_date BETWEEN @start AND @end
          AND SAFE_CAST(cd.check_number AS INT64) IS NOT NULL
        GROUP BY cd.processing_date, cd.check_number
        """
        job = self.bq.query(q, job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start", "DATE", start),
                bigquery.ScalarQueryParameter("end", "DATE", end),
            ]
        ))
        return {
            (str(row.processing_date), int(row.check_number)): float(row.collected or 0.0)
            for row in job.result()
        }

    def _fetch_all_champagne_rings(self, start: str, end: str) -> List[ChampagneRing]:
        """All champagne items (comp OR paid) in the window with metadata."""
        # Explicit list of champagne keywords for BQ predicate (SQL doesn't like Python tuples)
        like_clauses = " OR ".join(
            f"LOWER(isd.menu_item) LIKE '%{kw}%'" for kw in CHAMPAGNE_KEYWORDS
        )
        q = f"""
        SELECT
          isd.processing_date,
          isd.server,
          isd.tab_name,
          CAST(isd.check_id AS STRING) AS check_id,
          SAFE_CAST(cd.check_number AS INT64) AS check_number,
          isd.menu_item,
          SAFE_CAST(isd.gross_price AS FLOAT64) AS gross_price,
          SAFE_CAST(isd.discount AS FLOAT64) AS discount
        FROM `{PROJECT_ID}.{DATASET_ID}.ItemSelectionDetails_raw` isd
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.CheckDetails_raw` cd
          ON CAST(cd.check_id AS STRING) = CAST(isd.check_id AS STRING)
          AND cd.processing_date = isd.processing_date
        WHERE isd.processing_date BETWEEN @start AND @end
          AND ({like_clauses})
        """
        job = self.bq.query(q, job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start", "DATE", start),
                bigquery.ScalarQueryParameter("end", "DATE", end),
            ]
        ))
        out: List[ChampagneRing] = []
        for row in job.result():
            out.append(ChampagneRing(
                processing_date=str(row.processing_date),
                server=row.server,
                tab_name=row.tab_name,
                check_id=row.check_id,
                check_number=row.check_number,
                menu_item=row.menu_item or "",
                gross_price=float(row.gross_price or 0.0),
                discount=float(row.discount or 0.0),
            ))
        return out

    def _tab_is_birthday(self, tab_name: Optional[str]) -> bool:
        if not tab_name:
            return False
        t = tab_name.lower()
        return "bday" in t or "birthday" in t

    def reconcile(self, start: str, end: str) -> BirthdayReconciliationResult:
        result = BirthdayReconciliationResult(period_start=start, period_end=end)

        reservations = self._fetch_sr_birthdays(start, end)
        check_totals = self._fetch_check_totals_by_number(start, end)
        all_rings = self._fetch_all_champagne_rings(start, end)
        result.all_champagne_rings = all_rings

        # Index rings by (date, check_number) for reservation matching
        rings_by_check: Dict[Tuple[str, int], List[ChampagneRing]] = {}
        for r in all_rings:
            if r.check_number is not None:
                rings_by_check.setdefault((r.processing_date, r.check_number), []).append(r)

        # Index rings by date for off-book bday-tab matching
        rings_by_date_bday_tab: Dict[str, List[ChampagneRing]] = {}
        for r in all_rings:
            if self._tab_is_birthday(r.tab_name):
                rings_by_date_bday_tab.setdefault(r.processing_date, []).append(r)

        # SR check_numbers that we successfully matched to a reservation
        matched_check_keys: Set[Tuple[str, int]] = set()

        # Build tab-attribution index: {(date, first_name_lower): [rings]}
        # Only for tabs matching Bday-{D}-{Name} convention (all_champagne_rings).
        tab_attribution: Dict[Tuple[str, str], List[ChampagneRing]] = {}
        for r in all_rings:
            if not r.tab_name:
                continue
            m = BDAY_TAB_PATTERN.match(r.tab_name.strip())
            if not m:
                continue
            key = (r.processing_date, m.group("first_name").lower())
            tab_attribution.setdefault(key, []).append(r)

        # Reconcile each SR reservation
        for res in reservations:
            # ── ELIGIBILITY GATE (locked 2026-07-30) ─────────────────
            # Only pre-registered Birthday Package reservations have any
            # obligation. Everything else is NOT_ELIGIBLE and stops here.
            if not res.is_pre_registered:
                res.status = NOT_ELIGIBLE
                res.status_reason = (
                    "Birthday-tagged but no 'Birthday Dinner Package' in "
                    "SR notes — no comp obligation"
                )
                result.reservations.append(res)
                continue

            # Day-of-week program check
            try:
                dow = date.fromisoformat(res.date).weekday()
            except (ValueError, TypeError):
                dow = -1
            program = BIRTHDAY_PROGRAM_BY_DOW.get(dow)

            # Bridge check
            if not res.check_numbers:
                res.status = NO_BRIDGE
                res.status_reason = "SR check_numbers empty — cannot verify delivery"
                result.reservations.append(res)
                continue

            # Sum spend across all bridge checks + collect on-check champagne
            for cn in res.check_numbers:
                key = (res.date, cn)
                res.total_spend += check_totals.get(key, 0.0)
                for ring in rings_by_check.get(key, []):
                    res.champagne_rings.append(ring)
                    matched_check_keys.add(key)

            # If pre-reg but day-of-week has no program — should not happen
            # in practice (LOV3 wouldn't accept a package pre-reg for Thu) but
            # handle defensively.
            if not program:
                res.status = NO_PROGRAM
                res.status_reason = (
                    f"Pre-reg but {date.fromisoformat(res.date).strftime('%A')} "
                    f"has no birthday program — declined package?"
                )
                result.reservations.append(res)
                continue

            expected_kw = program["item_keyword"]
            expected_mode = program["mode"]

            # ── STEP 1 — Tab-name attribution (highest confidence) ───
            attribution_key = (res.date, res.first_name.strip().lower())
            attributed_rings = tab_attribution.get(attribution_key, [])
            if attributed_rings:
                # Deterministic 1:1 pairing via naming convention
                ring = attributed_rings[0]
                res.attributed_tab = ring.tab_name
                res.status = DELIVERED_ATTRIBUTED
                res.status_reason = (
                    f"Tab '{ring.tab_name}' matched {res.first_name} · "
                    f"{ring.menu_item} (${ring.gross_price:.0f})"
                )
                # Mark ring's check as matched (avoid double-count)
                if ring.check_number is not None:
                    matched_check_keys.add((ring.processing_date, ring.check_number))
                result.reservations.append(res)
                continue

            # ── STEP 2 — On-check champagne matching expected item ───
            matching_rings = [
                r for r in res.champagne_rings
                if expected_kw in r.menu_item.lower()
            ]
            if expected_mode == "comped":
                delivered = [r for r in matching_rings if r.is_fully_comped]
                status_if_delivered = DELIVERED_COMPED
            elif expected_mode == "cost_basis":
                delivered = [r for r in matching_rings if r.is_owner_cost_basis]
                status_if_delivered = DELIVERED_COST_BASIS
            else:  # retail_or_cost_basis (Sunday Bellaire)
                delivered = matching_rings
                status_if_delivered = DELIVERED_COMPED

            if delivered:
                res.status = status_if_delivered
                res.status_reason = ", ".join(
                    f"{r.menu_item} (${r.gross_price:.0f})" for r in delivered[:2]
                )
                result.reservations.append(res)
                continue

            if res.champagne_rings:
                res.status = WRONG_ITEM
                res.status_reason = (
                    f"Program expects {program['label']}; "
                    f"actual: {', '.join(r.menu_item for r in res.champagne_rings[:2])}"
                )
                result.reservations.append(res)
                continue

            # ── STEP 3 — Off-book delivery: birthday-labeled tabs only.
            # Bottle Manager OWNER-SKU rings on non-birthday tabs are owner
            # personal / owner-discretionary activity, NOT birthday delivery
            # (locked 2026-07-30). Only tabs matching the birthday pattern
            # count as candidate deliveries here.
            off_book_pool = [
                r for r in all_rings
                if r.processing_date == res.date
                and expected_kw in r.menu_item.lower()
                and (r.is_fully_comped or r.is_owner_cost_basis)
                and (r.processing_date, r.check_number) not in matched_check_keys
                and self._tab_is_birthday(r.tab_name)
            ]
            if off_book_pool and res.hit_minimum:
                # Speculative match — flag it but claim delivery
                ring = off_book_pool[0]
                res.status = DELIVERED_OFF_BOOK
                res.status_reason = (
                    f"Same-day match candidate: {ring.menu_item} on "
                    f"'{ring.tab_name or '(blank)'}' by {ring.server}"
                )
                # Do NOT mark matched — off-book is speculative; other parties
                # same day can also claim the same ring
                result.reservations.append(res)
                continue

            if not res.hit_minimum:
                res.status = SUB_MINIMUM
                res.status_reason = (
                    f"Pre-reg but spend ${res.total_spend:.0f} below $200 "
                    f"minimum — no obligation released"
                )
            else:
                res.status = NOT_DELIVERED
                res.status_reason = (
                    f"Pre-reg party spent ${res.total_spend:.0f} on "
                    f"{date.fromisoformat(res.date).strftime('%a')} — "
                    f"{program['label']} owed but no champagne rung"
                )
            result.reservations.append(res)

        # Ashley cost-basis pattern audit: OWNER-prefixed champagne at $0 discount on bday tab
        result.cost_basis_pattern = [
            r for r in all_rings
            if r.is_owner_cost_basis and self._tab_is_birthday(r.tab_name)
        ]

        # Ghost birthdays: fully-comped champagne rings that:
        #   - aren't attached to any SR reservation via check_number match
        #   - AREN'T owner-personal / owner-discretionary (Maurice/Eddie/Derwin tabs)
        #   - AREN'T host-stand Wycliffe welcome pours
        #   - AREN'T tastings (distributor/owner tasting)
        # Only legitimate "unaccounted-for" birthday champagne comps remain.
        def _is_owner_personal_or_marketing(ring: ChampagneRing) -> bool:
            t = (ring.tab_name or "").lower()
            # Owner personal
            for kw in ("maurice", "eddie", "derwin", "per maurice"):
                if kw in t:
                    return True
            # Host stand / Wycliffe welcome / DNL
            if t in ("door", "dnl") or "host stand" in t or "wycliffe" in t:
                return True
            # Owner tasting / distributor tasting
            if "tasting" in t or "distributor" in t:
                return True
            return False

        result.ghost_birthdays = [
            r for r in all_rings
            if r.is_fully_comped
            and r.check_number is not None
            and (r.processing_date, r.check_number) not in matched_check_keys
            and not _is_owner_personal_or_marketing(r)
        ]
        return result


# ── CLI runner (one-shot) ────────────────────────────────────────────


def run_and_print(start: str, end: str) -> BirthdayReconciliationResult:
    recon = BirthdayReconciliation()
    result = recon.reconcile(start, end)

    print(f"\n{'='*88}")
    print(f"BIRTHDAY RECONCILIATION — {start} to {end}")
    print(f"{'='*88}\n")

    print(f"SR birthday-related reservations completed: {result.total_reservations}")
    pre_reg_count = sum(1 for r in result.reservations if r.is_pre_registered)
    print(f"  of which pre-registered for Birthday Dinner Package: {pre_reg_count}")
    print(f"  eligible for delivery obligation: {len(result.eligible_reservations)}\n")

    by_status = result.by_status
    for status in [DELIVERED_ATTRIBUTED, DELIVERED_COMPED, DELIVERED_COST_BASIS,
                   DELIVERED_OFF_BOOK, WRONG_ITEM, SUB_MINIMUM, NOT_DELIVERED,
                   NO_PROGRAM, NO_BRIDGE, NOT_ELIGIBLE]:
        n = len(by_status.get(status, []))
        print(f"  {STATUS_LABELS[status]:<74s} {n:>3}")
    print(f"\nDelivery rate (of eligible pre-reg parties): {result.delivery_rate:.1f}%")
    print(f"  of which deterministically attributed via Bday-{{D}}-{{Name}} tab: "
          f"{result.attributed_delivery_rate:.1f}%\n")

    # Per-reservation table
    print(f"{'-'*88}")
    print(f"{'Date':<11}{'Guest':<22}{'Party':>6}{'Spend':>10}  Status / Detail")
    print(f"{'-'*88}")
    for r in result.reservations:
        icon = {
            DELIVERED_COMPED: "✅",
            DELIVERED_COST_BASIS: "🟡",
            DELIVERED_OFF_BOOK: "🟠",
            SUB_MINIMUM: "⬜",
            NOT_DELIVERED: "🔴",
            NO_BRIDGE: "⚠️",
        }.get(r.status, "?")
        print(f"{r.date:<11}{r.guest_label[:20]:<22}{r.party_size:>6}${r.total_spend:>8,.0f}  "
              f"{icon} {r.status_reason[:60]}")

    # Ashley cost-basis pattern
    print(f"\n{'-'*88}")
    print(f"OWNER-COST CHAMPAGNE ON BDAY TABS ({len(result.cost_basis_pattern)} rings)")
    print(f"  (Ashley's substitute-package pattern — policy interpretation gap)")
    print(f"{'-'*88}")
    ashley_total = 0.0
    for r in sorted(result.cost_basis_pattern, key=lambda x: x.processing_date):
        print(f"{r.processing_date}  {r.server or '—':<22} {r.tab_name or '—':<28} "
              f"{r.menu_item:<25} ${r.gross_price:>6,.0f}")
        ashley_total += r.gross_price
    print(f"{'':>77}Total: ${ashley_total:,.0f}")

    # Ghost birthdays
    if result.ghost_birthdays:
        print(f"\n{'-'*88}")
        print(f"GHOST BIRTHDAYS ({len(result.ghost_birthdays)} champagne comps with no SR match)")
        print(f"{'-'*88}")
        for r in result.ghost_birthdays[:20]:
            print(f"{r.processing_date}  {r.server or '—':<22} {r.tab_name or '—':<28} "
                  f"{r.menu_item:<25} ${r.discount:>6,.0f}")

    return result


if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 3:
        run_and_print(sys.argv[1], sys.argv[2])
    else:
        # Default: last completed week
        today = date.today()
        days_since_sun = (today.weekday() + 1) % 7
        last_sun = today - timedelta(days=days_since_sun)
        last_mon = last_sun - timedelta(days=6)
        run_and_print(last_mon.isoformat(), last_sun.isoformat())
