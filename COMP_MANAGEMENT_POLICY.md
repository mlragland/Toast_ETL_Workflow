# LOV3 Comp Management Policy

**Version:** 1.0 DRAFT (pending leadership sign-off)
**Owner:** Maurice Ragland
**Effective:** TBD (target: Sprint 5, Week of Aug 24, 2026)
**Last revised:** 2026-07-29

This document defines the comp management taxonomy, targets, tab-naming standards,
promoter caps, and enforcement rules for LOV3 Houston. It supersedes ad-hoc practices
and drives the Tuesday leadership Slack report and the `/comps` dashboard.

---

## Part 1 — Comp Bucket Taxonomy

Every comp on every check falls into exactly one of the following buckets. Buckets
are ordered by intent, not amount.

| Bucket | Definition | Approval Required |
|---|---|---|
| **Owner Personal** | Owner's own consumption tabs (Maurice, Eddie, Derwin) | None — pure visibility |
| **Owner Discretionary** | Owner-designated VIP hosting, industry pros, chef/media/celebrity guests, owner tastings | Any manager; PIN required *(pending, see 4.1)* |
| **VIP Comp** *(new)* | Non-owner VIP treatment: brand ambassadors, industry standings, curated guests | Manager approval — VIP list definition pending |
| **Programmatic — Birthday** | Birthday package comps per section 9 rules | Auto — when SR reservation matches + $200 min met |
| **Programmatic — Promoter** | Contracted comps under a promoter cap (see section 5) | Auto within cap; over cap requires ownership Slack |
| **Programmatic — Marketing** | Wycliffe host-stand welcome, distributor tastings, brand tastings | Manager approval |
| **Recovery** | Spillage, breakage, food quality, wrong item, insects/foreign objects | Manager approval; reason code required |
| **Manager Discretionary** | Manager-approved goodwill outside of Recovery: guest recovery, upsell reward, apology | Manager PIN required |
| **Employee Discount** | Staff meals, shift drinks, employee % off | HR policy — excluded from comp totals |
| **Uncategorized** | Open $/% ring-ins with no reason code | 🚫 Target: zero — must be recategorized |

---

## Part 2 — Targets & Benchmarks

### 2.1 Blended Comp Rate

Total comp $ as % of gross revenue.

- **Target:** < 4%
- **Watch:** 4–6%
- **Investigate:** > 6%

### 2.2 Bucket-Level Targets

| Bucket | Target | Rationale |
|---|---|---|
| Discretionary combined (Manager Discretionary + Uncategorized) | ≤ 1.0% of net sales | Manager judgment goodwill |
| Recovery | ≤ 0.5% of net sales | Kitchen/bar quality baseline |
| Programmatic (Birthday + Promoter + Marketing) | Measured vs plan | Not benchmarked — measured against cap/reservation match |
| Owner Personal | No target — full visibility | Audit trail only |
| Owner Discretionary | No target — full visibility | Audit trail only |
| VIP Comp | Cap TBD | Pending 4.3 sign-off |
| Employee Discount | Tracked separately | Not part of comp % |
| Uncategorized | **Zero** | Every comp must have a reason code |

### 2.3 Industry Reference Points

- Upscale bottle service venue target: 2–4% blended
- LOV3 current 90-day baseline: **5.31% blended, 4.25% discretionary** — Investigate zone
- Recovery peer benchmark: 0.3–0.5% for tight kitchens; up to 1% for volume venues

---

## Part 3 — Classification Precedence

When comp signals conflict (e.g., reason code says "Manager Comp" but item SKU says
"OWNER MOET ROSE"), the following precedence applies. Strongest signal wins.

1. **Tab Name** (strongest — closest to human intent)
2. **Item SKU prefix** (OWNER, Thursday, BIRTHDAY, WYCLIFF)
3. **Reason of Discount** (Toast POS reason code)

For the two known name-collision cases (Tiffany, Tony), use this sub-precedence:

1. Tab starts with `Promoter -` → Promoter (unambiguous)
2. Tab contains `DAE7`, `Karaoke`, or `Brunch` → Promoter Tiffany (Sunday Brunch)
3. Tab contains `Cassette` → Promoter Tony (Sunday Late Night)
4. Tab is `Tiffany` (or minor variant) on Sunday Brunch daypart → Promoter Tiffany
5. Tab is `Tony` (or minor variant) on Sunday Late Night daypart → Promoter Tony
6. Otherwise → Owner Personal or reason-code fallback

---

## Part 4 — Tab Name Classification Map

The classifier honors these patterns. Case-insensitive matching. Historical data
before the naming standard (section 5) rolls out is best-effort — use item SKU + reason
code fallback.

| Tab Pattern | Bucket |
|---|---|
| `Maurice*`, `Eddie*`, `Derwin*`, `Per Maurice*` | Owner Personal |
| `*Owner Tasting*`, `Owner Tasting - *` | Owner Discretionary |
| `Promoter*`, `Promo*` | Programmatic — Promoter |
| `*DAE7*`, `*Karaoke*`, `*Brunch*` (Sunday daypart) | Programmatic — Promoter (DAE7 Tiffany) |
| `*Cassette*` | Programmatic — Promoter (Cassette Tony) |
| Specific tab: `Kelvin`, `Anno`, `Swan`, `Keith` (promoter POCs) | Programmatic — Promoter |
| `*Wycliffe*`, `Tasting*`, `Distributor*` | Programmatic — Marketing |
| `*Birthday*`, `*Bday*` | Programmatic — Birthday |
| `Spill*`, `Bug*`, `Bottle Broke*`, `*Broke*` | Recovery |
| `VIP - *` | VIP Comp |

---

## Part 5 — Promoter Cap Table

Caps are **per event, per night**. Tier 1 bottles only (see Part 6). Overages
trigger flags in the Tuesday report and clawback logic (see Part 7).

| Day | Event | Time | Promoter | Type | Cap |
|---|---|---|---|---|---|
| Thursday | Afrikan | Late Night | Kelvin (external) | External | 2 bottles |
| Friday | 106 & Friday | All Night | In-House LOV3 | Internal | 3 bottles |
| Saturday | Nothing But R&B | All Night | In-House LOV3 | Internal | 3 bottles |
| Sunday | DAE7 Brunch + Karaoke | Noon – 8pm | Tiffany (external) | External | 2 bottles |
| Sunday | Cassette Sunday | 8pm – 2am | Tony (external) | External | 2 bottles |

### Tab Naming for Promoter Events

To enable clean automated classification, promoter tabs must use this format:

```
Promoter - {Day} - {Event Name} - {Promoter POC}
```

Examples:
- `Promoter - Thursday - Afrikan - Kelvin`
- `Promoter - Sunday - DAE7 Brunch - Tiffany`
- `Promoter - Sunday - Cassette - Tony`
- `Promoter - Friday - 106.9 - In-House`
- `Promoter - Saturday - Nothing But R&B - In-House`

---

## Part 6 — Bottle Tiers

### 6.1 Tier 1 (eligible for promoter comps)

**Definition:** Any bottle with **retail price under $500**.

Examples in current menu (subject to periodic revision):
- Don Julio Reposado
- Casamigos Reposado / Anejo (varies by size)
- Dusse
- Standard house bottles (Wycliffe, etc.)

### 6.2 Tier 2 (NOT eligible for promoter comps)

**Definition:** Retail price $500 and above.

Examples:
- Don Julio 1942
- Clase Azul
- Ace of Spades / Louis XIII
- Any Champagne/Cognac at $500+

**Rule:** Tier 2 bottles rung under a promoter tab automatically trigger a violation
flag regardless of cap remaining. Tier 2 comps require ownership approval.

---

## Part 7 — Enforcement Rules

### 7.1 Cap Enforcement Model

**Soft cap** — the POS does NOT block the ring-in. Enforcement happens at:
1. Tuesday leadership Slack report (visibility)
2. Weekly review meeting (accountability)
3. Promoter payout (financial recovery for external promoters)

### 7.2 External Promoter Overage — Clawback

For **external promoters** (Kelvin, Tiffany, Tony):
- Overage bottles counted at **80% of Tier 1 retail** as a clawback
- Clawback applied against promoter's weekly/monthly payout
- Documented in promoter payout system

### 7.3 In-House Promoter Overage (Fri/Sat)

For **In-House LOV3** events (106 & Friday, Nothing But R&B Saturday):
- No clawback (nobody to bill)
- Overage bottles classified as "Excess In-House Promotional Spend"
- Escalation Slack to ownership when overage happens
- Excess Promotional Spend total published in weekly report

### 7.4 Tier Violation

Any Tier 2 bottle rung under a promoter tab:
- Immediate Slack alert to ownership
- Counts against next promoter payout (external) or logged as excess spend (in-house)
- Manager who authorized flagged in the report

---

## Part 8 — Owner Comp Policy

### 8.1 Owner Personal

- Tabs prefixed `Maurice`, `Eddie`, `Derwin`, `Per Maurice`
- No target — pure audit visibility
- Every use logged in weekly report by owner name and item

### 8.2 Owner Discretionary

- Includes: owner tastings, VIP hosting by ownership, industry standings hosted by owner
- Tab prefix `Owner Tasting` or specific "Owner - {Guest}" format
- Currently unlocked — any manager can press the Owner button. **Pending decision on PIN lock (see 4.1 of leadership review).**

### 8.3 Owner Comp Button — Toast Configuration Recommendation

Recommended cleanup:
- **Delete OWNER-prefixed cost-basis SKUs** (OWNER MOET ROSE, OWNER DON REPO, etc.)
- **Use retail SKUs for all rings** — accurate revenue capture
- **Owner Comp discount code** — 100% off, applied at check level
- **Retail + COGS dual reporting** in `/comps` (both what we gave away AND what it cost us)

---

## Part 9 — Birthday Package Rules

### 9.1 Package Contents

When a birthday reservation meets the criteria, the guest receives:
- Custom dinner menu
- Complimentary champagne bottle (Wycliffe or specified)
- Rose bouquet
- Personal birthday cake
- One hookah
- Shot wheel (party-size dependent)

### 9.2 Eligibility Rules

- **Days:** Friday, Saturday (before 11pm only), Sunday (all day), Wednesday (all day)
- **Reservation:** Pre-registered in SevenRooms with birthday flag
- **Minimum spend:** $200 across food + liquor BEFORE items released
- **Party size:** 4–15 guests

### 9.3 Reconciliation

The Tuesday report reconciles birthday comps against SR reservations:
- Total birthday-flagged SR reservations that checked in
- Total birthday comps recorded in Toast
- Mismatch flags:
  - Birthday comps with NO matching SR reservation → ghost birthday
  - Birthday reservations with NO comps → package not delivered or manual override
  - Birthday comps below $200 spend threshold → policy violation

### 9.4 Item Classification

- Any comp on a `*Birthday*` or `*Bday*` tab → Programmatic — Birthday
- Any BIRTHDAY-prefixed item → Programmatic — Birthday
- Any comp of `Moet Rose Nectar BTL` (or equivalent champagne) on a birthday tab → Birthday

---

## Part 10 — Recovery Bucket Rules

### 10.1 What Counts as Recovery

- Spillage (bartender drops a drink, guest knocks one over)
- Broken bottles (dropped, defective)
- Food quality complaints (undercooked, cold, wrong preparation)
- Foreign objects in food (bug, hair, etc.)
- Wrong item delivered (guest ordered A, got B — remake and comp)

### 10.2 Reason Code Required

Every Recovery comp requires a Toast reason code selection. No "Open $" or "Open %"
ring-ins for recovery.

### 10.3 Target

- **≤ 0.5% of net sales** on a rolling weekly basis
- Above 0.5% triggers a training/kitchen review flag in the Tuesday report

---

## Part 11 — Manager & Bar Lead Scorecards

### 11.1 Manager Category (Tiffany, Tony, Daja)

- Ranked by discretionary + recovery approval $ per week
- Comparison group: other managers only
- Trend line: 4-week rolling
- Outlier flag: > 150% of manager peer median AND > $250 for the week

### 11.2 Bar Lead Category (Ashley)

- **Own category** — separate peer group of one for now
- Metrics: recovery %, discretionary %, item-level ring pattern (Spill / Bug / Bottle Broke frequency)
- Compared against her own 4-week trend, not other categories
- If additional Bar Leads are hired, they join this peer group

### 11.3 Bottle Manager Station (Anonymous)

- Tracked at station level, not personal
- Metrics: total comps, split by bucket (Owner / Programmatic / Discretionary)
- **Pending decision:** convert Bottle Manager to a named role with rotation assignment. See recommendation in Part 15.

---

## Part 12 — Toast Menu Structure (Confirmed Practice)

### 12.1 OWNER-Prefixed SKUs — Kept As-Is

The OWNER-prefixed SKUs (OWNER MOET ROSE, OWNER DON REPO, OWNER ACE, OWNER HENNESSY,
OWNER 1942, OWNER KETEL ONE, OWNER LALO, OWNER WYCLIFF BTL, etc.) are **priced at
cost basis by design** and stay in the menu.

**Why they exist:** Bottles rung as OWNER-prefixed represent situations where an owner
(Maurice, Eddie, Derwin) is personally contributing the bottle — either paying out of
pocket at time of service, or acting in an owner-designated hosting capacity where cost
recovery, not retail markup, is the accounting intent.

The "discount" applied to an OWNER SKU therefore represents the actual cost of goods —
which is the accurate accounting figure for these events. This is NOT a discount from
retail; it's a cost-basis ring-in.

### 12.2 Discount Code Cleanup — Still Recommended

The menu structure stays. What DOES need cleanup is the reason-of-discount codes.
Currently a single "Manager Comp - Item" reason code masks the intent of every ring-in.
Adding purpose-built codes lets the classifier honor tab-name and reason-code signals
in tandem without touching menu SKUs:

- Owner Comp — Discretionary (for owner-hosted guests)
- Owner Comp — Personal (for owner's own tabs)
- VIP Comp
- Promoter Comp — Thursday Afrikan
- Promoter Comp — Friday 106.9 (In-House)
- Promoter Comp — Saturday Nothing But R&B (In-House)
- Promoter Comp — Sunday DAE7 (Tiffany)
- Promoter Comp — Sunday Cassette (Tony)
- Birthday Package
- Wycliffe Welcome (Host Stand)
- Recovery — Spillage
- Recovery — Food Quality
- Recovery — Bottle Broke
- Manager Discretion — Guest Recovery
- Manager Discretion — VIP Upsell

### 12.3 Dual Reporting

`/comps` and the Tuesday report will surface:
- **Retail comp $** — using regular SKU prices when the item was rung at retail
- **Cost-basis $** — using OWNER-prefixed SKU values (the actual cost recovered)
- Both metrics visible so leadership sees the full economic picture

### 12.4 Classifier Behavior With OWNER SKUs

The current classifier already treats OWNER-prefixed items as `owner_discretion`.
That stays. Sub-classification between "owner personally paid" vs "owner-directed
house-eaten" is inferred by:

1. Tab name (`Maurice`, `Eddie`, `Derwin`, `Per Maurice` → Owner Personal)
2. Owner Tasting tab → Owner Discretionary
3. Everything else with OWNER SKU → Owner Discretionary (default)

### 12.5 Timeline

- **Sprint 3 (Aug 10-17):** Toast rep session to add the new reason-code taxonomy above
- No menu SKU changes required
- Staff retraining focuses on reason-code discipline, not menu changes

---

## Part 13 — Weekly Report Format

Every Tuesday 9 AM CT to `#lov3-leader-report`:

```
🔴 LOV3 Comp Report — Week of {Date Range}

Blended: X.XX% of gross revenue (target <4%)
  ✅ or 🔴 Grade

Discretionary + Recovery + Uncategorized combined: X.XX% (target ≤1.5%)
  - Manager Discretionary: X.XX% (target ≤1.0%)
  - Recovery: X.XX% (target ≤0.5%)
  - Uncategorized: $XXX (target zero)

Programmatic (vs plan):
  - Birthday: $X (Y comps · Z SR reservations · reconciliation X/Y ✅)
  - Promoter — Thursday Afrikan (Kelvin): X/2 bottles
  - Promoter — Friday 106.9 (In-House): X/3 bottles
  - Promoter — Saturday Nothing But R&B (In-House): X/3 bottles
  - Promoter — Sunday DAE7 (Tiffany): X/2 bottles
  - Promoter — Sunday Cassette (Tony): X/2 bottles
  - Marketing — Wycliffe Host: $X (Y bottles)
  - Marketing — Distributor/Tastings: $X

Owner (visibility only):
  - Owner Personal — Maurice: $X
  - Owner Personal — Eddie: $X
  - Owner Personal — Derwin: $X
  - Owner Discretionary: $X

VIP Comps: $X (Y guests) [pending bucket definition]

Excess In-House Promotional Spend: $X

Scorecards:
  Managers (Tiffany, Tony, Daja): ranked by discretionary + recovery $
  Bar Lead (Ashley): recovery %, discretionary %, own trend
  Bottle Manager station: total comps by bucket

Flagged:
  ⚠️ Uncategorized comps
  🚨 Promoter cap breaches
  🚨 Tier 2 bottles under promoter tab
  🚨 Ghost birthdays (comp but no SR reservation)
  🚨 Sub-$200 birthday comps
  🔁 SKU/reason/tab mismatches

Retail $ + COGS Impact $ (dual view)
```

---

## Part 14 — Reconciliation Requirements

### 14.1 Weekly Reconciliation

1. **Birthday reconciliation** — SR birthday-flagged reservations vs Toast birthday comps
2. **Promoter cap reconciliation** — Bottle count per event vs cap
3. **Uncategorized comp target** — Should be zero; anything above triggers reason-code enforcement escalation
4. **Recovery target check** — Above 0.5% triggers kitchen/bar quality review

### 14.2 Monthly Reconciliation

1. **Owner comp total** — Sum by owner (Maurice / Eddie / Derwin)
2. **Tier 2 bottle comps** — Any Tier 2 bottles comped this month, with authorizing manager
3. **Promoter payout clawbacks** — External promoter overages logged and applied

---

## Part 15 — Implementation Sequence

| Sprint | Duration | Deliverable |
|---|---|---|
| **1. Leadership sign-off** | Week of Jul 28 | Team approves this document; all opens closed |
| **2. Tab-naming rollout** | Week of Aug 3 | Managers trained on new tab-naming convention; posted at POS stations |
| **3. Toast comp code cleanup** | Weeks of Aug 10–17 | Delete OWNER SKUs, add new discount codes, staff retrained |
| **4. Classifier rewrite** | Week of Aug 17 | `comp_analytics.py` updated: tab-name-first precedence, VIP bucket, marketing bucket, retail+COGS dual reporting |
| **5. Deploy new /comps + Tuesday report** | Week of Aug 24 | Leadership sees clean data |
| **6. First reconciliation** | Sep 1 (Tuesday) | SR birthday reconciliation + promoter cap enforcement live |

---

## Part 16 — Outstanding Decisions

Items surfaced during the 2026-07-29 grilling that still need leadership decisions:

### 16.1 Owner Comp PIN Lock
- Current: any manager can press Owner Comp
- Decision needed: lock behind PIN + owner Slack notification, or trust with post-hoc audit?

### 16.2 VIP Comp Formal Definition
- Who authorizes VIP comps? (Manager PIN only? Ownership only?)
- Is there a VIP guest list?
- Monthly VIP comp cap?

### 16.3 Bottle Manager — Named Role Conversion
- Current: anonymous pooling station
- Recommendation: convert to formal role, assign specific bottle managers per shift with scheduled rotation
- Enables personal accountability while preserving pooling economics

### 16.4 Wycliffe Host-Stand Systematic Ring-In
- Current: not consistently rung when champagne is poured for waiting guests
- Recommendation: every Wycliffe bottle at host stand gets a "Wycliffe - Host Stand" tab and ring, even without a guest check
- Alternative: track by inventory depletion

### 16.5 Distributor Tasting Classification Detail
- Confirm all distributor tastings → Programmatic — Marketing
- "OWNER LALO" from history: was that a distributor tasting (Marketing) or owner personal use? Confirm handling of historical ambiguous items.

---

## Appendix — Industry References

- **Danny Meyer / Union Square Hospitality** — comp rate as % of net revenue, categorization by intent
- **TAO Group nightlife** — bottle service comp caps, promoter clawback contracts
- **Cornell School of Hotel Administration** — Prime Cost management including comp discipline
- **Toast POS best practice** — retail-based ringing with intent-driven discount codes

---

**Sign-Off Section (to be completed at leadership meeting):**

- [ ] Maurice Ragland (Owner) — ______________
- [ ] Eddie ______________ (Owner) — ______________
- [ ] Derwin ______________ (Owner) — ______________
- [ ] Tiffany Loving (Manager) — ______________
- [ ] Anthony Winn (Manager) — ______________
- [ ] Dajah Bishop (Manager) — ______________
- [ ] Ashley Baines (Bar Lead) — ______________

---

*Document maintained at `/COMP_MANAGEMENT_POLICY.md`. Revisions require leadership review.*
