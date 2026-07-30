# LOV3 POS Tab Naming Standards

**Status:** DRAFT (compiled 2026-07-30) — foundation for future staff training manual
**Owner:** Maurice Ragland
**Enforcement:** Manager-verified at end of shift; auto-parsed by `birthday_reconciliation.py` and `comp_analytics.py`

Every Toast tab that will generate a comp, promo, or reconciliation event MUST follow one of the conventions below. Consistent naming enables automated reporting, prevents inventory double-counting, and gives leadership an audit trail.

---

## 1. Birthday Package Tabs

### 1.1 Pre-registered Birthday Dinner Package (per-guest tab)

**Format:** `Bday-{DayLetter}-{GuestFirstName}[-{LastInitial}]`

**Day letters:**
- `W` — Wednesday
- `F` — Friday
- `S` — Saturday
- `U` — Sunday

**Examples:**
- `Bday-W-Michelle` — Wednesday birthday for Michelle
- `Bday-F-Kelly-A` — Friday birthday for Kelly Ashley (last initial disambiguates when multiple same-first-name parties)
- `Bday-S-Naya-N` — Saturday birthday for Naya Norfleet
- `Bday-U-Ceriah` — Sunday birthday for Ceriah

**Rules:**
- **One tab per guest** — never combine multiple birthday parties on a single tab
- First name must match the SR reservation's `first_name` exactly (case-insensitive)
- Use last initial when two guests share a first name that night
- Full day names also accepted (`Bday-Friday-Kelly`) but abbreviated form preferred

### 1.2 Non-Package Birthday (guest mentioned birthday but did NOT pre-register)

**Format:** Standard table/tab; do NOT use the `Bday-` prefix

**Rationale:** these guests have no package obligation. Using `Bday-` prefix incorrectly would confuse reconciliation.

### 1.3 Legacy Generic Birthday Tabs (deprecated)

Deprecated but still in the historical data:
- `Sat Bday`, `Fri Bday`, `Saturday Bday`, `Friday Bday`, `Fridaybday`
- `Wednesday Bday Package`, `Bday Comp-{server}`
- Just `Bday`, `Birthday`, `Birthday (Braids)`

**Migration:** rename all future birthday tabs per Section 1.1. Reconciler still credits these as "off-book delivery" but cannot attribute to a specific guest.

---

## 2. Promoter Tabs

**Format:** `Promoter - {DayName} - {EventName} - {POC}`

**Approved events (as of 2026-07-30):**

| Day | Event | POC | Cap |
|---|---|---|---|
| Thursday | Afrikan | Kelvin | 2 bottles Tier 1 |
| Friday | 106 & Friday | In-House LOV3 | 3 bottles Tier 1 |
| Saturday | Nothing But R&B | In-House LOV3 | 3 bottles Tier 1 |
| Sunday | DAE7 Brunch + Karaoke | Tiffany | 2 bottles Tier 1 |
| Sunday | Cassette Sunday | Tony | 2 bottles Tier 1 |

**Examples:**
- `Promoter - Thursday - Afrikan - Kelvin`
- `Promoter - Sunday - DAE7 Brunch - Tiffany`
- `Promoter - Sunday - Cassette - Tony`
- `Promoter - Friday - 106 - In-House`
- `Promoter - Saturday - Nothing But R&B - In-House`

**Rules:**
- Bottles rung on these tabs count against the promoter's cap
- Tier 2 bottles (retail ≥$500) on a promoter tab = automatic ownership alert
- Overages counted at 80% clawback (external) or logged as excess in-house spend

### 2.1 Deprecated Promoter Signals

Historical signals that reconciler still catches but should be phased out:
- `Comp Promo Thurs`, `Igbo`, `Afrikan`
- Bare `Tiffany` / `Tony` / `Keith` / `Anno` / `Swan` on their respective daypart

---

## 3. Owner Tabs

### 3.1 Owner Personal Consumption

**Format:** `Owner - {OwnerName}[-{GuestName}]`

**Examples:**
- `Owner - Maurice` — Maurice's own tab
- `Owner - Maurice - Alex` — Maurice hosting Alex
- `Owner - Eddie`
- `Owner - Derwin`

Owner names allowed: `Maurice`, `Eddie`, `Derwin` (case-insensitive)

### 3.2 Owner Tasting (owner-hosted event)

**Format:** `Owner Tasting - {DistributorOrItemName}`

**Examples:**
- `Owner Tasting - Lalo`
- `Owner Tasting - Casamigos`

### 3.3 Deprecated Owner Signals

Historical:
- `Maurice`, `Per Maurice`, `Maurice E9`, `Maurice's Table (Alex)`, `Maurce E12` (typo)
- Classifier still catches these but standardize going forward

---

## 4. VIP Tabs

**Format:** `VIP - {GuestName} - {Reason}`

**Examples:**
- `VIP - Chef Torres - Industry`
- `VIP - DJ Kayla - Brand Partner`
- `VIP - Music Producer Blake - Media`

**Rules:**
- Manager approval required before opening
- Reason field must specify category (Industry, Brand Partner, Media, Guest-of-Owner, etc.)
- Monthly cap TBD by leadership

---

## 5. Bottle Service Tabs (Guest-Paid)

Standard bottle service, no comp:

**Format:** `{TableCode} - {GuestFirstName}` OR `{GuestFirstName} - {TableCode}`

**Examples:**
- `E5 Ceriah` (table E5 booked by server Ceriah)
- `I3/Ceriah` (table I3 shared with Ceriah)
- `Marcus E3` (Marcus's table E3)

**Rules:**
- Bottle Manager's default; used when the party purchases bottles at retail
- Guest name required for identification
- Include table code so cross-reference works with reservation seating

---

## 6. Wycliffe Host-Stand Champagne

**Format:** `Wycliffe - Host Stand` OR `Door`

**Rationale:** Every Wycliffe bottle poured for waiting guests at the host stand should be rung on a dedicated tab, even if not tied to a guest check.

**Examples:**
- `Wycliffe - Host Stand`
- `Door` (legacy, still accepted)

**Rules:**
- Bottle goes out with `OWNER WYCLIFF BTL` SKU rung and comped
- Enables inventory tracking for the guest-welcome program

---

## 7. Recovery / Spillage Tabs

Server-side shortcuts for kitchen/bar quality issues:

**Format:** `{Reason} - {Item}`

**Examples:**
- `Spill - Don Julio`
- `Bottle Broke - Makers`
- `Bug - Fries` (bug found in food)
- `Wrong Order - Chicken Wings`

**Rules:**
- Reason keyword required at start (`Spill`, `Bug`, `Bottle Broke`, `Wrong Order`, `Cold`, `Undercooked`)
- Item name follows for inventory tracking
- Route to `Recovery` bucket per §10 of `COMP_MANAGEMENT_POLICY.md`

---

## 8. Distributor / Marketing Tastings

**Format:** `Tasting - {DistributorName}` OR `Distributor - {DistributorName}`

**Examples:**
- `Tasting - Sazerac`
- `Distributor - Southern Glazer`
- `Tasting - Casamigos Reserve`

**Rules:**
- Route to `Programmatic — Marketing` bucket
- Whether guest-facing or internal, both count as marketing spend

---

## 9. Regular Guest Tabs (No Special Category)

Any tab that doesn't fit above categories = standard guest tab.

**Format:** `{TableCode}` OR `{TableCode} - {GuestName}` OR `{GuestName}`

**Examples:**
- `T4`
- `E12 - Sarah`
- `Bar - Kayla`

**Rules:**
- No comp obligation; no reconciliation flag
- Just enables clear identification for staff

---

## 10. Enforcement & Migration

### 10.1 Rollout Sequence

1. **Week 1 (this week):** Managers train on the Bday and Promoter conventions during Monday pre-shift
2. **Week 2:** Full staff meeting; managers hand out this doc
3. **Week 3:** Reconciler emails Slack alert to any manager whose shift had non-compliant tabs
4. **Week 4+:** Compliance score published in Tuesday leadership report

### 10.2 Manager End-of-Shift Ritual

**5 minutes at close:**

1. Manager pulls SR birthday reservations for tonight (via SR portal or Slack bot)
2. In Toast, filter/search tabs by `Bday-{TodayLetter}-`
3. Cross-check every pre-registered SR birthday guest against a matching tab
4. For any missed pre-reg guest: note reason (party declined, closed early, etc.)
5. Post to `#lov3-leader-report`:
   > "Sat Bday check: 5/7 delivered. Missed: Sarai (declined), Chelsea (left early)"

### 10.3 Auto-Reconciliation

`birthday_reconciliation.py` parses `Bday-{D}-{Name}` tabs and deterministically pairs to SR reservations by first-name + date. When adoption reaches 80%+, the "off-book speculative match" category will collapse to zero.

---

## 11. Quick-Reference Sheet (for POS station laminate)

```
BIRTHDAY (per-guest):     Bday-{W|F|S|U}-{FirstName}[-{LastInitial}]
PROMOTER:                 Promoter - {Day} - {Event} - {POC}
OWNER PERSONAL:           Owner - {Maurice|Eddie|Derwin}[-{Guest}]
OWNER TASTING:            Owner Tasting - {Item}
VIP:                      VIP - {Guest} - {Reason}
BOTTLE SERVICE:           {TableCode} {GuestName}
WYCLIFFE DOOR:            Wycliffe - Host Stand
RECOVERY:                 {Reason} - {Item}
DISTRIBUTOR:              Tasting - {Distributor}
REGULAR:                  {TableCode} or {GuestName}
```

---

## 12. Version History

- **v0.1 (2026-07-30):** Initial capture. Compiled from 90-day audit + policy §3-§12. Awaiting leadership sign-off + staff training rollout.

---

*This document is the source of truth for tab-naming conventions. Any change requires leadership review because the reconciler + comp classifier depend on stable rules.*
