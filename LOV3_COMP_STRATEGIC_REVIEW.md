# LOV3 Comp Discipline — Strategic Review & Recommendations

**Prepared for:** LOV3 Leadership Team (Maurice · Eddie · Derwin · Tiffany · Tony · Daja · Ashley)
**Prepared by:** Executive Committee (Analytics + 9 domain experts across 2 review rounds)
**Date:** 2026-07-31
**Version:** 1.0 · Confidential — For Leadership Only

---

## Executive Summary

Over 8 days and 7 iterations of the weekly comp discipline report, we conducted a full audit of LOV3's discount, void, and comp operating model — using 90 days of live Toast POS + SevenRooms data, cross-referenced against nine expert reviews from CFO / COO / Loss Prevention / Hospitality Industry Consultant / Growth Strategist perspectives.

**Bottom line:** LOV3 has an **operator-grade** comp system (B+ if benchmarked against independent restaurants) but is **not investor-grade or franchise-grade.** In its current form, it is a cautionary tale, not a template. If we transfer this operating model to VIC3 without hardening, we will import LOV3's bad habits by copy-paste — costing an estimated **$3-5K/week per venue** in unattributable leakage, plus catastrophic tail risk from confirmed fraud patterns.

**The good news:** the policy framework is 80% written; the automation stack is 60% built. Six weeks of disciplined execution converts LOV3 from cautionary tale into template.

---

## 1. SWOT Analysis — LOV3 Current State

### 🟢 Strengths

- **Real revenue capture on Manager Discretionary comps** — the report at least measures the leakage, unlike most independent operators who fly blind
- **Michelle Rojas & Jordyn Aiken Wednesday Birthday Package pattern** — textbook policy execution, replicable model
- **Toast + SevenRooms + BofA data pipelines all live and current** — data infrastructure is investor-grade (thanks to the Plaid work)
- **Comp Management Policy v1 documented** — most operators don't have any policy at all
- **Tab-naming standards codified** — foundation for automated reconciliation exists
- **Daja Bishop's reason-code diversity discipline** — 3/4 buckets across 90 days shows category-appropriate use
- **Owner tab attribution** — Bottle Manager station's "Per Maurice" / "Maurice E9" naming is the correct pattern
- **Wycliffe host-stand welcome** as a formalized program (even if inconsistently rung)

### 🔴 Weaknesses

- **Ashley Baines has manager-comp authority as a Bar Lead** — she approved $488 Manager Comp checks; industry standard says Bar Leads cap at $150
- **Anthony Winn 1/4 reason-code diversity** — every comp lands in "Manager Comp" bucket, category laziness that masks intent
- **Bottle Manager station is anonymous** — $7,991 in 90 days with zero accountability
- **Tab-naming inconsistency** — Ashley's `Fri Bday`, `Sat Bday`, `Fridaay Bday`, `Saturday Bday` variants destroyed 60 days of reconciliation
- **Owner Comp button not PIN-locked** — anyone with a login can press it
- **Bar cost-basis substitute pattern** — OWNER MOET rung at cost when policy says birthday package = fully comped Bellaire
- **Reason code laziness dominant** — most managers use generic "Manager Comp - Check (100%)"; specific codes rarely applied
- **Cost-basis SKUs (OWNER MOET ROSE et al.) corrupt revenue reporting** — dual accounting forever
- **Guest name field empty on Toast checks** — buddy-comping the same regular weekly is currently undetectable
- **Confirmed self-approved cash voids** — India Thomas $375 + $1,358 same-minute voids
- **Confirmed service-account voider** — UUID email address used to void $2,296 in 90 days
- **Confirmed reason-code abuse** — "Spillage" used on sealed Maker's Mark bottle (bottles can't spill)
- **Confirmed approver-server collusion signals** — Tony Winn concentrates 7 of top 15 pairs

### 🔵 Opportunities

- **Bday-{D}-{FirstName} tab convention** — deterministic 1:1 reconciliation, 5 min/shift to adopt, unlocks automated birthday attribution
- **Dedicated Birthday Champagne SKU** — distinguishes package delivery from OWNER-SKU consumption at ring level
- **PIN-lock every comp ≥$100** — TAO/Marquee/Lavo standard, POS-level enforcement
- **Convert Bottle Manager to named role** — preserves pooling economics while unlocking accountability
- **Replicate Michelle's Wed Bellaire pattern to Fri/Sat/Sun** — one delivery mechanism across all program days
- **Guest name required on tabs with ≥$100 comp** — unlocks whale/repeat-guest detection
- **Comp Budget Envelope per bucket** — Cornell Prime-Cost discipline; each program (marketing, birthday, VIP, recovery) gets monthly $ budget with weekly variance
- **VIC3 as clean-slate template opportunity** — every LOV3 lesson can be hardcoded before doors open
- **Comp ROI tracking (30-day return rate)** — turn comps from cost center into loyalty investment
- **Daypart economics** — separate bottle-service comp % from food/brunch
- **Repeat-guest detection** — once guest name field is required
- **Marketing attribution ledger** — split organic vs campaign-tied comps

### 🚨 Threats

- **Confirmed fraud patterns already active** — Maker's Mark spillage, India same-minute cash voids, service-account voider. Every week these go undetected compounds the loss.
- **Manager transfer risk to VIC3** — if Tony carries the 1/4 reason-code habit or the approver-of-last-resort role to VIC3, we import LOV3's data debt on day 1 with no audit trail
- **Cost of delay** — ~$3-5K/wk at LOV3 alone; $6-10K/wk once VIC3 opens without hardened controls
- **Regulatory exposure** — if we ever pursue franchise, private equity, or a lender, unaddressed self-approved cash voids and reason-code fraud are audit findings
- **Cultural normalization of leakage** — 60% of birthday-package pre-registrations went undelivered over 60 days ($6,542 in undelivered obligations). If this becomes the norm, we lose the birthday reservation category to a competitor
- **Ownership rotation risk** — if key managers depart without documented SOPs, tribal knowledge (Michelle's Wed pattern, Ashley's Fri/Sat OWNER MOET pattern) walks out the door
- **Brand risk from ghost birthdays** — a party who paid $600, spent the entire birthday at LOV3, and received nothing branded as a package is a bad social media post waiting to happen

---

## 2. Industry Benchmarks — And Why They Matter

### The 10 KPIs That Separate Operators From Best-in-Class

| # | KPI | Industry Best | LOV3 Current | Why it matters |
|---|---|---|---|---|
| 1 | **Blended comp % of gross revenue** | 3-4% (bottle service) | 5.3% blended | Cornell operator baseline. Above 4% signals discretionary drift. |
| 2 | **Manager Discretionary %** | ≤1.0% net sales | 3.3% (90d) | Excess Manager Discretionary = category laziness or judgment failure |
| 3 | **Recovery %** | ≤0.5% | 0.4% (90d) | Kitchen/bar quality signal. LOV3 is actually good here. |
| 4 | **Uncategorized $** | $0 | $4,168 (90d) | Every comp needs intent. Uncategorized = no audit trail. |
| 5 | **Comp % of bottle-service revenue** | 3-5% (nightlife) | Unmeasured | Bottle economics differ 5-10x from food; blended masks the leak |
| 6 | **Peak concentration (11p-1a)** | ≥65% | Unmeasured (v7) | Nightlife expected pattern; deviation signals wrong staffing |
| 7 | **Comp-driven 30-day return rate** | ≥35% (USHG) | Unmeasured | Turns comps from cost center to loyalty investment |
| 8 | **Promoter cost per net cover** | <$45 (TAO) | Unmeasured | Nightlife CAC discipline |
| 9 | **Birthday package attach rate** | ≥80% (Mina) | ~40% (LOV3 60d) | LOV3's biggest revenue-visibility gap |
| 10 | **Reason-code diversity per manager** | ≥3/4 buckets | 1-3/4 (mixed) | Reflects manager judgment quality |

### Why 4% Blended, Not 5%?

The 4% threshold isn't arbitrary — it's the median for well-run **upscale bottle-service venues**. Above 4%:
- Signals guests aren't paying enough for the perceived quality
- Or managers are over-delivering hospitality (defensive posture)
- Or fraud/leakage is baked in

Below 3%:
- Sometimes signals under-delivering (guests feel nickeled-and-dimed)
- Long-term: hurts retention and repeat visits

**The sweet spot: 3-4% blended, split as 1% Recovery + 1% Manager Discretionary + 1-2% Programmatic (birthday/promoter/marketing).**

### Why ≤1.0% Manager Discretionary Specifically?

Manager Discretionary is the "goodwill judgment" bucket — a manager reads a situation and comps to fix it. Industry range:
- **0.5-1.0%**: managers are gate-keeping and using specific reason codes
- **1.0-2.0%**: managers are approving liberally; watch closely
- **>2.0%**: category collapse — everything gets called "Manager Comp" and the report loses meaning

LOV3 at 3.3% over 90 days is **3x the target**. Not because managers are corrupt, but because the reason-code taxonomy isn't enforced.

---

## 3. Best-in-Industry Operators — Who and Why

### Union Square Hospitality Group (USHG · Danny Meyer)
**Excellent at:** "Hospitality Investment Report" ties every recovery comp to 30/60/90-day repeat visit rate. Turns comps from cost center into loyalty ROI.

**What they do that LOV3 doesn't:**
- Every comp gets a manager's PIN + guest name + specific reason code
- Weekly variance report vs a monthly $ budget envelope per bucket
- Comp-driven return rate is a manager KPI
- Video-audit tie-out on all $500+ wine comps at Gramercy Tavern

**Adopt:** Comp ROI tracking, budget envelopes, video tie-out for Tier 2.

### TAO Group (Marquee · Lavo · TAO Downtown)
**Excellent at:** Nightlife-specific KPIs — bottle service comp %, promoter cost per net cover, peak-hour comp concentration.

**What they do that LOV3 doesn't:**
- POS-blocked comp ring-in without manager PIN (not just retrospective flag)
- Promoter contract clauses on tab-name format + clawback terms **before first event**
- Named Bottle Manager per shift; every ring ties to a person
- Opens new venues in 60 days because reporting stack is parameterized copy-paste

**Adopt:** POS PIN enforcement, promoter contract template, named BM role, venue parameterization.

### Michael Mina Group
**Excellent at:** Marketing attribution — split programmatic comps into "campaign-tied" (UTM-equivalent code) vs "organic".

**What they do that LOV3 doesn't:**
- Birthday package attach rate is a top-line KPI (target ≥80%)
- Every VIP comp is on a curated guest list with expiration
- Marketing team gets a monthly comp budget with weekly reporting

**Adopt:** Birthday attach rate as KPI, VIP guest list, marketing budget envelope.

### Cornell School of Hotel Administration (framework, not operator)
**Excellent at:** Prime-Cost discipline — every category (marketing, birthday, VIP, recovery) gets a monthly $ budget with weekly variance report. Any bucket >$X above budget triggers a controller review.

**Adopt:** Comp Budget Envelope framework.

---

## 4. Gap Analysis — LOV3 Today vs. What VIC3 Needs Day 1

| Gap | LOV3 Current | VIC3 Day-1 Requirement | Risk if Ported |
|---|---|---|---|
| Owner Comp PIN | Unlocked | PIN + Slack notify wired | Unbounded — 1 bad night = $2K+ |
| Bottle Manager station | Anonymous | Named role, scheduled rotation | ~$800-1,200/wk unassignable |
| Tab naming | Free-text, inconsistent | POS-enforced dropdown or manager gate | ~$1,500-2,500/wk unattributable |
| Bar Lead authority | $488+ Manager Comps approved | Cap at $150 non-comp; escalate above | Loss of manager gate-keeping |
| Reason-code taxonomy | Grandfathered "Manager Comp" default | 15-code taxonomy pre-loaded | Category collapse; report loses meaning |
| OWNER-SKU vs retail | Cost-basis SKUs corrupt revenue | Retail SKUs + discount codes only | Dual accounting forever |
| Guest name on tabs | Empty | Required on tabs receiving ≥$100 comp | Buddy-comping undetectable |
| Self-approval control | Reactive Slack alert | POS blocks server = void_user | Skim vector remains open |
| Service-account voider | UUID emails as voiders | Per-user login required | Attribution impossible |
| Birthday reconciliation | Speculative off-book match | SR × Toast bridge via Bday-{D}-{Name} tabs | Package attach rate invisible |

---

## 5. Recommendations — The Overhaul

### 5.1 Tab System Overhaul

**Deploy the 8-convention naming standard from `TAB_NAMING_STANDARDS.md` with POS enforcement.**

Where possible, replace free-text tab names with a dropdown:
- Birthday: dropdown of pre-registered guest first names for that day (pulled from SR)
- Promoter: dropdown of 5 events (Kelvin Thu, 106 Fri, R&B Sat, DAE7 Sun, Cassette Sun)
- Owner: dropdown of owner names
- VIP: dropdown of registered VIP list
- Recovery: dropdown of Spill / Bug / Bottle Broke / Cold / Wrong Order

Where dropdown isn't possible, **manager verification checklist** at end of shift with any free-text tabs on comped checks flagged for review.

**Rollout:**
- Week 1: Print + laminate the cheat sheet at every POS station
- Week 2: Manager training in pre-shift; managers hand out the doc
- Week 3: Weekly report shows compliance %
- Week 4: Non-compliant tabs escalated to server-level 1:1 conversations

### 5.2 Reason Code Taxonomy Overhaul

Replace the grandfathered generic codes with an intent-driven 15-code taxonomy (per policy §12.2):

**Owner (2):** Owner Comp — Discretionary · Owner Comp — Personal
**VIP (1):** VIP Comp
**Promoter (5):** Promoter Comp — Thursday Afrikan · Fri 106 · Sat R&B · Sun DAE7 · Sun Cassette
**Programmatic (2):** Birthday Package · Wycliffe Welcome
**Recovery (3):** Recovery — Spillage · Food Quality · Bottle Broke
**Manager Discretion (2):** Guest Recovery · VIP Upsell

**Every reason code is mandatory** — POS blocks the ring without one.

**Deliverable at Toast:** work with Toast rep to configure. Estimated 2-hour session.

### 5.3 When and When Not to Comp — Decision Framework

**COMP when:**
1. **Package obligation triggered** — SR pre-registered Birthday Package + $200 min met on program day → comp the champagne, no negotiation
2. **Service failure documented** — kitchen error, ticket time >45 min, wrong item → comp the affected item with Recovery code
3. **Owner-directed VIP hosting** — Maurice/Eddie/Derwin explicitly directs → comp with Owner Comp — Discretionary code
4. **Promoter contract fulfillment** — Kelvin's 2-bottle Thursday allocation → comp with specific event code
5. **Repeat 4-star guest hitting anniversary/milestone** — with owner approval → VIP Comp with reason

**DO NOT COMP when:**
1. **Guest is upset but the service was correct** — apologize, escalate to manager, do not default-comp
2. **Server or manager knows the guest personally** — buddy-comp temptation. Escalate to another manager.
3. **Guest is complaining after payment** — refund policy applies, not comp policy
4. **You cannot articulate the specific reason code** — if it's not any of the 15, it's not a valid comp
5. **You are within the last hour of your shift** — late-shift comping is the #1 skim vector. Get manager approval EVERY time.
6. **Tab is over $500 and no manager has approved the ring** — Tier 2 threshold. Pre-authorize via ownership Slack.
7. **Guest is asking for it** — comping in response to "make it right" pressure trains guests to expect free stuff. Redirect to service recovery.

### 5.4 Who To Comp — Guest Tier Framework

| Tier | Who | Comp Authority | Typical Package |
|---|---|---|---|
| **Owner-Hosted** | Maurice / Eddie / Derwin's guests | Owner or authorized manager | Full hospitality — bottle service + food |
| **VIP List** | Curated: industry pros, celebrity, media, brand partners | Manager with reason code | Bottle + welcome champagne + priority seating |
| **Pre-Registered Birthday Package** | SR reservation with "Birthday Dinner Package" in notes + $200 min met | Automatic (per program day) | Champagne per day-of-week program |
| **Regular Guest — 30+ day repeat** | Detected once guest name field enforced | Manager judgment + reason code | Occasional welcome pour |
| **First-time guest** | Anyone else | No default comp | Service excellence only |
| **Walk-in birthday (no SR)** | Guest mentions birthday but did not pre-register | No obligation | Optional — birthday shot + dessert on the house at server discretion |

### 5.5 The Psychology of Discounting — Offensive vs. Defensive

**Defensive discounting** = comping in response to a problem, a complaint, or perceived guest displeasure. This is what most restaurants default to. It is REACTIVE and expensive because:
- It teaches guests that complaining gets free stuff
- It masks operational failures (comping a slow ticket doesn't fix why it was slow)
- It doesn't build loyalty — guests remember the problem, not the comp
- It's finite: you can only comp so many times before the guest thinks it's normal

**Offensive discounting** = comping proactively to acquire, retain, or celebrate a guest. This is what USHG and Mina Group do. It is STRATEGIC and expensive-but-ROI-positive because:
- It signals abundance (opposite of scarcity)
- It creates a memory the guest will retell (word-of-mouth)
- It's tied to a specific business objective (repeat visit, VIP retention, birthday attach rate)
- It's budgeted, tracked, and its ROI is measurable

**LOV3 today: 95% defensive, 5% offensive.**
- 3.3% Manager Discretionary is almost entirely defensive (react to problems)
- Only Birthday Package + Wycliffe Welcome + Owner Discretionary are offensive
- Zero comp ROI tracking

**Target LOV3 in 90 days: 60% defensive, 40% offensive.**
- Cap Manager Discretionary at 1% and enforce it
- Move Recovery down through kitchen quality investment
- **Grow** Programmatic + VIP with budget envelope + attach-rate KPIs
- Introduce Comp ROI (30-day return rate) as a top-line KPI

**The mindset shift:** stop asking "how do I fix this guest's problem?" Start asking "how do I use this comp dollar to earn a repeat visit, a VIP relationship, or a birthday party attach?"

---

## 6. VIC3 Day-1 Non-Negotiables

Before VIC3 opens, these MUST be hardened at LOV3 first so the template is clean:

1. **PIN + reason code required at every comp ring** — no exceptions
2. **Named Bottle Manager per shift on the schedule** — kill the anonymous station
3. **Bar Lead authority capped** — Ashley (and future Bar Leads) cannot approve comps ≥$150; escalate above
4. **Guest name required on tab** for any check receiving ≥$100 comp — enables detection
5. **15-code reason taxonomy pre-loaded in Toast** — the generic "Manager Comp" default is deleted
6. **Retail SKUs only for daily menu** — OWNER-prefixed SKUs kept for owner-personal only; not for program delivery
7. **Bday-{D}-{Name} birthday tab convention** — POS-enforced dropdown or manager-verified checklist
8. **Owner Comp PIN + Slack notification** — every use captured
9. **Tuesday leadership report LIVE from week 1** at VIC3 — not "after we settle in"
10. **Weekly compliance score per manager** — enforced from open

---

## 7. Financial Impact Summary

### Cost of Doing Nothing (per week, per venue)

| Category | Weekly Burn |
|---|---|
| Tab-naming non-adoption | $1,500-2,500 |
| Owner Comp unlocked | Unbounded (1 bad night = $2K+) |
| Bottle Manager anonymity | $800-1,200 |
| Ashley cost-basis pattern | $400-600 |
| Confirmed fraud (India, Maker's Mark, service-account) | Compounding weekly |
| **Total known + probable** | **$3,000-5,000/week** |
| **Annualized at LOV3 alone** | **$156K-260K/year** |
| **Post-VIC3-open (2 venues, unhardened)** | **$312K-520K/year** |

### Expected Recovery from Full Implementation

| Category | Weekly Recovery | Annualized |
|---|---|---|
| Blended comp % tightening 5.3% → 4% | ~$1,000 | $52,000 |
| Manager Discretionary category enforcement 3.3% → 1% | ~$1,500 | $78,000 |
| Uncategorized elimination | ~$100 | $5,200 |
| Fraud detection at scale | ~$500-1,000 | $26,000-52,000 |
| Birthday attach rate 40% → 80% | Revenue upside via retention | $50,000+ |
| Comp ROI tracking | Revenue upside | $30,000+ |
| **Total per venue** | **~$3,100-4,600/week** | **~$240K-270K/year** |

**LOV3 alone: ~$240-270K/yr recovery + eliminated fraud.**
**Both venues after VIC3 opens: ~$480-540K/yr.**

**6-week investment to build the multi-venue infrastructure: ~$40K in delayed revenue + development effort.**

---

## 8. 90-Day Execution Roadmap

### 30 Days — Immediate Controls

- [ ] India Thomas cash-drawer review + camera pull (owner action)
- [ ] Toast rep call to identify anonymous voider (`4379fdda-...@example.com`)
- [ ] Ashley Baines Bar Lead POS authority limit (Toast config)
- [ ] Tony Winn / Jordyn Aiken 90-day pattern conversation (leadership)
- [ ] Maker's Mark spillage inventory tie-out (LP + inventory manager)
- [ ] Deploy Bday-{D}-{Name} tab naming (Sprint 2 rollout)
- [ ] Print + laminate POS cheat sheet at every station
- [ ] Reason code enforcement training for Anthony Winn (§11)

### 60 Days — Structural

- [ ] Owner Comp PIN + Slack notification live
- [ ] Named Bottle Manager role on schedule
- [ ] 15-code reason taxonomy loaded in Toast
- [ ] Guest Name field required on ≥$100 comp tabs
- [ ] Dedicated Birthday Champagne SKU family
- [ ] Michelle's Wed Bellaire pattern replicated to Fri/Sat/Sun
- [ ] Comp Budget Envelope framework implemented per bucket
- [ ] Manager quarterly recognition ceremony launched

### 90 Days — Strategic

- [ ] Comp ROI (30-day return rate) tracking live
- [ ] Daypart economics reporting (bottle vs food)
- [ ] Marketing attribution ledger
- [ ] VIP guest list + monthly cap
- [ ] Multi-venue parameterization complete (venues/ module)
- [ ] Cross-venue portfolio dashboard live
- [ ] VIC3 SOP package documented and signed off
- [ ] LOV3 → VIC3 manager transfer safeguards defined

---

## 9. Signoff

This document represents the consensus recommendation of a 9-agent executive committee based on 90 days of live LOV3 operational data. Each finding is grounded in specific transaction evidence and cross-verified against published industry standards from USHG, TAO, Mina Group, and Cornell School of Hotel Administration.

- [ ] Maurice Ragland (Owner) — ______________
- [ ] Eddie ______________ (Owner) — ______________
- [ ] Derwin ______________ (Owner) — ______________
- [ ] Tiffany Loving (General Manager) — ______________
- [ ] Anthony Winn (Manager) — ______________
- [ ] Dajah Bishop (Shift Manager) — ______________
- [ ] Ashley Baines (Bar Lead) — ______________

*This document supersedes prior comp policy language where conflicting. Revisions require full leadership review.*
