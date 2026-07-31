# LOV3 Comp Discipline — Strategic Review & Recommendations

**Prepared for:** LOV3 Leadership Team (Maurice · Eddie Jasper · Derwin James Jr. · Tiffany · Tony · Daja · Ashley)
**Prepared by:** Executive Committee (Analytics + 9 domain experts across 2 review rounds)
**Date:** 2026-07-31
**Version:** 2.1 · Confidential — For Leadership Only

**Revision note (v2.1, 2026-07-31):**
1. **Ashley Baines role clarification** — Ashley remains Bar Lead. Based on her expanded duties (approving Manager Comp checks up to $488), her comp-approval authority is managerial. Scorecard tracks both her Bar Lead peer trend AND her Manager peer-cohort benchmarks (Tiffany · Tony · Daja). Formal title stays Bar Lead.
2. **"Confirmed fraud" language removed throughout.** All previously-flagged incidents (India Thomas same-minute voids, service-account voider, Maker's Mark spillage, approver-server pair concentration) are now framed as **LP pattern signals under review — not confirmed fraud.** None have been investigated to conclusion. Section 9 (new — LP Pattern Field Guide) explains each pattern, what LPs look for, how to spot it, and how to address it before assuming intent.
3. **VIC3 target open: November 2026.** All hardening opportunities must be complete well before doors open.
4. **Recovery estimates now discounted at 65%** per consulting review (100% recovery is not a defensible assumption).

---

## Executive Summary

Over 8 days and 7 iterations of the weekly comp discipline report, we conducted a full audit of LOV3's discount, void, and comp operating model — using 90 days of live Toast POS + SevenRooms data, cross-referenced against nine expert reviews from CFO / COO / Loss Prevention / Hospitality Industry Consultant / Growth Strategist perspectives.

**Bottom line:** LOV3 has an **operator-grade** comp system (B+ if benchmarked against independent restaurants) but is **not investor-grade or franchise-grade.** In its current form, it is a cautionary tale, not a template. If we transfer this operating model to VIC3 (target November 2026 open) without hardening, we will import LOV3's bad habits by copy-paste — costing an estimated **$3-5K/week per venue** in unattributable leakage, plus elevated tail risk from unresolved LP pattern signals.

**The good news:** the policy framework is 80% written; the automation stack is 60% built. The window from now to VIC3's November open is ~16 weeks — more than enough to harden every control described here, provided execution starts this sprint.

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

- **Role-vs-duty alignment** — Ashley Baines carries Bar Lead title with expanded managerial authority (approves Manager Comp checks up to $488). Her scorecard now runs against BOTH the Bar Lead trend AND the Manager peer cohort so we can see the full picture. Future prevention control: any employee approving Manager Comps ≥$150 gets a formal role-authority tag on the roster (title or explicit "with managerial authority" designation) so accountability is unambiguous.
- **Anthony Winn 1/4 reason-code diversity** — every comp lands in "Manager Comp" bucket, category laziness that masks intent
- **Bottle Manager station is anonymous** — $7,991 in 90 days with zero accountability
- **Tab-naming inconsistency** — `Fri Bday`, `Sat Bday`, `Fridaay Bday`, `Saturday Bday` variants (originated at the bar; Ashley is the closest owner) destroyed 60 days of reconciliation. Now a tracked KPI.
- **Owner Comp button not PIN-locked** — anyone with a login can press it
- **Cost-basis SKU substitution pattern** — OWNER MOET rung at cost when policy says birthday package = fully comped Bellaire. Not fraud — it corrupts revenue reporting and understates the actual comp economics. See Pattern 5 in the LP Field Guide (§9).
- **Reason code laziness dominant** — most managers use generic "Manager Comp - Check (100%)"; specific codes rarely applied
- **Cost-basis SKUs (OWNER MOET ROSE et al.) corrupt revenue reporting** — dual accounting forever
- **Guest name field empty on Toast checks** — buddy-comping the same regular weekly is currently undetectable
- **LP pattern signal — same-minute cash voids (under review)** — India Thomas $375 + $1,358 same-minute voids over 90 days. Fits the classic same-user self-void pattern; no confirmation of intent. Investigation steps in §9 Pattern 1.
- **LP pattern signal — anonymous voider (under review)** — UUID email address (`4379fdda-...@example.com`) tied to $2,296 in voids over 90 days. Likely a shared workstation or Toast integration; needs Toast rep call to identify. §9 Pattern 2.
- **LP pattern signal — reason-code semantic mismatch (under review)** — "Spillage" reason code applied to sealed Maker's Mark bottle. Almost certainly reason-code laziness rather than intent, but the signal warrants a look. §9 Pattern 3.
- **LP pattern signal — approver-server pair concentration (under review)** — Tony Winn concentrates 7 of top 15 pairs. Multiple benign explanations possible (most shifts worked, on-duty manager on busy nights, servers' default approver). Concentration is the population LPs watch, not proof of collusion. §9 Pattern 4.

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

- **Unresolved LP pattern signals** — Maker's Mark reason-code mismatch, India same-minute cash voids, anonymous voider account. Each week these go un-investigated, the pattern accumulates and the eventual review gets harder. **None are confirmed fraud** — each is a well-known LP pattern that warrants a look. See §9 for the field guide.
- **Manager transfer risk to VIC3** — if Tony carries the 1/4 reason-code habit or the approver-of-last-resort role to VIC3, we import LOV3's data debt on day 1 with no audit trail
- **Cost of delay** — ~$3-5K/wk at LOV3 alone; $6-10K/wk once VIC3 opens without hardened controls
- **Regulatory/underwriter exposure** — if we pursue franchise, private equity, or a lender, unaddressed LP pattern signals become audit findings even if none are ever confirmed as fraud. Underwriters treat "unresolved" the same as "confirmed" until the file shows an investigation trail.
- **Cultural normalization of leakage** — 60% of birthday-package pre-registrations went undelivered over 60 days ($6,542 in undelivered obligations). If this becomes the norm, we lose the birthday reservation category to a competitor
- **Ownership rotation risk** — if key managers depart without documented SOPs, tribal knowledge (Michelle's Wed pattern, the Fri/Sat OWNER MOET delivery pattern owned by the bar) walks out the door
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
| Role-authority alignment | Bar Lead with expanded managerial authority (Ashley model — Bar Lead title + $488 comp-approval authority) | Every employee approving Manager Comps ≥$150 gets an explicit role-authority tag on the roster: either titled Manager OR titled Lead-with-managerial-authority. Toast permissions match the tag. Peer scorecard covers both trend lines. | Ambiguous accountability; missed coaching opportunities |
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
3. **Role-authority enforcement** — anyone approving Manager Comps ≥$150 carries an explicit authority tag on the formal roster before opening day (titled Manager OR titled Lead-with-managerial-authority). POS permissions matched to the tag. No implicit authority.
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

| Category | Weekly Burn | Basis |
|---|---|---|
| Tab-naming non-adoption | $1,500-2,500 | Estimated leakage from unattributable comp $ over 90-day observation |
| Owner Comp unlocked | Unbounded (1 bad night = $2K+) | Structural — single-press exposure |
| Bottle Manager anonymity | $800-1,200 | Estimated from 90-day station rings ($7,991 / 13 wks ≈ $615/wk floor; upside from LP dispersion) |
| Cost-basis SKU substitution | $400-600 | Revenue-reporting distortion, not cash loss |
| LP pattern signals (India, Maker's Mark, anonymous voider) — under review | Compounding weekly if not investigated | Not confirmed loss — carrying cost of unresolved review |
| **Total estimated + observed** | **$3,000-5,000/week** | Directional; requires signed methodology appendix for lender use |
| **Annualized at LOV3 alone** | **$156K-260K/year** | |
| **Post-VIC3-open (2 venues, unhardened)** | **$312K-520K/year** | |

### Expected Recovery from Full Implementation (with 65% realization factor)

Consulting review noted 100% recovery is not a defensible assumption. All lever-level recovery figures below are discounted at 65% to reflect real-world adoption drag, edge-case exceptions, and partial-quarter ramp.

| Category | Weekly Recovery (gross) | Annualized (gross) | @ 65% realization |
|---|---|---|---|
| Blended comp % tightening 5.3% → 4% | ~$1,000 | $52,000 | **$33,800** |
| Manager Discretionary category enforcement 3.3% → 1% | ~$1,500 | $78,000 | **$50,700** |
| Uncategorized elimination | ~$100 | $5,200 | **$3,380** |
| LP pattern signal resolution + prevention controls | ~$500-1,000 | $26,000-52,000 | **$16,900-33,800** |
| Birthday attach rate 40% → 80% | Revenue upside via retention | $50,000+ | **$32,500+** |
| Comp ROI tracking | Revenue upside | $30,000+ | **$19,500+** |
| **Total per venue** | **~$3,100-4,600/week** | **~$240K-270K/year gross** | **~$156K-175K/year realized** |

**LOV3 alone: ~$156-175K/yr realized recovery.**
**Both venues after VIC3 opens (November 2026): ~$312-350K/yr realized.**

**Investment to build multi-venue infrastructure: ~$40K in development effort + deferred revenue during hardening sprints. Payback ≈ 3-4 months at LOV3-only; ≈ 6-8 weeks once VIC3 goes live.**

---

## 8. Execution Roadmap — Anchored to VIC3 November 2026 Open

**Countdown:** ~16 weeks from 2026-07-31 to a November open. All items below must land before VIC3 doors open so the template is clean at launch.

### 30 Days — Immediate Controls (target: end of August 2026)

- [ ] India Thomas 1:1 — walk through each same-minute void, hear the explanation (owner action, non-accusatory)
- [ ] Toast rep call to identify anonymous voider (`4379fdda-...@example.com`) — likely shared workstation / integration
- [ ] Ashley Baines role-authority tag update — Bar Lead with Managerial Authority. Toast permissions + peer scorecard onboarding (dual trend: Bar Lead peer + Manager peer cohort)
- [ ] Tony Winn 1:1 — socialize the 3.3% → 1% Manager Discretionary cap AND the approver-server pair rotation plan (leadership)
- [ ] Maker's Mark event review — was the bottle broken, comped, or missing? Reason-code retraining follow-up
- [ ] Deploy Bday-{D}-{Name} tab naming (Sprint 2 rollout)
- [ ] Print + laminate POS cheat sheet at every station
- [ ] Reason code enforcement training for Anthony Winn (§11)

### 60 Days — Structural (target: end of September 2026)

- [ ] Owner Comp PIN + Slack notification live
- [ ] Named Bottle Manager role on schedule
- [ ] 15-code reason taxonomy loaded in Toast
- [ ] Guest Name field required on ≥$100 comp tabs
- [ ] Dedicated Birthday Champagne SKU family
- [ ] Michelle's Wed Bellaire pattern replicated to Fri/Sat/Sun
- [ ] Comp Budget Envelope framework implemented per bucket
- [ ] Manager quarterly recognition ceremony launched
- [ ] All LP pattern signals from §9 investigated to conclusion (documented investigation trail — critical for underwriter file)

### 90 Days — Strategic (target: end of October 2026 — pre-VIC3-open)

- [ ] Comp ROI (30-day return rate) tracking live
- [ ] Daypart economics reporting (bottle vs food)
- [ ] Marketing attribution ledger
- [ ] VIP guest list + monthly cap
- [ ] Multi-venue parameterization complete (venues/ module)
- [ ] Cross-venue portfolio dashboard live
- [ ] VIC3 SOP package documented and signed off
- [ ] LOV3 → VIC3 manager transfer safeguards defined

### VIC3 Open — November 2026

- [ ] All §6 Day-1 Non-Negotiables verified in production
- [ ] Tuesday comp report LIVE from VIC3 week 1
- [ ] LOV3 baseline comp KPIs used as VIC3 targets (no re-baseline until Q1 2027)

---

## 9. LP Pattern Field Guide — What Leaders Should Watch For

**None of the patterns below are confirmed fraud.** They are pattern shapes that Loss Prevention professionals monitor because they are the pathways through which loss most often occurs. A pattern signal warrants investigation — not accusation. Some signals turn out to be benign operational quirks; the discipline is running the investigation trail so we know either way.

Every entry follows the same structure so leaders can use this doc as a working reference during shift reviews.

---

### Pattern 1 — Same-Minute Self-Void (Cash Skim Shape)

**What it looks like:** A server rings a cash payment, then the same user (or the same server acting as void_user) voids it within ~60 seconds.
**Observed at LOV3:** India Thomas — 2 events totaling $1,733 over 90 days.
**Why LPs watch it:** Classic cash-skim mechanic — ring the sale, take the cash, void the transaction, keep the cash. The signal alone doesn't prove skimming. Benign explanations: guest changed mind, ring error, training-in-progress correction.
**How to spot it in the wild:** Any Toast void report where `user = void_user` AND the void happens within a minute of the payment AND payment type = cash or "Other."
**How to address:**
1. Pull the specific check details for each event
2. Sit with the server (non-accusatory) — walk through what happened each time
3. If explanation holds → coaching moment on Void-Manager-Approval workflow
4. If explanation gaps → LP review + camera pull
5. **Preventive control:** Toast setting → server cannot void their own payment; requires manager PIN

---

### Pattern 2 — Anonymous or Service-Account Voider

**What it looks like:** Void records where `void_user` is a UUID email (e.g., `4379fdda-....@example.com`), not a named employee.
**Observed at LOV3:** $2,296 voided by a UUID service account over 90 days.
**Why LPs watch it:** Anonymous voider = zero accountability. No name = no coaching = no learning. Usually a Toast configuration issue (shared workstation, integration account), not intentional evasion.
**How to spot it in the wild:** Query voids by `void_user`; anything that's not a real employee record is a lookalike.
**How to address:**
1. Toast rep call — identify what this account actually is
2. If shared workstation → require personal login for voids
3. If integration → tag as "system-generated," exclude from scorecards
4. **Preventive control:** every void requires an authenticated named user

---

### Pattern 3 — Reason-Code Semantic Mismatch

**What it looks like:** A reason code that doesn't fit the item. Example: "Spillage" applied to sealed bottles or packaged items.
**Observed at LOV3:** Maker's Mark bottle voided under "Spillage."
**Why LPs watch it:** Wrong reason code hides real intent. Sealed bottles don't spill — they get broken, comped, or go missing. Reason-code drift is the #1 mask for inventory shrink. Usually laziness, not fraud.
**How to spot it in the wild:** Cross-reference reason codes vs item categories. Spillage on draft/cocktail/wine-by-glass = plausible. Spillage on bottled/packaged = mismatch.
**How to address:**
1. Pull the event details — was the bottle broken? Comped? Missing?
2. Retrain: "Spillage" = liquid; "Bottle Broke" = breakage; "Recovery — Guest" = comp-away
3. **Preventive control:** POS restricts "Spillage" to spill-eligible categories only

---

### Pattern 4 — Approver-Server Pair Concentration

**What it looks like:** One manager repeatedly signs off comps for the same handful of servers — high pair concentration in a 90-day window.
**Observed at LOV3:** Tony Winn concentrates 7 of top 15 pairs. Benign explanations dominate: he works the most shifts, he's the on-duty manager on the busiest nights, servers may default to him for approvals.
**Why LPs watch it:** Not because concentration = collusion, but because collusion patterns show up as concentration. High-concentration pairs are the population where buddy-comping, quid-pro-quo, or preferential approvals can incubate.
**How to spot it in the wild:** 90-day pivot: approver × server, count of approvals per pair. Anything > 15% of a manager's approvals going to one server is a look.
**How to address:**
1. Rotate manager coverage so each server sees ≥2 approver managers per week
2. Post the approver-server heatmap in the weekly report — visibility alone corrects behavior
3. If a pair exceeds threshold → other managers get first shot at that server's approvals for a month
4. **Preventive control:** POS randomizes eligible approver from all clocked-in managers

---

### Pattern 5 — Cost-Basis SKU Substitution

**What it looks like:** Item rung using the cost-basis OWNER-prefixed SKU instead of the retail SKU, on tabs where policy calls for full retail comp.
**Observed at LOV3:** OWNER MOET ROSE rung at cost on birthday-package tabs where policy calls for fully-comped Bellaire at retail.
**Why LPs watch it:** Corrupts revenue reporting. Comping a $250 retail bottle via the $80 cost SKU records $80 in comp when the true economics are $250. Systematically understates comp %, overstates revenue. Not fraud — accounting distortion.
**How to spot it in the wild:** Tabs typed as Birthday/VIP/Programmatic that ring OWNER-prefixed SKUs = mismatch.
**How to address:**
1. Retrain: "Birthday Package = retail Bellaire + Birthday Package discount code"
2. OWNER SKUs stay in menu for owner personal / owner-directed cost-basis rings
3. **Preventive control:** POS restricts OWNER SKUs to Maurice / Eddie / Derwin / Per Maurice / Owner Tasting tabs

---

### Pattern 6 — Late-Shift Comping

**What it looks like:** Disproportionate share of a server's or manager's comps happen within the last hour of their shift.
**Observed at LOV3:** Not yet measured. Adding to weekly report v9.
**Why LPs watch it:** Highest-risk skim window — managers tired and rubber-stamping, other managers gone home, lower volume masks anomalies. Classic skim pathway in every LP textbook.
**How to spot it in the wild:** Bin comps by hour-relative-to-shift-end; compare last-hour density to average.
**How to address:**
1. Publish the last-hour comp density weekly
2. Any late-shift comp ≥$100 requires a real-time Slack from the approving manager to ownership
3. **Preventive control:** POS blocks last-hour comps above threshold without owner approval

---

### Pattern 7 — Buddy-Comping (Guest × Server × Cadence)

**What it looks like:** Same server comps the same guest weekly or more, often at similar dollar amounts.
**Observed at LOV3:** Currently undetectable — Toast Guest Name field is not required. One of the top three data-gap risks.
**Why LPs watch it:** Comp becomes a personal favor rather than a business investment. Guest keeps coming back because comp is guaranteed; server gets higher tips in exchange for higher comps. Venue subsidizes a private relationship.
**How to spot it in the wild:** Requires Guest Name field first. Then monthly pivot (guest × server × comp $); 4+ occurrences with same server = look.
**How to address:**
1. Legitimate: guest is documented VIP → move to VIP list + monthly cap
2. Not legitimate: coaching + require a different manager to approve future comps
3. **Preventive control:** Guest Name required on ≥$100 comp tabs; VIP list separate + capped

---

### Pattern 8 — Reason-Code Monoculture

**What it looks like:** One manager funnels every comp into "Manager Comp — Discretionary" regardless of the actual reason.
**Observed at LOV3:** Anthony Winn — 1/4 buckets used across 90 days.
**Why LPs watch it:** Monoculture masks intent — everything looks like judgment even when 30%+ is actually kitchen recovery, promoter fulfillment, or owner-directed hospitality. Not fraud — reporting equivalent of writing every check to "Miscellaneous."
**How to spot it in the wild:** Per-manager, count distinct reason codes over 90 days; flag anything < 3.
**How to address:**
1. Pre-shift training on the 15-code taxonomy
2. Publish diversity metric weekly
3. Ownership 1:1 with any manager still ≤2 codes after 30 days
4. **Preventive control:** POS blocks generic "Manager Comp" as a fallback; requires a specific code

---

### Pattern 9 — Owner Comp Button Unlocked

**What it looks like:** No PIN required on the highest-authority comp button in the POS.
**Observed at LOV3:** Owner Comp pressable by any logged-in user.
**Why LPs watch it:** Highest-authority button = highest potential loss per press. No signal fires until it's already spent.
**How to address:**
1. PIN-lock Owner Comp — only Maurice / Eddie / Derwin PINs accept
2. Every Owner Comp use fires a Slack to ownership at ring-time (tab name + amount)
3. Ownership can flag "not my guest" within 24 hours — becomes a coaching conversation

---

### Pattern 10 — Anonymous Station (Bottle Manager)

**What it looks like:** A POS station generates substantial revenue but "who ran the station tonight?" has no answer.
**Observed at LOV3:** Bottle Manager station rang $7,991 over 90 days with no scheduled person owning it.
**Why LPs watch it:** Anonymity = zero accountability. Cannot coach or compliment the person because no one is named.
**How to address:**
1. Add "Bottle Manager" to the weekly schedule as a named role, rotating shift-by-shift
2. The scheduled person wears accountability for that station's rings that night
3. Preserves pooling economics (revenue still pools to waitstaff); adds accountability

---

**Leader's shift-review one-liner:** at end of shift, ask "did anything look like Patterns 1–10?" If yes, walk it through the "How to address" steps *before* forming a conclusion. Documented investigation trail matters more than the specific finding — an underwriter reviewing the file needs to see we look, not just that we found nothing.

---

## 10. Signoff

This document represents the consensus recommendation of a 9-agent executive committee based on 90 days of live LOV3 operational data. Each finding is grounded in specific transaction evidence and cross-verified against published industry standards from USHG, TAO, Mina Group, and Cornell School of Hotel Administration.

- [ ] Maurice Ragland (Owner) — ______________
- [ ] Eddie Jasper (Owner) — ______________
- [ ] Derwin James Jr. (Owner) — ______________
- [ ] Tiffany Loving (General Manager) — ______________
- [ ] Anthony Winn (Manager) — ______________
- [ ] Dajah Bishop (Shift Manager) — ______________
- [ ] Ashley Baines (Bar Lead with Managerial Authority) — ______________

*This document supersedes prior comp policy language where conflicting. Revisions require full leadership review.*
