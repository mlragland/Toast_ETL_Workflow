# How Toast ETL Works

*A plain-language guide to the system that turns LOV3|HTX's register data into daily numbers, dashboards, and reports. Written for someone who has never seen the code.*

---

## 1. Why This Exists

LOV3|HTX is a nightlife and dining venue in Houston. Every night, its Toast cash-register system records hundreds of orders, payments, kitchen tickets, and staff clock-ins — but that information lives inside the register system, where it's hard to analyze, compare, or share.

Toast ETL is the system that collects all of that data every morning, cleans it up, and stores it in one organized company archive (a Google cloud service called BigQuery — think of it as a giant, searchable filing cabinet). On top of that archive, the app has grown into LOV3's full back-office analytics hub: about twenty web pages leadership can open in a browser, plus a set of reports that write and send themselves on a schedule.

**Who uses it:** the owner (Maurice Ragland) and the leadership team — the people who decide pricing, staffing, promotions, and payouts. Staff don't log into this system; a separate app (LOV3Synch) handles staff-facing scoring.

**Life without it:** someone would be exporting spreadsheets from the register by hand, retyping bank statements, calculating gratuity splits and promoter payouts in Excel templates, and emailing reports manually every week. Several of the pages in this app literally replaced named spreadsheets (the Promoter Payout page replaced a file called "Other Promoter Days.xlsx"). It also produced the lender-grade financial statements used in LOV3's SBA loan application — statements that would otherwise cost accountant hours to assemble.

---

## 2. What Users See

There is no login screen with usernames. Instead, the whole site sits behind a single shared access key — like a keypad code on an office door. Everyone who has the key sees the same pages, connected by one navigation bar. The pages fall into four groups.

### Money pages

| Page | What you see and why it matters |
|---|---|
| **P&L** | A profit-and-loss summary: revenue in, expenses out, what's left. The "did we make money?" page. |
| **Bank Review** | Every Bank of America transaction, waiting to be sorted into an expense category (liquor, payroll, rent...). Sorting here is what makes the P&L and budget pages accurate. |
| **Budget** | Planned spending vs. actual spending by category, with the ability to click into any line and see the underlying transactions. |
| **Cash Recon** | Compares cash the registers say was collected against cash that actually reached the bank — the page that surfaces missing cash. |
| **Prime Cost** | The restaurant industry's #1 health metric: food cost + liquor cost + labor, as a share of revenue. The page grades it (under 55% excellent, over 65% investigate) and rolls forward automatically each month. |
| **Comps** | Tracks free or discounted items ("comps") given away each week, sorted into planned giveaways (birthdays, promoters), manager judgment calls, and owner discretion — measured against industry limits. |
| **Q1 Report** | A polished quarterly financial report for leadership, comparing Q1 2026 to prior periods. |
| **Vendors** | Which suppliers get the most money, month-over-month trends, and unusual spikes — ammunition for negotiating contracts. |

### People pages

| Page | What you see and why it matters |
|---|---|
| **Servers** | Revenue per server, including a clever "attribution engine" that credits bottle sales rung through the shared "Bottle Manager" register station back to the actual server who earned them. |
| **Labor** | Hours worked and labor cost from staff clock-ins, so managers can see if the schedule matches the sales. |
| **Kitchen** | How fast the kitchen fulfills tickets, by station. |
| **ABC Invoice** | Auto-builds the weekly invoice for ABC Staffing (bussers, barbacks, valet) from their actual clock-in hours — no manual timesheet math. |
| **Promoter Payout** | A calculator that pulls a promoter night's sales, subtracts costs, and computes the promoter's cut — with a saved history of past payouts. |

### Guest and menu pages

| Page | What you see and why it matters |
|---|---|
| **Loyalty (Guest Intelligence)** | Guests grouped into segments (regulars, big spenders, at-risk) with an export button — the raw material for marketing outreach. |
| **Menu Mix** | What's selling and what isn't, item by item. |
| **Menu Engineering** | Crosses popularity with profitability to show which items to promote, reprice, or cut. |
| **Events / Event ROI** | A promotional calendar, and a page measuring whether each event night actually made money. |

### At-a-glance pages

| Page | What you see and why it matters |
|---|---|
| **Flash** | Yesterday's night in one screen: revenue, orders, guests, average check, top servers, expenses, and a comparison to the same night last week. |
| **KPI Benchmarks** | LOV3's key numbers side-by-side with hospitality-industry benchmarks. |
| **Analysis** | A comprehensive deep-dive combining many of the above views. |

---

## 3. Where the Information Comes From

| Source | What it provides | How often | If it's missing |
|---|---|---|---|
| **Toast nightly file drop** | Seven files covering orders, checks, payments, items sold, cash drawer entries, kitchen timings, and the full menu — delivered to a secure pickup folder overnight | Collected every morning at 6 AM | The morning run reports "no files found" and alerts the team on Slack; dashboards show yesterday's gap until it's re-run |
| **Toast's direct connection** | Staff clock-in/clock-out records (loaded every morning alongside the files), plus a way to reconstruct historical data when a nightly file never arrived | Daily; on demand for backfills | Labor pages go stale; a failure here doesn't block the main sales load |
| **Bank of America via Plaid** | Every bank transaction, pulled automatically through Plaid (a secure bank-connection service). This replaced an earlier service called Teller, and before that, manual statement uploads (the upload button still works as a backup) | Daily at 7:30 AM | Bank Review, P&L expenses, Budget, and Vendors stop updating; a staleness alarm escalates on Slack |
| **SevenRooms** | Reservation records from the booking system | Refreshed every 15 minutes during service hours | Reservation-linked analysis (like birthday-package reconciliation) falls behind |
| **Google Sheets check register** | The handwritten-check log kept in a shared spreadsheet, synced in for reconciliation | On demand | Check reconciliation can't match payments to checks |
| **Manual categorization** | A human sorting bank transactions into expense categories on the Bank Review page (the app pre-sorts using saved rules; humans handle the rest) | Whenever someone reviews | Uncategorized spending shows up as "unknown" in the P&L and budget |

Deliveries go out through **Slack** (leadership channels), **email** (via a service called Resend, from reports@lov3htx.com), and occasionally **text message** (via Twilio, for payout confirmations).

---

## 4. The Core Logic

Two things sit at the heart of this system: the **morning collection routine** and the **business rules** every report applies.

### The morning collection routine

Every morning at 6 AM, before anyone is awake, the app:

1. **Signs into Toast's pickup folder** and downloads yesterday's seven files.
2. **Checks each file for surprises** — if Toast added or removed a column, the app notes it in the log rather than silently breaking.
3. **Cleans and standardizes** the data (consistent column names, correctly typed dates and amounts).
4. **Replaces, never duplicates.** For each file, it first deletes anything already stored for that date, then inserts the fresh copy. This means re-running the same day twice is always safe — you get one clean copy, never doubles.
5. **Posts a Slack receipt** — success or failure, how many files, how many rows, how long it took. If a step fails, it automatically retries up to three times.

There's also a "backfill" mode that repeats this for a whole range of past dates to fill historical gaps.

### The business rules

Three rules, written once in a central settings file, keep every page and report consistent:

- **The 4 AM business day.** LOV3 is a nightlife venue: a drink sold at 1 AM Saturday belongs to *Friday's* business night. Every report counts a "day" as 4 AM to 3:59 AM.
- **The 20% service charge split.** Every check carries a mandatory 20% service charge (legally a service charge, not a tip). It splits: waitstaff keep 70% (bartenders 75%), the house keeps the rest; Bottle Manager station checks split 50/50. Voluntary tips are 100% staff, always.
- **Bottle Manager attribution.** "Bottle Manager" is a register station, not a person. The server-performance engine reads tab names and timing clues to credit that revenue back to the server who actually earned it.

### A worked example

A party runs a **$500 check** that closes at **1:15 AM on Saturday**.

- The 4 AM rule files it under **Friday's** business night.
- A 20% service charge adds **$100**. If a server rang it: **$70 to staff, $30 to the house**. If it went through the Bottle Manager station: **$50/$50**, and the attribution engine works out which server gets performance credit.
- **Saturday ~6 AM**, the collection routine picks it up. By **7 AM** it's in the archive; the Flash page shows it in Friday's totals, compared against the previous Friday.
- **Tuesday 10 AM**, it's rolled into the weekly leadership report posted to Slack — inside revenue totals, the server leaderboard, and the discount/void watch-list.
- At the next **pay-period close Monday**, its $70 staff share appears in the emailed gratuity breakdown that drives payroll.

One late-night check, five places it matters — all without anyone touching a spreadsheet.

---

## 5. What Runs By Itself

| When | What happens on its own |
|---|---|
| **Every morning, 6 AM** | Collects yesterday's seven Toast files and staff clock-ins into the archive; posts a Slack receipt |
| **Every morning, 7:30 AM** | Pulls new bank transactions from Bank of America via Plaid |
| **Every 15 minutes (service hours)** | Refreshes reservations from SevenRooms |
| **Monday, 9 AM** | Computes rolling Prime Cost and posts to Slack — red alert if it crosses 62% |
| **Monday evening (pay-period close only)** | Emails the bi-weekly gratuity breakdown PDF to leadership; on off-Mondays it checks the calendar and politely skips itself |
| **Tuesday, 9 AM** | Posts the weekly comp report to Slack — alert if giveaways exceed policy limits |
| **Tuesday, 10 AM** | Builds and posts the full weekly performance report to the #lov3-leader-report Slack channel |
| **Tuesday, 12 PM** | Runs the Afrikan Billionaires promoter payout for last Thursday: computes the cut, renders a PDF, saves copies to Dropbox and cloud storage, emails the promoter and owners, and texts confirmations |

The **Daily Flash Report** exists but is pulled up on demand (or sent on request) rather than firing on its own schedule. Bank-transaction categorization and check-register reconciliation remain human tasks — the app assists but a person decides.

---

## 6. One-Page Summary

| Step | What happens |
|---|---|
| **A guest orders** | The Toast register records the order, payment, service charge, and kitchen ticket |
| **Overnight** | Toast drops seven data files into a secure pickup folder |
| **6:00–7:30 AM** | The app collects the files, staff clock-ins, and bank transactions; cleans everything; files it by business night in the company archive; posts a Slack receipt |
| **All day** | Leadership opens browser dashboards — Flash, P&L, Servers, Prime Cost, and more — all reading from the same archive with the same rules |
| **On a schedule** | Weekly and bi-weekly reports write themselves and go out via Slack, email, and text |
| **What it affects** | Payroll gratuity splits, promoter payments, staffing invoices, menu and pricing decisions, comp discipline, budget control — and the financial statements behind LOV3's SBA loan package |

**In one sentence:** every night the registers talk, every morning this app listens, and by the time leadership wakes up, the numbers are organized, cross-checked, and ready to act on.

---

## Appendix — Observed While Writing

Items that appear outdated, replaced, or half-retired in the current code (not described as working above):

- **The README is out of date.** It says the daily run happens at 8 AM and includes instructions about data being "stale since October 10, 2025" — a snapshot of a past problem. The actual schedule is 6 AM, per the newer project notes and scheduler settings.
- **The old Teller bank connection is retired but its trigger still exists.** Bank syncing now runs through Plaid; the Teller pathway remains in the code as a deprecated leftover.
- **The weekly report's email backup is dead.** Its fallback email service (SendGrid) expired in April 2026 and was not renewed. Slack delivery is the working path; newer reports use Resend for email instead.
- **The Q1 Report page is frozen in time.** It is hard-set to Q1 2026 dates and will not roll forward to future quarters on its own.
- **Dashboards are only protected if the access key is configured.** If the key setting is left empty, every page is publicly reachable; and any request that simply presents a "Bearer"-style credential header bypasses the key check without verification inside the app (an outer cloud-level guard is assumed).
- **Housekeeping leftovers in the project folder:** a backup copy of the dashboards code (`dashboards.py.bak`), a temporary Excel lock file, and several generated PDF/Excel artifacts checked in alongside the code.
