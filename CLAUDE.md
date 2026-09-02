# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A Telegram bot for hotel operations management — tracking bar drink sales/stock, room bookings, expenses, debtors, staff activity, and financial allocation. Split Bar/Rooms P&L reporting. Currency is ₦ (Naira). Deployed on Railway (or Heroku) with a PostgreSQL backend.

## Running Locally

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in BOT_TOKEN and DATABASE_URL
python bot.py
```

Requires a live PostgreSQL database (`DATABASE_URL`) — `db.init_db()` auto-creates all tables on startup. There is no SQLite fallback.

## Environment Variables

See `.env.example` for all variables. The critical ones:
- `BOT_TOKEN` — from @BotFather
- `DATABASE_URL` — PostgreSQL connection string (Railway/Heroku set this automatically)
- `ADMIN_IDS` — comma-separated Telegram user IDs; if blank, first `/start` user becomes admin
- `REPORT_CHAT_ID` — if set, auto-schedules the daily report on startup

## Architecture

The bot is split across five modules with a strict layered dependency:

```
bot.py  →  logic.py  →  inventory.py  →  database.py
       →  reports.py →  metrics.py        (pure calc core)
                     →  inventory.py  →  database.py
                     →  database.py
config.py  (imported by all layers)
clock.py   (imported by all layers — sits below database.py)

dashboard/ (separate FastAPI web service, read-only) → metrics.py + database.py
```

**`bot.py`** — Entry point. All Telegram command handlers, access control decorators (`_require_auth`, `_require_admin`), argument parsing, and job scheduling. Delegates all business logic to `logic.py` and `reports.py`. `/help` is role-aware — staff see staff commands only, admins see all.

**`logic.py`** — Business logic and validation layer. All public functions return `(ok: bool, message: str)`. Validates inputs before calling `inventory.py` or `database.py`.

**`inventory.py`** — Drink stock operations only. Returns `StockResult` dataclass. Enforces no-negative-stock rule on bar sales, generates low-stock alerts, tracks cost prices. Exposes `transfer_to_bar()` for store→bar movements.

**`database.py`** — PostgreSQL persistence via SQLAlchemy + pandas. All queries use parameterised statements. `read_all(table)` returns `list[dict]` using `pd.read_sql`. The `upsert_drink()` function does an atomic `INSERT ... ON CONFLICT DO UPDATE`. `get_setting()`/`set_setting()` manage the `settings` table for configurable percentages.

**`reports.py`** — Telegram formatting: reads data from `database.py`/`inventory.py`, runs the numbers through `metrics.py`, builds Telegram MarkdownV2 strings. Reports separate Bar and Rooms P&L. Cost-of-drinks-sold uses the cost **stamped on each sale** (see "COGS is settled at the time of sale"). Salary expenses are always split out separately from other expenses. Profit calcs exclude `restock` (inventory purchase) and owner draws — see "Profit vs Cash vs Stock" below.

**`metrics.py`** — Pure financial calc core (no DB, no formatting). Functions take already-fetched rows + a cost-price map and return dataclasses (`compute_pnl` → `PnL`/`AccountPnL`, `summarize_outstanding`, plus shared row helpers `apply_filter`/`active`/`operating_expenses`/`cost_of_drinks_sold`/…). This is the **single source of truth** for the math, shared by `reports.py` (Telegram) and `dashboard/` (web) so the two can never disagree. The full report, position, allocation, and staff reports consume it (`compute_pnl`, `compute_cash_position`, `compute_allocation`, `staff_breakdown`), as do the performance reports (`compute_working_capital`, `compute_break_even`, `compute_room_metrics`, `menu_engineering`, `summarize_variance`). Golden-master tests in `tests/` assert the Telegram output is byte-for-byte unchanged.

**`clock.py`** — The single wall clock, answered in the **active hotel's** timezone. `now()` / `today()` return naive local values; the timezone is a ContextVar set per update / web request / scheduled job. Deliberately sits *below* `database.py` so `metrics.py` can ask what "this month" means without taking a database dependency. See "Timezones" below.

**`config.py`** — All env var loading via `python-dotenv`. Also holds allocation defaults (`ALLOC_*`) used as fallback when DB settings are not yet set.

## Timezones

**Every "now" belongs to a hotel, not to the server.** Railway runs UTC. When
`db.now_str()` was `datetime.now()`, a Lagos bar serving at 00:30 had the sale
stamped `23:30` the *previous* day — wrong day in `/report`, `/summary`,
`/history`, the daily report and the allocation, every night the bar traded past
midnight. Hotels span timezones, so a process-wide `TZ` cannot fix this: one
process serves them all.

`clock.py` holds the timezone as a **ContextVar**, set per Telegram update, per
web request and per scheduled job — exactly like `database._hotel_schema_var`.
The two are set *together* by `database.set_tenant(schema)`, which is the point:
scoping the engine to a hotel while leaving the clock on the server's timezone is
precisely how entries ended up filed under the wrong day. `reset_tenant(token)`
restores both, for nested scopes like the dashboard resolving one user's access
across several hotels.

- `clock.now()` / `clock.today()` — **naive** local values. Every stored timestamp
  is a naive local string, so naive-local is what the codebase already speaks;
  returning an aware datetime would break every comparison against a parsed row.
- `database._hotel_timezone(schema)` reads `public.hotels.timezone` once per
  schema and caches it. Unknown, blank or invalid falls back to `config.TIMEZONE`
  rather than raising — one hotel's bad row must not take its bot down.
- `/setup` writes the timezone, then calls `db.set_hotel_timezone()` (refreshes
  the cache and the live context) and `_reschedule_for_timezone()` — the nightly
  jobs were built at startup on the old clock and would otherwise keep firing on
  it until a restart.

The ⚙️ Manage → 📈 Insights callbacks (`_cb_insights_menu`,
`_period_kwargs_for`) were reading `datetime.now()` — the server clock — so on
Railway a period button tapped late at night in Lagos resolved "this month" and
"this week" against UTC. Both now use `clock.now()` / `clock.today()`, and the
two room views share `_period_kwargs_for()` so they cannot drift apart again.

**Never call `datetime.now()` or `date.today()` in application code.** They read
the server clock. The only survivors are `scripts/` (manual CLI tools, where
server time is the right answer for a filename).

**Scheduling was already correct** — `run_daily(..., tzinfo=pytz.timezone(tz))` is
timezone-aware. What was wrong is that every hotel was scheduled on the *env*
`TIMEZONE`: `get_all_hotel_configs()` didn't select `timezone` and `main()` passed
the process default, so the timezone `/setup` collected was written to two places
and read from neither. Both now carry the hotel's own value.

Tests freeze `clock.now`/`clock.today` (see `tests/conftest.py`); patching a
module's `datetime` no longer shifts time, since nothing calls `datetime.now()`
any more. `tests/test_clock.py` pins the midnight-rollover case directly.

## Access Control

Two roles: `admin` and `staff`. Role lookup hits the `users` table on every request (no caching). `ADMIN_IDS` in env provides a hardcoded admin override that bypasses the DB check.

**Staff can:**
- `/sell_drink` — record drink sales (tracked with `recorded_by`)
- `/room` — record room bookings
- `/turnaway` — record guests turned away (no money moves; feeds `/roomstats dow`). Reached by tapping **🛏 Book Room → 🚪 No room free**, which costs no slot on the six-button staff keyboard
- `/report`, `/stock`, `/summary`, `/history`, `/debtors` — view only

**Admin only:**
- `/expense`, `/add_debtor`, `/pay_debtor`, `/restock`, `/transfer`, `/delete`
- `/sales_report`, `/expense_report`, `/staff_report`, `/allocation`, `/setallocation`
- `/setthreshold`, `/addstaff`, `/removestaff`, `/dailyreport`
- `/cashcycle`, `/menu`, `/roomstats`, `/setrooms` — performance analysis
- `/count`, `/variance` — stocktakes and shrinkage
- `/restock_credit`, `/payables`, `/pay_supplier` — supplier credit

The analysis reports are also reachable from **⚙️ Manage → 📈 Insights** (a submenu, so the Manage keyboard stays readable); 🛏 Room Stats there opens a period picker (this week / last week / this month / all time) rather than jumping straight to the month. Supplier credit has its own **⚙️ Manage → 🧾 Suppliers** submenu — see below.

Staff cannot delete anything — audit trail is preserved. Mistakes are corrected by admin via `/delete` then re-entry.

## Guided flows — every screen has a way out

Every tap-through flow is a straight line of prompts, and the only exit from a
wrong turn used to be typing `/cancel` and starting the entry again. The front
desk makes that wrong turn constantly — the wrong drink tapped, the wrong
account, a figure typed with a digit missing — so the cost of a slip was the
whole entry.

`bot._step()` renders one prompt **and remembers it**: its text, its keyboard,
and a copy of what the flow knew at the moment it was shown. ⬅️ Back pops that
stack and puts the previous prompt back, restoring the snapshot with it.

**Restoring the snapshot is the load-bearing half**, not re-showing the prompt.
A booking that went down the hourly path and was asked the time of day must not
still be carrying that answer when the type is changed to a nightly room on the
way back — the answer would be stamped on a stay it cannot describe. So Back
drops everything answered *after* the step it returns to, and keeps everything
answered before it.

**A re-prompt after a rejected answer is the same step, not a new one**, so it
overwrites the top of the stack instead of pushing. Otherwise three fumbled
attempts at an amount would need three taps of Back to escape. On the *first*
prompt a re-prompt grows no Back button at all: there is nothing behind it.

`root=True` starts a fresh stack. A flow's opening prompt uses it, and so does
any step taken after something has been written to the database — there is
nothing safe to go back to past a write, so the escape row offers Cancel only.

**`bot._conv()` wires ⬅️ Back and ✖️ Cancel into every state of every flow**,
rather than each flow doing it for itself: a screen that quietly lacked an
escape would be exactly the screen someone gets stuck on. All 33
ConversationHandlers are built through it, and `tests/test_nav.py` asserts that
against the real registration. A nav tap that outlives its flow is caught by
`_nav_stale`, registered *behind* the conversations — PTB runs at most one
handler per group, so a global nav handler placed ahead of them would swallow
every live Back tap.

`_say()` is the counterpart for the line that *ends* a flow rather than asking
the next question: it sends the same way but records no step.

## Commands Reference

### Staff commands
| Command | Description |
|---|---|
| `/sell_drink <drink> <qty> <price> [YYYY-MM-DD]` | Record drink sale |
| `/room <type> <qty> <price> <nights> [YYYY-MM-DD]` | Record room booking. Add `<n>h` (e.g. `3h`) for a one-off short let on an otherwise nightly room |
| `/undo` | Reverse your own most recent sale or booking, within 2 minutes |
| `/turnaway <how many> [type] [reason] [YYYY-MM-DD]` | Log guests you had no room for — demand that never became a booking |
| `/report [today\|YYYY-MM-DD\|YYYY-MM\|all]` | Full financial report |
| `/summary [YYYY-MM-DD]` | Daily overview with set-aside nudge |
| `/stock` | Inventory table (store + bar columns) |
| `/history [YYYY-MM-DD]` | All entries for a date with IDs |
| `/debtors [bar\|rooms]` | Outstanding debtors |

### Admin-only commands
| Command | Description |
|---|---|
| `/sales_report [today\|YYYY-MM-DD\|YYYY-MM\|all]` | Drink-level sales breakdown with cost & profit |
| `/expense_report [today\|YYYY-MM-DD\|YYYY-MM\|all]` | Expense breakdown by category |
| `/expense <rooms\|bar\|overhead> <category> <amount> [note] [YYYY-MM-DD]` | Record expense. Use `salary` as category for staff wages. Draw-like categories (`drawings`, `owner`, `withdrawal`, …) are rejected and routed to `/draw` |
| `/draw <amount> [note] [YYYY-MM-DD]` | Record an owner withdrawal (equity draw). **Not** an expense — reduces cash, never profit |
| `/draws [today\|YYYY-MM-DD\|YYYY-MM\|all]` | List owner draws for a period (newest first, with IDs) and the total drawn |
| `/add_debtor <room\|bar> <name> <amount> [note] [YYYY-MM-DD]` | Log debtor |
| `/pay_debtor <room\|bar> <name> [amount]` | Full or partial payment against a person's **oldest** outstanding debt |
| `/pay_debt <id> [amount]` | Full or partial payment against **one specific debt**. This is what **⚙️ Manage → ✅ Pay Debtor** uses — the picker lists one button per outstanding debt (name, remaining, date, note) and the payment lands on the row that was tapped, never on an earlier one |
| `/debtor_history <bar\|rooms> <name>` | Full payment timeline for a debtor |
| `/restock <drink> <qty> <cost_price>` | Add inventory to store. Logged as a `restock` expense row but treated as a cash→stock movement, **not** a P&L cost |
| `/transfer <drink> <qty> [YYYY-MM-DD]` | Move store → bar. Logged to the `transfers` audit table; the optional date backdates that log row |
| `/delete <sale\|room\|expense\|draw> <id>` | Remove an entry |
| `/staff_report [today\|YYYY-MM-DD\|YYYY-MM]` | Sales per staff member |
| `/cashcycle [days]` | Cash conversion cycle (DIO + DSO − DPO), receivables aging, idle stock, bar break-even and the room-revenue target. Default window 30 days |
| `/menu [days]` | Menu engineering — every priced drink ranked into star / plow-horse / puzzle / dog with the action for each |
| `/roomstats [week\|lastweek\|period]` | Occupancy, ADR, RevPAR **and GOPPAR** read against the previous like-for-like period, with a raise/hold verdict, plus RevPAR split by room type |
| `/setrooms <n>` | Record the lettable room count — the denominator for occupancy and RevPAR |
| `/setrooms <type> <n>` | Rooms of one type — the denominator for **RevPAR per room type** |
| `/setduration <type> <hours>` | Declare a room type **sold by the hour** — its units become lets, not nights. `24` puts it back to nightly. Applies retroactively |
| `/review [period]` | Month-end check — entries at or above the capital threshold, plus anything flagged unsure. Also **⚙️ Manage → 🔎 Review** |
| `/reclassify <id> <account\|class> <value>` | Correct one expense's classification in place. Amount, date and author untouched |
| `/roomstats dow [period]` | Night-by-night split (occupancy/ADR/RevPAR per weekday) + turnaways, with a flat-rise vs weekend-premium verdict |
| `/count <drink> <units> [note] [YYYY-MM-DD]` | Physical stocktake: logs the variance vs what the books expected, then corrects bar stock to the counted figure |
| `/stocktake` | Month-end count sheet, then bar and store counts per item. Also **⚙️ Manage → 🧾 Month-End Verification** |
| `/roomaudit` | Were all room-nights logged, at the rate charged? The bot draws the days |
| `/variance [period]` | Shrinkage report built from stocktakes |
| `/restock_credit <drink> <qty> <cost> <supplier> [YYYY-MM-DD]` | Receive stock on supplier credit. Stock in now, **no cash out** — trailing date is the due date |
| `/payables` | Outstanding supplier invoices, soonest due first |
| `/pay_supplier <id> [amount]` | Settle an invoice fully or partially. **This** is where cash moves (writes a `supplier` expense row) |
| `/position [set <amount> [YYYY-MM-DD]]` | "What you have" snapshot — cash at hand (headline), stock value & receivables, profit as a one-line footnote. `set <amount> <YYYY-MM-DD>` anchors cash to your real bank balance **on that day**; only flows on/after it are counted, so earlier months are ignored and you can **re-anchor safely each period**. `set <amount>` without a date = all-time starting balance (before the first entry) — set once, never to a current balance |
| `/allocation [today\|YYYY-MM-DD\|YYYY-MM\|all]` | Revenue allocation + profit distribution |
| `/setallocation <key> <percent>` | Adjust allocation percentages (see below) |
| `/setthreshold <drink> <amount>` | Low-stock alert threshold |
| `/addstaff <user_id> <username>` | Grant staff access |
| `/removestaff <user_id>` | Revoke access |
| `/dailyreport on\|off` | Toggle scheduled daily report |

## Reporting

All date-filtered reports accept the same arguments:
- _(blank)_ — current month
- `today` — today only
- `YYYY-MM-DD` — specific date
- `YYYY-MM` — specific month
- `all` — all-time

### Report functions in `reports.py`
| Function | Used by |
|---|---|
| `generate_full_report()` | `/report` |
| `generate_sales_report()` | `/sales_report` |
| `generate_expense_report()` | `/expense_report` |
| `generate_staff_report()` | `/staff_report` |
| `generate_daily_summary()` | `/summary` + scheduled daily report |
| `generate_allocation_report()` | `/allocation` |
| `generate_position_report()` | `/position` |
| `generate_draws_report()` | `/draws` |
| `generate_stock_report()` | `/stock` |
| `generate_debtors_report()` | `/debtors` |
| `generate_cashcycle_report()` | `/cashcycle` |
| `generate_menu_report()` | `/menu` |
| `generate_room_stats_report()` | `/roomstats` |
| `generate_dow_split_report()` | `/roomstats dow` |
| `generate_review_report()` | `/review` |
| `generate_count_sheet()` | `/stocktake` (step 1) |
| `generate_room_audit_report()` | `/roomaudit` |
| `generate_variance_report()` | `/variance` |
| `generate_payables_report()` | `/payables` |

### Profit vs Cash vs Stock (accounting model)

Three figures are tracked separately and must never be conflated:

1. **Profit (performance)** — `revenue − cost-of-stock-sold (COGS) − operating expenses`. **Owner draws and inventory purchases (`restock`) are excluded.** Buying stock converts cash into a stock asset; its cost only hits the P&L as COGS when the drink is *sold*. Counting the restock purchase as an expense too would double-count it. `reports._operating_expenses()` strips `restock` (the `NON_PNL_CATEGORIES` set) from every profit calc (`generate_full_report`, `generate_daily_summary`, `generate_expense_report`, `generate_allocation_report`).
**Old debts settled later.** A tab raised in July and paid in August is cash
arriving with no revenue behind it in the window — its revenue was July's. It is
also not part of an anchored opening balance, because that balance was the bank
on the anchor day and the money had not come in yet. So `collected` cannot see
it from either side, and without `old_debt_cash` the estimate silently lost
every naira collected against a previous month's tabs. `compute_cash_position()`
takes `payment_rows` (the `debtor_payments` table) and adds payments made inside
the window against debts **created before the anchor** — only those, since a
debt raised and settled inside the window is already covered by `collected` and
counting it again would double it. With no anchor set, nothing is "old": the
all-time window already contains the original sale.

2. **Cash in bank (estimate)** — running balance: `opening + collected sales − operating expenses − stock purchases − owner draws`. Draws and restock **do** reduce cash. The `opening` anchor works two ways: with an **anchor date** (`/position set <amount> <YYYY-MM-DD>`, stored in `cash_opening_date`) only flows on/after that day are counted — `opening` is your real balance on that day, earlier months are ignored, and you can re-anchor each period without double-counting. Without a date, `opening` is the all-time starting balance before the first entry and every flow is added on top (set once; never to a current balance). Assumes sales are cash unless an outstanding debtor exists.
3. **Stock value on hand (asset)** — `Σ (store + bar units) × cost_price`. Shown as `TOTAL VALUE` in `/stock` and line ③ of `/position`.

`/position` shows all three side by side plus outstanding receivables and (when any exist) unpaid supplier invoices. Owner draws live in the dedicated `owner_draws` table, never in `expenses`, so they can never touch profit.

### COGS is settled at the time of sale

`sales.cost_price` stamps what a drink cost **at the moment it was sold**.
Before this, `cost_of_drinks_sold()` read the *current* price from inventory,
so restocking dearer silently rewrote every month already closed:

| May's report, run in | COGS | Profit | Gross margin |
|---|---|---|---|
| May (cost ₦400) | ₦40,000 | ₦60,000 | 60.0% |
| June (cost ₦550) | ₦55,000 | ₦45,000 | 45.0% |
| July (cost ₦700) | ₦70,000 | ₦30,000 | 30.0% |

Not one May row changed. In a high-inflation currency that is not an edge case,
and it undermined everything read across periods — the `/roomstats` verdicts,
the GOPPAR trend, shrinkage as a share of COGS, and any decision recorded
against a figure that later moved.

The cost is read in `inventory.record_sale()`, where the drink row is already
in hand, so a restock landing mid-sale cannot change what the sale is recorded
as having cost. Rows written before the column existed, and drinks with no cost
recorded, still fall back to `cost_map` — that is the old behaviour, kept only
where nothing better can be reconstructed. A stamp of 0 counts as missing, not
as free stock.

### A debt is not a sale

`/add_debtor` writes only to `debtors`. It creates no sale, deliberately — a
₦10,000 bar tab does not say which drinks were taken, so there is nothing to
compute COGS from. The intended sequence is **both**: record the sale, then
record the debt for the part not paid. `compute_cash_position` is built on that,
treating revenue as collected unless a debt says otherwise.

Entering only the debt sent the arithmetic somewhere impossible — it subtracted
a receivable from revenue that never included it:

| how the same ₦10,000 tab was keyed | revenue | profit | cash |
|---|---|---|---|
| sale + debtor (intended) | 10,000 | 5,000 | 0 |
| **debtor only** | 0 | 0 | **−10,000** |
| sale only (tab forgotten) | 10,000 | 5,000 | 10,000 |

Cash fell because a guest drank on credit. Two changes close it:

- `collected` is clamped at zero — you cannot collect a negative amount — and
  the shortfall is reported as `unmatched_receivables` rather than absorbed.
- `metrics.unmatched_debts()` finds the rows and `/position` names them, so the
  fix (record the missing sale) is actionable rather than just flagged.

Detection is **conservative on purpose**: it flags only a debt raised on a day
when that account took no money at all, since there is then nothing for it to be
part of. A room debt is checked against room revenue, a bar debt against bar
sales. Anything looser would flag honest entries, and a control that cries wolf
gets ignored.

### Stock purchases against revenue (`/report`)

Two numbers that mean little apart and a great deal together, and they used to
sit in different sections of the same report.

- **Purchases as a share of bar revenue** — a spending discipline, checked
  against `settings.purchase_cap` (default `metrics.DEFAULT_PURCHASE_CAP`, 40%),
  set from **⚙️ Manage → ⚙️ Settings → 📉 Purchase Cap**.
- **Stock movement** — purchases minus COGS. Negative means the shelf ran down.

The finding is the pair, not either half:

| | stock grew | **stock fell** |
|---|---|---|
| **over cap** | 💡 cash moved into inventory; it comes back as those drinks sell | ⚠️ **the cash went out and the shelf did not gain — count the bar** |
| under cap | ✅ buying in step | 💡 selling from stock you had; expect to buy next month |

August bought ₦345,377 against ₦647,600 of bar revenue — **53.3%, over a 40%
cap** — and still drew stock down ~₦46,000. Either the month outran the cap, or
something sits between the purchase and the sale, and only a physical count can
say which. That was spotted by eye; this makes it a standing check.

### Margins

`metrics.AccountPnL` and `metrics.PnL` expose margins as computed properties, so every surface divides identically:

- **Gross margin** = `(revenue − COGS) / revenue` — pricing and purchasing health, before any expense.
- **Net margin** = `profit / revenue` — whole-operation health.

The rooms account has `cogs = 0.0` by construction, so its gross margin is always 100% (read it as *contribution* margin). The blended `PnL.gross_margin_pct` is therefore inflated by room mix — **the per-account figures are the ones to act on**. `metrics.pct_of()` is the single divide-by-zero-safe helper behind all of them.

### Room yield (`/roomstats`) — RevPAR and GOPPAR

One period's RevPAR is a number; two make it a signal. `/roomstats` reports the current window **against the previous like-for-like one** and turns the pair into an instruction.

**Windows.** `reports._room_windows()` returns `(current, prior)` as `(start, end, label)` triples, with `end` capped at today. Both windows are cut to the **same elapsed length** — comparing two finished days of a new month against a whole finished month would make every month start look like a collapse. Day → previous day; week (Mon–Sun) → same weekdays 7 days back; month → the equal-length slice of the previous month; `all` → no predecessor, so no comparison. Weeks are `/roomstats`-only (`week` / `lastweek`): rate-versus-volume moves show up week to week, while a discount is still worth reversing.

**The verdict.** `metrics.compare_room_metrics()` reads the *direction* of occupancy against the direction of RevPAR — the rate/volume trade RevPAR exists to expose, read over time:

| | RevPAR ↑ | RevPAR → | RevPAR ↓ |
|---|---|---|---|
| **occupancy ↑** | growing properly, hold rates | underpriced — the extra rooms earn nothing | the discount cost more than it brought in |
| **occupancy →** | clean rate gain | steady | rate has slipped |
| **occupancy ↓** | rate-led: fewer rooms, each worth more | rate is absorbing the drop | overpriced, or demand is soft |

Moves inside `metrics.TREND_BAND` (5%) read as flat — without a dead band a hotel this size gets a fresh "raise your prices" verdict from one extra booking. Direction is judged on *relative* change (at 3% occupancy, one point is enormous); the report shows occupancy in percentage **points** and everything else in percent. `RoomTrend.comparable` is `False` when the prior window sold nothing, and the report says there is no baseline rather than printing `▲ ∞%`.

`rate_note` is the separate pass-through check: **did a rate change reach RevPAR?** ADR up + RevPAR up = sticking; ADR up + RevPAR flat = lost bookings are cancelling it out; ADR up + RevPAR down = it backfired. All six ADR×RevPAR combinations are spelled out, because "not up" covers held-flat and fell — two very different outcomes, and collapsing them once produced a "RevPAR followed it down" note beside a flat verdict.

**A short let is not a night, and it happens at a time.** Splitting the hourly
trade by weekday answers half the question; the half that sets the price is
*when in the day*, because a room turned away at 8pm and idle at 10am has two
problems and only one of them is a rate. `metrics.daypart_split()` groups hourly
lets into Morning (06–12), Afternoon (12–18), Evening (18–23) and Night (23–06),
and `/roomstats dow` prints it under **BY TIME OF DAY** with its own verdict —
kept beside its own data rather than stacked on the top-level verdicts.

**The band is asked for and stored, never inferred from the timestamp.** Hotel
85 records bookings in a paper book and keys them in the following morning, so
a row's timestamp is when the *typing* happened. Deriving the hour from it
looked reasonable and was flatly wrong — 81 evening lets keyed at 08:30 came
back as `{'Morning': 81}` with the verdict *"the hourly trade is a morning
business"*: a finding about the owner's admin routine, not about the hotel.

Only the book knows when the let was, so the booking flow asks. `rooms.daypart`
stores it, `metrics.daypart_of()` reads that field **and nothing else**, and a
booking with no band recorded is reported untimed rather than guessed at. The
question is asked only for hourly room types (`_is_hourly_type()`), so an
overnight booking costs no extra tap, and **🤷 Not noted** is always offered —
a forced guess is worse than an honest gap. The block needs five timed lets
before it reads the shape of the day at all.

This is the same failure as a pre-filled count sheet: a figure the system can
produce on its own is not an observation, and dressing one up as the other is
how a control quietly becomes decoration.

### Overnight capacity is the rooms actually on sale overnight

`metrics.nightly_rooms()` subtracts rooms whose type is hourly from
`total_rooms`, and that — not the full count — is the denominator for
occupancy, ADR, RevPAR and GOPPAR. A room that only ever does hourly lets was
never available for a night, and charging occupancy against it understates the
figure exactly as counting lets as nights overstated it:

| | 273 nights sold |
|---|---|
| lets counted as nights, all 13 rooms | 90.3% — inflated |
| lets excluded, still all 13 rooms | 70.0% — deflated |
| **lets excluded, 11 overnight rooms** | **82.7% — honest** |

**The identity `RevPAR == ADR × occupancy` is what catches this**, and it is
worth keeping as the check on any future change here. At ₦7,341 ADR and 70.0%
the report printed RevPAR of ₦5,544, when the identity demands ₦5,139 — the
two figures were built on different denominators. Fixing the denominator alone
was not enough: RevPAR's *numerator* had to become overnight revenue too, or
the identity still failed. Both halves of every overnight figure exclude the
hourly trade.

`compute_goppar()` must be passed `metrics.overnight_revenue()`, not
`pnl.rooms.revenue` — the latter includes hourly takings and would print a
second, higher RevPAR two lines below the first, on the same screen.

**`utilization_pct` keeps the whole building** in its denominator
(`total_rooms × days × 24`). It is the one figure that spans both trades: every
room has 24 hours to sell, however it sells them.

The fallback is deliberate — `nightly_rooms()` returns the full count whenever
the split cannot be worked out (no per-type counts, no hourly types, or hourly
counts that would leave nothing). A wrong denominator is worse than an
unrefined one.

**Every caller must pass BOTH maps.** `hours_by_type` says which types are
hourly; `rooms_by_type` says how many rooms each holds. With only one, the
fallback fires and the full count is used. `/report` passed hours but not
counts, so it ran on 390 room-nights while `/roomstats` used 330 for the same
month — RevPAR ₦5,138 against ₦6,073. The fallback made it silent, and nothing
on the `/report` screen showed which denominator it had used.

Two guards now: `/report` prints the basis it used (`On 11 overnight rooms × 30
days = 330 room-nights`), and a test asserts `/report` and `/roomstats` report
the same occupancy for the same data. A wrong denominator propagates into
RevPAR, GOPPAR and every pricing comparison built on them, so it is worth
pinning across surfaces rather than per function.

**RevPAR by room type** needs a per-type denominator, stored as `roomtype_rooms:<type>` in `settings` (`db.get_all_room_type_counts()`), set with `/setrooms <type> <n>` or from **⚙️ Manage → ⚙️ Settings → 🏨 Room Counts** (the `src` ConversationHandler). That screen lists the hotel total and every type the hotel has priced, counted or actually booked (`_known_room_types()` unions the three sources), shows which are still unset, and warns when the per-type counts don't sum to the total. Types are picked **by index** — they are free text and can contain spaces (`short time`), so they can't ride in `callback_data`. Because this is a setting you enter three or four times in a row, the confirmation carries a *Set another* button straight back into the flow. Its neighbour `sset:roomtype` sets room **prices**, and is labelled "🛏 Room Prices" so the two aren't confused. Each type divides by *its own* room count — borrowing `total_rooms` would credit every room in the building to one category — so a type with no count recorded shows `RevPAR n/a` and a prompt, never a wrong number. By-type ADR needs no denominator and is always populated. If `total_rooms` is unset but per-type counts exist, their sum becomes the hotel-wide denominator; if both are set and disagree, `/setrooms` warns (legitimate when rooms are out of service).

**GOPPAR — the bottom-line twin.** RevPAR is a revenue metric: it cannot see fuel, wages, restocking or maintenance, so a hotel can post a rising RevPAR straight through a month it lost money on. `metrics.compute_goppar()` divides *profit* by the same denominator and prints directly beneath it, so the gap between the two lines **is** the cost base.

GOP is taken straight from `compute_pnl` — whole-hotel GOP **is** `PnL.net_profit`, rooms GOP **is** `PnL.rooms.profit` — so GOPPAR can never drift from the P&L (pinned by tests, the same invariant as `compute_rooms_target`). Stock purchases and owner draws are already excluded upstream by `operating_expenses()`, so buying stock never depresses GOPPAR.

- **GOPPAR** — whole-hotel profit per available room-night. The headline.
- **Rooms only** — `PnL.rooms.profit` per available room-night, isolating the rooms department.
- **Conversion** — `goppar / revpar`, the share of RevPAR that survives as profit. It can exceed 100% when the bar carries the rooms, and the report says so rather than letting it look like an error. `rooms_conversion_pct` equals `PnL.rooms.net_margin_pct` exactly (same denominator both sides) and stays ≤ 100%.

Caveat worth keeping in mind: Hotel 85's expense categories don't separate fixed charges (rent, insurance) from operating ones, so this is nearer *net operating profit* per available room than strict USALI GOP.

`compare_goppar()` answers the question RevPAR alone cannot — **did a revenue gain reach the bottom line?** RevPAR ↑ + GOPPAR ↑ = the gain is real; RevPAR ↑ + GOPPAR → = rising costs absorbed the whole thing (the fuel-pass-through case: a rate rise that only covers the diesel it was raised for); RevPAR ↑ + GOPPAR ↓ = costs are rising faster than rates. When the prior period's GOP was zero or negative, the direction is reported without a percentage — coming back from a loss is a direction, not "−200% growth". The whole block is suppressed when no room count is set, since there is no denominator.

`reports._yield_gap_note()` flags the trap the split exists for: the type charging the **most** per night being out-earned per room owned by a **cheaper** one. A cheap type yielding least is not the trap — that is just a cheap room — so the note fires only when the winner's ADR is lower and the gap clears `TREND_BAND`.

### Night-by-night split (⚙️ Manage → 📈 Insights → 🗓 Night by Night, or `/roomstats dow`)

A period's RevPAR is one number, and a hotel that is full every Friday and half
empty every Tuesday reports a middling occupancy that describes neither night.
**⚙️ Manage → 📈 Insights → 🗓 Night by Night** (period picker, same four
choices as 🛏 Room Stats) splits the same window across the seven weekdays so a
rate decision can be made on the shape of demand rather than its average. The
report carries a **🚪 Log a turnaway** button at its foot, so the screen that
says "nothing recorded" is one tap from fixing that.

Two corrections make the split honest, and both are load-bearing:

1. **A booking is not one night.** A `rooms` row carries a start `timestamp`, a
   `nights` count and a `quantity`; crediting the whole stay to the weekday it
   *began* on would put a Friday check-in's Sunday night on Friday.
   `metrics.expand_room_nights()` explodes every row onto the calendar nights it
   actually occupies. The report widens its row scan back `_MAX_STAY_LOOKBACK`
   days so a stay that began before the window still counts for the nights
   inside it, and `compute_dow_split` clips to `[start, end]` afterwards.
2. **Each weekday divides by its own denominator.** A window rarely holds equal
   numbers of each weekday, so a Friday's occupancy is `nights sold / (rooms ×
   number of Fridays)` — never an even share of the period.

The revenue apportioned to a night spans **all** rooms in its booking, not one:
multiplying it by `quantity` again squares the room count and reports an ADR
several times the rate actually charged. There is a regression test pinning it.

**The verdict** (`metrics._pricing_shape()`) reads the occupancy gap in
percentage *points* against whether the weekend already carries a rate premium:

| | weekend priced the same | weekend already at a premium |
|---|---|---|
| **weekend ≥10pt fuller** | split the rate — raise the weekend, hold the weekday | premium is working; the weekday nights are the problem, and need volume not price |
| **occupancy level** | flat rise is the right shape | flat rise; no demand gap left for a wider split |
| **weekend ≥10pt emptier** | a weekend premium is not justified — this is a business/stopover trade, price the weekday as the peak | — |

Nights tied on occupancy are named together (`Friday & Saturday`) and suppressed
past three: picking between identical nights states a difference the numbers do
not contain.

Both bands need at least `metrics.MIN_BAND_NIGHTS` (2) nights before any verdict
is issued. `/roomstats dow today` on a Friday has *no* working-week nights to be
busier than, and without the guard returned "raise your weekend rates" off a
single booking; it now says the window is too short and names what it holds.

**Turnaways — the half occupancy cannot see.** A night at 100% looks identical
whether one guest was refused or twenty, and only the second says the rate is
too low. Nothing in the books can infer this, so it is recorded directly. Two tap paths reach the
same `ta_conv` flow — how many → what type → why, all buttons:

| Who | Path | Why there |
|---|---|---|
| Staff | **🛏 Book Room → 🚪 No room free — log a turnaway** | the exact moment a turnaway is discovered — you go to book someone in and there is nothing free. Costs no slot on the staff keyboard, which is full at six by design |
| Admin | **⚙️ Manage → 🚪 Turnaway** | admins have no 🛏 Book Room button, and at this hotel the front desk is often an admin account |

Both buttons carry the same `ta:start` callback straight into the conversation's
entry point, so there is one flow rather than two that can drift.

Its entry button deliberately carries a **`ta:start`** callback rather than a
`bt:` one. The booking conversation's room-type state only claims `^bt:`, so the
tap falls straight through to `ta_conv`'s entry point instead of needing a
second tap to escape the booking flow.

`/turnaway <how many> [type] [reason] [YYYY-MM-DD]` still types it in one line,
and a bare `/turnaway` offers the button. The `turnaways` table touches no
money: no revenue, no expense, no stock movement, and it is outside every P&L
path by construction.

The report prices refusals at the ADR **actually achieved**, which is the honest
floor — pricing them at a raised rate would assume the very rise the screen
exists to test. When nothing has ever been recorded it says so rather than
printing a zero: no turnaways and no turnaway *tracking* are opposite findings.
Refusals concentrated on nights already ≥70% full are flagged as the clearest
case for a weekend premium; refusals on quiet nights point at the wrong room
*type* rather than the price.

Backdating works as everywhere else, so a tally kept on paper can be entered
after the fact.

### Not every let is a night (hourly rooms)

Hotel 85 also sells rooms by the hour — a guest takes a room for one, two or
three hours rather than overnight. These are recorded against the **same**
`rooms` table with `nights=1`, so before this was modelled the `nights` column
was silently carrying two different units. Read as nights, hourly lets wreck two
figures and **only** two:

- **Occupancy** — a room let three times in a day reported *300% of itself*;
  hotel-wide occupancy went over 100%.
- **ADR** — a ₦3,000 two-hour let averaged with a ₦15,000 overnight stay to
  produce ₦9,000, a rate the hotel never charged for anything.

**RevPAR and GOPPAR were never wrong.** Their denominator is room-*days*, and
revenue per available room-day is a fair question whether the room earned it
from one guest or six. That is exactly why they remain the cross-trade
comparator: it is the one line the two trades can be added up on.

**The model.** Every room type has a stay length in hours
(`settings.roomtype_hours:<type>`), set from **⚙️ Manage → ⚙️ Settings → 🕐 Stay
Length** (the `ssl_conv` flow: pick the type, then 1h/2h/3h/6h/🌙 full night, or
type your own) or with `/setduration <type> <hours>`. Types are picked **by
index** for the same reason as room counts — "short time" has a space in it and
cannot ride in `callback_data`. The booking keyboard labels an hourly type
`₦3,000/2h let` rather than `/night`, so the front desk sees which trade it is
booking into. Anything below
`metrics.NIGHT_HOURS` (24) is an *hourly* type:

| | nightly type | hourly type |
|---|---|---|
| `rooms.nights` means | nights | **lets** |
| counted in | `room_nights_sold` | `short_lets` |
| its rate | `adr` (per night) | `arl` (per let) |
| its fill | `occupancy_pct` (nights / room-days) | `utilization_pct` (hours / room-hours) |
| spreads across days? | yes — a 3-night stay covers 3 days | **no** — 3 lets are 3 lets on one day |

`metrics.stay_hours(row, hours_map)` resolves it: a duration stored on the row
wins, otherwise the type's setting, otherwise 24. That ordering is what makes
the fix **retroactive** — declaring "short time is 2 hours" once reinterprets
every historical booking correctly, so nothing needs backfilling. `rooms.duration_hours`
exists for the negotiated one-off (`/room standard 1 5000 1 3h`) and is stored
so reconfiguring a type later can never rewrite what a past booking actually was.

**A hotel that configures nothing is untouched.** With an empty hours map every
type is nightly and every figure is what it always was — pinned by
`test_configuring_nothing_leaves_every_figure_where_it_was` and by the
golden-master snapshots, which did not move when this landed.

**Two trades, two verdicts.** `/roomstats dow` computes the rate verdict
separately for each and prints both, because they routinely disagree: overnight
demand peaks at the weekend far more reliably than hourly demand does, and a
single blended verdict would hide it. The hourly trade's volume is measured in
**lets per day** (normalised for the differing band lengths), not occupancy — an
hourly room's ceiling is how many times it can be turned over, not whether it is
full at midnight. Each verdict is suppressed when its trade has no activity, so
a nightly-only hotel sees exactly one.

### Cash conversion cycle (`/cashcycle`)

`CCC = DIO + DSO − DPO`, in days: how long cash is trapped between paying for stock and banking the proceeds. This is what explains a profitable month with an empty account. All of it lives in `metrics.compute_working_capital()`, which takes **all-time** rows and applies the trailing window internally so the three legs can never be windowed inconsistently.

- **DIO** — `avg stock value / daily COGS`. Uses the mean of `inventory_snapshots` when ≥2 days exist (`dio_basis == "snapshots"`), otherwise falls back to today's shelf (`"current"`). `inventory` is overwritten in place, so without snapshots there is no stock history at all.
- **DSO** — ratio estimate (`receivables / daily credit sales`) feeds the cycle; `collection_days` is the separately reported *measured* figure, **amount-weighted across `debtor_payments` events** so part-payments count. Falls back from window to all-time basis, flagged via `collection_basis`.
- **DPO** — from the `payables` table. When nothing has been bought on credit, `dpo_tracked` is `False` and the report says so rather than silently treating it as zero (which would overstate the cycle).

`compute_rooms_target()` answers the owner's actual monthly question — *how much must rooms bring in?* — by mirroring how a small hotel really runs: the bar carries only the costs it causes (its staff, its freezer), and room revenue carries the shared overheads (rent, diesel, security, room staff) that exist whether or not the bar opens. Because a room sale has no stock behind it, this is a **subtraction, not a division by a margin**: `room_sales_needed = shared_costs − bar_contribution`, clamped at 0. A loss-making bar has a negative contribution, which correctly *raises* the room target. `bar_contribution` equals `PnL.bar.profit` by construction and `surplus` equals `PnL.net_profit` — both pinned by tests, so the target can never drift from the P&L on the same screen.

`compute_break_even()` is **bar-account only** on every side: bar revenue, bar gross margin, bar operating expenses (treated as fixed for the period). Rooms are excluded deliberately — their zero-COGS revenue pushes a blended margin toward 100% and makes break-even look far easier than it is. Scoping only the *margin* to the bar while keeping whole-hotel costs would be worse still: it applies the bar's margin to room revenue that really does convert at ~100%, and reports "below break-even" in months that turned a genuine profit. Because costs and revenue move to the bar too, the figure can never disagree with the bar P&L — there's a regression test asserting the signs agree.

### Supplier credit and the two stock-purchase categories

`NON_PNL_CATEGORIES` is now `STOCK_PURCHASE_CATEGORIES = {"restock", "supplier"}`. Both are cash-out-not-cost:

- `restock` — paid on delivery (`/restock`).
- `supplier` — settlement of an earlier credit purchase (`/pay_supplier`).

`/restock_credit` deliberately writes **no** expense row: the stock arrives but no cash has moved. `restock_spend()` sums both categories so the `/position` cash estimate stays right, and `compute_working_capital` excludes `supplier` rows from `purchases_window` (they settle stock bought earlier — counting them would double-count).

**Tap-through: ⚙️ Manage → 🧾 Suppliers.** The submenu header shows what's currently owed, then offers the two halves of an invoice plus the list:

| Button | Flow | Equivalent |
|---|---|---|
| 📥 Stock on Credit | `sup_conv` — drink → qty → cost → supplier → due date | `/restock_credit` |
| ✅ Pay Supplier | `spy_conv` — pick invoice → full or partial | `/pay_supplier` |
| 🧾 What I Owe | `_cb_suppliers_list` | `/payables` |

The credit flow mirrors the restock flow up to the cost, then adds the two steps that make an invoice payable and DPO computable. The supplier step offers the 8 most recently used suppliers (`_known_suppliers()`) **by index** — supplier names carry spaces and colons, so they cannot ride in `callback_data` the way single-token drink names do, the same constraint as the staff-merge flow. Due date is quick-pick (7 / 14 / 30 days), a typed `YYYY-MM-DD`, or none. Invoice ids are ints, so the pay flow puts them in `callback_data` directly.

Both flows land on `logic.process_restock_credit()` / `logic.process_pay_supplier()` — the same entry points the slash commands use, so validation and the profit-vs-cash split can't diverge between the two surfaces. `/payables` names the button path first and the command second.

### Salary expenses
Record with category `salary`:
```
/expense bar salary 50000 bar staff wages march
/expense rooms salary 45000 rooms staff wages march
```
All reports pull salary out into its own line separate from other expenses. The allocation report warns if the salary bill exceeds the safe-to-use profit.

## Expense classification — two axes

Every expense is classified on **two independent axes** before it saves. They
answer different questions and neither substitutes for the other.

**AXIS 1 — `account`: whose cost is it?**

| | |
|---|---|
| `rooms` | disappears if rooms stopped being let |
| `bar` | disappears if the bar closed |
| `overhead` | serves the whole business, or neither department |

Overhead is **not** a bucket for anything shared-ish. Two deliberate exceptions,
both decided by the owner and both load-bearing:

- **Salaries stay on the department that causes them.** The barman is a bar
  cost. `compute_rooms_target` is built on the bar carrying its own staff and
  `compute_break_even` is bar-only on every side; moving wages to overhead
  inflates bar contribution, makes break-even look far easier than it is, and
  drops the room target. Overhead/Salary is for genuinely shared staff — the
  night watchman, not the barman. The category string is `salary` on **all
  three** accounts, because `split_salary()` matches that exact word; "salaries"
  would silently stop being a salary.
- **Diesel stays on rooms.** The generator does serve the bar, so by the strict
  Axis-1 test it is overhead — but `compute_rooms_target` names diesel
  explicitly as a shared cost that room revenue carries, and moving it would
  make rooms GOPPAR jump and break comparability with every prior month.

**AXIS 2 — `expense_class`: what kind of spend is it?**

| class | meaning | reaches the P&L? |
|---|---|---|
| `operating` | bought again next month, consumed in the month | yes |
| `irregular` | a one-off nobody could have forecast | yes — **tagged** (see below) |
| `periodic` | recurs every 3–12 months | as an accrual, not as the row |
| `capital` | creates or replaces an asset lasting 12+ months | **no** — cash only |
| `inventory` | stock for resale | **no** — cash only |

This is a separate column, **not a category value**. A category holds one
string, so "Maintenance that happens to be capital" could only be recorded by
giving up the category. `metrics.operating_expenses()` is the single gate: one
filter, so no report can drift out of agreement with another.

**A stock-purchase category always wins, before the stored class is read.**
`restock` and `supplier` resolve to `inventory` whatever the row claims to be.
Everything else defers to the stored class, then to `operating` — the safe
default, since over-expensing understates profit rather than flattering it.

That ordering is not defensive coding; it is the fix for a real failure.
`ALTER TABLE expenses ADD COLUMN expense_class TEXT DEFAULT 'operating'`
**backfilled every existing row in Postgres**, so the category fallback could
never fire — no row was legacy any more. August charged ₦345,376 of drink
purchases as a bar expense on the same screen that named them *"inventory buy —
cash to stock, not a profit cost"*, turning a ₦255,083 bar profit (39.4% net)
into a ₦90,293 loss. Every bottle was billed twice: once when bought, once as
COGS when sold. `init_db()` also carries an idempotent `UPDATE` correcting the
stored values so exports and the dashboard agree with the reports.

**The lesson for any future migration:** a backfilled `DEFAULT` is a *value*,
not an absence. Every other column added in this codebase defaults to a
sentinel that means "unset" — `cost_price 0`, `duration_hours 0`, `daypart ''`
— so a fallback can still fire. `'operating'` was a real classification, and it
silently overrode the rule it was meant to defer to.

### The overhead account was silently deleting money

`compute_pnl` filtered `account == "bar"` and `account == "rooms"`. An overhead
row matched **neither**, so it left the P&L entirely and profit was overstated
by its full amount — worse than landing in Misc, because nothing showed. Worse
still, `compute_rooms_target` filters `!= "bar"`, so it *did* count it, breaking
the documented `surplus == net_profit` invariant by exactly the overhead. `PnL`
now carries a third `overhead: AccountPnL` (revenue 0, profit = −cost) and both
invariants are pinned by tests.

### Capital: out of the P&L, not out of the bank

Capital is excluded from profit, every margin and GOPPAR — but the money left
the account, so `compute_cash_position` subtracts `capital_cash` alongside stock
purchases and draws. Excluding it from both would report money already spent.

**The capital test is enforced, not trusted.** An item is capital only if it
still exists in 12 months **and** costs at least the threshold
(`settings.capital_threshold`, default ₦50,000 — Naira and policy, so it is a
setting). Below the threshold the Capital button is simply **not rendered**, and
`process_expense` rejects it server-side, so no classification can quietly pull
a small purchase out of the P&L.

**Known limitation:** capital never enters the P&L at all, so there is no
depreciation. A ₦115,000 cable run lasting five years really costs ~₦23k/year,
and without depreciation GOPPAR flatters permanently. This is a deliberate
simplification at this size — read GOPPAR as *before capital consumption*.

### Costs nobody can predict

The accrual register handles bills you can name. Nobody can name the compressor
that fails next March — there is no expected amount to divide and no interval to
divide it over. Left as `operating` such a cost lands on one month in full:

```
normal month     Net Profit: ₦760,000
compressor dies  Net Profit: ₦340,000
```

Identical trading, a 55% collapse. So `irregular` exists, and it is handled
without forecasting anything.

**It stays in the P&L.** A dead compressor buys nothing, so unlike capital it is
a genuine cost of the business and excluding it would overstate profit. It is
*tagged*, not removed, which lets `/report` show the month two ways:

```
📈 *Net Profit:    ₦340,000*
  Net Margin:      27.0%
  🌩 One-off costs:  ₦420,000
  *Underlying:      ₦760,000*  (60.3%)  — if nothing had broken
```

`PnL.underlying_profit` is `net_profit + irregular_spend`. It is printed
**beneath** the actual figure and never instead of it — the money really was
spent; this answers the different question of whether the month traded badly or
something simply broke.

**Deliberately not accrued.** Accruing a guess while the real cost also lands in
the P&L would charge it twice — the trap `periodic` avoids by leaving the P&L
entirely. There is nothing honest to divide, so nothing is divided.

**The buffer is sized from history instead** (`metrics.compute_contingency`,
shown in `/allocation`). What the unforeseeable has *actually* cost per month
over a trailing window is compared with what the existing `buffer` allocation
sets aside. Self-calibrating, no prediction, and it plugs into the allocation
system that already exists rather than building a second reserve:

```
🌩 Contingency check (last 8 months)
    One-offs have cost ₦82,500/month on average
    Your 10% buffer sets aside ₦126,000/month
    ✅ Covered, with ₦43,500/month to spare.
```

Three things keep it honest:

- **`months_observed` counts real history, not the window.** Averaging three
  months over twelve reports a quarter of the true rate and tells the owner they
  are comfortably covered.
- **Below `MIN_CONTINGENCY_MONTHS` (6) it refuses to advise.** One breakdown in
  one month averages to that breakdown every month forever; recommending a
  tripled buffer off n=1 is worse than saying "not enough history yet".
- **It is advisory.** It reports the gap and names the percentage that would
  close it (`/setallocation buffer 12`), but never changes the allocation
  itself. Silently re-sizing something the owner set deliberately is how a tool
  stops being trusted.

Tagging happens at entry (**🌩 One-off — couldn't have seen it coming**) or
afterwards from **⚙️ Manage → 🔎 Review**, which now asks two questions of every
large entry: do you own something new (capital?), and could you have forecast it
(one-off?).

### Periodic accrual and the reserve

A bill landing every six months is a cost of **all six**. Charging it to the
month it happens to fall in makes that month look like a disaster and the other
five look better than they were — the distortion that makes an owner mistrust
their own P&L. So the cost and the payment are separated:

| | |
|---|---|
| **accrual** | `expected_amount ÷ months`, charged to the P&L every month |
| **reserve** | what those accruals have built up, minus what has been drawn |
| **payment** | the real invoice: cash out, a draw against the reserve, **not a cost** |

`periodic` is therefore **not** in `PNL_CLASSES`. A periodic expense row leaves
the P&L entirely; the accrual carries the cost instead. Counting both would
charge the bill twice.

**The accrual is computed, never stored.** `metrics.accrual_rows()` derives it
from the register on every read. Storing monthly accrual rows would need a
scheduler and would double-count on any re-run; computing it means any window,
including a historical one, gives the same answer every time. The rows are
synthesised with a real account and category, so they flow through the
Bar/Rooms/Overhead split, the salary split and the category breakdown untouched
— GOPPAR, margins and the allocation all see the cost without any of them
knowing accrual exists. They carry `id: None` and `accrual: True`, so the
expense report renders them as `🔁 accrual` rather than a tappable entry.

Accrual is **pro-rated by days within each month**: a full month charges exactly
the monthly share, half a month half of it, an all-time window one share per
month since the bill started. That is what lets `/summary` (one day) and
`/report` (one month) agree instead of one of them having to skip it.

**Two boundaries, both dates rather than flags:**

- Nothing accrues before `start_date` — registering a bill today must not
  retroactively rewrite months already reported.
- Nothing accrues after `retired_on` — but **everything before it still does**.
  Reading the `active` flag inside `accrued_for` was a bug: retiring a bill
  erased every accrual it had ever made, flipping the reserve to −₦90,000 and
  silently changing past months' profit. `set_obligation_active` stamps the
  date; retirement is never a delete.

**The reserve is a running balance**, so `compute_reserve()` reads *all-time*
rows — windowing it would report the month's movement as the whole pot. A
payment with no `obligation_id` still drains it (real money left) but is
reported as `unlinked_paid` rather than charged to whichever bill sorted first.

`ReserveLine.materially_short` is what the ⚠️ warning fires on, not `funded`: on
the 29th of a 30-day month the reserve is one day light by construction, and
warning about ₦500 on a ₦90,000 bill trains the owner to ignore the warning. A
gap has to exceed one monthly share to count.

**Every surface that computes profit must accrue**, or two screens disagree.
`reports._with_accrual()` and `_period_accrual()` (dashboard) are the shared
entry points; `compute_cash_position` takes `obligations` and accrues its two
windows internally (all-time and this month). This was got wrong once: the
accrual reached `/report` but not `/position`, and the same month read **−₦4,500
on one screen and +₦10,000 on the other**. There is a regression test pinning
`pos.month_profit == pnl.net_profit`.

**Tap path: ⚙️ Manage → 🔁 Periodic.**

| Button | Flow | What it does |
|---|---|---|
| ➕ Register a bill | `pob_conv` — name → cost → frequency → account | starts it accruing from this month |
| 💸 Pay one | `ppy_conv` — pick bill → amount | writes a `periodic` expense linked to that obligation |
| 📋 Reserve detail | `_cb_reserve_detail` | section ③ of the expense report — no second view of the same numbers |

The frequency buttons show the monthly share live (`Every 6 months — ₦15,000/mo`),
so the consequence of the choice is visible before it is made. A bill recurring
monthly or more often is refused: that is just an operating expense.

### Report sections (`/expense_report`)

1. **① OPERATING** — operating rows + the periodic **accrual**, split Bar / Rooms / Overhead
2. **② CAPITAL SPEND** — listed per item, never lumped: the repair-vs-replace
   question is per item and cannot be asked of a total
3. **③ RESERVE** — per bill: set aside, paid, balance; plus what was drawn this period

GOPPAR and net margin come from section ① only.

### Month-end check (⚙️ Manage → 🔎 Review, or `/review`)

Two lists, because they are two different mistakes. **Large entries** (≥ the
capital threshold, still in the P&L) get the repair-vs-replace question asked of
them once — the same tradesman produces a repair and an asset and the invoice
looks identical, so the question is never *who was paid* but *do I now own
something I did not own before*. **Flagged entries** are ones the person
recording them already said they were unsure about (the "🔎 Not sure" button
records `operating` + `needs_review`; Misc is treated as flagged too).

Reclassification is **in place** — `db.reclassify_expense()` changes account,
category or class and never touches the amount, date or author. Without it the
month-end check would be advice with no button behind it, since the only other
option is delete-and-rekey, which loses the original timestamp.
⚙️ Manage → 🔎 Review → ✏️ Reclassify, or `/reclassify <id> <account|class> <value>`.

### Entry flow (⚙️ Manage → 💸 Expense)

account → **category (per account)** → amount → **class** → note → date.

The class step comes *after* the amount because the capital test needs it. At or
above the threshold the prompt puts the repair-vs-replace question directly. The
category keyboard is account-aware and carries the full map — Pest Control and
Linen included — because a category that needs typing gets a near-miss picked
instead, which is the same failure as a fixed enum by another route.

### Migration

Existing rows default to `operating` on their current account, which is correct
by the "safe way to be wrong" rule. But **past capital purchases remain inside
historical P&L**, so months before the cut-over are not comparable with months
after it. Run `/review all` once to find and reclassify them.

## Month-end verification (`/stocktake`, `/roomaudit`)

Every other number in this system is the books confirming their own arithmetic.
These two commands are the only independent observations the business has, and
both are run on the last day of the month, before any report.

**⚙️ Manage → 🧾 Month-End Verification** shows whether each has been done and
opens all four steps.

### `/stocktake`

**The count sheet leaves the blanks blank.** `generate_count_sheet()` prints
item, unit cost and the date of the last count, then three empty columns. A
sheet pre-filled with what the books expect is not a count — whoever carries it
reads the expected figure and ticks it, and the one independent observation is
gone. Pinned by a test asserting every item row ends in three blanks.

**Bar and store are counted separately.** `stock_counts.location` splits them
into two rows, each measured against its own expectation, and
`record_stock_count()` trues up `current_stock` or `store_stock` accordingly —
writing a store count into `current_stock` would move phantom units onto the bar
shelf. A single combined total cannot tell a transfer from a loss.

**Status is read as a ratio, never in units** — 3 short of 12 is a different
event from 3 short of 600:

| band | status |
|---|---|
| 0 to −1% | 🟢 normal handling |
| −1 to −3% | 🟡 watch |
| worse than −3% | 🔴 flag |
| **any surplus** | 🔴 **flag** |

**A surplus is never good news.** More units than the books expect means sales
went unrecorded or a purchase was logged twice — the same leak seen from the
other side. The old report said *"✅ No shortages recorded — every count matched
or ran over"*, which read an overage as a clean bill of health; it now flags it
and says to investigate it like a shortage. `VarianceSummary.clean` requires
both `shrink_units` and `surplus_units` to be zero.

**Shrinkage is reported against the stock that went out** (`shrink_pct_of_cogs`)
— ₦2,000 short means nothing until you know whether ₦40,000 or ₦4m was sold.
Items are ordered by **naira lost, not units**: 200 of the cheapest drink can
matter less than 4 of the best. `variance_trend()` puts the last three months
side by side, and a month with **no count shows as a dash, not as zero loss**.

**A month with no stocktake is UNVERIFIED**, and `_verification_note()` stamps
that on `/report` and `/sales_report` as well as the variance report itself.
(The spec also says not to *generate* the bar report at all until the count is
done. I mark rather than block: a running business that cannot see its numbers
is worse off than one told the numbers are unverified, and the spec's own
fallback is the marking. Change it if you disagree.)

**Variance is reported, never attribution.** No count is tied to a person in any
output — the figure is about the process, and naming whoever held the sheet
turns a control into an accusation. `recorded_by` is stored but never rendered;
there is a test for it.

### Cash count — the stocktake, for money

The cash figure is every recorded inflow minus every recorded outflow. It
reconciles perfectly and proves nothing: it is the books agreeing with
themselves, exactly as stock levels were before anyone counted a bottle. What
it cannot see is money that moved without a row — an unentered expense is still
sitting inside the estimate.

**⚙️ Manage → 🧾 Month-End Verification → 💰 Count the cash** asks for the cash
physically present, then the bank balance, and compares the total with what
`/position` claims. `reports.cash_estimate()` is that exact figure, extracted so
the count is checked against what the report actually says — a count measured
against a differently-built number would compare two things and call the gap a
finding. **The expectation is read only after both figures are entered**, so the
count cannot be nudged toward a number the counter has already seen. Same
principle as the blank count sheet.

`db.record_cash_count()` does both halves in one transaction, mirroring
`record_stock_count()`: it **logs** the difference (re-anchoring alone would
erase the evidence) and **re-anchors** `cash_opening` / `cash_opening_date` to
the counted figure, so a known error stops compounding into every later report.

Bands come from `variance_status()`, shared with the stocktake, which means the
asymmetry carries over: −1.6% is 🟡 *watch*, but +1.3% is 🔴 because **any
surplus is a flag**. More money than the books expect means income was never
recorded — the same gap seen from the other side. The verdict is about the
process and never about a person: it says an amount is unaccounted for, not
that anyone took it.

`cash_count_trend()` shows the difference at each count, because one count is a
number and several are a direction. A run of shortfalls is reported as a leak
rather than as a series of unrelated errors.

### `/roomaudit`

Confirms every room-night was logged, at the rate charged.

**The bot draws the days** — `metrics.audit_days()` with `random.SystemRandom`,
never seeded from anything the operator supplies. Days a person picks are days
they remember clearly, and those are the days most likely to be correct: the
sample would be biased toward a clean result. Days are drawn from the whole
month, **not only from days that already carry bookings** — a day with no
entries at all is precisely the day worth auditing.

**The vacant lines are the exercise.** `audit_sheet()` prints every room on each
sampled date, occupied and vacant, because an occupied room that was logged
proves nothing. A room the system swears was empty is where an unlogged night
hides.

**The corrections are asked as a delta, not an absolute.** "How many nights
were actually occupied" forces mental arithmetic against a figure the bot
already knows; "how many did it miss" is nearly always a small number, so it is
a tap (`✅ None — all logged` / `+1` / `+2` / `+3` / … / `✏️ Other`). Rate
variance is the same shape — `✅ Every rate matched` covers the usual case.

**Stock counts are deliberately typed.** There is no "✓ matches the books"
button, and there must not be: it is the pre-filled-sheet problem in another
form, inviting the expected figure to be entered without anything being
counted. Typing the number is the one place friction is protective.

**Capture rate gates pricing.** Below `metrics.CAPTURE_FLOOR` (95%) the report
refuses to let a pricing decision proceed: a rate rise on nights you are not
collecting widens the gap rather than closing it. `monthly_leak` scales the gap
to a month at the **ADR actually achieved** — deliberately not at a raised rate,
which would assume the very increase the audit exists to make safe. Audits are
stored in `room_audits` and reported as a trend.

**The rate-spread check runs on the full month, always, and needs no audit
input at all.** Per room type it reports min, max, mode and the count of
distinct rates. `distinct == 1` across 30+ room-nights is 🔴: real trade
produces walk-ins, regulars, negotiated stays and the odd favour, so a perfectly
flat rate means discounts are going off-book or rates are not captured as
charged.

## Allocation System

Configured via `/setallocation <key> <percent>`. Percentages stored in the `settings` DB table; config defaults used as fallback.

### Set-aside keys (% of gross revenue — taken first, before anything else)
| Key | Default | Goes to |
|---|---|---|
| `buffer` | 10% | Savings Account |
| `restock` | 0% | Bar Account (fund from working capital by default) |

**Total default set-aside: 10%.** Nigerian corporate tax is not applicable unless annual revenue exceeds ₦50M. Personal income tax (PIT) on the owner's draw is shown as an informational estimate in the allocation report.

**Total default set-aside: 10%.** Increase to 20% (`/setallocation restock 10`) when revenue is consistent.

### Profit distribution keys (% of leftover after expenses + set-asides)
| Key | Default | Goes to |
|---|---|---|
| `draw` | 50% | Owner's personal account |
| `reinvest` | 30% | Business growth / reinvestment |
| `float` | 20% | Current account cash reserve |

### Business account structure
- **Bar Current Account** — bar sales in, bar expenses + bar salaries out
- **Rooms Current Account** — room sales in, room expenses + rooms salaries out
- **Savings Account** — weekly transfer of set-aside % from both current accounts

Weekly cadence: every Monday run `/allocation` to see exact amounts to transfer from each account into savings.

## Two-Location Inventory (Store + Bar/Freezer)

The `inventory` table tracks two separate stock locations:
- **`store_stock`** — drinks purchased and held in the store
- **`current_stock`** — drinks in the bar/freezer available for sale

**Workflow:**
1. `/restock heineken 24 300` → adds 24 to `store_stock` (admin only)
2. `/transfer heineken 12` → moves 12 from `store_stock` to `current_stock` (admin only)
3. `/sell_drink heineken 3 500` → deducts from `current_stock` (staff)

`/stock` renders a monospace table with Store and Bar columns. ⚠️ = low bar stock, 🔴 = empty store.

**Stocktakes.** `/count <drink> <units>` records what is *physically* in the bar. The database can never detect breakage or theft on its own — it only ever believes its own arithmetic — so a physical count is the one independent observation that makes loss visible. `db.record_stock_count()` does both halves in one transaction: it logs the variance (preserving the evidence) **and** corrects `current_stock` to the counted figure (stopping one bad count cascading). `/variance` rolls the counts up; `metrics.summarize_variance()` reports losses (`shrink_*`) separately from the net, since an overage in one drink must not mask a shortage in another.

**Nightly snapshots.** `_schedule_inventory_snapshot()` runs `db.record_inventory_snapshot()` at 23:55 local for every hotel, unconditionally (independent of whether the daily report is on). It is idempotent per day via `ON CONFLICT (snapshot_date, drink_name)`, so restarts and manual runs are safe.

## Backdated Entries

Any recording command accepts an optional `YYYY-MM-DD` as the **last argument**:

```
/sell_drink heineken 6 500 2025-03-15
/room standard 2 15000 3 2025-03-10
/expense bar salary 50000 2025-03-31
/add_debtor bar john 2500 tab from friday 2025-03-15
```

Detection: `_extract_date(args)` in `bot.py` checks if the last arg matches `^\d{4}-\d{2}-\d{2}$` and peels it off. The date flows down through `logic.py` → `inventory.py` → `database.py` via a `timestamp: str | None` parameter. `database._ts(custom)` converts `YYYY-MM-DD` to `YYYY-MM-DD 00:00:00`.

`/restock` does **not** support backdating.

`/transfer` backdates its **audit row only** (the `transfers` table). Store and bar
counts are a live snapshot rather than a per-date ledger, so the units always move
on execution — the date records *when the move happened*, not when stock existed where.

### Undo — `timestamp` vs `created_at`

`sales` and `rooms` carry **two** clocks, and conflating them is what broke undo:

- **`timestamp`** — the business date the entry is *for*. Backdating sets it to
  `YYYY-MM-DD 00:00:00`; every report filters on it.
- **`created_at`** — when the row was actually keyed in. Set to `now_str()` on
  insert, never backdated. This is the only column that can say whether the
  2-minute undo window is still open.

Judging the window on `timestamp` meant a backdated entry was born "days old" and
could never be undone; it also sorted *behind* today's rows, so `/undo` would reach
past it to a different entry. Both lookups (`get_last_staff_entry`,
`get_undoable_entry`) now order and age on `COALESCE(NULLIF(created_at,''), timestamp)`
— rows predating the migration fall back to `timestamp`, which is correct for
everything except backdated ones, and those had no working undo before anyway.

**The button is bound to its entry.** `record_sale()` / `record_room()` return the
new row id, which rides in the callback as `undo:<sale|room>:<id>`, so two entries
made in quick succession each undo *themselves* — reversing "the latest entry" let
the second one swallow the first one's button. `logic.process_undo_entry()` is the
targeted path; `logic.process_undo()` (the `/undo` command) still means "my latest",
and both share `_reverse_entry()`. `db.get_undoable_entry()` refuses an unknown id,
an already-voided row, another person's entry, or a closed window.

Undo is a **soft void** (`deleted_at`/`deleted_by`), never a delete — the audit trail
survives, and drink sales restore bar stock on the way out.

## Staff Tracking (`recorded_by`)

`/sell_drink` and `/room` record the Telegram username of whoever entered the entry in the `recorded_by` column (`user.username or user.first_name or str(user.id)`). `/staff_report` groups drink **and** room activity by this field.

**Reconciling duplicate names.** Because `recorded_by` is whatever string Telegram gave at entry time, the *same person* can appear under two names (a username change, or a fall-back to first-name/ID on some entries). Admins fix this from **⚙️ Manage → 👥 Staff**, which shows two linked views: the **bot-access list** (`users` table, with remove buttons) and the **report names** (distinct `recorded_by` values with txn counts; names lacking bot access are flagged ⚠️). **🔀 Reconcile Names** runs a merge flow — pick the duplicate, then pick (or type) the name to keep — and `db.merge_recorded_by()` relabels `recorded_by` across every activity table (`sales`, `rooms`, `expenses`, `owner_draws`, `debtors`, `debtor_payments`, `transfers`) plus `users.username`. It is **relabel-only**: no rows are deleted, so revenue/profit totals are unchanged — the two names simply collapse into one. To *remove* a staff name, revoke bot access from the same screen (or `/removestaff <id>`); historical records keep their `recorded_by` name. Logic lives in `logic.process_merge_staff()`; staff names are escaped with `reports._esc()` before going into MarkdownV2 (Telegram usernames routinely contain `_`).

**Correcting a debt's responsible staff.** A debtor row carries a `staff_name` (who *sold/booked* — set during `/add_debtor`, separate from `recorded_by`, the admin who keyed it). When it was attributed to the wrong staff, admins fix one debt at a time: **💳 Debtors → ✏️ Edit debt's staff** runs a pick-the-debt → pick-(or-type)-the-correct-staff flow (the `dsf` ConversationHandler), or `/set_debt_staff <id> <staff>` does it directly. Both go through `logic.process_set_debt_staff()` → `db.update_debt_staff_name()` (relabel-only on that one row; amounts/debtor untouched). This is the field shown in the dashboard Debtors table's **Staff** column.

## Database Tables

| Table | Key columns |
|---|---|
| `sales` | `id`, `timestamp`, `created_at`, `drink_name`, `quantity`, `selling_price`, `total_revenue`, `recorded_by`, `deleted_by`, `deleted_at`, `cost_price` — the cost carried at the moment of sale, so a closed month cannot be restated by a later restock |
| `rooms` | `id`, `timestamp`, `created_at`, `room_type`, `quantity`, `price_per_night`, `nights`, `total_revenue`, `recorded_by`, `deleted_by`, `deleted_at`, `duration_hours`, `daypart` (asked at entry for hourly lets, never derived from the timestamp) — `nights` is really *stay units* (nights, or lets for an hourly type); `duration_hours` of 0 defers to the room type |
| `expenses` | `id`, `timestamp`, `account`, `category`, `amount`, `description`, `expense_class`, `needs_review`, `obligation_id` — `account` is now `bar`/`rooms`/**`overhead`**; `expense_class` is the second axis and decides whether the row reaches the P&L at all |
| `owner_draws` | `id`, `timestamp`, `amount`, `account`, `description`, `recorded_by`, `deleted_by`, `deleted_at` — owner equity withdrawals, deliberately separate from `expenses` |
| `debtors` | `id`, `timestamp`, `account`, `name`, `amount`, `amount_paid`, `description`, `status`, `paid_at` |
| `debtor_payments` | `id`, `debtor_id`, `timestamp`, `amount`, `recorded_by` — one row per payment event |
| `inventory` | `drink_name`, `current_stock`, `store_stock`, `total_purchased`, `total_sold`, `cost_price`, `low_stock_threshold` |
| `users` | `user_id`, `username`, `role`, `added_at` |
| `stock_counts` | `id`, `timestamp`, `drink_name`, `expected`, `counted`, `variance`, `cost_price`, `note`, `recorded_by`, `location` (bar/store — counted separately), `period` — one physical stocktake; the only independent check on the books |
| `cash_counts` | `id`, `timestamp`, `period`, `expected`, `till`, `bank`, `counted`, `variance`, `note`, `recorded_by` — the only independent check on the cash figure |
| `room_audits` | `id`, `timestamp`, `audit_date`, `period`, `rooms_total`, `nights_logged`, `nights_actual`, `rate_variance`, `variance_count`, `note`, `recorded_by` — capture rate as a trend, not a one-off |
| `turnaways` | `id`, `timestamp`, `created_at`, `room_type`, `quantity`, `reason`, `recorded_by` — guests refused for want of a room. Touches no money; the only record of demand that never became a transaction |
| `payables` | `id`, `timestamp`, `supplier`, `drink_name`, `quantity`, `amount`, `amount_paid`, `due_date`, `status`, `paid_at`, `recorded_by` — supplier credit; makes DPO computable |
| `inventory_snapshots` | `snapshot_date`, `drink_name`, `bar_stock`, `store_stock`, `cost_price`, `stock_value` — PK `(snapshot_date, drink_name)`; nightly stock history for true DIO |
| `settings` | `key`, `value` — stores allocation percentages, `cash_opening` (opening bank balance for `/position`), `cash_opening_date` (optional anchor date; cash counts only flows on/after it), `total_rooms` (occupancy/RevPAR denominator), `roomtype_rooms:<type>` (per-type RevPAR denominator), `roomtype_hours:<type>` (hours per stay-unit — marks a type as hourly) `capital_threshold` (minimum spend that can be capital) and `purchase_cap` (stock spend ceiling as a % of bar revenue) |
| `periodic_obligations` | `id`, `name`, `account`, `category`, `expected_amount`, `months`, `start_date`, `active`, `retired_on`, `created_at`, `recorded_by` — the accrual register. Without it there is nothing to accrue *against* |

All schema migrations use `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` so existing databases upgrade safely on next startup.

## Deployment

- **Railway**: `railway.toml` configures `python bot.py` as start command with `on_failure` restart policy
- **Heroku**: `Procfile` with `worker: python bot.py` (no web dyno needed)
- `DATABASE_URL` starting with `postgres://` is auto-corrected to `postgresql://` in `database.py:get_engine()`

## Web Dashboard (read-only)

`dashboard/` is a **separate FastAPI service** that renders the hotel's numbers in a browser for viewing — it does **not** replace the bot (Telegram stays the source of truth) and is **read-only**. It reuses `metrics.py` + `database.py`, authenticates via the Telegram Login Widget (HMAC verified against `BOT_TOKEN`), and sets `database._hotel_schema_var` per request for tenant isolation. Deploy as its own service against the **same** `DATABASE_URL`; start command `uvicorn dashboard.app:app --host 0.0.0.0 --port $PORT`; health check `GET /healthz`. Web deps live in `requirements-web.txt` (kept out of the bot's `requirements.txt`). Full setup in `dashboard/README.md`. Tests/dev deps: `requirements-dev.txt`; run `python3 -m pytest`.
