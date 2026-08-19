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

**`reports.py`** — Telegram formatting: reads data from `database.py`/`inventory.py`, runs the numbers through `metrics.py`, builds Telegram MarkdownV2 strings. Reports separate Bar and Rooms P&L. Cost-of-drinks-sold uses *current* cost price (not historical per-sale cost). Salary expenses are always split out separately from other expenses. Profit calcs exclude `restock` (inventory purchase) and owner draws — see "Profit vs Cash vs Stock" below.

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
- `/report`, `/stock`, `/summary`, `/history`, `/debtors` — view only

**Admin only:**
- `/expense`, `/add_debtor`, `/pay_debtor`, `/restock`, `/transfer`, `/delete`
- `/sales_report`, `/expense_report`, `/staff_report`, `/allocation`, `/setallocation`
- `/setthreshold`, `/addstaff`, `/removestaff`, `/dailyreport`
- `/cashcycle`, `/menu`, `/roomstats`, `/setrooms` — performance analysis
- `/count`, `/variance` — stocktakes and shrinkage
- `/restock_credit`, `/payables`, `/pay_supplier` — supplier credit

The five analysis reports are also reachable from **⚙️ Manage → 📈 Insights** (a submenu, so the Manage keyboard stays readable); 🛏 Room Stats there opens a period picker (this week / last week / this month / all time) rather than jumping straight to the month. Supplier credit has its own **⚙️ Manage → 🧾 Suppliers** submenu — see below.

Staff cannot delete anything — audit trail is preserved. Mistakes are corrected by admin via `/delete` then re-entry.

## Commands Reference

### Staff commands
| Command | Description |
|---|---|
| `/sell_drink <drink> <qty> <price> [YYYY-MM-DD]` | Record drink sale |
| `/room <type> <qty> <price> <nights> [YYYY-MM-DD]` | Record room booking |
| `/undo` | Reverse your own most recent sale or booking, within 2 minutes |
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
| `/expense <room\|bar> <category> <amount> [note] [YYYY-MM-DD]` | Record expense. Use `salary` as category for staff wages. Draw-like categories (`drawings`, `owner`, `withdrawal`, …) are rejected and routed to `/draw` |
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
| `/count <drink> <units> [note] [YYYY-MM-DD]` | Physical stocktake: logs the variance vs what the books expected, then corrects bar stock to the counted figure |
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
| `generate_variance_report()` | `/variance` |
| `generate_payables_report()` | `/payables` |

### Profit vs Cash vs Stock (accounting model)

Three figures are tracked separately and must never be conflated:

1. **Profit (performance)** — `revenue − cost-of-stock-sold (COGS) − operating expenses`. **Owner draws and inventory purchases (`restock`) are excluded.** Buying stock converts cash into a stock asset; its cost only hits the P&L as COGS when the drink is *sold*. Counting the restock purchase as an expense too would double-count it. `reports._operating_expenses()` strips `restock` (the `NON_PNL_CATEGORIES` set) from every profit calc (`generate_full_report`, `generate_daily_summary`, `generate_expense_report`, `generate_allocation_report`).
2. **Cash in bank (estimate)** — running balance: `opening + collected sales − operating expenses − stock purchases − owner draws`. Draws and restock **do** reduce cash. The `opening` anchor works two ways: with an **anchor date** (`/position set <amount> <YYYY-MM-DD>`, stored in `cash_opening_date`) only flows on/after that day are counted — `opening` is your real balance on that day, earlier months are ignored, and you can re-anchor each period without double-counting. Without a date, `opening` is the all-time starting balance before the first entry and every flow is added on top (set once; never to a current balance). Assumes sales are cash unless an outstanding debtor exists.
3. **Stock value on hand (asset)** — `Σ (store + bar units) × cost_price`. Shown as `TOTAL VALUE` in `/stock` and line ③ of `/position`.

`/position` shows all three side by side plus outstanding receivables and (when any exist) unpaid supplier invoices. Owner draws live in the dedicated `owner_draws` table, never in `expenses`, so they can never touch profit.

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

**RevPAR by room type** needs a per-type denominator, stored as `roomtype_rooms:<type>` in `settings` (`db.get_all_room_type_counts()`), set with `/setrooms <type> <n>` or from **⚙️ Manage → ⚙️ Settings → 🏨 Room Counts** (the `src` ConversationHandler). That screen lists the hotel total and every type the hotel has priced, counted or actually booked (`_known_room_types()` unions the three sources), shows which are still unset, and warns when the per-type counts don't sum to the total. Types are picked **by index** — they are free text and can contain spaces (`short time`), so they can't ride in `callback_data`. Because this is a setting you enter three or four times in a row, the confirmation carries a *Set another* button straight back into the flow. Its neighbour `sset:roomtype` sets room **prices**, and is labelled "🛏 Room Prices" so the two aren't confused. Each type divides by *its own* room count — borrowing `total_rooms` would credit every room in the building to one category — so a type with no count recorded shows `RevPAR n/a` and a prompt, never a wrong number. By-type ADR needs no denominator and is always populated. If `total_rooms` is unset but per-type counts exist, their sum becomes the hotel-wide denominator; if both are set and disagree, `/setrooms` warns (legitimate when rooms are out of service).

**GOPPAR — the bottom-line twin.** RevPAR is a revenue metric: it cannot see fuel, wages, restocking or maintenance, so a hotel can post a rising RevPAR straight through a month it lost money on. `metrics.compute_goppar()` divides *profit* by the same denominator and prints directly beneath it, so the gap between the two lines **is** the cost base.

GOP is taken straight from `compute_pnl` — whole-hotel GOP **is** `PnL.net_profit`, rooms GOP **is** `PnL.rooms.profit` — so GOPPAR can never drift from the P&L (pinned by tests, the same invariant as `compute_rooms_target`). Stock purchases and owner draws are already excluded upstream by `operating_expenses()`, so buying stock never depresses GOPPAR.

- **GOPPAR** — whole-hotel profit per available room-night. The headline.
- **Rooms only** — `PnL.rooms.profit` per available room-night, isolating the rooms department.
- **Conversion** — `goppar / revpar`, the share of RevPAR that survives as profit. It can exceed 100% when the bar carries the rooms, and the report says so rather than letting it look like an error. `rooms_conversion_pct` equals `PnL.rooms.net_margin_pct` exactly (same denominator both sides) and stays ≤ 100%.

Caveat worth keeping in mind: Hotel 85's expense categories don't separate fixed charges (rent, insurance) from operating ones, so this is nearer *net operating profit* per available room than strict USALI GOP.

`compare_goppar()` answers the question RevPAR alone cannot — **did a revenue gain reach the bottom line?** RevPAR ↑ + GOPPAR ↑ = the gain is real; RevPAR ↑ + GOPPAR → = rising costs absorbed the whole thing (the fuel-pass-through case: a rate rise that only covers the diesel it was raised for); RevPAR ↑ + GOPPAR ↓ = costs are rising faster than rates. When the prior period's GOP was zero or negative, the direction is reported without a percentage — coming back from a loss is a direction, not "−200% growth". The whole block is suppressed when no room count is set, since there is no denominator.

`reports._yield_gap_note()` flags the trap the split exists for: the type charging the **most** per night being out-earned per room owned by a **cheaper** one. A cheap type yielding least is not the trap — that is just a cheap room — so the note fires only when the winner's ADR is lower and the gap clears `TREND_BAND`.

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
| `sales` | `id`, `timestamp`, `created_at`, `drink_name`, `quantity`, `selling_price`, `total_revenue`, `recorded_by`, `deleted_by`, `deleted_at` |
| `rooms` | `id`, `timestamp`, `created_at`, `room_type`, `quantity`, `price_per_night`, `nights`, `total_revenue`, `recorded_by`, `deleted_by`, `deleted_at` |
| `expenses` | `id`, `timestamp`, `account`, `category`, `amount`, `description` |
| `owner_draws` | `id`, `timestamp`, `amount`, `account`, `description`, `recorded_by`, `deleted_by`, `deleted_at` — owner equity withdrawals, deliberately separate from `expenses` |
| `debtors` | `id`, `timestamp`, `account`, `name`, `amount`, `amount_paid`, `description`, `status`, `paid_at` |
| `debtor_payments` | `id`, `debtor_id`, `timestamp`, `amount`, `recorded_by` — one row per payment event |
| `inventory` | `drink_name`, `current_stock`, `store_stock`, `total_purchased`, `total_sold`, `cost_price`, `low_stock_threshold` |
| `users` | `user_id`, `username`, `role`, `added_at` |
| `stock_counts` | `id`, `timestamp`, `drink_name`, `expected`, `counted`, `variance`, `cost_price`, `note`, `recorded_by` — one physical stocktake; the only independent check on the books |
| `payables` | `id`, `timestamp`, `supplier`, `drink_name`, `quantity`, `amount`, `amount_paid`, `due_date`, `status`, `paid_at`, `recorded_by` — supplier credit; makes DPO computable |
| `inventory_snapshots` | `snapshot_date`, `drink_name`, `bar_stock`, `store_stock`, `cost_price`, `stock_value` — PK `(snapshot_date, drink_name)`; nightly stock history for true DIO |
| `settings` | `key`, `value` — stores allocation percentages, `cash_opening` (opening bank balance for `/position`), `cash_opening_date` (optional anchor date; cash counts only flows on/after it), `total_rooms` (occupancy/RevPAR denominator) and `roomtype_rooms:<type>` (per-type RevPAR denominator) |

All schema migrations use `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` so existing databases upgrade safely on next startup.

## Deployment

- **Railway**: `railway.toml` configures `python bot.py` as start command with `on_failure` restart policy
- **Heroku**: `Procfile` with `worker: python bot.py` (no web dyno needed)
- `DATABASE_URL` starting with `postgres://` is auto-corrected to `postgresql://` in `database.py:get_engine()`

## Web Dashboard (read-only)

`dashboard/` is a **separate FastAPI service** that renders the hotel's numbers in a browser for viewing — it does **not** replace the bot (Telegram stays the source of truth) and is **read-only**. It reuses `metrics.py` + `database.py`, authenticates via the Telegram Login Widget (HMAC verified against `BOT_TOKEN`), and sets `database._hotel_schema_var` per request for tenant isolation. Deploy as its own service against the **same** `DATABASE_URL`; start command `uvicorn dashboard.app:app --host 0.0.0.0 --port $PORT`; health check `GET /healthz`. Web deps live in `requirements-web.txt` (kept out of the bot's `requirements.txt`). Full setup in `dashboard/README.md`. Tests/dev deps: `requirements-dev.txt`; run `python3 -m pytest`.
