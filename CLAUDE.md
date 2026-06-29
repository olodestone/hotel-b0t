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

dashboard/ (separate FastAPI web service, read-only) → metrics.py + database.py
```

**`bot.py`** — Entry point. All Telegram command handlers, access control decorators (`_require_auth`, `_require_admin`), argument parsing, and job scheduling. Delegates all business logic to `logic.py` and `reports.py`. `/help` is role-aware — staff see staff commands only, admins see all.

**`logic.py`** — Business logic and validation layer. All public functions return `(ok: bool, message: str)`. Validates inputs before calling `inventory.py` or `database.py`.

**`inventory.py`** — Drink stock operations only. Returns `StockResult` dataclass. Enforces no-negative-stock rule on bar sales, generates low-stock alerts, tracks cost prices. Exposes `transfer_to_bar()` for store→bar movements.

**`database.py`** — PostgreSQL persistence via SQLAlchemy + pandas. All queries use parameterised statements. `read_all(table)` returns `list[dict]` using `pd.read_sql`. The `upsert_drink()` function does an atomic `INSERT ... ON CONFLICT DO UPDATE`. `get_setting()`/`set_setting()` manage the `settings` table for configurable percentages.

**`reports.py`** — Telegram formatting: reads data from `database.py`/`inventory.py`, runs the numbers through `metrics.py`, builds Telegram MarkdownV2 strings. Reports separate Bar and Rooms P&L. Cost-of-drinks-sold uses *current* cost price (not historical per-sale cost). Salary expenses are always split out separately from other expenses. Profit calcs exclude `restock` (inventory purchase) and owner draws — see "Profit vs Cash vs Stock" below.

**`metrics.py`** — Pure financial calc core (no DB, no formatting). Functions take already-fetched rows + a cost-price map and return dataclasses (`compute_pnl` → `PnL`/`AccountPnL`, `summarize_outstanding`, plus shared row helpers `apply_filter`/`active`/`operating_expenses`/`cost_of_drinks_sold`/…). This is the **single source of truth** for the math, shared by `reports.py` (Telegram) and `dashboard/` (web) so the two can never disagree. The full report, position, and allocation reports consume it (`compute_pnl`, `compute_cash_position`, `compute_allocation`); per-staff is the remaining extraction target. Golden-master tests in `tests/` assert the Telegram output is byte-for-byte unchanged.

**`config.py`** — All env var loading via `python-dotenv`. Also holds allocation defaults (`ALLOC_*`) used as fallback when DB settings are not yet set.

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

Staff cannot delete anything — audit trail is preserved. Mistakes are corrected by admin via `/delete` then re-entry.

## Commands Reference

### Staff commands
| Command | Description |
|---|---|
| `/sell_drink <drink> <qty> <price> [YYYY-MM-DD]` | Record drink sale |
| `/room <type> <qty> <price> <nights> [YYYY-MM-DD]` | Record room booking |
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
| `/pay_debtor <room\|bar> <name> [amount]` | Full or partial debt payment |
| `/debtor_history <bar\|rooms> <name>` | Full payment timeline for a debtor |
| `/restock <drink> <qty> <cost_price>` | Add inventory to store. Logged as a `restock` expense row but treated as a cash→stock movement, **not** a P&L cost |
| `/transfer <drink> <qty>` | Move store → bar |
| `/delete <sale\|room\|expense\|draw> <id>` | Remove an entry |
| `/staff_report [today\|YYYY-MM-DD\|YYYY-MM]` | Sales per staff member |
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

### Profit vs Cash vs Stock (accounting model)

Three figures are tracked separately and must never be conflated:

1. **Profit (performance)** — `revenue − cost-of-stock-sold (COGS) − operating expenses`. **Owner draws and inventory purchases (`restock`) are excluded.** Buying stock converts cash into a stock asset; its cost only hits the P&L as COGS when the drink is *sold*. Counting the restock purchase as an expense too would double-count it. `reports._operating_expenses()` strips `restock` (the `NON_PNL_CATEGORIES` set) from every profit calc (`generate_full_report`, `generate_daily_summary`, `generate_expense_report`, `generate_allocation_report`).
2. **Cash in bank (estimate)** — running balance: `opening + collected sales − operating expenses − stock purchases − owner draws`. Draws and restock **do** reduce cash. The `opening` anchor works two ways: with an **anchor date** (`/position set <amount> <YYYY-MM-DD>`, stored in `cash_opening_date`) only flows on/after that day are counted — `opening` is your real balance on that day, earlier months are ignored, and you can re-anchor each period without double-counting. Without a date, `opening` is the all-time starting balance before the first entry and every flow is added on top (set once; never to a current balance). Assumes sales are cash unless an outstanding debtor exists.
3. **Stock value on hand (asset)** — `Σ (store + bar units) × cost_price`. Shown as `TOTAL VALUE` in `/stock` and line ③ of `/position`.

`/position` shows all three side by side plus outstanding receivables. Owner draws live in the dedicated `owner_draws` table, never in `expenses`, so they can never touch profit.

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

## Staff Tracking (`recorded_by`)

`/sell_drink` and `/room` record the Telegram username of whoever entered the entry in the `recorded_by` column (`user.username or user.first_name or str(user.id)`). `/staff_report` groups drink **and** room activity by this field.

**Reconciling duplicate names.** Because `recorded_by` is whatever string Telegram gave at entry time, the *same person* can appear under two names (a username change, or a fall-back to first-name/ID on some entries). Admins fix this from **⚙️ Manage → 👥 Staff**, which shows two linked views: the **bot-access list** (`users` table, with remove buttons) and the **report names** (distinct `recorded_by` values with txn counts; names lacking bot access are flagged ⚠️). **🔀 Reconcile Names** runs a merge flow — pick the duplicate, then pick (or type) the name to keep — and `db.merge_recorded_by()` relabels `recorded_by` across every activity table (`sales`, `rooms`, `expenses`, `owner_draws`, `debtors`, `debtor_payments`, `transfers`) plus `users.username`. It is **relabel-only**: no rows are deleted, so revenue/profit totals are unchanged — the two names simply collapse into one. To *remove* a staff name, revoke bot access from the same screen (or `/removestaff <id>`); historical records keep their `recorded_by` name. Logic lives in `logic.process_merge_staff()`; staff names are escaped with `reports._esc()` before going into MarkdownV2 (Telegram usernames routinely contain `_`).

## Database Tables

| Table | Key columns |
|---|---|
| `sales` | `id`, `timestamp`, `drink_name`, `quantity`, `selling_price`, `total_revenue`, `recorded_by` |
| `rooms` | `id`, `timestamp`, `room_type`, `quantity`, `price_per_night`, `nights`, `total_revenue` |
| `expenses` | `id`, `timestamp`, `account`, `category`, `amount`, `description` |
| `owner_draws` | `id`, `timestamp`, `amount`, `account`, `description`, `recorded_by`, `deleted_by`, `deleted_at` — owner equity withdrawals, deliberately separate from `expenses` |
| `debtors` | `id`, `timestamp`, `account`, `name`, `amount`, `amount_paid`, `description`, `status`, `paid_at` |
| `debtor_payments` | `id`, `debtor_id`, `timestamp`, `amount`, `recorded_by` — one row per payment event |
| `inventory` | `drink_name`, `current_stock`, `store_stock`, `total_purchased`, `total_sold`, `cost_price`, `low_stock_threshold` |
| `users` | `user_id`, `username`, `role`, `added_at` |
| `settings` | `key`, `value` — stores allocation percentages, `cash_opening` (opening bank balance for `/position`) and `cash_opening_date` (optional anchor date; cash counts only flows on/after it) |

All schema migrations use `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` so existing databases upgrade safely on next startup.

## Deployment

- **Railway**: `railway.toml` configures `python bot.py` as start command with `on_failure` restart policy
- **Heroku**: `Procfile` with `worker: python bot.py` (no web dyno needed)
- `DATABASE_URL` starting with `postgres://` is auto-corrected to `postgresql://` in `database.py:get_engine()`

## Web Dashboard (read-only)

`dashboard/` is a **separate FastAPI service** that renders the hotel's numbers in a browser for viewing — it does **not** replace the bot (Telegram stays the source of truth) and is **read-only**. It reuses `metrics.py` + `database.py`, authenticates via the Telegram Login Widget (HMAC verified against `BOT_TOKEN`), and sets `database._hotel_schema_var` per request for tenant isolation. Deploy as its own service against the **same** `DATABASE_URL`; start command `uvicorn dashboard.app:app --host 0.0.0.0 --port $PORT`; health check `GET /healthz`. Web deps live in `requirements-web.txt` (kept out of the bot's `requirements.txt`). Full setup in `dashboard/README.md`. Tests/dev deps: `requirements-dev.txt`; run `python3 -m pytest`.
