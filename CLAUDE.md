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
       →  reports.py →  inventory.py  →  database.py
                     →  database.py
config.py  (imported by all layers)
```

**`bot.py`** — Entry point. All Telegram command handlers, access control decorators (`_require_auth`, `_require_admin`), argument parsing, and job scheduling. Delegates all business logic to `logic.py` and `reports.py`. `/help` is role-aware — staff see staff commands only, admins see all.

**`logic.py`** — Business logic and validation layer. All public functions return `(ok: bool, message: str)`. Validates inputs before calling `inventory.py` or `database.py`.

**`inventory.py`** — Drink stock operations only. Returns `StockResult` dataclass. Enforces no-negative-stock rule on bar sales, generates low-stock alerts, tracks cost prices. Exposes `transfer_to_bar()` for store→bar movements.

**`database.py`** — PostgreSQL persistence via SQLAlchemy + pandas. All queries use parameterised statements. `read_all(table)` returns `list[dict]` using `pd.read_sql`. The `upsert_drink()` function does an atomic `INSERT ... ON CONFLICT DO UPDATE`. `get_setting()`/`set_setting()` manage the `settings` table for configurable percentages.

**`reports.py`** — Pure formatting: reads data from `database.py`/`inventory.py`, builds Telegram Markdown strings. Reports separate Bar and Rooms P&L. Cost-of-drinks-sold uses *current* cost price (not historical per-sale cost). Salary expenses are always split out separately from other expenses.

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
| `/expense <room\|bar> <category> <amount> [note] [YYYY-MM-DD]` | Record expense. Use `salary` as category for staff wages |
| `/add_debtor <room\|bar> <name> <amount> [note] [YYYY-MM-DD]` | Log debtor |
| `/pay_debtor <room\|bar> <name> [amount]` | Full or partial debt payment |
| `/debtor_history <bar\|rooms> <name>` | Full payment timeline for a debtor |
| `/restock <drink> <qty> <cost_price>` | Add inventory to store |
| `/transfer <drink> <qty>` | Move store → bar |
| `/delete <sale\|room\|expense> <id>` | Remove an entry |
| `/staff_report [today\|YYYY-MM-DD\|YYYY-MM]` | Sales per staff member |
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
| `generate_stock_report()` | `/stock` |
| `generate_debtors_report()` | `/debtors` |

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

`/sell_drink` records the Telegram username of whoever entered the sale in the `recorded_by` column of the `sales` table. `/staff_report` groups sales by this field. Room bookings do not yet track `recorded_by`.

## Database Tables

| Table | Key columns |
|---|---|
| `sales` | `id`, `timestamp`, `drink_name`, `quantity`, `selling_price`, `total_revenue`, `recorded_by` |
| `rooms` | `id`, `timestamp`, `room_type`, `quantity`, `price_per_night`, `nights`, `total_revenue` |
| `expenses` | `id`, `timestamp`, `account`, `category`, `amount`, `description` |
| `debtors` | `id`, `timestamp`, `account`, `name`, `amount`, `amount_paid`, `description`, `status`, `paid_at` |
| `debtor_payments` | `id`, `debtor_id`, `timestamp`, `amount`, `recorded_by` — one row per payment event |
| `inventory` | `drink_name`, `current_stock`, `store_stock`, `total_purchased`, `total_sold`, `cost_price`, `low_stock_threshold` |
| `users` | `user_id`, `username`, `role`, `added_at` |
| `settings` | `key`, `value` — stores allocation percentages |

All schema migrations use `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` so existing databases upgrade safely on next startup.

## Deployment

- **Railway**: `railway.toml` configures `python bot.py` as start command with `on_failure` restart policy
- **Heroku**: `Procfile` with `worker: python bot.py` (no web dyno needed)
- `DATABASE_URL` starting with `postgres://` is auto-corrected to `postgresql://` in `database.py:get_engine()`
