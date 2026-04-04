# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A Telegram bot for hotel operations management — tracking bar drink sales/stock, room bookings, expenses, and debtors, with split Bar/Rooms P&L reporting. Currency is ₦ (Naira). Deployed on Railway (or Heroku) with a PostgreSQL backend.

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

**`bot.py`** — Entry point. All Telegram command handlers, access control decorators (`_require_auth`, `_require_admin`), argument parsing, and job scheduling. Delegates all business logic to `logic.py` and `reports.py`.

**`logic.py`** — Business logic and validation layer. All public functions return `(ok: bool, message: str)`. Validates inputs before calling `inventory.py` or `database.py`.

**`inventory.py`** — Drink stock operations only. Returns `StockResult` dataclass. Enforces no-negative-stock rule on bar sales, generates low-stock alerts, tracks cost prices. Exposes `transfer_to_bar()` for store→bar movements.

**`database.py`** — PostgreSQL persistence via SQLAlchemy + pandas. All queries use parameterised statements. `read_all(table)` returns `list[dict]` using `pd.read_sql`. The `upsert_drink()` function does an atomic `INSERT ... ON CONFLICT DO UPDATE`.

**`reports.py`** — Pure formatting: reads data from `database.py`/`inventory.py`, builds Telegram Markdown strings. Reports separate Bar and Rooms P&L. Cost-of-drinks-sold uses *current* cost price (not historical per-sale cost).

**`config.py`** — All env var loading via `python-dotenv`. Imported directly wherever needed.

## Monthly Reporting

`/report` defaults to the **current month**. Sales, rooms, expenses are all filtered by month; inventory (`/stock`) is always cumulative and unfiltered.

| Command | Shows |
|---|---|
| `/report` | Current month |
| `/report today` | Today only |
| `/report 2025-03` | Specific month |
| `/report all` | All-time |

Implemented via `_filter_by_month(rows, year, month)` in `reports.py`. `generate_full_report()` accepts `for_date`, `for_month`, or `all_time=True`. Outstanding debtors always show all-time regardless of the period filter.

## Two-Location Inventory (Store + Bar/Freezer)

The `inventory` table tracks two separate stock locations:
- **`store_stock`** — drinks purchased and held in the store
- **`current_stock`** — drinks in the bar/freezer available for sale

**Workflow:**
1. `/restock heineken 24 300` → adds 24 to `store_stock`
2. `/transfer heineken 12` (admin only) → moves 12 from `store_stock` to `current_stock`
3. `/sell_drink heineken 3 500` → deducts from `current_stock`

`/stock` shows both columns side by side with separate alerts: ⚠️ for low bar stock (prompts a transfer), 🔴 for empty store (prompts a restock).

`database.transfer_drink()` does the store→bar move atomically and rejects if store stock is insufficient. The `init_db()` migration adds `store_stock` via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` so existing databases are upgraded safely on next startup.

## Backdated Entries

Any recording command accepts an optional `YYYY-MM-DD` as the **last argument** to log the entry under a past date:

```
/sell_drink heineken 6 500 2025-03-15
/room standard 2 15000 3 2025-03-10
/expense bar cleaning 5000 generator repair 2025-03-20
/add_debtor bar john 2500 tab from friday 2025-03-15
```

Detection: `_extract_date(args)` in `bot.py` checks if the last arg matches `^\d{4}-\d{2}-\d{2}$` and peels it off before the rest of the args are parsed. The date flows down through `logic.py` → `inventory.py` → `database.py` via a `timestamp: str | None` parameter on all `record_*` functions. `database._ts(custom)` converts `YYYY-MM-DD` to `YYYY-MM-DD 00:00:00`.

`/restock` does **not** support backdating — it only updates cumulative totals in the `inventory` table (no timestamped row exists for restocks).

## Access Control

Two roles: `admin` and `staff`. Role lookup hits the `users` table on every request (no caching). `ADMIN_IDS` in env provides a hardcoded admin override that bypasses the DB check.

## Deployment

- **Railway**: `railway.toml` configures `python bot.py` as start command with `on_failure` restart policy
- **Heroku**: `Procfile` with `worker: python bot.py` (no web dyno needed)
- `DATABASE_URL` starting with `postgres://` is auto-corrected to `postgresql://` in `database.py:get_engine()`
