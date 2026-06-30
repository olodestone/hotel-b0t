# Hotel Dashboard (read-only web view)

A separate FastAPI service that renders the hotel's numbers in a browser so you
can **view** them without opening Telegram. It does **not** write anything and
does **not** replace the bot — Telegram stays the source of truth. Every figure
comes from the shared `metrics.py` calc core, so the dashboard can never
disagree with the bot's reports.

## Run locally

```bash
pip install -r requirements-web.txt
export DATABASE_URL=postgresql://...      # same DB as the bot
export HOTEL_SCHEMA=hotel85               # same schema as the bot
export BOT_TOKEN=...                       # used to verify Telegram logins
export DASHBOARD_SECRET=$(openssl rand -hex 32)
export DASHBOARD_LOGIN_BOT_USERNAME=your_bot_username   # without the @

uvicorn dashboard.app:app --reload --port 8000
# open http://localhost:8000
```

For local dev without wiring a real Telegram bot, set
`DASHBOARD_DEV_LOGIN_ID=<your-telegram-id>` and use the **Dev login** button.
Never set that variable in production.

## Telegram Login Widget setup

1. Message **@BotFather** → `/setdomain` → choose your bot → set it to the
   dashboard's public domain (e.g. `dash.yourhotel.com`).
2. Set `DASHBOARD_LOGIN_BOT_USERNAME` to that bot's username.
3. The widget posts the signed login back to `/auth/telegram`, which verifies
   the HMAC with `BOT_TOKEN` and resolves the user's hotel access.

Who can sign in: the same identities the bot trusts — `OWNER_ID`, a hotel's
owner, anyone in its `admin_ids`, or any user in that hotel's `users` table.

## Deploy on Railway (separate service)

Run the dashboard as its **own** Railway service in the same project as the
bot, pointed at the **same** PostgreSQL — a distinct process so a web crash can
never take the bot down. Config-as-code lives in **`railway.dashboard.toml`**.

1. Railway project → **New service** → deploy from this same repo.
2. Service → **Settings → Config-as-code path** → `railway.dashboard.toml`
   (the bot service keeps the default `railway.toml` / `python bot.py`).
3. Service → **Variables**: `DATABASE_URL` (reference the Postgres plugin so it
   shares the bot's DB), `HOTEL_SCHEMA`, `BOT_TOKEN`, `DASHBOARD_SECRET`,
   `DASHBOARD_LOGIN_BOT_USERNAME`.
4. BotFather → `/setdomain` → the dashboard service's public domain.

That config installs `requirements-web.txt`, starts
`uvicorn dashboard.app:app --host 0.0.0.0 --port $PORT`, and health-checks
`GET /healthz`.

## Environment variables

| Var | Required | Notes |
|---|---|---|
| `DATABASE_URL` | yes | Same Postgres as the bot |
| `HOTEL_SCHEMA` | yes | Hotel schema to read |
| `BOT_TOKEN` / `DASHBOARD_LOGIN_BOT_TOKEN` | yes | Verifies Telegram logins |
| `DASHBOARD_LOGIN_BOT_USERNAME` | yes | Bot username for the Login Widget |
| `DASHBOARD_SECRET` | prod | Session cookie signing key (ephemeral if unset) |
| `DASHBOARD_SESSION_MAX_AGE` | no | Session lifetime, seconds (default 7 days) |
| `DASHBOARD_INSECURE_COOKIE` | no | `1` for local http only |
| `DASHBOARD_DEV_LOGIN_ID` | no | **Local only** — bypasses Telegram verification |

## Status

Live. A "what you have now" cash-at-hand / stock / receivables / profit snapshot
(same numbers as the bot's `/position`), plus period P&L, revenue trend,
allocation (set-asides + profit distribution), sales-by-drink, staff activity,
stock, and outstanding debtors — with a hotel switcher for multi-hotel users and
CSV export. Every figure matches the bot's reports (shared `metrics.py`).

Any period is browsable: the quick segments (This month / Today / All-time) sit
beside a **month picker** and a **day picker**, so you can jump to any prior month
or specific date. Changing the period or staff filter **swaps just the affected
region in place** (fetched from `GET /partial/period`) instead of reloading the
whole page — scroll position and the "what you have now" card are preserved, and
the URL still updates so links/back-forward work. JS-off or on fetch error it
falls back to a normal full navigation. A **Records** section renders the raw Sales, Rooms, Expenses and
Debtors entries for the selected period right in the browser (collapsible tables) —
viewing no longer requires a CSV download, though the export buttons remain.

The itemised **Expenses** table and its CSV export are **admin-only** (enforced
both in the template and server-side on `/export/expenses.csv`), mirroring the
bot: staff see expense *totals* in the P&L but not the per-row breakdown. Access
itself is authorization-gated — a verified Telegram login only proves identity;
the account must be the hotel's owner, in its admin list, or in its `users` table
(staff/admin) to get in, otherwise it is bounced with "no hotel access".
