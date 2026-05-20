# Hotel Bot — Operations & SaaS Management Guide

A Telegram bot for hotel operations management — bar sales, room bookings, expenses, debtors, staff tracking, and financial reporting. Built as a multi-tenant SaaS: one Railway service + one PostgreSQL database serves unlimited hotels, each fully isolated.

---

## Architecture

```
Railway Platform
├── One Service (bot.py)         ← all hotels share this
└── One PostgreSQL Database
    ├── public.hotels            ← master registry of all hotels
    ├── hotel85 schema           ← your hotel's data
    ├── kings_inn schema         ← hotel 2's data
    └── abc_hotel schema         ← hotel 3's data (and so on)
```

Each hotel's data lives in its own PostgreSQL schema. They cannot see each other's data even though they run in the same process.

---

## Access Levels

```
App Owner (you)
├── Onboard / suspend / delete hotels
├── Set who the hotel admin is
└── Full database access

    Hotel Owner (admin)
    ├── Add / remove their own staff
    ├── Record all operations
    ├── View all reports and financials
    ├── Configure prices, allocations, thresholds
    ├── Export their own data (/export)
    └── Cannot see other hotels or touch DB

        Staff
        ├── Record sales and room bookings
        ├── View reports, stock, debtors
        ├── Undo their own last entry (2 min window)
        └── No delete, no config, no financial settings
```

---

## 1. App Owner — You

You are the only person who can onboard, suspend, or remove hotels. All commands below work on **your hotel85 bot**.

### Onboarding a New Hotel

**Step 1 — Create the Telegram bot**
Message `@BotFather` → `/newbot` → copy the token.

**Step 2 — Get the hotel owner's Telegram ID**
Ask them to message `@userinfobot` and send you the number.

**Step 3 — Register the hotel**
```bash
DATABASE_URL="postgresql://..." python3 scripts/add_hotel.py \
  --schema kings_inn \
  --token 7123456789:AAGxx... \
  --admin-id 98765432
```

**Step 4 — Restart the Railway service**
Railway → your service → Redeploy. The new hotel's bot starts automatically.

### App Owner Bot Commands

| Command | What it does |
|---|---|
| `/hotels` | List all hotels with name, owner, and status |
| `/suspend kings_inn` | Suspend a hotel — data fully preserved, bot stops on restart |
| `/unsuspend kings_inn` | Reinstate a suspended hotel |
| `/export` | Download your own hotel's data as Excel |

> After `/suspend` or `/unsuspend`, restart the Railway service to apply the change.

### Managing Hotels via Scripts (local)

```bash
# List all hotels
DATABASE_URL="..." python3 scripts/manage_hotel.py list

# Suspend
DATABASE_URL="..." python3 scripts/manage_hotel.py suspend --schema kings_inn

# Unsuspend
DATABASE_URL="..." python3 scripts/manage_hotel.py unsuspend --schema kings_inn

# Export any hotel's full data to Excel
DATABASE_URL="..." python3 scripts/export_hotel.py --schema kings_inn
```

### Suspending vs Deleting

| Action | Data | Bot |
|---|---|---|
| Suspend | Fully preserved | Stops loading on restart |
| Delete schema | Permanently gone | Stops immediately |

To delete permanently: `DROP SCHEMA kings_inn CASCADE` in the DB. Irreversible.

---

## 2. Hotel Owner — Admin

The person whose Telegram ID was passed as `--admin-id` during registration.

### Getting Started
1. App owner shares bot link (e.g. `t.me/KingsInnBot`)
2. Hotel owner sends `/start` → welcomed as admin
3. Hotel owner runs `/setup` → sets hotel name + timezone

### Managing Staff

```
/addstaff 8078109914 Aisha      → grant staff access
/removestaff 8078109914         → revoke access
```

Staff get the bot link, send `/start`, and see:
```
🔒 Hi Aisha! Your ID is 8078109914.
Ask an admin to run: /addstaff 8078109914 Aisha
```
They send their ID to the hotel owner who then runs `/addstaff`.

### Admin Commands

**Recording**
```
/sell_drink heineken 6 500              → bar sale
/sell_drink heineken 6 500 2026-05-01   → backdated sale
/room standard 2 15000 3                → room booking
/expense bar salary 50000               → record expense
/add_debtor bar john 5000 tab           → log debtor
/pay_debtor bar john 2500               → record payment
/restock heineken 24 300                → add stock to store
/transfer heineken 12                   → move store → bar
/delete sale 45                         → remove wrong entry
```

**Reports**
```
/report                     → full P&L (current month)
/report today               → today only
/report 2026-04             → specific month
/report all                 → all time
/sales_report               → drink-level breakdown with cost & profit
/expense_report             → expenses by category
/staff_report               → sales per staff member
/allocation                 → revenue allocation (what to transfer to savings)
/summary                    → today's key numbers
/stock                      → inventory (store + bar columns)
/debtors                    → outstanding debtors
/history                    → all entries for a date
/activity                   → full activity log
```

**Configuration**
```
/setprice heineken 600          → set drink selling price
/setroomtype standard 15000     → set room type price
/setallocation buffer 10        → adjust savings percentage
/setthreshold heineken 5        → low-stock alert level
/dailyreport on|off             → schedule daily report
```

**Export**
```
/export     → sends full hotel data as Excel file to this chat
```
Excel contains: Summary, Sales, Rooms, Expenses, Debtors, Inventory, Transfers, Users, Settings.

---

## 3. Staff

Added by the hotel owner via `/addstaff`. Cannot delete, configure, or see financial settings.

### Staff Commands

```
/sell (or 🍺 Sell button)    → guided drink sale
/book (or 🛏 Book Room)      → guided room booking
/stock                        → check inventory levels
/debtors                      → view outstanding debtors
/report                       → view financial report
/summary                      → today's overview
/history                      → view entries for a date
/undo                         → cancel last entry (2 min window)
/prices                       → view drink + room prices
```

---

## 4. Common Scenarios

| Scenario | What to do |
|---|---|
| Staff loses phone | Gets new phone, logs into same Telegram number → instant access. Data is on the server. |
| Staff gets new SIM/number | `/removestaff <old_id>` then `/addstaff <new_id> username` |
| Hotel owner loses phone | Same as staff — log into Telegram on new phone, same ID, full access |
| Hotel owner gets new number | Run `add_hotel.py --admin-id <new_id>` again and restart |
| Random person finds bot link | Sees locked screen. Every command returns "Access denied" |
| Hotel stops paying | `/suspend kings_inn` → restart Railway → bot silent, data safe |
| Hotel pays again | `/unsuspend kings_inn` → restart Railway → bot restored with all data |
| Hotel wants their data | Admin types `/export` → Excel file sent to chat |

---

## 5. Data Isolation

Each hotel's tables live in their own PostgreSQL schema (e.g. `hotel85.sales`, `kings_inn.sales`). The `search_path` is set per database connection so queries from one hotel can never read another hotel's data. This is enforced at the database level, not just in code.

---

## 6. Deployment

- **Platform**: Railway
- **Start command**: `python bot.py` (see `railway.toml`)
- **Database**: PostgreSQL — `DATABASE_URL` set automatically by Railway
- **Required env vars**:

| Variable | Description |
|---|---|
| `BOT_TOKEN` | Your hotel85 bot token from @BotFather |
| `HOTEL_SCHEMA` | `hotel85` |
| `DATABASE_URL` | Set automatically by Railway |
| `ADMIN_IDS` | Your Telegram user ID (comma-separated) |
| `REPORT_CHAT_ID` | Optional — chat ID for scheduled daily reports |

---

## 7. Scripts Reference

| Script | Purpose |
|---|---|
| `scripts/add_hotel.py` | Register a new hotel |
| `scripts/manage_hotel.py` | List, suspend, unsuspend hotels |
| `scripts/export_hotel.py` | Export a hotel's full data to Excel |
| `scripts/migrate_to_schema.py` | One-time migration: move public tables → hotel schema |
| `scripts/backup.py` | Per-schema pg_dump backup |
