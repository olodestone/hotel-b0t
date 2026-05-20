"""
Configuration — loaded from environment variables.
Copy .env.example to .env for local dev, or set vars in Railway/Heroku dashboard.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Bot ──────────────────────────────────────────────────────────────
BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
HOTEL_NAME: str = os.getenv("HOTEL_NAME", "Hotel 85")

# ── Database ─────────────────────────────────────────────────────────
# On Heroku: heroku addons:create heroku-postgresql
# On Railway: add a PostgreSQL plugin — DATABASE_URL is set automatically
DATABASE_URL: str = os.getenv("DATABASE_URL", "")

# Unique slug identifying this hotel's PostgreSQL schema (e.g. "hotel85", "kings_inn").
# Every hotel deployment must set this. The schema is auto-created on first startup.
HOTEL_SCHEMA: str = os.getenv("HOTEL_SCHEMA", "")

# ── Access control ───────────────────────────────────────────────────
_raw_admins = os.getenv("ADMIN_IDS", "")
ADMIN_IDS: list[int] = [int(x.strip()) for x in _raw_admins.split(",") if x.strip().isdigit()]

# Single app owner — the only person who can /addhotel, /suspend, /unsuspend, /hotels.
# Set OWNER_ID in Railway env vars. Separate from ADMIN_IDS (hotel85 hotel admins).
_raw_owner = os.getenv("OWNER_ID", "")
OWNER_ID: int | None = int(_raw_owner) if _raw_owner.isdigit() else None

# Chat ID that receives daily automated reports (set to an admin chat or group)
_rcid = os.getenv("REPORT_CHAT_ID", "")
REPORT_CHAT_ID: int | None = int(_rcid) if _rcid.lstrip("-").isdigit() else None

# ── Scheduling ───────────────────────────────────────────────────────
DAILY_REPORT_TIME: str = os.getenv("DAILY_REPORT_TIME", "23:00")   # HH:MM 24-hour
TIMEZONE: str = os.getenv("TIMEZONE", "Africa/Lagos")

# ── Inventory ────────────────────────────────────────────────────────
LOW_STOCK_DEFAULT: int = int(os.getenv("LOW_STOCK_DEFAULT", "5"))

# ── Allocation defaults (overridden by DB settings via /setallocation) ─
ALLOC_BUFFER_DEFAULT: int = 10    # % of gross revenue → emergency buffer
ALLOC_RESTOCK_DEFAULT: int = 0    # % of gross revenue → restock budget (funded from working capital by default)

# Profit distribution defaults (% of leftover profit after expenses + set-asides)
ALLOC_DRAW_DEFAULT: int = 50      # % of profit → owner's draw
ALLOC_REINVEST_DEFAULT: int = 30  # % of profit → reinvestment
ALLOC_FLOAT_DEFAULT: int = 20     # % of profit → cash float / reserve

# Personal income tax estimate shown on owner's draw (informational only)
# Nigerian PIT: only applies to individuals, not businesses. Progressive bracket.
PIT_LOW_RATE: int = 15   # lower-end estimate %
PIT_HIGH_RATE: int = 24  # upper-end estimate %
