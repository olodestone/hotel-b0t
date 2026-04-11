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

# ── Access control ───────────────────────────────────────────────────
_raw_admins = os.getenv("ADMIN_IDS", "")
ADMIN_IDS: list[int] = [int(x.strip()) for x in _raw_admins.split(",") if x.strip().isdigit()]

# Chat ID that receives daily automated reports (set to an admin chat or group)
_rcid = os.getenv("REPORT_CHAT_ID", "")
REPORT_CHAT_ID: int | None = int(_rcid) if _rcid.lstrip("-").isdigit() else None

# ── Scheduling ───────────────────────────────────────────────────────
DAILY_REPORT_TIME: str = os.getenv("DAILY_REPORT_TIME", "23:00")   # HH:MM 24-hour
TIMEZONE: str = os.getenv("TIMEZONE", "Africa/Lagos")

# ── Inventory ────────────────────────────────────────────────────────
LOW_STOCK_DEFAULT: int = int(os.getenv("LOW_STOCK_DEFAULT", "5"))

# ── Allocation defaults (overridden by DB settings via /setallocation) ─
# Start at 25% — build the habit before pushing to 35%
ALLOC_TAX_DEFAULT: int = 15       # % of gross revenue → tax reserve
ALLOC_BUFFER_DEFAULT: int = 10    # % of gross revenue → emergency buffer
ALLOC_RESTOCK_DEFAULT: int = 0    # % of gross revenue → restock budget (funded from working capital by default)

# Profit distribution defaults (% of leftover profit after expenses + set-asides)
ALLOC_DRAW_DEFAULT: int = 50      # % of profit → owner's draw
ALLOC_REINVEST_DEFAULT: int = 30  # % of profit → reinvestment
ALLOC_FLOAT_DEFAULT: int = 20     # % of profit → cash float / reserve
