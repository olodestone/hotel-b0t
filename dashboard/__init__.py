"""
Read-only web dashboard for the hotel bot.

A SEPARATE FastAPI service (own process) that reads the same PostgreSQL database
as the Telegram bot and renders the numbers for viewing in a browser. It does
NOT write anything and does NOT replace the bot — Telegram remains the source of
truth. All financial figures come from the shared ``metrics.py`` calc core, so
the dashboard can never disagree with the bot's reports.
"""
