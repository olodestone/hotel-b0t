"""
Dashboard configuration (separate from the bot's config.py).

Reads its own env vars and reuses the bot's identity/env where it makes sense.
"""
from __future__ import annotations

import logging
import os
import secrets

# Reuse the bot's existing config so identity/tenancy stay consistent.
from config import (  # noqa: F401  (re-exported for the app)
    BOT_TOKEN,
    HOTEL_SCHEMA,
    HOTEL_NAME,
    OWNER_ID,
    ADMIN_IDS,
)

log = logging.getLogger("dashboard")

# ── Session ───────────────────────────────────────────────────────────
# Secret used to sign the session cookie. If unset we generate an ephemeral
# one — safe (never a shared hard-coded default) but sessions won't survive a
# restart, so SET DASHBOARD_SECRET in production.
SESSION_SECRET: str = os.getenv("DASHBOARD_SECRET") or ""
if not SESSION_SECRET:
    SESSION_SECRET = secrets.token_urlsafe(48)
    log.warning("DASHBOARD_SECRET not set — using an ephemeral secret (sessions reset on restart).")

SESSION_COOKIE = "hotel_dash"
SESSION_SALT = "hotel-dash-session.v1"
SESSION_MAX_AGE = int(os.getenv("DASHBOARD_SESSION_MAX_AGE", str(7 * 24 * 3600)))  # 7 days
# Set DASHBOARD_INSECURE_COOKIE=1 only for local http dev; prod is https → secure cookie.
COOKIE_SECURE = os.getenv("DASHBOARD_INSECURE_COOKIE", "") != "1"

# ── Telegram Login Widget ─────────────────────────────────────────────
# Bot username (without @) backing the Login Widget, and the token used to
# verify the login HMAC. Defaults to the env bot (single-tenant deploy).
LOGIN_BOT_USERNAME: str = os.getenv("DASHBOARD_LOGIN_BOT_USERNAME", "")
LOGIN_BOT_TOKEN: str = os.getenv("DASHBOARD_LOGIN_BOT_TOKEN", "") or BOT_TOKEN

# ── Dev login (LOCAL ONLY) ────────────────────────────────────────────
# When set to a Telegram user id, exposes /dev-login that logs in as that id
# WITHOUT Telegram verification. Never set this in production.
DEV_LOGIN_ID: int | None = (
    int(os.getenv("DASHBOARD_DEV_LOGIN_ID"))
    if (os.getenv("DASHBOARD_DEV_LOGIN_ID") or "").lstrip("-").isdigit()
    else None
)
