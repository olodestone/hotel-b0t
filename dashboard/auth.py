"""
Authentication & multi-tenant access resolution.

Identity comes from the Telegram Login Widget (verified by HMAC against the
bot token — same trust model as the bot). Authorization reuses the bot's rules:
a user may view a hotel if they are the app OWNER_ID, that hotel's owner, in its
admin_ids, or present in its per-schema ``users`` table.
"""
from __future__ import annotations

import hashlib
import hmac
import time

from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from sqlalchemy import text

import database as db
from . import settings

_serializer = URLSafeTimedSerializer(settings.SESSION_SECRET, salt=settings.SESSION_SALT)

# Telegram login is rejected if older than this (replay protection).
_AUTH_MAX_AGE_SECONDS = 24 * 3600


# ── Telegram Login Widget verification ────────────────────────────────

def verify_telegram_login(data: dict, bot_token: str) -> bool:
    """Validate the Login Widget payload per Telegram's spec.

    The widget sends fields plus a ``hash``. We recompute the HMAC-SHA256 over
    the sorted ``key=value`` lines using SHA256(bot_token) as the key.
    """
    data = dict(data)
    received_hash = data.pop("hash", None)
    if not received_hash or not bot_token:
        return False

    check_string = "\n".join(f"{k}={data[k]}" for k in sorted(data))
    secret_key = hashlib.sha256(bot_token.encode()).digest()
    expected = hmac.new(secret_key, check_string.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, received_hash):
        return False

    try:
        auth_date = int(data.get("auth_date", "0"))
    except (TypeError, ValueError):
        return False
    return (time.time() - auth_date) < _AUTH_MAX_AGE_SECONDS


# ── Session cookie (signed, stateless) ────────────────────────────────

def serialize_session(payload: dict) -> str:
    return _serializer.dumps(payload)


def read_session_token(token: str | None) -> dict | None:
    if not token:
        return None
    try:
        return _serializer.loads(token, max_age=settings.SESSION_MAX_AGE)
    except (BadSignature, SignatureExpired):
        return None


# ── Access resolution (which hotels can this Telegram user view?) ──────

def _hotels_meta() -> list[dict]:
    """Active hotels from the public.hotels registry (schema, name, owner, admin_ids)."""
    with db._base_engine().connect() as conn:
        rows = conn.execute(text(
            "SELECT schema_name, COALESCE(name, schema_name) AS name, "
            "       owner_telegram_id, COALESCE(admin_ids, '') AS admin_ids "
            "FROM public.hotels WHERE is_active = TRUE ORDER BY id"
        )).fetchall()
    return [
        {"schema": r[0], "name": r[1], "owner": r[2], "admin_ids": r[3]}
        for r in rows
    ]


def _parse_ids(raw: str) -> set[int]:
    return {int(x.strip()) for x in (raw or "").split(",") if x.strip().lstrip("-").isdigit()}


def resolve_access(telegram_id: int) -> list[dict]:
    """Return [{schema, name, role}] for every hotel this user may view.

    Mirrors the bot's _is_admin / _is_authorized logic, per hotel schema.
    """
    out: list[dict] = []
    for h in _hotels_meta():
        admin_ids = _parse_ids(h["admin_ids"]) | set(settings.ADMIN_IDS)
        role: str | None = None
        if telegram_id == settings.OWNER_ID or telegram_id == h["owner"] or telegram_id in admin_ids:
            role = "admin"
        else:
            # Fall back to the hotel's own users table — scope the engine to it.
            tok = db._hotel_schema_var.set(h["schema"])
            try:
                user = db.get_user(telegram_id)
            finally:
                db._hotel_schema_var.reset(tok)
            if user:
                role = user.get("role")
        if role:
            out.append({"schema": h["schema"], "name": h["name"], "role": role})
    return out
