"""
database.py — PostgreSQL persistence layer via SQLAlchemy.

Tables are auto-created on init_db(). All reads return list[dict],
all writes use parameterised queries. Same public API as before —
no changes needed in inventory.py, logic.py, or reports.py.

Multi-tenancy: each hotel deployment sets HOTEL_SCHEMA to a unique slug
(e.g. "hotel85"). get_engine() injects search_path=<schema>,public so all
unqualified table names resolve to that hotel's schema automatically.
The master registry lives in public.hotels (always schema-qualified).
"""
from __future__ import annotations

import contextvars
import hashlib
import os
from datetime import datetime
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

import clock
from config import TIMEZONE

DATABASE_URL: str = os.getenv("DATABASE_URL", "")

# Per-update context variable — set by each bot's schema-setter handler so all
# DB calls in that update automatically hit the right hotel schema.
_hotel_schema_var: contextvars.ContextVar[str] = contextvars.ContextVar("hotel_schema", default="")


def _canonical_url() -> str:
    url = DATABASE_URL
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    if not url:
        raise RuntimeError("DATABASE_URL environment variable is not set.")
    return url


_engines: dict[str, Engine] = {}


def get_engine() -> Engine:
    """One pooled Engine per hotel schema, reused across calls.

    Previously this created (and immediately discarded) a brand-new Engine —
    and therefore a brand-new connection pool — on every call, so no DB
    operation ever actually reused a pooled connection. Cached per schema
    since one process can serve several hotels' schemas (dashboard requests
    switch `_hotel_schema_var` per request; the bot runs one Application per
    hotel schema).
    """
    from config import HOTEL_SCHEMA
    schema = _hotel_schema_var.get() or HOTEL_SCHEMA or "public"
    engine = _engines.get(schema)
    if engine is None:
        engine = create_engine(
            _canonical_url(),
            connect_args={"options": f"-c search_path={schema},public"},
            pool_pre_ping=True,
        )
        _engines[schema] = engine
    return engine


def _base_engine() -> Engine:
    """Engine with no custom search_path — used for public-schema registry queries."""
    engine = _engines.get("")
    if engine is None:
        engine = create_engine(_canonical_url(), pool_pre_ping=True)
        _engines[""] = engine
    return engine


# ── Tenancy + clock ──────────────────────────────────────────────────

# schema → timezone, resolved once per process. public.hotels is the source of
# truth; a hotel that has never run /setup falls back to the configured default.
_tz_cache: dict[str, str] = {}


def _hotel_timezone(schema: str) -> str:
    """The hotel's timezone name, cached. Never raises — a broken row must not
    take that hotel's bot down, so any failure falls back to config.TIMEZONE."""
    if schema in _tz_cache:
        return _tz_cache[schema]
    tz = ""
    try:
        with _base_engine().connect() as conn:
            row = conn.execute(
                text("SELECT timezone FROM public.hotels WHERE schema_name = :s"),
                {"s": schema},
            ).first()
        tz = (row[0] or "") if row else ""
    except Exception:
        tz = ""
    _tz_cache[schema] = tz or TIMEZONE
    return _tz_cache[schema]


def set_tenant(schema: str) -> tuple[Any, Any]:
    """Enter a hotel's context: its schema *and* its clock.

    Every query and every "now" in the work that follows belongs to this hotel.
    Setting the two together is the point — a handler that scoped the engine but
    left the clock on the server's timezone is exactly how entries ended up
    filed under the wrong day.

    Returns a token for ``reset_tenant()``; callers that own the whole context
    (a bot update, a scheduled job) can ignore it.
    """
    schema_tok = _hotel_schema_var.set(schema)
    tz_tok = clock.enter(_hotel_timezone(schema))
    return schema_tok, tz_tok


def reset_tenant(token: tuple[Any, Any]) -> None:
    """Restore the tenant context a ``set_tenant()`` replaced."""
    schema_tok, tz_tok = token
    _hotel_schema_var.reset(schema_tok)
    clock.reset(tz_tok)


def set_hotel_timezone(schema: str, tz_name: str) -> None:
    """Persist a hotel's timezone and refresh the cache + current context."""
    with _base_engine().connect() as conn:
        conn.execute(
            text("UPDATE public.hotels SET timezone = :tz WHERE schema_name = :s"),
            {"tz": tz_name, "s": schema},
        )
        conn.commit()
    _tz_cache[schema] = tz_name
    clock.use(tz_name)


def now_str() -> str:
    """Timestamp for a new row — in the hotel's local time, not the server's."""
    return clock.now().strftime("%Y-%m-%d %H:%M:%S")


def _ts(custom: str | None = None) -> str:
    """Return a timestamp string: custom date (YYYY-MM-DD) → 'YYYY-MM-DD 00:00:00', else now."""
    if custom:
        return custom + " 00:00:00"
    return now_str()


# ── Init ─────────────────────────────────────────────────────────────

def init_db(schema: str | None = None, token: str | None = None) -> None:
    """Create all tables if they don't exist.

    schema/token default to the HOTEL_SCHEMA/BOT_TOKEN env vars when not passed.
    In multi-hotel mode the multi-bot runner passes each hotel's values explicitly.
    """
    from config import BOT_TOKEN, HOTEL_SCHEMA
    s = schema or HOTEL_SCHEMA
    t = token or BOT_TOKEN
    token_hash = hashlib.sha256(t.encode()).hexdigest() if t else ""

    # Step 1: create master registry + hotel schema using the base engine so
    # the search_path doesn't interfere with public.hotels creation.
    base = _base_engine()
    with base.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public.hotels (
                id                 SERIAL PRIMARY KEY,
                name               TEXT,
                schema_name        TEXT UNIQUE NOT NULL,
                owner_telegram_id  BIGINT,
                timezone           TEXT DEFAULT 'Africa/Lagos',
                created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active          BOOLEAN DEFAULT TRUE,
                bot_token_hash     TEXT,
                bot_token          TEXT,
                admin_ids          TEXT DEFAULT ''
            )
        """))
        conn.execute(text("ALTER TABLE public.hotels ADD COLUMN IF NOT EXISTS bot_token TEXT"))
        conn.execute(text("ALTER TABLE public.hotels ADD COLUMN IF NOT EXISTS admin_ids TEXT DEFAULT ''"))
        conn.execute(text("ALTER TABLE public.hotels ADD COLUMN IF NOT EXISTS subscription_expires_at DATE"))
        conn.execute(text("ALTER TABLE public.hotels ADD COLUMN IF NOT EXISTS expiry_notified_days TEXT DEFAULT ''"  ))
        if s:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{s}"'))
            conn.execute(text("""
                INSERT INTO public.hotels (schema_name, bot_token_hash)
                VALUES (:schema, :hash)
                ON CONFLICT (schema_name) DO NOTHING
            """), {"schema": s, "hash": token_hash})
        conn.commit()

    # Step 2: create hotel tables under the hotel schema. Build the engine from
    # `s` directly rather than get_engine() — get_engine() resolves its schema
    # from the per-request contextvar (or the global HOTEL_SCHEMA env var as a
    # fallback), neither of which reflects the schema this init_db() call was
    # asked to set up, so it would silently target the wrong hotel's schema.
    engine = create_engine(
        _canonical_url(),
        connect_args={"options": f"-c search_path={s},public"},
    )
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS inventory (
                drink_name          TEXT PRIMARY KEY,
                current_stock       INTEGER NOT NULL DEFAULT 0,
                store_stock         INTEGER NOT NULL DEFAULT 0,
                total_purchased     INTEGER NOT NULL DEFAULT 0,
                total_sold          INTEGER NOT NULL DEFAULT 0,
                cost_price          FLOAT   NOT NULL DEFAULT 0,
                low_stock_threshold INTEGER NOT NULL DEFAULT 5
            )
        """))
        # Migrations: add columns to existing inventory rows
        conn.execute(text(
            "ALTER TABLE inventory ADD COLUMN IF NOT EXISTS store_stock INTEGER NOT NULL DEFAULT 0"
        ))
        conn.execute(text(
            "ALTER TABLE inventory ADD COLUMN IF NOT EXISTS selling_price FLOAT NOT NULL DEFAULT 0"
        ))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS sales (
                id              SERIAL PRIMARY KEY,
                timestamp       TEXT,
                drink_name      TEXT,
                quantity        INTEGER,
                selling_price   FLOAT,
                total_revenue   FLOAT,
                recorded_by     TEXT DEFAULT ''
            )
        """))
        # Back-fill selling_price from the most recent sale per drink (one-time, safe to re-run).
        # Must run after `sales` is created above - it reads from that table.
        conn.execute(text("""
            UPDATE inventory
            SET selling_price = s.selling_price
            FROM (
                SELECT DISTINCT ON (lower(drink_name)) lower(drink_name) AS drink_key, selling_price
                FROM sales
                ORDER BY lower(drink_name), timestamp DESC
            ) s
            WHERE lower(inventory.drink_name) = s.drink_key
            AND inventory.selling_price = 0
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS rooms (
                id              SERIAL PRIMARY KEY,
                timestamp       TEXT,
                room_type       TEXT,
                quantity        INTEGER,
                price_per_night FLOAT,
                nights          INTEGER,
                total_revenue   FLOAT,
                recorded_by     TEXT DEFAULT ''
            )
        """))
        conn.execute(text("ALTER TABLE rooms ADD COLUMN IF NOT EXISTS recorded_by TEXT DEFAULT ''"))
        # Hours one stay-unit occupies the room. 0/NULL means "ask the room
        # type", so every historical row is reinterpreted correctly the moment
        # a short-stay type is configured — nothing needs backfilling.
        conn.execute(text("ALTER TABLE rooms ADD COLUMN IF NOT EXISTS duration_hours FLOAT DEFAULT 0"))
        # Which part of the day an hourly let actually happened in. Asked at
        # entry, never derived from the timestamp: bookings are written in a
        # paper book and keyed in the next morning, so the timestamp is when
        # the typing happened, not when the room was used.
        conn.execute(text("ALTER TABLE rooms ADD COLUMN IF NOT EXISTS daypart TEXT DEFAULT ''"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS expenses (
                id          SERIAL PRIMARY KEY,
                timestamp   TEXT,
                account     TEXT,
                category    TEXT,
                amount      FLOAT,
                description TEXT
            )
        """))
        # The second classification axis. Deliberately NOT a category value: a
        # category holds one string, so "Maintenance that happens to be capital"
        # could only be recorded by giving up the category. Defaulting to
        # 'operating' means every pre-existing row keeps the behaviour it had.
        conn.execute(text("ALTER TABLE expenses ADD COLUMN IF NOT EXISTS expense_class TEXT DEFAULT 'operating'"))
        # That DEFAULT backfilled every existing row, which stamped stock
        # purchases as 'operating' and put them into the P&L — ₦345,376 of
        # August's drinks were charged as a bar expense on the same screen
        # that called them "cash to stock, not a profit cost". metrics now
        # resolves the category first so it cannot recur, and this corrects
        # the stored values so exports and the dashboard read the same.
        # Idempotent: safe to re-run on every start.
        conn.execute(text("""
            UPDATE expenses SET expense_class = 'inventory'
            WHERE lower(category) IN ('restock', 'supplier')
              AND COALESCE(expense_class, '') <> 'inventory'
        """))
        # Set when the person entering it wasn't sure, or the category is Misc.
        # Over-expensing understates profit, which is the safe way to be wrong —
        # so an unsure row still lands in the P&L, it just gets listed at month end.
        conn.execute(text("ALTER TABLE expenses ADD COLUMN IF NOT EXISTS needs_review BOOLEAN DEFAULT FALSE"))
        # Which periodic obligation a payment settles. Null for everything else,
        # and for a periodic payment nobody attributed — that still drains the
        # reserve, it just cannot be charged to a particular bill.
        conn.execute(text("ALTER TABLE expenses ADD COLUMN IF NOT EXISTS obligation_id INTEGER"))
        # The register of bills that recur every few months. Without it there is
        # nothing to accrue *against*: you cannot charge a monthly share of a
        # cost the books have never been told to expect.
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS periodic_obligations (
                id              SERIAL PRIMARY KEY,
                name            TEXT NOT NULL,
                account         TEXT NOT NULL DEFAULT 'rooms',
                category        TEXT NOT NULL DEFAULT 'maintenance',
                expected_amount FLOAT NOT NULL DEFAULT 0,
                months          INTEGER NOT NULL DEFAULT 12,
                start_date      TEXT,
                active          BOOLEAN NOT NULL DEFAULT TRUE,
                retired_on      TEXT DEFAULT '',
                created_at      TEXT DEFAULT '',
                recorded_by     TEXT DEFAULT ''
            )
        """))
        conn.execute(text("ALTER TABLE periodic_obligations ADD COLUMN IF NOT EXISTS retired_on TEXT DEFAULT ''"))
        # Migrations: add columns to existing databases that predate them
        conn.execute(text("ALTER TABLE sales    ADD COLUMN IF NOT EXISTS id SERIAL"))
        conn.execute(text("ALTER TABLE rooms    ADD COLUMN IF NOT EXISTS id SERIAL"))
        conn.execute(text("ALTER TABLE expenses ADD COLUMN IF NOT EXISTS id SERIAL"))
        conn.execute(text("ALTER TABLE sales    ADD COLUMN IF NOT EXISTS recorded_by TEXT DEFAULT ''"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS debtors (
                id          SERIAL PRIMARY KEY,
                timestamp   TEXT,
                account     TEXT,
                name        TEXT,
                amount      FLOAT,
                description TEXT,
                status      TEXT DEFAULT 'outstanding',
                paid_at     TEXT DEFAULT ''
            )
        """))
        # Migrations: track who recorded expenses/debtors and who marked debts paid
        conn.execute(text("ALTER TABLE expenses ADD COLUMN IF NOT EXISTS recorded_by TEXT DEFAULT ''"))
        conn.execute(text("ALTER TABLE debtors  ADD COLUMN IF NOT EXISTS recorded_by  TEXT DEFAULT ''"))
        conn.execute(text("ALTER TABLE debtors  ADD COLUMN IF NOT EXISTS paid_by      TEXT DEFAULT ''"))
        conn.execute(text("ALTER TABLE debtors  ADD COLUMN IF NOT EXISTS amount_paid  FLOAT DEFAULT 0"))
        conn.execute(text("ALTER TABLE debtors  ADD COLUMN IF NOT EXISTS staff_name   TEXT DEFAULT ''"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS debtor_payments (
                id          SERIAL PRIMARY KEY,
                debtor_id   INTEGER,
                timestamp   TEXT,
                amount      FLOAT,
                recorded_by TEXT DEFAULT ''
            )
        """))
        # Migrations: soft-delete support (void instead of hard delete)
        conn.execute(text("ALTER TABLE sales    ADD COLUMN IF NOT EXISTS deleted_by TEXT DEFAULT ''"))
        conn.execute(text("ALTER TABLE sales    ADD COLUMN IF NOT EXISTS deleted_at TEXT DEFAULT ''"))
        # The cost this drink actually carried when it was sold. Without it,
        # COGS is computed at *today's* cost price, so restocking dearer
        # silently rewrites the profit of every month already closed. 0 means
        # a row written before this existed — those still fall back to the
        # current price, which is the best that can be reconstructed.
        conn.execute(text("ALTER TABLE sales    ADD COLUMN IF NOT EXISTS cost_price FLOAT DEFAULT 0"))
        conn.execute(text("ALTER TABLE rooms    ADD COLUMN IF NOT EXISTS deleted_by TEXT DEFAULT ''"))
        conn.execute(text("ALTER TABLE rooms    ADD COLUMN IF NOT EXISTS deleted_at TEXT DEFAULT ''"))
        conn.execute(text("ALTER TABLE expenses ADD COLUMN IF NOT EXISTS deleted_by TEXT DEFAULT ''"))
        conn.execute(text("ALTER TABLE expenses ADD COLUMN IF NOT EXISTS deleted_at TEXT DEFAULT ''"))
        # Migration: when the row was *keyed in*, as opposed to the business date
        # it was keyed in for. A backdated entry's `timestamp` is the night it
        # covers; only `created_at` can say whether the undo window is still open.
        conn.execute(text("ALTER TABLE sales ADD COLUMN IF NOT EXISTS created_at TEXT DEFAULT ''"))
        conn.execute(text("ALTER TABLE rooms ADD COLUMN IF NOT EXISTS created_at TEXT DEFAULT ''"))
        # Transfers log table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS transfers (
                id          SERIAL PRIMARY KEY,
                timestamp   TEXT,
                drink_name  TEXT,
                quantity    INTEGER,
                recorded_by TEXT DEFAULT ''
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS users (
                user_id     BIGINT PRIMARY KEY,
                username    TEXT,
                role        TEXT,
                added_at    TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS settings (
                key     TEXT PRIMARY KEY,
                value   TEXT NOT NULL
            )
        """))
        # Owner draws (equity withdrawals) — money the owner takes out of the
        # business. Deliberately NOT in the `expenses` table: a draw reduces
        # cash and owner's equity but is not a P&L cost, so it must never enter
        # the profit calculation.
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS owner_draws (
                id          SERIAL PRIMARY KEY,
                timestamp   TEXT,
                amount      FLOAT,
                account     TEXT DEFAULT '',
                description TEXT DEFAULT '',
                recorded_by TEXT DEFAULT '',
                deleted_by  TEXT DEFAULT '',
                deleted_at  TEXT DEFAULT ''
            )
        """))
        # Physical stocktakes. The books can only ever believe their own
        # arithmetic, so a counted figure is the one independent observation
        # that makes breakage/theft visible. `expected` is what the system
        # thought was in the bar at count time; `counted` is what was there.
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS stock_counts (
                id          SERIAL PRIMARY KEY,
                timestamp   TEXT,
                drink_name  TEXT,
                expected    INTEGER NOT NULL DEFAULT 0,
                counted     INTEGER NOT NULL DEFAULT 0,
                variance    INTEGER NOT NULL DEFAULT 0,
                cost_price  FLOAT   NOT NULL DEFAULT 0,
                note        TEXT DEFAULT '',
                recorded_by TEXT DEFAULT ''
            )
        """))
        # Bar and store are counted separately: a bottle moved to the bar and a
        # bottle sold look identical in a combined figure, so a single total
        # cannot tell a transfer from a loss. Legacy rows are bar counts.
        conn.execute(text("ALTER TABLE stock_counts ADD COLUMN IF NOT EXISTS location TEXT DEFAULT 'bar'"))
        # Which month this count verifies. A month with no count is reported
        # UNVERIFIED rather than silently treated as clean.
        conn.execute(text("ALTER TABLE stock_counts ADD COLUMN IF NOT EXISTS period TEXT DEFAULT ''"))
        # Room audits — was every room-night actually logged, at the rate charged?
        # The days are chosen by the bot, never by the operator: a person picks
        # days they remember clearly, and those are the days most likely correct.
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS room_audits (
                id             SERIAL PRIMARY KEY,
                timestamp      TEXT,
                audit_date     TEXT,
                period         TEXT DEFAULT '',
                rooms_total    INTEGER NOT NULL DEFAULT 0,
                nights_logged  INTEGER NOT NULL DEFAULT 0,
                nights_actual  INTEGER NOT NULL DEFAULT 0,
                rate_variance  FLOAT   NOT NULL DEFAULT 0,
                variance_count INTEGER NOT NULL DEFAULT 0,
                note           TEXT DEFAULT '',
                recorded_by    TEXT DEFAULT ''
            )
        """))
        # Turnaways — guests refused because nothing suitable was free. The one
        # thing the books cannot infer: a night that sold out looks identical
        # whether one guest was turned away or twenty, and only the second is
        # evidence that the rate is too low. Nothing here touches money, so it
        # is deliberately outside every P&L path.
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS turnaways (
                id          SERIAL PRIMARY KEY,
                timestamp   TEXT,
                created_at  TEXT DEFAULT '',
                room_type   TEXT DEFAULT '',
                quantity    INTEGER NOT NULL DEFAULT 1,
                reason      TEXT DEFAULT '',
                recorded_by TEXT DEFAULT ''
            )
        """))
        # Supplier credit. A credit purchase puts stock on the shelf without
        # cash leaving the account, so it must NOT create an expense row on
        # delivery — /pay_supplier writes the `supplier` expense (cash out) when
        # the invoice is actually settled. This table is what makes DPO (and so
        # a truthful cash conversion cycle) computable at all.
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS payables (
                id          SERIAL PRIMARY KEY,
                timestamp   TEXT,
                supplier    TEXT,
                drink_name  TEXT DEFAULT '',
                quantity    INTEGER NOT NULL DEFAULT 0,
                amount      FLOAT   NOT NULL DEFAULT 0,
                amount_paid FLOAT   NOT NULL DEFAULT 0,
                due_date    TEXT DEFAULT '',
                status      TEXT DEFAULT 'outstanding',
                paid_at     TEXT DEFAULT '',
                description TEXT DEFAULT '',
                recorded_by TEXT DEFAULT ''
            )
        """))
        # Daily inventory snapshots. `inventory` is overwritten in place, so
        # without this there is no stock history and DIO can only ever be
        # estimated from today's shelf. One row per drink per day.
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS inventory_snapshots (
                snapshot_date TEXT NOT NULL,
                drink_name    TEXT NOT NULL,
                bar_stock     INTEGER NOT NULL DEFAULT 0,
                store_stock   INTEGER NOT NULL DEFAULT 0,
                cost_price    FLOAT   NOT NULL DEFAULT 0,
                stock_value   FLOAT   NOT NULL DEFAULT 0,
                PRIMARY KEY (snapshot_date, drink_name)
            )
        """))
        conn.commit()


# ── Schema validation & hotel registration ───────────────────────────

def validate_hotel_schema(schema: str | None = None, token: str | None = None) -> None:
    """Called once per hotel on startup. Raises RuntimeError if misconfigured."""
    from config import BOT_TOKEN, HOTEL_SCHEMA
    s = schema or HOTEL_SCHEMA
    t = token or BOT_TOKEN
    if not s:
        raise RuntimeError(
            "HOTEL_SCHEMA env var is not set. "
            "Each hotel deployment must set a unique slug (e.g. HOTEL_SCHEMA=hotel85)."
        )
    token_hash = hashlib.sha256(t.encode()).hexdigest() if t else ""
    with _base_engine().connect() as conn:
        row = conn.execute(
            text("SELECT is_active, bot_token_hash FROM public.hotels WHERE schema_name = :s"),
            {"s": s},
        ).fetchone()
    if row is None:
        raise RuntimeError(
            f"Schema '{s}' is not registered in public.hotels. "
            "This should not happen — init_db() must run first."
        )
    if not row[0]:
        raise RuntimeError(
            f"Hotel '{s}' is suspended. Contact the service administrator."
        )
    stored_hash = row[1] or ""
    if stored_hash and stored_hash != token_hash:
        raise RuntimeError(
            f"This bot token is not authorised for schema '{s}'. "
            "Each hotel must use its own dedicated bot token."
        )


def register_hotel_details(name: str, owner_id: int, timezone: str) -> None:
    """Update public.hotels with hotel name, owner, and timezone after /setup."""
    schema = _hotel_schema_var.get()
    if not schema:
        from config import HOTEL_SCHEMA
        schema = HOTEL_SCHEMA
    with _base_engine().connect() as conn:
        conn.execute(text("""
            UPDATE public.hotels
               SET name = :name, owner_telegram_id = :oid, timezone = :tz
             WHERE schema_name = :schema
        """), {"name": name, "oid": owner_id, "tz": timezone, "schema": schema})
        conn.commit()


def set_subscription_expiry(schema: str, expiry_date: str) -> None:
    """Set subscription expiry date (YYYY-MM-DD) for a hotel. Resets notified days."""
    with _base_engine().connect() as conn:
        conn.execute(text("""
            UPDATE public.hotels
               SET subscription_expires_at = :exp, expiry_notified_days = ''
             WHERE schema_name = :s
        """), {"exp": expiry_date, "s": schema})
        conn.commit()


def get_expiring_hotels() -> list[dict]:
    """Return all active hotels that have a subscription expiry date set."""
    with _base_engine().connect() as conn:
        rows = conn.execute(text("""
            SELECT schema_name, name, bot_token, owner_telegram_id, admin_ids,
                   subscription_expires_at, expiry_notified_days
              FROM public.hotels
             WHERE is_active = TRUE
               AND subscription_expires_at IS NOT NULL
             ORDER BY subscription_expires_at
        """)).fetchall()
    return [
        {
            "schema": r[0], "name": r[1] or r[0], "token": r[2],
            "owner_id": r[3],
            "admin_ids": r[4] or "",
            "expires_at": str(r[5]),
            "notified_days": r[6] or "",
        }
        for r in rows
    ]


def mark_expiry_notified(schema: str, days_label: str) -> None:
    """Record that a notification was sent for this days_label (e.g. '7', '3', '1', '0')."""
    with _base_engine().connect() as conn:
        conn.execute(text("""
            UPDATE public.hotels
               SET expiry_notified_days = CASE
                   WHEN expiry_notified_days = '' THEN :label
                   ELSE expiry_notified_days || ',' || :label
               END
             WHERE schema_name = :s
        """), {"label": days_label, "s": schema})
        conn.commit()


def get_all_hotel_configs() -> list[dict]:
    """Return all active hotels that have a bot_token stored — used by the multi-bot runner."""
    with _base_engine().connect() as conn:
        rows = conn.execute(text(
            "SELECT schema_name, bot_token, admin_ids, timezone FROM public.hotels "
            "WHERE is_active = TRUE AND bot_token IS NOT NULL AND bot_token <> '' "
            "ORDER BY id"
        )).fetchall()
    # timezone drives both this hotel's job schedule and its clock — without it
    # every hotel would be scheduled on the process default.
    return [{"schema": r[0], "token": r[1], "admin_ids": r[2] or "", "timezone": r[3] or ""} for r in rows]


def add_hotel_config(schema: str, token: str, admin_ids: str = "") -> None:
    """Register a hotel in public.hotels with its bot token so the multi-bot runner picks it up."""
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    with _base_engine().connect() as conn:
        conn.execute(text("""
            INSERT INTO public.hotels (schema_name, bot_token, bot_token_hash, admin_ids)
            VALUES (:schema, :token, :hash, :admin_ids)
            ON CONFLICT (schema_name) DO UPDATE SET
                bot_token      = :token,
                bot_token_hash = :hash,
                admin_ids      = :admin_ids
        """), {"schema": schema, "token": token, "hash": token_hash, "admin_ids": admin_ids})
        conn.commit()


# ── Generic read (used by inventory.py, reports.py) ───────────────────

def _rows(sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Run a SELECT, return every row as a plain dict."""
    engine = get_engine()
    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(text(sql), params or {}).mappings().all()]


def _row(sql: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """Run a SELECT, return the first row as a dict, or None if no match."""
    engine = get_engine()
    with engine.connect() as conn:
        r = conn.execute(text(sql), params or {}).mappings().first()
        return dict(r) if r is not None else None


def read_all(table: str) -> list[dict[str, Any]]:
    """Return all rows as a list of dicts."""
    return _rows(f"SELECT * FROM {table}")


# ── Drink-sale record ─────────────────────────────────────────────────

def record_sale(drink: str, qty: int, price: float, timestamp: str | None = None,
                recorded_by: str = "", cost_price: float | None = None) -> int:
    """Insert one sale. Returns the new row id so callers can offer a targeted undo.

    The drink's cost at this moment is stamped onto the row. Reading it from
    inventory at report time instead meant a later restock at a higher price
    restated the profit of months that were already closed and acted on.
    """
    name = drink.lower()
    if cost_price is None:
        row = get_drink(name)
        cost_price = float(row.get("cost_price") or 0) if row else 0.0
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("""
            INSERT INTO sales (timestamp, created_at, drink_name, quantity, selling_price, total_revenue, recorded_by, cost_price)
            VALUES (:ts, :created, :drink, :qty, :price, :total, :recorded_by, :cost)
            RETURNING id
        """), {
            "ts": _ts(timestamp), "created": now_str(), "drink": name,
            "qty": qty, "price": price,
            "total": round(qty * price, 2),
            "recorded_by": recorded_by,
            "cost": round(float(cost_price or 0), 2),
        })
        new_id = int(result.scalar_one())
        conn.commit()
        return new_id


# ── Room-booking record ───────────────────────────────────────────────

def record_room(room_type: str, qty: int, price: float, nights: int, timestamp: str | None = None,
                recorded_by: str = "", duration_hours: float = 0, daypart: str = "") -> int:
    """Insert one booking. Returns the new row id so callers can offer a targeted undo.

    ``nights`` is really *stay units*: nights for a nightly room type, lets for
    an hourly one. ``duration_hours`` is how long one unit holds the room —
    pass it only for a negotiated stay; 0 leaves the row deferring to whatever
    the room type is configured for, which is what keeps history correctable.
    """
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("""
            INSERT INTO rooms (timestamp, created_at, room_type, quantity, price_per_night, nights, total_revenue, recorded_by, duration_hours, daypart)
            VALUES (:ts, :created, :rtype, :qty, :price, :nights, :total, :recorded_by, :hours, :daypart)
            RETURNING id
        """), {
            "ts": _ts(timestamp), "created": now_str(), "rtype": room_type.lower(),
            "qty": qty, "price": price, "nights": nights,
            "total": round(qty * price * nights, 2),
            "recorded_by": recorded_by, "hours": max(float(duration_hours or 0), 0),
            "daypart": str(daypart or "").strip().title(),
        })
        new_id = int(result.scalar_one())
        conn.commit()
        return new_id


# ── Expense record ────────────────────────────────────────────────────

def record_expense(account: str, category: str, amount: float, description: str = "",
                   timestamp: str | None = None, recorded_by: str = "",
                   expense_class: str = "operating", needs_review: bool = False,
                   obligation_id: int | None = None) -> None:
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO expenses (timestamp, account, category, amount, description,
                                  recorded_by, expense_class, needs_review, obligation_id)
            VALUES (:ts, :account, :category, :amount, :desc, :recorded_by, :cls, :review, :ob)
        """), {
            "ts": _ts(timestamp), "account": account.lower(),
            "category": category.lower(),
            "amount": round(amount, 2), "desc": description,
            "recorded_by": recorded_by,
            "cls": expense_class.lower(), "review": bool(needs_review),
            "ob": int(obligation_id) if obligation_id else None,
        })
        conn.commit()


def reclassify_expense(expense_id: int, account: str | None = None,
                       category: str | None = None, expense_class: str | None = None,
                       needs_review: bool | None = None) -> bool:
    """Correct an existing expense's classification. Amounts are never touched.

    The month-end check asks the repair-vs-replace question of every large
    entry; without this it could only ever be advice, since the bot's other
    option is delete-and-rekey, which loses the original timestamp and author.
    """
    sets, params = [], {"id": int(expense_id)}
    for col, val in (("account", account), ("category", category),
                     ("expense_class", expense_class)):
        if val is not None:
            sets.append(f"{col} = :{col}")
            params[col] = str(val).strip().lower()
    if needs_review is not None:
        sets.append("needs_review = :needs_review")
        params["needs_review"] = bool(needs_review)
    if not sets:
        return False

    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text(f"UPDATE expenses SET {', '.join(sets)} WHERE id = :id"), params)
        conn.commit()
        return result.rowcount > 0


def get_expense(expense_id: int) -> dict[str, Any] | None:
    rows = _rows("SELECT * FROM expenses WHERE id = :id", {"id": int(expense_id)})
    return rows[0] if rows else None


# ── Owner-draw record ─────────────────────────────────────────────────

def record_draw(amount: float, description: str = "", account: str = "", timestamp: str | None = None, recorded_by: str = "") -> None:
    """Record an owner withdrawal (equity draw). Never an expense — see owner_draws table comment."""
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO owner_draws (timestamp, amount, account, description, recorded_by)
            VALUES (:ts, :amount, :account, :desc, :recorded_by)
        """), {
            "ts": _ts(timestamp), "amount": round(amount, 2),
            "account": account.lower(), "desc": description,
            "recorded_by": recorded_by,
        })
        conn.commit()


# ── Debtor records ────────────────────────────────────────────────────

def record_debtor(account: str, name: str, amount: float, description: str = "", timestamp: str | None = None, recorded_by: str = "", staff_name: str = "") -> None:
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO debtors (timestamp, account, name, amount, description, status, paid_at, recorded_by, staff_name)
            VALUES (:ts, :account, :name, :amount, :desc, 'outstanding', '', :recorded_by, :staff_name)
        """), {
            "ts": _ts(timestamp), "account": account.lower(),
            "name": name.strip(),
            "amount": round(amount, 2), "desc": description,
            "recorded_by": recorded_by, "staff_name": staff_name.strip(),
        })
        conn.commit()


def get_debtors(account: str | None = None, month: str | None = None) -> list[dict[str, Any]]:
    """Return all outstanding debtor rows, optionally filtered by account and/or month (YYYY-MM)."""
    clauses = ["status = 'outstanding'"]
    params: dict[str, Any] = {}
    if account:
        clauses.append("account = :account")
        params["account"] = account.lower()
    if month:
        clauses.append("timestamp LIKE :month")
        params["month"] = f"{month}%"
    where = " AND ".join(clauses)
    return _rows(f"SELECT * FROM debtors WHERE {where} ORDER BY timestamp ASC", params)


def update_debt_staff_name(debt_id: int, staff_name: str) -> bool:
    """Set or update staff_name on a debt record. Returns True if a row was updated."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text("UPDATE debtors SET staff_name = :staff WHERE id = :id"),
            {"staff": staff_name.strip(), "id": debt_id},
        )
        conn.commit()
        return result.rowcount > 0


def get_debts_by_staff(staff_name: str) -> list[dict[str, Any]]:
    """Return all outstanding debts attributed to a staff member."""
    return _rows(
        "SELECT * FROM debtors WHERE lower(staff_name) = lower(:staff) AND status = 'outstanding' ORDER BY timestamp ASC",
        {"staff": staff_name.strip()},
    )


def get_outstanding_by_name(name: str) -> list[dict[str, Any]]:
    """Return all outstanding debts for a person across both accounts."""
    return _rows(
        "SELECT * FROM debtors WHERE lower(name) = lower(:name) AND status = 'outstanding' ORDER BY timestamp ASC",
        {"name": name.strip()},
    )


def mark_debtor_paid(name: str, account: str, paid_by: str = "", amount: float | None = None) -> dict[str, Any] | None:
    """
    Apply a payment (partial or full) to the oldest outstanding debt for name+account.
    If amount is None, pays the full remaining balance.
    Returns a result dict, or None if no outstanding debt found.
    """
    engine = get_engine()
    with engine.connect() as conn:
        row_result = conn.execute(text("""
            SELECT * FROM debtors
            WHERE lower(name) = lower(:name)
              AND account = :account
              AND status = 'outstanding'
            ORDER BY timestamp ASC
            LIMIT 1
        """), {"name": name.strip(), "account": account.lower()})
        row = row_result.fetchone()
        if row is None:
            return None

        debt = dict(row._mapping)
        debtor_id = int(debt["id"])
        original = float(debt["amount"])
        already_paid = float(debt.get("amount_paid") or 0)
        remaining_before = round(original - already_paid, 2)

        if amount is not None and round(amount, 2) > remaining_before:
            return {"error": "overpayment", "remaining": remaining_before, "debtor_id": debtor_id}

        pay_now = round(amount if amount is not None else remaining_before, 2)
        new_total_paid = round(already_paid + pay_now, 2)
        new_remaining = round(original - new_total_paid, 2)
        is_fully_paid = new_remaining <= 0

        if is_fully_paid:
            conn.execute(text("""
                UPDATE debtors SET
                    amount_paid = :total_paid,
                    status  = 'paid',
                    paid_at = :paid_at,
                    paid_by = :paid_by
                WHERE id = :id
            """), {"total_paid": new_total_paid, "paid_at": now_str(), "paid_by": paid_by, "id": debtor_id})
        else:
            conn.execute(text("""
                UPDATE debtors SET amount_paid = :total_paid WHERE id = :id
            """), {"total_paid": new_total_paid, "id": debtor_id})

        conn.execute(text("""
            INSERT INTO debtor_payments (debtor_id, timestamp, amount, recorded_by)
            VALUES (:debtor_id, :ts, :amount, :recorded_by)
        """), {"debtor_id": debtor_id, "ts": now_str(), "amount": pay_now, "recorded_by": paid_by})

        conn.commit()

    return {
        "debtor_id": debtor_id,
        "name": name.strip(),
        "account": account.lower(),
        "original_amount": original,
        "amount_paid_now": pay_now,
        "total_paid": new_total_paid,
        "remaining": max(new_remaining, 0),
        "is_fully_paid": is_fully_paid,
    }


def mark_debt_paid_by_id(debt_id: int, paid_by: str = "", amount: float | None = None) -> dict[str, Any] | None:
    """
    Apply a payment to a specific debt row by its ID.
    Returns the same result dict as mark_debtor_paid, or None if not found / already paid.
    """
    engine = get_engine()
    with engine.connect() as conn:
        row_result = conn.execute(text("""
            SELECT * FROM debtors WHERE id = :id AND status = 'outstanding'
        """), {"id": debt_id})
        row = row_result.fetchone()
        if row is None:
            return None

        debt = dict(row._mapping)
        original = float(debt["amount"])
        already_paid = float(debt.get("amount_paid") or 0)
        remaining_before = round(original - already_paid, 2)

        if amount is not None and round(amount, 2) > remaining_before:
            return {"error": "overpayment", "remaining": remaining_before, "debtor_id": debt_id}

        pay_now = round(amount if amount is not None else remaining_before, 2)
        new_total_paid = round(already_paid + pay_now, 2)
        new_remaining = round(original - new_total_paid, 2)
        is_fully_paid = new_remaining <= 0

        if is_fully_paid:
            conn.execute(text("""
                UPDATE debtors SET
                    amount_paid = :total_paid,
                    status  = 'paid',
                    paid_at = :paid_at,
                    paid_by = :paid_by
                WHERE id = :id
            """), {"total_paid": new_total_paid, "paid_at": now_str(), "paid_by": paid_by, "id": debt_id})
        else:
            conn.execute(text("""
                UPDATE debtors SET amount_paid = :total_paid WHERE id = :id
            """), {"total_paid": new_total_paid, "id": debt_id})

        conn.execute(text("""
            INSERT INTO debtor_payments (debtor_id, timestamp, amount, recorded_by)
            VALUES (:debtor_id, :ts, :amount, :recorded_by)
        """), {"debtor_id": debt_id, "ts": now_str(), "amount": pay_now, "recorded_by": paid_by})

        conn.commit()

    return {
        "debtor_id": debt_id,
        "name": str(debt["name"]),
        "account": str(debt["account"]),
        "original_amount": original,
        "amount_paid_now": pay_now,
        "total_paid": new_total_paid,
        "remaining": max(new_remaining, 0),
        "is_fully_paid": is_fully_paid,
    }


def get_debtor_history(name: str, account: str) -> dict[str, Any]:
    """Return all debts and payment events for a given person + account."""
    debts = _rows(
        "SELECT * FROM debtors WHERE lower(name) = lower(:name) AND account = :account ORDER BY timestamp ASC",
        {"name": name.strip(), "account": account.lower()},
    )
    if not debts:
        return {"debts": [], "payments": {}}

    debtor_ids = [int(d["id"]) for d in debts]
    payments = _rows(
        "SELECT * FROM debtor_payments WHERE debtor_id = ANY(:ids) ORDER BY timestamp ASC",
        {"ids": debtor_ids},
    )
    payments_by_id: dict[int, list[dict]] = {}
    for row in payments:
        did = int(row["debtor_id"])
        payments_by_id.setdefault(did, []).append(row)

    return {"debts": debts, "payments": payments_by_id}


# ── Inventory operations ──────────────────────────────────────────────

def get_drink(drink: str) -> dict[str, Any] | None:
    return _row(
        "SELECT * FROM inventory WHERE lower(drink_name) = lower(:name)",
        {"name": drink.lower()},
    )


def upsert_drink(
    drink: str,
    qty_to_store: int = 0,
    qty_to_bar: int = 0,
    qty_sold: int = 0,
    cost_price: float | None = None,
    threshold: int | None = None,
    selling_price: float | None = None,
) -> dict[str, Any]:
    """Create or update an inventory row atomically. Returns the updated row.

    qty_to_store  — units arriving in the store (restock)
    qty_to_bar    — units moving from store to bar (transfer, handled separately)
    qty_sold      — units sold from bar
    selling_price — canonical selling price set by admin (None = leave unchanged)
    """
    from config import LOW_STOCK_DEFAULT
    name = drink.lower()
    cp = round(cost_price, 2) if cost_price is not None else 0.0
    th = threshold if threshold is not None else LOW_STOCK_DEFAULT
    sp = round(selling_price, 2) if selling_price is not None else 0.0

    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO inventory
                (drink_name, current_stock, store_stock, total_purchased, total_sold,
                 cost_price, low_stock_threshold, selling_price)
            VALUES
                (:name, :bar_net, :store_net, :bought, :sold, :cp, :th, :sp)
            ON CONFLICT (drink_name) DO UPDATE SET
                current_stock       = inventory.current_stock + :bar_net,
                store_stock         = inventory.store_stock + :store_net,
                total_purchased     = inventory.total_purchased + :bought,
                total_sold          = inventory.total_sold + :sold,
                cost_price          = CASE WHEN :has_cp THEN :cp
                                          ELSE inventory.cost_price END,
                low_stock_threshold = CASE WHEN :has_th THEN :th
                                          ELSE inventory.low_stock_threshold END,
                selling_price       = CASE WHEN :has_sp THEN :sp
                                          ELSE inventory.selling_price END
        """), {
            "name": name,
            "bar_net": qty_to_bar - qty_sold,
            "store_net": qty_to_store,
            "bought": qty_to_store + qty_to_bar,
            "sold": qty_sold,
            "cp": cp,
            "has_cp": cost_price is not None,
            "th": th,
            "has_th": threshold is not None,
            "sp": sp,
            "has_sp": selling_price is not None,
        })
        conn.commit()
    return get_drink(name) or {}


def transfer_drink(drink: str, qty: int) -> dict[str, Any]:
    """Move qty from store to bar. Raises ValueError if store stock is insufficient."""
    name = drink.lower()
    row = get_drink(name)
    if row is None:
        raise ValueError(f"'{drink}' not found in inventory.")
    if int(row["store_stock"]) < qty:
        raise ValueError(
            f"Not enough store stock for *{drink.title()}*. "
            f"Store has {int(row['store_stock'])}, requested {qty}."
        )
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            UPDATE inventory
               SET store_stock   = store_stock - :qty,
                   current_stock = current_stock + :qty
             WHERE lower(drink_name) = lower(:name)
        """), {"qty": qty, "name": name})
        conn.commit()
    return get_drink(name) or {}


# ── Inventory corrections (rename/merge, set stock, delete) ───────────

def rename_or_merge_drink(old: str, new: str) -> dict[str, Any] | None:
    """Rename a drink, or merge it into an existing one.

    If `new` already exists, sums old's stock + lifetime totals into it
    (keeping new's cost/selling price + threshold), reassigns old's sales rows
    to new, then deletes old. Otherwise it is a plain rename. Returns a summary
    dict, or None if `old` does not exist.
    """
    old_n, new_n = old.strip().lower(), new.strip().lower()
    old_row = get_drink(old_n)
    if old_row is None:
        return None
    target = get_drink(new_n)
    o_store = int(old_row["store_stock"])
    o_bar = int(old_row["current_stock"])

    engine = get_engine()
    with engine.connect() as conn:
        if target is None:
            conn.execute(
                text("UPDATE inventory SET drink_name = :new WHERE lower(drink_name) = :old"),
                {"new": new_n, "old": old_n},
            )
            merged = False
        else:
            conn.execute(text("""
                UPDATE inventory SET
                    current_stock   = current_stock   + :bar,
                    store_stock     = store_stock     + :store,
                    total_purchased = total_purchased + :bought,
                    total_sold      = total_sold      + :sold
                WHERE lower(drink_name) = :new
            """), {
                "bar": o_bar, "store": o_store,
                "bought": int(old_row.get("total_purchased") or 0),
                "sold": int(old_row.get("total_sold") or 0),
                "new": new_n,
            })
            conn.execute(text("DELETE FROM inventory WHERE lower(drink_name) = :old"), {"old": old_n})
            merged = True
        # Keep sales history attached to the surviving SKU name.
        conn.execute(
            text("UPDATE sales SET drink_name = :new WHERE lower(drink_name) = :old"),
            {"new": new_n, "old": old_n},
        )
        conn.commit()

    return {
        "merged": merged, "old": old_n, "new": new_n,
        "moved_store": o_store, "moved_bar": o_bar,
        "row": get_drink(new_n) or {},
    }


def set_drink_stock(drink: str, store: int, bar: int) -> dict[str, Any] | None:
    """Overwrite store + bar counts for a drink (lifetime totals untouched).

    Returns the updated row, or None if the drink does not exist.
    """
    name = drink.strip().lower()
    if get_drink(name) is None:
        return None
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            UPDATE inventory
               SET store_stock = :store, current_stock = :bar
             WHERE lower(drink_name) = :name
        """), {"store": store, "bar": bar, "name": name})
        conn.commit()
    return get_drink(name)


def set_drink_cost(drink: str, cost_price: float) -> dict[str, Any] | None:
    """Overwrite a drink's cost price (lifetime totals & stock untouched).

    Returns the updated row, or None if the drink does not exist.
    """
    name = drink.strip().lower()
    if get_drink(name) is None:
        return None
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            UPDATE inventory
               SET cost_price = :cost
             WHERE lower(drink_name) = :name
        """), {"cost": round(cost_price, 2), "name": name})
        conn.commit()
    return get_drink(name)


def delete_drink(drink: str) -> dict[str, Any] | None:
    """Delete an inventory row. Returns the deleted row, or None if not found."""
    name = drink.strip().lower()
    row = get_drink(name)
    if row is None:
        return None
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM inventory WHERE lower(drink_name) = :name"), {"name": name})
        conn.commit()
    return row


# ── Entry history & deletion ─────────────────────────────────────────

def get_entries_by_date(date_str: str) -> list[dict[str, Any]]:
    """Return active (non-voided) sales, rooms, and expenses for a given YYYY-MM-DD."""
    entries: list[dict[str, Any]] = []

    for table, tag in (("sales", "sale"), ("rooms", "room"), ("expenses", "expense")):
        rows = _rows(
            f"SELECT * FROM {table} WHERE timestamp LIKE :prefix"
            f" AND (deleted_at = '' OR deleted_at IS NULL) ORDER BY timestamp",
            {"prefix": date_str + "%"},
        )
        for row in rows:
            row["entry_type"] = tag
            entries.append(row)

    entries.sort(key=lambda r: r.get("timestamp", ""))
    return entries


def void_sale(entry_id: int, actor: str = "") -> dict[str, Any] | None:
    """Soft-void a sale row. Returns the row (for stock restoration) or None if not found/already voided."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                UPDATE sales SET deleted_by = :actor, deleted_at = :ts
                WHERE id = :id AND (deleted_at = '' OR deleted_at IS NULL)
                RETURNING *
            """),
            {"id": entry_id, "actor": actor, "ts": now_str()},
        )
        conn.commit()
        row = result.fetchone()
        return dict(row._mapping) if row else None


def void_room(entry_id: int, actor: str = "") -> bool:
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                UPDATE rooms SET deleted_by = :actor, deleted_at = :ts
                WHERE id = :id AND (deleted_at = '' OR deleted_at IS NULL)
            """),
            {"id": entry_id, "actor": actor, "ts": now_str()},
        )
        conn.commit()
        return result.rowcount > 0


def void_expense(entry_id: int, actor: str = "") -> bool:
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                UPDATE expenses SET deleted_by = :actor, deleted_at = :ts
                WHERE id = :id AND (deleted_at = '' OR deleted_at IS NULL)
            """),
            {"id": entry_id, "actor": actor, "ts": now_str()},
        )
        conn.commit()
        return result.rowcount > 0


def void_draw(entry_id: int, actor: str = "") -> dict[str, Any] | None:
    """Soft-void an owner-draw row. Returns the row or None if not found/already voided."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                UPDATE owner_draws SET deleted_by = :actor, deleted_at = :ts
                WHERE id = :id AND (deleted_at = '' OR deleted_at IS NULL)
                RETURNING *
            """),
            {"id": entry_id, "actor": actor, "ts": now_str()},
        )
        conn.commit()
        row = result.fetchone()
        return dict(row._mapping) if row else None


# ── Transfer log ─────────────────────────────────────────────────────

def record_transfer(drink: str, qty: int, recorded_by: str = "", timestamp: str | None = None) -> None:
    """Log a store→bar stock transfer for audit purposes."""
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO transfers (timestamp, drink_name, quantity, recorded_by)
            VALUES (:ts, :drink, :qty, :recorded_by)
        """), {"ts": _ts(timestamp), "drink": drink.lower(), "qty": qty, "recorded_by": recorded_by})
        conn.commit()


# ── Activity log ────────────────────────────────────────────────────

def get_activity_log(date_str: str, username: str | None = None) -> list[dict[str, Any]]:
    """
    Return all activity for YYYY-MM-DD, tagged by entry_type.
    Includes voided/deleted entries (flagged via deleted_at).
    Optionally filter to a single actor (recorded_by / paid_by).
    """
    entries: list[dict[str, Any]] = []
    prefix = date_str + "%"
    u_filter = username

    # Sales, rooms, expenses — include voided rows (no deleted_at filter here)
    for table, tag in (
        ("sales",    "sale"),
        ("rooms",    "room"),
        ("expenses", "expense"),
        ("debtors",  "debtor_add"),
    ):
        if u_filter:
            rows = _rows(
                f"SELECT * FROM {table} WHERE timestamp LIKE :prefix AND recorded_by = :u ORDER BY timestamp",
                {"prefix": prefix, "u": u_filter},
            )
        else:
            rows = _rows(
                f"SELECT * FROM {table} WHERE timestamp LIKE :prefix ORDER BY timestamp",
                {"prefix": prefix},
            )
        for row in rows:
            row["entry_type"] = tag
            entries.append(row)

    # Debts marked paid on this date
    if u_filter:
        paid_rows = _rows(
            "SELECT * FROM debtors WHERE paid_at LIKE :prefix AND status = 'paid' AND paid_by = :u ORDER BY paid_at",
            {"prefix": prefix, "u": u_filter},
        )
    else:
        paid_rows = _rows(
            "SELECT * FROM debtors WHERE paid_at LIKE :prefix AND status = 'paid' ORDER BY paid_at",
            {"prefix": prefix},
        )
    for row in paid_rows:
        row["entry_type"] = "debtor_pay"
        row["timestamp"] = row.get("paid_at", "")
        entries.append(row)

    # Store→bar transfers
    if u_filter:
        tf_rows = _rows(
            "SELECT * FROM transfers WHERE timestamp LIKE :prefix AND recorded_by = :u ORDER BY timestamp",
            {"prefix": prefix, "u": u_filter},
        )
    else:
        tf_rows = _rows(
            "SELECT * FROM transfers WHERE timestamp LIKE :prefix ORDER BY timestamp",
            {"prefix": prefix},
        )
    for row in tf_rows:
        row["entry_type"] = "transfer"
        entries.append(row)

    entries.sort(key=lambda r: r.get("timestamp", ""))
    return entries


# ── Price list ───────────────────────────────────────────────────────

def get_drink_selling_prices() -> list[dict[str, Any]]:
    """Return drink_name and selling_price for all inventory rows."""
    return _rows("SELECT drink_name, selling_price FROM inventory ORDER BY drink_name")


# ── Undo (last staff entry within window) ────────────────────────────

# When an entry was keyed in. Rows predating the created_at migration have it
# blank, so fall back to `timestamp` — right for everything except the backdated
# rows, which had no working undo before the column existed anyway.
_ENTERED_AT = "COALESCE(NULLIF(created_at, ''), timestamp)"


def _entry_age_seconds(row: dict[str, Any]) -> float:
    """Seconds since the row was *keyed in* (not the date it was recorded for)."""
    raw = str(row.get("created_at") or "").strip() or str(row.get("timestamp") or "")
    try:
        entered = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return float("inf")
    return (clock.now() - entered).total_seconds()


def get_last_staff_entry(username: str, window_minutes: int = 2) -> dict[str, Any] | None:
    """
    Return the most recent sale or room entry recorded by `username`
    within the last `window_minutes` minutes, or None if outside the window.

    Recency is judged on when the row was *entered*, not the date it was entered
    for: ordering a backdated sale by `timestamp` would hide it behind today's
    rows and then reject it as days old.
    """
    sale = _row(
        f"SELECT *, 'sale' AS entry_type FROM sales "
        f"WHERE recorded_by = :u AND (deleted_at = '' OR deleted_at IS NULL)"
        f" ORDER BY {_ENTERED_AT} DESC, id DESC LIMIT 1",
        {"u": username},
    )
    room = _row(
        f"SELECT *, 'room' AS entry_type FROM rooms "
        f"WHERE recorded_by = :u AND (deleted_at = '' OR deleted_at IS NULL)"
        f" ORDER BY {_ENTERED_AT} DESC, id DESC LIMIT 1",
        {"u": username},
    )

    candidates = [c for c in (sale, room) if c is not None]
    if not candidates:
        return None

    best = min(candidates, key=_entry_age_seconds)
    if _entry_age_seconds(best) > window_minutes * 60:
        return None
    return best


def get_undoable_entry(entry_type: str, entry_id: int, username: str,
                       window_minutes: int = 2) -> dict[str, Any] | None:
    """Return one specific sale/room row if `username` may still undo it.

    The inline undo button carries the id of the entry it was attached to, so
    the reversal lands on that exact row — not on whatever happens to be the
    person's newest entry by the time they tap.

    None means: unknown id, already voided, entered by someone else, or the
    window has closed.
    """
    table = {"sale": "sales", "room": "rooms"}.get(entry_type)
    if table is None or entry_id <= 0:
        return None
    row = _row(
        f"SELECT *, '{entry_type}' AS entry_type FROM {table} "
        f"WHERE id = :id AND recorded_by = :u AND (deleted_at = '' OR deleted_at IS NULL)",
        {"id": entry_id, "u": username},
    )
    if row is None or _entry_age_seconds(row) > window_minutes * 60:
        return None
    return row


# ── Stocktakes ────────────────────────────────────────────────────────

def record_stock_count(drink: str, expected: int, counted: int, cost_price: float,
                       note: str = "", recorded_by: str = "",
                       timestamp: str | None = None, location: str = "bar",
                       period: str = "") -> dict[str, Any]:
    """Log one physical count and true the bar stock up to what was counted.

    Both halves matter: the log preserves the variance for the shrinkage report
    (overwriting the stock alone would erase the evidence), and the correction
    stops one bad count cascading into every later figure.
    """
    name = drink.strip().lower()
    loc = "store" if str(location).strip().lower() == "store" else "bar"
    variance = int(counted) - int(expected)
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO stock_counts
                (timestamp, drink_name, expected, counted, variance, cost_price,
                 note, recorded_by, location, period)
            VALUES (:ts, :name, :expected, :counted, :variance, :cost,
                    :note, :recorded_by, :loc, :period)
        """), {
            "ts": _ts(timestamp), "name": name,
            "expected": int(expected), "counted": int(counted), "variance": variance,
            "cost": round(float(cost_price), 2), "note": note, "recorded_by": recorded_by,
            "loc": loc, "period": period or _ts(timestamp)[:7],
        })
        # True up the column that was actually counted — writing a store count
        # into current_stock would move phantom units onto the bar shelf.
        column = "store_stock" if loc == "store" else "current_stock"
        conn.execute(text(
            f"UPDATE inventory SET {column} = :counted WHERE lower(drink_name) = :name"
        ), {"counted": int(counted), "name": name})
        conn.commit()
    return {
        "drink": name, "expected": int(expected), "counted": int(counted),
        "variance": variance, "value": round(variance * float(cost_price), 2),
        "location": loc,
    }


def record_turnaway(room_type: str = "", quantity: int = 1, reason: str = "",
                    recorded_by: str = "", timestamp: str | None = None) -> int:
    """Log guests turned away for want of a room. Returns the new row's id.

    Writes no revenue, no expense and no stock movement — a turnaway is demand
    that never became a transaction. It exists purely so a full night can be
    told apart from a night that was full *and* still selling.
    """
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text("""
            INSERT INTO turnaways (timestamp, created_at, room_type, quantity, reason, recorded_by)
            VALUES (:ts, :created, :rtype, :qty, :reason, :recorded_by)
            RETURNING id
        """), {
            "ts": _ts(timestamp), "created": now_str(),
            "rtype": room_type.strip().lower(), "qty": max(int(quantity), 0),
            "reason": reason.strip(), "recorded_by": recorded_by,
        }).fetchone()
        conn.commit()
    return int(row[0]) if row else 0


# ── Supplier credit (payables) ────────────────────────────────────────

def record_payable(supplier: str, amount: float, drink_name: str = "", quantity: int = 0,
                   due_date: str = "", description: str = "", recorded_by: str = "",
                   timestamp: str | None = None) -> int:
    """Log stock received on supplier credit. Returns the new payable's id.

    Deliberately writes no expense row: the stock is on the shelf but no cash
    has left the account yet. `pay_supplier` records the cash movement later.
    """
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text("""
            INSERT INTO payables
                (timestamp, supplier, drink_name, quantity, amount, amount_paid,
                 due_date, status, description, recorded_by)
            VALUES (:ts, :supplier, :drink, :qty, :amount, 0, :due, 'outstanding', :desc, :by)
            RETURNING id
        """), {
            "ts": _ts(timestamp), "supplier": supplier.strip(), "drink": drink_name.strip().lower(),
            "qty": int(quantity), "amount": round(float(amount), 2),
            "due": due_date, "desc": description, "by": recorded_by,
        }).fetchone()
        conn.commit()
    return int(row[0])


def get_outstanding_payables() -> list[dict[str, Any]]:
    return _rows(
        "SELECT * FROM payables WHERE status = 'outstanding' ORDER BY "
        "CASE WHEN due_date = '' THEN 1 ELSE 0 END, due_date, timestamp"
    )


def pay_supplier(payable_id: int, amount: float | None = None,
                 paid_by: str = "") -> dict[str, Any] | None:
    """Settle a supplier invoice (fully or partially).

    Writes the matching `supplier` expense row so cash falls at the moment the
    money actually leaves — the whole point of tracking credit separately.
    Returns None if the id is unknown or already settled.
    """
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT * FROM payables WHERE id = :id AND status = 'outstanding'"
        ), {"id": payable_id}).fetchone()
        if row is None:
            return None

        bill = dict(row._mapping)
        original = float(bill["amount"])
        already = float(bill.get("amount_paid") or 0)
        remaining_before = round(original - already, 2)

        if amount is not None and round(amount, 2) > remaining_before:
            return {"error": "overpayment", "remaining": remaining_before, "payable_id": payable_id}

        pay_now = round(amount if amount is not None else remaining_before, 2)
        total_paid = round(already + pay_now, 2)
        remaining = round(original - total_paid, 2)
        fully_paid = remaining <= 0

        if fully_paid:
            conn.execute(text("""
                UPDATE payables SET amount_paid = :paid, status = 'paid', paid_at = :ts
                WHERE id = :id
            """), {"paid": total_paid, "ts": now_str(), "id": payable_id})
        else:
            conn.execute(text(
                "UPDATE payables SET amount_paid = :paid WHERE id = :id"
            ), {"paid": total_paid, "id": payable_id})

        supplier = str(bill["supplier"])
        conn.execute(text("""
            INSERT INTO expenses (timestamp, account, category, amount, description, recorded_by)
            VALUES (:ts, 'bar', 'supplier', :amount, :desc, :by)
        """), {
            "ts": now_str(), "amount": pay_now,
            "desc": f"payment to {supplier} (invoice #{payable_id})", "by": paid_by,
        })
        conn.commit()

    return {
        "payable_id": payable_id, "supplier": supplier,
        "original_amount": original, "amount_paid_now": pay_now,
        "total_paid": total_paid, "remaining": max(remaining, 0),
        "is_fully_paid": fully_paid,
    }


# ── Inventory snapshots ───────────────────────────────────────────────

def record_inventory_snapshot(snapshot_date: str | None = None) -> int:
    """Freeze today's stock levels into `inventory_snapshots`. Returns row count.

    Idempotent per day — re-running overwrites that day's rows rather than
    duplicating them, so a restart or a manual run is always safe.
    """
    day = snapshot_date or clock.today().strftime("%Y-%m-%d")
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("""
            INSERT INTO inventory_snapshots
                (snapshot_date, drink_name, bar_stock, store_stock, cost_price, stock_value)
            SELECT :day, drink_name, current_stock, store_stock, cost_price,
                   ROUND(((current_stock + store_stock) * cost_price)::numeric, 2)
              FROM inventory
            ON CONFLICT (snapshot_date, drink_name) DO UPDATE SET
                bar_stock   = EXCLUDED.bar_stock,
                store_stock = EXCLUDED.store_stock,
                cost_price  = EXCLUDED.cost_price,
                stock_value = EXCLUDED.stock_value
        """), {"day": day})
        conn.commit()
        return result.rowcount


def get_inventory_snapshots(since: str) -> list[dict[str, Any]]:
    """Snapshot rows on/after `since` (YYYY-MM-DD). Scoped so the table can grow
    without every cash-cycle report dragging its whole history into memory."""
    return _rows(
        "SELECT * FROM inventory_snapshots WHERE snapshot_date >= :since ORDER BY snapshot_date",
        {"since": since},
    )


# ── Settings ─────────────────────────────────────────────────────────

def get_setting(key: str, default: str = "") -> str:
    """Return a setting value by key, or default if not set."""
    row = _row("SELECT value FROM settings WHERE key = :key", {"key": key})
    return str(row["value"]) if row is not None else default


def set_setting(key: str, value: str) -> None:
    """Upsert a setting value."""
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO settings (key, value) VALUES (:key, :value)
            ON CONFLICT (key) DO UPDATE SET value = :value
        """), {"key": key, "value": value})
        conn.commit()


# ── Room type presets ─────────────────────────────────────────────────

def get_room_type_price(room_type: str) -> float | None:
    val = get_setting(f"roomtype_price:{room_type.strip().lower()}")
    try:
        return float(val) if val else None
    except ValueError:
        return None


def set_room_type_price(room_type: str, price: float) -> None:
    set_setting(f"roomtype_price:{room_type.strip().lower()}", str(round(price, 2)))


def get_all_room_type_prices() -> list[dict[str, Any]]:
    """Return all configured room type presets as [{room_type, price}]."""
    raw = _rows("SELECT key, value FROM settings WHERE key LIKE 'roomtype_price:%' ORDER BY key")
    rows = []
    for r in raw:
        rtype = str(r["key"]).replace("roomtype_price:", "").title()
        try:
            rows.append({"room_type": rtype, "price": float(r["value"])})
        except ValueError:
            pass
    return rows


# ── Room type inventory (how many of each type exist) ─────────────────
#
# Separate from the price preset above: this is the *denominator* that makes
# RevPAR computable per type. Two Executive rooms at a high rate can out-earn
# five Standards on ADR while yielding far less per room the hotel owns, and
# only a per-type room count can show that.

def get_room_type_count(room_type: str) -> int | None:
    val = get_setting(f"roomtype_rooms:{room_type.strip().lower()}")
    try:
        return int(float(val)) if val else None
    except ValueError:
        return None


def set_room_type_count(room_type: str, count: int) -> None:
    set_setting(f"roomtype_rooms:{room_type.strip().lower()}", str(int(count)))


def get_all_room_type_counts() -> dict[str, int]:
    """All configured per-type room counts as {lower-cased type: count}."""
    raw = _rows("SELECT key, value FROM settings WHERE key LIKE 'roomtype_rooms:%' ORDER BY key")
    out: dict[str, int] = {}
    for r in raw:
        rtype = str(r["key"]).replace("roomtype_rooms:", "").strip().lower()
        try:
            count = int(float(r["value"]))
        except (TypeError, ValueError):
            continue
        if rtype and count > 0:
            out[rtype] = count
    return out


def get_room_type_hours(room_type: str) -> float | None:
    """Hours one stay-unit of this room type occupies the room, if configured."""
    val = get_setting(f"roomtype_hours:{room_type.strip().lower()}")
    try:
        return float(val) if val else None
    except ValueError:
        return None


def set_room_type_hours(room_type: str, hours: float) -> None:
    set_setting(f"roomtype_hours:{room_type.strip().lower()}", str(round(float(hours), 2)))


def get_all_room_type_hours() -> dict[str, float]:
    """All configured per-type stay lengths as {lower-cased type: hours}.

    A type absent from this map is nightly — the overwhelming majority — so the
    map stays small and an unconfigured hotel behaves exactly as it always has.
    """
    raw = _rows("SELECT key, value FROM settings WHERE key LIKE 'roomtype_hours:%' ORDER BY key")
    out: dict[str, float] = {}
    for r in raw:
        rtype = str(r["key"]).replace("roomtype_hours:", "").strip().lower()
        try:
            hours = float(r["value"])
        except (TypeError, ValueError):
            continue
        if rtype and hours > 0:
            out[rtype] = hours
    return out


# ── Room audits ───────────────────────────────────────────────────────

def record_room_audit(audit_date: str, rooms_total: int, nights_logged: int,
                      nights_actual: int, rate_variance: float = 0.0,
                      variance_count: int = 0, period: str = "", note: str = "",
                      recorded_by: str = "") -> int:
    """Store one day's audit. Capture rate is a trend, not a one-off."""
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text("""
            INSERT INTO room_audits
                (timestamp, audit_date, period, rooms_total, nights_logged,
                 nights_actual, rate_variance, variance_count, note, recorded_by)
            VALUES (:ts, :d, :p, :rt, :nl, :na, :rv, :vc, :note, :by)
            RETURNING id
        """), {
            "ts": now_str(), "d": audit_date, "p": period or audit_date[:7],
            "rt": int(rooms_total), "nl": int(nights_logged), "na": int(nights_actual),
            "rv": round(float(rate_variance), 2), "vc": int(variance_count),
            "note": note, "by": recorded_by,
        }).fetchone()
        conn.commit()
    return int(row[0]) if row else 0


def get_room_audits(limit: int = 60) -> list[dict[str, Any]]:
    return _rows("SELECT * FROM room_audits ORDER BY audit_date DESC LIMIT :n",
                 {"n": int(limit)})


# ── Periodic obligations (the accrual register) ───────────────────────

def add_obligation(name: str, expected_amount: float, months: int,
                   account: str = "rooms", category: str = "maintenance",
                   start_date: str = "", recorded_by: str = "") -> int:
    """Register a bill that recurs every few months. Returns its id."""
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(text("""
            INSERT INTO periodic_obligations
                (name, account, category, expected_amount, months, start_date, active, created_at, recorded_by)
            VALUES (:name, :account, :category, :amount, :months, :start, TRUE, :created, :by)
            RETURNING id
        """), {
            "name": name.strip(), "account": account.strip().lower(),
            "category": category.strip().lower(),
            "amount": round(float(expected_amount), 2), "months": int(months),
            "start": start_date or now_str()[:10], "created": now_str(), "by": recorded_by,
        }).fetchone()
        conn.commit()
    return int(row[0]) if row else 0


def get_obligations(include_inactive: bool = False) -> list[dict[str, Any]]:
    sql = "SELECT * FROM periodic_obligations"
    if not include_inactive:
        sql += " WHERE active = TRUE"
    return _rows(sql + " ORDER BY id")


def set_obligation_active(obligation_id: int, active: bool) -> bool:
    """Retire (or restore) an obligation.

    Deliberately not a delete, and deliberately dated: the accruals it already
    made are part of past months' profit. Stamping `retired_on` stops it
    accruing from that day while everything before it stands — reading the flag
    alone erased the whole history and flipped the reserve negative.
    """
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text("UPDATE periodic_obligations SET active = :a, retired_on = :on WHERE id = :id"),
            {"a": bool(active), "on": "" if active else now_str()[:10],
             "id": int(obligation_id)})
        conn.commit()
        return result.rowcount > 0


# ── User management ───────────────────────────────────────────────────

def get_all_staff() -> list[str]:
    """Distinct staff names that have outstanding debts assigned to them.

    De-duplicated case-insensitively (and trimmed) so the same person stored
    under different capitalisation — e.g. ``Aisha`` vs ``aisha`` — collapses to
    one entry. The lookup (`get_debts_by_staff`) is already case-insensitive, so
    any representative spelling resolves to all of that person's debts; we keep
    the first spelling seen as the display label.
    """
    rows = _rows(
        "SELECT DISTINCT staff_name FROM debtors WHERE status = 'outstanding' AND staff_name IS NOT NULL AND staff_name <> ''"
    )
    seen: dict[str, str] = {}
    for row in rows:
        raw = str(row["staff_name"]).strip()
        if raw:
            seen.setdefault(raw.lower(), raw)
    return sorted(seen.values(), key=str.lower)


def get_user(user_id: int) -> dict[str, Any] | None:
    return _row("SELECT * FROM users WHERE user_id = :uid", {"uid": user_id})


def upsert_user(user_id: int, username: str, role: str = "staff") -> None:
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO users (user_id, username, role, added_at)
            VALUES (:uid, :uname, :role, :ts)
            ON CONFLICT (user_id) DO UPDATE SET
                username = :uname,
                role     = :role
        """), {"uid": user_id, "uname": username, "role": role, "ts": now_str()})
        conn.commit()


def remove_user(user_id: int) -> bool:
    """Delete a user by ID. Returns True if a row was deleted."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text("DELETE FROM users WHERE user_id = :uid"),
            {"uid": user_id},
        )
        conn.commit()
        return result.rowcount > 0


# Tables that stamp the actor who entered a row in a `recorded_by` column.
# Merging a staff name relabels the actor across every one of them so the
# whole audit trail (not just the staff report) reconciles to one name.
_RECORDED_BY_TABLES = (
    "sales", "rooms", "expenses", "owner_draws",
    "debtors", "debtor_payments", "transfers",
    "stock_counts", "payables",
)


def merge_recorded_by(old: str, new: str) -> dict[str, int]:
    """Relabel one staff name to another everywhere it appears.

    Updates the `recorded_by` actor on every record across all activity
    tables, and the access-list label (`users.username`), from `old` to `new`.
    Nothing is deleted — totals are untouched; the two names simply collapse
    into one in the staff report. Match is on the exact stored string.

    Returns a per-target row count, e.g. {"sales": 12, "users": 1, ...}.
    """
    old, new = old.strip(), new.strip()
    counts: dict[str, int] = {}
    engine = get_engine()
    with engine.connect() as conn:
        for table in _RECORDED_BY_TABLES:
            result = conn.execute(
                text(f"UPDATE {table} SET recorded_by = :new WHERE recorded_by = :old"),
                {"new": new, "old": old},
            )
            if result.rowcount:
                counts[table] = result.rowcount
        # Keep the access-list label in step with the report name (the "link").
        result = conn.execute(
            text("UPDATE users SET username = :new WHERE username = :old"),
            {"new": new, "old": old},
        )
        if result.rowcount:
            counts["users"] = result.rowcount
        conn.commit()
    return counts
