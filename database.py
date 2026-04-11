"""
database.py — PostgreSQL persistence layer via SQLAlchemy.

Tables are auto-created on init_db(). All reads return list[dict],
all writes use parameterised queries. Same public API as before —
no changes needed in inventory.py, logic.py, or reports.py.
"""
from __future__ import annotations

import os
from datetime import datetime
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

DATABASE_URL: str = os.getenv("DATABASE_URL", "")


def get_engine() -> Engine:
    url = DATABASE_URL
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    if not url:
        raise RuntimeError("DATABASE_URL environment variable is not set.")
    return create_engine(url)


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _ts(custom: str | None = None) -> str:
    """Return a timestamp string: custom date (YYYY-MM-DD) → 'YYYY-MM-DD 00:00:00', else now."""
    if custom:
        return custom + " 00:00:00"
    return now_str()


# ── Init ─────────────────────────────────────────────────────────────

def init_db() -> None:
    """Create all tables if they don't exist."""
    engine = get_engine()
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
        # Migration: add store_stock to existing databases that predate this column
        conn.execute(text("""
            ALTER TABLE inventory ADD COLUMN IF NOT EXISTS store_stock INTEGER NOT NULL DEFAULT 0
        """))
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
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS rooms (
                id              SERIAL PRIMARY KEY,
                timestamp       TEXT,
                room_type       TEXT,
                quantity        INTEGER,
                price_per_night FLOAT,
                nights          INTEGER,
                total_revenue   FLOAT
            )
        """))
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
        conn.commit()


# ── Generic read (used by inventory.py, reports.py) ───────────────────

def read_all(table: str) -> list[dict[str, Any]]:
    """Return all rows as a list of dicts."""
    engine = get_engine()
    df = pd.read_sql(f"SELECT * FROM {table}", engine)
    return df.to_dict(orient="records")


# ── Drink-sale record ─────────────────────────────────────────────────

def record_sale(drink: str, qty: int, price: float, timestamp: str | None = None, recorded_by: str = "") -> None:
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO sales (timestamp, drink_name, quantity, selling_price, total_revenue, recorded_by)
            VALUES (:ts, :drink, :qty, :price, :total, :recorded_by)
        """), {
            "ts": _ts(timestamp), "drink": drink.lower(),
            "qty": qty, "price": price,
            "total": round(qty * price, 2),
            "recorded_by": recorded_by,
        })
        conn.commit()


# ── Room-booking record ───────────────────────────────────────────────

def record_room(room_type: str, qty: int, price: float, nights: int, timestamp: str | None = None) -> None:
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO rooms (timestamp, room_type, quantity, price_per_night, nights, total_revenue)
            VALUES (:ts, :rtype, :qty, :price, :nights, :total)
        """), {
            "ts": _ts(timestamp), "rtype": room_type.lower(),
            "qty": qty, "price": price, "nights": nights,
            "total": round(qty * price * nights, 2),
        })
        conn.commit()


# ── Expense record ────────────────────────────────────────────────────

def record_expense(account: str, category: str, amount: float, description: str = "", timestamp: str | None = None) -> None:
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO expenses (timestamp, account, category, amount, description)
            VALUES (:ts, :account, :category, :amount, :desc)
        """), {
            "ts": _ts(timestamp), "account": account.lower(),
            "category": category.lower(),
            "amount": round(amount, 2), "desc": description,
        })
        conn.commit()


# ── Debtor records ────────────────────────────────────────────────────

def record_debtor(account: str, name: str, amount: float, description: str = "", timestamp: str | None = None) -> None:
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO debtors (timestamp, account, name, amount, description, status, paid_at)
            VALUES (:ts, :account, :name, :amount, :desc, 'outstanding', '')
        """), {
            "ts": _ts(timestamp), "account": account.lower(),
            "name": name.strip(),
            "amount": round(amount, 2), "desc": description,
        })
        conn.commit()


def get_debtors(account: str | None = None) -> list[dict[str, Any]]:
    """Return all outstanding debtor rows, optionally filtered by account."""
    engine = get_engine()
    if account:
        df = pd.read_sql(
            "SELECT * FROM debtors WHERE status = 'outstanding' AND account = %(account)s",
            engine, params={"account": account.lower()},
        )
    else:
        df = pd.read_sql("SELECT * FROM debtors WHERE status = 'outstanding'", engine)
    return df.to_dict(orient="records")


def mark_debtor_paid(name: str, account: str) -> bool:
    """
    Mark the oldest outstanding debt for this name + account as paid.
    Returns True if a row was updated, False if not found.
    """
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("""
            UPDATE debtors SET status = 'paid', paid_at = :paid_at
            WHERE id = (
                SELECT id FROM debtors
                WHERE lower(name) = lower(:name)
                  AND account = :account
                  AND status  = 'outstanding'
                ORDER BY timestamp ASC
                LIMIT 1
            )
        """), {"paid_at": now_str(), "name": name.strip(), "account": account.lower()})
        conn.commit()
        return result.rowcount > 0


# ── Inventory operations ──────────────────────────────────────────────

def get_drink(drink: str) -> dict[str, Any] | None:
    engine = get_engine()
    df = pd.read_sql(
        "SELECT * FROM inventory WHERE lower(drink_name) = lower(%(name)s)",
        engine, params={"name": drink.lower()},
    )
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def upsert_drink(
    drink: str,
    qty_to_store: int = 0,
    qty_to_bar: int = 0,
    qty_sold: int = 0,
    cost_price: float | None = None,
    threshold: int | None = None,
) -> dict[str, Any]:
    """Create or update an inventory row atomically. Returns the updated row.

    qty_to_store — units arriving in the store (restock)
    qty_to_bar   — units moving from store to bar (transfer, handled separately)
    qty_sold     — units sold from bar
    """
    from config import LOW_STOCK_DEFAULT
    name = drink.lower()
    cp = round(cost_price, 2) if cost_price is not None else 0.0
    th = threshold if threshold is not None else LOW_STOCK_DEFAULT

    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO inventory
                (drink_name, current_stock, store_stock, total_purchased, total_sold, cost_price, low_stock_threshold)
            VALUES
                (:name, :bar_net, :store_net, :bought, :sold, :cp, :th)
            ON CONFLICT (drink_name) DO UPDATE SET
                current_stock       = inventory.current_stock + :bar_net,
                store_stock         = inventory.store_stock + :store_net,
                total_purchased     = inventory.total_purchased + :bought,
                total_sold          = inventory.total_sold + :sold,
                cost_price          = CASE WHEN :has_cp THEN :cp
                                          ELSE inventory.cost_price END,
                low_stock_threshold = CASE WHEN :has_th THEN :th
                                          ELSE inventory.low_stock_threshold END
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


# ── Entry history & deletion ─────────────────────────────────────────

def get_entries_by_date(date_str: str) -> list[dict[str, Any]]:
    """Return all sales, rooms, and expenses for a given YYYY-MM-DD, tagged by entry_type."""
    engine = get_engine()
    entries: list[dict[str, Any]] = []

    for table, tag in (("sales", "sale"), ("rooms", "room"), ("expenses", "expense")):
        df = pd.read_sql(
            f"SELECT * FROM {table} WHERE timestamp LIKE %(prefix)s ORDER BY timestamp",
            engine, params={"prefix": date_str + "%"},
        )
        for row in df.to_dict(orient="records"):
            row["entry_type"] = tag
            entries.append(row)

    entries.sort(key=lambda r: r.get("timestamp", ""))
    return entries


def delete_sale(entry_id: int) -> dict[str, Any] | None:
    """Delete a sale row by id. Returns the deleted row (for stock restoration) or None."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text("DELETE FROM sales WHERE id = :id RETURNING *"),
            {"id": entry_id},
        )
        conn.commit()
        row = result.fetchone()
        return dict(row._mapping) if row else None


def delete_room(entry_id: int) -> bool:
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text("DELETE FROM rooms WHERE id = :id"),
            {"id": entry_id},
        )
        conn.commit()
        return result.rowcount > 0


def delete_expense(entry_id: int) -> bool:
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text("DELETE FROM expenses WHERE id = :id"),
            {"id": entry_id},
        )
        conn.commit()
        return result.rowcount > 0


# ── Settings ─────────────────────────────────────────────────────────

def get_setting(key: str, default: str = "") -> str:
    """Return a setting value by key, or default if not set."""
    engine = get_engine()
    df = pd.read_sql(
        "SELECT value FROM settings WHERE key = %(key)s",
        engine, params={"key": key},
    )
    if df.empty:
        return default
    return str(df.iloc[0]["value"])


def set_setting(key: str, value: str) -> None:
    """Upsert a setting value."""
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO settings (key, value) VALUES (:key, :value)
            ON CONFLICT (key) DO UPDATE SET value = :value
        """), {"key": key, "value": value})
        conn.commit()


# ── User management ───────────────────────────────────────────────────

def get_user(user_id: int) -> dict[str, Any] | None:
    engine = get_engine()
    df = pd.read_sql(
        "SELECT * FROM users WHERE user_id = %(uid)s",
        engine, params={"uid": user_id},
    )
    if df.empty:
        return None
    return df.iloc[0].to_dict()


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
