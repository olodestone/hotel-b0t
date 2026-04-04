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
                total_purchased     INTEGER NOT NULL DEFAULT 0,
                total_sold          INTEGER NOT NULL DEFAULT 0,
                cost_price          FLOAT   NOT NULL DEFAULT 0,
                low_stock_threshold INTEGER NOT NULL DEFAULT 5
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS sales (
                timestamp       TEXT,
                drink_name      TEXT,
                quantity        INTEGER,
                selling_price   FLOAT,
                total_revenue   FLOAT
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS rooms (
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
                timestamp   TEXT,
                account     TEXT,
                category    TEXT,
                amount      FLOAT,
                description TEXT
            )
        """))
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
        conn.commit()


# ── Generic read (used by inventory.py, reports.py) ───────────────────

def read_all(table: str) -> list[dict[str, Any]]:
    """Return all rows as a list of dicts."""
    engine = get_engine()
    df = pd.read_sql(f"SELECT * FROM {table}", engine)
    return df.to_dict(orient="records")


# ── Drink-sale record ─────────────────────────────────────────────────

def record_sale(drink: str, qty: int, price: float, timestamp: str | None = None) -> None:
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO sales (timestamp, drink_name, quantity, selling_price, total_revenue)
            VALUES (:ts, :drink, :qty, :price, :total)
        """), {
            "ts": _ts(timestamp), "drink": drink.lower(),
            "qty": qty, "price": price,
            "total": round(qty * price, 2),
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
    qty_purchased: int = 0,
    qty_sold: int = 0,
    cost_price: float | None = None,
    threshold: int | None = None,
) -> dict[str, Any]:
    """Create or update an inventory row atomically. Returns the updated row."""
    from config import LOW_STOCK_DEFAULT
    name = drink.lower()
    cp = round(cost_price, 2) if cost_price is not None else 0.0
    th = threshold if threshold is not None else LOW_STOCK_DEFAULT

    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO inventory
                (drink_name, current_stock, total_purchased, total_sold, cost_price, low_stock_threshold)
            VALUES
                (:name, :net, :bought, :sold, :cp, :th)
            ON CONFLICT (drink_name) DO UPDATE SET
                current_stock       = inventory.current_stock + :net,
                total_purchased     = inventory.total_purchased + :bought,
                total_sold          = inventory.total_sold + :sold,
                cost_price          = CASE WHEN :has_cp THEN :cp
                                          ELSE inventory.cost_price END,
                low_stock_threshold = CASE WHEN :has_th THEN :th
                                          ELSE inventory.low_stock_threshold END
        """), {
            "name": name,
            "net": qty_purchased - qty_sold,
            "bought": qty_purchased,
            "sold": qty_sold,
            "cp": cp,
            "has_cp": cost_price is not None,
            "th": th,
            "has_th": threshold is not None,
        })
        conn.commit()
    return get_drink(name) or {}


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
