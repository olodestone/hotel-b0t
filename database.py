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
        # Migrations: add columns to existing inventory rows
        conn.execute(text(
            "ALTER TABLE inventory ADD COLUMN IF NOT EXISTS store_stock INTEGER NOT NULL DEFAULT 0"
        ))
        conn.execute(text(
            "ALTER TABLE inventory ADD COLUMN IF NOT EXISTS selling_price FLOAT NOT NULL DEFAULT 0"
        ))
        # Back-fill selling_price from the most recent sale per drink (one-time, safe to re-run)
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
                total_revenue   FLOAT,
                recorded_by     TEXT DEFAULT ''
            )
        """))
        conn.execute(text("ALTER TABLE rooms ADD COLUMN IF NOT EXISTS recorded_by TEXT DEFAULT ''"))
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
        conn.execute(text("ALTER TABLE rooms    ADD COLUMN IF NOT EXISTS deleted_by TEXT DEFAULT ''"))
        conn.execute(text("ALTER TABLE rooms    ADD COLUMN IF NOT EXISTS deleted_at TEXT DEFAULT ''"))
        conn.execute(text("ALTER TABLE expenses ADD COLUMN IF NOT EXISTS deleted_by TEXT DEFAULT ''"))
        conn.execute(text("ALTER TABLE expenses ADD COLUMN IF NOT EXISTS deleted_at TEXT DEFAULT ''"))
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

def record_room(room_type: str, qty: int, price: float, nights: int, timestamp: str | None = None, recorded_by: str = "") -> None:
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO rooms (timestamp, room_type, quantity, price_per_night, nights, total_revenue, recorded_by)
            VALUES (:ts, :rtype, :qty, :price, :nights, :total, :recorded_by)
        """), {
            "ts": _ts(timestamp), "rtype": room_type.lower(),
            "qty": qty, "price": price, "nights": nights,
            "total": round(qty * price * nights, 2),
            "recorded_by": recorded_by,
        })
        conn.commit()


# ── Expense record ────────────────────────────────────────────────────

def record_expense(account: str, category: str, amount: float, description: str = "", timestamp: str | None = None, recorded_by: str = "") -> None:
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO expenses (timestamp, account, category, amount, description, recorded_by)
            VALUES (:ts, :account, :category, :amount, :desc, :recorded_by)
        """), {
            "ts": _ts(timestamp), "account": account.lower(),
            "category": category.lower(),
            "amount": round(amount, 2), "desc": description,
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


def get_outstanding_by_name(name: str) -> list[dict[str, Any]]:
    """Return all outstanding debts for a person across both accounts."""
    engine = get_engine()
    df = pd.read_sql(
        "SELECT * FROM debtors WHERE lower(name) = lower(%(name)s) AND status = 'outstanding' ORDER BY timestamp ASC",
        engine, params={"name": name.strip()},
    )
    return df.to_dict(orient="records")


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
    engine = get_engine()
    debts_df = pd.read_sql(
        "SELECT * FROM debtors WHERE lower(name) = lower(%(name)s) AND account = %(account)s ORDER BY timestamp ASC",
        engine, params={"name": name.strip(), "account": account.lower()},
    )
    debts = debts_df.to_dict(orient="records")
    if not debts:
        return {"debts": [], "payments": {}}

    debtor_ids = [int(d["id"]) for d in debts]
    payments_df = pd.read_sql(
        "SELECT * FROM debtor_payments WHERE debtor_id = ANY(%(ids)s) ORDER BY timestamp ASC",
        engine, params={"ids": debtor_ids},
    )
    payments_by_id: dict[int, list[dict]] = {}
    for row in payments_df.to_dict(orient="records"):
        did = int(row["debtor_id"])
        payments_by_id.setdefault(did, []).append(row)

    return {"debts": debts, "payments": payments_by_id}


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


# ── Entry history & deletion ─────────────────────────────────────────

def get_entries_by_date(date_str: str) -> list[dict[str, Any]]:
    """Return active (non-voided) sales, rooms, and expenses for a given YYYY-MM-DD."""
    engine = get_engine()
    entries: list[dict[str, Any]] = []

    for table, tag in (("sales", "sale"), ("rooms", "room"), ("expenses", "expense")):
        df = pd.read_sql(
            f"SELECT * FROM {table} WHERE timestamp LIKE %(prefix)s"
            f" AND (deleted_at = '' OR deleted_at IS NULL) ORDER BY timestamp",
            engine, params={"prefix": date_str + "%"},
        )
        for row in df.to_dict(orient="records"):
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


# ── Transfer log ─────────────────────────────────────────────────────

def record_transfer(drink: str, qty: int, recorded_by: str = "") -> None:
    """Log a store→bar stock transfer for audit purposes."""
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO transfers (timestamp, drink_name, quantity, recorded_by)
            VALUES (:ts, :drink, :qty, :recorded_by)
        """), {"ts": now_str(), "drink": drink.lower(), "qty": qty, "recorded_by": recorded_by})
        conn.commit()


# ── Activity log ────────────────────────────────────────────────────

def get_activity_log(date_str: str, username: str | None = None) -> list[dict[str, Any]]:
    """
    Return all activity for YYYY-MM-DD, tagged by entry_type.
    Includes voided/deleted entries (flagged via deleted_at).
    Optionally filter to a single actor (recorded_by / paid_by).
    """
    engine = get_engine()
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
            df = pd.read_sql(
                f"SELECT * FROM {table} WHERE timestamp LIKE %(prefix)s AND recorded_by = %(u)s ORDER BY timestamp",
                engine, params={"prefix": prefix, "u": u_filter},
            )
        else:
            df = pd.read_sql(
                f"SELECT * FROM {table} WHERE timestamp LIKE %(prefix)s ORDER BY timestamp",
                engine, params={"prefix": prefix},
            )
        for row in df.to_dict(orient="records"):
            row["entry_type"] = tag
            entries.append(row)

    # Debts marked paid on this date
    if u_filter:
        paid_df = pd.read_sql(
            "SELECT * FROM debtors WHERE paid_at LIKE %(prefix)s AND status = 'paid' AND paid_by = %(u)s ORDER BY paid_at",
            engine, params={"prefix": prefix, "u": u_filter},
        )
    else:
        paid_df = pd.read_sql(
            "SELECT * FROM debtors WHERE paid_at LIKE %(prefix)s AND status = 'paid' ORDER BY paid_at",
            engine, params={"prefix": prefix},
        )
    for row in paid_df.to_dict(orient="records"):
        row["entry_type"] = "debtor_pay"
        row["timestamp"] = row.get("paid_at", "")
        entries.append(row)

    # Store→bar transfers
    if u_filter:
        tf_df = pd.read_sql(
            "SELECT * FROM transfers WHERE timestamp LIKE %(prefix)s AND recorded_by = %(u)s ORDER BY timestamp",
            engine, params={"prefix": prefix, "u": u_filter},
        )
    else:
        tf_df = pd.read_sql(
            "SELECT * FROM transfers WHERE timestamp LIKE %(prefix)s ORDER BY timestamp",
            engine, params={"prefix": prefix},
        )
    for row in tf_df.to_dict(orient="records"):
        row["entry_type"] = "transfer"
        entries.append(row)

    entries.sort(key=lambda r: r.get("timestamp", ""))
    return entries


# ── Price list ───────────────────────────────────────────────────────

def get_drink_selling_prices() -> list[dict[str, Any]]:
    """Return drink_name and selling_price for all inventory rows."""
    engine = get_engine()
    df = pd.read_sql("SELECT drink_name, selling_price FROM inventory ORDER BY drink_name", engine)
    return df.to_dict(orient="records")


# ── Undo (last staff entry within window) ────────────────────────────

def get_last_staff_entry(username: str, window_minutes: int = 2) -> dict[str, Any] | None:
    """
    Return the most recent sale or room entry recorded by `username`
    within the last `window_minutes` minutes, or None if outside the window.
    """
    engine = get_engine()
    cutoff = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    sale_df = pd.read_sql(
        "SELECT *, 'sale' AS entry_type FROM sales "
        "WHERE recorded_by = %(u)s AND (deleted_at = '' OR deleted_at IS NULL)"
        " ORDER BY timestamp DESC LIMIT 1",
        engine, params={"u": username},
    )
    room_df = pd.read_sql(
        "SELECT *, 'room' AS entry_type FROM rooms "
        "WHERE recorded_by = %(u)s AND (deleted_at = '' OR deleted_at IS NULL)"
        " ORDER BY timestamp DESC LIMIT 1",
        engine, params={"u": username},
    )

    candidates = []
    for df in (sale_df, room_df):
        if not df.empty:
            candidates.append(df.iloc[0].to_dict())

    if not candidates:
        return None

    # Pick the most recent
    def _ts(r: dict) -> datetime:
        try:
            return datetime.strptime(r["timestamp"], "%Y-%m-%d %H:%M:%S")
        except (ValueError, KeyError):
            return datetime.min

    best = max(candidates, key=_ts)
    age_seconds = (datetime.now() - _ts(best)).total_seconds()
    if age_seconds > window_minutes * 60:
        return None
    return best


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
