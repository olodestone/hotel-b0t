"""
database.py — Thread-safe CSV persistence layer.

Each table is a CSV file under DATA_DIR.  Files are auto-created with
the correct headers on first use.  All reads return list[dict], all
writes/appends are protected by a per-table lock.
"""
import csv
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from config import DATA_DIR, LOW_STOCK_DEFAULT

# ── Schema definitions ───────────────────────────────────────────────

SCHEMAS: dict[str, dict] = {
    "inventory": {
        "file": DATA_DIR / "inventory.csv",
        "headers": [
            "drink_name", "current_stock", "total_purchased",
            "total_sold", "cost_price", "low_stock_threshold",
        ],
    },
    "sales": {
        "file": DATA_DIR / "sales.csv",
        "headers": ["timestamp", "drink_name", "quantity", "selling_price", "total_revenue"],
    },
    "rooms": {
        "file": DATA_DIR / "rooms.csv",
        "headers": [
            "timestamp", "room_type", "quantity",
            "price_per_night", "nights", "total_revenue",
        ],
    },
    "expenses": {
        "file": DATA_DIR / "expenses.csv",
        # account = "rooms" or "bar"
        "headers": ["timestamp", "account", "category", "amount", "description"],
    },
    "debtors": {
        "file": DATA_DIR / "debtors.csv",
        # status = "outstanding" or "paid"
        "headers": ["timestamp", "account", "name", "amount", "description", "status", "paid_at"],
    },
    "users": {
        "file": DATA_DIR / "users.csv",
        "headers": ["user_id", "username", "role", "added_at"],
    },
}

_locks: dict[str, threading.Lock] = {k: threading.Lock() for k in SCHEMAS}


# ── Init ─────────────────────────────────────────────────────────────

def init_db() -> None:
    """Create DATA_DIR and all CSV files with headers if absent."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for schema in SCHEMAS.values():
        fp: Path = schema["file"]
        if not fp.exists():
            with open(fp, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(schema["headers"])
    _migrate_expenses()


def _migrate_expenses() -> None:
    """
    If expenses.csv exists with the old schema (no 'account' column),
    rewrite it adding account='bar' to every row — bar was the only
    account type before this migration.
    """
    fp: Path = SCHEMAS["expenses"]["file"]
    new_headers = SCHEMAS["expenses"]["headers"]
    if not fp.exists():
        return
    with open(fp, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames and "account" in reader.fieldnames:
            return  # already migrated
        rows = list(reader)
    # Rewrite with account column defaulting to "bar"
    with open(fp, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=new_headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "timestamp": row.get("timestamp", ""),
                "account": "bar",
                "category": row.get("category", ""),
                "amount": row.get("amount", ""),
                "description": row.get("description", ""),
            })


# ── Generic CRUD ─────────────────────────────────────────────────────

def read_all(table: str) -> list[dict[str, Any]]:
    """Return all rows as a list of dicts."""
    schema = SCHEMAS[table]
    with _locks[table]:
        with open(schema["file"], "r", newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))


def write_all(table: str, rows: list[dict[str, Any]]) -> None:
    """Overwrite the table with the given rows (used for updates)."""
    schema = SCHEMAS[table]
    with _locks[table]:
        with open(schema["file"], "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=schema["headers"])
            writer.writeheader()
            writer.writerows(rows)


def append_row(table: str, row: dict[str, Any]) -> None:
    """Append a single row without loading the whole file."""
    schema = SCHEMAS[table]
    with _locks[table]:
        with open(schema["file"], "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=schema["headers"])
            writer.writerow(row)


# ── Timestamped helpers ───────────────────────────────────────────────

def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ── Drink-sale record ─────────────────────────────────────────────────

def record_sale(drink: str, qty: int, price: float) -> None:
    append_row("sales", {
        "timestamp": now_str(),
        "drink_name": drink.lower(),
        "quantity": qty,
        "selling_price": price,
        "total_revenue": round(qty * price, 2),
    })


# ── Room-booking record ───────────────────────────────────────────────

def record_room(room_type: str, qty: int, price: float, nights: int) -> None:
    append_row("rooms", {
        "timestamp": now_str(),
        "room_type": room_type.lower(),
        "quantity": qty,
        "price_per_night": price,
        "nights": nights,
        "total_revenue": round(qty * price * nights, 2),
    })


# ── Expense record ────────────────────────────────────────────────────

def record_expense(account: str, category: str, amount: float, description: str = "") -> None:
    append_row("expenses", {
        "timestamp": now_str(),
        "account": account.lower(),
        "category": category.lower(),
        "amount": round(amount, 2),
        "description": description,
    })


# ── Debtor records ────────────────────────────────────────────────────

def record_debtor(account: str, name: str, amount: float, description: str = "") -> None:
    append_row("debtors", {
        "timestamp": now_str(),
        "account": account.lower(),
        "name": name.strip(),
        "amount": round(amount, 2),
        "description": description,
        "status": "outstanding",
        "paid_at": "",
    })


def get_debtors(account: str | None = None) -> list[dict[str, Any]]:
    """Return all outstanding debtor rows, optionally filtered by account."""
    rows = read_all("debtors")
    rows = [r for r in rows if r["status"] == "outstanding"]
    if account:
        rows = [r for r in rows if r["account"] == account.lower()]
    return rows


def mark_debtor_paid(name: str, account: str) -> bool:
    """
    Mark the oldest outstanding debt for this name+account as paid.
    Returns True if a row was updated, False if not found.
    """
    rows = read_all("debtors")
    updated = False
    for row in rows:
        if (
            row["name"].lower() == name.strip().lower()
            and row["account"] == account.lower()
            and row["status"] == "outstanding"
        ):
            row["status"] = "paid"
            row["paid_at"] = now_str()
            updated = True
            break  # mark one at a time
    if updated:
        write_all("debtors", rows)
    return updated


# ── Inventory operations ──────────────────────────────────────────────

def get_drink(drink: str) -> dict[str, Any] | None:
    """Return the inventory row for a drink, or None if not found."""
    name = drink.lower()
    for row in read_all("inventory"):
        if row["drink_name"].lower() == name:
            return row
    return None


def upsert_drink(
    drink: str,
    qty_purchased: int = 0,
    qty_sold: int = 0,
    cost_price: float | None = None,
    threshold: int | None = None,
) -> dict[str, Any]:
    """
    Create or update an inventory row atomically.
    Returns the updated row dict.
    """
    name = drink.lower()
    rows = read_all("inventory")
    found = False
    updated: dict[str, Any] = {}

    for row in rows:
        if row["drink_name"].lower() == name:
            cur_stock = int(row["current_stock"])
            new_stock = cur_stock + qty_purchased - qty_sold
            row["current_stock"] = new_stock
            row["total_purchased"] = int(row["total_purchased"]) + qty_purchased
            row["total_sold"] = int(row["total_sold"]) + qty_sold
            if cost_price is not None:
                row["cost_price"] = round(cost_price, 2)
            if threshold is not None:
                row["low_stock_threshold"] = threshold
            updated = row
            found = True
            break

    if not found:
        new_row: dict[str, Any] = {
            "drink_name": name,
            "current_stock": qty_purchased - qty_sold,
            "total_purchased": qty_purchased,
            "total_sold": qty_sold,
            "cost_price": round(cost_price, 2) if cost_price is not None else 0.0,
            "low_stock_threshold": threshold if threshold is not None else LOW_STOCK_DEFAULT,
        }
        rows.append(new_row)
        updated = new_row

    write_all("inventory", rows)
    return updated


# ── User management ───────────────────────────────────────────────────

def get_user(user_id: int) -> dict[str, Any] | None:
    for row in read_all("users"):
        if int(row["user_id"]) == user_id:
            return row
    return None


def upsert_user(user_id: int, username: str, role: str = "staff") -> None:
    rows = read_all("users")
    for row in rows:
        if int(row["user_id"]) == user_id:
            row["username"] = username
            row["role"] = role
            write_all("users", rows)
            return
    append_row("users", {
        "user_id": user_id,
        "username": username,
        "role": role,
        "added_at": now_str(),
    })
