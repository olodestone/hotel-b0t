"""
logic.py — Sales and expense processing with validation.

All public functions return (ok: bool, message: str).
They validate inputs before touching the database.
"""
from __future__ import annotations

import re

import database as db
import inventory as inv
from inventory import StockResult

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def parse_date(s: str) -> str | None:
    """Return s if it looks like YYYY-MM-DD, else None."""
    return s if _DATE_RE.match(s) else None


# ── Drink sale ────────────────────────────────────────────────────────

def process_drink_sale(drink: str, qty: int, price: float, timestamp: str | None = None) -> tuple[bool, str]:
    """Validate inputs and delegate to inventory.sell_drink."""
    if qty <= 0:
        return False, "❌ Quantity must be a positive integer."
    if price <= 0:
        return False, "❌ Price must be a positive number."

    result: StockResult = inv.sell_drink(drink.strip(), qty, price, timestamp=timestamp)
    msg = result.message
    if result.low_stock_alert:
        msg += f"\n\n{result.low_stock_alert}"
    return result.ok, msg


# ── Room sale ─────────────────────────────────────────────────────────

def process_room_sale(room_type: str, qty: int, price: float, nights: int, timestamp: str | None = None) -> tuple[bool, str]:
    if qty <= 0:
        return False, "❌ Quantity must be a positive integer."
    if price <= 0:
        return False, "❌ Price must be a positive number."
    if nights <= 0:
        return False, "❌ Number of nights must be a positive integer."

    db.record_room(room_type.strip(), qty, price, nights, timestamp=timestamp)
    total = qty * price * nights
    date_note = f" _(recorded for {timestamp})_" if timestamp else ""
    return True, (
        f"✅ Room booking recorded.{date_note}\n"
        f"Type: *{room_type.title()}* | Qty: {qty} | "
        f"₦{price:,.2f}/night × {nights} night(s)\n"
        f"Total Revenue: *₦{total:,.2f}*"
    )


# ── Expense ───────────────────────────────────────────────────────────

VALID_ACCOUNTS = ("rooms", "bar")


def process_expense(account: str, category: str, amount: float, description: str = "", timestamp: str | None = None) -> tuple[bool, str]:
    if account.lower() not in VALID_ACCOUNTS:
        return False, f"❌ Account must be *rooms* or *bar*. Got: `{account}`"
    if amount <= 0:
        return False, "❌ Amount must be a positive number."

    db.record_expense(account.strip(), category.strip(), amount, description.strip(), timestamp=timestamp)
    date_note = f"\nDate: {timestamp}" if timestamp else ""
    return True, (
        f"✅ Expense recorded.\n"
        f"Account: *{account.title()}* | Category: *{category.title()}* | Amount: ₦{amount:,.2f}"
        + (f"\nNote: {description}" if description else "")
        + date_note
    )


# ── Debtors ───────────────────────────────────────────────────────────

def process_add_debtor(account: str, name: str, amount: float, description: str = "", timestamp: str | None = None) -> tuple[bool, str]:
    if account.lower() not in VALID_ACCOUNTS:
        return False, f"❌ Account must be *rooms* or *bar*. Got: `{account}`"
    if not name.strip():
        return False, "❌ Debtor name cannot be empty."
    if amount <= 0:
        return False, "❌ Amount must be a positive number."

    db.record_debtor(account.strip(), name.strip(), amount, description.strip(), timestamp=timestamp)
    date_note = f"\nDate: {timestamp}" if timestamp else ""
    return True, (
        f"✅ Debtor recorded.\n"
        f"Account: *{account.title()}* | Name: *{name.title()}* | Owes: ₦{amount:,.2f}"
        + (f"\nNote: {description}" if description else "")
        + date_note
    )


def process_pay_debtor(account: str, name: str) -> tuple[bool, str]:
    if account.lower() not in VALID_ACCOUNTS:
        return False, f"❌ Account must be *rooms* or *bar*. Got: `{account}`"
    if not name.strip():
        return False, "❌ Debtor name cannot be empty."

    updated = db.mark_debtor_paid(name.strip(), account.strip())
    if updated:
        return True, f"✅ *{name.title()}* ({account.title()}) marked as paid."
    return False, f"❌ No outstanding debt found for *{name.title()}* in *{account.title()}*."


# ── Restock ───────────────────────────────────────────────────────────

def process_restock(drink: str, qty: int, cost_price: float) -> tuple[bool, str]:
    if qty <= 0:
        return False, "❌ Quantity must be a positive integer."
    if cost_price <= 0:
        return False, "❌ Cost price must be a positive number."

    result: StockResult = inv.restock_drink(drink.strip(), qty, cost_price)
    if result.ok:
        total_cost = round(qty * cost_price, 2)
        db.record_expense(
            account="bar",
            category="restock",
            amount=total_cost,
            description=f"Restock: {drink.strip().title()} ×{qty} @ ₦{cost_price:,.2f}",
        )
    return result.ok, result.message


# ── Store → Bar transfer ──────────────────────────────────────────────

def process_transfer(drink: str, qty: int) -> tuple[bool, str]:
    if qty <= 0:
        return False, "❌ Quantity must be a positive integer."

    result: StockResult = inv.transfer_to_bar(drink.strip(), qty)
    msg = result.message
    if result.low_stock_alert:
        msg += f"\n\n{result.low_stock_alert}"
    return result.ok, msg
