"""
logic.py — Sales and expense processing with validation.

All public functions return (ok: bool, message: str).
They validate inputs before touching the database.
"""
from __future__ import annotations

import database as db
import inventory as inv
from inventory import StockResult


# ── Drink sale ────────────────────────────────────────────────────────

def process_drink_sale(drink: str, qty: int, price: float) -> tuple[bool, str]:
    """Validate inputs and delegate to inventory.sell_drink."""
    if qty <= 0:
        return False, "❌ Quantity must be a positive integer."
    if price <= 0:
        return False, "❌ Price must be a positive number."

    result: StockResult = inv.sell_drink(drink.strip(), qty, price)
    msg = result.message
    if result.low_stock_alert:
        msg += f"\n\n{result.low_stock_alert}"
    return result.ok, msg


# ── Room sale ─────────────────────────────────────────────────────────

def process_room_sale(room_type: str, qty: int, price: float, nights: int) -> tuple[bool, str]:
    if qty <= 0:
        return False, "❌ Quantity must be a positive integer."
    if price <= 0:
        return False, "❌ Price must be a positive number."
    if nights <= 0:
        return False, "❌ Number of nights must be a positive integer."

    db.record_room(room_type.strip(), qty, price, nights)
    total = qty * price * nights
    return True, (
        f"✅ Room booking recorded.\n"
        f"Type: *{room_type.title()}* | Qty: {qty} | "
        f"₦{price:,.2f}/night × {nights} night(s)\n"
        f"Total Revenue: *₦{total:,.2f}*"
    )


# ── Expense ───────────────────────────────────────────────────────────

VALID_ACCOUNTS = ("rooms", "bar")


def process_expense(account: str, category: str, amount: float, description: str = "") -> tuple[bool, str]:
    if account.lower() not in VALID_ACCOUNTS:
        return False, f"❌ Account must be *rooms* or *bar*. Got: `{account}`"
    if amount <= 0:
        return False, "❌ Amount must be a positive number."

    db.record_expense(account.strip(), category.strip(), amount, description.strip())
    return True, (
        f"✅ Expense recorded.\n"
        f"Account: *{account.title()}* | Category: *{category.title()}* | Amount: ₦{amount:,.2f}"
        + (f"\nNote: {description}" if description else "")
    )


# ── Debtors ───────────────────────────────────────────────────────────

def process_add_debtor(account: str, name: str, amount: float, description: str = "") -> tuple[bool, str]:
    if account.lower() not in VALID_ACCOUNTS:
        return False, f"❌ Account must be *rooms* or *bar*. Got: `{account}`"
    if not name.strip():
        return False, "❌ Debtor name cannot be empty."
    if amount <= 0:
        return False, "❌ Amount must be a positive number."

    db.record_debtor(account.strip(), name.strip(), amount, description.strip())
    return True, (
        f"✅ Debtor recorded.\n"
        f"Account: *{account.title()}* | Name: *{name.title()}* | Owes: ₦{amount:,.2f}"
        + (f"\nNote: {description}" if description else "")
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
    return result.ok, result.message
