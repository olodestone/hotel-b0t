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

def process_drink_sale(drink: str, qty: int, timestamp: str | None = None, recorded_by: str = "") -> tuple[bool, str, str | None]:
    """Validate inputs and delegate to inventory.sell_drink.
    Price is read from inventory (set by admin via /setprice).
    Returns (ok, message, low_stock_alert) — alert is None if no alert."""
    if qty <= 0:
        return False, "❌ Quantity must be a positive integer.", None

    result: StockResult = inv.sell_drink(drink.strip(), qty, timestamp=timestamp, recorded_by=recorded_by)
    return result.ok, result.message, result.low_stock_alert


# ── Set drink price (admin) ───────────────────────────────────────────

def process_set_price(drink: str, price: float) -> tuple[bool, str]:
    if price <= 0:
        return False, "❌ Price must be a positive number."
    name = drink.strip().lower()
    existing = db.get_drink(name)
    if existing is None:
        return False, f"❌ *{drink.title()}* not found in inventory. Run `/restock` first."
    old_price = float(existing.get("selling_price", 0))
    inv.set_drink_price(name, price)
    if old_price > 0:
        return True, (
            f"✅ Price updated for *{drink.title()}*\n"
            f"  Old price: ₦{old_price:,.2f}\n"
            f"  New price: ₦{price:,.2f}"
        )
    return True, f"✅ Selling price for *{drink.title()}* set to *₦{price:,.2f}*."


# ── Room sale ─────────────────────────────────────────────────────────

def process_room_sale(room_type: str, qty: int, price: float, nights: int, timestamp: str | None = None, recorded_by: str = "") -> tuple[bool, str]:
    if qty <= 0:
        return False, "❌ Quantity must be a positive integer."
    if price <= 0:
        return False, "❌ Price must be a positive number."
    if nights <= 0:
        return False, "❌ Number of nights must be a positive integer."

    db.record_room(room_type.strip(), qty, price, nights, timestamp=timestamp, recorded_by=recorded_by)
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


def process_expense(account: str, category: str, amount: float, description: str = "", timestamp: str | None = None, recorded_by: str = "") -> tuple[bool, str]:
    if account.lower() not in VALID_ACCOUNTS:
        return False, f"❌ Account must be *rooms* or *bar*. Got: `{account}`"
    if amount <= 0:
        return False, "❌ Amount must be a positive number."

    db.record_expense(account.strip(), category.strip(), amount, description.strip(), timestamp=timestamp, recorded_by=recorded_by)
    date_note = f"\nDate: {timestamp}" if timestamp else ""
    return True, (
        f"✅ Expense recorded.\n"
        f"Account: *{account.title()}* | Category: *{category.title()}* | Amount: ₦{amount:,.2f}"
        + (f"\nNote: {description}" if description else "")
        + date_note
    )


# ── Debtors ───────────────────────────────────────────────────────────

def process_add_debtor(account: str, name: str, amount: float, description: str = "", timestamp: str | None = None, recorded_by: str = "") -> tuple[bool, str]:
    if account.lower() not in VALID_ACCOUNTS:
        return False, f"❌ Account must be *rooms* or *bar*. Got: `{account}`"
    if not name.strip():
        return False, "❌ Debtor name cannot be empty."
    if amount <= 0:
        return False, "❌ Amount must be a positive number."

    db.record_debtor(account.strip(), name.strip(), amount, description.strip(), timestamp=timestamp, recorded_by=recorded_by)
    date_note = f"\nDate: {timestamp}" if timestamp else ""
    return True, (
        f"✅ Debtor recorded.\n"
        f"Account: *{account.title()}* | Name: *{name.title()}* | Owes: ₦{amount:,.2f}"
        + (f"\nNote: {description}" if description else "")
        + date_note
    )


def process_pay_debtor(account: str, name: str, paid_by: str = "", amount: float | None = None) -> tuple[bool, str]:
    if account.lower() not in VALID_ACCOUNTS:
        return False, f"❌ Account must be *rooms* or *bar*. Got: `{account}`"
    if not name.strip():
        return False, "❌ Debtor name cannot be empty."
    if amount is not None and amount <= 0:
        return False, "❌ Payment amount must be a positive number."

    result = db.mark_debtor_paid(name.strip(), account.strip(), paid_by=paid_by, amount=amount)
    if result is None:
        return False, f"❌ No outstanding debt found for *{name.title()}* in *{account.title()}*."
    if result.get("error") == "overpayment":
        return False, (
            f"❌ Payment of ₦{amount:,.2f} exceeds remaining balance of "
            f"*₦{result['remaining']:,.2f}* for *{name.title()}*."
        )

    if result["is_fully_paid"]:
        return True, (
            f"✅ *{name.title()}* ({account.title()}) — debt fully cleared.\n"
            f"Paid: ₦{result['amount_paid_now']:,.2f} | Original: ₦{result['original_amount']:,.2f}"
        )
    return True, (
        f"💳 Partial payment recorded for *{name.title()}* ({account.title()}).\n"
        f"Paid now:   ₦{result['amount_paid_now']:,.2f}\n"
        f"Total paid: ₦{result['total_paid']:,.2f} / ₦{result['original_amount']:,.2f}\n"
        f"Still owes: *₦{result['remaining']:,.2f}*"
    )


def process_pay_debt_by_id(debt_id: int, paid_by: str = "", amount: float | None = None) -> tuple[bool, str]:
    """Pay a specific debt by its row ID (partial or full)."""
    if amount is not None and amount <= 0:
        return False, "❌ Payment amount must be a positive number."

    result = db.mark_debt_paid_by_id(debt_id, paid_by=paid_by, amount=amount)
    if result is None:
        return False, f"❌ Debt `#{debt_id}` not found or already cleared."
    if result.get("error") == "overpayment":
        return False, (
            f"❌ Payment of ₦{amount:,.2f} exceeds remaining balance of "
            f"*₦{result['remaining']:,.2f}* on debt `#{debt_id}`."
        )

    name = result["name"].title()
    account = result["account"].title()
    if result["is_fully_paid"]:
        return True, (
            f"✅ *{name}* ({account}) debt `#{debt_id}` fully cleared.\n"
            f"Paid: ₦{result['amount_paid_now']:,.2f} | Original: ₦{result['original_amount']:,.2f}"
        )
    return True, (
        f"💳 Partial payment on debt `#{debt_id}` — *{name}* ({account}).\n"
        f"Paid now:   ₦{result['amount_paid_now']:,.2f}\n"
        f"Total paid: ₦{result['total_paid']:,.2f} / ₦{result['original_amount']:,.2f}\n"
        f"Still owes: *₦{result['remaining']:,.2f}*"
    )


# ── Restock ───────────────────────────────────────────────────────────

def process_restock(drink: str, qty: int, cost_price: float, recorded_by: str = "") -> tuple[bool, str]:
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
            recorded_by=recorded_by,
        )
    return result.ok, result.message


# ── Entry deletion ───────────────────────────────────────────────────

_VALID_ENTRY_TYPES = ("sale", "room", "expense")


def process_delete(entry_type: str, entry_id: int, actor: str = "") -> tuple[bool, str]:
    if entry_type not in _VALID_ENTRY_TYPES:
        return False, f"❌ Type must be *sale*, *room*, or *expense*. Got: `{entry_type}`"

    if entry_type == "sale":
        row = db.void_sale(entry_id, actor=actor)
        if row is None:
            return False, f"❌ Sale entry `#{entry_id}` not found (or already voided)."
        drink = row["drink_name"].title()
        qty = int(row["quantity"])
        total = float(row["total_revenue"])
        inv.restore_bar_stock(row["drink_name"], qty)
        return True, (
            f"✅ Sale `#{entry_id}` voided.\n"
            f"{drink} ×{qty} — ₦{total:,.2f} removed from revenue.\n"
            f"Bar stock restored +{qty}."
        )

    if entry_type == "room":
        found = db.void_room(entry_id, actor=actor)
        if not found:
            return False, f"❌ Room entry `#{entry_id}` not found (or already voided)."
        return True, f"✅ Room entry `#{entry_id}` voided."

    # expense
    found = db.void_expense(entry_id, actor=actor)
    if not found:
        return False, f"❌ Expense entry `#{entry_id}` not found (or already voided)."
    return True, f"✅ Expense entry `#{entry_id}` voided."


# ── Undo last entry ──────────────────────────────────────────────────

def process_undo(username: str) -> tuple[bool, str]:
    """Soft-void the last sale or room entry by this user if within the 2-min window."""
    entry = db.get_last_staff_entry(username)
    if entry is None:
        return False, (
            "❌ Nothing to undo.\n"
            "Either you have no recent entries, or the 2-minute window has passed."
        )

    entry_type = entry["entry_type"]

    if entry_type == "sale":
        row = db.void_sale(int(entry["id"]), actor=username)
        if row is None:
            return False, "❌ Could not find the entry to undo."
        drink = row["drink_name"].title()
        qty = int(row["quantity"])
        total = float(row["total_revenue"])
        inv.restore_bar_stock(row["drink_name"], qty)
        return True, (
            f"↩️ Undone: Sale of {qty}× *{drink}* — ₦{total:,.2f}\n"
            f"Bar stock restored +{qty}."
        )

    if entry_type == "room":
        found = db.void_room(int(entry["id"]), actor=username)
        if not found:
            return False, "❌ Could not find the entry to undo."
        room_type = entry["room_type"].title()
        total = float(entry["total_revenue"])
        return True, f"↩️ Undone: *{room_type}* room booking — ₦{total:,.2f} removed."

    return False, "❌ Unknown entry type."


# ── Store → Bar transfer ──────────────────────────────────────────────

def process_transfer(drink: str, qty: int, recorded_by: str = "") -> tuple[bool, str]:
    if qty <= 0:
        return False, "❌ Quantity must be a positive integer."

    result: StockResult = inv.transfer_to_bar(drink.strip(), qty)
    if result.ok:
        db.record_transfer(drink.strip(), qty, recorded_by=recorded_by)
    msg = result.message
    if result.low_stock_alert:
        msg += f"\n\n{result.low_stock_alert}"
    return result.ok, msg
