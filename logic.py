"""
logic.py — Sales and expense processing with validation.

All public functions return (ok: bool, message: str).
They validate inputs before touching the database.
"""
from __future__ import annotations

import re

import database as db
import inventory as inv
import reports
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


# Categories that describe the owner taking money out, not a business cost.
# These don't reduce profit — they reduce cash and owner's equity — so they
# must be logged with /draw, never as an expense.
_DRAW_LIKE_CATEGORIES = {
    "draw", "draws", "drawing", "drawings", "withdraw", "withdrawal",
    "withdrawals", "owner", "owners", "personal", "dividend", "dividends",
}


def process_expense(account: str, category: str, amount: float, description: str = "", timestamp: str | None = None, recorded_by: str = "") -> tuple[bool, str]:
    if account.lower() not in VALID_ACCOUNTS:
        return False, f"❌ Account must be *rooms* or *bar*. Got: `{account}`"
    if amount <= 0:
        return False, "❌ Amount must be a positive number."
    if category.strip().lower() in _DRAW_LIKE_CATEGORIES:
        return False, (
            f"❌ *{category.title()}* is an owner withdrawal, not a business expense.\n"
            "Money you take out doesn't reduce profit — log it with:\n"
            f"`/draw {amount:.0f}{(' ' + description) if description else ''}`\n"
            "See /position for how draws, profit and cash are tracked separately."
        )

    db.record_expense(account.strip(), category.strip(), amount, description.strip(), timestamp=timestamp, recorded_by=recorded_by)
    date_note = f"\nDate: {timestamp}" if timestamp else ""
    return True, (
        f"✅ Expense recorded.\n"
        f"Account: *{account.title()}* | Category: *{category.title()}* | Amount: ₦{amount:,.2f}"
        + (f"\nNote: {description}" if description else "")
        + date_note
    )


# ── Owner draws ───────────────────────────────────────────────────────

def process_draw(amount: float, description: str = "", account: str = "", timestamp: str | None = None, recorded_by: str = "") -> tuple[bool, str]:
    """Record an owner withdrawal. This is an equity draw, not an expense — it
    reduces cash and owner's equity but never the business's profit."""
    if amount <= 0:
        return False, "❌ Amount must be a positive number."
    if account and account.lower() not in VALID_ACCOUNTS:
        return False, f"❌ Account must be *rooms* or *bar* (or leave blank). Got: `{account}`"

    db.record_draw(amount, description.strip(), account.strip(), timestamp=timestamp, recorded_by=recorded_by)
    date_note = f"\nDate: {timestamp}" if timestamp else ""
    acct_note = f"\nFrom: *{account.title()}* account" if account.strip() else ""
    return True, (
        f"💸 Owner draw recorded: ₦{amount:,.2f}\n"
        "_Recorded as an equity withdrawal — it reduces cash, not profit._"
        + acct_note
        + (f"\nNote: {description}" if description else "")
        + date_note
    )


# ── Debtors ───────────────────────────────────────────────────────────

def process_add_debtor(account: str, name: str, amount: float, description: str = "", timestamp: str | None = None, recorded_by: str = "", staff_name: str = "") -> tuple[bool, str]:
    if account.lower() not in VALID_ACCOUNTS:
        return False, f"❌ Account must be *rooms* or *bar*. Got: `{account}`"
    if not name.strip():
        return False, "❌ Debtor name cannot be empty."
    if amount <= 0:
        return False, "❌ Amount must be a positive number."

    db.record_debtor(account.strip(), name.strip(), amount, description.strip(), timestamp=timestamp, recorded_by=recorded_by, staff_name=staff_name)
    date_note = f"\nDate: {timestamp}" if timestamp else ""
    staff_note = f"\nSold by: *{staff_name.title()}*" if staff_name.strip() else ""
    return True, (
        f"✅ Debtor recorded.\n"
        f"Account: *{account.title()}* | Name: *{name.title()}* | Owes: ₦{amount:,.2f}"
        + (f"\nNote: {description}" if description else "")
        + staff_note
        + date_note
    )


def process_set_debt_staff(debt_id: int, staff_name: str) -> tuple[bool, str]:
    """Reassign / correct the staff responsible for an existing debt.

    Used when a debt was logged against the wrong staff member. Relabel-only:
    it just updates ``staff_name`` on that one row — amounts and the debtor are
    untouched. Returns ``(False, …)`` on an empty name or unknown debt id.
    """
    name = staff_name.strip()
    if not name:
        return False, "❌ Staff name cannot be empty"
    if db.update_debt_staff_name(debt_id, name):
        return True, f"✅ Debt `#{debt_id}` — staff updated to *{reports._esc(name.title())}*"
    return False, f"❌ No debt found with ID `#{debt_id}`"


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

_VALID_ENTRY_TYPES = ("sale", "room", "expense", "draw")


def process_delete(entry_type: str, entry_id: int, actor: str = "") -> tuple[bool, str]:
    if entry_type not in _VALID_ENTRY_TYPES:
        return False, f"❌ Type must be *sale*, *room*, *expense*, or *draw*. Got: `{entry_type}`"

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

    if entry_type == "draw":
        row = db.void_draw(entry_id, actor=actor)
        if row is None:
            return False, f"❌ Draw entry `#{entry_id}` not found (or already voided)."
        amount = float(row["amount"])
        return True, f"✅ Owner draw `#{entry_id}` voided — ₦{amount:,.2f} added back to cash."

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


# ── Inventory corrections ─────────────────────────────────────────────

def process_rename_drink(old: str, new: str) -> tuple[bool, str]:
    """Rename a drink, or merge it into an existing SKU (de-duplicate)."""
    old_n, new_n = old.strip().lower(), new.strip().lower()
    if not old_n or not new_n:
        return False, "❌ Provide both names: `/renamedrink <old> <new>`"
    if old_n == new_n:
        return False, "❌ Old and new names are the same — nothing to do."
    if db.get_drink(old_n) is None:
        return False, f"❌ *{old.title()}* not found in inventory."

    res = db.rename_or_merge_drink(old_n, new_n)
    if res is None:
        return False, f"❌ *{old.title()}* not found in inventory."

    row = res["row"]
    store, bar = int(row.get("store_stock", 0)), int(row.get("current_stock", 0))
    if res["merged"]:
        return True, (
            f"✅ Merged *{old.title()}* into *{new.title()}*.\n"
            f"  Moved: {res['moved_store']} store + {res['moved_bar']} bar\n"
            f"  *{new.title()}* now: {store} store · {bar} bar\n"
            f"  _Old SKU removed; sales history reattached._"
        )
    return True, (
        f"✅ Renamed *{old.title()}* → *{new.title()}*.\n"
        f"  {store} store · {bar} bar"
    )


def process_merge_staff(old: str, new: str) -> tuple[bool, str]:
    """Reconcile two staff names into one (relabel, never delete)."""
    old_s, new_s = old.strip(), new.strip()
    if not old_s or not new_s:
        return False, "❌ Provide both names: the duplicate, then the name to keep."
    if old_s == new_s:
        return False, "❌ Both names are the same — nothing to reconcile."

    counts = db.merge_recorded_by(old_s, new_s)
    moved = sum(v for k, v in counts.items() if k != "users")
    old_e, new_e = reports._esc(old_s), reports._esc(new_s)
    if not counts:
        return False, f"❌ No records found under *{old_e}* — nothing changed."

    lines = [f"✅ Reconciled *{old_e}* → *{new_e}*."]
    if moved:
        lines.append(f"  {moved} record{'s' if moved != 1 else ''} relabelled (totals unchanged).")
    if counts.get("users"):
        lines.append("  Access-list name updated to match.")
    lines.append("  _Both names now show as one in the staff report._")
    return True, "\n".join(lines)


def process_set_stock(drink: str, store: int, bar: int) -> tuple[bool, str]:
    """Overwrite a drink's store + bar counts (for miscounts/breakage/spoilage)."""
    name = drink.strip().lower()
    if not name:
        return False, "❌ Provide a drink name."
    if store < 0 or bar < 0:
        return False, "❌ Stock counts cannot be negative."
    existing = db.get_drink(name)
    if existing is None:
        return False, f"❌ *{drink.title()}* not found in inventory. Run `/restock` first."

    old_store, old_bar = int(existing["store_stock"]), int(existing["current_stock"])
    db.set_drink_stock(name, store, bar)
    return True, (
        f"✅ Stock corrected for *{drink.title()}*\n"
        f"  Store: {old_store} → *{store}*\n"
        f"  Bar:   {old_bar} → *{bar}*\n"
        f"  _Lifetime purchase/sales totals unchanged._"
    )


def process_set_cost(drink: str, cost_price: float) -> tuple[bool, str]:
    """Overwrite a drink's cost price (for wrong/changed purchase costs).

    Cost price drives COGS and stock-value figures, so correcting it here
    avoids having to do a dummy restock just to fix the number.
    """
    name = drink.strip().lower()
    if not name:
        return False, "❌ Provide a drink name."
    if cost_price <= 0:
        return False, "❌ Cost price must be a positive number."
    existing = db.get_drink(name)
    if existing is None:
        return False, f"❌ *{drink.title()}* not found in inventory. Run `/restock` first."

    old_cost = float(existing["cost_price"])
    db.set_drink_cost(name, cost_price)
    return True, (
        f"✅ Cost price corrected for *{drink.title()}*\n"
        f"  Cost: ₦{old_cost:,.2f} → *₦{cost_price:,.2f}* per unit\n"
        f"  _Stock counts & lifetime totals unchanged._"
    )


def process_delete_drink(drink: str, force: bool = False) -> tuple[bool, str]:
    """Delete an inventory SKU. Refuses if it still holds stock unless forced."""
    name = drink.strip().lower()
    if not name:
        return False, "❌ Provide a drink name."
    existing = db.get_drink(name)
    if existing is None:
        return False, f"❌ *{drink.title()}* not found in inventory."

    store, bar = int(existing["store_stock"]), int(existing["current_stock"])
    if (store + bar) > 0 and not force:
        return False, (
            f"❌ *{drink.title()}* still has stock ({store} store · {bar} bar).\n"
            f"Zero it first: `/setstock {name} 0 0` — then delete."
        )
    db.delete_drink(name)
    return True, f"🗑 Removed *{drink.title()}* from inventory."
