"""
logic.py — Sales and expense processing with validation.

All public functions return (ok: bool, message: str).
They validate inputs before touching the database.
"""
from __future__ import annotations

import re

import database as db
import metrics
import inventory as inv
import reports
from inventory import StockResult

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def parse_date(s: str) -> str | None:
    """Return s if it looks like YYYY-MM-DD, else None."""
    return s if _DATE_RE.match(s) else None


# ── Drink sale ────────────────────────────────────────────────────────

def process_drink_sale(drink: str, qty: int, timestamp: str | None = None, recorded_by: str = "") -> tuple[bool, str, str | None, int]:
    """Validate inputs and delegate to inventory.sell_drink.
    Price is read from inventory (set by admin via /setprice).
    Returns (ok, message, low_stock_alert, sale_id) — alert is None if no alert,
    sale_id is 0 when nothing was written. The id lets the caller offer an undo
    button bound to *this* sale rather than to whatever the newest one is."""
    if qty <= 0:
        return False, "❌ Quantity must be a positive integer.", None, 0

    result: StockResult = inv.sell_drink(drink.strip(), qty, timestamp=timestamp, recorded_by=recorded_by)
    return result.ok, result.message, result.low_stock_alert, result.entry_id


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

def process_room_sale(room_type: str, qty: int, price: float, nights: int, timestamp: str | None = None,
                      recorded_by: str = "", duration_hours: float | None = None) -> tuple[bool, str, int]:
    """Returns (ok, message, room_id) — room_id is 0 when nothing was written.

    ``nights`` counts *stay units*: nights for a nightly room type, lets for an
    hourly one. ``duration_hours`` overrides the type's configured length for
    this booking alone — a one-off short let on an otherwise nightly room.
    """
    if qty <= 0:
        return False, "❌ Quantity must be a positive integer.", 0
    if price <= 0:
        return False, "❌ Price must be a positive number.", 0
    if nights <= 0:
        return False, "❌ Number of nights must be a positive integer.", 0
    if duration_hours is not None and not (0 < duration_hours <= 24):
        return False, "❌ Stay length must be between 0 and 24 hours.", 0

    room_id = db.record_room(room_type.strip(), qty, price, nights, timestamp=timestamp,
                             recorded_by=recorded_by, duration_hours=duration_hours or 0)
    total = qty * price * nights

    # Name the unit the booking was actually sold in: calling a 3-hour let
    # "1 night at ₦5,000/night" is how the two trades got confused to begin with.
    hours = duration_hours if duration_hours is not None else db.get_room_type_hours(room_type)
    if hours is not None and hours < 24:
        unit = f"₦{price:,.2f} × {nights} let(s) of {hours:g}h"
    else:
        unit = f"₦{price:,.2f}/night × {nights} night(s)"

    date_note = f" _(recorded for {timestamp})_" if timestamp else ""
    return True, (
        f"✅ Room booking recorded.{date_note}\n"
        f"Type: *{room_type.title()}* | Qty: {qty} | {unit}\n"
        f"Total Revenue: *₦{total:,.2f}*"
    ), room_id


# ── Room stay length (hourly vs nightly types) ────────────────────────

def process_set_room_duration(room_type: str, hours: float) -> tuple[bool, str]:
    """Declare how long one let of a room type holds the room.

    This is what tells occupancy and ADR apart for a hotel that also sells by
    the hour. Until a type is declared hourly its lets are read as nights: a
    room let three times a day reports 300% occupancy, and its ₦3,000 rate is
    averaged into the overnight ADR. Declaring it fixes the *history* too —
    bookings defer to their type unless a negotiated length was stored on the
    row — so this is worth setting even years in.
    """
    name = room_type.strip()
    if not name:
        return False, "❌ Room type cannot be empty."
    if hours <= 0:
        return False, "❌ Stay length must be more than 0 hours."
    if hours > 24:
        return False, (
            "❌ A stay longer than 24 hours is measured in nights, not hours.\n"
            "Book it with `/room <type> <qty> <nights>` instead."
        )

    db.set_room_type_hours(name, hours)
    if hours == 24:
        return True, (
            f"✅ *{name.title()}* set back to a full night.\n"
            "Its bookings count as room-nights and its rate as ADR again."
        )
    return True, (
        f"✅ *{name.title()}* is an hourly type — {hours:g} hours per let.\n"
        "Its bookings now count as *lets*, not room-nights, and its rate is "
        "reported per let instead of as ADR.\n"
        "_Applies to past bookings too — see_ `/roomstats`."
    )


# ── Expense ───────────────────────────────────────────────────────────

VALID_ACCOUNTS = ("rooms", "bar")

# Expenses have a third home: costs that serve the whole business and belong to
# neither department. Draws and debtors deliberately do NOT — an owner draw
# comes out of a real account, and nobody owes money to "overhead".
VALID_EXPENSE_ACCOUNTS = ("rooms", "bar", "overhead")


# Categories that describe the owner taking money out, not a business cost.
# These don't reduce profit — they reduce cash and owner's equity — so they
# must be logged with /draw, never as an expense.
_DRAW_LIKE_CATEGORIES = {
    "draw", "draws", "drawing", "drawings", "withdraw", "withdrawal",
    "withdrawals", "owner", "owners", "personal", "dividend", "dividends",
}


def capital_threshold() -> float:
    """Minimum spend that can be capital. Naira and policy, so it is a setting."""
    try:
        return float(db.get_setting("capital_threshold", "") or metrics.CAPITAL_THRESHOLD)
    except (TypeError, ValueError):
        return metrics.CAPITAL_THRESHOLD


def process_expense(account: str, category: str, amount: float, description: str = "",
                    timestamp: str | None = None, recorded_by: str = "",
                    expense_class: str = "operating", needs_review: bool = False,
                    obligation_id: int | None = None) -> tuple[bool, str]:
    """Record an expense on both axes: which account, and what kind of spend.

    ``expense_class`` decides whether the row reaches the P&L at all. Capital
    and inventory leave the bank without touching profit, so they are excluded
    from every margin but still subtracted from the cash estimate.
    """
    if account.lower() not in VALID_EXPENSE_ACCOUNTS:
        return False, f"❌ Account must be *rooms*, *bar* or *overhead*. Got: `{account}`"
    if amount <= 0:
        return False, "❌ Amount must be a positive number."

    cls = (expense_class or "operating").strip().lower()
    if cls not in metrics.EXPENSE_CLASSES:
        listed = ", ".join(f"*{c}*" for c in metrics.EXPENSE_CLASSES)
        return False, f"❌ Class must be one of {listed}. Got: `{expense_class}`"

    # The capital test, enforced rather than trusted: under the threshold it is
    # expensed even when it is technically an asset, so the classification can
    # never quietly pull a small purchase out of the P&L.
    threshold = capital_threshold()
    if cls == metrics.CAPITAL_CLASS and amount < threshold:
        return False, (
            f"❌ ₦{amount:,.0f} is under the ₦{threshold:,.0f} capital threshold.\n"
            "Record it as *operating* — under the threshold you expense it even "
            "when it is technically an asset."
        )
    if cls == "inventory":
        return False, (
            "❌ Stock for resale isn't an expense — it's cash converting to stock.\n"
            "Use `/restock` (paid now) or `/restock_credit` (on supplier credit)."
        )

    if category.strip().lower() in _DRAW_LIKE_CATEGORIES:
        return False, (
            f"❌ *{category.title()}* is an owner withdrawal, not a business expense.\n"
            "Money you take out doesn't reduce profit — log it with:\n"
            f"`/draw {amount:.0f}{(' ' + description) if description else ''}`\n"
            "See /position for how draws, profit and cash are tracked separately."
        )

    db.record_expense(account.strip(), category.strip(), amount, description.strip(),
                      timestamp=timestamp, recorded_by=recorded_by,
                      expense_class=cls, needs_review=needs_review,
                      obligation_id=obligation_id)
    date_note = f"\nDate: {timestamp}" if timestamp else ""
    if cls == metrics.CAPITAL_CLASS:
        class_note = ("\n🏗 *Capital* — cash out, but not a cost. It stays out of "
                      "profit and margins, and shows in its own report section.")
    elif cls == metrics.RESERVE_CLASS:
        class_note = ("\n🔁 *Periodic* — drawn from the reserve, not charged to "
                      "this month. The monthly share was already in the P&L.")
    else:
        class_note = ""
    review_note = "\n🔎 _Flagged for month-end review._" if needs_review else ""
    return True, (
        f"✅ Expense recorded.\n"
        f"Account: *{account.title()}* | Category: *{category.title()}* | Amount: ₦{amount:,.2f}"
        + (f"\nNote: {description}" if description else "")
        + date_note + class_note + review_note
    )


def process_reclassify_expense(expense_id: int, account: str | None = None,
                               category: str | None = None,
                               expense_class: str | None = None,
                               needs_review: bool | None = None) -> tuple[bool, str]:
    """Correct one expense's classification. Never touches the amount or date."""
    row = db.get_expense(expense_id)
    if row is None:
        return False, f"❌ No expense with ID `{expense_id}`."

    if account is not None and account.lower() not in VALID_EXPENSE_ACCOUNTS:
        return False, f"❌ Account must be *rooms*, *bar* or *overhead*. Got: `{account}`"
    if expense_class is not None:
        cls = expense_class.strip().lower()
        if cls not in metrics.EXPENSE_CLASSES:
            return False, f"❌ Unknown class `{expense_class}`."
        if cls == metrics.CAPITAL_CLASS and float(row["amount"]) < capital_threshold():
            return False, (
                f"❌ ₦{float(row['amount']):,.0f} is under the "
                f"₦{capital_threshold():,.0f} capital threshold — keep it operating."
            )

    if not db.reclassify_expense(expense_id, account=account, category=category,
                                 expense_class=expense_class, needs_review=needs_review):
        return False, "❌ Nothing to change."

    after = db.get_expense(expense_id)
    return True, (
        f"✅ Entry `{expense_id}` reclassified.\n"
        f"*{str(after['account']).title()}* / *{str(after['category']).title()}* · "
        f"{str(after.get('expense_class') or 'operating').title()} · "
        f"₦{float(after['amount']):,.2f}\n"
        "_Amount and date untouched._"
    )


# ── Periodic obligations (the accrual register) ───────────────────────

def process_add_obligation(name: str, expected_amount: float, months: int,
                           account: str = "rooms", category: str = "maintenance",
                           recorded_by: str = "") -> tuple[bool, str]:
    """Register a bill that recurs every few months, so it can be accrued.

    Without a register there is nothing to accrue against: you cannot charge a
    monthly share of a cost the books have never been told to expect. Accrual
    starts today, never earlier — adding an obligation now must not retroactively
    rewrite months that have already been reported.
    """
    if not name.strip():
        return False, "❌ Give the bill a name (e.g. soakaway evacuation)."
    if expected_amount <= 0:
        return False, "❌ Expected amount must be a positive number."
    if months < 2:
        return False, ("❌ A bill that recurs monthly (or more often) is just an "
                       "operating expense — record it with 💸 Expense.")
    if months > 60:
        return False, "❌ Spread it over 60 months or fewer."
    if account.lower() not in VALID_EXPENSE_ACCOUNTS:
        return False, f"❌ Account must be *rooms*, *bar* or *overhead*. Got: `{account}`"

    ob_id = db.add_obligation(name, expected_amount, months, account=account,
                              category=category, recorded_by=recorded_by)
    share = expected_amount / months
    return True, (
        f"✅ *{name.strip().title()}* registered.\n"
        f"{_naira(expected_amount)} ÷ {months} months = *{_naira(share)}/month*\n"
        f"Charged to *{account.title()}* / {category.title()} from this month on.\n"
        "_When the bill actually arrives, record it as a *periodic* expense — "
        "it draws from the reserve instead of hitting profit._"
    )


def process_retire_obligation(obligation_id: int) -> tuple[bool, str]:
    """Stop a bill accruing. Never deletes — past accruals are past profit."""
    if not db.set_obligation_active(obligation_id, False):
        return False, f"❌ No periodic bill with ID `{obligation_id}`."
    return True, (
        f"✅ Bill `{obligation_id}` retired — it stops accruing from now.\n"
        "_Everything it already set aside stays in the reserve and in the months "
        "that carried it._"
    )


def _naira(amount: float) -> str:
    return f"₦{amount:,.2f}"


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


# ── Stocktake ─────────────────────────────────────────────────────────

def process_stock_count(drink: str, counted: int, note: str = "", recorded_by: str = "",
                        timestamp: str | None = None,
                        location: str = "bar") -> tuple[bool, str]:
    """Record a physical bar count, log the variance, and true the books up.

    Variance is the only signal the system has for breakage, unrecorded sales
    or theft — every other number is derived from what was keyed in.
    """
    if counted < 0:
        return False, "❌ Counted units cannot be negative."

    name = drink.strip().lower()
    existing = db.get_drink(name)
    if existing is None:
        return False, f"❌ '{drink}' not found in inventory. Restock it first with /restock."

    loc = "store" if str(location).strip().lower() == "store" else "bar"
    expected = int(existing["store_stock" if loc == "store" else "current_stock"])
    cost = float(existing.get("cost_price") or 0)
    result = db.record_stock_count(
        name, expected=expected, counted=counted, cost_price=cost,
        note=note, recorded_by=recorded_by, timestamp=timestamp, location=loc,
    )

    variance, value = result["variance"], result["value"]
    where = loc.title()
    pct = round(variance / expected * 100, 1) if expected else 0.0
    header = (f"✅ Counted *{drink.title()}* in the *{where}*: {counted} units "
              f"(books said {expected}).")
    if variance == 0:
        return True, f"{header}\n🎯 Exact match — no variance."
    if variance < 0:
        mark, _note = metrics.variance_status(pct)
        return True, (
            f"{header}\n"
            f"{mark} *Short by {abs(variance)} units* ({pct:+.1f}%) — ₦{abs(value):,.2f} at cost.\n"
            f"{where} stock corrected to {counted}. Check breakage, unrecorded sales or theft."
        )
    # A surplus is never a clean count: it is the same leak from the other side.
    return True, (
        f"{header}\n"
        f"🔴 *Over by {variance} units* ({pct:+.1f}%) — ₦{value:,.2f} at cost.\n"
        f"{where} stock corrected to {counted}.\n"
        "_Not good news — usually a sale that was never rung up, or a purchase "
        "logged twice. Investigate it like a shortage._"
    )


# ── Room audit ────────────────────────────────────────────────────────

def process_room_audit(audit_date: str, rooms_total: int, nights_logged: int,
                       nights_actual: int, rate_variance: float = 0.0,
                       variance_count: int = 0, recorded_by: str = "") -> tuple[bool, str]:
    """Store one audited day. Capture rate is only meaningful as a trend."""
    if nights_actual < 0 or nights_logged < 0:
        return False, "❌ Night counts cannot be negative."
    if nights_actual > rooms_total > 0:
        return False, (
            f"❌ {nights_actual} nights occupied but the hotel has {rooms_total} rooms.\n"
            "Check the figure, or update the room count with `/setrooms`."
        )

    db.record_room_audit(
        audit_date, rooms_total, nights_logged, nights_actual,
        rate_variance=rate_variance, variance_count=variance_count,
        recorded_by=recorded_by,
    )
    missing = max(nights_actual - nights_logged, 0)
    if missing:
        return True, (f"✅ {audit_date} recorded — *{missing} unlogged "
                      f"{'night' if missing == 1 else 'nights'}* found.")
    return True, f"✅ {audit_date} recorded — every night was in the system."


# ── Turnaways (refused bookings) ──────────────────────────────────────

def process_turnaway(quantity: int = 1, room_type: str = "", reason: str = "",
                     recorded_by: str = "", timestamp: str | None = None) -> tuple[bool, str]:
    """Record guests turned away because nothing suitable was free.

    The books can prove a night was full. They can never show how much demand
    kept arriving after it filled, and that difference is the whole argument
    for a rate rise — 100% occupancy with nobody refused means the rate is
    about right, 100% with twenty refusals means it is too low.
    """
    if quantity <= 0:
        return False, "❌ Number turned away must be at least 1."
    if quantity > 500:
        return False, f"❌ {quantity} turned away in one entry looks like a typo."

    db.record_turnaway(
        room_type=room_type, quantity=quantity, reason=reason,
        recorded_by=recorded_by, timestamp=timestamp,
    )

    who = f"{quantity} turned away"
    if room_type.strip():
        who += f" wanting *{room_type.strip().title()}*"
    when = f" on {timestamp}" if timestamp else ""
    tail = f"\n_Reason: {reason.strip()}_" if reason.strip() else ""
    return True, (
        f"📝 Logged: {who}{when}.{tail}\n"
        "Counts toward the demand you could not serve — see `/roomstats dow`."
    )


# ── Supplier credit ───────────────────────────────────────────────────

def process_restock_credit(drink: str, qty: int, cost_price: float, supplier: str,
                           due_date: str | None = None, recorded_by: str = "",
                           timestamp: str | None = None) -> tuple[bool, str]:
    """Receive stock on supplier credit: stock in now, cash out later.

    No expense row is written here — the money hasn't moved. /pay_supplier
    records the cash when the invoice is settled.
    """
    if qty <= 0:
        return False, "❌ Quantity must be a positive integer."
    if cost_price <= 0:
        return False, "❌ Cost price must be a positive number."
    if not supplier.strip():
        return False, "❌ Supplier name is required."
    if due_date and not parse_date(due_date):
        return False, "❌ Due date must be in YYYY-MM-DD format."

    result: StockResult = inv.restock_drink(drink.strip(), qty, cost_price)
    if not result.ok:
        return False, result.message

    total = round(qty * cost_price, 2)
    payable_id = db.record_payable(
        supplier=supplier, amount=total, drink_name=drink.strip(), quantity=qty,
        due_date=due_date or "",
        description=f"{drink.strip().title()} ×{qty} @ ₦{cost_price:,.2f}",
        recorded_by=recorded_by, timestamp=timestamp,
    )
    due_note = f"\n📅 Due: {due_date}" if due_date else ""
    return True, (
        f"{result.message}\n\n"
        f"🧾 *On credit from {supplier.strip().title()}* — ₦{total:,.2f} owed (invoice #{payable_id}).{due_note}\n"
        f"_No cash has left yet. Settle with_ `/pay_supplier {payable_id}`_._"
    )


def process_pay_supplier(payable_id: int, amount: float | None = None,
                         paid_by: str = "") -> tuple[bool, str]:
    if amount is not None and amount <= 0:
        return False, "❌ Payment amount must be a positive number."

    result = db.pay_supplier(payable_id, amount=amount, paid_by=paid_by)
    if result is None:
        return False, f"❌ No outstanding supplier invoice with ID `{payable_id}`."
    if result.get("error") == "overpayment":
        return False, (
            f"❌ That's more than is owed on invoice #{payable_id}.\n"
            f"Remaining: ₦{result['remaining']:,.2f}"
        )

    supplier = str(result["supplier"]).title()
    if result["is_fully_paid"]:
        return True, (
            f"✅ Invoice #{payable_id} to *{supplier}* fully settled.\n"
            f"  Paid now: ₦{result['amount_paid_now']:,.2f}\n"
            f"  Total: ₦{result['total_paid']:,.2f}"
        )
    return True, (
        f"✅ Part payment to *{supplier}* on invoice #{payable_id}.\n"
        f"  Paid now: ₦{result['amount_paid_now']:,.2f}\n"
        f"  Still owed: ₦{result['remaining']:,.2f}"
    )


# ── Room count (occupancy basis) ──────────────────────────────────────

def process_set_rooms(total: int) -> tuple[bool, str]:
    """Record how many lettable rooms the hotel has.

    Without it occupancy and RevPAR are undefined — there's no denominator.
    """
    if total <= 0:
        return False, "❌ Room count must be a positive whole number."
    db.set_setting(reports.TOTAL_ROOMS_KEY, str(int(total)))
    msg = (
        f"✅ Hotel room count set to *{int(total)}*.\n"
        f"Occupancy, ADR and RevPAR now show in /report and /roomstats."
    )
    by_type = db.get_all_room_type_counts()
    if by_type:
        listed = sum(by_type.values())
        if listed != int(total):
            msg += (
                f"\n\n⚠️ _Your per-type counts add up to {listed}, not {int(total)}._\n"
                "_The hotel-wide figures use this total; per-type RevPAR uses each_\n"
                "_type's own count._"
            )
    return True, msg


def process_set_room_type_count(room_type: str, count: int) -> tuple[bool, str]:
    """Record how many rooms of one type exist — the per-type RevPAR denominator.

    Kept separate from the hotel total: RevPAR by type divides each type's
    revenue by its *own* room count, which is the only way a high-rate,
    low-volume category shows its real yield.
    """
    room_type = room_type.strip().lower()
    if not room_type:
        return False, "❌ Room type is required."
    if count <= 0:
        return False, "❌ Room count must be a positive whole number."

    db.set_room_type_count(room_type, count)
    counts = db.get_all_room_type_counts()
    listed = sum(counts.values())
    total = 0
    try:
        total = int(float(db.get_setting(reports.TOTAL_ROOMS_KEY, "0") or 0))
    except (TypeError, ValueError):
        pass

    lines = [
        f"✅ *{room_type.title()}*: {int(count)} rooms.",
        "",
        "*Rooms by type*",
    ]
    lines += [f"  • {t.title()}: {n}" for t, n in sorted(counts.items())]
    lines.append(f"  _Total listed: {listed}_")

    if not total:
        lines.append(
            f"\n_No hotel-wide total set — /roomstats will use {listed}._\n"
            "_Set it explicitly with_ `/setrooms <n>` _if some rooms aren't lettable._"
        )
    elif listed != total:
        lines.append(
            f"\n⚠️ _These add up to {listed}, but the hotel total is {total}._\n"
            "_Fine if some rooms are out of service — otherwise fix one of them._"
        )
    return True, "\n".join(lines)


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
    """Soft-void this user's most recent sale or room entry, if still in window."""
    entry = db.get_last_staff_entry(username)
    if entry is None:
        return False, (
            "❌ Nothing to undo.\n"
            "Either you have no recent entries, or the 2-minute window has passed."
        )
    return _reverse_entry(entry, username)


def process_undo_entry(entry_type: str, entry_id: int, username: str) -> tuple[bool, str]:
    """Soft-void one *specific* sale or room entry — what the inline button uses.

    The button carries the id of the entry it was posted under, so two entries
    in quick succession each undo themselves. Reversing "the latest entry"
    instead would let the second one swallow the first one's button.
    """
    entry = db.get_undoable_entry(entry_type, entry_id, username)
    if entry is None:
        return False, (
            "❌ Can't undo this one.\n"
            "It has already been undone, or the 2-minute window has passed. "
            "Ask an admin to remove it with /delete."
        )
    return _reverse_entry(entry, username)


def _reverse_entry(entry: dict, username: str) -> tuple[bool, str]:
    """Void one already-validated sale/room row and restore any stock behind it."""
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

def process_transfer(drink: str, qty: int, recorded_by: str = "", timestamp: str | None = None) -> tuple[bool, str]:
    if qty <= 0:
        return False, "❌ Quantity must be a positive integer."

    # `timestamp` backdates the audit row only. Store/bar counts are a live
    # snapshot, not a per-date ledger, so the units always move now.
    result: StockResult = inv.transfer_to_bar(drink.strip(), qty)
    if result.ok:
        db.record_transfer(drink.strip(), qty, recorded_by=recorded_by, timestamp=timestamp)
    msg = result.message
    if result.ok and timestamp:
        msg += f"\nDate: {timestamp}"
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
