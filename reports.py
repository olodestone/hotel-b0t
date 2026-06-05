"""
reports.py — Financial calculations and Telegram-formatted report strings.

All monetary values are in ₦ (Naira) — change the symbol in _fmt() if needed.
"""
from __future__ import annotations

from datetime import datetime, date
from math import ceil
from typing import Any

import database as db
import inventory as inv
from config import (
    HOTEL_NAME,
    ALLOC_BUFFER_DEFAULT, ALLOC_RESTOCK_DEFAULT,
    ALLOC_DRAW_DEFAULT, ALLOC_REINVEST_DEFAULT, ALLOC_FLOAT_DEFAULT,
    PIT_LOW_RATE, PIT_HIGH_RATE,
)

_SEP = "─" * 30


def _fmt(amount: float) -> str:
    return f"₦{amount:,.0f}"


def _esc(text: str) -> str:
    """Escape the MarkdownV2 *markup* delimiters in a single dynamic field.

    Only the characters that escape_markdown_v2() treats as intentional markup
    (``_ * `` and ``[``) need pre-escaping here, so that literal underscores in
    a user-typed description/name are not mistaken for italics. Every other V2
    special character (``. - ( ) ! + = | { } # > ~``) is escaped automatically
    by escape_markdown_v2() at send time, so it must NOT be doubled here.
    """
    for ch in ("_", "*", "`", "["):
        text = text.replace(ch, f"\\{ch}")
    return text


# MarkdownV2 characters that must be backslash-escaped outside of code blocks.
_MD2_SPECIAL = set("_*[]()~`>#+-=|{}.!")


def escape_markdown_v2(s: str) -> str:
    """Escape a fully-assembled message for Telegram MarkdownV2.

    This is a *markup-aware* escaper applied once to the final string at send
    time. Bare ``*``/``_``/`` ` `` (and ``` ``` ``` fences) are left intact as
    intentional bold/italic/code markup; every other special character is
    backslash-escaped so literal dates (``2026-05-05``), amounts, parentheses
    and punctuation render verbatim. Content inside inline code spans and
    fenced code blocks is passed through untouched (Telegram only treats
    backtick/backslash specially there). Sequences already escaped by _esc()
    (e.g. ``\\_``) are preserved as-is rather than double-escaped.
    """
    out: list[str] = []
    i, n = 0, len(s)
    in_block = False   # inside a ``` fenced block
    in_span = False    # inside an inline `code` span
    while i < n:
        # Fenced code block delimiter (only outside an inline span).
        if not in_span and s.startswith("```", i):
            in_block = not in_block
            out.append("```")
            i += 3
            continue
        ch = s[i]
        if in_block:
            out.append("\\\\" if ch == "\\" else ch)
            i += 1
            continue
        if ch == "`":
            in_span = not in_span
            out.append("`")
            i += 1
            continue
        if in_span:
            out.append("\\\\" if ch == "\\" else ch)
            i += 1
            continue
        # Outside any code context.
        if ch == "\\":
            # Preserve an already-escaped special pair from _esc(); otherwise
            # escape a literal backslash.
            if i + 1 < n and s[i + 1] in _MD2_SPECIAL:
                out.append(s[i:i + 2])
                i += 2
            else:
                out.append("\\\\")
                i += 1
            continue
        if ch in "*_":
            out.append(ch)            # intentional bold/italic markup
            i += 1
            continue
        if ch in _MD2_SPECIAL:
            out.append("\\")
            out.append(ch)
            i += 1
            continue
        out.append(ch)
        i += 1
    return "".join(out)


# ── Core aggregations ─────────────────────────────────────────────────

def _sum_revenue(rows: list[dict], key: str = "total_revenue") -> float:
    return sum(float(r[key]) for r in rows)


def _parse_ts(raw) -> datetime | None:
    """Parse a timestamp value that may be a string, pandas Timestamp, or have microseconds."""
    try:
        ts_str = str(raw).split(".")[0]  # strip microseconds if present
        return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None


def _filter_by_date(rows: list[dict], target: date) -> list[dict]:
    result = []
    for r in rows:
        try:
            dt = _parse_ts(r["timestamp"])
            if dt and dt.date() == target:
                result.append(r)
        except KeyError:
            pass
    return result


def _filter_by_month(rows: list[dict], year: int, month: int) -> list[dict]:
    result = []
    for r in rows:
        try:
            dt = _parse_ts(r["timestamp"])
            if dt and dt.year == year and dt.month == month:
                result.append(r)
        except KeyError:
            pass
    return result


def _period_label(for_date: date | None, for_month: tuple[int, int] | None, all_time: bool) -> str:
    now = datetime.now()
    if for_date:
        return for_date.strftime("%d %b %Y")
    if all_time:
        return "ALL-TIME"
    year, month = for_month if for_month else (now.year, now.month)
    label = datetime(year, month, 1).strftime("%B %Y")
    return f"{label} (current month)" if (year, month) == (now.year, now.month) else label


def _apply_filter(rows: list[dict], for_date: date | None, for_month: tuple[int, int] | None, all_time: bool) -> list[dict]:
    now = datetime.now()
    if for_date:
        return _filter_by_date(rows, for_date)
    if all_time:
        return rows
    year, month = for_month if for_month else (now.year, now.month)
    return _filter_by_month(rows, year, month)


def _split_salary(expense_rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """Split expense rows into (salary_rows, other_rows)."""
    salary = [r for r in expense_rows if r.get("category", "").lower() == "salary"]
    other  = [r for r in expense_rows if r.get("category", "").lower() != "salary"]
    return salary, other


def _active(rows: list[dict]) -> list[dict]:
    """Exclude soft-voided/deleted rows from financial aggregations."""
    return [r for r in rows if not r.get("deleted_at")]


# Expense categories that are cash/stock movements, NOT P&L operating costs.
# Buying inventory (restock) converts cash into a stock asset; that cost only
# reaches the P&L as cost-of-goods-sold when the drink is actually sold.
# Counting the restock purchase as an expense too would double-count it
# against profit, so it is stripped out of every profit calculation below.
NON_PNL_CATEGORIES = {"restock"}


def _operating_expenses(rows: list[dict]) -> list[dict]:
    """Keep only P&L operating-expense rows (excludes restock / stock purchases)."""
    return [r for r in rows if str(r.get("category", "")).lower() not in NON_PNL_CATEGORIES]


def _restock_spend(rows: list[dict]) -> float:
    """Total inventory-purchase (restock) cash outflow — a cash movement, not a P&L cost."""
    return round(
        sum(float(r["amount"]) for r in rows
            if str(r.get("category", "")).lower() == "restock"),
        2,
    )


def _cost_of_drinks_sold(sales_rows: list[dict]) -> float:
    """Match each sale to its current cost price from inventory."""
    total = 0.0
    inventory_rows = {r["drink_name"].lower(): float(r["cost_price"]) for r in db.read_all("inventory")}
    for row in sales_rows:
        name = row["drink_name"].lower()
        qty = int(row["quantity"])
        cost = inventory_rows.get(name, 0.0)
        total += qty * cost
    return round(total, 2)


# ── Full financial report ─────────────────────────────────────────────

def generate_full_report(
    for_date: date | None = None,
    for_month: tuple[int, int] | None = None,
    all_time: bool = False,
    staff_view: bool = False,
) -> str:
    sales_rows = _active(db.read_all("sales"))
    room_rows = _active(db.read_all("rooms"))
    expense_rows = _active(db.read_all("expenses"))
    debtor_rows = db.read_all("debtors")

    sales_rows = _apply_filter(sales_rows, for_date, for_month, all_time)
    room_rows = _apply_filter(room_rows, for_date, for_month, all_time)
    expense_rows = _apply_filter(expense_rows, for_date, for_month, all_time)
    label = _period_label(for_date, for_month, all_time)

    # Only P&L operating expenses count toward profit — restock (inventory
    # purchase) is a cash/stock movement and is reported separately below.
    bar_expenses = _operating_expenses([r for r in expense_rows if r.get("account", "bar") == "bar"])
    room_expenses = _operating_expenses([r for r in expense_rows if r.get("account", "rooms") == "rooms"])
    restock_total = _restock_spend(expense_rows)

    drink_revenue = _sum_revenue(sales_rows)
    room_revenue = _sum_revenue(room_rows)
    total_revenue = drink_revenue + room_revenue

    if staff_view:
        lines = [
            f"🏨 *{HOTEL_NAME} — Revenue Summary*",
            f"📅 Period: {label}",
            _SEP,
            f"🍺 Bar Sales:      {_fmt(drink_revenue)}  ({len(sales_rows)} transactions)",
            f"🛏 Room Bookings:  {_fmt(room_revenue)}  ({len(room_rows)} bookings)",
            _SEP,
            f"*Total Revenue:   {_fmt(total_revenue)}*",
            _SEP,
            f"_Generated {datetime.now().strftime('%d %b %Y %H:%M')}_",
        ]
        return "\n".join(lines)

    cost_of_drinks = _cost_of_drinks_sold(sales_rows)
    bar_expense_total = _sum_revenue(bar_expenses, key="amount")
    bar_profit = drink_revenue - cost_of_drinks - bar_expense_total

    room_expense_total = _sum_revenue(room_expenses, key="amount")
    room_profit = room_revenue - room_expense_total
    total_outgoings = cost_of_drinks + bar_expense_total + room_expense_total
    net_profit = total_revenue - total_outgoings

    bar_emoji = "📈" if bar_profit >= 0 else "📉"
    room_emoji = "📈" if room_profit >= 0 else "📉"
    net_emoji = "📈" if net_profit >= 0 else "📉"

    bar_salary, bar_other = _split_salary(bar_expenses)
    room_salary, room_other = _split_salary(room_expenses)
    bar_salary_total  = sum(float(r["amount"]) for r in bar_salary)
    room_salary_total = sum(float(r["amount"]) for r in room_salary)

    lines = [
        f"🏨 *{HOTEL_NAME} — Financial Report*",
        f"📅 Period: {label}",
        _SEP,
        "🍺 *BAR ACCOUNT*",
        f"  Revenue: {_fmt(drink_revenue)}",
        f"  Cost of Stock Sold: {_fmt(cost_of_drinks)}",
        f"  Salaries: {_fmt(bar_salary_total)}",
        f"  Other Expenses: {_fmt(_sum_revenue(bar_other, key='amount'))}",
        f"  {bar_emoji} *Profit: {_fmt(bar_profit)}*",
    ]

    if bar_other:
        cat_totals: dict[str, float] = {}
        for r in bar_other:
            cat = r["category"].title()
            cat_totals[cat] = cat_totals.get(cat, 0.0) + float(r["amount"])
        lines.append("  _Other breakdown:_")
        for cat, amt in sorted(cat_totals.items()):
            lines.append(f"    • {_esc(cat)}: {_fmt(amt)}")

    lines += [
        _SEP,
        "🛏 *ROOMS ACCOUNT*",
        f"  Revenue: {_fmt(room_revenue)}",
        f"  Salaries: {_fmt(room_salary_total)}",
        f"  Other Expenses: {_fmt(_sum_revenue(room_other, key='amount'))}",
        f"  {room_emoji} *Profit: {_fmt(room_profit)}*",
    ]

    if room_other:
        cat_totals = {}
        for r in room_other:
            cat = r["category"].title()
            cat_totals[cat] = cat_totals.get(cat, 0.0) + float(r["amount"])
        lines.append("  _Other breakdown:_")
        for cat, amt in sorted(cat_totals.items()):
            lines.append(f"    • {_esc(cat)}: {_fmt(amt)}")

    lines += [
        _SEP,
        "📊 *COMBINED*",
        f"  Total Revenue:   {_fmt(total_revenue)}",
        f"  Total Outgoings: {_fmt(total_outgoings)}",
        f"  {net_emoji} *Net Profit:    {_fmt(net_profit)}*",
        _SEP,
    ]

    if restock_total > 0:
        lines += [
            f"📦 Stock purchased: {_fmt(restock_total)}",
            "  _Inventory buy — cash → stock, not a profit cost. See /position._",
            _SEP,
        ]

    outstanding = [r for r in debtor_rows if r["status"] == "outstanding"]
    if outstanding:
        def _rem(r: dict) -> float:
            return round(float(r["amount"]) - float(r.get("amount_paid") or 0), 2)
        bar_debtors = [r for r in outstanding if r["account"] == "bar"]
        room_debtors = [r for r in outstanding if r["account"] == "rooms"]
        bar_owed = sum(_rem(r) for r in bar_debtors)
        room_owed = sum(_rem(r) for r in room_debtors)
        lines.append("💳 *OUTSTANDING DEBTORS*")
        if bar_debtors:
            lines.append(f"  🍺 Bar ({len(bar_debtors)}):    {_fmt(bar_owed)}")
        if room_debtors:
            lines.append(f"  🛏 Rooms ({len(room_debtors)}):  {_fmt(room_owed)}")
        lines.append(f"  Total Owed:    {_fmt(bar_owed + room_owed)}")
        lines.append(_SEP)

    lines.append(f"_Generated {datetime.now().strftime('%d %b %Y %H:%M')}_")
    return "\n".join(lines)


# ── Sales report ──────────────────────────────────────────────────────

def generate_sales_report(
    for_date: date | None = None,
    for_month: tuple[int, int] | None = None,
    all_time: bool = False,
) -> str:
    """Drink-level sales breakdown with cost and profit (admin-only)."""
    sales_rows = _active(db.read_all("sales"))
    sales_rows = _apply_filter(sales_rows, for_date, for_month, all_time)
    label = _period_label(for_date, for_month, all_time)

    if not sales_rows:
        return f"🍺 *Sales Report — {label}*\n\nNo sales recorded for this period."

    # Aggregate by drink
    totals: dict[str, dict] = {}
    inventory_costs = {
        r["drink_name"].lower(): float(r["cost_price"]) for r in db.read_all("inventory")
    }
    for r in sales_rows:
        name = r["drink_name"].lower()
        qty = int(r["quantity"])
        rev = float(r["total_revenue"])
        cost = inventory_costs.get(name, 0.0) * qty
        if name not in totals:
            totals[name] = {"qty": 0, "revenue": 0.0, "cost": 0.0}
        totals[name]["qty"] += qty
        totals[name]["revenue"] += rev
        totals[name]["cost"] += cost

    col_drink = max(len(n.title()) for n in totals) + 1
    col_drink = max(col_drink, 10)

    header = f"{'Drink':<{col_drink}} {'Qty':>5}  {'Revenue':>12}  {'Cost':>12}  {'Profit':>12}"
    divider = "-" * len(header)

    rows_out = []
    t_qty, t_rev, t_cost = 0, 0.0, 0.0
    for name in sorted(totals):
        d = totals[name]
        profit = d["revenue"] - d["cost"]
        rows_out.append(
            f"{name.title():<{col_drink}} {d['qty']:>5}  {_fmt(d['revenue']):>12}  {_fmt(d['cost']):>12}  {_fmt(profit):>12}"
        )
        t_qty += d["qty"]
        t_rev += d["revenue"]
        t_cost += d["cost"]

    t_profit = t_rev - t_cost
    total_line = f"{'TOTAL':<{col_drink}} {t_qty:>5}  {_fmt(t_rev):>12}  {_fmt(t_cost):>12}  {_fmt(t_profit):>12}"

    lines = [
        f"🍺 *Sales Report — {label}*",
        f"Transactions: {len(sales_rows)}",
        "```",
        header,
        divider,
        *rows_out,
        divider,
        total_line,
        "```",
        f"_Generated {datetime.now().strftime('%d %b %Y %H:%M')}_",
    ]
    return "\n".join(lines)


# ── Expense report ────────────────────────────────────────────────────

def generate_expense_report(
    for_date: date | None = None,
    for_month: tuple[int, int] | None = None,
    all_time: bool = False,
) -> str:
    """Expense breakdown by account and category."""
    expense_rows = _active(db.read_all("expenses"))
    expense_rows = _apply_filter(expense_rows, for_date, for_month, all_time)
    label = _period_label(for_date, for_month, all_time)

    if not expense_rows:
        return f"💸 *Expense Report — {label}*\n\nNo expenses recorded for this period."

    # Restock (inventory purchases) is a cash → stock movement, not an operating
    # expense — shown separately so it never inflates the expense total.
    restock_total = _restock_spend(expense_rows)
    expense_rows = _operating_expenses(expense_rows)
    bar_expenses = [r for r in expense_rows if r.get("account") == "bar"]
    room_expenses = [r for r in expense_rows if r.get("account") == "rooms"]

    def _section(rows: list[dict], title: str) -> list[str]:
        if not rows:
            return []
        salary_rows, other_rows = _split_salary(rows)
        salary_total = sum(float(r["amount"]) for r in salary_rows)
        out = [title]
        cat_total = 0.0

        def _note(e: dict) -> str:
            desc = str(e.get("description") or "").strip()
            return f" _{_esc(desc)}_" if desc else ""

        # Salary block first
        if salary_rows:
            out.append(f"  👤 *Salary* — {_fmt(salary_total)}")
            for e in salary_rows:
                ts = e.get("timestamp", "")[:10]
                out.append(f"    `[{e['id']}]` {ts}  {_fmt(float(e['amount']))}{_note(e)}")
            cat_total += salary_total

        # Other expenses grouped by category
        cat_rows: dict[str, list[dict]] = {}
        for r in other_rows:
            cat = r["category"].title()
            cat_rows.setdefault(cat, []).append(r)
        for cat in sorted(cat_rows):
            entries = cat_rows[cat]
            cat_sum = sum(float(e["amount"]) for e in entries)
            cat_total += cat_sum
            out.append(f"  *{_esc(cat)}* — {_fmt(cat_sum)}")
            for e in entries:
                ts = e.get("timestamp", "")[:10]
                out.append(f"    `[{e['id']}]` {ts}  {_fmt(float(e['amount']))}{_note(e)}")

        out.append(f"  *Subtotal: {_fmt(cat_total)}*")
        return out

    bar_section = _section(bar_expenses, "🍺 *BAR EXPENSES*")
    room_section = _section(room_expenses, "🛏 *ROOMS EXPENSES*")
    grand_total = sum(float(r["amount"]) for r in expense_rows)

    lines = [
        f"💸 *Expense Report — {label}*",
        _SEP,
        *bar_section,
    ]
    if bar_section and room_section:
        lines.append(_SEP)
    lines += [
        *room_section,
        _SEP,
        f"*Grand Total: {_fmt(grand_total)}*",
    ]
    if restock_total > 0:
        lines.append(f"📦 Stock purchased: {_fmt(restock_total)} _(inventory buy — not an operating expense)_")
    lines.append(f"_Generated {datetime.now().strftime('%d %b %Y %H:%M')}_")
    return "\n".join(lines)


# ── Staff report ──────────────────────────────────────────────────────

def generate_staff_report(
    for_date: date | None = None,
    for_month: tuple[int, int] | None = None,
) -> str:
    """Sales breakdown by staff member who recorded the entry."""
    sales_rows = _active(db.read_all("sales"))
    room_rows = _active(db.read_all("rooms"))
    sales_rows = _apply_filter(sales_rows, for_date, for_month, False)
    room_rows = _apply_filter(room_rows, for_date, for_month, False)
    label = _period_label(for_date, for_month, False)

    # Aggregate drink sales by recorder
    staff: dict[str, dict] = {}
    for r in sales_rows:
        name = (r.get("recorded_by") or "Unknown").strip() or "Unknown"
        if name not in staff:
            staff[name] = {"drink_txns": 0, "drink_revenue": 0.0, "room_txns": 0, "room_revenue": 0.0}
        staff[name]["drink_txns"] += 1
        staff[name]["drink_revenue"] += float(r["total_revenue"])

    for r in room_rows:
        name = (r.get("recorded_by") or "Unknown").strip() or "Unknown"
        if name not in staff:
            staff[name] = {"drink_txns": 0, "drink_revenue": 0.0, "room_txns": 0, "room_revenue": 0.0}
        staff[name]["room_txns"] += 1
        staff[name]["room_revenue"] += float(r["total_revenue"])

    if not staff:
        return f"👥 *Staff Report — {label}*\n\nNo activity recorded for this period."

    col_name = max(len(n) for n in staff) + 1
    col_name = max(col_name, 10)
    header = f"{'Staff':<{col_name}} {'DrinkTxn':>9}  {'DrinkRev':>13}  {'RoomTxn':>8}  {'RoomRev':>13}"
    divider = "-" * len(header)

    rows_out = []
    t_dtxn, t_drev, t_rtxn, t_rrev = 0, 0.0, 0, 0.0
    for name in sorted(staff):
        d = staff[name]
        rows_out.append(
            f"{name:<{col_name}} {d['drink_txns']:>9}  {_fmt(d['drink_revenue']):>13}  "
            f"{d['room_txns']:>8}  {_fmt(d['room_revenue']):>13}"
        )
        t_dtxn += d["drink_txns"]
        t_drev += d["drink_revenue"]
        t_rtxn += d["room_txns"]
        t_rrev += d["room_revenue"]

    total_line = (
        f"{'TOTAL':<{col_name}} {t_dtxn:>9}  {_fmt(t_drev):>13}  "
        f"{t_rtxn:>8}  {_fmt(t_rrev):>13}"
    )

    lines = [
        f"👥 *Staff Report — {label}*",
        "```",
        header,
        divider,
        *rows_out,
        divider,
        total_line,
        "```",
        f"_Generated {datetime.now().strftime('%d %b %Y %H:%M')}_",
    ]
    return "\n".join(lines)


# ── Daily summary ─────────────────────────────────────────────────────

def generate_daily_summary(target: date | None = None, staff_view: bool = False) -> str:
    """Compact one-screen overview of a single day's activity."""
    today = target or datetime.now().date()
    label = today.strftime("%A, %d %b %Y")

    sales_rows = _filter_by_date(_active(db.read_all("sales")), today)
    room_rows = _filter_by_date(_active(db.read_all("rooms")), today)

    # Top selling drinks today
    drink_qty: dict[str, int] = {}
    for r in sales_rows:
        name = r["drink_name"].title()
        drink_qty[name] = drink_qty.get(name, 0) + int(r["quantity"])
    top_drinks = sorted(drink_qty.items(), key=lambda x: x[1], reverse=True)[:3]

    # Stock alerts
    items = inv.get_inventory_summary()
    low_bar = [i["drink"] for i in items if i["is_low"]]
    empty_store = [i["drink"] for i in items if i["store_stock"] == 0]

    if staff_view:
        bar_rev  = _sum_revenue(sales_rows)
        room_rev = _sum_revenue(room_rows)
        outstanding = [r for r in db.read_all("debtors") if r["status"] == "outstanding"]
        owed = sum(float(r["amount"]) - float(r.get("amount_paid") or 0) for r in outstanding)

        lines = [
            f"📋 *Daily Summary — {label}*",
            _SEP,
            "💰 *Revenue*",
            f"  🍺 Bar:    {_fmt(bar_rev)}  ({len(sales_rows)} sales)",
            f"  🛏 Rooms:  {_fmt(room_rev)}  ({len(room_rows)} bookings)",
            f"  *Total:   {_fmt(bar_rev + room_rev)}*",
        ]
        if top_drinks:
            lines.append(_SEP)
            lines.append("🏆 *Top Sellers*")
            for drink, qty in top_drinks:
                lines.append(f"  • {_esc(drink)}: {qty} units")
        if outstanding:
            lines.append(_SEP)
            lines.append(f"💳 *Debtors:* {len(outstanding)} outstanding — {_fmt(owed)} owed")
        if low_bar:
            lines.append(_SEP)
            lines.append(f"⚠️ Low Bar Stock: {', '.join(_esc(d) for d in low_bar)}")
            lines.append("_Ask admin to transfer from store._")
        lines.append(f"\n_Generated {datetime.now().strftime('%d %b %Y %H:%M')}_")
        return "\n".join(lines)

    expense_rows = _filter_by_date(_active(db.read_all("expenses")), today)
    outstanding = [r for r in db.read_all("debtors") if r["status"] == "outstanding"]

    bar_rev = _sum_revenue(sales_rows)
    room_rev = _sum_revenue(room_rows)
    total_rev = bar_rev + room_rev

    # Restock (inventory purchase) is a cash/stock movement, not a P&L cost.
    op_expenses = _operating_expenses(expense_rows)
    bar_exp = sum(float(r["amount"]) for r in op_expenses if r.get("account") == "bar")
    room_exp = sum(float(r["amount"]) for r in op_expenses if r.get("account") == "rooms")
    cost_drinks = _cost_of_drinks_sold(sales_rows)
    total_out = bar_exp + room_exp + cost_drinks
    net = total_rev - total_out
    net_emoji = "📈" if net >= 0 else "📉"

    lines = [
        f"📋 *Daily Summary — {label}*",
        _SEP,
        "💰 *Revenue*",
        f"  🍺 Bar Sales:   {_fmt(bar_rev)}  ({len(sales_rows)} txns)",
        f"  🛏 Room Sales:  {_fmt(room_rev)}  ({len(room_rows)} bookings)",
        f"  *Total:        {_fmt(total_rev)}*",
        _SEP,
        "💸 *Outgoings*",
        f"  Drink Cost:    {_fmt(cost_drinks)}",
        f"  Bar Expenses:  {_fmt(bar_exp)}",
        f"  Room Expenses: {_fmt(room_exp)}",
        f"  *Total:        {_fmt(total_out)}*",
        _SEP,
        f"{net_emoji} *Net for Today:  {_fmt(net)}*",
    ]

    if top_drinks:
        lines.append(_SEP)
        lines.append("🏆 *Top Sellers*")
        for drink, qty in top_drinks:
            lines.append(f"  • {_esc(drink)}: {qty} units")

    if outstanding:
        owed = sum(float(r["amount"]) - float(r.get("amount_paid") or 0) for r in outstanding)
        lines.append(_SEP)
        lines.append(f"💳 Outstanding Debtors: {len(outstanding)} ({_fmt(owed)} owed)")

    if low_bar or empty_store:
        lines.append(_SEP)
        if low_bar:
            lines.append(f"⚠️ Low Bar Stock: {', '.join(_esc(d) for d in low_bar)}")
        if empty_store:
            lines.append(f"🔴 Empty Store: {', '.join(_esc(d) for d in empty_store)}")

    if total_rev > 0:
        buffer_pct, restock_pct = _get_alloc_pcts()
        total_pct = buffer_pct + restock_pct
        save_amt = round(total_rev * total_pct / 100, 2)
        lines.append(_SEP)
        lines.append(f"🏦 Set aside today: *{_fmt(save_amt)}* ({total_pct}% of {_fmt(total_rev)})")
        lines.append(f"_Run /allocation for full breakdown_")

    lines.append(f"\n_Generated {datetime.now().strftime('%d %b %Y %H:%M')}_")
    return "\n".join(lines)


# ── Allocation helpers ────────────────────────────────────────────────

def _get_alloc_pcts() -> tuple[int, int]:
    """Return (buffer%, restock%) from DB settings, falling back to config defaults."""
    buffer_ = int(db.get_setting("alloc_buffer",  str(ALLOC_BUFFER_DEFAULT)))
    restock = int(db.get_setting("alloc_restock", str(ALLOC_RESTOCK_DEFAULT)))
    return buffer_, restock


def _get_profit_dist_pcts() -> tuple[int, int, int]:
    """Return (draw%, reinvest%, float%) from DB settings, falling back to config defaults."""
    draw     = int(db.get_setting("alloc_draw",     str(ALLOC_DRAW_DEFAULT)))
    reinvest = int(db.get_setting("alloc_reinvest", str(ALLOC_REINVEST_DEFAULT)))
    float_   = int(db.get_setting("alloc_float",    str(ALLOC_FLOAT_DEFAULT)))
    return draw, reinvest, float_


def _burn_rate_label(rate: float) -> str:
    if rate <= 40:
        return f"✅ Healthy ({rate:.1f}%)"
    if rate <= 60:
        return f"⚠️ Watch closely ({rate:.1f}%)"
    return f"🔴 Danger — expenses eating revenue ({rate:.1f}%)"


# ── Allocation report ─────────────────────────────────────────────────

def generate_allocation_report(
    for_date: date | None = None,
    for_month: tuple[int, int] | None = None,
    all_time: bool = False,
) -> str:
    """
    Show recommended set-asides (tax, buffer, restock) calculated on gross revenue,
    actual expenses, net working capital, and burn rate.
    """
    sales_rows  = _apply_filter(_active(db.read_all("sales")),    for_date, for_month, all_time)
    room_rows   = _apply_filter(_active(db.read_all("rooms")),    for_date, for_month, all_time)
    expense_rows = _apply_filter(_active(db.read_all("expenses")), for_date, for_month, all_time)
    label = _period_label(for_date, for_month, all_time)

    bar_rev  = _sum_revenue(sales_rows)
    room_rev = _sum_revenue(room_rows)
    total_rev = bar_rev + room_rev

    # Operating expenses only — restock (inventory purchase) is cash → stock,
    # and the cost of stock reaches the P&L as cost-of-stock-sold below.
    op_expense_rows = _operating_expenses(expense_rows)
    bar_salary_rows,  bar_other_rows  = _split_salary([r for r in op_expense_rows if r.get("account") == "bar"])
    room_salary_rows, room_other_rows = _split_salary([r for r in op_expense_rows if r.get("account") == "rooms"])

    bar_salary_amt  = sum(float(r["amount"]) for r in bar_salary_rows)
    room_salary_amt = sum(float(r["amount"]) for r in room_salary_rows)
    total_salary    = bar_salary_amt + room_salary_amt

    bar_exp  = sum(float(r["amount"]) for r in op_expense_rows if r.get("account") == "bar")
    room_exp = sum(float(r["amount"]) for r in op_expense_rows if r.get("account") == "rooms")
    total_exp = bar_exp + room_exp                       # operating expenses (excl restock)
    cost_of_drinks  = _cost_of_drinks_sold(sales_rows)   # COGS — the real P&L stock cost
    restock_total   = _restock_spend(expense_rows)       # cash spent buying inventory
    total_outgoings = cost_of_drinks + total_exp         # true P&L cost base

    buffer_pct, restock_pct = _get_alloc_pcts()
    total_pct = buffer_pct + restock_pct

    buffer_amt  = round(total_rev * buffer_pct / 100, 2)
    restock_amt = round(total_rev * restock_pct / 100, 2)
    total_save  = buffer_amt + restock_amt

    # Bar and Rooms share of set-aside (proportional to their revenue)
    bar_share  = round(total_save * (bar_rev / total_rev), 2) if total_rev else 0.0
    room_share = round(total_save * (room_rev / total_rev), 2) if total_rev else 0.0

    other_exp     = total_exp - total_salary
    working_capital = total_rev - total_outgoings   # = net profit (revenue − COGS − operating exp)
    after_setaside  = working_capital - total_save
    burn_rate = (total_exp / total_rev * 100) if total_rev else 0.0

    # Room type breakdown
    room_by_type: dict[str, dict] = {}
    for r in room_rows:
        rt = r["room_type"].title()
        if rt not in room_by_type:
            room_by_type[rt] = {"bookings": 0, "revenue": 0.0}
        room_by_type[rt]["bookings"] += int(r["quantity"])
        room_by_type[rt]["revenue"] += float(r["total_revenue"])

    lines = [
        f"📊 *{HOTEL_NAME} — Allocation Report*",
        f"📅 Period: {label}",
        _SEP,
        "💰 *REVENUE*",
        f"  🍺 Bar:            {_fmt(bar_rev)}",
        f"  🛏 Rooms:          {_fmt(room_rev)}",
        f"  *Total:           {_fmt(total_rev)}*",
    ]

    if room_by_type and room_rev > 0:
        lines.append("  _Room breakdown:_")
        for rt in sorted(room_by_type):
            d = room_by_type[rt]
            pct = round(d["revenue"] / room_rev * 100)
            lines.append(f"    • {_esc(rt)} ({d['bookings']} bookings): {_fmt(d['revenue'])}  {pct}%")

    lines += [
        _SEP,
        f"🏦 *RECOMMENDED SET-ASIDES* _{total_pct}% of gross revenue_",
        f"  Buffer ({buffer_pct}%):    {_fmt(buffer_amt)}  → Savings Account",
    ]

    if restock_pct > 0:
        lines.append(f"  Restock ({restock_pct}%):  {_fmt(restock_amt)}  → Bar Account")

    lines += [
        f"  *Total to save:   {_fmt(total_save)}*",
        "",
        "  _How to split it:_",
        f"  From Bar Account:   {_fmt(bar_share)}",
        f"  From Rooms Account: {_fmt(room_share)}",
        _SEP,
        "💸 *COSTS* _(what reduces profit)_",
        f"  🍺 Cost of stock sold: {_fmt(cost_of_drinks)}",
        f"  👤 Salaries:       {_fmt(total_salary)}",
        f"    🍺 Bar staff:    {_fmt(bar_salary_amt)}",
        f"    🛏 Rooms staff:  {_fmt(room_salary_amt)}",
        f"  🔧 Other:          {_fmt(other_exp)}",
        f"  *Total:           {_fmt(total_outgoings)}*",
        _SEP,
        "📈 *NET POSITION*",
        f"  Net profit:       {_fmt(working_capital)}",
        f"  After set-asides: *{_fmt(after_setaside)}*  ← safe to use",
        f"  Burn rate:        {_burn_rate_label(burn_rate)}",
    ]

    if restock_total > 0:
        lines.append(f"  📦 Stock purchased: {_fmt(restock_total)} _(cash → stock, not a cost)_")

    if total_salary > after_setaside:
        lines.append(f"  ⚠️ Salary bill ({_fmt(total_salary)}) exceeds safe amount — review set-aside %")

    # Profit distribution
    draw_pct, reinvest_pct, float_pct = _get_profit_dist_pcts()
    dist_total_pct = draw_pct + reinvest_pct + float_pct

    if after_setaside > 0 and dist_total_pct > 0:
        draw_amt     = round(after_setaside * draw_pct / 100, 2)
        reinvest_amt = round(after_setaside * reinvest_pct / 100, 2)
        float_amt    = round(after_setaside * float_pct / 100, 2)
        unallocated  = round(after_setaside - draw_amt - reinvest_amt - float_amt, 2)

        lines += [
            _SEP,
            f"💼 *PROFIT DISTRIBUTION* _of {_fmt(after_setaside)} safe profit_",
            f"  👤 Owner's Draw ({draw_pct}%):   {_fmt(draw_amt)}  → Personal Account",
            f"  📈 Reinvestment ({reinvest_pct}%): {_fmt(reinvest_amt)}  → Business Growth",
            f"  🏦 Cash Float ({float_pct}%):    {_fmt(float_amt)}  → Current Account Reserve",
        ]
        if unallocated:
            lines.append(f"  Unallocated:          {_fmt(unallocated)}")

        if draw_amt > 0:
            pit_low  = round(draw_amt * PIT_LOW_RATE / 100, 2)
            pit_high = round(draw_amt * PIT_HIGH_RATE / 100, 2)
            lines += [
                _SEP,
                "ℹ️ *PERSONAL INCOME TAX (estimate)*",
                f"  Owner's Draw:    {_fmt(draw_amt)}",
                f"  PIT estimate:    {_fmt(pit_low)} – {_fmt(pit_high)}  ({PIT_LOW_RATE}–{PIT_HIGH_RATE}%)",
                "  _Nigerian PIT applies to personal income, not the business._",
                "  _Consult a tax advisor for your exact bracket._",
            ]
    elif after_setaside <= 0:
        lines += [
            _SEP,
            "💼 *PROFIT DISTRIBUTION*",
            "  Nothing to distribute — expenses + set-asides exceed revenue.",
        ]

    lines += [
        _SEP,
        "_Use /setallocation to adjust percentages_",
        f"_Generated {datetime.now().strftime('%d %b %Y %H:%M')}_",
    ]

    if not total_rev:
        return f"📊 *Allocation Report — {label}*\n\nNo revenue recorded for this period."

    return "\n".join(lines)


# ── Position report (profit vs cash vs stock) ─────────────────────────

CASH_OPENING_KEY = "cash_opening"


def _net_profit(sales_rows: list[dict], room_rows: list[dict], expense_rows: list[dict]) -> tuple[float, float, float, float]:
    """Return (revenue, cogs, operating_expenses, net_profit) for the given active rows."""
    revenue = _sum_revenue(sales_rows) + _sum_revenue(room_rows)
    cogs = _cost_of_drinks_sold(sales_rows)
    op_exp = round(sum(float(r["amount"]) for r in _operating_expenses(expense_rows)), 2)
    return round(revenue, 2), cogs, op_exp, round(revenue - cogs - op_exp, 2)


def generate_position_report() -> str:
    """The three figures that should never be mixed: profit, cash, stock value.

    - Profit is a *period* figure (performance); owner draws never reduce it.
    - Cash in bank is a *running balance* estimate; draws and restock DO reduce it.
    - Stock value and receivables are point-in-time assets.
    """
    sales_all   = _active(db.read_all("sales"))
    rooms_all   = _active(db.read_all("rooms"))
    expense_all = _active(db.read_all("expenses"))
    draws_all   = _active(db.read_all("owner_draws"))
    debtor_rows = db.read_all("debtors")
    now = datetime.now()

    # ① Profit — this month and all-time
    month_sales = _filter_by_month(sales_all, now.year, now.month)
    month_rooms = _filter_by_month(rooms_all, now.year, now.month)
    month_exp   = _filter_by_month(expense_all, now.year, now.month)
    *_, month_profit = _net_profit(month_sales, month_rooms, month_exp)
    rev_all, _cogs_all, opex_all, profit_all = _net_profit(sales_all, rooms_all, expense_all)

    # ③ Stock value on hand (asset)
    stock_value = round(sum(i["stock_value"] for i in inv.get_inventory_summary()), 2)

    # Receivables — revenue earned but not yet collected as cash
    outstanding = [r for r in debtor_rows if r["status"] == "outstanding"]
    receivables = round(sum(float(r["amount"]) - float(r.get("amount_paid") or 0) for r in outstanding), 2)

    # ② Cash in bank (estimate)
    try:
        opening = float(db.get_setting(CASH_OPENING_KEY, "0") or 0)
    except (TypeError, ValueError):
        opening = 0.0
    restock_all = _restock_spend(expense_all)
    draws_total = round(sum(float(r["amount"]) for r in draws_all), 2)
    collected   = round(rev_all - receivables, 2)   # assume cash unless an outstanding debtor exists
    cash = round(opening + collected - opex_all - restock_all - draws_total, 2)

    m_emoji = "📈" if month_profit >= 0 else "📉"
    a_emoji = "📈" if profit_all >= 0 else "📉"

    lines = [
        f"🧭 *{HOTEL_NAME} — Position*",
        f"📅 As of {now.strftime('%d %b %Y %H:%M')}",
        _SEP,
        "① 📊 *PROFIT* _(performance)_",
        f"  {m_emoji} This month:  *{_fmt(month_profit)}*",
        f"  {a_emoji} All-time:    {_fmt(profit_all)}",
        "  _Revenue − cost of stock sold − expenses._",
        "  _Owner draws are NOT subtracted here._",
        _SEP,
        "② 🏦 *CASH IN BANK* _(estimate)_",
        f"  Opening balance:    {_fmt(opening)}",
        f"  + Collected sales:  {_fmt(collected)}",
        f"  − Expenses:         {_fmt(opex_all)}",
        f"  − Stock purchases:  {_fmt(restock_all)}",
        f"  − Owner draws:      {_fmt(draws_total)}",
        f"  = *Cash on hand:    {_fmt(cash)}*",
        "  _Assumes sales are cash unless logged as a debtor._",
        _SEP,
        "③ 📦 *STOCK VALUE ON HAND* _(asset)_",
        f"  Bar + store @ cost:  *{_fmt(stock_value)}*",
        _SEP,
        "💳 *OWED TO US* _(receivables)_",
        f"  Outstanding debtors: {_fmt(receivables)}  ({len(outstanding)} owing)",
        _SEP,
        "_Profit ≠ cash ≠ stock — keep them separate._",
        "_Owner draws reduce cash, never profit. Log them with /draw._",
        "_Set your real starting balance: /position set <amount>_",
        f"_Generated {now.strftime('%d %b %Y %H:%M')}_",
    ]
    return "\n".join(lines)


# ── Debtors report ─────────────────────────────────────────────────────

def _debt_age(timestamp_str: str) -> str:
    """Return a human-readable age string and flag for overdue debts."""
    created = _parse_ts(timestamp_str)
    if not created:
        return ""
    try:
        days = (datetime.now() - created).days
    except Exception:
        return ""
    date_str = created.strftime("%d %b")
    if days == 0:
        return f" _(today, {date_str})_"
    if days == 1:
        return f" _(1 day, {date_str})_"
    flag = " ⚠️" if days >= 7 else ""
    return f" _({days} days, {date_str}){flag}_"


def generate_debtors_report(account: str | None = None, staff_view: bool = False, month: str | None = None) -> str:
    """List all outstanding debtors, optionally filtered to one account and/or month."""
    rows = db.get_debtors(account=account, month=month)

    acct_label  = f"{account.title()} " if account else ""
    month_label = f" — {month}" if month else ""
    if not rows:
        return f"✅ No outstanding {acct_label}debtors{month_label}."

    bar_rows = [r for r in rows if r["account"] == "bar"]
    room_rows = [r for r in rows if r["account"] == "rooms"]

    title = f"🏨 *{HOTEL_NAME} — {acct_label}Debtors{month_label}*" if (account or month) else f"🏨 *{HOTEL_NAME} — Outstanding Debtors*"
    lines = [title, _SEP]

    def _remaining(r: dict) -> float:
        return round(float(r["amount"]) - float(r.get("amount_paid") or 0), 2)

    def _debt_lines(r: dict) -> list[str]:
        did = r["id"]
        name = _esc(str(r["name"]).title())
        desc = str(r.get("description") or "").strip()
        note = f" — {_esc(desc)}" if desc else ""
        age = _debt_age(r.get("timestamp", ""))
        staff = r.get("staff_name", "") or ""
        by_tag = f" _(by {_esc(staff.title())})_" if staff.strip() else ""
        original = float(r["amount"])
        paid = float(r.get("amount_paid") or 0)
        rem = round(original - paid, 2)
        out = [f"  • `[#{did}]` {name}: {_fmt(original)}{note}{by_tag}{age}"]
        if paid > 0:
            out.append(f"      Paid: {_fmt(paid)} | *Remaining: {_fmt(rem)}*")
        return out

    if bar_rows and (account is None or account == "bar"):
        lines.append("🍺 *BAR*")
        for r in bar_rows:
            lines.extend(_debt_lines(r))
        lines.append(f"  *Total remaining: {_fmt(sum(_remaining(r) for r in bar_rows))}*")
        lines.append("")

    if room_rows and (account is None or account == "rooms"):
        lines.append("🛏 *ROOMS*")
        for r in room_rows:
            lines.extend(_debt_lines(r))
        lines.append(f"  *Total remaining: {_fmt(sum(_remaining(r) for r in room_rows))}*")

    def _days_old(r: dict) -> int:
        dt = _parse_ts(r.get("timestamp"))
        return (datetime.now() - dt).days if dt else 0

    overdue = [r for r in rows if _days_old(r) >= 7]
    lines.append(_SEP)
    if overdue:
        lines.append(f"⚠️ {len(overdue)} debt(s) outstanding for 7+ days — follow up needed.")
    if not staff_view:
        lines.append("_Use_ `/pay_debt <id> [amount]` _to pay a specific debt._")
    lines.append(f"_Updated {datetime.now().strftime('%d %b %Y %H:%M')}_")
    return "\n".join(lines)


# ── Debtor name lookup ────────────────────────────────────────────────

def generate_debtor_lookup(name: str) -> str:
    """All outstanding debts for a single person across bar and rooms."""
    rows = db.get_outstanding_by_name(name)
    display = _esc(name.title())

    if not rows:
        return f"✅ No outstanding debts found for *{display}*."

    bar_rows  = [r for r in rows if r["account"] == "bar"]
    room_rows = [r for r in rows if r["account"] == "rooms"]

    def _remaining(r: dict) -> float:
        return round(float(r["amount"]) - float(r.get("amount_paid") or 0), 2)

    def _debt_lines(r: dict) -> list[str]:
        did  = r["id"]
        desc = str(r.get("description") or "").strip()
        note = f" — {_esc(desc)}" if desc else ""
        age  = _debt_age(r.get("timestamp", ""))
        staff = r.get("staff_name", "") or ""
        original = float(r["amount"])
        paid     = float(r.get("amount_paid") or 0)
        rem      = round(original - paid, 2)
        out = [f"  • `[#{did}]` {_fmt(original)}{note}{age}"]
        if staff.strip():
            out.append(f"      Sold by: *{_esc(staff.title())}*")
        if paid > 0:
            out.append(f"      Paid: {_fmt(paid)} | *Remaining: {_fmt(rem)}*")
        return out

    lines = [f"🏨 *{HOTEL_NAME} — Debts for {display}*", _SEP]

    if bar_rows:
        lines.append("🍺 *BAR*")
        for r in bar_rows:
            lines.extend(_debt_lines(r))
        lines.append(f"  *Total: {_fmt(sum(_remaining(r) for r in bar_rows))}*")
        lines.append("")

    if room_rows:
        lines.append("🛏 *ROOMS*")
        for r in room_rows:
            lines.extend(_debt_lines(r))
        lines.append(f"  *Total: {_fmt(sum(_remaining(r) for r in room_rows))}*")
        lines.append("")

    grand = sum(_remaining(r) for r in rows)
    lines.append(_SEP)
    lines.append(f"*Total outstanding: {_fmt(grand)}*")
    lines.append("_Use_ `/pay_debt <id> [amount]` _to pay a specific debt._")
    return "\n".join(lines)


# ── Staff debtor report ───────────────────────────────────────────────

def generate_staff_debtors(staff_name: str) -> str:
    """All outstanding debts attributed to a specific staff member."""
    rows = db.get_debts_by_staff(staff_name)
    display = _esc(staff_name.title())

    if not rows:
        return f"✅ No outstanding debts attributed to *{display}*."

    bar_rows  = [r for r in rows if r["account"] == "bar"]
    room_rows = [r for r in rows if r["account"] == "rooms"]

    def _remaining(r: dict) -> float:
        return round(float(r["amount"]) - float(r.get("amount_paid") or 0), 2)

    def _debt_line(r: dict) -> list[str]:
        did      = r["id"]
        customer = _esc(str(r["name"]).title())
        desc     = str(r.get("description") or "").strip()
        note     = f" — {_esc(desc)}" if desc else ""
        age      = _debt_age(r.get("timestamp", ""))
        original = float(r["amount"])
        paid     = float(r.get("amount_paid") or 0)
        rem      = round(original - paid, 2)
        out = [f"  • `[#{did}]` {customer}: {_fmt(original)}{note}{age}"]
        if paid > 0:
            out.append(f"      Paid: {_fmt(paid)} | *Remaining: {_fmt(rem)}*")
        return out

    lines = [f"🏨 *{HOTEL_NAME} — Debtors under {display}*", _SEP]

    if bar_rows:
        lines.append("🍺 *BAR*")
        for r in bar_rows:
            lines.extend(_debt_line(r))
        lines.append(f"  *Total: {_fmt(sum(_remaining(r) for r in bar_rows))}*")
        lines.append("")

    if room_rows:
        lines.append("🛏 *ROOMS*")
        for r in room_rows:
            lines.extend(_debt_line(r))
        lines.append(f"  *Total: {_fmt(sum(_remaining(r) for r in room_rows))}*")
        lines.append("")

    grand = sum(_remaining(r) for r in rows)
    lines.append(_SEP)
    lines.append(f"*Total outstanding under {display}: {_fmt(grand)}*")
    lines.append("_Use_ `/pay_debt <id> [amount]` _to pay a specific debt._")
    return "\n".join(lines)


# ── Stock report ──────────────────────────────────────────────────────

def generate_stock_report(staff_view: bool = False) -> str:
    items = inv.get_inventory_summary()
    if not items:
        return "📦 Inventory is empty. Use /restock to add drinks."

    col = max(len(i["drink"]) for i in items) + 1
    col = max(col, 10)

    if staff_view:
        header  = f"{'Drink':<{col}} {'Bar':>6}"
        divider = "-" * len(header)
        rows_out = []
        low_bar_items = []

        for item in items:
            flag = " !" if item["is_low"] else ""
            rows_out.append(f"{item['drink'][:col]:<{col}} {item['bar_stock']:>6}{flag}")
            if item["is_low"]:
                low_bar_items.append(item["drink"])

        lines = [
            f"🏨 *{HOTEL_NAME} — Bar Stock*",
            "```",
            header,
            divider,
            *rows_out,
            "```",
        ]
        if low_bar_items:
            lines.append("⚠️ *Low Bar Stock* — ask admin to transfer:")
            for name in low_bar_items:
                lines.append(f"  • {_esc(name)}")
        lines.append(f"\n_Updated {datetime.now().strftime('%d %b %Y %H:%M')}_")
        return "\n".join(lines)

    # Admin view — full table with margin
    header  = f"{'Drink':<{col}} {'Store':>6} {'Bar':>6} {'Cost':>10} {'Price':>10} {'Margin':>10}"
    divider = "-" * len(header)

    rows_out = []
    total_value = 0.0
    low_bar_items = []
    empty_store_items = []

    for item in items:
        flag = " !" if item["is_low"] else "  "
        margin = item.get("margin", 0.0)
        price = item.get("selling_price", 0.0)
        price_str = _fmt(price) if price > 0 else "—"
        margin_str = _fmt(margin) if price > 0 else "—"
        line = (
            f"{item['drink'][:col]:<{col}} "
            f"{item['store_stock']:>6} "
            f"{item['bar_stock']:>6} "
            f"{_fmt(item['cost_price']):>10} "
            f"{price_str:>10} "
            f"{margin_str:>10}"
            f"{flag}"
        )
        rows_out.append(line)
        total_value += item["stock_value"]
        if item["is_low"]:
            low_bar_items.append(item["drink"])
        if item["store_stock"] == 0:
            empty_store_items.append(item["drink"])

    total_line = f"{'TOTAL VALUE':<{col}} {'':>6} {'':>6} {'':>10} {'':>10} {_fmt(total_value):>10}"

    lines = [
        f"🏨 *{HOTEL_NAME} — Stock Report*",
        "```",
        header,
        divider,
        *rows_out,
        divider,
        total_line,
        "```",
    ]

    if low_bar_items:
        lines.append("⚠️ *Low Bar Stock* (transfer from store):")
        for name in low_bar_items:
            lines.append(f"  • {_esc(name)}")

    if empty_store_items:
        lines.append("🔴 *Store Empty* (needs restock):")
        for name in empty_store_items:
            lines.append(f"  • {_esc(name)}")

    lines.append(f"\n_Updated {datetime.now().strftime('%d %b %Y %H:%M')}_")
    return "\n".join(lines)


# ── Restock plan (advisory) ───────────────────────────────────────────

# Restock-advisor tuning constants.
RESTOCK_BUDGET_PCT = 40    # max monthly bar restock as % of bar revenue
HIGH_MARGIN = 400          # ₦/unit considered "high margin"
_SLOW_MONTHLY = 5          # sold fewer than this/month = slow mover
_NOW_WEEKS = 1.75          # store-empty + < this cover = reorder now
_SOON_WEEKS = 3.5          # store-empty + < this cover = reorder soon

_WEEK_CYCLE = {
    1: "Week 1 — sell stock, spot fast movers",
    2: "Week 2 — small targeted reorder",
    3: "Week 3 — mid-month review, reorder on pace",
    4: "Week 4 — minimal top-up, protect month-end cash",
}


def _order_range(weekly: float) -> tuple[str, int]:
    """Return (display range, low-end qty) per the 2-week order guide."""
    if weekly >= 16:
        return "24–30", 24
    if weekly >= 6:
        return "14–20", 14
    if weekly >= 1:
        return "6–10", 6
    return "6", 6


def generate_restock_plan(
    for_date: date | None = None,
    for_month: tuple[int, int] | None = None,
    all_time: bool = False,
) -> str:
    """Advisory weekly restock plan: transfers, reorder tiers, budget, warnings.

    Velocity is derived from the period's sales (treated as a 4-week month);
    defaults to the current month, which is the intended view.
    """
    items = inv.get_inventory_summary()
    if not items:
        return "📦 Inventory is empty. Use /restock to add drinks first."

    label = _period_label(for_date, for_month, all_time)
    today = for_date or datetime.now().date()
    week_no = min((today.day - 1) // 7 + 1, 4)

    # Sales velocity + bar revenue for the period.
    sales_rows = _apply_filter(_active(db.read_all("sales")), for_date, for_month, all_time)
    qty_by_drink: dict[str, int] = {}
    bar_revenue = 0.0
    for r in sales_rows:
        nm = str(r["drink_name"]).lower()
        qty_by_drink[nm] = qty_by_drink.get(nm, 0) + int(r["quantity"])
        bar_revenue += float(r["total_revenue"])

    # Restock already spent this period (bar / "restock" expenses).
    exp_rows = _apply_filter(_active(db.read_all("expenses")), for_date, for_month, all_time)
    restock_spent = sum(
        float(e["amount"]) for e in exp_rows
        if e.get("account") == "bar" and str(e.get("category", "")).lower() == "restock"
    )

    stock_value = sum(i["stock_value"] for i in items)
    budget_cap = round(bar_revenue * RESTOCK_BUDGET_PCT / 100, 2)

    transfers: list[tuple] = []   # (name, move, bar, new_bar, result_cover)
    now: list[tuple] = []         # (name, monthly, weekly, rng, est, weeks, margin)
    soon: list[tuple] = []
    hold: list[tuple] = []        # (name, reason, monthly)
    pricing: list[tuple] = []     # (name, idle_units, tied_value)
    stockouts: list[str] = []     # selling items with bar == 0

    for it in items:
        name, bar, store = it["drink"], it["bar_stock"], it["store_stock"]
        cost, price, margin = it["cost_price"], it["selling_price"], it["margin"]
        monthly = qty_by_drink.get(name.lower(), 0)
        weekly = monthly / 4.0
        total = bar + store

        # Transfer suggestion (store → bar) when bar cover is thin.
        # Skip unpriced items — they can't be sold from the bar anyway.
        move = 0
        if store > 0 and price > 0:
            if weekly > 0 and bar / weekly < 1.5:
                move = min(store, max(0, ceil(weekly * 2) - bar))
            elif weekly == 0 and bar == 0:
                move = min(store, 5)   # make a dead-bar item sellable
        if move > 0:
            new_bar = bar + move
            transfers.append((name, move, bar, new_bar, new_bar / weekly if weekly else 999))
        store_after = store - move
        weeks = total / weekly if weekly > 0 else float("inf")

        if price <= 0:
            pricing.append((name, total, round(total * cost, 2)))
            continue
        if bar == 0 and monthly > 0:
            stockouts.append(name)

        rng, low_qty = _order_range(weekly)
        two_wk = ceil(weekly * 2)
        note = f" (≈{two_wk}/2wk)" if two_wk > 30 else ""
        row = (name, monthly, weekly, rng + note, round(low_qty * cost, 2), weeks, margin)

        if monthly < _SLOW_MONTHLY:
            hold.append((name, "slow mover", monthly))
        elif store_after > 0:
            hold.append((name, "store stock left", monthly))
        elif weeks < _NOW_WEEKS:
            now.append(row)
        elif weeks < _SOON_WEEKS:
            soon.append(row)
        else:
            hold.append((name, "ample bar cover", monthly))

    now.sort(key=lambda r: r[5])      # most urgent (least cover) first
    soon.sort(key=lambda r: r[5])
    transfers.sort(key=lambda t: t[4])
    est_now = sum(r[4] for r in now)

    # Possible duplicate SKUs (same alphanumerics, different rows).
    norm: dict[str, list[str]] = {}
    for it in items:
        key = "".join(c for c in it["drink"].lower() if c.isalnum())
        norm.setdefault(key, []).append(it["drink"])
    dupes = [v for v in norm.values() if len(v) > 1]

    # ── Stock Turn Ratio ──
    ratio = bar_revenue / stock_value if stock_value else 0.0
    if ratio < 2:
        rating = "🔴 Too much capital in slow stock"
    elif ratio < 3:
        rating = "🟡 Acceptable — monitor closely"
    elif ratio <= 4:
        rating = "🟢 Healthy"
    else:
        rating = "⚠️ Stockout risk — raise order quantities"

    L: list[str] = [
        f"📦 *{HOTEL_NAME} — Restock Plan*",
        f"📅 {label} · {_WEEK_CYCLE[week_no]}",
        _SEP,
        "*1️⃣ Stock Turn Ratio*",
        f"  {_fmt(bar_revenue)} ÷ {_fmt(stock_value)} = *{ratio:.2f}x*",
        f"  {rating}",
        _SEP,
        "*2️⃣ Transfers* _store → bar · ₦0 cost_",
    ]
    if transfers:
        for name, mv, b, nb, _cov in transfers:
            L.append(f"  • *{_esc(name)}*: move *{mv}*  _(bar {b}→{nb})_")
    else:
        L.append("  _None needed._")

    L += [_SEP, "*3️⃣ 🔴 Reorder Now* _store empty, running out_"]
    if now:
        for name, mo, wk, rng, est, weeks, mg in now:
            L.append(f"  • *{_esc(name)}* — {weeks:.1f}wk left · {wk:.0f}/wk → *{rng}* ~{_fmt(est)}")
    else:
        L.append("  _Nothing critical. 🎉_")

    L += [_SEP, "*4️⃣ 🟡 Reorder Soon*"]
    if soon:
        for name, mo, wk, rng, est, weeks, mg in soon:
            L.append(f"  • *{_esc(name)}* — {weeks:.1f}wk · {wk:.0f}/wk → *{rng}*")
    else:
        L.append("  _Nothing pending._")

    L += [_SEP, f"*5️⃣ 🟢 Hold* ({len(hold)} items)"]
    slow = [n for n, reason, mo in hold if reason == "slow mover" and mo == 0]
    if slow:
        L.append(f"  _Not selling (review/promote):_ {', '.join(_esc(n) for n in slow)}")
    else:
        L.append("  _Stocked items with adequate cover._")

    L += [_SEP, "*6️⃣ 🚩 Pricing Gaps*"]
    if pricing:
        for name, units, tied in pricing:
            unit_word = "unit" if units == 1 else "units"
            L.append(f"  • *{_esc(name)}*: no price — {units} {unit_word} idle ({_fmt(tied)})")
    else:
        L.append("  _All stocked items are priced. ✅_")
    for names in dupes:
        L.append(f"  ⚠️ Possible duplicate SKU: {', '.join(_esc(n) for n in names)}")

    # ── Budget ──
    spent_pct = (restock_spent / bar_revenue * 100) if bar_revenue else 0
    L += [
        _SEP,
        "*7️⃣ Budget*",
        f"  {RESTOCK_BUDGET_PCT}% of {_fmt(bar_revenue)} = *{_fmt(budget_cap)}* cap",
        f"  Restock spent this period: {_fmt(restock_spent)} ({spent_pct:.0f}%)",
        f"  🔴 Reorder Now (min qty): {_fmt(est_now)}",
    ]

    # ── Warnings ──
    warns: list[str] = []
    if budget_cap and restock_spent > budget_cap:
        mult = restock_spent / budget_cap
        warns.append(f"Restock spend {_fmt(restock_spent)} is *{mult:.1f}×* the {RESTOCK_BUDGET_PCT}% cap ({_fmt(budget_cap)}) — {spent_pct:.0f}% of revenue.")
    if ratio and ratio < 2:
        warns.append(f"Stock Turn Ratio {ratio:.2f}x is below 2x — too much cash in slow stock.")
    elif 2 <= ratio < 2.3:
        warns.append(f"Stock Turn Ratio {ratio:.2f}x is just above the 2x floor — capital tied in slow SKUs.")
    elif ratio > 4:
        warns.append(f"Stock Turn Ratio {ratio:.2f}x is above 4x — stockout risk, raise order quantities.")
    if pricing:
        warns.append(f"{len(pricing)} item(s) with no price set — cannot earn until priced.")
    if dupes:
        warns.append(f"{len(dupes)} possible duplicate SKU(s) splitting stock/sales.")
    if stockouts:
        warns.append(f"Complete bar stockout on selling item(s): {', '.join(_esc(n) for n in stockouts)}.")
    if budget_cap and (restock_spent + est_now) > budget_cap:
        warns.append(f"Adding 🔴 reorders ({_fmt(est_now)}) pushes spend to {_fmt(restock_spent + est_now)} — over cap. Stagger into weekly batches.")

    L += [_SEP, "*8️⃣ ⚠️ Warnings*"]
    if warns:
        L += [f"  • {w}" for w in warns]
    else:
        L.append("  _None. ✅_")

    L += [_SEP, "_Tip: do transfers first, then only the 🔴 items this week._",
          f"_Generated {datetime.now().strftime('%d %b %Y %H:%M')}_"]
    return "\n".join(L)


# ── Price list ────────────────────────────────────────────────────────

def generate_price_list() -> str:
    """Show all drinks with their canonical selling price set by admin."""
    price_rows = db.get_drink_selling_prices()

    if not price_rows:
        return "📦 No drinks in inventory yet."

    col = max(len(r["drink_name"].title()) for r in price_rows) + 1
    col = max(col, 10)

    header  = f"{'Drink':<{col}} {'Price':>12}"
    divider = "-" * len(header)
    rows_out = []
    unpriced = []

    for r in price_rows:
        name = r["drink_name"].title()
        price = float(r["selling_price"])
        if price > 0:
            rows_out.append(f"{name:<{col}} {_fmt(price):>12}")
        else:
            rows_out.append(f"{name:<{col}} {'—':>12}")
            unpriced.append(name)

    lines = [
        f"🍺 *{HOTEL_NAME} — Drink Prices*",
        "```",
        header,
        divider,
        *rows_out,
        "```",
    ]
    if unpriced:
        lines.append(f"⚠️ No price set for: {', '.join(_esc(u) for u in unpriced)}")
        lines.append("_Admin: use /setprice <drink> <amount> to set._")

    room_presets = db.get_all_room_type_prices()
    if room_presets:
        rcol = max(len(r["room_type"]) for r in room_presets) + 1
        rcol = max(rcol, 10)
        lines += [
            "",
            "🛏 *Room Types*",
            "```",
            f"{'Type':<{rcol}} {'Price/Night':>12}",
            "-" * (rcol + 13),
            *[f"{r['room_type']:<{rcol}} {_fmt(r['price']):>12}" for r in room_presets],
            "```",
        ]

    lines.append(f"_Updated {datetime.now().strftime('%d %b %Y %H:%M')}_")
    return "\n".join(lines)


# ── Debtor history ────────────────────────────────────────────────────

def generate_debtor_history(account: str, name: str) -> str:
    """Full payment timeline for one person + account (admin-only)."""
    data = db.get_debtor_history(name, account)
    debts = data["debts"]
    payments_by_id = data["payments"]

    if not debts:
        return f"🧾 No debt history for *{_esc(name.title())}* in *{_esc(account.title())}*."

    lines = [
        f"🧾 *Debt History — {_esc(name.title())} ({_esc(account.title())})*",
        _SEP,
    ]

    grand_remaining = 0.0
    for debt in debts:
        did = int(debt["id"])
        original = float(debt["amount"])
        paid_total = float(debt.get("amount_paid") or 0)
        remaining = round(original - paid_total, 2)
        status = debt["status"]
        ts = str(debt.get("timestamp", ""))[:10]
        desc = debt.get("description", "")

        staff = str(debt.get("staff_name", "") or "").strip()
        icon = "✅" if status == "paid" else "🔴"
        desc = str(desc).strip()
        desc_note = f" — {_esc(desc)}" if desc else ""
        staff_note = f" _(sold by {_esc(staff.title())})_" if staff else ""
        lines.append(f"{icon} `[#{did}]` Opened {ts}: *{_fmt(original)}*{desc_note}{staff_note}")

        for p in payments_by_id.get(did, []):
            pts = str(p.get("timestamp", ""))[:10]
            pamt = float(p["amount"])
            pby = p.get("recorded_by", "")
            by_note = f" by @{_esc(pby)}" if pby else ""
            lines.append(f"    💳 {pts}: paid {_fmt(pamt)}{by_note}")

        if status == "outstanding":
            lines.append(f"    Balance: *{_fmt(remaining)}* outstanding")
            grand_remaining += remaining
        else:
            paid_at = str(debt.get("paid_at", ""))[:10]
            lines.append(f"    ✅ Cleared on {paid_at}")
        lines.append("")

    if lines and lines[-1] == "":
        lines.pop()

    lines += [
        _SEP,
        f"*Total still owed: {_fmt(grand_remaining)}*" if grand_remaining > 0 else "✅ All debts cleared.",
        f"_Generated {datetime.now().strftime('%d %b %Y %H:%M')}_",
    ]
    return "\n".join(lines)


# ── Daily report (for scheduler) ─────────────────────────────────────

def generate_daily_report() -> str:
    return generate_daily_summary()


# ── Activity log ──────────────────────────────────────────────────────

def generate_activity_log(date_str: str, username_filter: str | None = None) -> str:
    """Chronological admin view of all staff activity for a given date."""
    entries = db.get_activity_log(date_str, username=username_filter)

    try:
        label = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d %b %Y")
    except ValueError:
        label = date_str

    filter_note = f" — @{_esc(username_filter)}" if username_filter else ""

    if not entries:
        msg = f"No activity recorded for *{_esc(username_filter)}*." if username_filter else "No activity recorded."
        return f"📋 *Activity Log — {label}*{filter_note}\n\n{msg}"

    # Group by actor (recorded_by for most; paid_by for debtor_pay)
    by_actor: dict[str, list[dict]] = {}
    for entry in entries:
        if entry["entry_type"] == "debtor_pay":
            actor = (entry.get("paid_by") or "Unknown").strip() or "Unknown"
        else:
            actor = (entry.get("recorded_by") or "Unknown").strip() or "Unknown"
        by_actor.setdefault(actor, []).append(entry)

    lines = [f"📋 *Activity Log — {label}*{filter_note}", _SEP]

    for actor in sorted(by_actor):
        lines.append(f"👤 *@{_esc(actor)}*")
        for e in by_actor[actor]:
            ts = e.get("timestamp", "")
            try:
                time_str = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").strftime("%H:%M")
            except ValueError:
                time_str = "--:--"

            etype = e["entry_type"]
            is_voided = bool(e.get("deleted_at"))
            void_suffix = ""
            if is_voided:
                voided_by = e.get("deleted_by") or "?"
                try:
                    void_time = datetime.strptime(e["deleted_at"], "%Y-%m-%d %H:%M:%S").strftime("%H:%M")
                except (ValueError, KeyError):
                    void_time = "?"
                void_suffix = f" [VOIDED {void_time} by {voided_by}]"

            if etype == "sale":
                drink = _esc(str(e.get("drink_name", "?")).title())
                qty = int(e.get("quantity", 0))
                total = float(e.get("total_revenue", 0))
                icon = "🔴" if is_voided else "🍺"
                lines.append(f"  {time_str}  {icon} Sold {qty}× {drink} — {_fmt(total)}{void_suffix}")
            elif etype == "room":
                rtype = _esc(str(e.get("room_type", "?")).title())
                qty = int(e.get("quantity", 0))
                nights = int(e.get("nights", 0))
                total = float(e.get("total_revenue", 0))
                icon = "🔴" if is_voided else "🏨"
                lines.append(f"  {time_str}  {icon} Room: {qty}× {rtype}, {nights}n — {_fmt(total)}{void_suffix}")
            elif etype == "expense":
                acct = _esc(str(e.get("account", "?")).title())
                cat = _esc(str(e.get("category", "?")).title())
                amt = float(e.get("amount", 0))
                desc = _esc(str(e.get("description", "") or ""))
                desc_note = f' "{desc}"' if desc else ""
                icon = "🔴" if is_voided else "💸"
                lines.append(f"  {time_str}  {icon} Expense {acct}/{cat} {_fmt(amt)}{desc_note}{void_suffix}")
            elif etype == "debtor_add":
                acct = _esc(str(e.get("account", "?")).title())
                name = _esc(str(e.get("name", "?")).title())
                amt = float(e.get("amount", 0))
                lines.append(f"  {time_str}  🧾 Added debtor: {name} ({acct}) — {_fmt(amt)}")
            elif etype == "debtor_pay":
                acct = _esc(str(e.get("account", "?")).title())
                name = _esc(str(e.get("name", "?")).title())
                amt = float(e.get("amount", 0))
                lines.append(f"  {time_str}  ✅ Paid debtor: {name} ({acct}) — {_fmt(amt)}")
            elif etype == "transfer":
                drink = _esc(str(e.get("drink_name", "?")).title())
                qty = int(e.get("quantity", 0))
                lines.append(f"  {time_str}  📦 Transfer: {qty}× {drink} store→bar")
        lines.append("")

    # Remove trailing blank line
    if lines and lines[-1] == "":
        lines.pop()

    lines.append(_SEP)
    lines.append(f"_Generated {datetime.now().strftime('%d %b %Y %H:%M')}_")
    return "\n".join(lines)
