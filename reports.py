"""
reports.py — Financial calculations and Telegram-formatted report strings.

All monetary values are in ₦ (Naira) — change the symbol in _fmt() if needed.
"""
from __future__ import annotations

from datetime import datetime, date, timedelta
from math import ceil
from typing import Any

import database as db
import inventory as inv
import metrics
from metrics import (
    NON_PNL_CATEGORIES,
    sum_revenue as _sum_revenue,
    parse_ts as _parse_ts,
    filter_by_date as _filter_by_date,
    filter_by_month as _filter_by_month,
    apply_filter as _apply_filter,
    split_salary as _split_salary,
    active as _active,
    operating_expenses as _operating_expenses,
    restock_spend as _restock_spend,
)
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


# ── Presentation helpers ──────────────────────────────────────────────
# The pure data/aggregation helpers (_sum_revenue, _parse_ts, _filter_*,
# _apply_filter, _active, _operating_expenses, _restock_spend, _split_salary,
# NON_PNL_CATEGORIES) now live in metrics.py and are imported above, so the
# Telegram bot and the web dashboard share one implementation. Only
# presentation/formatting helpers remain in this module.

def _period_label(for_date: date | None, for_month: tuple[int, int] | None, all_time: bool) -> str:
    now = datetime.now()
    if for_date:
        return for_date.strftime("%d %b %Y")
    if all_time:
        return "ALL-TIME"
    year, month = for_month if for_month else (now.year, now.month)
    label = datetime(year, month, 1).strftime("%B %Y")
    return f"{label} (current month)" if (year, month) == (now.year, now.month) else label


def _cost_price_map() -> dict[str, float]:
    """Current cost price per drink (lower-cased name → cost) from inventory."""
    return {r["drink_name"].lower(): float(r["cost_price"]) for r in db.read_all("inventory")}


def _cost_of_drinks_sold(sales_rows: list[dict]) -> float:
    """Match each sale to its *current* cost price from inventory (delegates to metrics)."""
    return metrics.cost_of_drinks_sold(sales_rows, _cost_price_map())


# ── Full financial report ─────────────────────────────────────────────

def generate_full_report(
    for_date: date | None = None,
    for_month: tuple[int, int] | None = None,
    all_time: bool = False,
    staff_view: bool = False,
) -> str:
    sales_rows = _apply_filter(_active(db.read_all("sales")), for_date, for_month, all_time)
    room_rows = _apply_filter(_active(db.read_all("rooms")), for_date, for_month, all_time)
    expense_rows = _apply_filter(_active(db.read_all("expenses")), for_date, for_month, all_time)
    debtor_rows = db.read_all("debtors")
    label = _period_label(for_date, for_month, all_time)

    # All P&L arithmetic (Bar/Rooms split, COGS, restock exclusion) lives in
    # metrics.compute_pnl — the single source of truth shared with the dashboard.
    pnl = metrics.compute_pnl(sales_rows, room_rows, expense_rows, _cost_price_map())
    bar, rooms = pnl.bar, pnl.rooms

    if staff_view:
        lines = [
            f"🏨 *{HOTEL_NAME} — Revenue Summary*",
            f"📅 Period: {label}",
            _SEP,
            f"🍺 Bar Sales:      {_fmt(bar.revenue)}  ({pnl.sales_count} transactions)",
            f"🛏 Room Bookings:  {_fmt(rooms.revenue)}  ({pnl.rooms_count} bookings)",
            _SEP,
            f"*Total Revenue:   {_fmt(pnl.total_revenue)}*",
            _SEP,
            f"_Generated {datetime.now().strftime('%d %b %Y %H:%M')}_",
        ]
        return "\n".join(lines)

    bar_emoji = "📈" if bar.profit >= 0 else "📉"
    room_emoji = "📈" if rooms.profit >= 0 else "📉"
    net_emoji = "📈" if pnl.net_profit >= 0 else "📉"

    lines = [
        f"🏨 *{HOTEL_NAME} — Financial Report*",
        f"📅 Period: {label}",
        _SEP,
        "🍺 *BAR ACCOUNT*",
        f"  Revenue: {_fmt(bar.revenue)}",
        f"  Cost of Stock Sold: {_fmt(bar.cogs)}",
        f"  Salaries: {_fmt(bar.salary)}",
        f"  Other Expenses: {_fmt(bar.other_expense)}",
        f"  {bar_emoji} *Profit: {_fmt(bar.profit)}*",
    ]

    if bar.other_breakdown:
        lines.append("  _Other breakdown:_")
        for cat, amt in sorted(bar.other_breakdown.items()):
            lines.append(f"    • {_esc(cat)}: {_fmt(amt)}")

    lines += [
        _SEP,
        "🛏 *ROOMS ACCOUNT*",
        f"  Revenue: {_fmt(rooms.revenue)}",
        f"  Salaries: {_fmt(rooms.salary)}",
        f"  Other Expenses: {_fmt(rooms.other_expense)}",
        f"  {room_emoji} *Profit: {_fmt(rooms.profit)}*",
    ]

    if rooms.other_breakdown:
        lines.append("  _Other breakdown:_")
        for cat, amt in sorted(rooms.other_breakdown.items()):
            lines.append(f"    • {_esc(cat)}: {_fmt(amt)}")

    lines += [
        _SEP,
        "📊 *COMBINED*",
        f"  Total Revenue:   {_fmt(pnl.total_revenue)}",
        f"  Total Outgoings: {_fmt(pnl.total_outgoings)}",
        f"  {net_emoji} *Net Profit:    {_fmt(pnl.net_profit)}*",
        _SEP,
    ]

    if pnl.restock_spend > 0:
        lines += [
            f"📦 Stock purchased: {_fmt(pnl.restock_spend)}",
            "  _Inventory buy — cash → stock, not a profit cost. See /position._",
            _SEP,
        ]

    od = metrics.summarize_outstanding(debtor_rows)
    if od.outstanding_count:
        lines.append("💳 *OUTSTANDING DEBTORS*")
        if od.bar_count:
            lines.append(f"  🍺 Bar ({od.bar_count}):    {_fmt(od.bar_owed)}")
        if od.rooms_count:
            lines.append(f"  🛏 Rooms ({od.rooms_count}):  {_fmt(od.rooms_owed)}")
        lines.append(f"  Total Owed:    {_fmt(od.total_owed)}")
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

    # All allocation arithmetic lives in metrics.compute_allocation (shared with
    # the dashboard); the percentages come from DB settings.
    buffer_pct, restock_pct = _get_alloc_pcts()
    draw_pct, reinvest_pct, float_pct = _get_profit_dist_pcts()
    alloc = metrics.compute_allocation(
        sales_rows, room_rows, expense_rows, _cost_price_map(),
        buffer_pct=buffer_pct, restock_pct=restock_pct,
        draw_pct=draw_pct, reinvest_pct=reinvest_pct, float_pct=float_pct,
        pit_low_rate=PIT_LOW_RATE, pit_high_rate=PIT_HIGH_RATE,
    )
    bar_rev, room_rev, total_rev = alloc.bar_rev, alloc.room_rev, alloc.total_rev
    room_by_type = alloc.room_by_type
    total_pct = alloc.total_pct
    buffer_amt, restock_amt, total_save = alloc.buffer_amt, alloc.restock_amt, alloc.total_save
    bar_share, room_share = alloc.bar_share, alloc.room_share
    cost_of_drinks = alloc.cost_of_drinks
    total_salary, bar_salary_amt, room_salary_amt = alloc.total_salary, alloc.bar_salary_amt, alloc.room_salary_amt
    other_exp, total_outgoings = alloc.other_exp, alloc.total_outgoings
    working_capital, after_setaside, burn_rate = alloc.working_capital, alloc.after_setaside, alloc.burn_rate
    restock_total = alloc.restock_total
    dist_total_pct = alloc.dist_total_pct
    draw_amt, reinvest_amt, float_amt, unallocated = alloc.draw_amt, alloc.reinvest_amt, alloc.float_amt, alloc.unallocated
    pit_low, pit_high = alloc.pit_low_amt, alloc.pit_high_amt

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

    # Profit distribution — amounts precomputed in metrics.compute_allocation.
    if after_setaside > 0 and dist_total_pct > 0:
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
# Optional "as of" anchor date (stored "YYYY-MM-DD 00:00:00"). When set, the cash
# estimate counts only flows on/after this date — opening is your real balance on
# that day, so earlier months are ignored (already baked into it). Lets you
# re-anchor safely each period without double-counting history. Empty = all-time.
CASH_OPENING_DATE_KEY = "cash_opening_date"


def generate_position_report() -> str:
    """What you have right now — cash first. Profit is a one-line footnote.

    Three figures that must never be conflated:
    - Cash at hand — a *running balance* estimate; draws and restock DO reduce it.
    - Stock value and receivables — point-in-time assets.
    - Profit — a *performance* figure (revenue − COGS − expenses); draws/restock
      are excluded. Shown small at the bottom; it is NOT money in your pocket.

    All arithmetic lives in metrics.compute_cash_position (shared with the dashboard).
    """
    sales_all   = _active(db.read_all("sales"))
    rooms_all   = _active(db.read_all("rooms"))
    expense_all = _active(db.read_all("expenses"))
    draws_all   = _active(db.read_all("owner_draws"))
    debtor_rows = db.read_all("debtors")
    now = datetime.now()

    # Stock value on hand (asset), plus the cash anchor (opening balance + date).
    stock_value = round(sum(i["stock_value"] for i in inv.get_inventory_summary()), 2)
    try:
        opening = float(db.get_setting(CASH_OPENING_KEY, "0") or 0)
    except (TypeError, ValueError):
        opening = 0.0
    anchor_dt = _parse_ts(db.get_setting(CASH_OPENING_DATE_KEY, "") or "")

    pos = metrics.compute_cash_position(
        sales_all, rooms_all, expense_all, draws_all, debtor_rows,
        stock_value=stock_value, opening=opening, anchor_dt=anchor_dt,
        cost_map=_cost_price_map(), now=now,
    )

    if anchor_dt:
        open_label = f"  Balance on {anchor_dt.strftime('%d %b %Y')}: {_fmt(opening)}"
        since_note = [f"  _Counting everything since {anchor_dt.strftime('%d %b %Y')}._"]
    else:
        open_label = f"  Opening balance:    {_fmt(opening)}"
        since_note = []

    lines = [
        f"🧭 *{HOTEL_NAME} — What You Have*",
        f"📅 As of {now.strftime('%d %b %Y %H:%M')}",
        _SEP,
        "🏦 *CASH AT HAND* _(estimate)_",
        *since_note,
        open_label,
        f"  + Collected sales:  {_fmt(pos.collected)}",
        f"  − Expenses:         {_fmt(pos.opex_cash)}",
        f"  − Stock purchases:  {_fmt(pos.restock_cash)}",
        f"  − Owner draws:      {_fmt(pos.draws_cash)}",
        f"  = *💰 {_fmt(pos.cash)}*",
        "  _Money in, minus stock bought, expenses & draws._",
        _SEP,
        "📦 *STOCK VALUE ON HAND* _(asset)_",
        f"  Bar + store @ cost:  {_fmt(pos.stock_value)}",
        "💳 *OWED TO US* _(receivables)_",
        f"  Outstanding debtors: {_fmt(pos.receivables)}  ({pos.outstanding_count} owing)",
        _SEP,
        f"📊 _Profit (not cash): this month {_fmt(pos.month_profit)} · all-time {_fmt(pos.profit_all)}_",
        "_Performance only — owner draws & stock buys are excluded from profit._",
        _SEP,
        "_Anchor cash to a real balance: /position set <amount> <YYYY-MM-DD>_",
        f"_Generated {now.strftime('%d %b %Y %H:%M')}_",
    ]
    return "\n".join(lines)


def generate_draws_report(
    for_date: date | None = None,
    for_month: tuple[int, int] | None = None,
    all_time: bool = False,
) -> str:
    """List owner draws (equity withdrawals) for a period, newest first, with IDs.

    Draws are deliberately NOT expenses — they reduce cash, never profit. This
    report is the audit trail: every withdrawal, who recorded it, and the total.
    """
    draw_rows = _active(db.read_all("owner_draws"))
    draw_rows = _apply_filter(draw_rows, for_date, for_month, all_time)
    label = _period_label(for_date, for_month, all_time)

    if not draw_rows:
        return (
            f"💵 *Owner Draws — {label}*\n\n"
            "No owner draws recorded for this period.\n"
            "_Log one with_ `/draw <amount> [note]`."
        )

    draw_rows = sorted(draw_rows, key=lambda r: str(r.get("timestamp") or ""), reverse=True)
    total = sum(float(r["amount"]) for r in draw_rows)

    lines = [f"💵 *Owner Draws — {label}*", _SEP]
    for r in draw_rows:
        ts = str(r.get("timestamp") or "")[:10]
        desc = str(r.get("description") or "").strip()
        by = str(r.get("recorded_by") or "").strip()
        tail = f" _{_esc(desc)}_" if desc else ""
        if by:
            tail += f" _(by {_esc(by)})_"
        lines.append(f"`[{r['id']}]` {ts}  {_fmt(float(r['amount']))}{tail}")
    lines += [
        _SEP,
        f"*Total drawn: {_fmt(total)}*  ({len(draw_rows)} draw{'s' if len(draw_rows) != 1 else ''})",
        "_Owner draws reduce cash, never profit. Remove one with_ `/delete draw <id>`.",
        f"_Generated {datetime.now().strftime('%d %b %Y %H:%M')}_",
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
_VELOCITY_WINDOW_DAYS = 30 # default rolling window for velocity/turn/budget
_TREND_WINDOW_DAYS = 7     # recent window compared against the 30-day pace
_TREND_WEIGHT = 0.6        # how hard to lean on the recent pace when accelerating

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


def _in_date_window(rows: list[dict], start: date, end: date) -> list[dict]:
    """Rows whose timestamp falls within [start, end] inclusive."""
    out = []
    for r in rows:
        dt = _parse_ts(r.get("timestamp"))
        if dt and start <= dt.date() <= end:
            out.append(r)
    return out


def _days_covered(rows: list[dict], end: date, window: int) -> int:
    """Days of history the window actually spans (1..window).

    Anchored to the earliest sale inside the window, so a bar younger than
    `window` days isn't divided by the full window (which would understate
    velocity)."""
    dates = [dt.date() for dt in (_parse_ts(r.get("timestamp")) for r in rows) if dt]
    if not dates:
        return window
    span = (end - min(dates)).days + 1
    return max(1, min(window, span))


def generate_restock_plan(
    for_date: date | None = None,
    for_month: tuple[int, int] | None = None,
    all_time: bool = False,
) -> str:
    """Advisory weekly restock plan: transfers, reorder tiers, budget, warnings.

    Default view uses a rolling trailing-30-day window for velocity, revenue,
    turn ratio and budget — so the plan doesn't collapse in the first days of a
    new month (it leans on the prior month's sales until the new month fills
    in). An explicit period (for_date/for_month/all_time) instead filters to
    that span and treats it as a 4-week month, for historical look-back.
    """
    items = inv.get_inventory_summary()
    if not items:
        return "📦 Inventory is empty. Use /restock to add drinks first."

    # No explicit period → rolling trailing-30-day window (the intended view).
    rolling = for_date is None and for_month is None and not all_time
    today = for_date or datetime.now().date()
    week_no = min((today.day - 1) // 7 + 1, 4)

    all_sales = _active(db.read_all("sales"))
    all_exp   = _active(db.read_all("expenses"))
    qty_recent: dict[str, int] = {}   # units sold in the trailing _TREND_WINDOW_DAYS
    recent_weeks = 0.0                # set only in rolling mode
    if rolling:
        cutoff = today - timedelta(days=_VELOCITY_WINDOW_DAYS - 1)
        sales_rows = _in_date_window(all_sales, cutoff, today)
        exp_rows   = _in_date_window(all_exp, cutoff, today)
        days_covered = _days_covered(sales_rows, today, _VELOCITY_WINDOW_DAYS)
        weeks_in_period = days_covered / 7.0
        label = (f"last {_VELOCITY_WINDOW_DAYS} days" if days_covered >= _VELOCITY_WINDOW_DAYS
                 else f"last {days_covered} days (limited history)")
        # Recent pace, to lean orders ahead of an accelerating (e.g. festive) trend.
        recent_cut = today - timedelta(days=_TREND_WINDOW_DAYS - 1)
        recent_weeks = min(_TREND_WINDOW_DAYS, days_covered) / 7.0
        for r in _in_date_window(sales_rows, recent_cut, today):
            nm = str(r["drink_name"]).lower()
            qty_recent[nm] = qty_recent.get(nm, 0) + int(r["quantity"])
    else:
        sales_rows = _apply_filter(all_sales, for_date, for_month, all_time)
        exp_rows   = _apply_filter(all_exp, for_date, for_month, all_time)
        weeks_in_period = 4.0   # month ≈ 4 weeks (legacy basis for explicit periods)
        label = _period_label(for_date, for_month, all_time)

    # Sales velocity + bar revenue for the window/period.
    qty_by_drink: dict[str, int] = {}
    bar_revenue = 0.0
    for r in sales_rows:
        nm = str(r["drink_name"]).lower()
        qty_by_drink[nm] = qty_by_drink.get(nm, 0) + int(r["quantity"])
        bar_revenue += float(r["total_revenue"])

    # Restock already spent in the window/period (bar / "restock" expenses).
    restock_spent = sum(
        float(e["amount"]) for e in exp_rows
        if e.get("account") == "bar" and str(e.get("category", "")).lower() == "restock"
    )

    stock_value = sum(i["stock_value"] for i in items)
    budget_cap = round(bar_revenue * RESTOCK_BUDGET_PCT / 100, 2)

    transfers: list[tuple] = []   # (name, move, bar, new_bar, result_cover)
    now: list[tuple] = []         # (name, monthly, weekly, rng, est, weeks, margin, trend_up)
    soon: list[tuple] = []
    hold: list[tuple] = []        # (name, reason, monthly)
    pricing: list[tuple] = []     # (name, idle_units, tied_value)
    stockouts: list[str] = []     # selling items with bar == 0

    for it in items:
        name, bar, store = it["drink"], it["bar_stock"], it["store_stock"]
        cost, price, margin = it["cost_price"], it["selling_price"], it["margin"]
        units = qty_by_drink.get(name.lower(), 0)
        base_weekly = units / weeks_in_period if weeks_in_period else 0.0
        # Trend-aware lean: when the recent week is outpacing the 30-day average
        # (demand accelerating into a season), order ahead of the curve instead
        # of trailing it. Decelerating items just ride the steadier 30-day pace.
        weekly = base_weekly
        trend_up = False
        if rolling and recent_weeks:
            recent_weekly = qty_recent.get(name.lower(), 0) / recent_weeks
            if recent_weekly > base_weekly:
                weekly = base_weekly + _TREND_WEIGHT * (recent_weekly - base_weekly)
                trend_up = recent_weekly > base_weekly * 1.25
        monthly = round(weekly * 4)   # 4-week-equivalent rate (slow-mover basis)
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
        row = (name, monthly, weekly, rng + note, round(low_qty * cost, 2), weeks, margin, trend_up)

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
        for name, mo, wk, rng, est, weeks, mg, tr in now:
            mark = " 📈" if tr else ""
            L.append(f"  • *{_esc(name)}* — {weeks:.1f}wk left · {wk:.0f}/wk → *{rng}* ~{_fmt(est)}{mark}")
    else:
        L.append("  _Nothing critical. 🎉_")

    L += [_SEP, "*4️⃣ 🟡 Reorder Soon*"]
    if soon:
        for name, mo, wk, rng, est, weeks, mg, tr in soon:
            mark = " 📈" if tr else ""
            L.append(f"  • *{_esc(name)}* — {weeks:.1f}wk · {wk:.0f}/wk → *{rng}*{mark}")
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

    L += [_SEP, "_Tip: do transfers first, then only the 🔴 items this week._"]
    if any(r[7] for r in now) or any(r[7] for r in soon):
        L.append("_📈 = selling faster than its 30-day pace; order leans on the recent week._")
    L.append(f"_Generated {datetime.now().strftime('%d %b %Y %H:%M')}_")
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
