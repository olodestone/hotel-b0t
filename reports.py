"""
reports.py — Financial calculations and Telegram-formatted report strings.

All monetary values are in ₦ (Naira) — change the symbol in _fmt() if needed.
"""
from __future__ import annotations

from datetime import datetime, date
from typing import Any

import database as db
import inventory as inv
from config import HOTEL_NAME

_SEP = "─" * 30


def _fmt(amount: float) -> str:
    return f"₦{amount:,.2f}"


# ── Core aggregations ─────────────────────────────────────────────────

def _sum_revenue(rows: list[dict], key: str = "total_revenue") -> float:
    return sum(float(r[key]) for r in rows)


def _filter_by_date(rows: list[dict], target: date) -> list[dict]:
    result = []
    for r in rows:
        try:
            row_date = datetime.strptime(r["timestamp"], "%Y-%m-%d %H:%M:%S").date()
            if row_date == target:
                result.append(r)
        except (ValueError, KeyError):
            pass
    return result


def _cost_of_drinks_sold(sales_rows: list[dict]) -> float:
    """
    Match each sale to its current cost price from inventory.
    Note: uses current cost_price, not historical — acceptable for daily ops.
    """
    total = 0.0
    inventory_rows = {r["drink_name"].lower(): float(r["cost_price"]) for r in db.read_all("inventory")}
    for row in sales_rows:
        name = row["drink_name"].lower()
        qty = int(row["quantity"])
        cost = inventory_rows.get(name, 0.0)
        total += qty * cost
    return round(total, 2)


# ── Full report ───────────────────────────────────────────────────────

def generate_full_report(for_date: date | None = None) -> str:
    """
    Build the Telegram-formatted financial report with separate
    Bar and Rooms P&L sections, plus outstanding debtors.
    If for_date is given, filter to that day; otherwise all-time.
    """
    sales_rows = db.read_all("sales")
    room_rows = db.read_all("rooms")
    expense_rows = db.read_all("expenses")
    debtor_rows = db.read_all("debtors")

    label = "ALL-TIME"
    if for_date:
        sales_rows = _filter_by_date(sales_rows, for_date)
        room_rows = _filter_by_date(room_rows, for_date)
        expense_rows = _filter_by_date(expense_rows, for_date)
        label = for_date.strftime("%d %b %Y")

    bar_expenses = [r for r in expense_rows if r.get("account", "bar") == "bar"]
    room_expenses = [r for r in expense_rows if r.get("account", "rooms") == "rooms"]

    drink_revenue = _sum_revenue(sales_rows)
    cost_of_drinks = _cost_of_drinks_sold(sales_rows)
    bar_expense_total = _sum_revenue(bar_expenses, key="amount")
    bar_profit = drink_revenue - cost_of_drinks - bar_expense_total

    room_revenue = _sum_revenue(room_rows)
    room_expense_total = _sum_revenue(room_expenses, key="amount")
    room_profit = room_revenue - room_expense_total

    total_revenue = drink_revenue + room_revenue
    total_outgoings = cost_of_drinks + bar_expense_total + room_expense_total
    net_profit = total_revenue - total_outgoings

    bar_emoji = "📈" if bar_profit >= 0 else "📉"
    room_emoji = "📈" if room_profit >= 0 else "📉"
    net_emoji = "📈" if net_profit >= 0 else "📉"

    lines = [
        f"🏨 *{HOTEL_NAME} — Financial Report*",
        f"📅 Period: {label}",
        _SEP,
        f"🍺 *BAR ACCOUNT*",
        f"  Revenue:       {_fmt(drink_revenue)}",
        f"  Cost of Stock: {_fmt(cost_of_drinks)}",
        f"  Expenses:      {_fmt(bar_expense_total)}",
        f"  {bar_emoji} Profit:      *{_fmt(bar_profit)}*",
    ]

    if bar_expenses:
        cat_totals: dict[str, float] = {}
        for r in bar_expenses:
            cat = r["category"].title()
            cat_totals[cat] = cat_totals.get(cat, 0.0) + float(r["amount"])
        lines.append("  _Expense breakdown:_")
        for cat, amt in sorted(cat_totals.items()):
            lines.append(f"    • {cat}: {_fmt(amt)}")

    lines += [
        _SEP,
        f"🛏 *ROOMS ACCOUNT*",
        f"  Revenue:       {_fmt(room_revenue)}",
        f"  Expenses:      {_fmt(room_expense_total)}",
        f"  {room_emoji} Profit:      *{_fmt(room_profit)}*",
    ]

    if room_expenses:
        cat_totals = {}
        for r in room_expenses:
            cat = r["category"].title()
            cat_totals[cat] = cat_totals.get(cat, 0.0) + float(r["amount"])
        lines.append("  _Expense breakdown:_")
        for cat, amt in sorted(cat_totals.items()):
            lines.append(f"    • {cat}: {_fmt(amt)}")

    lines += [
        _SEP,
        f"📊 *COMBINED*",
        f"  Total Revenue:   {_fmt(total_revenue)}",
        f"  Total Outgoings: {_fmt(total_outgoings)}",
        f"  {net_emoji} *Net Profit:    {_fmt(net_profit)}*",
        _SEP,
    ]

    # Debtors summary (all-time outstanding, not date-filtered)
    outstanding = [r for r in debtor_rows if r["status"] == "outstanding"]
    if outstanding:
        bar_debtors = [r for r in outstanding if r["account"] == "bar"]
        room_debtors = [r for r in outstanding if r["account"] == "rooms"]
        bar_owed = sum(float(r["amount"]) for r in bar_debtors)
        room_owed = sum(float(r["amount"]) for r in room_debtors)
        lines.append(f"💳 *OUTSTANDING DEBTORS*")
        if bar_debtors:
            lines.append(f"  🍺 Bar ({len(bar_debtors)}):    {_fmt(bar_owed)}")
        if room_debtors:
            lines.append(f"  🛏 Rooms ({len(room_debtors)}):  {_fmt(room_owed)}")
        lines.append(f"  Total Owed:    {_fmt(bar_owed + room_owed)}")
        lines.append(_SEP)

    lines.append(f"_Generated {datetime.now().strftime('%d %b %Y %H:%M')}_")
    return "\n".join(lines)


# ── Debtors report ─────────────────────────────────────────────────────

def generate_debtors_report(account: str | None = None) -> str:
    """List all outstanding debtors, optionally filtered to one account."""
    rows = db.get_debtors(account=account)

    if not rows:
        label = f"{account.title()} " if account else ""
        return f"✅ No outstanding {label}debtors."

    bar_rows = [r for r in rows if r["account"] == "bar"]
    room_rows = [r for r in rows if r["account"] == "rooms"]

    lines = [
        f"🏨 *{HOTEL_NAME} — Outstanding Debtors*",
        _SEP,
    ]

    if bar_rows and (account is None or account == "bar"):
        lines.append("🍺 *BAR*")
        for r in bar_rows:
            note = f" — {r['description']}" if r.get("description") else ""
            lines.append(f"  • {r['name'].title()}: {_fmt(float(r['amount']))}{note}")
        lines.append(f"  *Total: {_fmt(sum(float(r['amount']) for r in bar_rows))}*")
        lines.append("")

    if room_rows and (account is None or account == "rooms"):
        lines.append("🛏 *ROOMS*")
        for r in room_rows:
            note = f" — {r['description']}" if r.get("description") else ""
            lines.append(f"  • {r['name'].title()}: {_fmt(float(r['amount']))}{note}")
        lines.append(f"  *Total: {_fmt(sum(float(r['amount']) for r in room_rows))}*")

    lines.append(_SEP)
    lines.append(f"_Updated {datetime.now().strftime('%d %b %Y %H:%M')}_")
    return "\n".join(lines)


# ── Stock report ──────────────────────────────────────────────────────

def generate_stock_report() -> str:
    items = inv.get_inventory_summary()
    if not items:
        return "📦 Inventory is empty. Use /restock to add drinks."

    lines = [
        f"🏨 *{HOTEL_NAME} — Inventory*",
        _SEP,
        f"{'Drink':<18} {'Stock':>6} {'Cost':>10} {'Value':>12}",
        _SEP,
    ]

    total_value = 0.0
    low_stock_items = []

    for item in items:
        flag = " ⚠️" if item["is_low"] else ""
        line = (
            f"{item['drink'][:17]:<18} "
            f"{item['closing_stock']:>6} "
            f"{_fmt(item['cost_price']):>10} "
            f"{_fmt(item['stock_value']):>12}"
            f"{flag}"
        )
        lines.append(f"`{line}`")
        total_value += item["stock_value"]
        if item["is_low"]:
            low_stock_items.append(item["drink"])

    lines.append(_SEP)
    lines.append(f"*Total Stock Value: {_fmt(total_value)}*")

    if low_stock_items:
        lines.append("")
        lines.append("⚠️ *Low Stock Alerts:*")
        for name in low_stock_items:
            lines.append(f"  • {name}")

    lines.append(f"\n_Updated {datetime.now().strftime('%d %b %Y %H:%M')}_")
    return "\n".join(lines)


# ── Daily report (for scheduler) ─────────────────────────────────────

def generate_daily_report() -> str:
    today = datetime.now().date()
    return generate_full_report(for_date=today)
