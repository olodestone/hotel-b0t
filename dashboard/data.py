"""
Read-only view models for the dashboard.

Every number here is produced by fetching rows from ``database.read_all`` and
running them through the shared ``metrics`` calc core — the exact same path the
Telegram reports use. Nothing here writes to the database or formats Telegram
strings, so the dashboard and the bot can never disagree.
"""
from __future__ import annotations

from datetime import datetime

import database as db
import inventory as inv
import metrics
from config import PIT_LOW_RATE, PIT_HIGH_RATE
from reports import (  # presentation label + setting keys + pct getters, shared with the bot
    _period_label,
    _get_alloc_pcts,
    _get_profit_dist_pcts,
    CASH_OPENING_KEY,
    CASH_OPENING_DATE_KEY,
)


def parse_period(arg: str | None):
    """Map a period string to (for_date, for_month, all_time), matching the bot.

    blank → current month · ``today`` · ``YYYY-MM-DD`` · ``YYYY-MM`` · ``all``.
    """
    arg = (arg or "").strip().lower()
    if not arg:
        return (None, None, False)
    if arg == "all":
        return (None, None, True)
    if arg == "today":
        return (datetime.now().date(), None, False)
    try:
        return (datetime.strptime(arg, "%Y-%m-%d").date(), None, False)
    except ValueError:
        pass
    try:
        d = datetime.strptime(arg, "%Y-%m")
        return (None, (d.year, d.month), False)
    except ValueError:
        pass
    return (None, None, False)  # unrecognised → current month


def _cost_price_map() -> dict:
    return {r["drink_name"].lower(): float(r["cost_price"]) for r in db.read_all("inventory")}


def _revenue_by_day(sales_rows, room_rows) -> list[dict]:
    """Daily bar/room revenue series for the trend chart, sorted by date."""
    by_day: dict[str, dict] = {}
    for r in sales_rows:
        dt = metrics.parse_ts(r.get("timestamp"))
        if dt:
            by_day.setdefault(dt.date().isoformat(), {"bar": 0.0, "rooms": 0.0})["bar"] += float(r["total_revenue"])
    for r in room_rows:
        dt = metrics.parse_ts(r.get("timestamp"))
        if dt:
            by_day.setdefault(dt.date().isoformat(), {"bar": 0.0, "rooms": 0.0})["rooms"] += float(r["total_revenue"])
    return [
        {"date": d, "bar": round(v["bar"], 2), "rooms": round(v["rooms"], 2),
         "total": round(v["bar"] + v["rooms"], 2)}
        for d, v in sorted(by_day.items())
    ]


def cash_position() -> metrics.CashPosition:
    """Cash-at-hand / assets / profit snapshot — 'as of now', independent of the
    selected period. Same numbers as the bot's /position."""
    sales_all   = metrics.active(db.read_all("sales"))
    rooms_all   = metrics.active(db.read_all("rooms"))
    expense_all = metrics.active(db.read_all("expenses"))
    draws_all   = metrics.active(db.read_all("owner_draws"))
    debtor_rows = db.read_all("debtors")
    stock_value = round(sum(i["stock_value"] for i in inv.get_inventory_summary()), 2)
    try:
        opening = float(db.get_setting(CASH_OPENING_KEY, "0") or 0)
    except (TypeError, ValueError):
        opening = 0.0
    anchor_dt = metrics.parse_ts(db.get_setting(CASH_OPENING_DATE_KEY, "") or "")
    return metrics.compute_cash_position(
        sales_all, rooms_all, expense_all, draws_all, debtor_rows,
        stock_value=stock_value, opening=opening, anchor_dt=anchor_dt,
        cost_map=_cost_price_map(), now=datetime.now(),
    )


def _recorder(r) -> str:
    """Normalised recorder name (blank/missing → 'Unknown'), matching staff_breakdown."""
    return (r.get("recorded_by") or "Unknown").strip() or "Unknown"


def _by_time_desc(rows) -> list[dict]:
    """Newest-first ordering for a raw record table (string sort is chronological
    for ``YYYY-MM-DD HH:MM:SS`` timestamps)."""
    return sorted(rows, key=lambda r: str(r.get("timestamp") or ""), reverse=True)


def _outstanding_debtors(debtor_rows) -> list[dict]:
    """Outstanding debtors (matches /debtors + the CSV export) with each one's
    remaining balance and the staff responsible for the sale, newest first.

    ``staff`` is the debt's ``staff_name`` (who sold/booked) — NOT ``recorded_by``
    (who keyed the entry, usually an admin). Mirrors the bot's "(by …)" tag.
    """
    out = []
    for r in debtor_rows:
        if r.get("status") != "outstanding":
            continue
        rem = round(float(r["amount"]) - float(r.get("amount_paid") or 0), 2)
        staff = str(r.get("staff_name") or "").strip()
        out.append({**r, "remaining": rem, "staff": staff.title() if staff else "—"})
    return _by_time_desc(out)


def _sales_breakdown(sales_rows, cost_map) -> list[dict]:
    """Per-drink qty / revenue / cost / profit for the period (matches /sales_report)."""
    totals: dict[str, dict] = {}
    for r in sales_rows:
        name = r["drink_name"].lower()
        qty = int(r["quantity"])
        d = totals.setdefault(name, {"drink": name.title(), "qty": 0, "revenue": 0.0, "cost": 0.0})
        d["qty"] += qty
        d["revenue"] += float(r["total_revenue"])
        d["cost"] += cost_map.get(name, 0.0) * qty
    out = []
    for name in sorted(totals):
        d = totals[name]
        d["profit"] = round(d["revenue"] - d["cost"], 2)
        out.append(d)
    return out


def dashboard_view(period_arg: str | None, staff: str | None = None) -> dict:
    """Assemble everything the dashboard home page needs for one period.

    ``staff`` optionally focuses the per-staff drill-down on one recorder. It
    scopes only revenue/activity (sales-by-drink, transaction counts) — the
    hotel-wide figures (P&L, cash, allocation, stock, debtors) are never
    filtered by staff, since they aren't attributable to a single recorder.
    """
    for_date, for_month, all_time = parse_period(period_arg)
    cost_map = _cost_price_map()

    sales = metrics.apply_filter(metrics.active(db.read_all("sales")), for_date, for_month, all_time)
    rooms = metrics.apply_filter(metrics.active(db.read_all("rooms")), for_date, for_month, all_time)
    expenses = metrics.apply_filter(metrics.active(db.read_all("expenses")), for_date, for_month, all_time)
    debtors = db.read_all("debtors")

    pnl = metrics.compute_pnl(sales, rooms, expenses, cost_map)
    owed = metrics.summarize_outstanding(debtors)

    buffer_pct, restock_pct = _get_alloc_pcts()
    draw_pct, reinvest_pct, float_pct = _get_profit_dist_pcts()
    allocation = metrics.compute_allocation(
        sales, rooms, expenses, cost_map,
        buffer_pct=buffer_pct, restock_pct=restock_pct,
        draw_pct=draw_pct, reinvest_pct=reinvest_pct, float_pct=float_pct,
        pit_low_rate=PIT_LOW_RATE, pit_high_rate=PIT_HIGH_RATE,
    )

    stock = inv.get_inventory_summary()
    stock_value = round(sum(i["stock_value"] for i in stock), 2)
    low_stock = [i for i in stock if i["is_low"]]

    staff_rows = metrics.staff_breakdown(sales, rooms)
    # Optional per-staff focus — scopes ONLY this recorder's revenue/activity.
    selected_staff = (staff or "").strip() or None
    staff_detail = None
    if selected_staff:
        s_sales = [r for r in sales if _recorder(r) == selected_staff]
        s_rooms = [r for r in rooms if _recorder(r) == selected_staff]
        staff_detail = {
            "name": selected_staff,
            "bar_rev": metrics.sum_revenue(s_sales),
            "room_rev": metrics.sum_revenue(s_rooms),
            "total_rev": metrics.sum_revenue(s_sales) + metrics.sum_revenue(s_rooms),
            "drink_txns": len(s_sales),
            "room_txns": len(s_rooms),
            "sales_breakdown": _sales_breakdown(s_sales, cost_map),
            "trend": _revenue_by_day(s_sales, s_rooms),
        }

    view = {
        "period_label": _period_label(for_date, for_month, all_time),
        # Pre-fills for the date / month period pickers — non-empty only when the
        # current period IS that kind of selection (so the inputs reflect where
        # you are, and the quick-segment buttons stay the source of truth otherwise).
        "picker": {
            "date": for_date.isoformat() if for_date else "",
            "month": (f"{for_month[0]:04d}-{for_month[1]:02d}" if for_month else ""),
        },
        # Raw records for the period — viewable in-browser instead of CSV-only.
        # Sales/Rooms/Expenses follow the selected period; debtors are outstanding
        # balances "as of now" (matches /debtors and the debtors export).
        "ledger": {
            "sales": _by_time_desc(sales),
            "rooms": _by_time_desc(rooms),
            "expenses": _by_time_desc(expenses),
            "debtors": _outstanding_debtors(debtors),
        },
        "pnl": pnl,
        "owed": owed,
        "allocation": allocation,
        "alloc_pcts": {"buffer": buffer_pct, "restock": restock_pct,
                       "draw": draw_pct, "reinvest": reinvest_pct, "float": float_pct},
        "sales_breakdown": _sales_breakdown(sales, cost_map),
        "staff": staff_rows,
        "selected_staff": selected_staff,
        "staff_detail": staff_detail,
        "stock": stock,
        "stock_value": stock_value,
        "low_stock": low_stock,
        "trend": _revenue_by_day(sales, rooms),
        "position": cash_position(),  # 'as of now', not period-filtered
    }
    return view


# ── CSV export ────────────────────────────────────────────────────────

# kind → (columns, table, is_period_filtered)
_EXPORTS = {
    "sales":    (["timestamp", "drink_name", "quantity", "selling_price", "total_revenue", "recorded_by"], "sales", True),
    "rooms":    (["timestamp", "room_type", "quantity", "price_per_night", "nights", "total_revenue", "recorded_by"], "rooms", True),
    "expenses": (["timestamp", "account", "category", "amount", "description"], "expenses", True),
    "debtors":  (["timestamp", "account", "name", "staff_name", "amount", "amount_paid", "description", "status"], "debtors", False),
}


def export_dataset(kind: str, period_arg: str | None):
    """Return (columns, rows) for a CSV export, or None for an unknown kind."""
    spec = _EXPORTS.get(kind)
    if not spec:
        return None
    cols, table, period_filtered = spec
    if table == "debtors":
        rows = [r for r in db.read_all("debtors") if r.get("status") == "outstanding"]
    else:
        for_date, for_month, all_time = parse_period(period_arg)
        rows = metrics.apply_filter(metrics.active(db.read_all(table)), for_date, for_month, all_time)
    return cols, rows
