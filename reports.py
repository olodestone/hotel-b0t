"""
reports.py — Financial calculations and Telegram-formatted report strings.

All monetary values are in ₦ (Naira) — change the symbol in _fmt() if needed.
"""
from __future__ import annotations

from calendar import monthrange
from datetime import datetime, date, timedelta
from math import ceil
from typing import Any

import clock
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


def _pct(value: float | None, dash: str = "—") -> str:
    """Format a percentage; `dash` when the ratio is undefined (no denominator)."""
    return dash if value is None else f"{value:.1f}%"


def _days(value: float | None, dash: str = "—") -> str:
    if value is None:
        return dash
    return f"{value:.0f} day" + ("" if f"{value:.0f}" == "1" else "s")


def _plural(count: int, word: str, suffix: str = "s") -> str:
    """Pluralise, honouring the -y → -ies rule.

    "2 entrys" appeared in the month-end header. A consonant before the y takes
    -ies (entry → entries); a vowel keeps it (day → days), which is why the rule
    checks the preceding letter rather than just the ending.
    """
    if count == 1:
        return f"{count} {word}"
    if suffix == "s" and word.endswith("y") and word[-2:-1].lower() not in "aeiou":
        return f"{count} {word[:-1]}ies"
    return f"{count} {word}{suffix}"


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
    now = clock.now()
    if for_date:
        return for_date.strftime("%d %b %Y")
    if all_time:
        return "ALL-TIME"
    year, month = for_month if for_month else (now.year, now.month)
    label = datetime(year, month, 1).strftime("%B %Y")
    return f"{label} (current month)" if (year, month) == (now.year, now.month) else label


# Number of lettable rooms — the denominator for occupancy and RevPAR.
TOTAL_ROOMS_KEY = "total_rooms"


def _total_rooms() -> int:
    try:
        return int(float(db.get_setting(TOTAL_ROOMS_KEY, "0") or 0))
    except (TypeError, ValueError):
        return 0


def _room_type_counts() -> dict[str, int]:
    """Rooms per type (lower-cased) — the per-type RevPAR denominator."""
    return db.get_all_room_type_counts()


def _room_type_hours() -> dict[str, float]:
    """Hours per stay-unit for the hourly room types (lower-cased).

    Empty for a hotel that only sells nights, which is why configuring nothing
    leaves every existing figure exactly where it was.
    """
    return db.get_all_room_type_hours()


def _range_label(start: date, end: date) -> str:
    """Name a window the way an owner would say it out loud."""
    if start == end:
        return start.strftime("%d %b %Y")
    if start.day == 1 and end.day == monthrange(end.year, end.month)[1] \
            and (start.year, start.month) == (end.year, end.month):
        return start.strftime("%B %Y")
    if (start.year, start.month) == (end.year, end.month):
        return f"{start.day}–{end.day} {end.strftime('%b %Y')}"
    return f"{start.strftime('%d %b')} – {end.strftime('%d %b %Y')}"


def _room_windows(
    for_date: date | None, for_month: tuple[int, int] | None, all_time: bool,
    for_week: date | None, rows: list[dict],
) -> tuple[tuple[date, date, str], tuple[date, date, str] | None]:
    """The window to report, and the one to compare it against.

    Both windows are truncated to the same elapsed length. Comparing two
    finished days of a new month against a whole finished month would make
    every month start look like a collapse — the prior window is cut to match
    so the two are genuinely like-for-like.

    All-time has no meaningful predecessor, so it compares against nothing.
    """
    today = clock.now().date()

    if for_date:
        prev = for_date - timedelta(days=1)
        return ((for_date, for_date, _range_label(for_date, for_date)),
                (prev, prev, _range_label(prev, prev)))

    if for_week:
        monday = for_week - timedelta(days=for_week.weekday())
        sunday = monday + timedelta(days=6)
        end = min(sunday, today)
        label = "This week" if monday <= today <= sunday else _range_label(monday, sunday)
        p_start = monday - timedelta(days=7)
        p_end = p_start + (end - monday)
        return ((monday, end, label), (p_start, p_end, _range_label(p_start, p_end)))

    if all_time:
        seen = [dt.date() for dt in (_parse_ts(r.get("timestamp")) for r in rows) if dt]
        return ((min(seen) if seen else today, today, "ALL-TIME"), None)

    year, month = for_month if for_month else (today.year, today.month)
    start = date(year, month, 1)
    end = min(date(year, month, monthrange(year, month)[1]), today)
    label = start.strftime("%B %Y")
    if (year, month) == (today.year, today.month):
        label += " (current month)"

    p_year, p_month = (year - 1, 12) if month == 1 else (year, month - 1)
    p_start = date(p_year, p_month, 1)
    p_end = min(p_start + (end - start), date(p_year, p_month, monthrange(p_year, p_month)[1]))
    return ((start, end, label), (p_start, p_end, _range_label(p_start, p_end)))


# Direction pairs → the tone to lead the verdict with. Presentation only; the
# verdict wording itself is metrics.TREND_VERDICTS so the two surfaces agree.
_TREND_ICONS = {
    ("up", "up"): "✅", ("flat", "up"): "✅", ("down", "up"): "✅",
    ("up", "flat"): "💡", ("flat", "flat"): "➖", ("down", "flat"): "➖",
    ("up", "down"): "⚠️", ("flat", "down"): "⚠️", ("down", "down"): "🔻",
}


def _delta_tag(pct: float) -> str:
    """'▲ 29%' / '▼ 12%' / '→ flat', with the same dead band metrics judges on."""
    if pct > metrics.TREND_BAND:
        return f"▲ {pct:,.0f}%"
    if pct < -metrics.TREND_BAND:
        return f"▼ {abs(pct):,.0f}%"
    return "→ flat"


def _period_days(for_date: date | None, for_month: tuple[int, int] | None,
                 all_time: bool, rows: list[dict] | tuple = ()) -> int:
    """Days a period actually covers — the denominator for occupancy/RevPAR.

    The current month counts only elapsed days: charging a half-finished month
    for its unsold future nights would understate occupancy every time.
    """
    now = clock.now()
    if for_date:
        return 1
    if all_time:
        days = [dt.date() for dt in (_parse_ts(r.get("timestamp")) for r in rows) if dt]
        return (now.date() - min(days)).days + 1 if days else 0
    year, month = for_month if for_month else (now.year, now.month)
    if (year, month) == (now.year, now.month):
        return now.day
    return monthrange(year, month)[1]


def _window_bounds(for_date: date | None, for_month: tuple[int, int] | None,
                   all_time: bool, rows: list[dict] | tuple = ()) -> tuple[date, date]:
    """The [start, end] the same arguments select, as real dates.

    `_apply_filter` answers "is this row in the period"; the accrual needs the
    period itself, because it charges by elapsed days rather than by rows.
    """
    today = clock.now().date()
    if for_date:
        return for_date, for_date
    if all_time:
        seen = [dt.date() for dt in (_parse_ts(r.get("timestamp")) for r in rows) if dt]
        return (min(seen) if seen else today), today
    year, month = for_month if for_month else (today.year, today.month)
    start = date(year, month, 1)
    return start, min(date(year, month, monthrange(year, month)[1]), today)


def _obligations() -> list:
    """The accrual register as metrics.Obligation values."""
    out = []
    # Retired bills are included: their past accruals are part of the
    # reserve and of months already reported.
    for r in db.get_obligations(include_inactive=True):
        try:
            start = datetime.strptime(str(r.get("start_date") or "")[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            start = date(2000, 1, 1)      # no start recorded: accrue from always
        try:
            retired = datetime.strptime(str(r.get("retired_on") or "")[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            retired = None
        out.append(metrics.Obligation(
            id=int(r["id"]), name=str(r["name"]),
            account=str(r.get("account") or "rooms").lower(),
            category=str(r.get("category") or "maintenance").lower(),
            expected_amount=float(r.get("expected_amount") or 0),
            months=int(r.get("months") or 0),
            start_date=start, active=bool(r.get("active", True)),
            retired_on=retired,
        ))
    return out


def _with_accrual(expense_rows: list[dict], for_date, for_month, all_time,
                  rows_for_span: list[dict] | tuple = ()) -> list[dict]:
    """Expense rows plus this window's periodic accrual.

    One helper, used by every surface that computes profit, so the accrual can
    never appear on one screen and be missing from the next. The synthesised
    rows carry a real account and category, so they flow through the Bar/Rooms/
    Overhead split and the category breakdown without anything downstream
    needing to know they aren't real entries.
    """
    obligations = _obligations()
    if not obligations:
        return expense_rows
    start, end = _window_bounds(for_date, for_month, all_time,
                               rows_for_span or expense_rows)
    return expense_rows + metrics.accrual_rows(obligations, start, end)


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
    pnl = metrics.compute_pnl(
        sales_rows, room_rows,
        _with_accrual(expense_rows, for_date, for_month, all_time, room_rows),
        _cost_price_map())
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
            f"_Generated {clock.now().strftime('%d %b %Y %H:%M')}_",
        ]
        return "\n".join(lines)

    bar_emoji = "📈" if bar.profit >= 0 else "📉"
    room_emoji = "📈" if rooms.profit >= 0 else "📉"
    net_emoji = "📈" if pnl.net_profit >= 0 else "📉"

    lines = [
        f"🏨 *{HOTEL_NAME} — Financial Report*",
        f"📅 Period: {label}",
        *_verification_note(for_date, for_month, all_time),
        _SEP,
        "🍺 *BAR ACCOUNT*",
        f"  Revenue: {_fmt(bar.revenue)}",
        f"  Cost of Stock Sold: {_fmt(bar.cogs)}",
        f"  Gross Profit: {_fmt(bar.gross_profit)}  ({_pct(bar.gross_margin_pct)} margin)",
        f"  Salaries: {_fmt(bar.salary)}",
        f"  Other Expenses: {_fmt(bar.other_expense)}",
        f"  {bar_emoji} *Profit: {_fmt(bar.profit)}*  ({_pct(bar.net_margin_pct)} net)",
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
        f"  {room_emoji} *Profit: {_fmt(rooms.profit)}*  ({_pct(rooms.net_margin_pct)} net)",
    ]

    # Yield metrics — only meaningful once the hotel's room count is recorded.
    rm = metrics.compute_room_metrics(
        room_rows, _total_rooms(), _period_days(for_date, for_month, all_time, room_rows),
        hours_by_type=_room_type_hours(),
    )
    if rm.room_nights_sold:
        lines.append(f"  Room-nights sold: {rm.room_nights_sold}  ·  ADR {_fmt(rm.adr)}")
        if rm.available_room_nights:
            lines.append(f"  Occupancy: {_pct(rm.occupancy_pct)}  ·  RevPAR {_fmt(rm.revpar)}")
        else:
            lines.append("  _Set the room count with_ `/setrooms <n>` _for occupancy & RevPAR._")
    if rm.has_short_stay:
        # Kept on its own line: lets are not nights and their rate is not ADR.
        lines.append(f"  Short-stay lets: {rm.short_lets}  ·  Avg per let {_fmt(rm.arl)}")

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
        f"  Net Margin:      {_pct(pnl.net_margin_pct)}  _(₦ kept per ₦100 of sales)_",
    ]
    # Read beside net profit, never instead of it. The money was spent — this
    # answers the different question of whether the month traded badly or
    # something simply broke.
    if pnl.has_one_offs:
        lines += [
            f"  🌩 One-off costs:  {_fmt(pnl.irregular_spend)}",
            f"  *Underlying:      {_fmt(pnl.underlying_profit)}*  "
            f"({_pct(pnl.underlying_margin_pct)})  _— if nothing had broken_",
        ]
    lines.append(_SEP)

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

    lines.append(f"_Generated {clock.now().strftime('%d %b %Y %H:%M')}_")
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
        *_verification_note(for_date, for_month, all_time),
        f"Transactions: {len(sales_rows)}",
        "```",
        header,
        divider,
        *rows_out,
        divider,
        total_line,
        "```",
        f"_Generated {clock.now().strftime('%d %b %Y %H:%M')}_",
    ]
    return "\n".join(lines)


# ── Expense report ────────────────────────────────────────────────────

def generate_expense_report(
    for_date: date | None = None,
    for_month: tuple[int, int] | None = None,
    all_time: bool = False,
) -> str:
    """Expense breakdown by account and category."""
    every_row = _active(db.read_all("expenses"))
    expense_rows = _apply_filter(every_row, for_date, for_month, all_time)
    label = _period_label(for_date, for_month, all_time)

    obligations = _obligations()
    start, end = _window_bounds(for_date, for_month, all_time, expense_rows)
    accruals = metrics.accrual_rows(obligations, start, end)

    if not expense_rows and not accruals:
        return f"💸 *Expense Report — {label}*\n\nNo expenses recorded for this period."

    # Restock (inventory purchases) is a cash → stock movement, not an operating
    # expense — shown separately so it never inflates the expense total.
    restock_total = _restock_spend(expense_rows)
    all_rows = expense_rows
    capital = metrics.capital_rows(all_rows)
    # Periodic *payments* are reserve draws, not costs, so they come from the
    # raw rows — operating_expenses drops them on the way past.
    periodic = metrics.periodic_rows(all_rows)
    # A reserve is a running balance, so it reads every row ever recorded;
    # windowing it would report the month's movement as the whole pot.
    reserve = metrics.compute_reserve(obligations, _active(every_row), end)

    expense_rows = _operating_expenses(expense_rows + accruals)
    bar_expenses  = [r for r in expense_rows if metrics.expense_account(r) == "bar"]
    room_expenses = [r for r in expense_rows if metrics.expense_account(r) == "rooms"]
    over_expenses = [r for r in expense_rows if metrics.expense_account(r) == "overhead"]

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
                out.append(f"    {_entry_ref(e)}  {_fmt(float(e['amount']))}{_note(e)}")
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
                out.append(f"    {_entry_ref(e)}  {_fmt(float(e['amount']))}{_note(e)}")

        out.append(f"  *Subtotal: {_fmt(cat_total)}*")
        return out

    sections = [s for s in (
        _section(bar_expenses,  "🍺 *BAR EXPENSES*"),
        _section(room_expenses, "🛏 *ROOMS EXPENSES*"),
        _section(over_expenses, "🏢 *OVERHEAD*"),
    ) if s]
    grand_total = sum(float(r["amount"]) for r in expense_rows)

    # ① OPERATING P&L — operating + periodic only. Capital and inventory were
    #    already filtered out above, so nothing here can inflate a margin.
    lines = [f"💸 *Expense Report — {label}*", _SEP,
             "*① OPERATING — in the P&L*"]
    for i, s in enumerate(sections):
        if i:
            lines.append(_SEP)
        lines += s
    if not sections:
        lines.append("  _No operating expenses this period._")
    lines += [_SEP, f"*Operating total: {_fmt(grand_total)}*"]

    # ② CAPITAL SPEND — cash out, deliberately outside every margin.
    lines += _capital_section(capital)

    # ③ RESERVE — what the accruals built up, and what was drawn from it.
    lines += _reserve_section(reserve, periodic)

    if restock_total > 0:
        lines += [
            _SEP,
            f"📦 Stock purchased: {_fmt(restock_total)} _(inventory buy — cash to stock, not a cost)_",
        ]
    flagged = metrics.review_rows(all_rows)
    if flagged:
        lines.append(f"🔎 _{_plural(len(flagged), 'entry')} flagged for review "
                     f"— tap_ ⚙️ Manage → 🔎 Review")
    lines.append(f"_Generated {clock.now().strftime('%d %b %Y %H:%M')}_")
    return "\n".join(lines)


def _capital_section(rows: list[dict]) -> list[str]:
    """Asset purchases: cash out, never a cost.

    Listed in full rather than as a total, because the month-end check is a
    per-item question — do I now own something I did not own before? — and it
    cannot be asked of a lump sum.
    """
    if not rows:
        return []
    total = sum(float(r["amount"]) for r in rows)
    out = [_SEP, "*② CAPITAL SPEND — cash out, not a cost*"]
    for e in sorted(rows, key=lambda r: -float(r["amount"])):
        ts = str(e.get("timestamp", ""))[:10]
        desc = str(e.get("description") or "").strip()
        out.append(f"  `[{e['id']}]` {ts}  {_fmt(float(e['amount']))} — "
                   f"{_esc(str(e['account']).title())}/{_esc(str(e['category']).title())}"
                   + (f" _{_esc(desc)}_" if desc else ""))
    out += [
        f"  *Capital total: {_fmt(total)}*",
        "  _Excluded from profit, margins and GOPPAR. It still left the bank,_",
        "  _so /position counts it against cash._",
    ]
    return out


def _entry_ref(e: dict) -> str:
    """`[id] date` for a real entry; an accrual has neither to show."""
    if e.get("accrual"):
        return "  🔁 _accrual_"
    return f"`[{e['id']}]` {str(e.get('timestamp', ''))[:10]}"


def _reserve_section(reserve, payments: list[dict]) -> list[str]:
    """What the accruals have built up, and what has been drawn from it.

    The reserve is the whole point of accruing: a bill that lands every six
    months is a cost of all six, so each month puts its share aside and the
    invoice, when it comes, is a draw rather than a disaster. A negative
    balance is the useful signal — it means the bill arrived before enough
    months had paid into it, which is worth seeing rather than smoothing over.
    """
    if not reserve.lines and not payments:
        return []

    out = [_SEP, "*③ RESERVE — periodic bills*"]
    if reserve.monthly_total:
        out.append(f"  Accruing {_fmt(reserve.monthly_total)}/month across "
                   f"{_plural(len([l for l in reserve.lines if l.obligation.active]), 'bill')}")

    for line in sorted(reserve.lines, key=lambda l: -l.obligation.monthly_share):
        ob = line.obligation
        mark = " ⚠️" if line.materially_short else ""
        out.append(
            f"  • *{_esc(ob.name)}* — {_fmt(ob.expected_amount)} ÷ {ob.months} "
            f"= {_fmt(ob.monthly_share)}/mo"
        )
        out.append(
            f"      _set aside {_fmt(line.accrued)} · paid {_fmt(line.paid)} · "
            f"balance {_fmt(line.balance)}{mark}_"
        )

    out.append(f"  *Reserve balance: {_fmt(reserve.balance)}*")
    if reserve.unlinked_paid:
        out.append(f"  _Includes {_fmt(reserve.unlinked_paid)} paid without a bill attached._")

    if payments:
        out.append("  *Drawn this period:*")
        for e in sorted(payments, key=lambda r: -float(r["amount"])):
            ts = str(e.get("timestamp", ""))[:10]
            out.append(f"    `[{e['id']}]` {ts}  {_fmt(float(e['amount']))} — "
                       f"{_esc(str(e['category']).title())}")

    under = reserve.underfunded
    if under:
        out.append("  ⚠️ _Paid out before enough months had been set aside: "
                   + _esc(", ".join(l.obligation.name for l in under)) + "._")
    out.append("  _The monthly share is in section ①. These payments are draws,_")
    out.append("  _not costs — counting both would charge the bill twice._")
    return out


def generate_review_report(
    for_date: date | None = None,
    for_month: tuple[int, int] | None = None,
    all_time: bool = False,
) -> str:
    """The month-end check: every entry big enough to be capital, plus the flagged ones.

    Two lists, because they are two different mistakes. A large entry may have
    been classified perfectly and still deserves the repair-vs-replace question
    asked of it once — the same tradesman produces a repair and an asset, and
    the invoice looks identical. A flagged entry is one the person recording it
    already said they were unsure about.
    """
    rows = _apply_filter(_active(db.read_all("expenses")), for_date, for_month, all_time)
    label = _period_label(for_date, for_month, all_time)
    threshold = _capital_threshold()

    big = sorted(
        [r for r in rows
         if float(r["amount"]) >= threshold
         and metrics.expense_class(r) in metrics.PNL_CLASSES],
        key=lambda r: -float(r["amount"]),
    )
    flagged = [r for r in metrics.review_rows(rows) if r not in big]

    lines = [f"🔎 *Month-End Check — {label}*", _SEP]
    if not big and not flagged:
        lines.append(f"✅ Nothing to review. No entry reached {_fmt(threshold)} "
                     "and nothing was flagged.")
        lines.append(f"_Generated {clock.now().strftime('%d %b %Y %H:%M')}_")
        return "\n".join(lines)

    if big:
        lines += [
            f"*{_plural(len(big), 'entry')} at or above {_fmt(threshold)}*",
            "_Two questions for each:_",
            "_1. Do you now own something you did not own before?_ "
            "_No — maintenance. Yes — capital._",
            "_2. Was it a one-off nobody could have forecast?_ "
            "_If so, tag it — it stays a real cost, but the month can then be_ "
            "_read without it, and it sizes your buffer._",
            "",
        ]
        for e in big:
            lines += _review_line(e)
        lines.append(_SEP)

    if flagged:
        lines += [f"*{_plural(len(flagged), 'entry')} flagged as unsure*", ""]
        for e in flagged:
            lines += _review_line(e)
        lines.append(_SEP)

    lines += [
        "_Reclassify from_ ⚙️ Manage → 🔎 Review_, or_ `/reclassify <id> <field> <value>`",
        f"_Generated {clock.now().strftime('%d %b %Y %H:%M')}_",
    ]
    return "\n".join(lines)


def _review_line(e: dict) -> list[str]:
    desc = str(e.get("description") or "").strip()
    cls = metrics.expense_class(e)
    return [
        f"  `[{e['id']}]` *{_fmt(float(e['amount']))}* — "
        f"{_esc(str(e['account']).title())}/{_esc(str(e['category']).title())}"
        f"  ·  {cls.title()}",
        f"      _{str(e.get('timestamp', ''))[:10]}"
        + (f" · {_esc(desc)}" if desc else "") + "_",
    ]


def _capital_threshold() -> float:
    try:
        return float(db.get_setting("capital_threshold", "") or metrics.CAPITAL_THRESHOLD)
    except (TypeError, ValueError):
        return metrics.CAPITAL_THRESHOLD


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

    # Aggregate drink + room activity by recorder (shared with the dashboard).
    staff_rows = metrics.staff_breakdown(sales_rows, room_rows)

    if not staff_rows:
        return f"👥 *Staff Report — {label}*\n\nNo activity recorded for this period."

    col_name = max(len(d["name"]) for d in staff_rows) + 1
    col_name = max(col_name, 10)
    header = f"{'Staff':<{col_name}} {'DrinkTxn':>9}  {'DrinkRev':>13}  {'RoomTxn':>8}  {'RoomRev':>13}"
    divider = "-" * len(header)

    rows_out = []
    t_dtxn, t_drev, t_rtxn, t_rrev = 0, 0.0, 0, 0.0
    for d in staff_rows:
        rows_out.append(
            f"{d['name']:<{col_name}} {d['drink_txns']:>9}  {_fmt(d['drink_revenue']):>13}  "
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
        f"_Generated {clock.now().strftime('%d %b %Y %H:%M')}_",
    ]
    return "\n".join(lines)


# ── Daily summary ─────────────────────────────────────────────────────

def generate_daily_summary(target: date | None = None, staff_view: bool = False) -> str:
    """Compact one-screen overview of a single day's activity."""
    today = target or clock.now().date()
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
        lines.append(f"\n_Generated {clock.now().strftime('%d %b %Y %H:%M')}_")
        return "\n".join(lines)

    expense_rows = _filter_by_date(_active(db.read_all("expenses")), today)
    outstanding = [r for r in db.read_all("debtors") if r["status"] == "outstanding"]

    bar_rev = _sum_revenue(sales_rows)
    room_rev = _sum_revenue(room_rows)
    total_rev = bar_rev + room_rev

    # Restock (inventory purchase) is a cash/stock movement, not a P&L cost.
    # One day accrues one day's share of each periodic bill — small, but the
    # alternative is a summary that disagrees with the month it belongs to.
    expense_rows = _with_accrual(expense_rows, today, None, False)
    op_expenses = _operating_expenses(expense_rows)
    bar_exp = sum(float(r["amount"]) for r in op_expenses if metrics.expense_account(r) == "bar")
    room_exp = sum(float(r["amount"]) for r in op_expenses if metrics.expense_account(r) == "rooms")
    over_exp = sum(float(r["amount"]) for r in op_expenses if metrics.expense_account(r) == "overhead")
    cost_drinks = _cost_of_drinks_sold(sales_rows)
    total_out = bar_exp + room_exp + over_exp + cost_drinks
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
        *( [f"  Overhead:      {_fmt(over_exp)}"] if over_exp else [] ),
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

    lines.append(f"\n_Generated {clock.now().strftime('%d %b %Y %H:%M')}_")
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

def _contingency_block(buffer_pct: float) -> list[str]:
    """Is the buffer big enough for what actually goes wrong?

    Sized from history rather than a forecast: nobody can name the compressor
    that fails next March, but the tagged one-offs say what the unforeseeable
    has really been costing. Advisory on purpose — it reports the gap and
    leaves the percentage alone, because silently re-sizing an allocation the
    owner set deliberately is how a tool stops being trusted.
    """
    c = metrics.compute_contingency(
        _active(db.read_all("expenses")), _active(db.read_all("sales")),
        _active(db.read_all("rooms")), buffer_pct, clock.now(),
    )
    if not c.has_history:
        return [
            "",
            "  🌩 _No one-off costs tagged yet, so there is nothing to size the_",
            "  _buffer against. Tag them as they happen — ⚙️ Manage → 🔎 Review._",
        ]

    out = ["", f"  🌩 *Contingency check* _(last {_plural(c.months_observed, 'month')})_"]
    if not c.reliable:
        # An average off two or three months is an accident, not a rate.
        return out + [
            f"      {_fmt(c.total_irregular)} of one-off costs so far",
            f"      _Too little history to size a buffer from — "
            f"{_plural(metrics.MIN_CONTINGENCY_MONTHS, 'month')} makes the_",
            "      _average mean something. Keep tagging them._",
        ]
    out.append(f"      One-offs have cost {_fmt(c.monthly_average)}/month on average")
    out.append(f"      Your {c.buffer_pct:g}% buffer sets aside {_fmt(c.buffer_monthly)}/month")
    if c.covered:
        out.append(f"      ✅ _Covered, with {_fmt(abs(c.gap))}/month to spare._")
    else:
        out.append(f"      ⚠️ _Short by {_fmt(c.gap)}/month — "
                   f"{c.suggested_pct:g}% would cover it._")
        out.append(f"      _Raise it with_ `/setallocation buffer {c.suggested_pct:g}`")
    if c.biggest:
        top = c.biggest[0]
        desc = str(top.get("description") or top.get("category") or "").strip()
        out.append(f"      _Biggest: {_fmt(float(top['amount']))} — {_esc(desc.title())}_")
    return out


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
    expense_rows = _with_accrual(expense_rows, for_date, for_month, all_time, room_rows)
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
    ]
    lines += _contingency_block(buffer_pct)
    lines += [
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
        f"_Generated {clock.now().strftime('%d %b %Y %H:%M')}_",
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
    now = clock.now()

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
        cost_map=_cost_price_map(), now=now, obligations=_obligations(),
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
        # Capital never reduced profit, but it certainly reduced the bank.
        *([f"  − Asset purchases:  {_fmt(pos.capital_cash)}"] if pos.capital_cash else []),
        # Drawn from the reserve, but the money still left the bank.
        *([f"  − Periodic bills:   {_fmt(pos.periodic_cash)}"] if pos.periodic_cash else []),
        f"  − Owner draws:      {_fmt(pos.draws_cash)}",
        f"  = *💰 {_fmt(pos.cash)}*",
        "  _Money in, minus stock bought, expenses & draws._",
        _SEP,
        "📦 *STOCK VALUE ON HAND* _(asset)_",
        f"  Bar + store @ cost:  {_fmt(pos.stock_value)}",
        "💳 *OWED TO US* _(receivables)_",
        f"  Outstanding debtors: {_fmt(pos.receivables)}  ({pos.outstanding_count} owing)",
    ]
    lines += _unmatched_debt_block(pos)

    # Supplier credit is a claim on cash you already have — show it so the
    # headline balance is never mistaken for money that's free to spend.
    payables = db.get_outstanding_payables()
    if payables:
        owed = round(sum(float(r["amount"]) - float(r.get("amount_paid") or 0) for r in payables), 2)
        lines += [
            "🧾 *WE OWE* _(supplier credit)_",
            f"  Unpaid invoices:     {_fmt(owed)}  ({len(payables)} outstanding)",
            f"  _Cash free of supplier claims: {_fmt(round(pos.cash - owed, 2))}_",
        ]

    lines += [
        _SEP,
        f"📊 _Profit (not cash): this month {_fmt(pos.month_profit)} · all-time {_fmt(pos.profit_all)}_",
        "_Performance only — owner draws & stock buys are excluded from profit._",
        _SEP,
        "_Anchor cash to a real balance: /position set <amount> <YYYY-MM-DD>_",
        f"_Generated {now.strftime('%d %b %Y %H:%M')}_",
    ]
    return "\n".join(lines)


# ── Cash conversion cycle / working capital ───────────────────────────

_CCC_WINDOW_DAYS = 30

# How far back to widen the row scan so a stay that began before the window is
# still credited for the nights it occupies inside it. Longer than any
# plausible stay at this hotel, and cheap — the rows are clipped afterwards.
_MAX_STAY_LOOKBACK = 30

_DIO_BASIS_NOTE = {
    "snapshots": "from daily stock snapshots",
    "current": "from today's shelf — snapshots are still building history",
    "none": "nothing sold in the window",
}


def generate_cashcycle_report(window_days: int = _CCC_WINDOW_DAYS) -> str:
    """How many days cash is locked up between paying for stock and banking it.

    All arithmetic lives in metrics.compute_working_capital / compute_break_even
    (shared with the dashboard).
    """
    now = clock.now()
    cost_map = _cost_price_map()
    stock_rows = inv.get_inventory_summary()
    since = (now.date() - timedelta(days=window_days - 1)).strftime("%Y-%m-%d")

    wc = metrics.compute_working_capital(
        sales_all=db.read_all("sales"),
        expense_all=db.read_all("expenses"),
        debtor_rows=db.read_all("debtors"),
        payment_rows=db.read_all("debtor_payments"),
        stock_rows=stock_rows,
        cost_map=cost_map,
        payable_rows=db.read_all("payables"),
        snapshot_rows=db.get_inventory_snapshots(since),
        window_days=window_days,
        now=now,
    )

    lines = [
        f"🔄 *{HOTEL_NAME} — Cash Conversion Cycle*",
        f"📅 Last {window_days} days · as of {now.strftime('%d %b %Y')}",
        _SEP,
        "⏱ *THE CYCLE*",
    ]

    if wc.ccc_days is None:
        lines += [
            "  _Not enough trading in this window to measure._",
            "  Record some sales and it fills in automatically.",
            _SEP,
        ]
    else:
        lines += [
            f"  ① Stock sits:        {_days(wc.dio_days)}",
            f"  ② Debtors pay in:  + {_days(wc.dso_days, '0 days')}",
            f"  ③ Supplier credit: − {_days(wc.dpo_days, '0 days')}",
            f"  = 🔄 *{wc.ccc_days:.0f} days of cash locked up*",
            f"  _Every ₦1 spent on stock comes back {wc.ccc_days:.0f} days later._",
            _SEP,
        ]

    lines += [
        "📦 *① STOCK (DIO)*",
        f"  Stock at cost:     {_fmt(wc.stock_value)}",
        f"  Sold at cost:      {_fmt(wc.cogs_window)}  ({_fmt(wc.daily_cogs)}/day)",
        f"  _Basis: {_DIO_BASIS_NOTE[wc.dio_basis]}._",
        _SEP,
        "💳 *② DEBTORS (DSO)*",
        f"  Owed to us now:    {_fmt(wc.receivables)}",
        f"  Sales on credit:   {_fmt(wc.credit_sales_window)}",
    ]
    if wc.collection_days is not None:
        basis = "in window" if wc.collection_basis == "window" else "all-time"
        lines.append(
            f"  Actual collection: {_days(wc.collection_days)} avg "
            f"({_plural(wc.settled_count, 'payment')}, {basis})"
        )
    if wc.receivables > 0:
        aged = "  ·  ".join(f"{b.label}: {_fmt(b.amount)}" for b in wc.aging if b.amount)
        if aged:
            lines.append(f"  Aging → {aged}")
        old = next((b for b in wc.aging if b.label.startswith("61")), None)
        if old and old.amount:
            lines.append(f"  ⚠️ {_fmt(old.amount)} is over 60 days old — chase it or write it off.")
    lines.append(_SEP)

    lines.append("🧾 *③ SUPPLIER CREDIT (DPO)*")
    if not wc.dpo_tracked:
        lines += [
            "  Not tracked — every restock is recorded as paid on the spot.",
            f"  Stock bought: {_fmt(wc.purchases_window)}",
            "  _Buying on terms is the cheapest way to shorten the cycle:_",
            "  _it costs nothing but a conversation with your supplier._",
            "  _Record credit purchases with_ `/restock_credit` _to measure it._",
        ]
    else:
        lines += [
            f"  Owed to suppliers: {_fmt(wc.payables_outstanding)}",
            f"  Stock bought:      {_fmt(wc.purchases_window)}",
        ]
    lines.append(_SEP)

    if wc.dead_stock:
        lines.append("💤 *IDLE STOCK* _(nothing sold this window)_")
        lines.append(f"  {len(wc.dead_stock)} drinks · {_fmt(wc.dead_stock_value)} tied up")
        for d in wc.dead_stock[:5]:
            lines.append(f"    • {_esc(d['drink'])}: {d['units']} units ({_fmt(d['value'])})")
        if len(wc.dead_stock) > 5:
            lines.append(f"    _…and {len(wc.dead_stock) - 5} more._")
        lines.append(_SEP)

    # Break-even and the room target run on a whole month — the horizon the owner
    # budgets to. On the first days of a new month there is nothing to measure
    # yet, and reporting "₦0 needed, ✅ covered" would read as reassurance when
    # nothing has happened. Fall back to the last month that actually traded.
    all_sales, all_rooms = _active(db.read_all("sales")), _active(db.read_all("rooms"))
    all_exp = _active(db.read_all("expenses"))

    obligations = _obligations()

    def _month(year, month):
        # Break-even and the room target both read a month's costs, so both
        # accrue that month's share — otherwise /cashcycle disagrees with the
        # /report for the same month by exactly the accrual.
        last = monthrange(year, month)[1]
        m_start, m_end = date(year, month, 1), min(date(year, month, last), now.date())
        return (_filter_by_month(all_sales, year, month),
                _filter_by_month(all_rooms, year, month),
                _filter_by_month(all_exp, year, month)
                + metrics.accrual_rows(obligations, m_start, m_end))

    be_year, be_month = now.year, now.month
    month_sales, month_rooms, month_exp = _month(be_year, be_month)
    month_label = now.strftime("%B %Y")
    if not (month_sales or month_rooms):
        prev = datetime(now.year, now.month, 1) - timedelta(days=1)
        be_year, be_month = prev.year, prev.month
        month_sales, month_rooms, month_exp = _month(be_year, be_month)
        month_label = f"{prev.strftime('%B %Y')} — no {now.strftime('%B')} entries yet"

    if not (month_sales or month_rooms):
        lines += [
            "⚖️ *BREAK-EVEN*",
            "  _No trading recorded yet — record some sales and this fills in._",
            _SEP,
            f"_Generated {now.strftime('%d %b %Y %H:%M')}_",
        ]
        return "\n".join(lines)

    be = metrics.compute_break_even(month_sales, month_exp, cost_map)

    lines.append(f"⚖️ *BAR BREAK-EVEN* _({month_label})_")
    if be.break_even_revenue is None:
        lines.append(f"  Bar fixed costs: {_fmt(be.fixed_costs)}")
        if be.actual_revenue <= 0:
            lines.append("  _No bar sales this period — no margin to work from._")
        else:
            lines.append("  _No positive gross margin yet — check cost vs selling prices._")
    else:
        verdict = "✅ above break-even" if be.surplus >= 0 else "🔻 below break-even"
        lines += [
            f"  Bar fixed costs: {_fmt(be.fixed_costs)}",
            f"  Bar gross margin:{be.gross_margin_ratio * 100:>7.1f}%",
            f"  Bar sales needed:{_fmt(be.break_even_revenue):>12}",
            f"  Actual bar sales:{_fmt(be.actual_revenue):>12}  → {verdict} by {_fmt(abs(be.surplus))}",
        ]
        if be.surplus >= 0:
            lines.append(f"  _Safety margin: {_pct(be.margin_of_safety_pct)} — how far bar sales can fall before a loss._")
        lines.append("  _Bar only — room revenue carries no stock cost, so blending the_")
        lines.append("  _two would flatter this number badly._")

    # What rooms have to bring in, once the bar has done its bit. This is the
    # month's real target: the shared overheads (rent, diesel, security, room
    # staff) exist whether or not the bar opens, so rooms carry them.
    rt = metrics.compute_rooms_target(month_sales, month_rooms, month_exp, cost_map)
    lines += [_SEP, f"🛏 *ROOMS MUST COVER* _({month_label})_"]
    if rt.bar_contribution >= 0:
        bar_line = f"  − Bar contribution:  {_fmt(rt.bar_contribution)}"
    else:
        bar_line = f"  + Bar shortfall:     {_fmt(abs(rt.bar_contribution))}  _(rooms cover this too)_"
    lines += [
        f"  Shared costs:        {_fmt(rt.shared_costs)}",
        bar_line,
        f"  = *Room sales needed: {_fmt(rt.room_sales_needed)}*",
        f"  Actual room sales:   {_fmt(rt.actual_room_revenue)}",
    ]
    if rt.covered:
        lines.append(f"  ✅ Covered — {_fmt(rt.surplus)} clear.")
    else:
        lines.append(f"  🔻 Short by {_fmt(abs(rt.surplus))} — the month isn't paid for yet.")
    lines.append("  _Rooms carry the shared overheads; a room sale has no stock cost._")

    lines += [_SEP, f"_Generated {now.strftime('%d %b %Y %H:%M')}_"]
    return "\n".join(lines)


# ── Stocktake variance (shrinkage) ────────────────────────────────────

def _unmatched_debt_block(pos) -> list[str]:
    """Debts with no sale behind them — the one thing that breaks the cash sum.

    A debt is meant to be recorded *alongside* the sale, not instead of it: the
    cash estimate treats revenue as collected unless a debt says otherwise. A
    debt on its own subtracts money that was never added, so the shortfall is
    reported here rather than quietly absorbed.
    """
    if not pos.unmatched_receivables:
        return []
    um = metrics.unmatched_debts(
        [r for r in db.read_all("debtors") if r.get("status") == "outstanding"],
        _active(db.read_all("sales")), _active(db.read_all("rooms")))
    out = [
        "",
        f"  ⚠️ *{_fmt(pos.unmatched_receivables)} of debt has no sale behind it.*",
        "  _A debt records who owes you. The sale records what they took._",
        "  _Both are needed, or the money never entered the books at all._",
    ]
    for r in um.rows[:5]:
        who = _esc(str(r.get("name", "")).title())
        out.append(f"    `[{r['id']}]` {str(r.get('timestamp',''))[:10]}  "
                   f"{_fmt(float(r['amount']))} — {who} "
                   f"({str(r.get('account','')).title()})")
    if len(um.rows) > 5:
        out.append(f"    _…and {len(um.rows) - 5} more._")
    out.append("  _Fix: record the missing sale for that day, then this clears._")
    return out


def _verification_note(for_date, for_month, all_time) -> list[str]:
    """A month with no stocktake is UNVERIFIED, and every report must say so.

    Silence would let an uncounted month read exactly like a clean one. The
    figures are still the best available — they are just the books talking to
    themselves, with no independent observation behind them.
    """
    if all_time:
        return []
    now = clock.now()
    year, month = ((for_date.year, for_date.month) if for_date
                   else for_month if for_month else (now.year, now.month))
    counted = any(str(r.get("timestamp", ""))[:7] == f"{year:04d}-{month:02d}"
                  for r in db.read_all("stock_counts"))
    if counted:
        return []
    return ["⚠️ _UNVERIFIED — no stocktake entered for this month._"]


def generate_count_sheet() -> str:
    """STEP 1 — the sheet that gets carried and filled in by hand.

    Blanks stay blank. A sheet pre-filled with what the books expect is not a
    count: whoever carries it will read the expected figure and tick it, and
    the one independent observation the business has is gone.
    """
    items = inv.get_inventory_summary()
    if not items:
        return "📋 *Count Sheet*\n\nNo stock items yet — add one with `/restock`."

    last = {}
    for r in db.read_all("stock_counts"):
        name = str(r["drink_name"]).lower()
        ts = str(r.get("timestamp", ""))[:10]
        if ts > last.get(name, ""):
            last[name] = ts

    width = max(len(i["drink"]) for i in items)
    width = max(min(width, 16), 8)
    header = (f"{'ITEM':<{width}} {'COST':>7} {'LAST':>10}  BAR   STORE  TOTAL")
    body = ""
    for i in sorted(items, key=lambda r: r["drink"]):
        name = i["drink"][:width]
        seen = last.get(i["drink"].lower(), "never")[-5:] if last.get(i["drink"].lower()) else "never"
        body += (f"{name:<{width}} {float(i['cost_price']):>7,.0f} {seen:>10}  "
                 f"____  ____   ____\n")

    return "\n".join([
        f"📋 *{HOTEL_NAME} — Count Sheet*",
        f"📅 {clock.now().strftime('%d %b %Y')}",
        _SEP,
        "```",
        header,
        body + "```",
        "_Count *bar* and *store* separately — a bottle moved and a bottle sold_",
        "_look identical in one total._",
        "_Never estimate. Never count from memory. Same day each month._",
        "",
        "When the sheet is filled, enter it with ⚙️ Manage → 📦 Stocktake.",
    ])


def generate_variance_report(
    for_date: date | None = None,
    for_month: tuple[int, int] | None = None,
    all_time: bool = False,
) -> str:
    """STEPS 3-4 — variance per item, then the summary and the trend.

    Reports variance only. No count is ever attributed to a person: the figure
    is about the process, and naming whoever happened to hold the sheet turns a
    control into an accusation.
    """
    all_counts = db.read_all("stock_counts")
    rows = _apply_filter(all_counts, for_date, for_month, all_time)
    label = _period_label(for_date, for_month, all_time)

    if not rows:
        return (
            f"🔍 *Stock Variance — {label}*\n\n"
            "⚠️ *This month is UNVERIFIED.*\n"
            "No stocktake has been entered, so every stock and bar figure for "
            "the period is the books talking to themselves.\n\n"
            "Start with ⚙️ Manage → 📦 Stocktake."
        )

    cost_map = _cost_price_map()
    sales = _apply_filter(_active(db.read_all("sales")), for_date, for_month, all_time)
    cogs = metrics.cost_of_drinks_sold(sales, cost_map)
    vs = metrics.summarize_variance(rows, cost_map, cogs)

    lines = [
        f"🔍 *{HOTEL_NAME} — Stock Variance*",
        f"📅 Period: {label}",
        _SEP,
        f"  Counts taken: {vs.counts} across {vs.drinks} items",
        _SEP,
        "*BY ITEM* _(worst by value first)_",
    ]

    width = max((len(d["drink"]) for d in vs.by_drink), default=8)
    width = max(min(width, 14), 8)
    table = f"{'ITEM':<{width}} {'EXP':>5} {'CNT':>5} {'VAR':>5} {'₦ VALUE':>9}\n"
    for d in vs.by_drink:
        table += (f"{d['drink'][:width]:<{width}} {d['expected']:>5} {d['counted']:>5} "
                  f"{d['units']:>+5} {d['value']:>9,.0f}\n")
    lines.append("```\n" + table + "```")

    for d in vs.by_drink:
        if d["units"] == 0:
            continue
        lines.append(f"  {d['status']} *{_esc(d['drink'])}* {d['pct']:+.1f}% — {d['status_note']}")

    lines += [_SEP, "*SUMMARY*"]
    if vs.shrink_units:
        pct = f" ({_pct(vs.shrink_pct_of_cogs)} of stock sold)" if vs.cogs else ""
        lines.append(f"  🔻 *Shrinkage: {_fmt(abs(vs.shrink_value))}*{pct}")
    # A surplus is not a clean count. More units than the books expect is the
    # same leak seen from the other side — sales that were never rung up.
    if vs.surplus_units:
        lines += [
            f"  🔺 *Surplus: {vs.surplus_units} units — {_fmt(vs.surplus_value)}*",
            "  _Not good news: usually sales that were never recorded, or a_",
            "  _purchase logged twice. Investigate it like a shortage._",
        ]
    if vs.clean:
        lines.append("  🎯 Every count matched exactly.")

    if vs.worst:
        lines += ["", "  *Worst by value:*"]
        for d in vs.worst:
            lines.append(f"    {d['status']} {_esc(d['drink'])} — {_fmt(abs(d['value']))} "
                         f"({d['units']:+} units)")

    lines += _shrinkage_trend_block(all_counts, cost_map)
    lines += [
        _SEP,
        "_Count the same day each month, before opening — a variance only means_",
        "_something if the count is routine._",
        f"_Generated {clock.now().strftime('%d %b %Y %H:%M')}_",
    ]
    return "\n".join(lines)


def _shrinkage_trend_block(all_counts: list[dict], cost_map: dict) -> list[str]:
    """Shrinkage % across the last three months — one month is not a trend."""
    now = clock.now()
    all_sales = _active(db.read_all("sales"))
    cogs_by_month = {}
    for yr, mo in {(int(str(r["timestamp"])[:4]), int(str(r["timestamp"])[5:7]))
                   for r in all_counts if str(r.get("timestamp", ""))[:7]}:
        cogs_by_month[(yr, mo)] = metrics.cost_of_drinks_sold(
            _filter_by_month(all_sales, yr, mo), cost_map)

    trend = metrics.variance_trend(all_counts, cost_map, cogs_by_month, now)
    if sum(1 for _l, p, _v in trend if p is not None) < 2:
        return []

    out = [_SEP, "*TREND* _(shrinkage as % of stock sold)_"]
    parts = []
    for label, pct, _value in trend:
        parts.append(f"{label} —" if pct is None else f"{label} {pct:.1f}%")
    out.append("  " + "  ·  ".join(parts))

    known = [(l, p) for l, p, _v in trend if p is not None]
    if len(known) >= 2:
        first, last = known[0][1], known[-1][1]
        if last > first + 0.5:
            out.append("  🔻 _Getting worse — losses are growing against sales._")
        elif last < first - 0.5:
            out.append("  ✅ _Improving._")
        else:
            out.append("  ➖ _Holding steady._")
    if any(p is None for _l, p, _v in trend):
        out.append("  _A dash is a month with no count — unverified, not clean._")
    return out


# ── Room audit ────────────────────────────────────────────────────────

def audit_sheet(days: list, year: int, month: int) -> str:
    """STEP 2 — every room on each sampled date, occupied and vacant.

    The vacant lines are the exercise. An occupied room that was logged proves
    nothing; a room the system swears was empty is where an unlogged night
    hides, and it can only be seen by printing the empties alongside.
    """
    all_rooms = _active(db.read_all("rooms"))
    hours = _room_type_hours()
    total = _total_rooms() or sum(_room_type_counts().values())

    lines = [f"🔎 *{HOTEL_NAME} — Room Audit*",
             f"📅 {date(year, month, 1).strftime('%B %Y')} — "
             f"{_plural(len(days), 'day')} drawn at random",
             _SEP]
    if not total:
        lines.append("⚠️ _Set your room count first — `/setrooms <n>` — or the_")
        lines.append("_vacant lines cannot be worked out._")
        return "\n".join(lines)

    for day in days:
        d = metrics.build_audit_day(all_rooms, day, total, hours)
        lines.append(f"*{day.strftime('%a %d %b')}*")
        rows = "```\nTYPE            RATE      ✓\n"
        for e in d.logged:
            rate = e.revenue / e.rooms if e.rooms else 0.0
            rows += f"{e.room_type[:14]:<14}  {rate:>8,.0f}  __\n"
        for _ in range(d.vacant):
            rows += f"{'VACANT (per system)':<14}{'':>10}  __\n"
        lines.append(rows + "```")
        lines.append(f"  _{d.nights_logged} logged · {d.vacant} shown vacant "
                     f"of {total} rooms_")
    lines += [
        _SEP,
        "_Check each line against the physical register. Mark a vacant room_",
        "_that was actually occupied, and any rate that differs from what was_",
        "_charged. Then enter the totals._",
    ]
    return "\n".join(lines)


def generate_room_audit_report(result, audit_rows=()) -> str:
    """STEP 4 — capture rate, rate variance and the monthly leak it implies."""
    lines = [f"🔎 *{HOTEL_NAME} — Audit Result*", _SEP,
             f"  Nights logged .......... {result.nights_logged}",
             f"  Nights actual .......... {result.nights_actual}",
             f"  Capture rate ........... *{_pct(result.capture_pct)}*  _(target 100%)_"]

    if result.variance_count:
        lines.append(f"  Rate variance .......... {_fmt(result.rate_variance)} "
                     f"across {_plural(result.variance_count, 'entry')}")
    if result.missing:
        lines.append(f"  Estimated monthly leak . *{_fmt(result.monthly_leak)}*")
        lines.append("  _The gap, scaled to a month at the rate you actually achieve._")

    if not result.trustworthy:
        lines += [
            _SEP,
            f"🔴 *Capture is below {metrics.CAPTURE_FLOOR:g}% — do not act on any "
            "pricing decision yet.*",
            "_A rate rise on nights you are not collecting widens the gap rather_",
            "_than closing it. Fix the recording first._",
        ]
    elif result.capture_pct >= 100:
        lines += [_SEP, "✅ _Every night in the register was in the system._"]
    else:
        lines += [_SEP, "🟡 _Close, but not everything is being captured._"]

    trend = metrics.capture_trend(audit_rows)
    if len(trend) >= 2:
        lines += [_SEP, "*CAPTURE TREND*",
                  "  " + "  ·  ".join(f"{d[-5:]} {p:.0f}%" for d, p in trend)]

    lines += _rate_spread_block()
    lines.append(f"_Generated {clock.now().strftime('%d %b %Y %H:%M')}_")
    return "\n".join(lines)


def _rate_spread_block(for_month: tuple[int, int] | None = None) -> list[str]:
    """STEP 5 — runs on the full month, always, and needs no audit input.

    A single flat rate across a whole month is the finding. Real trade produces
    walk-ins, regulars, negotiated stays and the odd favour; a perfectly even
    rate means discounts are being given off-book, or the rate is not captured
    as charged.
    """
    now = clock.now()
    year, month = for_month if for_month else (now.year, now.month)
    rows = _filter_by_month(_active(db.read_all("rooms")), year, month)
    spreads = metrics.rate_spread(rows, _room_type_hours())
    if not spreads:
        return []

    out = [_SEP, "*RATE SPREAD* _(full month)_"]
    for s in spreads:
        out.append(f"  • *{_esc(s.room_type)}* — {_plural(s.nights, 'night')}")
        out.append(f"      _min {_fmt(s.min_rate)} · max {_fmt(s.max_rate)} · "
                   f"mode {_fmt(s.mode_rate)} · {s.distinct} distinct_")
        if s.suspicious:
            out.append("      🔴 _One single rate across the whole month. That is_")
            out.append("      _not normal pricing — either discounts are going_")
            out.append("      _off-book, or rates are not captured as charged._")
    return out


# ── Room performance (occupancy / ADR / RevPAR) ───────────────────────

def generate_room_stats_report(
    for_date: date | None = None,
    for_month: tuple[int, int] | None = None,
    all_time: bool = False,
    for_week: date | None = None,
) -> str:
    """Occupancy, ADR and RevPAR — read against the previous like-for-like period.

    One period's RevPAR is a number; two make it a signal. The direction of
    occupancy against the direction of RevPAR is what says raise, hold or cut.
    """
    all_rooms = _active(db.read_all("rooms"))
    (start, end, label), prior = _room_windows(for_date, for_month, all_time, for_week, all_rooms)
    room_rows = metrics.filter_by_range(all_rooms, start, end)
    days = max((end - start).days + 1, 0)

    counts = _room_type_counts()
    total_rooms = _total_rooms()
    derived = sum(counts.values())
    rooms_from_types = not total_rooms and derived > 0
    if rooms_from_types:
        total_rooms = derived

    hours = _room_type_hours()
    rm = metrics.compute_room_metrics(room_rows, total_rooms, days, rooms_by_type=counts,
                                      hours_by_type=hours)

    header = f"📅 Period: {label}"
    if prior:
        header = f"📅 {label}  vs  {prior[2]}"

    lines = [f"🛏 *{HOTEL_NAME} — Room Performance*", header, _SEP]

    if not room_rows:
        lines.append("No room bookings recorded for this period.")
        return "\n".join(lines)

    # GOPPAR needs the whole P&L, not just rooms — RevPAR cannot see fuel, wages
    # or restocking, which is exactly what makes the pair worth reading together.
    all_sales = _active(db.read_all("sales"))
    all_expenses = _active(db.read_all("expenses"))
    cost_map = _cost_price_map()

    obligations = _obligations()

    def _goppar_for(w_start, w_end, room_slice, available):
        # Both windows accrue on their own elapsed days, so a like-for-like
        # comparison stays like-for-like — a half month accrues half a share.
        expenses = (metrics.filter_by_range(all_expenses, w_start, w_end)
                    + metrics.accrual_rows(obligations, w_start, w_end))
        pnl = metrics.compute_pnl(
            metrics.filter_by_range(all_sales, w_start, w_end),
            room_slice, expenses, cost_map,
        )
        return metrics.compute_goppar(pnl, available, room_revenue=pnl.rooms.revenue)

    gp = _goppar_for(start, end, room_rows, rm.available_room_nights)

    trend = gp_trend = None
    if prior:
        p_start, p_end, p_label = prior
        p_days = max((p_end - p_start).days + 1, 0)
        p_rooms = metrics.filter_by_range(all_rooms, p_start, p_end)
        p_rm = metrics.compute_room_metrics(
            p_rooms, total_rooms, p_days, rooms_by_type=counts, hours_by_type=hours,
        )
        trend = metrics.compare_room_metrics(rm, p_rm, label, p_label)
        gp_trend = metrics.compare_goppar(
            gp, _goppar_for(p_start, p_end, p_rooms, p_rm.available_room_nights)
        )

    def _with_delta(text: str, delta_pct: float, was: str) -> str:
        if not (trend and trend.comparable):
            return text
        return f"{text}  {_delta_tag(delta_pct)}  _(was {was})_"

    lines += [
        _with_delta(f"  Revenue:          {_fmt(rm.revenue)}",
                    trend.revenue_delta_pct if trend else 0.0,
                    _fmt(trend.prior.revenue) if trend else ""),
        f"  Room-nights sold: {rm.room_nights_sold}",
        _with_delta(f"  📈 *ADR:  {_fmt(rm.adr)}*",
                    trend.adr_delta_pct if trend else 0.0,
                    _fmt(trend.prior.adr) if trend else ""),
    ]
    if not (trend and trend.comparable):
        lines[-1] += "  _(average rate per night sold)_"
    lines += _short_stay_block(rm)

    if not total_rooms:
        lines += [
            _SEP,
            "⚠️ *Occupancy and RevPAR need your room count.*",
            "  Run `/setrooms <number of rooms>` once — then they appear here",
            "  and in every /report.",
        ]
    else:
        occ = f"  🏨 *Occupancy: {_pct(rm.occupancy_pct)}*"
        if trend and trend.comparable:
            sign = "▲" if trend.occupancy_delta_pt > 0 else ("▼" if trend.occupancy_delta_pt < 0 else "→")
            occ += f"  {sign} {abs(trend.occupancy_delta_pt):,.1f}pt"
        occ += f"  ({rm.room_nights_sold} of {rm.available_room_nights} room-nights)"
        lines.append(occ)
        lines.append(_with_delta(
            f"  💰 *RevPAR: {_fmt(rm.revpar)}*",
            trend.revpar_delta_pct if trend else 0.0,
            _fmt(trend.prior.revpar) if trend else "",
        ))
        if not (trend and trend.comparable):
            lines[-1] += "  _(revenue per available room-night)_"
        basis = f"  _Basis: {total_rooms} rooms × {days} days"
        basis += " — from your per-type counts._" if rooms_from_types else "._"
        lines.append(basis)

    # The verdict — the whole reason for comparing two windows.
    if trend and trend.comparable and total_rooms:
        icon = _TREND_ICONS[(trend.occupancy_dir, trend.revpar_dir)]
        lines += ["", f"  {icon} {_esc(trend.verdict)}"]
        if trend.rate_note:
            lines.append(f"  _{_esc(trend.rate_note)}_")
    elif trend and not trend.comparable:
        lines += ["", f"  _No bookings in {_esc(trend.prior_label)} — no baseline to compare against yet._"]

    if total_rooms:
        lines += _goppar_block(gp, gp_trend)

    if rm.by_type:
        lines += [_SEP, "*BY ROOM TYPE*"]
        ranked = sorted(rm.by_type.items(), key=lambda kv: -kv[1]["revenue"])
        for rtype, d in ranked:
            rate_label = "per let" if d["is_short"] else "ADR"
            head = f"  • *{_esc(rtype)}* — {rate_label} {_fmt(d['adr'])}"
            head += f" · RevPAR {_fmt(d['revpar'])}" if d["rooms"] else " · RevPAR _n/a_"
            lines.append(head)
            units = (_plural(d["lets"], "let") if d["is_short"]
                     else _plural(d["nights"], "night") + " sold")
            if d["rooms"]:
                fill = (f"{_pct(d['utilization_pct'])} of its hours"
                        if d["is_short"] else f"{_pct(d['occupancy_pct'])} full")
                lines.append(
                    f"      _{_plural(d['rooms'], 'room')} · {units} · "
                    f"{fill} · {_fmt(d['revenue'])}_"
                )
            else:
                lines.append(
                    f"      _{units} · {_fmt(d['revenue'])}_  "
                    f"·  _set rooms:_ `/setrooms {rtype.lower()} <n>`"
                )

        lines += _yield_gap_note(ranked)

    lines += [
        _SEP,
        "_RevPAR is the honest one: discounting to fill rooms lifts occupancy_",
        "_while RevPAR stays flat._",
        f"_Generated {clock.now().strftime('%d %b %Y %H:%M')}_",
    ]
    return "\n".join(lines)


def _short_stay_block(rm) -> list[str]:
    """The hourly trade, reported in its own units.

    Kept apart from ADR and occupancy on purpose. A two-hour let is not a
    night, and averaging the two produced a rate the hotel never charged for
    anything; counting lets as nights produced occupancy above 100%. What the
    two trades *can* share is RevPAR, since revenue per available room-day is
    a fair question however the room earned it — so that line stays combined.
    """
    if not rm.has_short_stay:
        return []
    lines = [
        f"  🕐 *Short stays: {_plural(rm.short_lets, 'let')}*  ·  "
        f"avg {_fmt(rm.arl)} per let",
    ]
    if rm.available_room_hours:
        lines.append(
            f"  _Room-time used: {_pct(rm.utilization_pct)} "
            f"({rm.room_hours_sold:,.0f} of {rm.available_room_hours:,.0f} room-hours, both trades)_"
        )
    if rm.room_nights_sold:
        lines.append("  _Occupancy and ADR above are overnight only — lets are counted here._")
    return lines


def _goppar_block(gp, gp_trend) -> list[str]:
    """RevPAR's bottom-line twin, printed directly beneath it.

    RevPAR cannot see fuel, wages, restocking or maintenance. GOPPAR divides
    profit by the same denominator, so the gap between the two lines *is* the
    cost base — and a rate rise that only covers the diesel it was raised for
    shows up here as RevPAR climbing while GOPPAR stands still.
    """
    lines = [_SEP, "*PROFIT PER AVAILABLE ROOM*"]

    goppar_line = f"  🏦 *GOPPAR: {_fmt(gp.goppar)}*"
    if gp_trend and gp_trend.comparable:
        # A % change is only meaningful off a positive base — coming back from a
        # loss is a direction, not a percentage (see compare_goppar).
        if gp_trend.prior.goppar > 0:
            goppar_line += f"  {_delta_tag(gp_trend.goppar_delta_pct)}"
        goppar_line += f"  _(was {_fmt(gp_trend.prior.goppar)})_"
    lines.append(goppar_line)
    lines.append(f"  🛏 _Rooms only: {_fmt(gp.rooms_goppar)} per available room-night_")

    if gp.revpar:
        kept = (f"  _{_pct(gp.conversion_pct)} of RevPAR survives as profit "
                f"({_pct(gp.rooms_conversion_pct)} from rooms alone)_")
        if gp.conversion_pct > 100:
            kept += "\n  _— above 100% because the bar is carrying the rooms._"
        lines.append(kept)

    # The whole point of the pair: did a revenue move reach the bottom line?
    if gp_trend and gp_trend.comparable:
        icon = "✅" if gp_trend.goppar_dir == "up" else (
            "⚠️" if gp_trend.revpar_dir == "up" else "➖")
        lines += ["", f"  {icon} {_esc(gp_trend.verdict)}"]
    elif gp.gop < 0:
        lines.append("  ⚠️ _Negative — the hotel spent more than it earned this period._")

    lines.append("  _After every recorded cost: fuel, wages, restocking, maintenance._")
    return lines


def _yield_gap_note(ranked: list[tuple[str, dict]]) -> list[str]:
    """Flag the type whose headline rate flatters its actual yield.

    The trap: a premium type charges the most per night sold and so looks like
    the best category, while a *cheaper* type earns more per room the hotel
    owns, because the premium rooms sit empty. ADR alone can never show it —
    the gap only opens once each type is divided by its own room count.

    A cheap type yielding least is not the trap; that is just a cheap room. The
    signal is specifically being out-earned by something you charge less for,
    and only once the gap clears the same dead band the trend verdict uses.
    """
    priced = [(t, d) for t, d in ranked if d["rooms"] and d["nights"]]
    if len(priced) < 2:
        return []
    name, top = max(priced, key=lambda kv: kv[1]["adr"])
    best_name, best = max(priced, key=lambda kv: kv[1]["revpar"])
    if best_name == name or best["adr"] >= top["adr"]:
        return []
    if top["revpar"] >= best["revpar"] * (1 - metrics.TREND_BAND / 100):
        return []      # out-earned, but not by enough to be worth acting on
    return [
        f"  ⚠️ _{_esc(name)} charges the most per night ({_fmt(top['adr'])}) but earns_",
        f"  _{_fmt(top['revpar'])} per room owned — {_esc(best_name)}, at {_fmt(best['adr'])} a night,_",
        f"  _earns {_fmt(best['revpar'])} from the same space. {_esc(name)} is only "
        f"{_pct(top['occupancy_pct'])} full._",
    ]


# ── Night-by-night split (flat rise vs weekend premium) ───────────────

def generate_dow_split_report(
    for_date: date | None = None,
    for_month: tuple[int, int] | None = None,
    all_time: bool = False,
    for_week: date | None = None,
) -> str:
    """Room performance split across the seven nights, plus turnaways.

    The screen for one decision: does a single rate fit every night, or do
    Friday and Saturday carry demand the working week does not? A blended
    RevPAR cannot answer it — a hotel full every Friday and half empty every
    Tuesday reports one middling occupancy that describes neither night.

    Turnaways sit alongside because they are the other half of the evidence: a
    night at 100% looks identical whether nobody or twenty people were refused,
    and only the second says the rate is too low.
    """
    all_rooms = _active(db.read_all("rooms"))
    (start, end, label), _ = _room_windows(for_date, for_month, all_time, for_week, all_rooms)
    room_rows = metrics.filter_by_range(all_rooms, start, end)

    # Stays that began before the window still occupy nights inside it, so the
    # rows are widened backwards; compute_dow_split clips to [start, end].
    lookback = metrics.filter_by_range(
        all_rooms, start - timedelta(days=_MAX_STAY_LOOKBACK), start - timedelta(days=1),
    )
    turnaways = db.read_all("turnaways")

    counts = _room_type_counts()
    total_rooms = _total_rooms() or sum(counts.values())
    split = metrics.compute_dow_split(
        room_rows + lookback, start, end, total_rooms, turnaway_rows=turnaways,
        hours_map=_room_type_hours(),
    )
    ta = metrics.summarize_turnaways(turnaways, start, end, adr=split.overall.adr)

    lines = [
        f"🗓 *{HOTEL_NAME} — Night by Night*",
        f"📅 Period: {label}",
        _SEP,
    ]

    # sold_units, not nights_sold: a hotel that only sells by the hour has no
    # room-nights at all, and was being told it had no bookings.
    if not split.overall.sold_units:
        lines.append("No room bookings fall in this period.")
        return "\n".join(lines)

    # The answer first — this report exists to settle one question. When the
    # hotel runs both trades there are two answers, and they can disagree:
    # overnight demand peaks at the weekend far more reliably than hourly does.
    if split.verdict:
        head = "🛏 *Overnight:* " if split.short_verdict else "*"
        tail = "" if split.short_verdict else "*"
        lines += [f"{head}{_esc(split.verdict)}{tail}", f"_{_esc(split.detail)}_"]
    if split.short_verdict:
        if split.verdict:
            lines.append("")
        lines += [f"🕐 *Hourly:* {_esc(split.short_verdict)}",
                  f"_{_esc(split.short_detail)}_"]
    if split.verdict or split.short_verdict:
        lines.append(_SEP)

    # Monospace table: weekday names are fixed width, so the columns line up
    # without the padding gymnastics the drink table needs.
    # A hotel that also sells by the hour needs the lets column and the
    # time-based fill; one that doesn't must not be shown two empty columns.
    hourly = split.overall.lets > 0
    if hourly:
        header = "```\nNight  Use    Nights Lets  ADR      RevPAR   Away\n"
    else:
        header = "```\nNight  Occ    ADR      RevPAR   Away\n"
    body = ""
    for i, b in enumerate(split.by_dow):
        if not b.days:
            continue
        revpar = f"{b.revpar:,.0f}" if split.has_rooms else "—"
        away = str(b.turnaways) if b.turnaways else "·"
        if hourly:
            fill = f"{b.utilization_pct:.0f}%" if split.has_rooms else "—"
            body += (f"{metrics.DOW_SHORT[i]:<6} {fill:<6} {b.nights_sold:<6} "
                     f"{b.lets:<5} {b.adr:<8,.0f} {revpar:<8} {away}\n")
        else:
            occ = f"{b.occupancy_pct:.0f}%" if split.has_rooms else "—"
            body += (f"{metrics.DOW_SHORT[i]:<6} {occ:<6} {b.adr:<8,.0f} "
                     f"{revpar:<8} {away}\n")
    lines.append(header + body + "```")
    if hourly:
        lines.append("  _Use = share of room-hours sold · ADR is the overnight rate_")

    # Either end can come back empty when too many nights tie to name a peak,
    # so they are rendered independently rather than as one line that vanishes.
    peaks = [f"Busiest: *{split.busiest}*"] if split.busiest else []
    peaks += [f"Quietest: *{split.quietest}*"] if split.quietest else []
    if peaks:
        lines.append("  " + "  ·  ".join(peaks))

    # The comparison the decision actually turns on.
    lines += [_SEP, "*WEEKDAY vs WEEKEND*  _(weekend = Fri & Sat nights)_"]
    for b in (split.weekday, split.weekend):
        head = f"  • *{b.label}* — ADR {_fmt(b.adr)}"
        if b.lets:
            head += f" · {_fmt(b.arl)} per let"
        if split.has_rooms:
            head += f" · {_pct(b.occupancy_pct)} full · RevPAR {_fmt(b.revpar)}"
        lines.append(head)
        sold = _plural(b.nights_sold, "room-night") + " sold"
        if b.lets:
            sold += f" · {_plural(b.lets, 'let')}"
        lines.append(f"      _{_plural(b.days, 'night')} · {sold} · {_fmt(b.revenue)}_")
    if split.has_rooms:
        gap = split.occupancy_gap_pt
        direction = "fuller" if gap >= 0 else "emptier"
        lines.append(
            f"  _Weekend runs {abs(gap):,.1f} points {direction}"
            f" and is priced {_signed_pct(split.adr_gap_pct)}._"
        )

    lines += _daypart_block(room_rows + lookback, start, end)
    lines += _turnaway_block(ta, split)

    lines += [
        _SEP,
        "_A blended rate hides the difference between your best night and your_",
        "_worst. This screen is what a weekday/weekend split is argued from._",
        f"_Generated {clock.now().strftime('%d %b %Y %H:%M')}_",
    ]
    return "\n".join(lines)


def _signed_pct(pct: float) -> str:
    """'8% higher' / '5% lower' / 'the same' — for a rate gap read in prose."""
    if pct > metrics.TREND_BAND:
        return f"{pct:,.0f}% higher"
    if pct < -metrics.TREND_BAND:
        return f"{abs(pct):,.0f}% lower"
    return "about the same"


def _daypart_block(room_rows: list[dict], start: date, end: date) -> list[str]:
    """Where an hourly trade actually sits in the day.

    A short let is not a night. Splitting it by weekday answers half the
    question; the half that sets the price is *when in the day*, because a room
    turned away at 8pm and idle at 10am has two problems and only one is a rate.
    """
    split = metrics.daypart_split(room_rows, start, end, _room_type_hours())
    if not split.total_lets:
        return []

    out = [_SEP, "*BY TIME OF DAY* _(hourly lets only)_"]
    width = max(len(b.label) for b in split.bands)
    table = f"{'WHEN':<{width}}  {'LETS':>5}  {'PER LET':>9}  {'HOURS':>6}\n"
    for b in split.bands:
        if not b.lets:
            continue
        table += (f"{b.label:<{width}}  {b.lets:>5}  {b.arl:>9,.0f}  {b.hours:>6,.0f}\n")
    out.append("```\n" + table + "```")
    out.append(f"  _{split.lets_per_day} lets a day across {_plural(split.days, 'day')}_")

    if split.untimed_lets:
        out.append(
            f"  _{_plural(split.untimed_lets, 'let')} carry no time of day — "
            "backdated entries are stamped midnight, so they cannot be placed._")

    verdict, detail = metrics.daypart_verdict(split)
    if verdict:
        out += ["", f"  🕐 *{_esc(verdict)}*", f"  _{_esc(detail)}_"]
    elif not split.readable:
        out.append("  _Too few timed lets yet to read the shape of the day._")
    return out


def _turnaway_block(ta, split) -> list[str]:
    """Refused bookings — the demand a full night leaves invisible.

    When nothing has ever been recorded the block says so plainly rather than
    printing a zero. Zero turnaways and no turnaway *tracking* are opposite
    findings: one says demand stops at the door, the other says nobody looked.
    """
    lines = [_SEP, "*TURNED AWAY*"]

    if not ta.tracked:
        lines += [
            "  ⚠️ _Nothing recorded for this period._",
            "  _A sold-out night and a night that turned away twenty guests_",
            "  _look identical in the books. Until these are logged, occupancy_",
            "  _cannot tell you whether your rate is too low._",
            "  Log them as they happen: `/turnaway <how many> [type] [reason]`",
        ]
        return lines

    lines.append(
        f"  🚪 *{_plural(ta.total, 'guest')} turned away* "
        f"across {_plural(ta.days_with_data, 'day')}"
    )
    if ta.lost_revenue:
        lines.append(
            f"  _≈ {_fmt(ta.lost_revenue)} of demand not served, priced at your own ADR._"
        )
    if ta.by_type:
        top = ", ".join(f"{_esc(t)} ({n})" for t, n in list(ta.by_type.items())[:4])
        lines.append(f"  _Wanted: {top}_")
    if ta.by_reason and list(ta.by_reason) != ["not given"]:
        top = ", ".join(f"{_esc(r)} ({n})" for r, n in list(ta.by_reason.items())[:3])
        lines.append(f"  _Reasons: {top}_")

    # This is the pricing signal: refusals concentrated on nights that are
    # already full mean the rate on those nights is below what the market will
    # pay. Refusals on quiet nights mean the wrong *type* of room, not price.
    wknd = split.weekend.turnaways
    wkdy = split.weekday.turnaways
    if split.has_rooms and ta.total:
        if wknd > wkdy and split.weekend.occupancy_pct >= 70:
            lines.append(
                "  💡 _Concentrated on nights that are already full — that is "
                "unmet demand at the weekend, and the clearest case there is for "
                "a weekend premium rather than a flat rise._"
            )
        elif wkdy > wknd:
            lines.append(
                "  💡 _Mostly on working-week nights — check whether it is the "
                "room type you are short of rather than the price._"
            )
    return lines


# ── Supplier invoices (payables) ──────────────────────────────────────

def generate_payables_report() -> str:
    """Outstanding supplier invoices, soonest due first."""
    rows = db.get_outstanding_payables()
    now = clock.now()

    if not rows:
        return (
            "🧾 *Supplier Invoices*\n\n"
            "Nothing owed to suppliers.\n"
            "_Received stock on credit? Tap_ ⚙️ *Manage → 🧾 Suppliers → 📥 Stock on Credit*\n"
            "_or use_ `/restock_credit <drink> <qty> <cost> <supplier> [YYYY-MM-DD]`"
        )

    total = round(sum(float(r["amount"]) - float(r.get("amount_paid") or 0) for r in rows), 2)
    lines = [
        f"🧾 *{HOTEL_NAME} — Supplier Invoices*",
        f"📅 As of {now.strftime('%d %b %Y')}",
        _SEP,
        f"  *Total owed: {_fmt(total)}* across {_plural(len(rows), 'invoice')}",
        _SEP,
    ]

    for r in rows:
        remaining = round(float(r["amount"]) - float(r.get("amount_paid") or 0), 2)
        opened = _parse_ts(r.get("timestamp"))
        age = f"{(now.date() - opened.date()).days}d old" if opened else ""
        flag = ""
        due = str(r.get("due_date") or "")
        if due:
            try:
                due_date = datetime.strptime(due, "%Y-%m-%d").date()
                overdue = (now.date() - due_date).days
                flag = f" ⚠️ *{overdue}d overdue*" if overdue > 0 else f" · due {due}"
            except ValueError:
                pass
        lines.append(f"  #{r['id']} {_esc(str(r['supplier']).title())} — {_fmt(remaining)}{flag}")
        detail = str(r.get("description") or "")
        if detail:
            lines.append(f"      _{_esc(detail)}_  ·  {age}")
        if float(r.get("amount_paid") or 0) > 0:
            lines.append(f"      _part-paid {_fmt(float(r['amount_paid']))} of {_fmt(float(r['amount']))}_")

    lines += [
        _SEP,
        "_Settle from_ ⚙️ *Manage → 🧾 Suppliers → ✅ Pay Supplier*",
        "_or_ `/pay_supplier <id> [amount]` _— cash only moves then._",
        f"_Generated {now.strftime('%d %b %Y %H:%M')}_",
    ]
    return "\n".join(lines)


# ── Menu engineering ──────────────────────────────────────────────────

_QUADRANT_DISPLAY = (
    ("star",        "⭐ STARS",       "High margin, high volume"),
    ("plow-horse",  "🐴 PLOW-HORSES", "Popular but thin margin"),
    ("puzzle",      "🧩 PUZZLES",     "Good margin, few sales"),
    ("dog",         "🐕 DOGS",        "Low margin, low volume"),
    ("not-selling", "🚫 NOT SELLING", "Zero sales in this window"),
)


def generate_menu_report(window_days: int = _CCC_WINDOW_DAYS) -> str:
    """Rank every drink by margin against popularity, and say what to do about it."""
    now = clock.now()
    stock_rows = inv.get_inventory_summary()
    start = now.date() - timedelta(days=window_days - 1)
    sales_rows = metrics.filter_by_range(_active(db.read_all("sales")), start, now.date())

    items = metrics.menu_engineering(sales_rows, stock_rows, window_days=window_days)
    if not items:
        return (
            "🍽 *Menu Analysis*\n\n"
            "No drinks have a selling price yet.\n"
            "Set one with `/setprice <drink> <amount>`."
        )

    total_gp = round(sum(i.gross_profit for i in items), 2)
    lines = [
        f"🍽 *{HOTEL_NAME} — Menu Analysis*",
        f"📅 Last {window_days} days · as of {now.strftime('%d %b %Y')}",
        _SEP,
        f"  Gross profit from drinks: {_fmt(total_gp)}",
        _SEP,
    ]

    for key, heading, blurb in _QUADRANT_DISPLAY:
        group = [i for i in items if i.quadrant == key]
        if not group:
            continue
        lines.append(f"{heading} _({blurb})_")
        lines.append(f"  ➜ {metrics.QUADRANT_ACTIONS[key]}")
        for i in group:
            if key == "not-selling":
                # "0 sold · GP ₦0" on every line is noise; what matters is how
                # much cash each one is holding still.
                held = (f" · {_plural(i.stock_units, 'unit')} held ({_fmt(i.tied_value)})"
                        if i.stock_units else " · none in stock")
                lines.append(f"  • {_esc(i.drink)}: {_fmt(i.unit_margin)}/unit ({_pct(i.margin_pct)}){held}")
                # Stock sitting entirely in the store can't be sold at all — that's
                # a missed transfer, not a dead product. Say so before "delist".
                if i.stranded_in_store:
                    lines.append(
                        f"    ⚠️ _all in the store — it never reached the bar._ "
                        f"`/transfer {i.drink.lower()} {i.store_units}`"
                    )
            else:
                lines.append(
                    f"  • {_esc(i.drink)}: {i.units} sold · {_fmt(i.unit_margin)}/unit "
                    f"({_pct(i.margin_pct)}) · GP {_fmt(i.gross_profit)}"
                )
        if key in metrics.IDLE_QUADRANTS:
            tied = round(sum(i.tied_value for i in group), 2)
            if tied:
                lines.append(f"  _{_fmt(tied)} of cash is sitting in these._")
        lines.append("")

    idle = [i for i in items if i.quadrant in metrics.IDLE_QUADRANTS]
    idle_cash = round(sum(i.tied_value for i in idle), 2)
    if idle_cash:
        lines.append(f"💤 *{_fmt(idle_cash)} tied up across {_plural(len(idle), 'drink')} earning nothing.*")

    lines += [
        _SEP,
        "_Quadrants compare each drink to the menu average, so they shift as_",
        "_sales change. Act on the ranking, not on one week's noise._",
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
        f"_Generated {clock.now().strftime('%d %b %Y %H:%M')}_",
    ]
    return "\n".join(lines)


# ── Debtors report ─────────────────────────────────────────────────────

def _debt_age(timestamp_str: str) -> str:
    """Return a human-readable age string and flag for overdue debts."""
    created = _parse_ts(timestamp_str)
    if not created:
        return ""
    try:
        days = (clock.now() - created).days
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
        return (clock.now() - dt).days if dt else 0

    overdue = [r for r in rows if _days_old(r) >= 7]
    lines.append(_SEP)
    if overdue:
        lines.append(f"⚠️ {len(overdue)} debt(s) outstanding for 7+ days — follow up needed.")
    if not staff_view:
        lines.append("_Use_ `/pay_debt <id> [amount]` _to pay a specific debt._")
    lines.append(f"_Updated {clock.now().strftime('%d %b %Y %H:%M')}_")
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
        lines.append(f"\n_Updated {clock.now().strftime('%d %b %Y %H:%M')}_")
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

    lines.append(f"\n_Updated {clock.now().strftime('%d %b %Y %H:%M')}_")
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
    today = for_date or clock.now().date()
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
    L.append(f"_Generated {clock.now().strftime('%d %b %Y %H:%M')}_")
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

    lines.append(f"_Updated {clock.now().strftime('%d %b %Y %H:%M')}_")
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
        f"_Generated {clock.now().strftime('%d %b %Y %H:%M')}_",
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
    lines.append(f"_Generated {clock.now().strftime('%d %b %Y %H:%M')}_")
    return "\n".join(lines)
