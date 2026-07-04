"""
metrics.py — Pure financial computation core.

Single source of truth for the numbers behind every report. Functions here are
PURE: they take already-fetched rows (``list[dict]``) plus a cost-price map and
return plain values / dataclasses. They never touch the database, the network,
or Telegram formatting — so both ``reports.py`` (Telegram) and a future web
dashboard consume the exact same calculations and can never drift apart.

Money is plain ``float`` Naira; rounding mirrors the legacy ``reports.py``
behaviour exactly (the golden-master tests under ``tests/`` enforce this).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


# ── Generic row helpers ───────────────────────────────────────────────

def sum_revenue(rows, key="total_revenue"):
    return sum(float(r[key]) for r in rows)


def parse_ts(raw):
    """Parse a timestamp (str / pandas Timestamp / with microseconds) → datetime | None."""
    try:
        ts_str = str(raw).split(".")[0]  # strip microseconds if present
        return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None


def filter_by_date(rows, target):
    result = []
    for r in rows:
        try:
            dt = parse_ts(r["timestamp"])
            if dt and dt.date() == target:
                result.append(r)
        except KeyError:
            pass
    return result


def filter_by_month(rows, year, month):
    result = []
    for r in rows:
        try:
            dt = parse_ts(r["timestamp"])
            if dt and dt.year == year and dt.month == month:
                result.append(r)
        except KeyError:
            pass
    return result


def filter_by_range(rows, start, end):
    """Inclusive [start, end] local-calendar-date filter (e.g. a Mon-Sun week)."""
    result = []
    for r in rows:
        dt = parse_ts(r.get("timestamp"))
        if dt and start <= dt.date() <= end:
            result.append(r)
    return result


def apply_filter(rows, for_date, for_month, all_time):
    now = datetime.now()
    if for_date:
        return filter_by_date(rows, for_date)
    if all_time:
        return rows
    year, month = for_month if for_month else (now.year, now.month)
    return filter_by_month(rows, year, month)


def active(rows):
    """Exclude soft-voided/deleted rows from financial aggregations."""
    return [r for r in rows if not r.get("deleted_at")]


def split_salary(expense_rows):
    """Split expense rows into (salary_rows, other_rows)."""
    salary = [r for r in expense_rows if r.get("category", "").lower() == "salary"]
    other  = [r for r in expense_rows if r.get("category", "").lower() != "salary"]
    return salary, other


# Expense categories that are cash/stock movements, NOT P&L operating costs.
# Buying inventory (restock) converts cash into a stock asset; that cost only
# reaches the P&L as cost-of-goods-sold when the drink is actually sold.
NON_PNL_CATEGORIES = {"restock"}


def operating_expenses(rows):
    """Keep only P&L operating-expense rows (excludes restock / stock purchases)."""
    return [r for r in rows if str(r.get("category", "")).lower() not in NON_PNL_CATEGORIES]


def restock_spend(rows):
    """Total inventory-purchase (restock) cash outflow — a cash movement, not a P&L cost."""
    return round(
        sum(float(r["amount"]) for r in rows
            if str(r.get("category", "")).lower() == "restock"),
        2,
    )


def cost_of_drinks_sold(sales_rows, cost_map):
    """COGS at *current* cost price.

    ``cost_map`` maps a lower-cased drink name → its current cost price. Sales of
    a drink not present in the map count as zero cost (mirrors legacy behaviour).
    """
    total = 0.0
    for row in sales_rows:
        name = row["drink_name"].lower()
        qty = int(row["quantity"])
        total += qty * cost_map.get(name, 0.0)
    return round(total, 2)


# ── Split Bar/Rooms P&L ───────────────────────────────────────────────

@dataclass(frozen=True)
class AccountPnL:
    revenue: float
    cogs: float                 # cost of stock sold (0 for the rooms account)
    expense_total: float        # all operating expenses (salary + other), single sum
    salary: float
    other_expense: float
    other_breakdown: dict       # category.title() → amount (for display)
    profit: float


@dataclass(frozen=True)
class PnL:
    bar: AccountPnL
    rooms: AccountPnL
    total_revenue: float
    total_outgoings: float
    net_profit: float
    restock_spend: float
    sales_count: int
    rooms_count: int


def _breakdown(rows):
    """Sum operating, non-salary expense rows by Title-cased category."""
    out: dict = {}
    for r in rows:
        cat = r["category"].title()
        out[cat] = out.get(cat, 0.0) + float(r["amount"])
    return out


def compute_pnl(sales_rows, room_rows, expense_rows, cost_map):
    """Split Bar/Rooms P&L. Mirrors generate_full_report's arithmetic exactly.

    ``cost_map``: lower-cased drink name → current cost price (see
    ``cost_of_drinks_sold``). Pass already date-filtered, active (non-deleted)
    rows — filtering is the caller's job.
    """
    bar_expenses  = operating_expenses([r for r in expense_rows if r.get("account", "bar") == "bar"])
    room_expenses = operating_expenses([r for r in expense_rows if r.get("account", "rooms") == "rooms"])

    drink_revenue = sum_revenue(sales_rows)
    room_revenue  = sum_revenue(room_rows)
    total_revenue = drink_revenue + room_revenue

    cost_of_drinks     = cost_of_drinks_sold(sales_rows, cost_map)
    bar_expense_total  = sum_revenue(bar_expenses, key="amount")
    room_expense_total = sum_revenue(room_expenses, key="amount")

    bar_profit  = drink_revenue - cost_of_drinks - bar_expense_total
    room_profit = room_revenue - room_expense_total
    total_outgoings = cost_of_drinks + bar_expense_total + room_expense_total
    net_profit = total_revenue - total_outgoings

    bar_salary, bar_other   = split_salary(bar_expenses)
    room_salary, room_other = split_salary(room_expenses)

    bar = AccountPnL(
        revenue=drink_revenue,
        cogs=cost_of_drinks,
        expense_total=bar_expense_total,
        salary=sum(float(r["amount"]) for r in bar_salary),
        other_expense=sum_revenue(bar_other, key="amount"),
        other_breakdown=_breakdown(bar_other),
        profit=bar_profit,
    )
    rooms = AccountPnL(
        revenue=room_revenue,
        cogs=0.0,
        expense_total=room_expense_total,
        salary=sum(float(r["amount"]) for r in room_salary),
        other_expense=sum_revenue(room_other, key="amount"),
        other_breakdown=_breakdown(room_other),
        profit=room_profit,
    )
    return PnL(
        bar=bar,
        rooms=rooms,
        total_revenue=total_revenue,
        total_outgoings=total_outgoings,
        net_profit=net_profit,
        restock_spend=restock_spend(expense_rows),
        sales_count=len(sales_rows),
        rooms_count=len(room_rows),
    )


# ── Outstanding debtors ───────────────────────────────────────────────

@dataclass(frozen=True)
class OutstandingDebt:
    outstanding_count: int      # all outstanding rows (any account)
    bar_count: int
    bar_owed: float
    rooms_count: int
    rooms_owed: float

    @property
    def total_owed(self):
        return self.bar_owed + self.rooms_owed


def summarize_outstanding(debtor_rows):
    """Bar/Rooms split of outstanding (unpaid remainder) debtor balances."""
    outstanding = [r for r in debtor_rows if r.get("status") == "outstanding"]

    def _rem(r):
        return round(float(r["amount"]) - float(r.get("amount_paid") or 0), 2)

    bar   = [r for r in outstanding if r.get("account") == "bar"]
    rooms = [r for r in outstanding if r.get("account") == "rooms"]
    return OutstandingDebt(
        outstanding_count=len(outstanding),
        bar_count=len(bar), bar_owed=sum(_rem(r) for r in bar),
        rooms_count=len(rooms), rooms_owed=sum(_rem(r) for r in rooms),
    )


# ── Cash position: profit vs cash vs stock ────────────────────────────

def net_profit(sales_rows, room_rows, expense_rows, cost_map):
    """(revenue, cogs, operating_expenses, net_profit) for the given active rows."""
    revenue = sum_revenue(sales_rows) + sum_revenue(room_rows)
    cogs = cost_of_drinks_sold(sales_rows, cost_map)
    op_exp = round(sum(float(r["amount"]) for r in operating_expenses(expense_rows)), 2)
    return round(revenue, 2), cogs, op_exp, round(revenue - cogs - op_exp, 2)


@dataclass(frozen=True)
class CashPosition:
    # Cash-at-hand running estimate (anchor-aware)
    opening: float
    anchor_dt: object            # datetime | None
    collected: float
    opex_cash: float
    restock_cash: float
    draws_cash: float
    cash: float
    # Point-in-time assets
    stock_value: float
    receivables: float
    outstanding_count: int
    # Profit footnote (performance, anchor-independent)
    month_profit: float
    profit_all: float


def compute_cash_position(sales_all, rooms_all, expense_all, draws_all, debtor_rows,
                          stock_value, opening, anchor_dt, cost_map, now):
    """Cash-at-hand estimate + asset snapshot + profit footnote.

    Mirrors generate_position_report exactly. ``stock_value``, ``opening`` and
    ``anchor_dt`` (a datetime or None) are resolved by the caller from inventory
    / settings; everything else is derived here. When an anchor date is set, only
    flows on/after it count toward cash (the opening balance is the real balance
    on that day).
    """
    outstanding = [r for r in debtor_rows if r.get("status") == "outstanding"]
    receivables = round(sum(float(r["amount"]) - float(r.get("amount_paid") or 0) for r in outstanding), 2)

    def _since(rows):
        if not anchor_dt:
            return rows
        return [r for r in rows if (parse_ts(r.get("timestamp")) or datetime.min) >= anchor_dt]

    cash_sales, cash_rooms, cash_exp, cash_draws = _since(sales_all), _since(rooms_all), _since(expense_all), _since(draws_all)
    rev_cash     = sum_revenue(cash_sales) + sum_revenue(cash_rooms)
    opex_cash    = sum_revenue(operating_expenses(cash_exp), key="amount")
    restock_cash = restock_spend(cash_exp)
    draws_cash   = round(sum(float(r["amount"]) for r in cash_draws), 2)
    # Only subtract debts CREATED in the counted window — pre-anchor debts were
    # never collected and aren't part of the anchored opening balance either.
    recv_cash = round(sum(
        float(r["amount"]) - float(r.get("amount_paid") or 0)
        for r in _since(outstanding)), 2)
    collected = round(rev_cash - recv_cash, 2)      # assume cash unless an outstanding debtor exists
    cash = round(opening + collected - opex_cash - restock_cash - draws_cash, 2)

    *_, profit_all = net_profit(sales_all, rooms_all, expense_all, cost_map)
    month_sales = filter_by_month(sales_all, now.year, now.month)
    month_rooms = filter_by_month(rooms_all, now.year, now.month)
    month_exp   = filter_by_month(expense_all, now.year, now.month)
    *_, month_profit = net_profit(month_sales, month_rooms, month_exp, cost_map)

    return CashPosition(
        opening=opening, anchor_dt=anchor_dt, collected=collected,
        opex_cash=opex_cash, restock_cash=restock_cash, draws_cash=draws_cash, cash=cash,
        stock_value=stock_value, receivables=receivables, outstanding_count=len(outstanding),
        month_profit=month_profit, profit_all=profit_all,
    )


# ── Allocation (set-asides + profit distribution) ─────────────────────

@dataclass(frozen=True)
class Allocation:
    bar_rev: float
    room_rev: float
    total_rev: float
    room_by_type: dict            # Title-cased room type → {bookings, revenue}
    # Set-asides
    total_pct: int
    buffer_amt: float
    restock_amt: float
    total_save: float
    bar_share: float
    room_share: float
    # Costs
    cost_of_drinks: float
    total_salary: float
    bar_salary_amt: float
    room_salary_amt: float
    other_exp: float
    total_outgoings: float
    restock_total: float
    # Net position
    working_capital: float        # = net profit (revenue − COGS − operating exp)
    after_setaside: float
    burn_rate: float
    # Profit distribution (computed regardless; caller decides whether to show)
    dist_total_pct: int
    draw_amt: float
    reinvest_amt: float
    float_amt: float
    unallocated: float
    pit_low_amt: float
    pit_high_amt: float


def compute_allocation(sales_rows, room_rows, expense_rows, cost_map,
                       buffer_pct, restock_pct, draw_pct, reinvest_pct, float_pct,
                       pit_low_rate, pit_high_rate):
    """Recommended set-asides + profit distribution. Mirrors generate_allocation_report.

    Percentages are passed in (the bot reads them from DB settings); everything
    else is derived from the already date-filtered, active rows.
    """
    bar_rev  = sum_revenue(sales_rows)
    room_rev = sum_revenue(room_rows)
    total_rev = bar_rev + room_rev

    op = operating_expenses(expense_rows)
    bar_salary_rows,  _bar_other  = split_salary([r for r in op if r.get("account") == "bar"])
    room_salary_rows, _room_other = split_salary([r for r in op if r.get("account") == "rooms"])

    bar_salary_amt  = sum(float(r["amount"]) for r in bar_salary_rows)
    room_salary_amt = sum(float(r["amount"]) for r in room_salary_rows)
    total_salary    = bar_salary_amt + room_salary_amt

    bar_exp  = sum(float(r["amount"]) for r in op if r.get("account") == "bar")
    room_exp = sum(float(r["amount"]) for r in op if r.get("account") == "rooms")
    total_exp = bar_exp + room_exp
    cost_of_drinks  = cost_of_drinks_sold(sales_rows, cost_map)
    restock_total   = restock_spend(expense_rows)
    total_outgoings = cost_of_drinks + total_exp

    total_pct  = buffer_pct + restock_pct
    buffer_amt  = round(total_rev * buffer_pct / 100, 2)
    restock_amt = round(total_rev * restock_pct / 100, 2)
    total_save  = buffer_amt + restock_amt
    bar_share  = round(total_save * (bar_rev / total_rev), 2) if total_rev else 0.0
    room_share = round(total_save * (room_rev / total_rev), 2) if total_rev else 0.0

    other_exp       = total_exp - total_salary
    working_capital = total_rev - total_outgoings
    after_setaside  = working_capital - total_save
    burn_rate = (total_exp / total_rev * 100) if total_rev else 0.0

    room_by_type: dict = {}
    for r in room_rows:
        rt = r["room_type"].title()
        if rt not in room_by_type:
            room_by_type[rt] = {"bookings": 0, "revenue": 0.0}
        room_by_type[rt]["bookings"] += int(r["quantity"])
        room_by_type[rt]["revenue"] += float(r["total_revenue"])

    dist_total_pct = draw_pct + reinvest_pct + float_pct
    draw_amt     = round(after_setaside * draw_pct / 100, 2)
    reinvest_amt = round(after_setaside * reinvest_pct / 100, 2)
    float_amt    = round(after_setaside * float_pct / 100, 2)
    unallocated  = round(after_setaside - draw_amt - reinvest_amt - float_amt, 2)
    pit_low_amt  = round(draw_amt * pit_low_rate / 100, 2)
    pit_high_amt = round(draw_amt * pit_high_rate / 100, 2)

    return Allocation(
        bar_rev=bar_rev, room_rev=room_rev, total_rev=total_rev, room_by_type=room_by_type,
        total_pct=total_pct, buffer_amt=buffer_amt, restock_amt=restock_amt, total_save=total_save,
        bar_share=bar_share, room_share=room_share,
        cost_of_drinks=cost_of_drinks, total_salary=total_salary,
        bar_salary_amt=bar_salary_amt, room_salary_amt=room_salary_amt,
        other_exp=other_exp, total_outgoings=total_outgoings, restock_total=restock_total,
        working_capital=working_capital, after_setaside=after_setaside, burn_rate=burn_rate,
        dist_total_pct=dist_total_pct, draw_amt=draw_amt, reinvest_amt=reinvest_amt,
        float_amt=float_amt, unallocated=unallocated,
        pit_low_amt=pit_low_amt, pit_high_amt=pit_high_amt,
    )


# ── Staff activity (by recorder) ──────────────────────────────────────

def staff_breakdown(sales_rows, room_rows):
    """Drink + room activity grouped by who recorded it (`recorded_by`).

    Returns a list of dicts sorted by name; blank/missing recorders collapse to
    "Unknown". Mirrors generate_staff_report's aggregation.
    """
    staff: dict[str, dict] = {}

    def _row(name):
        return staff.setdefault(name, {
            "name": name, "drink_txns": 0, "drink_revenue": 0.0,
            "room_txns": 0, "room_revenue": 0.0,
        })

    for r in sales_rows:
        name = (r.get("recorded_by") or "Unknown").strip() or "Unknown"
        d = _row(name)
        d["drink_txns"] += 1
        d["drink_revenue"] += float(r["total_revenue"])

    for r in room_rows:
        name = (r.get("recorded_by") or "Unknown").strip() or "Unknown"
        d = _row(name)
        d["room_txns"] += 1
        d["room_revenue"] += float(r["total_revenue"])

    return [staff[n] for n in sorted(staff)]
