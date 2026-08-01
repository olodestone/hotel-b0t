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
from datetime import datetime, timedelta


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
# `supplier` is the settlement of a credit purchase (see /pay_supplier): the
# stock arrived earlier, so the payment is pure cash-out with no P&L effect —
# the cost still reaches the P&L as COGS when the drink sells.
STOCK_PURCHASE_CATEGORIES = {"restock", "supplier"}
NON_PNL_CATEGORIES = STOCK_PURCHASE_CATEGORIES


def operating_expenses(rows):
    """Keep only P&L operating-expense rows (excludes restock / stock purchases)."""
    return [r for r in rows if str(r.get("category", "")).lower() not in NON_PNL_CATEGORIES]


def restock_spend(rows):
    """Total inventory-purchase cash outflow — a cash movement, not a P&L cost.

    Covers both paid-on-the-spot restocks and `supplier` rows (settlements of
    earlier credit purchases): each is cash actually leaving the account.
    """
    return round(
        sum(float(r["amount"]) for r in rows
            if str(r.get("category", "")).lower() in STOCK_PURCHASE_CATEGORIES),
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

def pct_of(part, whole):
    """`part` as a percentage of `whole`; 0.0 when `whole` is zero.

    One helper so every margin/ratio in this module divides the same way and
    never raises on an empty period.
    """
    return round(part / whole * 100, 1) if whole else 0.0


@dataclass(frozen=True)
class AccountPnL:
    revenue: float
    cogs: float                 # cost of stock sold (0 for the rooms account)
    expense_total: float        # all operating expenses (salary + other), single sum
    salary: float
    other_expense: float
    other_breakdown: dict       # category.title() → amount (for display)
    profit: float

    @property
    def gross_profit(self):
        """Revenue left after the cost of the stock sold — before any expense."""
        return round(self.revenue - self.cogs, 2)

    @property
    def gross_margin_pct(self):
        """Pricing/purchasing health: how much of each ₦100 of sales survives COGS.

        The rooms account carries no COGS, so its gross margin is always 100% —
        read it as a *contribution* margin there, not a like-for-like comparison
        with the bar.
        """
        return pct_of(self.gross_profit, self.revenue)

    @property
    def net_margin_pct(self):
        """Whole-operation health: profit as a percentage of this account's revenue."""
        return pct_of(self.profit, self.revenue)


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

    @property
    def gross_profit(self):
        return round(self.total_revenue - self.bar.cogs, 2)

    @property
    def gross_margin_pct(self):
        """Blended gross margin. Distorted upward by room revenue (zero COGS) —
        the per-account figures are the ones to act on."""
        return pct_of(self.gross_profit, self.total_revenue)

    @property
    def net_margin_pct(self):
        return pct_of(self.net_profit, self.total_revenue)


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


# ── Working capital / cash conversion cycle ───────────────────────────
#
# The cash conversion cycle answers "how many days is my money locked up
# between paying for stock and having the cash back in hand?":
#
#     CCC = DIO + DSO − DPO
#
#   DIO — days a drink sits in store+bar before it sells
#   DSO — days between a sale on credit and the customer actually paying
#   DPO — days we take to pay our own supplier
#
# A high CCC is why a profitable month can still leave the account empty: the
# profit is sitting in crates and in debtors' pockets. Every day shaved off the
# cycle is cash returned to the owner without borrowing a naira.

_AGING_BUCKETS = ((0, 30, "0–30 days"), (31, 60, "31–60 days"), (61, None, "61+ days"))


@dataclass(frozen=True)
class AgingBucket:
    label: str
    count: int
    amount: float


@dataclass(frozen=True)
class WorkingCapital:
    window_days: int
    # ① Inventory leg
    stock_value: float
    avg_stock_value: float
    cogs_window: float
    daily_cogs: float
    dio_days: float | None          # None when nothing sold — the ratio is undefined
    dio_basis: str                  # "snapshots" | "current" | "none"
    # ② Receivables leg
    credit_sales_window: float
    receivables: float
    dso_days: float | None          # ratio estimate, the CCC input
    collection_days: float | None   # *measured* amount-weighted days-to-collect
    collection_basis: str           # "window" | "all-time" | "none"
    settled_count: int              # payment events behind that average
    aging: list
    # ③ Payables leg
    purchases_window: float
    payables_outstanding: float
    dpo_days: float | None
    dpo_tracked: bool               # False → no supplier credit recorded at all
    # Cycle
    ccc_days: float | None
    # Idle capital
    dead_stock: list
    dead_stock_value: float


def _mean(values):
    vals = list(values)
    return sum(vals) / len(vals) if vals else 0.0


def _avg_snapshot_value(snapshot_rows, start, end):
    """Mean daily total stock value across inventory snapshots in [start, end]."""
    by_day: dict = {}
    for r in snapshot_rows:
        raw = r.get("snapshot_date")
        day = str(raw)[:10]
        try:
            d = datetime.strptime(day, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            continue
        if start <= d <= end:
            by_day[d] = by_day.get(d, 0.0) + float(r.get("stock_value") or 0)
    return _mean(by_day.values()), len(by_day)


def compute_working_capital(sales_all, expense_all, debtor_rows, payment_rows,
                            stock_rows, cost_map, payable_rows=(), snapshot_rows=(),
                            window_days=30, now=None):
    """Cash conversion cycle + the working capital tied up in stock and debtors.

    Pass *all-time* rows — the trailing window is applied here so callers can't
    window one leg differently from another. ``stock_rows`` is
    ``inventory.get_inventory_summary()`` output (keys: drink, bar_stock,
    store_stock, cost_price, stock_value). ``payable_rows`` and ``snapshot_rows``
    are optional: without them DPO is reported as untracked and DIO falls back
    to today's stock level rather than a true period average.
    """
    now = now or datetime.now()
    end = now.date()
    start = end - timedelta(days=window_days - 1)

    # ── ① Inventory: how long does a crate sit before it sells? ──
    sales_window = filter_by_range(active(sales_all), start, end)
    cogs_window = cost_of_drinks_sold(sales_window, cost_map)
    daily_cogs = round(cogs_window / window_days, 2) if window_days else 0.0

    stock_value = round(sum(float(i.get("stock_value") or 0) for i in stock_rows), 2)
    snap_avg, snap_days = _avg_snapshot_value(snapshot_rows, start, end)
    if snap_days >= 2:
        avg_stock_value, dio_basis = round(snap_avg, 2), "snapshots"
    else:
        avg_stock_value, dio_basis = stock_value, "current"
    dio_days = round(avg_stock_value / daily_cogs, 1) if daily_cogs else None
    if dio_days is None:
        dio_basis = "none"

    # ── ② Receivables: how long until a credit sale becomes cash? ──
    outstanding = [r for r in debtor_rows if r.get("status") == "outstanding"]
    receivables = round(sum(
        float(r["amount"]) - float(r.get("amount_paid") or 0) for r in outstanding), 2)

    debts_window = filter_by_range(debtor_rows, start, end)
    credit_sales_window = round(sum(float(r["amount"]) for r in debts_window), 2)
    daily_credit = credit_sales_window / window_days if window_days else 0.0
    # No credit sales → nothing waiting to be collected → the leg costs 0 days.
    dso_days = round(receivables / daily_credit, 1) if daily_credit else None

    # Measured collection time beats the ratio for explaining *why* the cash is
    # late. Measured per payment *event* and weighted by amount, so a debtor who
    # pays in instalments is counted honestly — a settled-debts-only average
    # would ignore every part-payment and read as though nothing gets collected.
    opened_at = {}
    for r in debtor_rows:
        if r.get("id") is not None:
            opened_at[int(r["id"])] = parse_ts(r.get("timestamp"))

    events = []     # (days_to_pay, amount, payment_date)
    for p in payment_rows:
        did = p.get("debtor_id")
        opened = opened_at.get(int(did)) if did is not None else None
        paid_dt = parse_ts(p.get("timestamp"))
        if opened and paid_dt and paid_dt >= opened:
            events.append(((paid_dt - opened).total_seconds() / 86400,
                           float(p.get("amount") or 0), paid_dt.date()))

    def _weighted(evts):
        total = sum(amt for _, amt, _ in evts)
        if not total:
            return _mean(d for d, _, _ in evts)
        return sum(d * amt for d, amt, _ in evts) / total

    # A small hotel may collect nothing in 30 days — fall back to all-time
    # before giving up, and label which basis was used.
    in_window = [e for e in events if start <= e[2] <= end]
    if in_window:
        collection_days, collection_basis, settled_count = round(_weighted(in_window), 1), "window", len(in_window)
    elif events:
        collection_days, collection_basis, settled_count = round(_weighted(events), 1), "all-time", len(events)
    else:
        collection_days, collection_basis, settled_count = None, "none", 0

    aging = []
    for lo, hi, label in _AGING_BUCKETS:
        rows = []
        for r in outstanding:
            opened = parse_ts(r.get("timestamp"))
            if not opened:
                continue
            age = (end - opened.date()).days
            if age >= lo and (hi is None or age <= hi):
                rows.append(r)
        aging.append(AgingBucket(
            label=label,
            count=len(rows),
            amount=round(sum(float(r["amount"]) - float(r.get("amount_paid") or 0) for r in rows), 2),
        ))

    # ── ③ Payables: how long do we take to pay the supplier? ──
    exp_window = filter_by_range(active(expense_all), start, end)
    cash_purchases = round(sum(
        float(r["amount"]) for r in exp_window
        if str(r.get("category", "")).lower() == "restock"), 2)
    payables_window = filter_by_range(payable_rows, start, end)
    credit_purchases = round(sum(float(r["amount"]) for r in payables_window), 2)
    # Stock *acquired* in the window. `supplier` expense rows are settlements of
    # earlier purchases, not new stock, so they're deliberately excluded here.
    purchases_window = round(cash_purchases + credit_purchases, 2)

    payables_outstanding = round(sum(
        float(r["amount"]) - float(r.get("amount_paid") or 0)
        for r in payable_rows if r.get("status") == "outstanding"), 2)
    dpo_tracked = bool(list(payable_rows))
    daily_purchases = purchases_window / window_days if window_days else 0.0
    dpo_days = round(payables_outstanding / daily_purchases, 1) if (dpo_tracked and daily_purchases) else None

    # ── The cycle ──
    if dio_days is None:
        ccc_days = None
    else:
        ccc_days = round(dio_days + (dso_days or 0.0) - (dpo_days or 0.0), 1)

    # ── Idle capital: stock that didn't move at all this window ──
    sold_names = {str(r["drink_name"]).lower() for r in sales_window}
    dead_stock = []
    for i in stock_rows:
        units = int(i.get("bar_stock", 0)) + int(i.get("store_stock", 0))
        if units > 0 and str(i["drink"]).lower() not in sold_names:
            dead_stock.append({
                "drink": i["drink"],
                "units": units,
                "value": round(units * float(i.get("cost_price") or 0), 2),
            })
    dead_stock.sort(key=lambda d: d["value"], reverse=True)

    return WorkingCapital(
        window_days=window_days,
        stock_value=stock_value, avg_stock_value=avg_stock_value,
        cogs_window=cogs_window, daily_cogs=daily_cogs,
        dio_days=dio_days, dio_basis=dio_basis,
        credit_sales_window=credit_sales_window, receivables=receivables,
        dso_days=dso_days, collection_days=collection_days,
        collection_basis=collection_basis, settled_count=settled_count, aging=aging,
        purchases_window=purchases_window, payables_outstanding=payables_outstanding,
        dpo_days=dpo_days, dpo_tracked=dpo_tracked,
        ccc_days=ccc_days,
        dead_stock=dead_stock,
        dead_stock_value=round(sum(d["value"] for d in dead_stock), 2),
    )


# ── Break-even ────────────────────────────────────────────────────────

@dataclass(frozen=True)
class BreakEven:
    """Bar-account break-even. Every field is scoped to the bar (see
    compute_break_even for why rooms are deliberately excluded)."""
    fixed_costs: float              # bar operating expenses
    gross_margin_ratio: float       # 0.0–1.0, bar only
    break_even_revenue: float | None
    actual_revenue: float           # bar revenue
    surplus: float | None           # revenue above (or below) break-even
    margin_of_safety_pct: float | None


def compute_break_even(sales_rows, expense_rows, cost_map):
    """Bar revenue needed to cover the bar's own operating costs.

    Scoped to the bar account on both sides, deliberately. A margin blended
    across bar and rooms is dominated by room revenue (zero COGS), pushing the
    ratio toward 100% and making break-even look far easier than it is.

    Scoping only the *margin* to the bar while leaving whole-hotel costs and
    revenue in place would be worse still: it applies the bar's margin to room
    revenue that really does convert at ~100%, and so reports "below break-even"
    in months that turned a genuine profit. Costs and revenue therefore move to
    the bar too, which keeps the figure self-consistent — it can never disagree
    with the bar P&L.

    Treats bar operating expenses as fixed for the period — true enough for
    wages, rent, diesel and utilities.
    """
    revenue = round(sum_revenue(sales_rows), 2)
    cogs = cost_of_drinks_sold(sales_rows, cost_map)
    bar_expenses = [r for r in operating_expenses(expense_rows)
                    if r.get("account", "bar") == "bar"]
    fixed_costs = round(sum(float(r["amount"]) for r in bar_expenses), 2)
    ratio = round((revenue - cogs) / revenue, 4) if revenue else 0.0

    if ratio <= 0:
        # Either no bar sales at all (no margin to work from) or selling at/below
        # cost (no revenue level ever breaks even). `actual_revenue` tells them apart.
        return BreakEven(fixed_costs=fixed_costs, gross_margin_ratio=ratio,
                         break_even_revenue=None, actual_revenue=revenue,
                         surplus=None, margin_of_safety_pct=None)

    be = round(fixed_costs / ratio, 2)
    return BreakEven(
        fixed_costs=fixed_costs, gross_margin_ratio=ratio,
        break_even_revenue=be, actual_revenue=revenue,
        surplus=round(revenue - be, 2),
        margin_of_safety_pct=pct_of(revenue - be, revenue),
    )


@dataclass(frozen=True)
class RoomsTarget:
    shared_costs: float           # operating expenses NOT charged to the bar
    bar_contribution: float       # what the bar handed over (negative = bar lost money)
    room_sales_needed: float
    actual_room_revenue: float
    surplus: float                # room revenue above (or below) the target
    covered: bool


def compute_rooms_target(sales_rows, room_rows, expense_rows, cost_map):
    """How much room revenue has to bring in, once the bar has done its bit.

    Mirrors how a small hotel actually runs: the bar carries only the costs it
    genuinely causes (its own staff, its freezer), while room revenue carries
    the shared overheads — rent, diesel, security, room staff — which exist
    whether or not the bar opens.

    A room sale converts at ~100% (there is no stock behind a room), so this is
    a subtraction rather than a division by a margin. ``bar_contribution`` is the
    same figure as ``PnL.bar.profit`` by construction; a test pins them together.

    When the bar loses money its contribution is negative, which correctly
    *raises* the room target — rooms then cover the overheads AND the bar's loss.
    """
    op = operating_expenses(expense_rows)
    bar_expenses = round(sum(float(r["amount"]) for r in op
                             if r.get("account", "bar") == "bar"), 2)
    shared_costs = round(sum(float(r["amount"]) for r in op
                             if r.get("account", "bar") != "bar"), 2)

    bar_revenue = sum_revenue(sales_rows)
    bar_contribution = round(
        bar_revenue - cost_of_drinks_sold(sales_rows, cost_map) - bar_expenses, 2)

    # Clamped: once the bar covers everything, rooms need nothing — a negative
    # "sales needed" would be meaningless as a target.
    needed = round(max(shared_costs - bar_contribution, 0.0), 2)
    actual = round(sum_revenue(room_rows), 2)
    return RoomsTarget(
        shared_costs=shared_costs, bar_contribution=bar_contribution,
        room_sales_needed=needed, actual_room_revenue=actual,
        surplus=round(actual - needed, 2), covered=actual >= needed,
    )


# ── Room performance (occupancy / ADR / RevPAR) ───────────────────────

@dataclass(frozen=True)
class RoomMetrics:
    total_rooms: int
    days: int
    available_room_nights: int
    room_nights_sold: int
    occupancy_pct: float
    adr: float          # average daily rate — revenue per room-night *sold*
    revpar: float       # revenue per *available* room-night
    revenue: float
    by_type: dict


def compute_room_metrics(room_rows, total_rooms, days):
    """The three standard hotel yield metrics.

    ADR says what you charge; occupancy says how full you are; RevPAR combines
    both and is the only one that can't be gamed — discounting to fill rooms
    lifts occupancy while RevPAR stays flat or falls.

    ``total_rooms`` of 0 means the owner hasn't recorded the room count yet, so
    occupancy and RevPAR are undefined (0.0) while ADR still works.
    """
    revenue = round(sum_revenue(room_rows), 2)
    nights_sold = sum(int(r["quantity"]) * int(r["nights"]) for r in room_rows)
    available = max(int(total_rooms), 0) * max(int(days), 0)

    by_type: dict = {}
    for r in room_rows:
        rt = str(r["room_type"]).title()
        d = by_type.setdefault(rt, {"nights": 0, "revenue": 0.0, "bookings": 0})
        d["bookings"] += int(r["quantity"])
        d["nights"] += int(r["quantity"]) * int(r["nights"])
        d["revenue"] += float(r["total_revenue"])
    for d in by_type.values():
        d["adr"] = round(d["revenue"] / d["nights"], 2) if d["nights"] else 0.0

    return RoomMetrics(
        total_rooms=int(total_rooms), days=int(days),
        available_room_nights=available, room_nights_sold=nights_sold,
        occupancy_pct=pct_of(nights_sold, available),
        adr=round(revenue / nights_sold, 2) if nights_sold else 0.0,
        revpar=round(revenue / available, 2) if available else 0.0,
        revenue=revenue, by_type=by_type,
    )


# ── Menu engineering ──────────────────────────────────────────────────
#
# Kasavana–Smith: rank every drink on unit contribution margin (₦ kept per unit)
# against popularity (units sold), each measured relative to the menu average.
# Four quadrants, four different actions:
#
#   STAR       high margin, high volume  → protect; never let it stock out
#   PLOW-HORSE low margin,  high volume  → working for the supplier; raise price
#   PUZZLE     high margin, low volume   → push it; placement and staff prompts
#   DOG        low margin,  low volume   → delist; frees shelf space and cash
#
# Popularity threshold is 70% of the average units per item, the standard
# allowance so a menu of many items doesn't classify almost everything as unpopular.

_POPULARITY_FACTOR = 0.7

# Items that sold *nothing* in the window are pulled out before the quadrants
# are applied. On unit margin alone a zero-seller often scores as a "puzzle" and
# gets told to "push it harder", which is the wrong advice for something nobody
# bought at all — the question there is whether it belongs on the menu, or never
# reached the bar. They keep their own bucket so that advice, and the cash they
# tie up, stay visible instead of hiding among genuine low-volume earners.
QUADRANT_ACTIONS = {
    "star":        "Protect — keep it always in stock",
    "plow-horse":  "Raise price or renegotiate cost",
    "puzzle":      "Promote — push it harder",
    "dog":         "Delist — frees cash and shelf space",
    "not-selling": "Delist, or find out why it isn't moving",
}

# Quadrants whose stock is idle capital worth totalling for the owner.
IDLE_QUADRANTS = ("dog", "not-selling")


@dataclass(frozen=True)
class MenuItem:
    drink: str
    units: int
    revenue: float
    cogs: float
    gross_profit: float
    unit_margin: float
    margin_pct: float
    weekly_velocity: float
    quadrant: str
    stock_units: int
    tied_value: float


def menu_engineering(sales_rows, stock_rows, window_days=30):
    """Classify every priced drink into a menu-engineering quadrant.

    ``stock_rows`` is ``inventory.get_inventory_summary()`` output — it carries
    both the current cost/selling price and the units on hand, so unsold items
    still appear (as dogs holding idle cash) instead of vanishing from the list.
    """
    units_by: dict = {}
    revenue_by: dict = {}
    for r in sales_rows:
        name = str(r["drink_name"]).lower()
        units_by[name] = units_by.get(name, 0) + int(r["quantity"])
        revenue_by[name] = revenue_by.get(name, 0.0) + float(r["total_revenue"])

    priced = [i for i in stock_rows if float(i.get("selling_price") or 0) > 0]
    if not priced:
        return []

    weeks = (window_days / 7.0) or 1.0
    rows = []
    for i in priced:
        name = str(i["drink"]).lower()
        units = units_by.get(name, 0)
        cost = float(i.get("cost_price") or 0)
        price = float(i.get("selling_price") or 0)
        revenue = round(revenue_by.get(name, 0.0), 2)
        rows.append({
            "drink": i["drink"], "units": units, "revenue": revenue,
            "cogs": round(units * cost, 2),
            "unit_margin": round(price - cost, 2),
            "margin_pct": pct_of(price - cost, price),
            "weekly_velocity": round(units / weeks, 1),
            "stock_units": int(i.get("bar_stock", 0)) + int(i.get("store_stock", 0)),
            "cost_price": cost,
        })

    # Menu averages define the two axes.
    total_units = sum(r["units"] for r in rows)
    avg_units = (total_units / len(rows)) * _POPULARITY_FACTOR
    total_margin = sum(r["unit_margin"] * r["units"] for r in rows)
    # Contribution-weighted average margin — an item selling 200 units should
    # move the bar far more than one selling 2. With no sales yet, fall back to
    # the plain average so the quadrants still mean something.
    avg_margin = (total_margin / total_units) if total_units else _mean(r["unit_margin"] for r in rows)

    out = []
    for r in rows:
        popular = r["units"] >= avg_units
        profitable = r["unit_margin"] >= avg_margin
        # Zero-sellers are split out first — see IDLE_QUADRANTS above. They stay
        # in the averages, so pulling them into their own bucket never shifts the
        # boundaries between the four real quadrants.
        quadrant = ("not-selling" if r["units"] == 0 else
                    "star" if popular and profitable else
                    "plow-horse" if popular else
                    "puzzle" if profitable else "dog")
        out.append(MenuItem(
            drink=r["drink"], units=r["units"], revenue=r["revenue"], cogs=r["cogs"],
            gross_profit=round(r["revenue"] - r["cogs"], 2),
            unit_margin=r["unit_margin"], margin_pct=r["margin_pct"],
            weekly_velocity=r["weekly_velocity"], quadrant=quadrant,
            stock_units=r["stock_units"],
            tied_value=round(r["stock_units"] * r["cost_price"], 2),
        ))
    out.sort(key=lambda m: (-m.gross_profit, m.drink))
    return out


# ── Stocktake variance (shrinkage) ────────────────────────────────────

@dataclass(frozen=True)
class VarianceSummary:
    counts: int
    drinks: int
    total_units: int            # net unit variance (negative = stock missing)
    total_value: float          # net ₦ variance at cost
    shrink_units: int           # losses only (the number that matters)
    shrink_value: float
    by_drink: list              # per-drink rollup, worst loss first


def summarize_variance(count_rows, cost_map):
    """Roll up stocktake counts into a shrinkage picture.

    A negative variance means fewer units were physically present than the books
    expected — breakage, an unrecorded sale, or theft. The database can never
    detect this on its own: it only ever believes its own arithmetic, so a
    physical count is the one independent observation that makes loss visible.
    """
    by: dict = {}
    for r in count_rows:
        name = str(r["drink_name"]).lower()
        var = int(r["counted"]) - int(r["expected"])
        d = by.setdefault(name, {"drink": name.title(), "counts": 0, "units": 0, "value": 0.0})
        d["counts"] += 1
        d["units"] += var
        d["value"] += round(var * cost_map.get(name, 0.0), 2)

    rows = sorted(by.values(), key=lambda d: d["units"])
    losses = [d for d in rows if d["units"] < 0]
    return VarianceSummary(
        counts=len(count_rows),
        drinks=len(rows),
        total_units=sum(d["units"] for d in rows),
        total_value=round(sum(d["value"] for d in rows), 2),
        shrink_units=sum(d["units"] for d in losses),
        shrink_value=round(sum(d["value"] for d in losses), 2),
        by_drink=rows,
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
