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
from calendar import monthrange
from datetime import date, datetime, timedelta
from typing import NamedTuple

import clock


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
    now = clock.now()
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

# ── The two axes every expense is classified on ───────────────────────
#
# AXIS 1, `account`: which department the cost belongs to. OVERHEAD is for
# costs that serve the whole business — it is NOT a dumping ground for
# anything shared-ish. Salaries stay on the department that causes them (the
# barman is a bar cost) and diesel stays on rooms, because compute_rooms_target
# and compute_break_even are both built on that split and silently change
# meaning if it moves.
#
# AXIS 2, `expense_class`: what kind of spend it is, which decides whether it
# reaches the P&L at all. This is deliberately a separate axis from `category`
# rather than a category value: a category holds one string, so "Maintenance
# that happens to be capital" could only be recorded by giving up the category.
#
#   operating — bought again next month, consumed in the month     → P&L
#   irregular — a one-off nobody could have forecast                → P&L, tagged
#   periodic  — recurs every 3-12 months                           → reserve draw
#   capital   — creates or replaces an asset lasting 12+ months    → cash only
#   inventory — stock for resale                                   → cash only
#
# `irregular` is the compressor that dies, the window a guest breaks, the levy
# nobody saw coming. It stays in the P&L because it is a real cost — excluding
# it would overstate profit, and unlike capital it buys nothing. It is NOT
# accrued: there is no expected amount to divide and no interval to divide it
# over, and accruing a guess while the real cost also lands would charge it
# twice. Tagging it instead lets a month be read two ways — what it actually
# cost, and what it would have cost if nothing had broken.
#
# `periodic` is deliberately NOT in the P&L. A soakaway emptied every six
# months is a cost of all six, and charging it to whichever month it happened
# to fall in makes that month look terrible and the other five look better than
# they were. Instead the monthly *share* is accrued from the obligations
# register (see accrual_rows) and the actual payment is a draw against the
# reserve those accruals built up.
#
# Capital and inventory are cash-out-not-cost: they leave the bank but never
# touch profit. They must still be subtracted from the cash estimate, which is
# what restock_spend() and capital_spend() are for.

ACCOUNTS = ("bar", "rooms", "overhead")
EXPENSE_CLASSES = ("operating", "irregular", "periodic", "capital", "inventory")
PNL_CLASSES = ("operating", "irregular")   # both are real costs of the period
RESERVE_CLASS = "periodic"            # reaches the P&L as an accrual instead
IRREGULAR_CLASS = "irregular"
CAPITAL_CLASS = "capital"

# An item is capital only if it still exists in 12 months AND costs at least
# this much. Below it, expense it even when it is technically an asset — the
# threshold is Naira and is policy, so the bot reads it from settings and this
# is only the fallback.
CAPITAL_THRESHOLD = 50_000.0


def expense_class(row):
    """Which class a row belongs to, tolerating rows written before the axis existed.

    Legacy rows carry no `expense_class`. Rather than a second exclusion
    mechanism running alongside this one, the old restock/supplier category
    rule is expressed *as* a class: those rows are inventory, everything else
    is operating. That is also the safe default for unclassified history —
    over-expensing understates profit, which is the right way to be wrong.
    """
    cls = str(row.get("expense_class") or "").strip().lower()
    if cls in EXPENSE_CLASSES:
        return cls
    if str(row.get("category", "")).lower() in STOCK_PURCHASE_CATEGORIES:
        return "inventory"
    return "operating"


def expense_account(row):
    """Which account a row belongs to, defaulting to bar as the schema always has."""
    acct = str(row.get("account") or "").strip().lower()
    return acct if acct in ACCOUNTS else "bar"


def operating_expenses(rows):
    """Keep only the rows that belong in the P&L.

    Capital and inventory are excluded here and nowhere else, so there is one
    gate rather than a filter per report that can drift out of agreement.
    """
    return [r for r in rows if expense_class(r) in PNL_CLASSES]


def capital_rows(rows):
    """Capital purchases — cash out, listed apart, excluded from every margin."""
    return [r for r in rows if expense_class(r) == CAPITAL_CLASS]


def irregular_rows(rows):
    """One-off costs nobody could have forecast. Real, but not of this month."""
    return [r for r in rows if expense_class(r) == IRREGULAR_CLASS]


def irregular_spend(rows):
    return round(sum(float(r["amount"]) for r in irregular_rows(rows)), 2)


def periodic_rows(rows):
    """Actual payments of a periodic bill — draws against the reserve."""
    return [r for r in rows if expense_class(r) == RESERVE_CLASS]


def periodic_spend(rows):
    """Cash paid out on periodic bills this window.

    Out of the P&L (the accrual carries the cost) but not out of the bank.
    """
    return round(sum(float(r["amount"]) for r in periodic_rows(rows)), 2)


def capital_spend(rows):
    """Total capital cash outflow.

    Kept out of the P&L but NOT out of the cash estimate: the money has left
    the bank whether or not it reduced profit.
    """
    return round(sum(float(r["amount"]) for r in capital_rows(rows)), 2)


def review_rows(rows):
    """Rows flagged for a second look — an unsure classification, or Misc."""
    return [r for r in rows
            if r.get("needs_review")
            or str(r.get("category", "")).strip().lower() in ("misc", "")]


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
    """COGS at the cost each sale actually carried.

    A sale stamps the drink's cost onto its own row, so what a month cost is
    settled once the month is over. Reading today's price instead restated
    every closed month: the same May sales reported ₦60,000 profit in May and
    ₦30,000 in July, without a single May row changing.

    ``cost_map`` (lower-cased drink name → current cost) is the fallback for
    rows written before the stamp existed, and for a drink with no cost
    recorded. That is the old behaviour, kept only where nothing better can be
    reconstructed.
    """
    total = 0.0
    for row in sales_rows:
        qty = int(row["quantity"])
        try:
            stamped = float(row.get("cost_price") or 0)
        except (TypeError, ValueError):
            stamped = 0.0
        unit = stamped if stamped > 0 else cost_map.get(row["drink_name"].lower(), 0.0)
        total += qty * unit
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
    # Costs serving the whole business, belonging to neither department. It
    # earns nothing, so its "profit" is its cost negated; it exists as an
    # AccountPnL only so every surface can render it the same way as the other
    # two. Before this existed an overhead row matched neither the `== "bar"`
    # nor the `== "rooms"` filter and left the P&L entirely, overstating profit
    # by its full amount.
    overhead: AccountPnL = None
    capital_spend: float = 0.0
    irregular_spend: float = 0.0

    @property
    def overhead_total(self):
        return self.overhead.expense_total if self.overhead else 0.0

    @property
    def underlying_profit(self):
        """Profit with one-off costs stripped — the run rate.

        Read *beside* net_profit, never instead of it. The money was spent; the
        question this answers is whether the month traded badly or something
        simply broke.
        """
        return round(self.net_profit + self.irregular_spend, 2)

    @property
    def underlying_margin_pct(self):
        return pct_of(self.underlying_profit, self.total_revenue)

    @property
    def has_one_offs(self):
        return self.irregular_spend > 0

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
    op = operating_expenses(expense_rows)
    bar_expenses  = [r for r in op if expense_account(r) == "bar"]
    room_expenses = [r for r in op if expense_account(r) == "rooms"]
    over_expenses = [r for r in op if expense_account(r) == "overhead"]

    drink_revenue = sum_revenue(sales_rows)
    room_revenue  = sum_revenue(room_rows)
    total_revenue = drink_revenue + room_revenue

    cost_of_drinks     = cost_of_drinks_sold(sales_rows, cost_map)
    bar_expense_total  = sum_revenue(bar_expenses, key="amount")
    room_expense_total = sum_revenue(room_expenses, key="amount")
    over_expense_total = sum_revenue(over_expenses, key="amount")

    bar_profit  = drink_revenue - cost_of_drinks - bar_expense_total
    room_profit = room_revenue - room_expense_total
    total_outgoings = (cost_of_drinks + bar_expense_total
                       + room_expense_total + over_expense_total)
    net_profit = total_revenue - total_outgoings

    bar_salary, bar_other   = split_salary(bar_expenses)
    room_salary, room_other = split_salary(room_expenses)
    over_salary, over_other = split_salary(over_expenses)

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
    overhead = AccountPnL(
        revenue=0.0,
        cogs=0.0,
        expense_total=over_expense_total,
        salary=sum(float(r["amount"]) for r in over_salary),
        other_expense=sum_revenue(over_other, key="amount"),
        other_breakdown=_breakdown(over_other),
        profit=-over_expense_total,      # it earns nothing; its cost is its result
    )
    return PnL(
        bar=bar,
        rooms=rooms,
        overhead=overhead,
        total_revenue=total_revenue,
        total_outgoings=total_outgoings,
        net_profit=net_profit,
        restock_spend=restock_spend(expense_rows),
        capital_spend=capital_spend(expense_rows),
        irregular_spend=irregular_spend(expense_rows),
        sales_count=len(sales_rows),
        rooms_count=len(room_rows),
    )


# ── Periodic obligations: accrual and reserve ─────────────────────────
#
# A bill that lands every six months is a cost of all six. Charging it to the
# month it happens to fall in makes that month look like a disaster and the
# other five look better than they were, which is exactly the distortion that
# makes a small hotel mistrust its own P&L.
#
# So the cost and the payment are separated:
#
#   accrual  — expected_amount / months, charged to the P&L every month
#   reserve  — what those accruals have built up, minus what has been paid out
#   payment  — the real invoice: cash out, drawn against the reserve, NOT a cost
#
# The accrual is *computed from the register*, never stored as rows. Storing it
# would need a monthly scheduler and would double-count on any re-run; computing
# it means asking any window, including historical ones, gives the same answer
# every time.


@dataclass(frozen=True)
class Obligation:
    id: int
    name: str
    account: str
    category: str
    expected_amount: float
    months: int             # the divisor: soakaway 6, licences 12, repaint 24
    start_date: object      # date — accrual begins here, never before
    active: bool
    retired_on: object = None   # date — accrual stops here, history stands

    @property
    def monthly_share(self):
        return round(self.expected_amount / self.months, 2) if self.months else 0.0


def _month_span(start, end):
    """Yield (year, month, days_of_that_month_inside_[start, end])."""
    if start > end:
        return
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        last = monthrange(y, m)[1]
        m_start, m_end = date(y, m, 1), date(y, m, last)
        lo, hi = max(m_start, start), min(m_end, end)
        if lo <= hi:
            yield y, m, (hi - lo).days + 1, last
        m = m + 1 if m < 12 else 1
        y = y if m != 1 else y + 1


def accrued_for(obligation, start, end):
    """What this obligation accrues over [start, end].

    Pro-rated by days within each month, so a full month charges exactly the
    monthly share, half a month charges half of it, and an all-time window
    charges one share per month since the obligation started.

    Two boundaries, and both are dates rather than flags:

    * Nothing accrues before ``start_date`` — adding an obligation today must
      not retroactively rewrite last year's profit.
    * Nothing accrues after ``retired_on`` — but everything before it still
      does. Reading the `active` flag here instead was a bug: retiring a bill
      erased every accrual it had ever made, flipping the reserve negative and
      silently changing months that had already been reported.
    """
    if obligation.months <= 0:
        return 0.0
    begin = max(start, obligation.start_date)
    stop = end
    if obligation.retired_on:
        stop = min(stop, obligation.retired_on)
    elif not obligation.active:
        # Retired before the date was recorded: stop at the window's end rather
        # than at zero, which is still better than deleting the history.
        pass
    total = 0.0
    for _y, _m, days_in_window, days_in_month in _month_span(begin, stop):
        total += obligation.monthly_share * days_in_window / days_in_month
    return round(total, 2)


def accrual_rows(obligations, start, end):
    """The accrual as expense-shaped rows, so it flows through everything.

    Synthesised rather than special-cased: given the right account and category
    they pass through the bar/rooms/overhead split, the salary split and the
    category breakdown untouched, so GOPPAR, margins and the allocation all see
    the cost without a single one of them needing to know accrual exists.

    They carry no `id` and are marked `accrual` so the expense report can list
    them apart from real entries — there is nothing to tap on an accrual.
    """
    rows = []
    for ob in obligations:
        amount = accrued_for(ob, start, end)
        if not ob.active and not ob.retired_on:
            continue        # retired with no date: safest to stop accruing now
        if amount <= 0:
            continue
        rows.append({
            "id": None, "timestamp": f"{end} 00:00:00",
            "account": ob.account, "category": ob.category,
            "amount": amount, "description": f"{ob.name} (1/{ob.months} monthly share)",
            "expense_class": "operating",   # it IS this period's operating cost
            "accrual": True, "obligation_id": ob.id, "obligation_name": ob.name,
        })
    return rows


@dataclass(frozen=True)
class ReserveLine:
    obligation: Obligation
    accrued: float          # built up since the obligation started
    paid: float             # drawn against it
    balance: float          # accrued − paid; negative means underfunded

    @property
    def funded(self):
        return self.balance >= 0

    @property
    def materially_short(self):
        """Short by more than a month's own share.

        A part-month gap is timing, not a funding problem: on the 29th of a
        30-day month the reserve is one day light by construction, and warning
        about ₦500 on a ₦90,000 bill trains the owner to ignore the warning.
        """
        return self.balance < -self.obligation.monthly_share

    @property
    def months_covered(self):
        share = self.obligation.monthly_share
        return round(self.balance / share, 1) if share else 0.0


@dataclass(frozen=True)
class Reserve:
    lines: tuple
    accrued_total: float
    paid_total: float
    balance: float
    unlinked_paid: float    # periodic payments not tied to any obligation
    monthly_total: float    # what the register costs per month, all in

    @property
    def underfunded(self):
        """Only the materially short ones — see ReserveLine.materially_short."""
        return tuple(l for l in self.lines if l.materially_short)


def compute_reserve(obligations, expense_rows, today):
    """Accrued-to-date against paid-to-date, per obligation and in total.

    ``expense_rows`` should be **all-time**: a reserve is a running balance, so
    windowing it would report the month's movement as the whole pot.

    A payment carrying no ``obligation_id`` still drains the reserve — it is
    real money out — but it cannot be attributed, so it is reported separately
    rather than silently charged against whichever obligation sorted first.
    """
    payments = periodic_rows(expense_rows)
    by_ob: dict = {}
    unlinked = 0.0
    for p in payments:
        oid = p.get("obligation_id")
        if oid:
            by_ob[int(oid)] = by_ob.get(int(oid), 0.0) + float(p["amount"])
        else:
            unlinked += float(p["amount"])

    lines = []
    for ob in obligations:
        accrued = accrued_for(ob, ob.start_date, today)
        paid = round(by_ob.get(ob.id, 0.0), 2)
        lines.append(ReserveLine(obligation=ob, accrued=accrued, paid=paid,
                                 balance=round(accrued - paid, 2)))

    accrued_total = round(sum(l.accrued for l in lines), 2)
    paid_total = round(sum(l.paid for l in lines) + unlinked, 2)
    return Reserve(
        lines=tuple(lines),
        accrued_total=accrued_total,
        paid_total=paid_total,
        balance=round(accrued_total - paid_total, 2),
        unlinked_paid=round(unlinked, 2),
        monthly_total=round(sum(ob.monthly_share for ob in obligations if ob.active), 2),
    )


# ── Unmatched credit sales ────────────────────────────────────────────
#
# A debt is recorded on its own table and creates no sale. The intended
# sequence is both: record the sale, then record the debt for the part not
# paid. The cash estimate is built on that — it treats revenue as collected
# unless a debt says otherwise.
#
# When only the debt is entered, the arithmetic goes somewhere impossible: it
# subtracts a receivable from revenue that never included it, and reports cash
# falling because a guest drank on credit. Nothing linked the two records, so
# nothing could notice.
#
# Detection is deliberately conservative. A debt raised on a day when that
# account recorded no revenue at all cannot have a matching sale behind it —
# there is nothing for it to be part of. Anything looser would flag honest
# entries, and a control that cries wolf gets ignored.


@dataclass(frozen=True)
class UnmatchedDebts:
    rows: tuple
    total: float

    @property
    def any(self):
        return bool(self.rows)


def unmatched_debts(debtor_rows, sales_rows, room_rows):
    """Debts raised on a day their account took no money at all.

    Returns the rows so they can be named and corrected, not just counted: the
    fix is to add the missing sale, and that needs the date and the amount.
    """
    bar_days, room_days = set(), set()
    for r in sales_rows:
        dt = parse_ts(r.get("timestamp"))
        if dt:
            bar_days.add(dt.date())
    for r in room_rows:
        dt = parse_ts(r.get("timestamp"))
        if dt:
            room_days.add(dt.date())

    found = []
    for d in debtor_rows:
        dt = parse_ts(d.get("timestamp"))
        if not dt:
            continue
        account = str(d.get("account") or "").lower()
        days = room_days if account == "rooms" else bar_days
        if dt.date() not in days:
            found.append(d)
    return UnmatchedDebts(
        rows=tuple(found),
        total=round(sum(float(r["amount"]) for r in found), 2),
    )


# ── Contingency: sizing the buffer from history, not a forecast ───────
#
# The register accrues bills you can name. Nobody can name the compressor that
# fails next March, so there is nothing to divide and nothing to accrue.
#
# What *can* be measured is how much the unforeseeable has actually cost. A
# trailing average of tagged one-offs is a self-calibrating figure: it needs no
# prediction, it moves as the building ages, and it can be compared directly
# with what the `buffer` allocation already sets aside each month.
#
# This is deliberately advisory. It reports the gap and leaves the percentage
# alone — silently re-sizing an allocation the owner set on purpose is how a
# tool stops being trusted.


# Below this much history a trailing average is noise, not a rate: one
# compressor failure in a single month reads as ₦420,000 every month forever.
MIN_CONTINGENCY_MONTHS = 6


@dataclass(frozen=True)
class Contingency:
    months_observed: int
    window_months: int
    total_irregular: float
    monthly_average: float      # what the unforeseeable has actually cost
    monthly_revenue: float
    buffer_pct: float
    buffer_monthly: float       # what the buffer allocation sets aside
    gap: float                  # average − buffer; positive means short
    biggest: tuple              # the largest one-offs, for context
    suggested_pct: float        # the buffer % that would cover the average

    @property
    def covered(self):
        return self.gap <= 0

    @property
    def has_history(self):
        return self.months_observed > 0 and self.total_irregular > 0

    @property
    def reliable(self):
        """Enough months to call the average a rate rather than an accident.

        One breakdown in one month of data averages to that breakdown every
        month forever, and a tool that recommends tripling the buffer off n=1
        is worse than one that says it does not know yet.
        """
        return self.months_observed >= MIN_CONTINGENCY_MONTHS


def compute_contingency(expense_all, sales_all, rooms_all, buffer_pct, now,
                        window_months=12):
    """What the unforeseeable has cost per month, against what is being set aside.

    ``expense_all`` / ``sales_all`` / ``rooms_all`` are all-time rows; the
    trailing window is applied here so the two halves can never be windowed
    against each other inconsistently.

    ``months_observed`` counts the months actually covered by the data, not the
    window length — averaging three months of history over twelve would report
    a quarter of the true rate and tell the owner they are comfortably covered.
    """
    today = now.date() if hasattr(now, "date") else now
    start_month = today.month - (window_months - 1)
    start_year = today.year
    while start_month <= 0:
        start_month += 12
        start_year -= 1
    start = date(start_year, start_month, 1)

    window_exp = filter_by_range(expense_all, start, today)
    one_offs = irregular_rows(window_exp)
    total = round(sum(float(r["amount"]) for r in one_offs), 2)

    # Months of real history: from the first entry of any kind, capped at the
    # window. A hotel three months old must not be averaged over a year.
    stamps = [dt.date() for dt in
              (parse_ts(r.get("timestamp"))
               for r in list(expense_all) + list(sales_all) + list(rooms_all)) if dt]
    first = max(min(stamps), start) if stamps else today
    observed = (today.year - first.year) * 12 + (today.month - first.month) + 1
    observed = max(min(observed, window_months), 0)

    monthly_avg = round(total / observed, 2) if observed else 0.0

    revenue = round(sum_revenue(filter_by_range(sales_all, start, today))
                    + sum_revenue(filter_by_range(rooms_all, start, today)), 2)
    monthly_rev = round(revenue / observed, 2) if observed else 0.0
    buffer_monthly = round(monthly_rev * float(buffer_pct) / 100, 2)

    biggest = tuple(sorted(one_offs, key=lambda r: -float(r["amount"]))[:3])
    return Contingency(
        months_observed=observed, window_months=window_months,
        total_irregular=total, monthly_average=monthly_avg,
        monthly_revenue=monthly_rev, buffer_pct=float(buffer_pct),
        buffer_monthly=buffer_monthly,
        gap=round(monthly_avg - buffer_monthly, 2),
        biggest=biggest,
        suggested_pct=round(monthly_avg / monthly_rev * 100, 1) if monthly_rev else 0.0,
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
    capital_cash: float = 0.0    # asset purchases: out of the P&L, out of the bank
    periodic_cash: float = 0.0   # periodic bills paid: drawn from the reserve
    unmatched_receivables: float = 0.0   # debts exceeding the revenue they came from


def compute_cash_position(sales_all, rooms_all, expense_all, draws_all, debtor_rows,
                          stock_value, opening, anchor_dt, cost_map, now,
                          obligations=()):
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
    # Capital is out of the P&L, not out of the bank. Excluding it here too
    # would report money that has already been spent.
    capital_cash = capital_spend(cash_exp)
    # Same story: the accrual carried the cost, but the invoice was paid in real
    # money. A reserve is a bookkeeping pot, not a separate bank account.
    periodic_cash = periodic_spend(cash_exp)
    draws_cash   = round(sum(float(r["amount"]) for r in cash_draws), 2)
    # Only subtract debts CREATED in the counted window — pre-anchor debts were
    # never collected and aren't part of the anchored opening balance either.
    recv_cash = round(sum(
        float(r["amount"]) - float(r.get("amount_paid") or 0)
        for r in _since(outstanding)), 2)
    # Assume cash unless an outstanding debtor says otherwise. Clamped at zero:
    # you cannot collect a negative amount, and an unmatched debt used to drive
    # this below it — reporting cash falling because a guest drank on credit.
    # `unmatched_debts` is what explains the shortfall rather than hiding it.
    collected = round(max(rev_cash - recv_cash, 0.0), 2)
    uncollectable = round(max(recv_cash - rev_cash, 0.0), 2)
    cash = round(opening + collected - opex_cash - restock_cash
                 - capital_cash - periodic_cash - draws_cash, 2)

    # The profit footnote must agree with /report, which means it accrues too.
    # Each window accrues over its own span: all-time from the earliest
    # obligation, this month from the 1st. Without this the same month read
    # -₦4,500 on /report and +₦10,000 here.
    today = now.date() if hasattr(now, "date") else now
    starts = [ob.start_date for ob in obligations] or [today]
    all_accrual = accrual_rows(obligations, min(starts), today)
    month_accrual = accrual_rows(obligations, date(now.year, now.month, 1), today)

    *_, profit_all = net_profit(sales_all, rooms_all, expense_all + all_accrual, cost_map)
    month_sales = filter_by_month(sales_all, now.year, now.month)
    month_rooms = filter_by_month(rooms_all, now.year, now.month)
    month_exp   = filter_by_month(expense_all, now.year, now.month)
    *_, month_profit = net_profit(month_sales, month_rooms,
                                  month_exp + month_accrual, cost_map)

    return CashPosition(
        opening=opening, anchor_dt=anchor_dt, collected=collected,
        opex_cash=opex_cash, restock_cash=restock_cash, draws_cash=draws_cash, cash=cash,
        capital_cash=capital_cash, periodic_cash=periodic_cash,
        unmatched_receivables=uncollectable,
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
    bar_salary_rows,  _bar_other  = split_salary([r for r in op if expense_account(r) == "bar"])
    room_salary_rows, _room_other = split_salary([r for r in op if expense_account(r) == "rooms"])
    over_salary_rows, _over_other = split_salary([r for r in op if expense_account(r) == "overhead"])

    bar_salary_amt  = sum(float(r["amount"]) for r in bar_salary_rows)
    room_salary_amt = sum(float(r["amount"]) for r in room_salary_rows)
    over_salary_amt = sum(float(r["amount"]) for r in over_salary_rows)
    total_salary    = bar_salary_amt + room_salary_amt + over_salary_amt

    bar_exp  = sum(float(r["amount"]) for r in op if expense_account(r) == "bar")
    room_exp = sum(float(r["amount"]) for r in op if expense_account(r) == "rooms")
    over_exp = sum(float(r["amount"]) for r in op if expense_account(r) == "overhead")
    total_exp = bar_exp + room_exp + over_exp
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
    now = now or clock.now()
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
#
# Not every let is a night. A hotel that also sells rooms by the hour records
# those against the same `rooms` table, and the `nights` column on such a row
# is really a count of *lets*, not nights. Treated as nights they wreck two
# figures and only two:
#
#   occupancy — a room let three times in a day reports 300% of itself
#   ADR       — a ₦3,000 two-hour let averages with a ₦15,000 overnight stay
#
# RevPAR and GOPPAR are unharmed, because their denominator is room-*days*:
# revenue per available room-day is a fair question whether the room earned it
# from one guest or six. That is why they stay the cross-trade comparator here.
#
# The fix is to give every room type a stay length. Anything shorter than a
# full room-day is an hourly type: its units are lets, its rate is per let, and
# its share of the room is measured in hours. A type with no configured length
# is nightly, so a hotel that has never touched this is unaffected — the
# golden-master tests pin that.

NIGHT_HOURS = 24.0  # an overnight stay holds the room for the whole room-day


def stay_hours(row, hours_map=None):
    """How long one stay-unit of this booking holds the room, in hours.

    A duration stored on the row wins (a negotiated let), so reconfiguring a
    room type later can never rewrite what a past booking actually was. With
    nothing stored the row defers to its type, which is what lets a hotel fix
    years of history by declaring "short time is 2 hours" once.
    """
    try:
        stored = float(row.get("duration_hours") or 0)
    except (TypeError, ValueError):
        stored = 0.0
    if stored > 0:
        return stored
    rtype = str(row.get("room_type") or "").strip().lower()
    return float((hours_map or {}).get(rtype, NIGHT_HOURS))


def is_short_stay(row, hours_map=None):
    return stay_hours(row, hours_map) < NIGHT_HOURS


@dataclass(frozen=True)
class RoomMetrics:
    total_rooms: int
    days: int
    available_room_nights: int
    room_nights_sold: int   # overnight units only — lets are counted separately
    occupancy_pct: float    # overnight occupancy, the classic figure
    adr: float              # revenue per room-night sold — overnight only
    revpar: float           # revenue per *available* room-night — all trades
    revenue: float
    by_type: dict
    short_lets: int = 0             # hourly units sold
    short_revenue: float = 0.0
    night_revenue: float = 0.0
    room_hours_sold: float = 0.0
    available_room_hours: float = 0.0
    utilization_pct: float = 0.0    # share of room-time sold, both trades
    arl: float = 0.0                # average revenue per let (hourly ADR)

    @property
    def has_short_stay(self):
        return self.short_lets > 0


def compute_room_metrics(room_rows, total_rooms, days, rooms_by_type=None,
                         hours_by_type=None):
    """The standard hotel yield metrics, told apart by stay length.

    ADR says what you charge; occupancy says how full you are; RevPAR combines
    both and is the only one that can't be gamed — discounting to fill rooms
    lifts occupancy while RevPAR stays flat or falls.

    ``total_rooms`` of 0 means the owner hasn't recorded the room count yet, so
    occupancy and RevPAR are undefined (0.0) while ADR still works.

    ``rooms_by_type`` (lower-cased type → room count) unlocks the same split per
    room type. It is optional and per-type: a type with no recorded count gets
    ``rooms == 0`` and ``revpar == 0.0`` rather than borrowing the hotel-wide
    denominator, which would silently credit every room in the building to one
    category. By-type ADR needs no denominator, so it is always populated.

    ``hours_by_type`` (lower-cased type → hours per stay-unit) marks the hourly
    types. Their units are *lets*, kept out of ``room_nights_sold`` and out of
    ``adr`` — a two-hour let is neither a night nor a nightly rate — and
    reported as ``short_lets`` / ``arl`` instead. Both trades still count in
    full toward ``revenue``, ``revpar`` and ``utilization_pct``. Omit the map
    and every type is nightly, which is exactly the old behaviour.
    """
    revenue = round(sum_revenue(room_rows), 2)
    available = max(int(total_rooms), 0) * max(int(days), 0)
    counts = {str(k).lower(): int(v) for k, v in (rooms_by_type or {}).items()}
    hours_map = {str(k).lower(): float(v) for k, v in (hours_by_type or {}).items()}

    nights_sold = lets_sold = 0
    night_revenue = short_revenue = hours_sold = 0.0
    by_type: dict = {}
    for r in room_rows:
        units = int(r["quantity"]) * int(r["nights"])
        rev = float(r["total_revenue"])
        per_unit = stay_hours(r, hours_map)
        short = per_unit < NIGHT_HOURS
        hours_sold += units * per_unit
        if short:
            lets_sold += units
            short_revenue += rev
        else:
            nights_sold += units
            night_revenue += rev

        rt = str(r["room_type"]).title()
        d = by_type.setdefault(rt, {"nights": 0, "lets": 0, "revenue": 0.0,
                                    "bookings": 0, "hours": 0.0, "is_short": short,
                                    "stay_hours": per_unit})
        d["bookings"] += int(r["quantity"])
        d["revenue"] += rev
        d["hours"] += units * per_unit
        d["lets" if short else "nights"] += units

    available_hours = available * NIGHT_HOURS
    for rtype, d in by_type.items():
        # A type's rate is per night or per let, never a blend of the two.
        units = d["lets"] if d["is_short"] else d["nights"]
        d["adr"] = round(d["revenue"] / units, 2) if units else 0.0
        rooms = max(counts.get(rtype.lower(), 0), 0)
        type_available = rooms * max(int(days), 0)
        d["rooms"] = rooms
        d["available"] = type_available
        # An hourly type's "occupancy" is the share of its room-time sold —
        # counting lets against room-days is what produced 300% occupancy.
        d["utilization_pct"] = pct_of(d["hours"], type_available * NIGHT_HOURS)
        d["occupancy_pct"] = (d["utilization_pct"] if d["is_short"]
                              else pct_of(d["nights"], type_available))
        d["revpar"] = round(d["revenue"] / type_available, 2) if type_available else 0.0

    return RoomMetrics(
        total_rooms=int(total_rooms), days=int(days),
        available_room_nights=available, room_nights_sold=nights_sold,
        occupancy_pct=pct_of(nights_sold, available),
        adr=round(night_revenue / nights_sold, 2) if nights_sold else 0.0,
        revpar=round(revenue / available, 2) if available else 0.0,
        revenue=revenue, by_type=by_type,
        short_lets=lets_sold,
        short_revenue=round(short_revenue, 2),
        night_revenue=round(night_revenue, 2),
        room_hours_sold=round(hours_sold, 2),
        available_room_hours=round(available_hours, 2),
        utilization_pct=pct_of(hours_sold, available_hours),
        arl=round(short_revenue / lets_sold, 2) if lets_sold else 0.0,
    )


# ── Period-over-period room trend ─────────────────────────────────────
#
# A single period's RevPAR is a number; two periods make it a signal. The
# direction of occupancy against the direction of RevPAR is what tells you
# whether to raise, hold or cut — the same rate/volume trade RevPAR exists to
# expose, read over time:
#
#   occupancy ↑  RevPAR ↑   growing properly — hold rates
#   occupancy ↑  RevPAR →   underpriced — the extra rooms are earning nothing
#   occupancy ↑  RevPAR ↓   the discount cost more than it brought in
#   occupancy ↓  RevPAR ↑   rate-led: fewer rooms, each worth more
#   occupancy ↓  RevPAR ↓   overpriced, or demand is genuinely soft
#
# Moves smaller than TREND_BAND% are read as flat. Without a dead band, a hotel
# this size would get a fresh "raise your prices" verdict from one extra booking.

TREND_BAND = 5.0  # relative % change below which a move is noise, not a trend

TREND_VERDICTS = {
    ("up",   "up"):   "Growing the healthy way — hold rates.",
    ("up",   "flat"): "Filling more rooms for the same revenue per room — you look underpriced. Test a rate rise.",
    ("up",   "down"): "More rooms filled, less earned per available room — the discount cost more than it brought in.",
    ("flat", "up"):   "Same occupancy, more revenue per room — a clean rate gain.",
    ("flat", "flat"): "Steady — no real movement either way.",
    ("flat", "down"): "Same occupancy but earning less per room — rate has slipped.",
    ("down", "up"):   "Fewer rooms sold but each worth more — the rate rise is carrying it.",
    ("down", "flat"): "Emptier, but revenue per room held — rate is absorbing the drop.",
    ("down", "down"): "Emptier and earning less — either overpriced, or demand is genuinely soft.",
}


@dataclass(frozen=True)
class RoomTrend:
    current: RoomMetrics
    prior: RoomMetrics
    label: str
    prior_label: str
    comparable: bool           # False when the prior window has nothing to compare against
    revpar_delta_pct: float    # relative %
    adr_delta_pct: float       # relative %
    occupancy_delta_pt: float  # percentage POINTS — occupancy is already a %
    occupancy_delta_pct: float # relative %, which is what direction is read from
    revenue_delta_pct: float
    occupancy_dir: str         # "up" | "flat" | "down"
    revpar_dir: str
    adr_dir: str
    verdict: str
    rate_note: str             # the pass-through check; "" when the rate barely moved


def _rel_change(now_val, then_val):
    """Relative % change, or 0.0 when there's no base to divide by."""
    return round((now_val - then_val) / then_val * 100, 1) if then_val else 0.0


def _direction(delta_pct, band=TREND_BAND):
    if delta_pct > band:
        return "up"
    if delta_pct < -band:
        return "down"
    return "flat"


def compare_room_metrics(current, prior, label="", prior_label=""):
    """Read two RoomMetrics windows as a trend, with a verdict and a rate check.

    ``comparable`` is False when the prior window sold nothing: every delta
    would divide by zero, and "RevPAR up ∞%" is worse than saying there is no
    baseline yet.
    """
    comparable = (prior.room_nights_sold + prior.short_lets) > 0 and prior.revenue > 0

    revpar_d = _rel_change(current.revpar, prior.revpar)
    adr_d    = _rel_change(current.adr, prior.adr)
    occ_d    = _rel_change(current.occupancy_pct, prior.occupancy_pct)
    rev_d    = _rel_change(current.revenue, prior.revenue)

    occ_dir    = _direction(occ_d) if comparable else "flat"
    revpar_dir = _direction(revpar_d) if comparable else "flat"
    adr_dir    = _direction(adr_d) if comparable else "flat"

    verdict = TREND_VERDICTS[(occ_dir, revpar_dir)] if comparable else ""

    # The pass-through check: a rate change only counts for what reaches RevPAR.
    # Every combination is spelled out because "not up" covers two very different
    # outcomes — held flat (the rise was cancelled out) and fell (it backfired).
    rate_note = ""
    if comparable and adr_dir == "up":
        rate_note = {
            "up":   "The rate rise is sticking — it reached RevPAR.",
            "flat": "The rate rise is not reaching RevPAR — lost bookings are cancelling it out.",
            "down": "The rate rise backfired — it lost more in bookings than it gained in rate.",
        }[revpar_dir]
    elif comparable and adr_dir == "down":
        rate_note = {
            "up":   "Rate fell but RevPAR rose — the extra volume more than paid for it.",
            "flat": "Rate fell and RevPAR held — the extra volume covered it, no more.",
            "down": "Rate fell and RevPAR followed it down.",
        }[revpar_dir]

    return RoomTrend(
        current=current, prior=prior, label=label, prior_label=prior_label,
        comparable=comparable,
        revpar_delta_pct=revpar_d, adr_delta_pct=adr_d,
        occupancy_delta_pt=round(current.occupancy_pct - prior.occupancy_pct, 1),
        occupancy_delta_pct=occ_d, revenue_delta_pct=rev_d,
        occupancy_dir=occ_dir, revpar_dir=revpar_dir, adr_dir=adr_dir,
        verdict=verdict, rate_note=rate_note,
    )


# ── GOPPAR (profit per available room-night) ──────────────────────────
#
# RevPAR is a revenue metric. It cannot see fuel, wages, restocking or
# maintenance, so a hotel can post a rising RevPAR straight through a month it
# lost money on. GOPPAR divides *profit* by the same denominator and is the
# bottom-line twin: read side by side, the gap between them is the cost base.
#
# This matters most where room rates move with generator diesel — a rate rise
# that only covers the fuel it was raised for lifts RevPAR and leaves GOPPAR
# exactly where it was. Only the pair shows that.
#
# GOP here is taken straight from compute_pnl: whole-hotel GOP *is*
# PnL.net_profit and rooms GOP *is* PnL.rooms.profit, so GOPPAR can never drift
# from the P&L on the same screen (pinned by tests). Note this is profit after
# *all* recorded operating costs — Hotel 85's expense categories don't separate
# fixed charges (rent, insurance) from operating ones, so it is nearer "net
# operating profit per available room" than strict USALI GOP. Owner draws and
# stock purchases are already excluded upstream by operating_expenses().


@dataclass(frozen=True)
class Goppar:
    available_room_nights: int
    revpar: float             # room revenue per available room-night (top line)
    goppar: float             # whole-hotel profit per available room-night
    rooms_goppar: float       # rooms-account profit per available room-night
    bar_par: float            # bar profit per available room-night
    gop: float                # == PnL.net_profit
    rooms_gop: float          # == PnL.rooms.profit
    bar_gop: float            # == PnL.bar.profit
    conversion_pct: float     # goppar / revpar — can exceed 100% when the bar carries
    rooms_conversion_pct: float   # rooms_goppar / revpar — the rooms margin, always ≤ 100%


def compute_goppar(pnl, available_room_nights, room_revenue=None):
    """Profit per available room-night, alongside the RevPAR it must be read with.

    ``room_revenue`` defaults to ``pnl.rooms.revenue``; pass it only when the
    caller already has a RoomMetrics whose revenue it must match exactly.
    """
    available = max(int(available_room_nights), 0)
    revenue = pnl.rooms.revenue if room_revenue is None else room_revenue

    def _par(amount):
        return round(amount / available, 2) if available else 0.0

    revpar = _par(revenue)
    goppar = _par(pnl.net_profit)
    rooms_goppar = _par(pnl.rooms.profit)

    return Goppar(
        available_room_nights=available,
        revpar=revpar, goppar=goppar, rooms_goppar=rooms_goppar,
        bar_par=_par(pnl.bar.profit),
        gop=round(pnl.net_profit, 2),
        rooms_gop=round(pnl.rooms.profit, 2),
        bar_gop=round(pnl.bar.profit, 2),
        conversion_pct=pct_of(goppar, revpar),
        rooms_conversion_pct=pct_of(rooms_goppar, revpar),
    )


# What a change in top-line room revenue actually did to the bottom line. This
# is the question RevPAR alone can never answer: revenue up + profit flat means
# the increase was eaten on the way through.
GOPPAR_VERDICTS = {
    ("up",   "up"):   "Revenue up and profit up — the gain is reaching the bottom line.",
    ("up",   "flat"): "Revenue up but profit flat — rising costs are absorbing the whole gain.",
    ("up",   "down"): "Revenue up while profit fell — costs are rising faster than rates.",
    ("flat", "up"):   "Same revenue, more profit — costs came down.",
    ("flat", "flat"): "Revenue and profit both steady.",
    ("flat", "down"): "Same revenue, less profit — costs are creeping up.",
    ("down", "up"):   "Earning less but keeping more — a leaner month.",
    ("down", "flat"): "Revenue fell but profit held — costs fell with it.",
    ("down", "down"): "Revenue and profit both down.",
}


@dataclass(frozen=True)
class GopparTrend:
    current: Goppar
    prior: Goppar
    comparable: bool
    revpar_delta_pct: float
    goppar_delta_pct: float
    conversion_delta_pt: float   # percentage POINTS of revenue kept
    revpar_dir: str
    goppar_dir: str
    verdict: str


def compare_goppar(current, prior):
    """Did a change in RevPAR reach GOPPAR, or did costs eat it on the way?"""
    comparable = prior.revpar != 0 and prior.available_room_nights > 0

    revpar_d = _rel_change(current.revpar, prior.revpar)
    # GOP can cross zero between periods, where a relative % is meaningless
    # (−₦5,000 → +₦5,000 is not "−200% growth"). Fall back to direction only.
    if prior.goppar > 0:
        goppar_d = _rel_change(current.goppar, prior.goppar)
        goppar_dir = _direction(goppar_d) if comparable else "flat"
    else:
        goppar_d = 0.0
        goppar_dir = ("up" if current.goppar > prior.goppar else
                      "down" if current.goppar < prior.goppar else "flat") if comparable else "flat"

    revpar_dir = _direction(revpar_d) if comparable else "flat"

    return GopparTrend(
        current=current, prior=prior, comparable=comparable,
        revpar_delta_pct=revpar_d, goppar_delta_pct=goppar_d,
        conversion_delta_pt=round(current.conversion_pct - prior.conversion_pct, 1),
        revpar_dir=revpar_dir, goppar_dir=goppar_dir,
        verdict=GOPPAR_VERDICTS[(revpar_dir, goppar_dir)] if comparable else "",
    )



# ── Day-of-week split (flat rise vs weekday/weekend pricing) ──────────
#
# RevPAR for a whole period answers "is the rate right?" with a single number,
# which is exactly the wrong shape for the decision it gets used for. A hotel
# that is full every Friday and half empty every Tuesday has one blended
# occupancy figure hiding two completely different problems, and a flat rate
# rise applied to both will push the weak nights emptier while still leaving
# money on the table on the strong ones.
#
# Two corrections make this split honest:
#
#   1. A booking is not one night. A row carries a start `timestamp`, a
#      `nights` count and a `quantity` of rooms; attributing the whole stay to
#      the weekday it *began* on would credit a Friday check-in for its Sunday
#      night too. Every room-night is therefore expanded onto the calendar day
#      it was actually slept in — see `expand_room_nights`.
#
#   2. The denominator differs per weekday. A window rarely holds equal numbers
#      of each weekday, so occupancy is divided by that weekday's *own*
#      available room-nights, never by an even share of the period.
#
# Turnaways ride in the same structure because they answer the half of the
# question the bookings cannot: a night that sold out at 100% occupancy looks
# identical whether one guest was refused or twenty, and only the second is
# evidence of pricing power.

# Which nights are "the weekend" for pricing. Friday and Saturday *nights* —
# a Sunday night belongs to the working week for almost every hotel, and the
# guest who checks out on Sunday morning never slept through it.
WEEKEND_NIGHTS = (4, 5)  # Monday = 0

# Both bands need at least this many nights before the gap between them is read
# as a pricing signal. A one-day window has zero weekday nights to compare its
# Friday against, and would otherwise hand back "raise your weekend rates" from
# a single booking.
MIN_BAND_NIGHTS = 2

DOW_NAMES = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
DOW_SHORT = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")


class RoomNight(NamedTuple):
    """One room-type's occupancy of one calendar day."""
    date: object
    room_type: str
    rooms: int          # room-nights (nightly) or lets (hourly) on this day
    revenue: float      # spans every room in the booking, not one
    hours: float        # room-hours this contributes to the day
    short: bool


def expand_room_nights(room_rows, hours_map=None):
    """Explode booking rows onto the calendar days they actually occupy.

    Two shapes, and conflating them is the whole point of this function:

    * A **nightly** stay of N nights spans N consecutive days, so a Friday
      check-in for three nights is credited to Friday, Saturday and Sunday —
      not to three Fridays. Its revenue is apportioned evenly across them.
    * An **hourly** let never leaves the day it happened on. Three two-hour
      lets are three lets on one day, and spreading them over three days the
      way nights spread would invent two days of trade that never existed.

    ``revenue`` spans **all** rooms in its booking — it is a per-day figure,
    not a per-room one. Rows with an unparseable timestamp or a non-positive
    unit count are skipped rather than guessed at: a booking we cannot place on
    the calendar must not silently land on whichever day the raw string sorted
    to.
    """
    for r in room_rows:
        dt = parse_ts(r.get("timestamp"))
        if not dt:
            continue
        try:
            units = int(r["nights"])
            rooms = int(r["quantity"])
            revenue = float(r["total_revenue"])
        except (KeyError, TypeError, ValueError):
            continue
        if units <= 0 or rooms <= 0:
            continue
        per_unit = stay_hours(r, hours_map)
        rtype = str(r.get("room_type", "")).title()
        if per_unit < NIGHT_HOURS:
            yield RoomNight(dt.date(), rtype, rooms * units, revenue,
                            rooms * units * per_unit, True)
            continue
        for i in range(units):
            yield RoomNight(dt.date() + timedelta(days=i), rtype, rooms,
                            revenue / units, rooms * per_unit, False)


def _weekday_counts(start, end):
    """How many of each weekday fall in the inclusive window [start, end]."""
    counts = [0] * 7
    if start > end:
        return counts
    for i in range((end - start).days + 1):
        counts[(start + timedelta(days=i)).weekday()] += 1
    return counts


def _plural_nights(n, kind):
    return f"{n} {kind} night" + ("" if n == 1 else "s")


def _name_days(days):
    """Name the tied days, or nothing when so many tie there is no peak at all.

    Three nights at identical occupancy have no "busiest" between them, and
    picking whichever the sort happened to leave last states a difference the
    numbers do not contain.
    """
    if not days or len(days) > 3:
        return ""
    return " & ".join(DOW_NAMES[d] for d in days)


@dataclass(frozen=True)
class DayBand:
    """One weekday, or one aggregate band (weekday / weekend / all)."""
    label: str
    days: int               # calendar days of this kind in the window
    available: int          # available room-nights
    nights_sold: int        # overnight units only
    revenue: float          # both trades
    turnaways: int
    lets: int = 0           # hourly units
    hours: float = 0.0      # room-hours sold, both trades
    night_revenue: float = 0.0
    short_revenue: float = 0.0

    @property
    def occupancy_pct(self):
        """Overnight occupancy — the classic figure, nights against room-days."""
        return pct_of(self.nights_sold, self.available)

    @property
    def utilization_pct(self):
        """Share of room-time sold, counting hourly lets for what they are."""
        return pct_of(self.hours, self.available * NIGHT_HOURS)

    @property
    def adr(self):
        """Rate per room-night. Overnight only — a let is not a night."""
        return round(self.night_revenue / self.nights_sold, 2) if self.nights_sold else 0.0

    @property
    def arl(self):
        """Average revenue per let, the hourly trade's answer to ADR."""
        return round(self.short_revenue / self.lets, 2) if self.lets else 0.0

    @property
    def revpar(self):
        """Revenue per available room-day — valid across both trades."""
        return round(self.revenue / self.available, 2) if self.available else 0.0

    @property
    def sold_units(self):
        return self.nights_sold + self.lets

    @property
    def turnaways_per_day(self):
        return round(self.turnaways / self.days, 2) if self.days else 0.0


@dataclass(frozen=True)
class DowSplit:
    by_dow: tuple           # 7 DayBands, Monday-first
    weekday: DayBand
    weekend: DayBand
    overall: DayBand
    has_rooms: bool         # a room count exists, so occupancy/RevPAR are real
    turnaways_tracked: bool # at least one turnaway has ever been recorded
    occupancy_gap_pt: float # weekend − weekday, percentage POINTS
    adr_gap_pct: float      # weekend vs weekday rate, relative %
    revpar_gap_pct: float
    verdict: str            # flat rise, split, or fix the weak nights (overnight)
    detail: str             # the reasoning, one or two sentences
    busiest: str            # weekday name with the highest occupancy (or ADR)
    quietest: str
    short_verdict: str = ""   # the same question asked of the hourly trade
    short_detail: str = ""
    lets_gap_pct: float = 0.0 # weekend vs weekday lets per day, relative %
    arl_gap_pct: float = 0.0  # weekend vs weekday rate per let, relative %


def _band(label, days, total_rooms, entries, turnaways):
    # `e.revenue` already spans every room in its booking — multiplying by
    # `e.rooms` again would square the room count and inflate the rate.
    return DayBand(
        label=label, days=days, available=max(int(total_rooms), 0) * days,
        nights_sold=sum(e.rooms for e in entries if not e.short),
        lets=sum(e.rooms for e in entries if e.short),
        revenue=round(sum(e.revenue for e in entries), 2),
        night_revenue=round(sum(e.revenue for e in entries if not e.short), 2),
        short_revenue=round(sum(e.revenue for e in entries if e.short), 2),
        hours=round(sum(e.hours for e in entries), 2),
        turnaways=turnaways,
    )


def compute_dow_split(room_rows, start, end, total_rooms, turnaway_rows=(),
                      hours_map=None):
    """Split a window's room performance across the seven weekdays.

    This is the evidence for the pricing question a single RevPAR figure cannot
    answer: does one rate fit every night, or do Friday and Saturday carry
    demand the working week does not? Pass the *window* rows; ``start``/``end``
    set the denominators, so they must be the same window the rows came from.

    ``total_rooms`` of 0 leaves occupancy and RevPAR at 0.0 and sets
    ``has_rooms`` False — ADR and the turnaway counts still work, so the split
    is worth reading before /setrooms has ever been run.
    """
    day_counts = _weekday_counts(start, end)
    entries = list(expand_room_nights(room_rows, hours_map))
    # Only nights inside the window count — a stay that began before `start`
    # or runs past `end` must contribute only the nights that actually fall in
    # it, or a long booking would inflate a weekday it never occupied here.
    entries = [e for e in entries if start <= e.date <= end]

    turnaways = [0] * 7
    tracked = False
    for t in turnaway_rows:
        dt = parse_ts(t.get("timestamp"))
        if not dt or not (start <= dt.date() <= end):
            continue
        try:
            qty = int(t.get("quantity") or 0)
        except (TypeError, ValueError):
            continue
        tracked = True
        turnaways[dt.date().weekday()] += max(qty, 0)

    by_dow = tuple(
        _band(DOW_NAMES[d], day_counts[d], total_rooms,
              [e for e in entries if e.date.weekday() == d], turnaways[d])
        for d in range(7)
    )
    wknd_idx = set(WEEKEND_NIGHTS)
    weekend = _band("Weekend", sum(day_counts[d] for d in wknd_idx), total_rooms,
                    [e for e in entries if e.date.weekday() in wknd_idx],
                    sum(turnaways[d] for d in wknd_idx))
    weekday = _band("Weekday", sum(day_counts[d] for d in range(7) if d not in wknd_idx),
                    total_rooms, [e for e in entries if e.date.weekday() not in wknd_idx],
                    sum(turnaways[d] for d in range(7) if d not in wknd_idx))
    overall = _band("All nights", sum(day_counts), total_rooms, entries, sum(turnaways))

    has_rooms = max(int(total_rooms), 0) > 0
    occ_gap = round(weekend.occupancy_pct - weekday.occupancy_pct, 1)
    adr_gap = _rel_change(weekend.adr, weekday.adr)
    revpar_gap = _rel_change(weekend.revpar, weekday.revpar)

    # Rank on occupancy where there is a room count, on room-nights where there
    # isn't — ADR would rank by what you charge, not by what sells.
    def _rank_key(d):
        return by_dow[d].utilization_pct if has_rooms else by_dow[d].sold_units

    present = [d for d in range(7) if day_counts[d]]
    # With nothing sold every weekday ties on zero and `max` would hand back
    # whichever landed first — a "busiest night" invented out of no data.
    if overall.sold_units and present:
        top, bottom = max(map(_rank_key, present)), min(map(_rank_key, present))
        busiest = _name_days([d for d in present if _rank_key(d) == top])
        quietest = _name_days([d for d in present if _rank_key(d) == bottom])
    else:
        busiest = quietest = ""

    # Each trade answers for itself. A hotel selling only by the hour has no
    # overnight occupancy to read, and one selling only nights has no lets —
    # issuing the other's verdict from a gap of zero is how a blended number
    # gets mistaken for a finding.
    verdict = detail = ""
    if overall.nights_sold:
        verdict, detail = _pricing_shape(weekday, weekend, occ_gap, adr_gap,
                                         has_rooms, tracked)

    lets_gap = _rel_change(_per_day(weekend.lets, weekend.days),
                           _per_day(weekday.lets, weekday.days))
    arl_gap = _rel_change(weekend.arl, weekday.arl)
    short_verdict, short_detail = ("", "")
    if overall.lets:
        short_verdict, short_detail = _short_stay_shape(
            weekday, weekend, lets_gap, arl_gap)

    return DowSplit(
        by_dow=by_dow, weekday=weekday, weekend=weekend, overall=overall,
        has_rooms=has_rooms, turnaways_tracked=tracked,
        occupancy_gap_pt=occ_gap, adr_gap_pct=adr_gap, revpar_gap_pct=revpar_gap,
        verdict=verdict, detail=detail, busiest=busiest, quietest=quietest,
        short_verdict=short_verdict, short_detail=short_detail,
        lets_gap_pct=lets_gap, arl_gap_pct=arl_gap,
    )


def _pricing_shape(weekday, weekend, occ_gap, adr_gap, has_rooms, tracked):
    """Flat rise or weekday/weekend split — and why.

    The decision turns on whether demand differs by night, and whether the rate
    has already been moved to match. Occupancy is compared in percentage points
    (a 20-point gap is a different hotel on Friday than on Tuesday); the rate
    gap is relative, since what matters is whether the weekend already carries
    a premium worth the name.
    """
    if not weekend.sold_units and not weekday.sold_units:
        return "", "No room-nights in this window to split."
    if min(weekday.days, weekend.days) < MIN_BAND_NIGHTS:
        return ("Too short a window to compare nights.",
                f"This period holds {_plural_nights(weekend.days, 'weekend')} and "
                f"{_plural_nights(weekday.days, 'working-week')} — not enough of each "
                "to tell a rate difference from a single busy booking. Read a week or "
                "a month.")
    if not has_rooms:
        return ("Set your room count for the full answer.",
                "ADR and the night-by-night volumes are below, but without "
                "`/setrooms` there is no occupancy to compare, and occupancy is "
                "what says whether the weekend is genuinely tighter.")

    strong_weekend = occ_gap >= 10          # points — a real demand difference
    weak_weekend = occ_gap <= -10
    premium_priced = adr_gap > TREND_BAND   # the weekend already costs more
    discounted = adr_gap < -TREND_BAND

    if strong_weekend and not premium_priced:
        return ("Split the rate — raise the weekend, hold the weekday.",
                f"The weekend runs {occ_gap:,.1f} points fuller than the working week "
                f"and is charged {'the same' if not discounted else 'less'}. That gap is "
                "unmet demand you are giving away, and a flat rise would take it out of "
                "the weekday nights that can least afford it.")
    if strong_weekend and premium_priced:
        return ("Weekend premium is working — the weekday nights are the problem.",
                f"The weekend already earns {adr_gap:,.0f}% more per night and still runs "
                f"{occ_gap:,.1f} points fuller, so it can likely take more. The weekday "
                "nights need volume, not a higher price — a flat rise would push them "
                "further down.")
    if weak_weekend:
        return ("A weekend premium is not justified here.",
                f"The weekend runs {abs(occ_gap):,.1f} points *emptier* than the working "
                "week — this reads as a business-travel or stopover trade, not a leisure "
                "one. Price the weekday nights as the peak.")
    if premium_priced:
        return ("Flat rise is the safer move.",
                f"Occupancy is level across the week ({occ_gap:+,.1f} points) while the "
                f"weekend already carries a {adr_gap:,.0f}% premium. There is no demand "
                "gap left for a wider split to capture.")
    return ("Flat rise is the right shape.",
            f"Occupancy is level across the week ({occ_gap:+,.1f} points) and the rate is "
            "effectively the same every night. Demand is not telling you to treat the "
            "weekend differently"
            + ("." if tracked else " — though no turnaways have been recorded, so a "
               "sold-out night's unmet demand is still invisible."))


def _per_day(count, days):
    return count / days if days else 0.0


def _short_stay_shape(weekday, weekend, lets_gap, arl_gap):
    """The rate question asked of the hourly trade in its own units.

    Volume here is *lets per day*, not occupancy: an hourly room's ceiling is
    how many times it can be turned over, not whether it is full at midnight.
    The bands hold different numbers of days, so the raw counts have to be
    normalised before the weekend can be compared with the working week.
    """
    if not (weekday.lets and weekend.lets):
        busy = "weekend" if weekend.lets else "working week"
        return ("Hourly trade sits on one part of the week.",
                f"Every let this period fell on the {busy}. Price that separately "
                "from your overnight rooms — but there is nothing to compare it "
                "against yet.")

    busier = lets_gap > TREND_BAND
    quieter = lets_gap < -TREND_BAND
    premium = arl_gap > TREND_BAND

    if busier and not premium:
        return ("Raise the weekend let price.",
                f"Weekend lets run {lets_gap:,.0f}% ahead of the working week at "
                "the same price per let. That is the clearest rate rise on this "
                "screen, and it touches none of your overnight rooms.")
    if busier and premium:
        return ("Weekend let premium is working.",
                f"Weekend lets are {lets_gap:,.0f}% busier and already priced "
                f"{arl_gap:,.0f}% higher. It is holding — the working-week lets "
                "are where the slack is.")
    if quieter:
        return ("The hourly trade is a working-week business.",
                f"Weekend lets run {abs(lets_gap):,.0f}% behind the working week. "
                "Whatever you do to the overnight weekend rate, do not copy it "
                "across to the lets.")
    return ("Hourly demand is level across the week.",
            "Lets per day barely move between the weekend and the working week, "
            "so a single let price is the right shape — even if your overnight "
            "rooms need a split.")


# ── Time of day: where an hourly trade actually lives ─────────────────
#
# A short let is not a night. It runs for an hour or three and it happens at a
# time — mid-morning, after work, late. Splitting it by weekday answers half
# the question; the half that sets the price is *when in the day*, because a
# room that is turned away at 8pm and idle at 10am has two different problems
# and only one of them is a rate.
#
# The band is asked for and stored, never inferred from the timestamp. This
# hotel records bookings in a paper book and keys them in the next morning, so
# a row's timestamp is when the *typing* happened. Reading the hour from it
# reported "the hourly trade is a morning business" — a finding about the
# owner's admin routine, not about the hotel. Only the book knows when the let
# actually was, so the booking flow asks, and a booking that was never asked is
# reported untimed rather than guessed at.

DAYPARTS = (
    ("Morning",   6, 12),
    ("Afternoon", 12, 18),
    ("Evening",   18, 23),
    ("Night",     23, 6),      # wraps midnight
)


DAYPART_NAMES = tuple(name for name, _s, _e in DAYPARTS)


def daypart_of(row):
    """The band recorded on the booking, or None if none was.

    Deliberately reads the stored field and nothing else. Deriving it from the
    timestamp looked reasonable and was wrong: entries keyed the morning after
    all landed in Morning, which then read as a genuine trading pattern.
    """
    val = str(row.get("daypart") or "").strip().title()
    return val if val in DAYPART_NAMES else None


@dataclass(frozen=True)
class DaypartBand:
    label: str
    lets: int
    revenue: float
    hours: float

    @property
    def arl(self):
        return round(self.revenue / self.lets, 2) if self.lets else 0.0


@dataclass(frozen=True)
class DaypartSplit:
    bands: tuple
    untimed_lets: int
    total_lets: int
    days: int

    @property
    def timed_lets(self):
        return sum(b.lets for b in self.bands)

    @property
    def busiest(self):
        live = [b for b in self.bands if b.lets]
        return max(live, key=lambda b: b.lets).label if live else ""

    @property
    def quietest(self):
        live = [b for b in self.bands if b.lets]
        return min(live, key=lambda b: b.lets).label if live else ""

    @property
    def readable(self):
        """Enough timed lets to say anything. Untimed rows carry no hour."""
        return self.timed_lets >= 5

    @property
    def lets_per_day(self):
        return round(self.total_lets / self.days, 2) if self.days else 0.0


def daypart_split(room_rows, start, end, hours_map=None):
    """Hourly lets grouped by time of day, for the window [start, end]."""
    buckets = {name: {"lets": 0, "revenue": 0.0, "hours": 0.0}
               for name, _s, _e in DAYPARTS}
    untimed = total = 0

    for r in room_rows:
        dt = parse_ts(r.get("timestamp"))
        if not dt or not (start <= dt.date() <= end):
            continue
        if not is_short_stay(r, hours_map):
            continue
        band = daypart_of(r)
        try:
            units = int(r["quantity"]) * int(r["nights"])
            revenue = float(r["total_revenue"])
        except (KeyError, TypeError, ValueError):
            continue
        if units <= 0:
            continue
        total += units
        if band is None:
            untimed += units
            continue
        b = buckets[band]
        b["lets"] += units
        b["revenue"] += revenue
        b["hours"] += units * stay_hours(r, hours_map)

    return DaypartSplit(
        bands=tuple(DaypartBand(label=name, lets=buckets[name]["lets"],
                                revenue=round(buckets[name]["revenue"], 2),
                                hours=round(buckets[name]["hours"], 2))
                    for name, _s, _e in DAYPARTS),
        untimed_lets=untimed, total_lets=total,
        days=max((end - start).days + 1, 0),
    )


def daypart_verdict(split):
    """What the day's shape says about the let price.

    Volume is the signal here, not occupancy: an hourly room's ceiling is how
    many times it turns over, and a band nobody books is a band priced for
    demand that is not there at that hour.
    """
    if not split.readable:
        return ("", "")
    live = [b for b in split.bands if b.lets]
    if len(live) == 1:
        band = live[0].label.lower()
        article = "an" if band[0] in "aeiou" else "a"
        return (f"The hourly trade is {article} {band} business.",
                f"Every timed let this period fell in the {band}. "
                "Price that band on its own — the rest of the day is a different "
                "product, or not a product at all.")

    busiest = max(live, key=lambda b: b.lets)
    quietest = min(live, key=lambda b: b.lets)
    spread = pct_of(busiest.lets - quietest.lets, busiest.lets)
    rate_gap = _rel_change(busiest.arl, quietest.arl)

    if spread < 25:
        return ("Demand is even across the day.",
                "No band stands out, so a single let price is the right shape.")
    if rate_gap > TREND_BAND:
        return (f"{busiest.label} is busiest and already priced higher.",
                f"{busiest.label} runs {busiest.lets} lets against "
                f"{quietest.lets} in the {quietest.label.lower()}, and already "
                f"earns {rate_gap:,.0f}% more per let. It is holding — the quiet "
                "band is where the empty hours are.")
    return (f"Charge more in the {busiest.label.lower()}.",
            f"{busiest.label} takes {busiest.lets} lets against "
            f"{quietest.lets} in the {quietest.label.lower()}, at about the same "
            "price. That is the clearest rate rise on this screen, and it leaves "
            "your overnight rooms alone.")


@dataclass(frozen=True)
class TurnawaySummary:
    total: int              # guests/rooms refused
    days_with_data: int     # distinct days that carry at least one record
    by_type: dict           # room type → count
    by_reason: dict         # reason → count
    tracked: bool
    lost_revenue: float     # turnaways × ADR — the visible cost of saying no


def summarize_turnaways(turnaway_rows, start, end, adr=0.0):
    """Roll up refused bookings for a window.

    ``lost_revenue`` prices the refusals at the rate actually achieved (ADR),
    which is the honest floor: had the rooms existed they would have sold at
    about what the sold ones did. It is deliberately not priced at a raised
    rate — that would assume the very rise this report is meant to test.
    """
    total = 0
    by_type: dict = {}
    by_reason: dict = {}
    days = set()
    for t in turnaway_rows:
        dt = parse_ts(t.get("timestamp"))
        if not dt or not (start <= dt.date() <= end):
            continue
        try:
            qty = max(int(t.get("quantity") or 0), 0)
        except (TypeError, ValueError):
            continue
        total += qty
        days.add(dt.date())
        rtype = str(t.get("room_type") or "").strip().title() or "Unspecified"
        by_type[rtype] = by_type.get(rtype, 0) + qty
        reason = str(t.get("reason") or "").strip().lower() or "not given"
        by_reason[reason] = by_reason.get(reason, 0) + qty
    return TurnawaySummary(
        total=total, days_with_data=len(days),
        by_type=dict(sorted(by_type.items(), key=lambda kv: -kv[1])),
        by_reason=dict(sorted(by_reason.items(), key=lambda kv: -kv[1])),
        tracked=bool(days), lost_revenue=round(total * float(adr or 0), 2),
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
    stock_units: int            # bar + store
    bar_units: int
    store_units: int
    tied_value: float

    @property
    def stranded_in_store(self):
        """Stock exists but none of it is in the bar, so it *cannot* be sold.

        A zero-seller in this state is a missed `/transfer`, not a demand
        problem — a very different fix from delisting it.
        """
        return self.bar_units == 0 and self.store_units > 0


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
            "bar_units": int(i.get("bar_stock", 0)),
            "store_units": int(i.get("store_stock", 0)),
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
            stock_units=r["bar_units"] + r["store_units"],
            bar_units=r["bar_units"], store_units=r["store_units"],
            tied_value=round((r["bar_units"] + r["store_units"]) * r["cost_price"], 2),
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
    surplus_units: int = 0
    surplus_value: float = 0.0
    cogs: float = 0.0
    shrink_pct_of_cogs: float = 0.0
    flagged: tuple = ()

    @property
    def worst(self):
        """The five costliest items, in the order they should be worked."""
        return tuple(d for d in self.by_drink if d["units"] != 0)[:5]

    @property
    def clean(self):
        """No shortage AND no surplus. A surplus is not a clean count."""
        return self.shrink_units == 0 and self.surplus_units == 0


# Status bands, as a percentage of what the books expected. Absolute units
# cannot carry this: 3 bottles short of 12 is a different event from 3 short of
# 600, and only the ratio tells them apart.
VARIANCE_WATCH = -1.0     # 🟡 below this
VARIANCE_FLAG = -3.0      # 🔴 below this


def variance_status(pct):
    """🟢 normal handling / 🟡 watch / 🔴 flag — and 🔴 for any surplus.

    A positive variance is never good news. More bottles than the books expect
    means sales went unrecorded or a purchase was logged twice: the same leak
    seen from the other side, and it must never read as a clean count.
    """
    if pct > 0:
        return "🔴", "surplus — unrecorded sales or a double-logged purchase"
    if pct >= VARIANCE_WATCH:
        return "🟢", "normal handling"
    if pct >= VARIANCE_FLAG:
        return "🟡", "watch"
    return "🔴", "flag"


def summarize_variance(count_rows, cost_map, cogs=0.0):
    """Roll up stocktake counts into a shrinkage picture.

    A negative variance means fewer units were physically present than the books
    expected — breakage, an unrecorded sale, or theft. The database can never
    detect this on its own: it only ever believes its own arithmetic, so a
    physical count is the one independent observation that makes loss visible.

    ``cogs`` (cost of stock sold for the same period) turns the absolute loss
    into the ratio that can be compared month to month: ₦40,000 short means
    nothing until you know whether ₦400,000 or ₦4m of stock went out.

    Bar and store are summed per drink but their counts stay separate rows, so
    a transfer in flight cannot read as a loss in one place and a gain in the
    other — each location is measured against its own expectation.
    """
    by: dict = {}
    for r in count_rows:
        name = str(r["drink_name"]).lower()
        var = int(r["counted"]) - int(r["expected"])
        d = by.setdefault(name, {"drink": name.title(), "counts": 0, "units": 0,
                                 "value": 0.0, "expected": 0, "counted": 0})
        d["counts"] += 1
        d["units"] += var
        d["expected"] += int(r["expected"])
        d["counted"] += int(r["counted"])
        d["value"] += round(var * cost_map.get(name, 0.0), 2)

    for d in by.values():
        d["pct"] = round(d["units"] / d["expected"] * 100, 1) if d["expected"] else 0.0
        d["status"], d["status_note"] = variance_status(d["pct"])

    # Sorted by naira value of loss, which is the order the owner should work
    # in — 200 units of the cheapest drink can matter less than 4 of the best.
    rows = sorted(by.values(), key=lambda d: d["value"])
    losses = [d for d in rows if d["units"] < 0]
    surpluses = [d for d in rows if d["units"] > 0]
    shrink_value = round(sum(d["value"] for d in losses), 2)
    return VarianceSummary(
        counts=len(count_rows),
        drinks=len(rows),
        total_units=sum(d["units"] for d in rows),
        total_value=round(sum(d["value"] for d in rows), 2),
        shrink_units=sum(d["units"] for d in losses),
        shrink_value=shrink_value,
        by_drink=rows,
        surplus_units=sum(d["units"] for d in surpluses),
        surplus_value=round(sum(d["value"] for d in surpluses), 2),
        cogs=round(float(cogs), 2),
        shrink_pct_of_cogs=pct_of(abs(shrink_value), cogs),
        flagged=tuple(d for d in rows if d["status"] == "🔴"),
    )


def variance_trend(count_rows, cost_map, cogs_by_month, now, months=3):
    """Shrinkage % for each of the last N months, oldest first.

    One month's shrinkage is a number; three make it a direction. Returns
    ``[(label, pct, value), ...]`` — a month with no count is included with
    ``pct`` of None so a skipped month reads as a gap rather than as zero loss.
    """
    out = []
    y, m = now.year, now.month
    span = []
    for _ in range(months):
        span.append((y, m))
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    for yr, mo in reversed(span):
        rows = [r for r in count_rows
                if str(r.get("timestamp", ""))[:7] == f"{yr:04d}-{mo:02d}"]
        label = date(yr, mo, 1).strftime("%b")
        if not rows:
            out.append((label, None, 0.0))
            continue
        vs = summarize_variance(rows, cost_map, cogs_by_month.get((yr, mo), 0.0))
        out.append((label, vs.shrink_pct_of_cogs, vs.shrink_value))
    return out


# ── Room audit: was every night logged, at the rate charged? ──────────
#
# Occupancy can only ever report what was keyed in. A night nobody recorded is
# indistinguishable from a night the room stood empty, and the money for it has
# already been taken. The audit is the one check that compares the system
# against the physical register.
#
# The days are chosen at random *by the bot*. Letting the operator pick them
# selects for days they remember clearly, and those are exactly the days most
# likely to be correct — the sample would be biased towards a clean result.

CAPTURE_FLOOR = 95.0   # below this, fix recording before touching prices


@dataclass(frozen=True)
class RoomAuditDay:
    day: object                 # date
    rooms_total: int
    logged: tuple               # rows the system holds for that date
    nights_logged: int

    @property
    def vacant(self):
        """Rooms the system believes were empty. The point of the exercise."""
        return max(self.rooms_total - self.nights_logged, 0)


def audit_days(room_rows, year, month, count, rng):
    """`count` days drawn at random from the month, never chosen by a person.

    Days are drawn from the whole month up to today, not only days that already
    carry bookings: a day with no entries at all is precisely the day worth
    auditing, and sampling from logged days only would guarantee a clean sheet.
    """
    last = monthrange(year, month)[1]
    today = clock.today()
    end = min(last, today.day) if (year, month) == (today.year, today.month) else last
    pool = [date(year, month, d) for d in range(1, end + 1)]
    if not pool:
        return []
    return sorted(rng.sample(pool, min(count, len(pool))))


def build_audit_day(room_rows, day, rooms_total, hours_map=None):
    """What the system says about one date, room by room."""
    entries = [e for e in expand_room_nights(room_rows, hours_map) if e.date == day]
    nights = sum(e.rooms for e in entries if not e.short)
    return RoomAuditDay(day=day, rooms_total=int(rooms_total),
                        logged=tuple(entries), nights_logged=nights)


@dataclass(frozen=True)
class RoomAuditResult:
    days: int
    nights_logged: int
    nights_actual: int
    rate_variance: float
    variance_count: int
    adr: float
    days_in_month: int

    @property
    def missing(self):
        return max(self.nights_actual - self.nights_logged, 0)

    @property
    def capture_pct(self):
        return pct_of(self.nights_logged, self.nights_actual)

    @property
    def trustworthy(self):
        """Above the floor, pricing decisions can be acted on. Below it, not."""
        return self.capture_pct >= CAPTURE_FLOOR

    @property
    def monthly_leak(self):
        """The gap, scaled to a month and priced at the rate actually achieved.

        Deliberately priced at current ADR, not at a raised rate: the estimate
        must not assume the increase the audit exists to make safe.
        """
        if not self.days:
            return 0.0
        return round(self.missing / self.days * self.days_in_month * self.adr, 2)


def compute_room_audit(days, nights_logged, nights_actual, rate_variance,
                       variance_count, adr, days_in_month):
    return RoomAuditResult(
        days=int(days), nights_logged=int(nights_logged),
        nights_actual=int(nights_actual), rate_variance=round(float(rate_variance), 2),
        variance_count=int(variance_count), adr=round(float(adr), 2),
        days_in_month=int(days_in_month),
    )


def capture_trend(audit_rows, limit=6):
    """Capture rate per stored audit, oldest first — a trend, not a one-off."""
    out = []
    for r in sorted(audit_rows, key=lambda r: str(r.get("audit_date", "")))[-limit:]:
        actual = int(r.get("nights_actual") or 0)
        logged = int(r.get("nights_logged") or 0)
        out.append((str(r.get("audit_date", ""))[:10], pct_of(logged, actual)))
    return out


# ── Rate spread: the check that needs no audit at all ─────────────────


@dataclass(frozen=True)
class RateSpread:
    room_type: str
    nights: int
    min_rate: float
    max_rate: float
    mode_rate: float
    distinct: int

    @property
    def suspicious(self):
        """One single rate across a whole month is not normal pricing.

        Real trade produces walk-ins, regulars, negotiated stays and the odd
        favour. A perfectly flat rate over 30+ room-nights means discounts are
        being given off-book, or the rate is not being captured as charged.
        """
        return self.distinct == 1 and self.nights >= 30


def rate_spread(room_rows, hours_map=None):
    """Min / max / mode rate per room type, and how many distinct rates exist."""
    by: dict = {}
    for r in room_rows:
        try:
            units = int(r["quantity"]) * int(r["nights"])
            rate = round(float(r["total_revenue"]) / units, 2) if units else 0.0
        except (KeyError, TypeError, ValueError, ZeroDivisionError):
            continue
        if units <= 0:
            continue
        rtype = str(r.get("room_type") or "").title()
        by.setdefault(rtype, []).append((rate, units))

    out = []
    for rtype, pairs in by.items():
        rates = [p[0] for p in pairs]
        weight: dict = {}
        for rate, units in pairs:
            weight[rate] = weight.get(rate, 0) + units
        mode = max(weight.items(), key=lambda kv: kv[1])[0] if weight else 0.0
        out.append(RateSpread(
            room_type=rtype, nights=sum(p[1] for p in pairs),
            min_rate=min(rates), max_rate=max(rates), mode_rate=mode,
            distinct=len(set(rates)),
        ))
    return sorted(out, key=lambda s: -s.nights)


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
