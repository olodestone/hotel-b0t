"""
Unit tests for the pure calc core (metrics.py).

These lock the *math* independently of any report formatting, so the dashboard
can rely on compute_pnl / summarize_outstanding directly.
"""
import metrics


def test_compute_pnl_split_and_exclusions():
    sales = [{"drink_name": "beer", "quantity": 2, "total_revenue": 1000}]
    rooms = [{"total_revenue": 2000}]
    expenses = [
        {"account": "bar",   "category": "salary",    "amount": 100},
        {"account": "bar",   "category": "misc",      "amount": 50},
        {"account": "rooms", "category": "laundry",   "amount": 20},
        {"account": "bar",   "category": "restock",   "amount": 999},  # NON-P&L, excluded
    ]
    cost_map = {"beer": 300}

    pnl = metrics.compute_pnl(sales, rooms, expenses, cost_map)

    # Bar account
    assert pnl.bar.revenue == 1000
    assert pnl.bar.cogs == 600                 # 2 × 300
    assert pnl.bar.salary == 100
    assert pnl.bar.other_expense == 50
    assert pnl.bar.expense_total == 150        # salary + other (restock excluded)
    assert pnl.bar.other_breakdown == {"Misc": 50}
    assert pnl.bar.profit == 250               # 1000 - 600 - 150

    # Rooms account (no COGS)
    assert pnl.rooms.revenue == 2000
    assert pnl.rooms.cogs == 0.0
    assert pnl.rooms.expense_total == 20
    assert pnl.rooms.profit == 1980

    # Combined
    assert pnl.total_revenue == 3000
    assert pnl.total_outgoings == 770          # 600 + 150 + 20
    assert pnl.net_profit == 2230
    assert pnl.restock_spend == 999            # tracked separately, never in profit
    assert pnl.sales_count == 1
    assert pnl.rooms_count == 1


def test_restock_never_reduces_profit():
    sales = [{"drink_name": "x", "quantity": 1, "total_revenue": 500}]
    base = metrics.compute_pnl(sales, [], [], {"x": 100})
    with_restock = metrics.compute_pnl(
        sales, [], [{"account": "bar", "category": "restock", "amount": 9_000_000}], {"x": 100}
    )
    assert base.net_profit == with_restock.net_profit  # restock excluded from P&L
    assert with_restock.restock_spend == 9_000_000


def test_cost_of_drinks_unknown_drink_is_zero_cost():
    sales = [{"drink_name": "mystery", "quantity": 5, "total_revenue": 100}]
    assert metrics.cost_of_drinks_sold(sales, {}) == 0.0


def test_summarize_outstanding():
    debtors = [
        {"status": "outstanding", "account": "bar",   "amount": 500,  "amount_paid": 200},
        {"status": "outstanding", "account": "rooms", "amount": 1000, "amount_paid": 0},
        {"status": "paid",        "account": "bar",   "amount": 100,  "amount_paid": 100},
    ]
    od = metrics.summarize_outstanding(debtors)
    assert od.outstanding_count == 2
    assert od.bar_count == 1 and od.bar_owed == 300
    assert od.rooms_count == 1 and od.rooms_owed == 1000
    assert od.total_owed == 1300


def test_active_excludes_soft_deleted():
    rows = [{"id": 1, "deleted_at": None}, {"id": 2, "deleted_at": "2026-06-11 00:00:00"}]
    assert [r["id"] for r in metrics.active(rows)] == [1]


def test_operating_expenses_strips_restock():
    rows = [{"category": "salary"}, {"category": "restock"}, {"category": "utilities"}]
    kept = [r["category"] for r in metrics.operating_expenses(rows)]
    assert kept == ["salary", "utilities"]


# ── Cash position ─────────────────────────────────────────────────────

from datetime import datetime  # noqa: E402

_SALES = [{"drink_name": "x", "quantity": 2, "total_revenue": 1000, "timestamp": "2026-06-10 10:00:00"}]
_ROOMS = [{"total_revenue": 500, "timestamp": "2026-06-10 10:00:00"}]
_EXP = [
    {"account": "bar", "category": "misc",    "amount": 100, "timestamp": "2026-06-05 10:00:00"},
    {"account": "bar", "category": "restock", "amount": 300, "timestamp": "2026-06-05 10:00:00"},
]
_DRAWS = [{"amount": 200, "timestamp": "2026-06-20 10:00:00"}]
_DEBTORS = [{"status": "outstanding", "account": "bar", "amount": 400, "amount_paid": 100, "timestamp": "2026-06-12 10:00:00"}]
_COST = {"x": 100}
_NOW = datetime(2026, 6, 29, 12, 0, 0)


def test_net_profit():
    assert metrics.net_profit(_SALES, _ROOMS, _EXP, _COST) == (1500, 200, 100, 1200)


def test_cash_position_no_anchor():
    pos = metrics.compute_cash_position(
        _SALES, _ROOMS, _EXP, _DRAWS, _DEBTORS,
        stock_value=5000, opening=1000, anchor_dt=None, cost_map=_COST, now=_NOW,
    )
    assert pos.collected == 1200          # 1500 revenue − 300 still-owed
    assert pos.opex_cash == 100           # restock excluded
    assert pos.restock_cash == 300
    assert pos.draws_cash == 200
    assert pos.cash == 1600               # 1000 + 1200 − 100 − 300 − 200
    assert pos.receivables == 300 and pos.outstanding_count == 1
    assert pos.stock_value == 5000
    assert pos.profit_all == 1200 and pos.month_profit == 1200


def test_cash_position_anchor_ignores_prior_flows_but_not_profit():
    pos = metrics.compute_cash_position(
        _SALES, _ROOMS, _EXP, _DRAWS, _DEBTORS,
        stock_value=5000, opening=1000,
        anchor_dt=datetime(2026, 6, 15), cost_map=_COST, now=_NOW,
    )
    # Only the 20 Jun draw is on/after the anchor; sales/rooms/exp/debt are before.
    assert pos.collected == 0 and pos.opex_cash == 0 and pos.restock_cash == 0
    assert pos.draws_cash == 200
    assert pos.cash == 800                # 1000 − 200
    assert pos.profit_all == 1200         # profit is anchor-independent


# ── Allocation ────────────────────────────────────────────────────────

def test_compute_allocation():
    sales = [{"drink_name": "x", "quantity": 2, "total_revenue": 1000}]
    rooms = [{"room_type": "standard", "quantity": 2, "total_revenue": 4000}]
    expenses = [
        {"account": "bar",   "category": "salary",  "amount": 100},
        {"account": "bar",   "category": "misc",    "amount": 50},
        {"account": "rooms", "category": "laundry", "amount": 20},
        {"account": "bar",   "category": "restock", "amount": 500},  # excluded from P&L
    ]
    a = metrics.compute_allocation(
        sales, rooms, expenses, {"x": 100},
        buffer_pct=10, restock_pct=0, draw_pct=50, reinvest_pct=30, float_pct=20,
        pit_low_rate=15, pit_high_rate=24,
    )
    assert a.total_rev == 5000
    assert a.cost_of_drinks == 200 and a.total_salary == 100 and a.other_exp == 70
    assert a.total_outgoings == 370 and a.restock_total == 500
    assert a.buffer_amt == 500 and a.total_save == 500
    assert a.bar_share == 100 and a.room_share == 400      # proportional to revenue
    assert a.working_capital == 4630 and a.after_setaside == 4130
    assert a.draw_amt == 2065 and a.reinvest_amt == 1239 and a.float_amt == 826
    assert a.unallocated == 0
    assert a.pit_low_amt == 309.75 and a.pit_high_amt == 495.6
    assert a.room_by_type == {"Standard": {"bookings": 2, "revenue": 4000.0}}


# ── Staff activity ────────────────────────────────────────────────────

def test_staff_breakdown_groups_by_recorder():
    sales = [
        {"recorded_by": "john", "total_revenue": 3000},
        {"recorded_by": "mary", "total_revenue": 2000},
        {"recorded_by": "john", "total_revenue": 1000},
        {"recorded_by": "",     "total_revenue": 500},   # blank → Unknown
    ]
    rooms = [{"recorded_by": "mary", "total_revenue": 90000}]
    rows = metrics.staff_breakdown(sales, rooms)
    by_name = {r["name"]: r for r in rows}
    assert [r["name"] for r in rows] == ["Unknown", "john", "mary"]  # sorted
    assert by_name["john"]["drink_txns"] == 2 and by_name["john"]["drink_revenue"] == 4000
    assert by_name["mary"]["drink_txns"] == 1 and by_name["mary"]["room_revenue"] == 90000
    assert by_name["mary"]["room_txns"] == 1
    assert by_name["Unknown"]["drink_revenue"] == 500
