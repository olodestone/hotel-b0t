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


# ── Margins (Phase 0) ─────────────────────────────────────────────────

def test_margins_are_computed_from_the_same_pnl():
    sales = [{"drink_name": "beer", "quantity": 10, "total_revenue": 5000}]
    rooms = [{"total_revenue": 5000}]
    expenses = [{"account": "bar", "category": "misc", "amount": 1000}]
    pnl = metrics.compute_pnl(sales, rooms, expenses, {"beer": 200})

    # Bar: 5000 revenue − 2000 COGS = 3000 gross (60%); − 1000 expense = 2000 net (40%)
    assert pnl.bar.gross_profit == 3000
    assert pnl.bar.gross_margin_pct == 60.0
    assert pnl.bar.net_margin_pct == 40.0
    # Rooms carry no COGS, so gross margin is definitionally 100%.
    assert pnl.rooms.gross_margin_pct == 100.0
    # Combined: 10000 revenue − 2000 COGS = 8000 gross; net 7000.
    assert pnl.gross_margin_pct == 80.0
    assert pnl.net_margin_pct == 70.0


def test_margins_do_not_divide_by_zero_on_an_empty_period():
    pnl = metrics.compute_pnl([], [], [], {})
    assert pnl.net_margin_pct == 0.0
    assert pnl.gross_margin_pct == 0.0
    assert pnl.bar.gross_margin_pct == 0.0


def test_supplier_payments_are_cash_not_a_pnl_cost():
    """A credit purchase settled later must never reach the profit calc."""
    sales = [{"drink_name": "x", "quantity": 1, "total_revenue": 500}]
    expenses = [{"account": "bar", "category": "supplier", "amount": 9999}]
    pnl = metrics.compute_pnl(sales, [], expenses, {"x": 100})
    assert pnl.net_profit == 400              # 500 − 100 COGS, supplier ignored
    assert pnl.restock_spend == 9999          # but it IS cash out


# ── Working capital / CCC (Phase 1) ───────────────────────────────────

from datetime import datetime  # noqa: E402

_NOW = datetime(2026, 6, 30, 12, 0, 0)


def _wc(**overrides):
    args = dict(
        sales_all=[{"timestamp": "2026-06-10 10:00:00", "drink_name": "beer",
                    "quantity": 30, "total_revenue": 15000}],
        expense_all=[{"timestamp": "2026-06-05 10:00:00", "account": "bar",
                      "category": "restock", "amount": 6000}],
        debtor_rows=[],
        payment_rows=[],
        stock_rows=[{"drink": "Beer", "bar_stock": 20, "store_stock": 10,
                     "cost_price": 100, "stock_value": 3000, "selling_price": 500}],
        cost_map={"beer": 100},
        window_days=30,
        now=_NOW,
    )
    args.update(overrides)
    return metrics.compute_working_capital(**args)


def test_dio_uses_todays_shelf_when_there_are_no_snapshots():
    wc = _wc()
    # 30 units × ₦100 = ₦3000 COGS over 30 days = ₦100/day; ₦3000 stock = 30 days.
    assert wc.cogs_window == 3000
    assert wc.daily_cogs == 100
    assert wc.dio_days == 30.0
    assert wc.dio_basis == "current"


def test_dio_prefers_snapshot_averages_when_history_exists():
    snaps = [
        {"snapshot_date": "2026-06-28", "drink_name": "beer", "stock_value": 5000},
        {"snapshot_date": "2026-06-29", "drink_name": "beer", "stock_value": 1000},
    ]
    wc = _wc(snapshot_rows=snaps)
    assert wc.dio_basis == "snapshots"
    assert wc.avg_stock_value == 3000        # mean of the two daily totals
    assert wc.dio_days == 30.0


def test_ccc_is_zero_days_of_receivables_when_everything_is_cash():
    wc = _wc()
    assert wc.dso_days is None               # no credit sales → ratio undefined
    assert wc.ccc_days == 30.0               # DIO only


def test_dpo_is_reported_as_untracked_rather_than_zero():
    """A missing DPO overstates the cycle — the report must be able to say so."""
    wc = _wc()
    assert wc.dpo_tracked is False
    assert wc.dpo_days is None

    payables = [{"timestamp": "2026-06-20 10:00:00", "amount": 3000,
                 "amount_paid": 0, "status": "outstanding"}]
    wc2 = _wc(payable_rows=payables)
    assert wc2.dpo_tracked is True
    # Purchases in window = 6000 cash + 3000 credit = 9000 → ₦300/day; 3000/300 = 10 days.
    assert wc2.purchases_window == 9000
    assert wc2.dpo_days == 10.0
    assert wc2.ccc_days == 20.0              # 30 + 0 − 10


def test_supplier_settlements_are_not_counted_as_new_purchases():
    """`supplier` rows settle stock bought earlier; counting them would double up."""
    expenses = [
        {"timestamp": "2026-06-05 10:00:00", "account": "bar", "category": "restock", "amount": 6000},
        {"timestamp": "2026-06-25 10:00:00", "account": "bar", "category": "supplier", "amount": 4000},
    ]
    wc = _wc(expense_all=expenses)
    assert wc.purchases_window == 6000


def test_measured_collection_days_beat_the_ratio_estimate():
    debtors = [
        {"id": 1, "timestamp": "2026-06-01 10:00:00", "amount": 1000, "amount_paid": 1000, "status": "paid"},
        {"id": 2, "timestamp": "2026-06-01 10:00:00", "amount": 1000, "amount_paid": 1000, "status": "paid"},
    ]
    payments = [
        {"debtor_id": 1, "timestamp": "2026-06-06 10:00:00", "amount": 1000},   # 5 days
        {"debtor_id": 2, "timestamp": "2026-06-16 10:00:00", "amount": 1000},   # 15 days
    ]
    wc = _wc(debtor_rows=debtors, payment_rows=payments)
    assert wc.collection_days == 10.0
    assert wc.collection_basis == "window"
    assert wc.settled_count == 2


def test_collection_time_is_weighted_by_amount_across_part_payments():
    """A debt paid in instalments must count each instalment, not just the close.

    An unweighted, settled-debts-only average would report nothing at all here.
    """
    debtors = [{"id": 1, "timestamp": "2026-06-01 10:00:00", "amount": 10000,
                "amount_paid": 9000, "status": "outstanding"}]
    payments = [
        {"debtor_id": 1, "timestamp": "2026-06-03 10:00:00", "amount": 9000},   # 2 days, most of the money
        {"debtor_id": 1, "timestamp": "2026-06-21 10:00:00", "amount": 1000},   # 20 days, a sliver
    ]
    wc = _wc(debtor_rows=debtors, payment_rows=payments)
    # (2×9000 + 20×1000) / 10000 = 3.8 — not the 11.0 an unweighted mean gives.
    assert wc.collection_days == 3.8
    assert wc.settled_count == 2


def test_collection_time_falls_back_to_all_time_when_the_window_is_quiet():
    debtors = [{"id": 1, "timestamp": "2026-01-01 10:00:00", "amount": 500, "status": "paid"}]
    payments = [{"debtor_id": 1, "timestamp": "2026-01-08 10:00:00", "amount": 500}]
    wc = _wc(debtor_rows=debtors, payment_rows=payments)
    assert wc.collection_days == 7.0
    assert wc.collection_basis == "all-time"


def test_receivables_age_into_buckets():
    debtors = [
        {"timestamp": "2026-06-20 10:00:00", "amount": 1000, "amount_paid": 0, "status": "outstanding"},
        {"timestamp": "2026-05-10 10:00:00", "amount": 2000, "amount_paid": 500, "status": "outstanding"},
        {"timestamp": "2026-03-01 10:00:00", "amount": 4000, "amount_paid": 0, "status": "outstanding"},
    ]
    wc = _wc(debtor_rows=debtors)
    buckets = {b.label: b.amount for b in wc.aging}
    assert buckets["0–30 days"] == 1000
    assert buckets["31–60 days"] == 1500     # remainder only, not the face value
    assert buckets["61+ days"] == 4000
    assert wc.receivables == 6500


def test_dead_stock_is_stock_that_did_not_move():
    stock = [
        {"drink": "Beer", "bar_stock": 20, "store_stock": 10, "cost_price": 100, "stock_value": 3000},
        {"drink": "Gin",  "bar_stock": 5,  "store_stock": 0,  "cost_price": 400, "stock_value": 2000},
    ]
    wc = _wc(stock_rows=stock)
    assert [d["drink"] for d in wc.dead_stock] == ["Gin"]
    assert wc.dead_stock_value == 2000


# ── Break-even (Phase 3) ──────────────────────────────────────────────

def test_break_even_revenue_and_margin_of_safety():
    sales = [{"drink_name": "beer", "quantity": 10, "total_revenue": 10000}]
    expenses = [{"account": "bar", "category": "rent", "amount": 3000}]
    be = metrics.compute_break_even(sales, expenses, {"beer": 500})
    # Bar gross margin = (10000 − 5000) / 10000 = 0.5 → break-even = 3000 / 0.5 = 6000.
    assert be.gross_margin_ratio == 0.5
    assert be.break_even_revenue == 6000
    assert be.surplus == 4000
    assert be.margin_of_safety_pct == 40.0


def test_break_even_ignores_rooms_on_both_sides():
    """Room revenue has no stock cost; blending it in flatters the ratio toward
    100%. Room *expenses* leave with it, so the figure stays self-consistent."""
    sales = [{"drink_name": "beer", "quantity": 10, "total_revenue": 10000}]
    expenses = [
        {"account": "bar",   "category": "rent",   "amount": 3000},
        {"account": "rooms", "category": "salary", "amount": 99000},   # excluded
    ]
    be = metrics.compute_break_even(sales, expenses, {"beer": 500})
    assert be.fixed_costs == 3000              # bar costs only
    assert be.gross_margin_ratio == 0.5        # unchanged by room revenue
    assert be.actual_revenue == 10000          # bar revenue only
    assert be.break_even_revenue == 6000


def test_break_even_never_contradicts_a_profitable_bar():
    """Guards the bug that scoping only the *margin* to the bar would reintroduce:
    reporting 'below break-even' in a period the bar actually made money."""
    sales = [{"drink_name": "beer", "quantity": 100, "total_revenue": 50000}]
    expenses = [{"account": "bar", "category": "rent", "amount": 10000}]
    cost_map = {"beer": 200}
    be = metrics.compute_break_even(sales, expenses, cost_map)
    bar_profit = 50000 - metrics.cost_of_drinks_sold(sales, cost_map) - 10000
    assert bar_profit > 0
    assert be.surplus > 0                      # sign agrees with the bar P&L


def test_break_even_is_undefined_when_selling_below_cost():
    sales = [{"drink_name": "beer", "quantity": 10, "total_revenue": 1000}]
    be = metrics.compute_break_even(sales, [{"account": "bar", "category": "rent", "amount": 500}], {"beer": 500})
    assert be.break_even_revenue is None       # no revenue level ever covers costs
    assert be.actual_revenue == 1000           # … distinguishes this from "no bar sales"
    assert be.margin_of_safety_pct is None


def test_rooms_target_is_shared_costs_less_what_the_bar_handed_over():
    """The owner's real monthly target: rooms carry the overheads that exist
    whether or not the bar opens."""
    sales = [{"drink_name": "beer", "quantity": 500, "total_revenue": 250_000}]
    rooms = [{"total_revenue": 1_200_000}]
    expenses = [
        {"account": "bar",   "category": "salary",  "amount": 80_000},    # bar's own cost
        {"account": "rooms", "category": "salary",  "amount": 300_000},   # shared …
        {"account": "rooms", "category": "diesel",  "amount": 300_000},   # … overheads
        {"account": "bar",   "category": "restock", "amount": 999_999},   # NON-P&L, ignored
    ]
    rt = metrics.compute_rooms_target(sales, rooms, expenses, {"beer": 300})

    assert rt.shared_costs == 600_000
    assert rt.bar_contribution == 20_000       # 250k − 150k stock − 80k salary
    assert rt.room_sales_needed == 580_000     # 600k − 20k
    assert rt.surplus == 620_000
    assert rt.covered is True


def test_rooms_target_ties_exactly_to_hotel_profit():
    """room revenue − room target must equal the hotel's net profit, or the
    number is lying to the owner."""
    sales = [{"drink_name": "beer", "quantity": 500, "total_revenue": 250_000}]
    rooms = [{"total_revenue": 1_200_000}]
    expenses = [
        {"account": "bar",   "category": "salary", "amount": 80_000},
        {"account": "rooms", "category": "salary", "amount": 600_000},
    ]
    cost_map = {"beer": 300}
    rt = metrics.compute_rooms_target(sales, rooms, expenses, cost_map)
    pnl = metrics.compute_pnl(sales, rooms, expenses, cost_map)
    assert rt.surplus == pnl.net_profit
    # And the bar leg is the same figure the P&L reports, by construction.
    assert rt.bar_contribution == pnl.bar.profit


def test_a_loss_making_bar_raises_the_room_target():
    """A negative contribution means rooms cover the overheads AND the bar's loss."""
    sales = [{"drink_name": "beer", "quantity": 100, "total_revenue": 50_000}]
    rooms = [{"total_revenue": 400_000}]
    expenses = [
        {"account": "bar",   "category": "salary", "amount": 80_000},
        {"account": "rooms", "category": "salary", "amount": 300_000},
    ]
    rt = metrics.compute_rooms_target(sales, rooms, expenses, {"beer": 300})
    assert rt.bar_contribution == -60_000      # 50k − 30k stock − 80k salary
    assert rt.room_sales_needed == 360_000     # 300k overheads + 60k bar loss
    assert rt.covered is True


def test_rooms_need_nothing_when_the_bar_covers_everything():
    sales = [{"drink_name": "beer", "quantity": 1000, "total_revenue": 900_000}]
    expenses = [{"account": "rooms", "category": "salary", "amount": 100_000}]
    rt = metrics.compute_rooms_target(sales, [], expenses, {"beer": 300})
    assert rt.bar_contribution == 600_000
    assert rt.room_sales_needed == 0.0         # clamped — a negative target is meaningless
    assert rt.covered is True


def test_break_even_is_undefined_with_no_bar_sales_at_all():
    be = metrics.compute_break_even([], [{"account": "bar", "category": "rent", "amount": 500}], {})
    assert be.break_even_revenue is None
    assert be.actual_revenue == 0              # the branch the report words differently
    assert be.fixed_costs == 500


# ── Room metrics (Phase 2b) ───────────────────────────────────────────

def test_occupancy_adr_and_revpar():
    rooms = [
        {"room_type": "standard", "quantity": 2, "nights": 3, "total_revenue": 90000},
        {"room_type": "deluxe",   "quantity": 1, "nights": 2, "total_revenue": 50000},
    ]
    rm = metrics.compute_room_metrics(rooms, total_rooms=10, days=30)
    assert rm.room_nights_sold == 8           # 2×3 + 1×2
    assert rm.available_room_nights == 300
    assert rm.occupancy_pct == 2.7
    assert rm.adr == 17500.0                  # 140000 / 8
    assert rm.revpar == round(140000 / 300, 2)
    assert rm.by_type["Standard"]["adr"] == 15000.0


def test_room_metrics_without_a_room_count_still_give_adr():
    rooms = [{"room_type": "standard", "quantity": 1, "nights": 2, "total_revenue": 30000}]
    rm = metrics.compute_room_metrics(rooms, total_rooms=0, days=30)
    assert rm.adr == 15000.0
    assert rm.occupancy_pct == 0.0            # undefined without a denominator
    assert rm.revpar == 0.0


# ── Menu engineering (Phase 3) ────────────────────────────────────────

def _stock(name, cost, price, units=0):
    return {"drink": name, "cost_price": cost, "selling_price": price,
            "bar_stock": units, "store_stock": 0, "stock_value": units * cost}


def test_menu_quadrants_split_on_margin_and_popularity():
    stock = [
        _stock("Star", 100, 600),      # high margin, high volume
        _stock("Plow", 100, 150),      # low margin, high volume
        _stock("Puzzle", 100, 700),    # high margin, low volume
        _stock("Dog", 100, 130, 10),   # low margin, low volume
    ]
    sales = [
        {"drink_name": "star", "quantity": 100, "total_revenue": 60000},
        {"drink_name": "plow", "quantity": 100, "total_revenue": 15000},
        {"drink_name": "puzzle", "quantity": 1, "total_revenue": 700},
        {"drink_name": "dog", "quantity": 1, "total_revenue": 130},   # sells a little
    ]
    quadrants = {m.drink: m.quadrant for m in metrics.menu_engineering(sales, stock)}
    assert quadrants == {"Star": "star", "Plow": "plow-horse",
                         "Puzzle": "puzzle", "Dog": "dog"}


def test_zero_sellers_get_their_own_bucket_not_promotion_advice():
    """On unit margin alone a zero-seller can score as a "puzzle" and be told to
    "push it harder" — wrong advice for something nobody bought at all."""
    stock = [_stock("Sold", 100, 500), _stock("Idle", 100, 900, units=12)]
    sales = [{"drink_name": "sold", "quantity": 50, "total_revenue": 25000}]
    items = {m.drink: m for m in metrics.menu_engineering(sales, stock)}
    assert items["Idle"].units == 0
    assert items["Idle"].quadrant == "not-selling"      # high margin, but zero sales
    assert items["Idle"].tied_value == 1200
    assert "Delist" in metrics.QUADRANT_ACTIONS["not-selling"]


def test_splitting_zero_sellers_out_does_not_move_the_other_quadrants():
    """They stay in the averages, so the star/plow/puzzle/dog boundaries hold."""
    stock = [_stock("Star", 100, 600), _stock("Plow", 100, 150),
             _stock("Puzzle", 100, 700), _stock("Dog", 100, 130, 10)]
    sales = [
        {"drink_name": "star", "quantity": 100, "total_revenue": 60000},
        {"drink_name": "plow", "quantity": 100, "total_revenue": 15000},
        {"drink_name": "puzzle", "quantity": 1, "total_revenue": 700},
        {"drink_name": "dog", "quantity": 1, "total_revenue": 130},
    ]
    before = {m.drink: m.quadrant for m in metrics.menu_engineering(sales, stock)}
    with_zero = metrics.menu_engineering(sales, stock + [_stock("Ghost", 100, 5000, 3)])
    after = {m.drink: m.quadrant for m in with_zero}
    assert after["Ghost"] == "not-selling"
    assert {k: v for k, v in after.items() if k != "Ghost"} == before


def test_idle_quadrants_cover_every_bucket_holding_dead_cash():
    assert set(metrics.IDLE_QUADRANTS) == {"dog", "not-selling"}
    for q in metrics.IDLE_QUADRANTS:
        assert q in metrics.QUADRANT_ACTIONS


def test_unpriced_drinks_are_excluded_from_the_menu():
    assert metrics.menu_engineering([], [_stock("NoPrice", 100, 0)]) == []


# ── Stocktake variance (Phase 2a) ─────────────────────────────────────

def test_variance_rolls_up_losses_separately_from_the_net():
    counts = [
        {"drink_name": "beer", "expected": 20, "counted": 17},   # 3 short
        {"drink_name": "beer", "expected": 17, "counted": 17},   # exact
        {"drink_name": "gin",  "expected": 5,  "counted": 6},    # 1 over
    ]
    vs = metrics.summarize_variance(counts, {"beer": 100, "gin": 400})
    assert vs.counts == 3
    assert vs.shrink_units == -3              # losses only …
    assert vs.shrink_value == -300
    assert vs.total_units == -2               # … net nets the overage off
    assert vs.total_value == 100.0            # -300 + 400
    assert vs.by_drink[0]["drink"] == "Beer"  # worst loss first


def test_menu_item_reports_where_the_stock_actually_is():
    """A zero-seller with stock only in the store is a missed transfer, not a
    dead product — it literally cannot be sold from the bar."""
    stranded = {"drink": "Tiger", "cost_price": 775, "selling_price": 1200,
                "bar_stock": 0, "store_stock": 14}
    on_bar = {"drink": "Lager", "cost_price": 500, "selling_price": 800,
              "bar_stock": 9, "store_stock": 0}
    items = {m.drink: m for m in metrics.menu_engineering([], [stranded, on_bar])}

    t = items["Tiger"]
    assert t.quadrant == "not-selling"
    assert (t.bar_units, t.store_units, t.stock_units) == (0, 14, 14)
    assert t.stranded_in_store is True
    assert t.tied_value == 10_850

    # Same zero sales, but it IS on the bar — so the stock isn't the explanation.
    assert items["Lager"].stranded_in_store is False


def test_stranded_needs_stock_in_the_store_not_merely_an_empty_bar():
    empty = {"drink": "Gone", "cost_price": 500, "selling_price": 800,
             "bar_stock": 0, "store_stock": 0}
    m = metrics.menu_engineering([], [empty])[0]
    assert m.stranded_in_store is False      # nothing anywhere — nothing to transfer
    assert m.tied_value == 0


# ── Room yield: per-type RevPAR ───────────────────────────────────────

def _room(rtype, qty, nights, revenue):
    return {"room_type": rtype, "quantity": qty, "nights": nights,
            "total_revenue": revenue, "timestamp": "2026-07-05 12:00:00"}


def test_revpar_by_type_divides_each_type_by_its_own_room_count():
    """The trap ADR alone can't show: the premium type charges the most per
    night and yields the least per room the hotel actually owns."""
    rows = [_room("standard", 1, 20, 300_000),   # 20 nights @ 15k
            _room("executive", 1, 2,  50_000)]   # 2 nights  @ 25k
    rm = metrics.compute_room_metrics(
        rows, total_rooms=7, days=30, rooms_by_type={"standard": 5, "executive": 2},
    )
    std, exe = rm.by_type["Standard"], rm.by_type["Executive"]

    assert exe["adr"] == 25_000 and std["adr"] == 15_000      # Executive looks best …
    assert std["revpar"] == 2000.0     # 300k / (5 × 30)
    assert exe["revpar"] == round(50_000 / 60, 2)   # 833.33 — … and yields a third
    assert exe["revpar"] < std["revpar"]

    assert std["rooms"] == 5 and std["available"] == 150
    assert std["occupancy_pct"] == round(20 / 150 * 100, 1)


def test_type_without_a_room_count_gets_no_revpar_rather_than_the_hotel_total():
    """Borrowing total_rooms would credit every room in the building to one type."""
    rows = [_room("standard", 1, 4, 60_000), _room("short-time", 1, 6, 18_000)]
    rm = metrics.compute_room_metrics(
        rows, total_rooms=8, days=10, rooms_by_type={"standard": 5},
    )
    assert rm.by_type["Standard"]["revpar"] == 1200.0        # 60k / (5 × 10)
    st = rm.by_type["Short-Time"]
    assert st["rooms"] == 0 and st["revpar"] == 0.0 and st["occupancy_pct"] == 0.0
    assert st["adr"] == 3000.0        # ADR needs no denominator, so it still works


def test_room_metrics_without_type_counts_is_unchanged():
    rows = [_room("standard", 2, 3, 90_000)]
    bare = metrics.compute_room_metrics(rows, 8, 29)
    assert bare.by_type["Standard"]["revpar"] == 0.0
    assert bare.revpar == round(90_000 / (8 * 29), 2)   # hotel-wide unaffected


# ── Room yield: period-over-period trend ──────────────────────────────

def _metrics(revenue, nights, rooms=10, days=30):
    return metrics.compute_room_metrics(
        [_room("standard", 1, nights, revenue)] if nights else [], rooms, days,
    )


def test_discounting_to_fill_rooms_is_called_out_not_congratulated():
    """Occupancy up, RevPAR down — the exact self-deception RevPAR exists for."""
    prior = _metrics(200_000, 10)      # ADR 20,000
    now   = _metrics(180_000, 15)      # ADR 12,000 — more nights, less money
    t = metrics.compare_room_metrics(now, prior)

    assert (t.occupancy_dir, t.revpar_dir) == ("up", "down")
    assert "discount cost more" in t.verdict
    assert t.adr_dir == "down"
    assert "RevPAR followed it down" in t.rate_note


def test_filling_up_at_a_flat_revpar_reads_as_underpriced():
    prior = _metrics(200_000, 10)
    now   = _metrics(204_000, 15)      # 50% more nights, RevPAR barely moved
    t = metrics.compare_room_metrics(now, prior)
    assert (t.occupancy_dir, t.revpar_dir) == ("up", "flat")
    assert "underpriced" in t.verdict


def test_rate_rise_that_does_not_reach_revpar_is_flagged():
    """Point of the fuel pass-through check: did the higher rate survive
    contact with bookings, or did the lost nights cancel it out?"""
    prior = _metrics(200_000, 10)      # ADR 20,000
    now   = _metrics(198_000, 6)       # ADR 33,000 — big rate rise, RevPAR flat
    t = metrics.compare_room_metrics(now, prior)

    assert t.adr_dir == "up" and t.revpar_dir == "flat"
    assert "not reaching RevPAR" in t.rate_note

    # …and when it does hold up, it says so instead.
    held = metrics.compare_room_metrics(_metrics(300_000, 12), prior)
    assert held.adr_dir == "up" and held.revpar_dir == "up"
    assert "sticking" in held.rate_note


def test_healthy_growth_and_soft_demand_read_opposite_ways():
    prior = _metrics(200_000, 10)
    healthy = metrics.compare_room_metrics(_metrics(300_000, 14), prior)
    assert (healthy.occupancy_dir, healthy.revpar_dir) == ("up", "up")
    assert "healthy" in healthy.verdict

    soft = metrics.compare_room_metrics(_metrics(120_000, 7), prior)
    assert (soft.occupancy_dir, soft.revpar_dir) == ("down", "down")
    assert "overpriced, or demand" in soft.verdict


def test_small_moves_fall_inside_the_dead_band():
    """One extra booking must not produce a fresh 'raise your prices' verdict."""
    prior = _metrics(200_000, 10)
    now   = _metrics(204_000, 10)      # +2%
    t = metrics.compare_room_metrics(now, prior)
    assert (t.occupancy_dir, t.revpar_dir, t.adr_dir) == ("flat", "flat", "flat")
    assert t.verdict == metrics.TREND_VERDICTS[("flat", "flat")]
    assert t.rate_note == ""


def test_an_empty_prior_period_reports_no_baseline_rather_than_infinite_growth():
    t = metrics.compare_room_metrics(_metrics(200_000, 10), _metrics(0, 0))
    assert t.comparable is False
    assert t.verdict == "" and t.rate_note == ""
    assert t.revpar_delta_pct == 0.0        # no division by zero


def test_occupancy_delta_is_reported_in_points_and_percent():
    prior = _metrics(200_000, 10, rooms=10, days=30)   # 10/300 = 3.3%
    now   = _metrics(300_000, 20, rooms=10, days=30)   # 20/300 = 6.7%
    t = metrics.compare_room_metrics(now, prior)
    assert t.occupancy_delta_pt == round(6.7 - 3.3, 1)     # 3.4 points
    assert t.occupancy_delta_pct == round((6.7 - 3.3) / 3.3 * 100, 1)


def test_a_rate_cut_that_revpar_absorbed_is_not_reported_as_a_fall():
    """Regression: 'rate fell and RevPAR followed it down' fired whenever RevPAR
    merely failed to rise — contradicting the flat verdict printed beside it."""
    prior = _metrics(200_000, 10)      # ADR 20,000
    now   = _metrics(204_000, 20)      # ADR 10,200 — halved rate, RevPAR flat
    t = metrics.compare_room_metrics(now, prior)

    assert t.adr_dir == "down" and t.revpar_dir == "flat"
    assert "followed it down" not in t.rate_note
    assert "held" in t.rate_note


def test_a_rate_rise_that_lost_more_than_it_gained_reads_worse_than_flat():
    prior = _metrics(200_000, 10)      # ADR 20,000
    now   = _metrics(150_000, 5)       # ADR 30,000, RevPAR down 25%
    t = metrics.compare_room_metrics(now, prior)
    assert t.adr_dir == "up" and t.revpar_dir == "down"
    assert "backfired" in t.rate_note


# ── GOPPAR: the bottom-line twin of RevPAR ────────────────────────────

def _pnl(drink_rev=0.0, room_rev=0.0, cogs_units=0, expenses=()):
    sales = [{"drink_name": "beer", "quantity": cogs_units, "total_revenue": drink_rev}]
    rooms = [{"room_type": "standard", "quantity": 1, "nights": 1,
              "total_revenue": room_rev}] if room_rev else []
    return metrics.compute_pnl(sales, rooms, list(expenses), {"beer": 300})


def test_goppar_is_the_pnl_divided_by_available_room_nights():
    """The invariant that stops the two disagreeing on the same screen."""
    pnl = _pnl(drink_rev=100_000, room_rev=200_000, cogs_units=100, expenses=[
        {"account": "bar",   "category": "salary", "amount": 20_000},
        {"account": "rooms", "category": "diesel", "amount": 50_000},
        {"account": "bar",   "category": "restock", "amount": 90_000},   # excluded
    ])
    gp = metrics.compute_goppar(pnl, available_room_nights=240)

    assert gp.gop == pnl.net_profit
    assert gp.rooms_gop == pnl.rooms.profit
    assert gp.bar_gop == pnl.bar.profit
    assert gp.goppar == round(pnl.net_profit / 240, 2)
    assert gp.rooms_goppar == round(pnl.rooms.profit / 240, 2)
    assert gp.revpar == round(200_000 / 240, 2)

    # Restocking is a cash→stock move, never a cost — it must not reach GOPPAR.
    assert pnl.net_profit == 100_000 + 200_000 - 30_000 - 20_000 - 50_000


def test_rooms_conversion_is_the_rooms_net_margin():
    """Both divide by the same denominator, so the ratio must survive it."""
    pnl = _pnl(room_rev=200_000, expenses=[
        {"account": "rooms", "category": "diesel", "amount": 50_000},
    ])
    gp = metrics.compute_goppar(pnl, available_room_nights=240)
    assert gp.rooms_conversion_pct == pnl.rooms.net_margin_pct == 75.0


def test_goppar_survives_a_zero_denominator():
    gp = metrics.compute_goppar(_pnl(room_rev=1000), available_room_nights=0)
    assert gp.goppar == 0.0 and gp.revpar == 0.0 and gp.conversion_pct == 0.0


def test_a_rate_rise_swallowed_by_fuel_shows_revpar_up_and_goppar_flat():
    """The case RevPAR alone cannot see: rooms earned more, the diesel took it."""
    before = metrics.compute_goppar(
        _pnl(room_rev=200_000, expenses=[{"account": "rooms", "category": "diesel", "amount": 60_000}]),
        available_room_nights=240,
    )
    after = metrics.compute_goppar(
        _pnl(room_rev=260_000, expenses=[{"account": "rooms", "category": "diesel", "amount": 122_000}]),
        available_room_nights=240,
    )
    t = metrics.compare_goppar(after, before)

    assert t.revpar_dir == "up"          # RevPAR says the rate rise worked …
    assert t.goppar_dir == "flat"        # … GOPPAR says none of it was kept
    assert "absorbing the whole gain" in t.verdict


def test_revenue_up_while_profit_falls_is_called_out(monkeypatch):
    before = metrics.compute_goppar(
        _pnl(room_rev=200_000, expenses=[{"account": "rooms", "category": "diesel", "amount": 50_000}]),
        available_room_nights=240,
    )
    after = metrics.compute_goppar(
        _pnl(room_rev=240_000, expenses=[{"account": "rooms", "category": "diesel", "amount": 150_000}]),
        available_room_nights=240,
    )
    t = metrics.compare_goppar(after, before)
    assert (t.revpar_dir, t.goppar_dir) == ("up", "down")
    assert "costs are rising faster than rates" in t.verdict


def test_recovering_from_a_loss_reports_a_direction_not_a_percentage():
    """−₦5,000 → +₦5,000 is not '−200% growth'."""
    loss = metrics.compute_goppar(
        _pnl(room_rev=100_000, expenses=[{"account": "rooms", "category": "diesel", "amount": 150_000}]),
        available_room_nights=240,
    )
    profit = metrics.compute_goppar(
        _pnl(room_rev=200_000, expenses=[{"account": "rooms", "category": "diesel", "amount": 50_000}]),
        available_room_nights=240,
    )
    assert loss.goppar < 0
    t = metrics.compare_goppar(profit, loss)
    assert t.goppar_dir == "up"
    assert t.goppar_delta_pct == 0.0        # deliberately not reported


def test_conversion_can_exceed_100_percent_when_the_bar_carries_the_rooms():
    pnl = _pnl(drink_rev=400_000, room_rev=100_000, cogs_units=100, expenses=[
        {"account": "rooms", "category": "diesel", "amount": 40_000},
    ])
    gp = metrics.compute_goppar(pnl, available_room_nights=240)
    assert gp.conversion_pct > 100          # whole-hotel profit exceeds room revenue
    assert gp.rooms_conversion_pct == 60.0  # rooms alone stay under it
