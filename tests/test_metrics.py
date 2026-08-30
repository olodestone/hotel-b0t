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


# ── Day-of-week split: flat rise vs weekday/weekend pricing ───────────

from datetime import date  # noqa: E402

# June 2026 starts on a Monday and 2026-06-28 is a Sunday — exactly four of
# every weekday, so occupancy denominators are equal across the seven and any
# difference the tests find is real rather than a calendar artefact.
_JUNE = (date(2026, 6, 1), date(2026, 6, 28))


def _night(day: int, qty: int, nights: int, revenue: float, rtype: str = "standard"):
    return {"timestamp": f"2026-06-{day:02d} 12:00:00", "room_type": rtype,
            "quantity": qty, "nights": nights, "total_revenue": revenue}


def test_a_stay_is_credited_to_every_night_it_occupies():
    """A Friday check-in for 3 nights is not three Fridays."""
    rows = [_night(5, 2, 3, 2 * 3 * 20_000)]          # Fri 5 Jun → Fri/Sat/Sun
    nights = list(metrics.expand_room_nights(rows))
    assert [n[0].strftime("%a") for n in nights] == ["Fri", "Sat", "Sun"]
    assert all(n[2] == 2 for n in nights)             # 2 rooms on each night

    split = metrics.compute_dow_split(rows, *_JUNE, total_rooms=10)
    sold = [b.nights_sold for b in split.by_dow]
    assert sold == [0, 0, 0, 0, 2, 2, 2]              # Mon..Sun


def test_adr_is_per_room_night_not_per_booking():
    """The revenue apportioned to a night covers every room in that booking.

    Multiplying it by the room count again squares the rooms and reports an ADR
    several times the rate actually charged.
    """
    rows = [_night(1, 4, 1, 4 * 15_000)]              # 4 rooms, one night
    split = metrics.compute_dow_split(rows, *_JUNE, total_rooms=10)
    assert split.by_dow[0].adr == 15_000.0
    assert split.by_dow[0].nights_sold == 4
    assert split.overall.revenue == 60_000.0


def test_each_weekday_divides_by_its_own_available_nights():
    rows = [_night(5, 8, 1, 8 * 15_000)]              # one Friday only
    split = metrics.compute_dow_split(rows, *_JUNE, total_rooms=10)
    friday = split.by_dow[4]
    assert friday.days == 4                           # four Fridays in the window
    assert friday.available == 40                     # not 10 × 28
    assert friday.occupancy_pct == 20.0               # 8 of 40, not 8 of 10


def test_nights_outside_the_window_are_clipped():
    """A stay that began before the window contributes only the nights inside it."""
    rows = [{"timestamp": "2026-05-30 12:00:00", "room_type": "standard",
             "quantity": 1, "nights": 4, "total_revenue": 4 * 10_000}]
    split = metrics.compute_dow_split(rows, *_JUNE, total_rooms=10)
    assert split.overall.nights_sold == 2             # 1 & 2 June; 30 & 31 May drop
    assert split.overall.revenue == 20_000.0


def _weekly(weekend_qty: int, weekday_qty: int, rate: float = 15_000,
            weekend_rate: float | None = None):
    """One booking per night for the whole of June 2026."""
    rows = []
    for d in range(1, 29):
        wknd = date(2026, 6, d).weekday() in metrics.WEEKEND_NIGHTS
        qty = weekend_qty if wknd else weekday_qty
        r = (weekend_rate or rate) if wknd else rate
        if qty:
            rows.append(_night(d, qty, 1, qty * r))
    return rows


def test_busy_weekend_at_the_same_rate_says_split_the_rate():
    split = metrics.compute_dow_split(_weekly(8, 2), *_JUNE, total_rooms=10)
    assert split.occupancy_gap_pt == 60.0
    assert split.adr_gap_pct == 0.0
    assert "Split the rate" in split.verdict


def test_busy_weekend_already_at_a_premium_points_at_the_weekday_nights():
    split = metrics.compute_dow_split(
        _weekly(8, 2, weekend_rate=25_000), *_JUNE, total_rooms=10)
    assert split.adr_gap_pct > metrics.TREND_BAND
    assert "weekday nights are the problem" in split.verdict


def test_level_demand_says_a_flat_rise_is_the_right_shape():
    split = metrics.compute_dow_split(_weekly(5, 5), *_JUNE, total_rooms=10)
    assert split.occupancy_gap_pt == 0.0
    assert "Flat rise" in split.verdict


def test_an_emptier_weekend_rules_out_a_weekend_premium():
    split = metrics.compute_dow_split(_weekly(1, 8), *_JUNE, total_rooms=10)
    assert split.occupancy_gap_pt < 0
    assert "not justified" in split.verdict


def test_without_a_room_count_the_split_says_so_but_still_gives_adr():
    split = metrics.compute_dow_split(_weekly(8, 2), *_JUNE, total_rooms=0)
    assert split.has_rooms is False
    assert split.by_dow[4].adr == 15_000.0            # ADR needs no denominator
    assert split.by_dow[4].occupancy_pct == 0.0
    assert "room count" in split.verdict


def test_tied_nights_are_named_together_never_picked_between():
    """Three nights at identical occupancy have no single 'busiest'."""
    split = metrics.compute_dow_split(_weekly(5, 5), *_JUNE, total_rooms=10)
    assert split.busiest == ""                        # all seven tie → no peak
    rows = _weekly(0, 0) + [_night(5, 4, 1, 60_000), _night(6, 4, 1, 60_000)]
    two = metrics.compute_dow_split(rows, *_JUNE, total_rooms=10)
    assert two.busiest == "Friday & Saturday"


def test_an_empty_window_invents_no_peak():
    split = metrics.compute_dow_split([], *_JUNE, total_rooms=10)
    assert (split.busiest, split.quietest) == ("", "")
    assert split.overall.nights_sold == 0


# ── Turnaways: the demand a full night hides ──────────────────────────

def _turn(day: int, qty: int, rtype: str = "standard", reason: str = "fully booked"):
    return {"timestamp": f"2026-06-{day:02d} 20:00:00", "room_type": rtype,
            "quantity": qty, "reason": reason}


def test_turnaways_land_on_the_night_they_happened():
    rows = [_turn(5, 3), _turn(6, 4), _turn(1, 1)]    # Fri, Sat, Mon
    split = metrics.compute_dow_split(_weekly(8, 2), *_JUNE, total_rooms=10,
                                      turnaway_rows=rows)
    assert split.by_dow[4].turnaways == 3
    assert split.by_dow[5].turnaways == 4
    assert split.weekend.turnaways == 7
    assert split.weekday.turnaways == 1
    assert split.turnaways_tracked is True


def test_no_turnaways_recorded_is_not_the_same_as_none_happening():
    split = metrics.compute_dow_split(_weekly(8, 2), *_JUNE, total_rooms=10)
    assert split.turnaways_tracked is False
    summary = metrics.summarize_turnaways([], *_JUNE)
    assert summary.tracked is False and summary.total == 0


def test_turnaway_summary_groups_and_prices_at_achieved_adr():
    rows = [_turn(5, 3), _turn(6, 2, "deluxe"), _turn(12, 1, "", "")]
    s = metrics.summarize_turnaways(rows, *_JUNE, adr=15_000)
    assert s.total == 6
    assert s.days_with_data == 3
    assert s.by_type == {"Standard": 3, "Deluxe": 2, "Unspecified": 1}
    assert s.by_reason["fully booked"] == 5
    assert s.by_reason["not given"] == 1
    assert s.lost_revenue == 90_000.0                 # 6 × the rate actually achieved


def test_turnaways_outside_the_window_are_ignored():
    s = metrics.summarize_turnaways([_turn(5, 3), {"timestamp": "2026-05-05 20:00:00",
                                                   "quantity": 9}], *_JUNE)
    assert s.total == 3


def test_a_window_too_short_to_compare_refuses_to_issue_a_verdict():
    """A single Friday has no working-week nights to be busier than."""
    friday = date(2026, 6, 5)
    split = metrics.compute_dow_split(
        [_night(5, 8, 1, 8 * 15_000)], friday, friday, total_rooms=10)
    assert "Too short a window" in split.verdict
    assert "Split the rate" not in split.verdict
    assert split.by_dow[4].occupancy_pct == 80.0     # the night itself still reports


def test_a_full_week_is_long_enough_to_compare():
    split = metrics.compute_dow_split(
        _weekly(8, 2), date(2026, 6, 1), date(2026, 6, 7), total_rooms=10)
    assert "Split the rate" in split.verdict


# ── Hourly lets: not every stay is a night ────────────────────────────

def _let(day, qty, units, revenue, rtype="short time", hours=0):
    return {"timestamp": f"2026-06-{day:02d} 14:00:00", "room_type": rtype,
            "quantity": qty, "nights": units, "total_revenue": revenue,
            "duration_hours": hours}


_HOURLY = {"short time": 2}


def test_lets_are_not_counted_as_room_nights():
    """One room let three times in a day used to report 300% occupancy."""
    rows = [_let(5, 1, 3, 9_000)]
    rm = metrics.compute_room_metrics(rows, total_rooms=1, days=1,
                                      hours_by_type=_HOURLY)
    assert rm.room_nights_sold == 0        # no night was sold
    assert rm.short_lets == 3
    assert rm.occupancy_pct == 0.0         # overnight occupancy, honestly zero
    assert rm.utilization_pct == 25.0      # 6 of 24 room-hours


def test_a_lets_rate_never_enters_adr():
    """A ₦3,000 two-hour let averaged with a ₦15,000 night is a rate nobody charged."""
    rows = [_let(5, 1, 3, 9_000), _room_night(5, 3, 1, 45_000)]
    rm = metrics.compute_room_metrics(rows, total_rooms=4, days=1,
                                      hours_by_type=_HOURLY)
    assert rm.adr == 15_000.0              # overnight rate, not the ₦9,000 blend
    assert rm.arl == 3_000.0               # lets get their own average
    assert rm.occupancy_pct == 75.0        # 3 nights of 4 room-days


def _room_night(day, qty, nights, revenue, rtype="standard"):
    return {"timestamp": f"2026-06-{day:02d} 20:00:00", "room_type": rtype,
            "quantity": qty, "nights": nights, "total_revenue": revenue,
            "duration_hours": 0}


def test_revpar_is_unaffected_by_stay_length():
    """The invariant that makes RevPAR the cross-trade comparator.

    Its denominator is room-*days*, so revenue per available room-day is a fair
    question whether the room earned it from one guest or six.
    """
    rows = [_let(5, 1, 3, 9_000), _room_night(5, 3, 1, 45_000)]
    blind = metrics.compute_room_metrics(rows, 4, 1)
    aware = metrics.compute_room_metrics(rows, 4, 1, hours_by_type=_HOURLY)
    assert blind.revpar == aware.revpar == 13_500.0
    assert blind.revenue == aware.revenue
    assert blind.occupancy_pct == 150.0 and aware.occupancy_pct == 75.0  # only this moved


def test_configuring_nothing_leaves_every_figure_where_it_was():
    """A hotel that only sells nights must be untouched by any of this."""
    rows = [_room_night(5, 2, 3, 90_000)]
    before = metrics.compute_room_metrics(rows, 8, 29)
    after = metrics.compute_room_metrics(rows, 8, 29, hours_by_type={})
    assert before == after
    assert after.short_lets == 0 and after.has_short_stay is False


def test_a_negotiated_duration_on_the_row_beats_the_type():
    """Reconfiguring a type later must not rewrite what a past booking was."""
    rows = [_let(5, 1, 1, 5_000, rtype="standard", hours=3)]
    rm = metrics.compute_room_metrics(rows, 1, 1)      # 'standard' is nightly
    assert rm.short_lets == 1                          # the row says otherwise
    assert rm.room_nights_sold == 0
    assert rm.room_hours_sold == 3.0


def test_lets_never_spread_across_calendar_days():
    """Three lets are three lets on one day, not one a day for three days."""
    rows = [_let(5, 1, 3, 9_000)]                      # Friday 5 June
    split = metrics.compute_dow_split(rows, *_JUNE, total_rooms=1,
                                      hours_map=_HOURLY)
    assert [b.lets for b in split.by_dow] == [0, 0, 0, 0, 3, 0, 0]
    assert sum(b.nights_sold for b in split.by_dow) == 0


def test_nights_still_spread_when_the_hotel_also_sells_hours():
    rows = [_room_night(5, 1, 3, 60_000), _let(5, 1, 2, 6_000)]
    split = metrics.compute_dow_split(rows, *_JUNE, total_rooms=4,
                                      hours_map=_HOURLY)
    assert [b.nights_sold for b in split.by_dow] == [0, 0, 0, 0, 1, 1, 1]
    assert [b.lets for b in split.by_dow] == [0, 0, 0, 0, 2, 0, 0]


def _mixed(weekend_lets, weekday_lets, weekend_nights, weekday_nights,
           let_rate=3_000, weekend_let_rate=None):
    rows = []
    for d in range(1, 29):
        wknd = date(2026, 6, d).weekday() in metrics.WEEKEND_NIGHTS
        lets = weekend_lets if wknd else weekday_lets
        rate = (weekend_let_rate or let_rate) if wknd else let_rate
        nights = weekend_nights if wknd else weekday_nights
        if lets:
            rows.append(_let(d, 1, lets, lets * rate))
        if nights:
            rows.append(_room_night(d, nights, 1, nights * 15_000))
    return rows


def test_each_trade_gets_its_own_verdict():
    """Overnight can peak at the weekend while the hourly trade peaks midweek."""
    split = metrics.compute_dow_split(
        _mixed(weekend_lets=2, weekday_lets=6, weekend_nights=5, weekday_nights=2),
        *_JUNE, total_rooms=8, hours_map=_HOURLY)
    assert "Split the rate" in split.verdict            # overnight: raise weekend
    assert "working-week business" in split.short_verdict  # hourly: do not
    assert split.lets_gap_pct < 0


def test_a_busy_weekend_at_a_flat_let_price_says_raise_the_let_price():
    split = metrics.compute_dow_split(
        _mixed(weekend_lets=8, weekday_lets=2, weekend_nights=0, weekday_nights=0),
        *_JUNE, total_rooms=8, hours_map=_HOURLY)
    assert "Raise the weekend let price" in split.short_verdict
    assert split.verdict == ""              # no overnight trade to speak for


def test_a_nightly_only_hotel_gets_no_hourly_verdict():
    split = metrics.compute_dow_split(_weekly(8, 2), *_JUNE, total_rooms=10)
    assert split.short_verdict == ""
    assert "Split the rate" in split.verdict


# ── Expense classification: two axes ──────────────────────────────────

def _exp(account, category, amount, cls=None, review=False):
    row = {"account": account, "category": category, "amount": amount}
    if cls:
        row["expense_class"] = cls
    if review:
        row["needs_review"] = True
    return row


def test_overhead_reaches_the_pnl_instead_of_vanishing():
    """`== "bar"` and `== "rooms"` matched neither, so overhead left the P&L."""
    pnl = metrics.compute_pnl(
        [], [{"room_type": "std", "quantity": 1, "nights": 1, "total_revenue": 100_000}],
        [_exp("overhead", "levies", 30_000)], {})
    assert pnl.overhead_total == 30_000
    assert pnl.total_outgoings == 30_000
    assert pnl.net_profit == 70_000          # not 100_000
    assert pnl.overhead.profit == -30_000    # it earns nothing


def test_overhead_keeps_the_rooms_target_invariants():
    """surplus == net_profit and bar_contribution == bar.profit, documented and pinned."""
    sales = [{"drink_name": "coke", "quantity": 10, "total_revenue": 20_000}]
    rooms = [{"room_type": "std", "quantity": 1, "nights": 1, "total_revenue": 100_000}]
    exp = [_exp("bar", "salary", 10_000), _exp("rooms", "fuel", 20_000),
           _exp("overhead", "levies", 30_000)]
    cost = {"coke": 500}
    pnl = metrics.compute_pnl(sales, rooms, exp, cost)
    tgt = metrics.compute_rooms_target(sales, rooms, exp, cost)
    assert tgt.surplus == pnl.net_profit
    assert tgt.bar_contribution == pnl.bar.profit


def test_capital_is_out_of_the_pnl_but_not_out_of_the_bank():
    exp = [_exp("rooms", "maintenance", 20_000),
           _exp("rooms", "maintenance", 115_000, cls="capital")]
    pnl = metrics.compute_pnl(
        [], [{"room_type": "std", "quantity": 1, "nights": 1, "total_revenue": 200_000}],
        exp, {})
    assert pnl.rooms.expense_total == 20_000     # the cable run is not a room cost
    assert pnl.net_profit == 180_000
    assert pnl.capital_spend == 115_000
    assert metrics.capital_spend(exp) == 115_000


def test_capital_reduces_the_cash_estimate():
    """Excluding it from profit must not exclude it from the bank."""
    exp = [_exp("rooms", "maintenance", 115_000, cls="capital")]
    pos = metrics.compute_cash_position(
        [], [], exp, [], [], stock_value=0, opening=500_000, anchor_dt=None,
        cost_map={}, now=clock_now())
    assert pos.capital_cash == 115_000
    assert pos.cash == 385_000                  # 500,000 − 115,000


def clock_now():
    from datetime import datetime
    return datetime(2026, 6, 29, 14, 30)


def test_a_periodic_payment_is_a_reserve_draw_not_a_cost():
    """The accrual carried the cost; charging the invoice too would double it."""
    exp = [_exp("rooms", "maintenance", 90_000, cls="periodic")]
    assert metrics.operating_expenses(exp) == []      # out of the P&L
    assert metrics.periodic_spend(exp) == 90_000      # still out of the bank


def test_legacy_rows_keep_their_old_behaviour():
    """Restock/supplier were excluded by category before the class axis existed."""
    assert metrics.expense_class({"category": "restock"}) == "inventory"
    assert metrics.expense_class({"category": "supplier"}) == "inventory"
    assert metrics.expense_class({"category": "fuel"}) == "operating"
    assert metrics.operating_expenses([_exp("bar", "restock", 30_000)]) == []


def test_an_unknown_class_falls_back_to_operating():
    """Over-expensing understates profit — the safe way to be wrong."""
    assert metrics.expense_class({"category": "fuel", "expense_class": "nonsense"}) == "operating"


def test_an_unknown_account_falls_back_to_bar_as_the_schema_always_did():
    assert metrics.expense_account({"account": "sideways"}) == "bar"
    assert metrics.expense_account({}) == "bar"
    assert metrics.expense_account({"account": "OVERHEAD"}) == "overhead"


def test_review_rows_catch_unsure_and_misc():
    rows = [_exp("rooms", "fuel", 1000), _exp("rooms", "misc", 4000),
            _exp("bar", "consumables", 500, review=True)]
    flagged = metrics.review_rows(rows)
    assert len(flagged) == 2
    assert all(r["category"] in ("misc", "consumables") for r in flagged)


def test_overhead_salary_is_split_out_like_the_others():
    """split_salary matches the exact string 'salary' — 'salaries' would not."""
    pnl = metrics.compute_pnl([], [], [_exp("overhead", "salary", 25_000)], {})
    assert pnl.overhead.salary == 25_000
    assert pnl.overhead.other_expense == 0


# ── Periodic accrual and the reserve ──────────────────────────────────

def _ob(months=6, amount=90_000, start=date(2026, 1, 1), active=True, retired=None, oid=1):
    return metrics.Obligation(oid, "Soakaway", "rooms", "maintenance",
                              amount, months, start, active, retired)


def test_a_bill_accrues_its_share_and_no_more():
    ob = _ob()
    assert ob.monthly_share == 15_000
    assert metrics.accrued_for(ob, date(2026, 6, 1), date(2026, 6, 30)) == 15_000
    # six months of accrual is exactly the bill — never more, never less
    assert metrics.accrued_for(ob, date(2026, 1, 1), date(2026, 6, 30)) == 90_000


def test_part_months_accrue_in_proportion():
    ob = _ob()
    assert metrics.accrued_for(ob, date(2026, 6, 1), date(2026, 6, 15)) == 7_500
    assert metrics.accrued_for(ob, date(2026, 6, 5), date(2026, 6, 5)) == 500


def test_nothing_accrues_before_the_bill_was_registered():
    """Adding an obligation today must not rewrite last year's profit."""
    ob = _ob(start=date(2026, 6, 1))
    assert metrics.accrued_for(ob, date(2025, 1, 1), date(2025, 12, 31)) == 0.0
    assert metrics.accrued_for(ob, date(2026, 5, 1), date(2026, 6, 30)) == 15_000


def test_retiring_a_bill_stops_it_accruing_without_erasing_its_history():
    """Reading the active flag alone wiped every accrual it had ever made."""
    retired = _ob(active=False, retired=date(2026, 6, 30))
    assert metrics.accrued_for(retired, date(2026, 1, 1), date(2026, 6, 30)) == 90_000
    # and it stops there — the rest of the year adds nothing
    assert metrics.accrued_for(retired, date(2026, 1, 1), date(2026, 12, 31)) == 90_000


def test_a_retired_bill_no_longer_appears_in_new_accruals():
    live = metrics.accrual_rows([_ob()], date(2026, 7, 1), date(2026, 7, 31))
    dead = metrics.accrual_rows([_ob(active=False, retired=date(2026, 6, 30))],
                                date(2026, 7, 1), date(2026, 7, 31))
    assert len(live) == 1 and dead == []


def test_accrual_rows_carry_a_real_account_so_they_flow_through_the_split():
    rows = metrics.accrual_rows([_ob()], date(2026, 6, 1), date(2026, 6, 30))
    pnl = metrics.compute_pnl([], [], rows, {})
    assert pnl.rooms.expense_total == 15_000     # lands on rooms, not nowhere
    assert pnl.net_profit == -15_000
    assert rows[0]["id"] is None and rows[0]["accrual"] is True


def test_paying_the_bill_empties_exactly_that_reserve():
    ob = _ob()
    paid = [{"account": "rooms", "category": "maintenance", "amount": 90_000,
             "expense_class": "periodic", "obligation_id": 1,
             "timestamp": "2026-06-30 00:00:00"}]
    res = metrics.compute_reserve([ob], paid, date(2026, 6, 30))
    assert res.accrued_total == 90_000
    assert res.paid_total == 90_000
    assert res.balance == 0.0
    assert res.underfunded == ()


def test_paying_early_reports_a_material_shortfall():
    ob = _ob()
    paid = [{"account": "rooms", "category": "maintenance", "amount": 90_000,
             "expense_class": "periodic", "obligation_id": 1,
             "timestamp": "2026-03-31 00:00:00"}]
    res = metrics.compute_reserve([ob], paid, date(2026, 3, 31))
    assert res.balance == -45_000                # three months short
    assert [l.obligation.name for l in res.underfunded] == ["Soakaway"]


def test_a_part_month_gap_is_timing_not_a_funding_problem():
    """₦500 short on a ₦90,000 bill is the 29th of a 30-day month, not a warning."""
    ob = _ob()
    paid = [{"account": "rooms", "category": "maintenance", "amount": 90_000,
             "expense_class": "periodic", "obligation_id": 1,
             "timestamp": "2026-06-29 00:00:00"}]
    res = metrics.compute_reserve([ob], paid, date(2026, 6, 29))
    assert res.balance == -500
    assert res.lines[0].funded is False          # it is short
    assert res.lines[0].materially_short is False  # but not by enough to say so
    assert res.underfunded == ()


def test_an_unattributed_payment_still_drains_the_reserve():
    """It is real money out; it just cannot be charged to a particular bill."""
    paid = [{"account": "rooms", "category": "maintenance", "amount": 20_000,
             "expense_class": "periodic", "timestamp": "2026-06-30 00:00:00"}]
    res = metrics.compute_reserve([_ob()], paid, date(2026, 6, 30))
    assert res.unlinked_paid == 20_000
    assert res.paid_total == 20_000
    assert res.balance == 70_000                 # 90,000 accrued − 20,000 drawn


def test_the_cash_position_accrues_the_same_way_the_pnl_does():
    """The same month read -4,500 on /report and +10,000 on /position."""
    from datetime import datetime as _dt
    rooms = [{"timestamp": "2026-06-05 15:00:00", "room_type": "std", "quantity": 2,
              "nights": 3, "total_revenue": 90_000}]
    exp = [{"timestamp": "2026-06-07 12:00:00", "account": "rooms",
            "category": "fuel", "amount": 80_000}]
    ob = _ob()
    pos = metrics.compute_cash_position(
        [], rooms, exp, [], [], stock_value=0, opening=0, anchor_dt=None,
        cost_map={}, now=_dt(2026, 6, 30, 12, 0), obligations=[ob])
    pnl = metrics.compute_pnl(
        [], rooms, exp + metrics.accrual_rows([ob], date(2026, 6, 1), date(2026, 6, 30)), {})
    assert pos.month_profit == pnl.net_profit == -5_000


def test_a_hotel_with_no_register_accrues_nothing():
    assert metrics.accrual_rows([], date(2026, 6, 1), date(2026, 6, 30)) == []
    res = metrics.compute_reserve([], [], date(2026, 6, 30))
    assert res.balance == 0.0 and res.monthly_total == 0.0


# ── Irregular (one-off) costs and the contingency check ───────────────

def test_a_one_off_stays_a_real_cost():
    """Unlike capital, a dead compressor buys nothing — excluding it would lie."""
    exp = [_exp("bar", "maintenance", 420_000, cls="irregular")]
    assert len(metrics.operating_expenses(exp)) == 1
    pnl = metrics.compute_pnl(
        [], [{"room_type": "std", "quantity": 1, "nights": 1, "total_revenue": 1_000_000}],
        exp, {})
    assert pnl.net_profit == 580_000          # the money really was spent
    assert pnl.irregular_spend == 420_000


def test_underlying_profit_reads_the_month_without_the_breakdown():
    exp = [_exp("rooms", "fuel", 300_000),
           _exp("bar", "maintenance", 420_000, cls="irregular")]
    pnl = metrics.compute_pnl(
        [], [{"room_type": "std", "quantity": 1, "nights": 1, "total_revenue": 1_260_000}],
        exp, {})
    assert pnl.net_profit == 540_000          # what it actually made
    assert pnl.underlying_profit == 960_000   # what it would have, if nothing broke
    assert pnl.has_one_offs is True


def test_a_month_with_no_one_offs_has_nothing_to_strip():
    pnl = metrics.compute_pnl([], [], [_exp("rooms", "fuel", 300_000)], {})
    assert pnl.has_one_offs is False
    assert pnl.underlying_profit == pnl.net_profit


def _months(n, events, revenue=1_260_000):
    exp = [{"timestamp": f"2026-{mo:02d}-10 12:00:00", "account": "bar",
            "category": "maintenance", "amount": amt, "expense_class": "irregular"}
           for mo, amt in events]
    rooms = [{"timestamp": f"2026-{mo:02d}-05 12:00:00", "total_revenue": revenue}
             for mo in range(1, n + 1)]
    from datetime import datetime as _dt
    return metrics.compute_contingency(exp, [], rooms, 10, _dt(2026, n, 29))


def test_contingency_sizes_the_buffer_from_what_actually_went_wrong():
    c = _months(8, [(2, 420_000), (5, 90_000), (6, 150_000)])
    assert c.months_observed == 8
    assert c.total_irregular == 660_000
    assert c.monthly_average == 82_500        # 660,000 / 8, no forecast anywhere
    assert c.reliable is True


def test_one_breakdown_in_one_month_is_not_a_monthly_rate():
    """n=1 would recommend tripling the buffer off a single accident."""
    c = _months(1, [(1, 420_000)])
    assert c.months_observed == 1
    assert c.reliable is False


def test_contingency_compares_against_what_the_buffer_sets_aside():
    c = _months(8, [(2, 420_000), (5, 90_000), (6, 150_000)])
    assert c.buffer_monthly == 126_000        # 10% of 1,260,000
    assert c.gap == 82_500 - 126_000
    assert c.covered is True                  # comfortably

    lean = _months(8, [(m, 200_000) for m in range(1, 8)])
    assert lean.covered is False
    assert lean.suggested_pct > lean.buffer_pct


def test_contingency_never_averages_over_months_that_have_no_history():
    """Three months of data averaged over twelve reports a quarter of the rate."""
    c = _months(3, [(1, 300_000)])
    assert c.months_observed == 3             # not 12
    assert c.monthly_average == 100_000


def test_contingency_with_nothing_tagged_says_so():
    c = _months(8, [])
    assert c.has_history is False
    assert c.monthly_average == 0.0


# ── Month-end verification: stocktake ─────────────────────────────────

def _count(drink, expected, counted, cost=300, ts="2026-06-28 08:00:00", loc="bar"):
    return {"drink_name": drink, "expected": expected, "counted": counted,
            "variance": counted - expected, "cost_price": cost,
            "timestamp": ts, "location": loc}


def test_status_bands_read_the_ratio_not_the_units():
    """3 short of 12 is a different event from 3 short of 600."""
    assert metrics.variance_status(-0.5)[0] == "🟢"
    assert metrics.variance_status(-2.0)[0] == "🟡"
    assert metrics.variance_status(-5.0)[0] == "🔴"


def test_a_surplus_is_flagged_never_treated_as_a_clean_count():
    """More bottles than expected is the same leak seen from the other side."""
    mark, note = metrics.variance_status(+10.0)
    assert mark == "🔴"
    assert "unrecorded" in note

    vs = metrics.summarize_variance([_count("coke", 30, 33, 120)], {"coke": 120})
    assert vs.surplus_units == 3
    assert vs.clean is False                  # a surplus is not a clean count
    assert vs.flagged and vs.flagged[0]["drink"] == "Coke"


def test_shrinkage_is_reported_against_the_stock_that_went_out():
    """₦2,000 short means nothing until you know what was sold."""
    vs = metrics.summarize_variance(
        [_count("heineken", 40, 34)], {"heineken": 300}, cogs=60_000)
    assert vs.shrink_value == -1_800
    assert vs.shrink_pct_of_cogs == 3.0


def test_items_are_ordered_by_naira_lost_not_by_units():
    """200 units of the cheapest drink can matter less than 4 of the best."""
    vs = metrics.summarize_variance(
        [_count("cheap", 500, 300, 10), _count("premium", 20, 16, 2_000)],
        {"cheap": 10, "premium": 2_000})
    assert vs.by_drink[0]["drink"] == "Premium"     # -8,000 beats -2,000
    assert vs.by_drink[0]["units"] == -4


def test_bar_and_store_are_measured_against_their_own_expectation():
    rows = [_count("heineken", 12, 10, loc="bar"),
            _count("heineken", 24, 24, loc="store")]
    vs = metrics.summarize_variance(rows, {"heineken": 300})
    assert vs.by_drink[0]["expected"] == 36         # both locations
    assert vs.by_drink[0]["units"] == -2            # only the bar was short


def test_a_month_with_no_count_reads_as_a_gap_not_as_zero_loss():
    from datetime import datetime as _dt
    rows = [_count("heineken", 40, 34, ts="2026-06-28 08:00:00")]
    trend = metrics.variance_trend(rows, {"heineken": 300},
                                   {(2026, 6): 60_000}, _dt(2026, 6, 29))
    labels = [t[0] for t in trend]
    assert labels == ["Apr", "May", "Jun"]
    assert trend[0][1] is None and trend[1][1] is None   # skipped, not clean
    assert trend[2][1] == 3.0


# ── Month-end verification: room audit ────────────────────────────────

def test_vacant_rooms_are_the_point_of_the_exercise():
    rows = [{"timestamp": "2026-06-05 15:00:00", "room_type": "standard",
             "quantity": 3, "nights": 1, "total_revenue": 45_000}]
    d = metrics.build_audit_day(rows, date(2026, 6, 5), rooms_total=8)
    assert d.nights_logged == 3
    assert d.vacant == 5


def test_capture_rate_below_the_floor_blocks_pricing_decisions():
    r = metrics.compute_room_audit(3, 18, 21, 0, 0, adr=15_000, days_in_month=30)
    assert r.capture_pct == 85.7
    assert r.trustworthy is False
    assert r.missing == 3
    assert r.monthly_leak == 450_000        # 3/3 × 30 × 15,000


def test_full_capture_is_trustworthy():
    r = metrics.compute_room_audit(3, 21, 21, 0, 0, adr=15_000, days_in_month=30)
    assert r.capture_pct == 100.0 and r.trustworthy is True
    assert r.monthly_leak == 0.0


def test_audit_days_are_drawn_from_the_whole_month_not_only_booked_days():
    """A day with no entries is exactly the day worth auditing."""
    import random
    days = metrics.audit_days([], 2026, 4, 3, random.Random(1))
    assert len(days) == 3
    assert all(d.month == 4 for d in days)
    assert len(set(days)) == 3              # no repeats


def test_one_flat_rate_across_a_month_is_flagged():
    """Real trade produces walk-ins, regulars and the odd favour."""
    flat = [{"room_type": "standard", "quantity": 2, "nights": 1,
             "total_revenue": 30_000, "timestamp": f"2026-06-{d:02d} 15:00:00"}
            for d in range(1, 29)]
    s = metrics.rate_spread(flat)[0]
    assert s.distinct == 1 and s.nights >= 30
    assert s.suspicious is True


def test_a_normal_spread_of_rates_is_not_flagged():
    mixed = [{"room_type": "standard", "quantity": 1, "nights": 1,
              "total_revenue": r, "timestamp": f"2026-06-{d:02d} 15:00:00"}
             for d, r in enumerate((15000, 12000, 15000, 14000, 15000), start=1)]
    s = metrics.rate_spread(mixed)[0]
    assert s.distinct == 3
    assert s.mode_rate == 15_000
    assert s.suspicious is False


def test_a_flat_rate_over_a_handful_of_nights_is_not_yet_a_finding():
    few = [{"room_type": "suite", "quantity": 1, "nights": 1, "total_revenue": 25_000,
            "timestamp": f"2026-06-{d:02d} 15:00:00"} for d in range(1, 5)]
    assert metrics.rate_spread(few)[0].suspicious is False


# ── COGS is settled at the time of sale ───────────────────────────────

def test_a_closed_month_does_not_move_when_cost_prices_rise():
    """The same May sales reported ₦60,000 profit in May and ₦30,000 in July."""
    may = [{"timestamp": "2026-05-10 20:00:00", "drink_name": "beer",
            "quantity": 100, "total_revenue": 100_000, "cost_price": 400}]
    figures = {metrics.compute_pnl(may, [], [], {"beer": c}).net_profit
               for c in (400, 550, 700)}
    assert figures == {60_000}          # one answer, whenever it is run


def test_rows_written_before_the_stamp_fall_back_to_current_cost():
    legacy = [{"timestamp": "2026-05-10 20:00:00", "drink_name": "beer",
               "quantity": 100, "total_revenue": 100_000}]
    assert metrics.cost_of_drinks_sold(legacy, {"beer": 550}) == 55_000


def test_a_zero_stamp_is_treated_as_missing_not_as_free_stock():
    row = [{"drink_name": "beer", "quantity": 10, "cost_price": 0}]
    assert metrics.cost_of_drinks_sold(row, {"beer": 500}) == 5_000


def test_the_stamp_wins_over_the_current_price():
    row = [{"drink_name": "beer", "quantity": 10, "cost_price": 300}]
    assert metrics.cost_of_drinks_sold(row, {"beer": 900}) == 3_000


# ── Credit sales with no sale behind them ─────────────────────────────

_TAB = [{"id": 9, "timestamp": "2026-06-10 20:00:00", "account": "bar",
         "name": "john", "amount": 10_000, "amount_paid": 0,
         "status": "outstanding"}]
_SALE = [{"timestamp": "2026-06-10 20:00:00", "drink_name": "beer",
          "quantity": 10, "total_revenue": 10_000, "cost_price": 500}]


def _pos(sales, debtors):
    from datetime import datetime as _dt
    return metrics.compute_cash_position(
        sales, [], [], [], debtors, stock_value=0, opening=0, anchor_dt=None,
        cost_map={"beer": 500}, now=_dt(2026, 6, 30, 12, 0))


def test_cash_can_never_go_negative_because_someone_drank_on_credit():
    """A debt with no sale used to subtract revenue that was never added."""
    assert _pos([], _TAB).cash == 0.0
    assert _pos([], _TAB).unmatched_receivables == 10_000


def test_the_intended_pairing_reports_nothing_amiss():
    pos = _pos(_SALE, _TAB)
    assert pos.cash == 0.0                  # sold on credit: nothing collected
    assert pos.unmatched_receivables == 0.0
    assert metrics.unmatched_debts(_TAB, _SALE, []).any is False


def test_an_unmatched_debt_is_named_so_it_can_be_fixed():
    um = metrics.unmatched_debts(_TAB, [], [])
    assert um.any is True and um.total == 10_000
    assert um.rows[0]["id"] == 9            # the row itself, not just a count


def test_detection_does_not_fire_on_a_day_that_took_money():
    """Conservative on purpose — a control that cries wolf gets ignored."""
    partial = [{"timestamp": "2026-06-10 20:00:00", "drink_name": "beer",
                "quantity": 2, "total_revenue": 2_000, "cost_price": 500}]
    assert metrics.unmatched_debts(_TAB, partial, []).any is False


def test_a_room_debt_is_checked_against_room_revenue_not_bar():
    room_debt = [{"id": 1, "timestamp": "2026-06-10 15:00:00", "account": "rooms",
                  "name": "acme", "amount": 30_000, "amount_paid": 0,
                  "status": "outstanding"}]
    assert metrics.unmatched_debts(room_debt, _SALE, []).any is True   # bar sales don't count
    rooms = [{"timestamp": "2026-06-10 15:00:00", "room_type": "std",
              "quantity": 1, "nights": 1, "total_revenue": 30_000}]
    assert metrics.unmatched_debts(room_debt, [], rooms).any is False
