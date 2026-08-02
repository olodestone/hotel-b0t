"""
Golden-master (characterization) tests.

These snapshot the EXACT Telegram-formatted output of every report that the
metrics refactor touches. The snapshots are captured from the original
``reports.py`` *before* the refactor (run once with ``REGEN=1``); afterwards the
refactored code must reproduce them byte-for-byte. This is the guarantee that
the Telegram bot's behaviour is unchanged.

Reports excluded: generate_debtors_report (uses db.get_debtors, not the moved
helpers, so it is unaffected by the metrics extraction).
"""
from datetime import date

import reports


def test_full_report_admin_all_time(snapshot):
    snapshot("full_report_admin_all", reports.generate_full_report(all_time=True))


def test_full_report_current_month(snapshot):
    snapshot("full_report_month", reports.generate_full_report())


def test_full_report_for_date(snapshot):
    snapshot("full_report_date", reports.generate_full_report(for_date=date(2026, 6, 5)))


def test_full_report_staff_view(snapshot):
    snapshot("full_report_staff", reports.generate_full_report(all_time=True, staff_view=True))


def test_sales_report_all_time(snapshot):
    snapshot("sales_report_all", reports.generate_sales_report(all_time=True))


def test_expense_report_all_time(snapshot):
    snapshot("expense_report_all", reports.generate_expense_report(all_time=True))


def test_staff_report_current_month(snapshot):
    snapshot("staff_report_month", reports.generate_staff_report())


def test_allocation_report_all_time(snapshot):
    snapshot("allocation_report_all", reports.generate_allocation_report(all_time=True))


def test_position_report(snapshot):
    snapshot("position_report", reports.generate_position_report())


def test_draws_report_all_time(snapshot):
    snapshot("draws_report_all", reports.generate_draws_report(all_time=True))


def test_daily_summary(snapshot):
    snapshot("daily_summary", reports.generate_daily_summary(date(2026, 6, 5)))


def test_stock_report(snapshot):
    snapshot("stock_report", reports.generate_stock_report())


# ── Performance & working-capital reports ─────────────────────────────

def test_cashcycle_report(snapshot):
    snapshot("cashcycle_report", reports.generate_cashcycle_report())


def test_menu_report(snapshot):
    snapshot("menu_report", reports.generate_menu_report())


def test_room_stats_report_current_month(snapshot):
    snapshot("room_stats_month", reports.generate_room_stats_report())


def test_variance_report_current_month(snapshot):
    snapshot("variance_report_month", reports.generate_variance_report())


def test_payables_report(snapshot):
    snapshot("payables_report", reports.generate_payables_report())


def test_cashcycle_break_even_falls_back_to_the_last_trading_month(monkeypatch):
    """On the first days of a new month there is nothing to measure yet.

    Reporting "₦0 needed, ✅ covered" there would read as reassurance when
    nothing has happened, so both blocks fall back to the last month that traded.
    """
    from datetime import datetime
    import metrics as _metrics

    class _JulyFirst(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 7, 1, 9, 0, 0)

    monkeypatch.setattr(reports, "datetime", _JulyFirst)
    monkeypatch.setattr(_metrics, "datetime", _JulyFirst)
    out = reports.generate_cashcycle_report()

    assert "no July entries yet" in out          # fallback is labelled, not silent
    assert "June 2026" in out
    assert "Bar sales needed" in out             # real figures, not a row of zeros
    assert "✅ Covered — ₦0 clear" not in out


# ── /roomstats windows: the two periods must be like-for-like ─────────

def test_month_to_date_is_compared_against_the_same_days_last_month():
    """Two elapsed days of a new month against a whole finished month would
    make every month start look like a collapse."""
    (start, end, label), prior = reports._room_windows(
        None, (2026, 6), False, None, []
    )
    assert (start, end) == (date(2026, 6, 1), date(2026, 6, 29))   # today is 29 Jun
    assert "current month" in label

    p_start, p_end, p_label = prior
    assert (p_start, p_end) == (date(2026, 5, 1), date(2026, 5, 29))
    assert (end - start) == (p_end - p_start)                      # same elapsed span
    assert p_label == "1–29 May 2026"


def test_a_finished_month_compares_against_an_equal_length_slice():
    (start, end, _), (p_start, p_end, _) = reports._room_windows(
        None, (2026, 4), False, None, []
    )
    assert (start, end) == (date(2026, 4, 1), date(2026, 4, 30))
    assert (end - start) == (p_end - p_start)
    assert p_start == date(2026, 3, 1)


def test_january_compares_against_december_of_the_previous_year():
    _, (p_start, _, _) = reports._room_windows(None, (2026, 1), False, None, [])
    assert p_start == date(2025, 12, 1)


def test_week_windows_are_monday_to_sunday_and_shift_by_exactly_seven_days():
    (start, end, label), (p_start, p_end, _) = reports._room_windows(
        None, None, False, date(2026, 6, 29), []      # Mon 29 Jun, and "today"
    )
    assert start == date(2026, 6, 29) and start.weekday() == 0
    assert end == date(2026, 6, 29)                   # truncated at today
    assert label == "This week"
    assert (p_start, p_end) == (date(2026, 6, 22), date(2026, 6, 22))
    assert (end - start) == (p_end - p_start)


def test_a_single_day_compares_against_the_day_before():
    (start, end, _), (p_start, p_end, _) = reports._room_windows(
        date(2026, 6, 5), None, False, None, []
    )
    assert start == end == date(2026, 6, 5)
    assert p_start == p_end == date(2026, 6, 4)


def test_all_time_has_no_predecessor_to_compare_against():
    (start, end, label), prior = reports._room_windows(
        None, None, True, None,
        [{"timestamp": "2026-05-20 11:00:00"}, {"timestamp": "2026-06-05 15:00:00"}],
    )
    assert (start, end) == (date(2026, 5, 20), date(2026, 6, 29))
    assert label == "ALL-TIME"
    assert prior is None


def test_roomstats_all_time_renders_without_a_comparison():
    out = reports.generate_room_stats_report(all_time=True)
    assert "Period: ALL-TIME" in out
    assert " vs " not in out
    assert "was ₦" not in out


def test_roomstats_says_so_when_there_is_no_baseline(monkeypatch):
    """May has bookings; April does not. Reporting '▲ ∞%' would be worse than
    admitting there is nothing to compare against."""
    out = reports.generate_room_stats_report(for_month=(2026, 5))
    assert "no baseline to compare against yet" in out
    assert "▲" not in out


def test_roomstats_flags_the_high_rate_low_yield_room_type(monkeypatch):
    """Deluxe charges the most per night (ADR ₦25,000 vs ₦15,000) and, with ten
    rooms to fill against Standard's two, earns the least per room owned.
    Invisible on ADR, obvious on RevPAR."""
    monkeypatch.setattr(reports.db, "get_all_room_type_counts",
                        lambda: {"standard": 2, "deluxe": 10})
    out = reports.generate_room_stats_report()

    assert "Deluxe charges the most per night (₦25,000)" in out
    assert "Standard, at ₦15,000 a night," in out
    assert "from the same space" in out


def test_yield_gap_stays_quiet_when_the_top_rate_also_yields_best(monkeypatch):
    """No warning when there's no trap — Deluxe out-earns Standard per room here."""
    monkeypatch.setattr(reports.db, "get_all_room_type_counts",
                        lambda: {"standard": 6, "deluxe": 2})
    out = reports.generate_room_stats_report()
    assert "charges the most per night" not in out


def test_roomstats_asks_for_a_count_only_for_the_types_missing_one(monkeypatch):
    monkeypatch.setattr(reports.db, "get_all_room_type_counts", lambda: {"standard": 6})
    out = reports.generate_room_stats_report()
    assert "RevPAR _n/a_" in out
    assert "/setrooms deluxe <n>" in out
    assert "• *Standard* — ADR ₦15,000 · RevPAR" in out       # the priced one is fine


def test_roomstats_falls_back_to_per_type_counts_when_no_total_is_set(monkeypatch):
    monkeypatch.setattr(reports.db, "get_setting",
                        lambda k, d="": "" if k == "total_rooms" else d)
    monkeypatch.setattr(reports.db, "get_all_room_type_counts",
                        lambda: {"standard": 6, "deluxe": 2})
    out = reports.generate_room_stats_report()
    assert "Basis: 8 rooms" in out
    assert "from your per-type counts" in out
    assert "Occupancy and RevPAR need your room count" not in out


def test_yield_gap_fires_when_a_cheaper_type_out_earns_the_premium_one(monkeypatch):
    """Regression: the flag only fired when the premium type yielded *least* of
    all, so a mid-table Executive beaten by Standard stayed invisible — which is
    exactly the case the by-type split exists to surface."""
    monkeypatch.setattr(reports.db, "get_all_room_type_counts",
                        lambda: {"standard": 2, "deluxe": 4, "annex": 1})
    monkeypatch.setattr(reports.db, "read_all", lambda t: [
        {"id": 1, "timestamp": "2026-06-05 15:00:00", "room_type": "standard",
         "quantity": 2, "nights": 3, "price_per_night": 15000,
         "total_revenue": 90000, "deleted_at": None},
        {"id": 2, "timestamp": "2026-06-22 16:00:00", "room_type": "deluxe",
         "quantity": 1, "nights": 2, "price_per_night": 25000,
         "total_revenue": 50000, "deleted_at": None},
        {"id": 3, "timestamp": "2026-06-23 16:00:00", "room_type": "annex",
         "quantity": 1, "nights": 1, "price_per_night": 4000,
         "total_revenue": 4000, "deleted_at": None},
    ] if t == "rooms" else [])
    out = reports.generate_room_stats_report()

    # Deluxe: top ADR (₦25,000), but 4 rooms → RevPAR ₦431 vs Standard's ₦1,552.
    # Annex yields least of all and is correctly NOT the one flagged.
    assert "Deluxe charges the most per night (₦25,000)" in out
    assert "Standard, at ₦15,000 a night," in out
    assert "Annex charges the most" not in out


def test_a_single_room_type_is_not_pluralised(monkeypatch):
    monkeypatch.setattr(reports.db, "get_all_room_type_counts",
                        lambda: {"standard": 6, "deluxe": 1})
    out = reports.generate_room_stats_report()
    assert "_1 room · " in out
    assert "1 rooms" not in out


# ── GOPPAR block ──────────────────────────────────────────────────────

def test_roomstats_goppar_matches_the_full_report_profit():
    """The two reports must never disagree about the same month's profit."""
    import metrics as _metrics
    out = reports.generate_room_stats_report()

    rooms = _metrics.filter_by_range(_metrics.active(reports.db.read_all("rooms")),
                                     date(2026, 6, 1), date(2026, 6, 29))
    sales = _metrics.filter_by_range(_metrics.active(reports.db.read_all("sales")),
                                     date(2026, 6, 1), date(2026, 6, 29))
    exp = _metrics.filter_by_range(_metrics.active(reports.db.read_all("expenses")),
                                   date(2026, 6, 1), date(2026, 6, 29))
    pnl = _metrics.compute_pnl(sales, rooms, exp, reports._cost_price_map())
    expected = round(pnl.net_profit / (8 * 29), 2)

    assert "*PROFIT PER AVAILABLE ROOM*" in out
    assert f"GOPPAR: ₦{expected:,.0f}" in out


def test_roomstats_goppar_is_hidden_without_a_room_count(monkeypatch):
    """No denominator, no per-available-room anything."""
    monkeypatch.setattr(reports.db, "get_setting",
                        lambda k, d="": "" if k == "total_rooms" else d)
    monkeypatch.setattr(reports.db, "get_all_room_type_counts", lambda: {})
    out = reports.generate_room_stats_report()
    assert "PROFIT PER AVAILABLE ROOM" not in out
    assert "Occupancy and RevPAR need your room count" in out


def test_roomstats_warns_when_rising_revenue_is_eaten_by_costs(monkeypatch):
    """Rooms took more this month than last, and kept less of it — the exact
    blind spot in reading RevPAR on its own."""
    monkeypatch.setattr(reports.db, "read_all", lambda t: {
        "rooms": [
            {"id": 1, "timestamp": "2026-05-10 12:00:00", "room_type": "standard",
             "quantity": 1, "nights": 8, "price_per_night": 15000,
             "total_revenue": 120000, "deleted_at": None},
            {"id": 2, "timestamp": "2026-06-10 12:00:00", "room_type": "standard",
             "quantity": 1, "nights": 8, "price_per_night": 22500,
             "total_revenue": 180000, "deleted_at": None},
        ],
        "expenses": [
            {"id": 1, "timestamp": "2026-05-12 09:00:00", "account": "rooms",
             "category": "diesel", "amount": 30000, "description": "", "deleted_at": None},
            {"id": 2, "timestamp": "2026-06-12 09:00:00", "account": "rooms",
             "category": "diesel", "amount": 140000, "description": "", "deleted_at": None},
        ],
    }.get(t, []))
    out = reports.generate_room_stats_report()

    assert "💰 *RevPAR: ₦776*  ▲ 50%" in out          # top line looks great …
    assert "⚠️ Revenue up while profit fell" in out    # … bottom line does not
    assert "costs are rising faster than rates" in out


def test_roomstats_flags_a_negative_goppar_with_no_baseline(monkeypatch):
    monkeypatch.setattr(reports.db, "read_all", lambda t: {
        "rooms": [
            {"id": 1, "timestamp": "2026-06-10 12:00:00", "room_type": "standard",
             "quantity": 1, "nights": 4, "price_per_night": 15000,
             "total_revenue": 60000, "deleted_at": None},
        ],
        "expenses": [
            {"id": 1, "timestamp": "2026-06-12 09:00:00", "account": "rooms",
             "category": "diesel", "amount": 200000, "description": "", "deleted_at": None},
        ],
    }.get(t, []))
    out = reports.generate_room_stats_report()
    assert "GOPPAR: ₦-603" in out
    assert "spent more than it earned" in out
