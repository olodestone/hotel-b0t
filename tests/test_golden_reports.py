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
    import clock

    july_first = datetime(2026, 7, 1, 9, 0, 0)
    # clock is the single wall clock now — patching reports.datetime would leave
    # the report reading the real one.
    monkeypatch.setattr(clock, "now", lambda: july_first)
    monkeypatch.setattr(clock, "today", lambda: july_first.date())
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
    assert "Basis: 8 overnight rooms" in out
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


# ── Night-by-night split ──────────────────────────────────────────────

def test_dow_split_report_current_month(snapshot):
    snapshot("dow_split_month", reports.generate_dow_split_report())


def test_dow_split_credits_a_stay_to_every_night_it_covers():
    """The fixture's only multi-night stay checks in on a Friday for 3 nights."""
    out = reports.generate_dow_split_report()
    # Fri, Sat and Sun each carry it — a Friday check-in is not three Fridays.
    for night in ("Fri", "Sat", "Sun"):
        assert f"\n{night:<6} " in out
    assert "Busiest: *Friday & Saturday & Sunday*" in out


def test_dow_split_says_when_no_turnaways_were_ever_recorded(monkeypatch):
    """A zero and an unmeasured zero are opposite findings, not the same one."""
    real = reports.db.read_all
    monkeypatch.setattr(reports.db, "read_all",
                        lambda t: [] if t == "turnaways" else real(t))
    out = reports.generate_dow_split_report()
    assert "Nothing recorded for this period" in out
    assert "/turnaway" in out
    assert "turned away*" not in out          # never prints a count it does not have


def test_dow_split_flags_weekend_refusals_on_nights_already_full(monkeypatch):
    """Refusals on sold-out weekend nights are the case for a weekend premium."""
    rooms = [{"id": 1, "timestamp": f"2026-06-{d:02d} 15:00:00", "room_type": "standard",
              "quantity": 8, "nights": 1, "price_per_night": 15000,
              "total_revenue": 120000, "deleted_at": None}
             for d in (5, 6, 12, 13, 19, 20, 26, 27)]        # every Fri & Sat
    turnaways = [{"id": 1, "timestamp": "2026-06-05 20:00:00", "room_type": "standard",
                  "quantity": 6, "reason": "fully booked"},
                 {"id": 2, "timestamp": "2026-06-06 20:00:00", "room_type": "standard",
                  "quantity": 5, "reason": "fully booked"}]
    monkeypatch.setattr(reports.db, "read_all",
                        lambda t: {"rooms": rooms, "turnaways": turnaways}.get(t, []))
    out = reports.generate_dow_split_report()
    assert "Split the rate" in out
    assert "11 guests turned away" in out
    assert "unmet demand at the weekend" in out


# ── Hotels that also sell rooms by the hour ───────────────────────────

def _hourly_hotel(monkeypatch, weekend_lets=5, weekday_lets=3):
    """June 2026: a 2-room hourly trade alongside 6 overnight rooms."""
    rooms = []
    for d in range(1, 29):
        wknd = date(2026, 6, d).weekday() in (4, 5)
        lets = weekend_lets if wknd else weekday_lets
        rooms.append({"id": 100 + d, "timestamp": f"2026-06-{d:02d} 14:00:00",
                      "room_type": "short time", "quantity": 1, "nights": lets,
                      "price_per_night": 3000, "total_revenue": lets * 3000,
                      "deleted_at": None, "duration_hours": 0})
        n = 5 if wknd else 3
        rooms.append({"id": 200 + d, "timestamp": f"2026-06-{d:02d} 20:00:00",
                      "room_type": "standard", "quantity": n, "nights": 1,
                      "price_per_night": 15000, "total_revenue": n * 15000,
                      "deleted_at": None, "duration_hours": 0})
    monkeypatch.setattr(reports.db, "read_all", lambda t: {"rooms": rooms}.get(t, []))
    monkeypatch.setattr(reports.db, "get_all_room_type_hours", lambda: {"short time": 2})
    monkeypatch.setattr(reports.db, "get_all_room_type_counts",
                        lambda: {"short time": 2, "standard": 6})


def test_roomstats_reports_the_two_trades_apart(monkeypatch):
    _hourly_hotel(monkeypatch)
    out = reports.generate_room_stats_report()
    assert "📈 *ADR:  ₦15,000*" in out              # the overnight rate, unblended
    assert "avg ₦3,000 per let" in out
    # 100 nights over 6 overnight rooms × 29 days — the 2 short-time rooms are
    # not overnight capacity and no longer sit in the denominator.
    assert "Occupancy: 57.5%" in out                # never above 100%, never deflated
    assert "6 overnight rooms × 29 days" in out
    assert "2 hourly-only rooms excluded" in out
    assert "Room-time used: 46.7%" in out           # utilization still spans all 8
    assert "• *Short Time* — per let ₦3,000" in out
    assert "of its hours" in out                    # not "% full" for an hourly type


def test_dow_split_gives_each_trade_its_own_verdict(monkeypatch):
    """The two can disagree, and a single blended verdict would hide it."""
    _hourly_hotel(monkeypatch, weekend_lets=2, weekday_lets=6)
    out = reports.generate_dow_split_report()
    assert "🛏 *Overnight:* Split the rate" in out
    assert "🕐 *Hourly:* The hourly trade is a working-week business." in out
    assert "Night  Use    Nights Lets" in out       # lets get their own column


def test_dow_split_keeps_the_plain_table_for_a_nightly_only_hotel(monkeypatch):
    _hourly_hotel(monkeypatch)
    monkeypatch.setattr(reports.db, "get_all_room_type_hours", lambda: {})
    out = reports.generate_dow_split_report()
    assert "Night  Occ    ADR      RevPAR   Away" in out
    assert "Lets" not in out
    assert "Hourly:" not in out


# ── Expense classification: the three report sections ─────────────────

def _classified(monkeypatch):
    rows = [
        {"id": 1, "timestamp": "2026-06-03 09:00:00", "account": "bar", "category": "salary",
         "amount": 50000, "description": "bar wages", "deleted_at": None},
        {"id": 3, "timestamp": "2026-06-07 12:00:00", "account": "rooms", "category": "fuel",
         "amount": 80000, "description": "diesel", "deleted_at": None},
        {"id": 5, "timestamp": "2026-06-09 12:00:00", "account": "overhead", "category": "levies",
         "amount": 15000, "description": "area levy", "deleted_at": None},
        {"id": 7, "timestamp": "2026-06-11 12:00:00", "account": "rooms", "category": "maintenance",
         "amount": 115000, "description": "new cable run", "expense_class": "capital",
         "deleted_at": None},
        {"id": 8, "timestamp": "2026-06-12 12:00:00", "account": "rooms", "category": "maintenance",
         "amount": 90000, "description": "soakaway", "expense_class": "periodic",
         "deleted_at": None},
        {"id": 10, "timestamp": "2026-06-14 12:00:00", "account": "rooms", "category": "misc",
         "amount": 4000, "description": "unlabelled", "deleted_at": None},
    ]
    monkeypatch.setattr(reports.db, "read_all",
                        lambda t: [dict(r) for r in rows] if t == "expenses" else [])
    return rows


def test_expense_report_splits_operating_capital_and_periodic(monkeypatch):
    _classified(monkeypatch)
    out = reports.generate_expense_report(for_month=(2026, 6))
    assert "*① OPERATING — in the P&L*" in out
    assert "*② CAPITAL SPEND — cash out, not a cost*" in out
    assert "*③ RESERVE — periodic bills*" in out
    assert "🏢 *OVERHEAD*" in out
    # 50,000 bar + (80,000 fuel + 4,000 misc) + 15,000 overhead.
    # Neither the 115,000 cable (capital) nor the 90,000 soakaway (periodic,
    # a reserve draw) belongs in an operating total.
    assert "*Operating total: ₦149,000*" in out
    assert "*Capital total: ₦115,000*" in out
    assert "1 entry flagged for review" in out


def test_capital_is_named_in_full_not_lumped(monkeypatch):
    """The repair-vs-replace question is per item; it cannot be asked of a total."""
    _classified(monkeypatch)
    out = reports.generate_expense_report(for_month=(2026, 6))
    assert "`[7]` 2026-06-11  ₦115,000 — Rooms/Maintenance _new cable run_" in out


def test_review_report_lists_large_entries_and_flagged_ones(monkeypatch):
    _classified(monkeypatch)
    monkeypatch.setattr(reports.db, "get_setting", lambda k, d="": d)
    out = reports.generate_review_report(for_month=(2026, 6))
    assert "do you now own something you did not own before" in out.lower()
    assert "`[3]`" in out                         # diesel, at or above ₦50,000
    assert "`[10]`" in out                        # Misc, flagged
    assert "`[7]`" not in out                     # capital: already out of the P&L
    assert "`[8]`" not in out                     # periodic: a reserve draw, not a cost
    assert "`[1]`" in out                         # bar salary is 50,000 exactly
    assert "entrys" not in out                    # the plural rule


def test_review_report_says_so_when_there_is_nothing_to_check(monkeypatch):
    monkeypatch.setattr(reports.db, "read_all", lambda t: [])
    monkeypatch.setattr(reports.db, "get_setting", lambda k, d="": d)
    out = reports.generate_review_report(for_month=(2026, 6))
    assert "Nothing to review" in out


def test_position_shows_capital_leaving_the_bank(monkeypatch):
    real = reports.db.read_all
    monkeypatch.setattr(reports.db, "read_all", lambda t: (
        [{"id": 7, "timestamp": "2026-06-11 12:00:00", "account": "rooms",
          "category": "maintenance", "amount": 115000, "description": "cable",
          "expense_class": "capital", "deleted_at": None}]
        if t == "expenses" else real(t)))
    out = reports.generate_position_report()
    assert "− Asset purchases:" in out
    assert "₦115,000" in out


# ── One-off costs ─────────────────────────────────────────────────────

def test_report_shows_actual_and_underlying_when_something_broke(monkeypatch):
    rooms = [{"id": i, "timestamp": f"2026-06-{d:02d} 15:00:00", "room_type": "standard",
              "quantity": 3, "price_per_night": 15000, "nights": 1,
              "total_revenue": 45000, "deleted_at": None}
             for i, d in enumerate(range(1, 29))]
    exp = [{"id": 2, "timestamp": "2026-06-07 12:00:00", "account": "rooms",
            "category": "fuel", "amount": 300000, "description": "diesel", "deleted_at": None},
           {"id": 3, "timestamp": "2026-06-14 12:00:00", "account": "bar",
            "category": "maintenance", "amount": 420000, "description": "compressor",
            "expense_class": "irregular", "deleted_at": None}]
    monkeypatch.setattr(reports.db, "read_all", lambda t: {
        "expenses": [dict(r) for r in exp], "rooms": [dict(r) for r in rooms]}.get(t, []))
    out = reports.generate_full_report(for_month=(2026, 6))
    assert "🌩 One-off costs:  ₦420,000" in out
    assert "*Underlying:      ₦960,000*" in out
    assert "*Net Profit:    ₦540,000*" in out      # actual is still the headline


_ALLOC_ROOMS = [{"id": 1, "timestamp": "2026-06-05 15:00:00", "room_type": "standard",
                 "quantity": 3, "price_per_night": 15000, "nights": 1,
                 "total_revenue": 450000, "deleted_at": None}]


def test_allocation_will_not_size_a_buffer_off_one_accident(monkeypatch):
    exp = [{"id": 3, "timestamp": "2026-06-14 12:00:00", "account": "bar",
            "category": "maintenance", "amount": 420000, "description": "compressor",
            "expense_class": "irregular", "deleted_at": None}]
    monkeypatch.setattr(reports.db, "read_all", lambda t: {
        "expenses": [dict(r) for r in exp],
        "rooms": [dict(r) for r in _ALLOC_ROOMS]}.get(t, []))
    out = reports.generate_allocation_report(for_month=(2026, 6))
    assert "Too little history to size a buffer from" in out
    assert "setallocation buffer" not in out       # no advice off n=1


def test_allocation_says_when_nothing_has_been_tagged(monkeypatch):
    monkeypatch.setattr(reports.db, "read_all",
                        lambda t: [dict(r) for r in _ALLOC_ROOMS] if t == "rooms" else [])
    out = reports.generate_allocation_report(for_month=(2026, 6))
    assert "No one-off costs tagged yet" in out


# ── Month-end verification ────────────────────────────────────────────

def test_count_sheet_leaves_the_blanks_blank(monkeypatch):
    """A sheet pre-filled with the expected figure is not a count."""
    out = reports.generate_count_sheet()
    assert "BAR   STORE  TOTAL" in out
    grid = out.split("```")[1]
    # The unit cost belongs on the sheet; the counts must not. Every item row
    # ends in three empty blanks — nothing pre-filled to read off and tick.
    item_rows = [l for l in grid.splitlines() if "____" in l]
    assert item_rows
    for row in item_rows:
        assert row.count("____") == 3


def test_variance_report_flags_a_surplus_rather_than_congratulating(monkeypatch):
    counts = [{"id": 1, "timestamp": "2026-06-28 08:00:00", "drink_name": "coke",
               "expected": 30, "counted": 33, "variance": 3, "cost_price": 120,
               "location": "bar"}]
    real = reports.db.read_all
    monkeypatch.setattr(reports.db, "read_all",
                        lambda t: counts if t == "stock_counts" else real(t))
    out = reports.generate_variance_report(for_month=(2026, 6))
    assert "🔺 *Surplus:" in out
    assert "Not good news" in out
    assert "No shortages recorded" not in out   # the old clean bill of health


def test_a_month_with_no_stocktake_is_marked_unverified(monkeypatch):
    real = reports.db.read_all
    monkeypatch.setattr(reports.db, "read_all",
                        lambda t: [] if t == "stock_counts" else real(t))
    for out in (reports.generate_full_report(for_month=(2026, 6)),
                reports.generate_sales_report(for_month=(2026, 6))):
        assert "UNVERIFIED" in out
    variance = reports.generate_variance_report(for_month=(2026, 6))
    assert "UNVERIFIED" in variance


def test_a_counted_month_carries_no_warning():
    out = reports.generate_full_report(for_month=(2026, 6))
    assert "UNVERIFIED" not in out             # the fixture has June counts


def test_variance_report_never_names_a_person(monkeypatch):
    counts = [{"id": 1, "timestamp": "2026-06-28 08:00:00", "drink_name": "heineken",
               "expected": 40, "counted": 34, "variance": -6, "cost_price": 300,
               "location": "bar", "recorded_by": "zenobia"}]
    real = reports.db.read_all
    monkeypatch.setattr(reports.db, "read_all",
                        lambda t: counts if t == "stock_counts" else real(t))
    out = reports.generate_variance_report(for_month=(2026, 6))
    assert "zenobia" not in out.lower()        # variance only, never who held the sheet


def test_audit_sheet_prints_the_vacant_rooms(monkeypatch):
    from datetime import date as _d
    out = reports.audit_sheet([_d(2026, 6, 5)], 2026, 6)
    assert "VACANT (per system)" in out
    assert "shown vacant" in out


# ── Audit fixes ───────────────────────────────────────────────────────

def test_position_names_debts_with_no_sale_behind_them(monkeypatch):
    debts = [{"id": 9, "timestamp": "2026-06-10 20:00:00", "account": "bar",
              "name": "john", "amount": 10000, "amount_paid": 0,
              "status": "outstanding", "description": "tab"}]
    monkeypatch.setattr(reports.db, "read_all",
                        lambda t: [dict(r) for r in debts] if t == "debtors" else [])
    monkeypatch.setattr(reports.db, "get_outstanding_payables", lambda: [])
    out = reports.generate_position_report()
    assert "has no sale behind it" in out
    assert "`[9]` 2026-06-10  ₦10,000 — John (Bar)" in out
    assert "-₦" not in out.split("CASH AT HAND")[1].split("STOCK VALUE")[0]


def test_position_is_quiet_when_debts_are_paired():
    """The fixture has debtors and matching sales — nothing to warn about."""
    out = reports.generate_position_report()
    assert "has no sale behind it" not in out


def test_dow_report_splits_the_hourly_trade_by_time_of_day(monkeypatch):
    rooms = []
    for d in range(1, 29):
        # All keyed in the morning after; the band comes from the book.
        for band, n in (("Morning", 1), ("Evening", 4)):
            rooms.append({"id": len(rooms) + 1,
                          "timestamp": f"2026-06-{d:02d} 08:30:00",
                          "room_type": "short time", "quantity": 1, "nights": n,
                          "price_per_night": 3000, "total_revenue": n * 3000,
                          "daypart": band, "deleted_at": None})
    monkeypatch.setattr(reports.db, "read_all",
                        lambda t: [dict(r) for r in rooms] if t == "rooms" else [])
    monkeypatch.setattr(reports.db, "get_all_room_type_hours", lambda: {"short time": 2})
    monkeypatch.setattr(reports.db, "get_all_room_type_counts", lambda: {"short time": 2})
    out = reports.generate_dow_split_report(for_month=(2026, 6))
    assert "*BY TIME OF DAY* _(hourly lets only)_" in out
    assert "Evening" in out and "Morning" in out
    assert "Charge more in the evening" in out
    assert "lets a day across" in out


def test_a_nightly_only_hotel_gets_no_time_of_day_block():
    out = reports.generate_dow_split_report(for_month=(2026, 6))
    assert "BY TIME OF DAY" not in out
