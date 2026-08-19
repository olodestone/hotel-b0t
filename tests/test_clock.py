"""
The hotel clock: every "now" must be answered in the *hotel's* timezone.

The bug these guard against: the server runs UTC, so a Lagos bar serving at
00:30 had its sale stamped 23:30 the previous day and filed into the wrong
day's report. Hotels span timezones, so a process-wide TZ can't fix it — the
timezone is per-tenant context, set alongside the schema.
"""
from __future__ import annotations

from datetime import datetime

import pytz

import clock
import database

# Captured at import, before conftest's autouse fixture freezes it. Tests that
# need the *real* clock behaviour reinstate this.
_REAL_NOW = clock.now


def test_now_is_naive_local_time_for_the_active_hotel(monkeypatch):
    """Naive-local is the representation the whole codebase speaks: every stored
    timestamp is a naive local string, so an aware datetime here would break
    every comparison against a parsed row."""
    monkeypatch.setattr(clock, "now", _REAL_NOW)

    clock.use("America/Denver")
    got = clock.now()
    expected = datetime.now(pytz.timezone("America/Denver")).replace(tzinfo=None)

    assert got.tzinfo is None, "callers compare this against naive parsed rows"
    assert abs((got - expected).total_seconds()) < 2


def test_two_hotels_read_different_clocks_in_one_process(monkeypatch):
    """The whole reason a process-wide TZ isn't enough."""
    monkeypatch.setattr(clock, "now", _REAL_NOW)

    clock.use("Africa/Lagos")
    lagos = clock.now()
    clock.use("America/Denver")
    denver = clock.now()

    def utc_offset_hours(tz_name, at):
        return pytz.timezone(tz_name).utcoffset(at).total_seconds() / 3600

    expected_gap = utc_offset_hours("Africa/Lagos", lagos) - utc_offset_hours("America/Denver", denver)
    assert round((lagos - denver).total_seconds() / 3600) == expected_gap


def test_a_broken_timezone_falls_back_instead_of_raising():
    """One hotel's bad row must not take that hotel's bot down."""
    clock.use("Africa/Lagos")
    for bad in ("Not/AZone", "", None, "   "):
        clock.use(bad)
        assert clock.timezone_name() == clock.TIMEZONE


def test_nested_tenant_contexts_restore_the_outer_clock():
    """The dashboard resolves one user's access across several hotels in a loop;
    each inner scope must hand the outer one back unchanged."""
    clock.use("Africa/Lagos")
    token = clock.enter("America/Denver")
    assert clock.timezone_name() == "America/Denver"
    clock.reset(token)
    assert clock.timezone_name() == "Africa/Lagos"


def test_set_tenant_moves_the_clock_with_the_schema(monkeypatch):
    """Scoping the engine without scoping the clock is exactly how entries ended
    up filed under the wrong day — set_tenant must do both."""
    monkeypatch.setattr(clock, "now", _REAL_NOW)
    # Pre-seed the cache so this stays a pure unit test (no live engine).
    monkeypatch.setitem(database._tz_cache, "lagos_hotel", "Africa/Lagos")
    monkeypatch.setitem(database._tz_cache, "denver_hotel", "America/Denver")

    database.set_tenant("lagos_hotel")
    assert clock.timezone_name() == "Africa/Lagos"
    lagos_stamp = database.now_str()

    database.set_tenant("denver_hotel")
    assert clock.timezone_name() == "America/Denver"
    denver_stamp = database.now_str()

    assert lagos_stamp != denver_stamp, "both hotels stamped on the same clock"


def test_reset_tenant_restores_both_schema_and_clock(monkeypatch):
    monkeypatch.setitem(database._tz_cache, "outer", "Africa/Lagos")
    monkeypatch.setitem(database._tz_cache, "inner", "Asia/Tokyo")

    database.set_tenant("outer")
    token = database.set_tenant("inner")
    assert database._hotel_schema_var.get() == "inner"
    assert clock.timezone_name() == "Asia/Tokyo"

    database.reset_tenant(token)
    assert database._hotel_schema_var.get() == "outer"
    assert clock.timezone_name() == "Africa/Lagos"


def test_midnight_rollover_lands_in_the_right_day(monkeypatch):
    """The reported symptom, pinned.

    A sale keyed at 00:30 Lagos is 23:30 UTC the *previous* day. Stamping it on
    the server clock filed it into yesterday's /report, /summary and /history.
    """
    lagos = pytz.timezone("Africa/Lagos")
    local_wall_clock = lagos.localize(datetime(2026, 8, 20, 0, 30))
    server_utc = local_wall_clock.astimezone(pytz.UTC).replace(tzinfo=None)

    assert server_utc.date().day == 19, "precondition: UTC is still on the 19th"

    monkeypatch.setattr(clock, "now", lambda: local_wall_clock.replace(tzinfo=None))
    monkeypatch.setitem(database._tz_cache, "lagos_hotel", "Africa/Lagos")
    database.set_tenant("lagos_hotel")

    assert database.now_str().startswith("2026-08-20"), "sale filed under the wrong day"
