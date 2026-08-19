"""
clock.py — the one wall clock, answered in the *hotel's* timezone.

Every "what time is it" in this codebase used to be `datetime.now()`, which is
the server's clock. Railway runs UTC, so a Lagos bar serving at 00:30 had the
sale filed under the previous day — wrong day in /report, /summary, /history and
the daily report, every night the bar traded past midnight.

Hotels span timezones, so this cannot be fixed by setting a process-wide TZ: one
process serves them all. The active hotel's timezone is a ContextVar set per
update / per web request / per scheduled job, exactly like
``database._hotel_schema_var`` — and ``database.set_tenant()`` sets both together
so the two can never disagree.

This module sits *below* database.py deliberately: metrics.py is the pure calc
core and must stay free of database imports, but it still needs to know what
"this month" means. clock.py depends on nothing but config and pytz.

``now()`` returns a **naive** datetime already shifted into the hotel's local
time. Every timestamp in the database is a naive local string, so naive-local is
the representation the whole codebase already speaks; handing back an aware
datetime would break every comparison against a parsed row.
"""
from __future__ import annotations

import contextvars
from datetime import date, datetime

import pytz

from config import TIMEZONE

# Timezone of the hotel whose request/update/job is currently being handled.
# Default covers process-level work that runs outside any tenant context.
_tz_var: contextvars.ContextVar[str] = contextvars.ContextVar("hotel_tz", default=TIMEZONE)


def _resolve(tz_name: str | None) -> str:
    """A usable timezone name. Unknown or blank falls back to the configured
    default rather than raising — a bad string in one hotel's row must not take
    that hotel's bot down."""
    name = (tz_name or "").strip() or TIMEZONE
    try:
        pytz.timezone(name)
    except Exception:
        return TIMEZONE
    return name


def use(tz_name: str | None) -> None:
    """Point the clock at a hotel's timezone for the current context."""
    _tz_var.set(_resolve(tz_name))


def enter(tz_name: str | None):
    """Like ``use()``, but returns a token for ``reset()`` — for nested contexts
    such as the dashboard resolving one user's access across several hotels."""
    return _tz_var.set(_resolve(tz_name))


def reset(token) -> None:
    _tz_var.reset(token)


def timezone_name() -> str:
    """Name of the timezone currently in effect (for display and scheduling)."""
    return _tz_var.get()


def now() -> datetime:
    """Current local time for the active hotel, as a naive datetime."""
    return datetime.now(pytz.timezone(_tz_var.get())).replace(tzinfo=None)


def today() -> date:
    """The active hotel's current date — what "today" means in its reports."""
    return now().date()
