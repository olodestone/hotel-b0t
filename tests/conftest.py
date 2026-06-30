"""
Shared test fixtures.

Two things make the financial reports testable without a live database:

1. ``database.read_all`` / ``database.get_setting`` are monkeypatched to serve an
   in-memory fixture dataset, so no PostgreSQL is needed.
2. ``datetime`` is frozen in ``reports`` and ``metrics`` so the "Generated …" /
   "current month" output is deterministic.

The golden-master tests (``test_golden_reports.py``) snapshot the exact Telegram
strings; these fixtures keep that snapshot reproducible run-to-run.
"""
from __future__ import annotations

import copy
import os
import sys
from datetime import datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import database  # noqa: E402
import metrics    # noqa: E402
import reports    # noqa: E402

SNAP_DIR = Path(__file__).parent / "snapshots"
FROZEN_NOW = datetime(2026, 6, 29, 14, 30, 0)


class _FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):  # noqa: ARG003 - tz unused, matches datetime.now signature
        return FROZEN_NOW


# In-memory fixture dataset. Most rows fall in the frozen "current month"
# (June 2026); a few prior-month rows exercise the date filters, one soft-deleted
# row exercises _active(), and a restock expense exercises the NON-P&L path.
def _dataset() -> dict:
    return {
        "sales": [
            {"id": 1, "timestamp": "2026-06-01 10:00:00", "drink_name": "heineken", "quantity": 6, "selling_price": 500, "total_revenue": 3000, "recorded_by": "john", "deleted_at": None},
            {"id": 2, "timestamp": "2026-06-15 12:30:00", "drink_name": "heineken", "quantity": 4, "selling_price": 500, "total_revenue": 2000, "recorded_by": "mary", "deleted_at": None},
            {"id": 3, "timestamp": "2026-06-20 18:00:00", "drink_name": "coke",     "quantity": 10, "selling_price": 200, "total_revenue": 2000, "recorded_by": "john", "deleted_at": None},
            {"id": 4, "timestamp": "2026-05-30 09:00:00", "drink_name": "coke",     "quantity": 5,  "selling_price": 200, "total_revenue": 1000, "recorded_by": "mary", "deleted_at": None},
            {"id": 5, "timestamp": "2026-06-10 14:00:00", "drink_name": "heineken", "quantity": 2,  "selling_price": 500, "total_revenue": 1000, "recorded_by": "john", "deleted_at": "2026-06-11 00:00:00"},
        ],
        "rooms": [
            {"id": 1, "timestamp": "2026-06-05 15:00:00", "room_type": "standard", "quantity": 2, "price_per_night": 15000, "nights": 3, "total_revenue": 90000, "recorded_by": "mary", "deleted_at": None},
            {"id": 2, "timestamp": "2026-06-22 16:00:00", "room_type": "deluxe",   "quantity": 1, "price_per_night": 25000, "nights": 2, "total_revenue": 50000, "recorded_by": "john", "deleted_at": None},
            {"id": 3, "timestamp": "2026-05-20 11:00:00", "room_type": "standard", "quantity": 1, "price_per_night": 15000, "nights": 1, "total_revenue": 15000, "recorded_by": "mary", "deleted_at": None},
        ],
        "expenses": [
            {"id": 1, "timestamp": "2026-06-03 09:00:00", "account": "bar",   "category": "salary",    "amount": 50000, "description": "bar wages", "deleted_at": None},
            {"id": 2, "timestamp": "2026-06-04 09:00:00", "account": "rooms", "category": "salary",    "amount": 45000, "description": "rooms wages", "deleted_at": None},
            {"id": 3, "timestamp": "2026-06-07 12:00:00", "account": "bar",   "category": "utilities", "amount": 8000,  "description": "electricity", "deleted_at": None},
            {"id": 4, "timestamp": "2026-06-09 12:00:00", "account": "rooms", "category": "laundry",   "amount": 6000,  "description": "linens", "deleted_at": None},
            {"id": 5, "timestamp": "2026-06-12 12:00:00", "account": "bar",   "category": "restock",   "amount": 30000, "description": "restock heineken", "deleted_at": None},
            {"id": 6, "timestamp": "2026-06-18 12:00:00", "account": "bar",   "category": "utilities", "amount": 2000,  "description": "water", "deleted_at": None},
            {"id": 7, "timestamp": "2026-05-25 12:00:00", "account": "bar",   "category": "utilities", "amount": 9999,  "description": "prior month", "deleted_at": None},
        ],
        "debtors": [
            {"id": 1, "timestamp": "2026-06-08 17:00:00", "account": "bar",   "name": "sam",       "amount": 5000,  "amount_paid": 2000, "description": "tab",   "status": "outstanding", "paid_at": None, "recorded_by": "john", "staff_name": "peter"},
            {"id": 2, "timestamp": "2026-06-14 17:00:00", "account": "rooms", "name": "acme corp", "amount": 30000, "amount_paid": 0,    "description": "event", "status": "outstanding", "paid_at": None, "recorded_by": "mary"},
            {"id": 3, "timestamp": "2026-06-02 17:00:00", "account": "bar",   "name": "paid guy",  "amount": 1000,  "amount_paid": 1000, "description": "",      "status": "paid", "paid_at": "2026-06-03 10:00:00", "recorded_by": "john"},
        ],
        "debtor_payments": [
            {"id": 1, "debtor_id": 1, "timestamp": "2026-06-10 10:00:00", "amount": 2000, "recorded_by": "john"},
        ],
        "inventory": [
            {"drink_name": "heineken", "current_stock": 12, "store_stock": 24, "total_purchased": 50, "total_sold": 12, "cost_price": 300, "low_stock_threshold": 5, "selling_price": 500},
            {"drink_name": "coke",     "current_stock": 3,  "store_stock": 0,  "total_purchased": 30, "total_sold": 15, "cost_price": 120, "low_stock_threshold": 5, "selling_price": 200},
        ],
        "owner_draws": [
            {"id": 1, "timestamp": "2026-06-25 19:00:00", "amount": 40000, "account": "", "description": "owner withdrawal", "recorded_by": "john", "deleted_at": None, "deleted_by": None},
        ],
        "transfers": [
            {"id": 1, "timestamp": "2026-06-06 08:00:00", "drink_name": "heineken", "quantity": 12, "recorded_by": "john"},
        ],
    }


# Settings empty → reports fall back to config allocation defaults (deterministic).
_SETTINGS: dict = {}


@pytest.fixture(autouse=True)
def patch_db_and_time(monkeypatch):
    data = _dataset()

    def fake_read_all(table):
        # deepcopy so a report mutating its working copy can't bleed across tests
        return copy.deepcopy(data.get(table, []))

    def fake_get_setting(key, default=""):
        return _SETTINGS.get(key, default)

    monkeypatch.setattr(database, "read_all", fake_read_all)
    monkeypatch.setattr(database, "get_setting", fake_get_setting)
    monkeypatch.setattr(reports, "datetime", _FrozenDateTime)
    monkeypatch.setattr(metrics, "datetime", _FrozenDateTime)
    yield


@pytest.fixture
def snapshot():
    """Golden-master assert. Run with REGEN=1 to (re)write snapshots from current output."""
    def _assert(name: str, output: str):
        SNAP_DIR.mkdir(exist_ok=True)
        path = SNAP_DIR / f"{name}.txt"
        if os.getenv("REGEN") or not path.exists():
            path.write_text(output, encoding="utf-8")
            return
        expected = path.read_text(encoding="utf-8")
        assert output == expected, (
            f"\nSnapshot mismatch for '{name}'.\n"
            f"Telegram output changed — if this is intentional, re-run with REGEN=1.\n"
        )
    return _assert
