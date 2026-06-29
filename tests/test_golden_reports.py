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
