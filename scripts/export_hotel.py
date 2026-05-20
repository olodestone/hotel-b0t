"""
export_hotel.py — Export all data for a hotel schema to an Excel file.

Usage:
    python scripts/export_hotel.py --schema hotel85 --out hotel85_export.xlsx

Each table becomes its own sheet. A Summary sheet at the front shows key totals.
The file can be opened in Excel, Google Sheets, or LibreOffice Calc.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
from sqlalchemy import create_engine, text


TABLES = [
    "sales",
    "rooms",
    "expenses",
    "debtors",
    "debtor_payments",
    "inventory",
    "transfers",
    "users",
    "settings",
]


def get_engine(schema: str):
    raw = os.getenv("DATABASE_URL", "")
    if not raw:
        print("ERROR: DATABASE_URL is not set.", file=sys.stderr)
        sys.exit(1)
    url = raw.replace("postgres://", "postgresql://", 1)
    return create_engine(url, connect_args={"options": f"-c search_path={schema},public"})


def build_summary(engine) -> pd.DataFrame:
    rows = []

    with engine.connect() as conn:
        # Total bar revenue
        r = conn.execute(text(
            "SELECT COALESCE(SUM(total_revenue),0) FROM sales "
            "WHERE deleted_at = '' OR deleted_at IS NULL"
        )).scalar()
        rows.append(("Bar Revenue (₦)", round(float(r), 2)))

        # Total rooms revenue
        r = conn.execute(text(
            "SELECT COALESCE(SUM(total_revenue),0) FROM rooms "
            "WHERE deleted_at = '' OR deleted_at IS NULL"
        )).scalar()
        rows.append(("Rooms Revenue (₦)", round(float(r), 2)))

        # Total expenses
        r = conn.execute(text(
            "SELECT COALESCE(SUM(amount),0) FROM expenses "
            "WHERE deleted_at = '' OR deleted_at IS NULL"
        )).scalar()
        rows.append(("Total Expenses (₦)", round(float(r), 2)))

        # Outstanding debtors
        r = conn.execute(text(
            "SELECT COALESCE(SUM(amount - amount_paid),0) FROM debtors "
            "WHERE status = 'outstanding'"
        )).scalar()
        rows.append(("Outstanding Debt (₦)", round(float(r), 2)))

        # Drink stock value (bar)
        r = conn.execute(text(
            "SELECT COALESCE(SUM(current_stock * cost_price),0) FROM inventory"
        )).scalar()
        rows.append(("Bar Stock Value (₦)", round(float(r), 2)))

        # Drink stock value (store)
        r = conn.execute(text(
            "SELECT COALESCE(SUM(store_stock * cost_price),0) FROM inventory"
        )).scalar()
        rows.append(("Store Stock Value (₦)", round(float(r), 2)))

        # Number of sales records
        r = conn.execute(text(
            "SELECT COUNT(*) FROM sales WHERE deleted_at = '' OR deleted_at IS NULL"
        )).scalar()
        rows.append(("Total Sale Transactions", int(r)))

        # Number of room bookings
        r = conn.execute(text(
            "SELECT COUNT(*) FROM rooms WHERE deleted_at = '' OR deleted_at IS NULL"
        )).scalar()
        rows.append(("Total Room Bookings", int(r)))

        # Staff count
        r = conn.execute(text("SELECT COUNT(*) FROM users")).scalar()
        rows.append(("Registered Users", int(r)))

    rows.append(("Export Date", datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    return pd.DataFrame(rows, columns=["Metric", "Value"])


def export(schema: str, out_path: str) -> None:
    engine = get_engine(schema)

    print(f"Exporting schema '{schema}' → {out_path}")

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        # Summary sheet first
        print("  Writing Summary…")
        summary = build_summary(engine)
        summary.to_excel(writer, sheet_name="Summary", index=False)

        # One sheet per table
        for table in TABLES:
            try:
                df = pd.read_sql(f"SELECT * FROM {table}", engine)
                df.to_excel(writer, sheet_name=table.title(), index=False)
                print(f"  Writing {table} ({len(df)} rows)…")
            except Exception as e:
                print(f"  Skipping {table}: {e}")

    print(f"\n✅ Export complete: {out_path}")
    print(f"   Open in Excel, Google Sheets, or LibreOffice Calc.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Export all hotel data to Excel.")
    parser.add_argument("--schema", required=True, help="Hotel schema slug, e.g. hotel85")
    parser.add_argument("--out", default=None, help="Output filename (default: <schema>_export_<date>.xlsx)")
    args = parser.parse_args()

    out = args.out or f"{args.schema}_export_{datetime.now().strftime('%Y%m%d')}.xlsx"
    export(args.schema, out)


if __name__ == "__main__":
    main()
