"""
manage_hotel.py — Suspend, unsuspend, or list hotels.

Usage:
    python scripts/manage_hotel.py list
    python scripts/manage_hotel.py suspend  --schema kings_inn
    python scripts/manage_hotel.py unsuspend --schema kings_inn

Suspending sets is_active=FALSE in public.hotels. The hotel's data is fully
preserved — sales, rooms, expenses, everything stays untouched. The bot simply
stops loading for that hotel on next restart.

After suspend or unsuspend: restart the Railway service to apply the change.
"""
from __future__ import annotations

import argparse
import os
import sys

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()


def get_engine():
    raw = os.getenv("DATABASE_URL", "")
    if not raw:
        print("ERROR: DATABASE_URL is not set.", file=sys.stderr)
        sys.exit(1)
    return create_engine(raw.replace("postgres://", "postgresql://", 1))


def cmd_list() -> None:
    with get_engine().connect() as conn:
        rows = conn.execute(text(
            "SELECT schema_name, name, is_active, created_at FROM public.hotels ORDER BY id"
        )).fetchall()

    if not rows:
        print("No hotels registered yet.")
        return

    print(f"\n{'Schema':<20} {'Name':<25} {'Status':<12} {'Created'}")
    print("-" * 75)
    for r in rows:
        status = "✅ active" if r[2] else "🔴 suspended"
        name = r[1] or "(not set up)"
        created = str(r[3])[:10] if r[3] else "—"
        print(f"{r[0]:<20} {name:<25} {status:<12} {created}")
    print()


def cmd_suspend(schema: str) -> None:
    with get_engine().connect() as conn:
        result = conn.execute(
            text("UPDATE public.hotels SET is_active = FALSE WHERE schema_name = :s"),
            {"s": schema},
        )
        conn.commit()

    if result.rowcount == 0:
        print(f"ERROR: Schema '{schema}' not found.", file=sys.stderr)
        sys.exit(1)

    print(f"🔴 '{schema}' suspended. Data is fully preserved.")
    print("   Restart the Railway service to apply — the bot will stop loading for this hotel.")


def cmd_unsuspend(schema: str) -> None:
    with get_engine().connect() as conn:
        result = conn.execute(
            text("UPDATE public.hotels SET is_active = TRUE WHERE schema_name = :s"),
            {"s": schema},
        )
        conn.commit()

    if result.rowcount == 0:
        print(f"ERROR: Schema '{schema}' not found.", file=sys.stderr)
        sys.exit(1)

    print(f"✅ '{schema}' unsuspended.")
    print("   Restart the Railway service to apply — the bot will start loading again.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage hotel subscriptions.")
    sub = parser.add_subparsers(dest="action", required=True)

    sub.add_parser("list", help="List all hotels and their status")

    p_suspend = sub.add_parser("suspend", help="Suspend a hotel (preserves all data)")
    p_suspend.add_argument("--schema", required=True)

    p_unsuspend = sub.add_parser("unsuspend", help="Re-activate a suspended hotel")
    p_unsuspend.add_argument("--schema", required=True)

    args = parser.parse_args()

    if args.action == "list":
        cmd_list()
    elif args.action == "suspend":
        cmd_suspend(args.schema)
    elif args.action == "unsuspend":
        cmd_unsuspend(args.schema)


if __name__ == "__main__":
    main()
