"""
migrate_to_schema.py — One-time migration for the original Hotel 85 deployment.

Moves all tables from the default `public` schema into a named hotel schema
(default: hotel85) so the bot can run in multi-tenant mode.

IMPORTANT — run in this order:
  1.  Stop the running bot (Railway: pause the service)
  2.  Take a backup:  pg_dump $DATABASE_URL > backup_pre_migration.sql
  3.  Run this script: python scripts/migrate_to_schema.py
  4.  Set HOTEL_SCHEMA=hotel85 in Railway environment variables
  5.  Redeploy the bot

Do NOT deploy the new bot code before running this script — the new init_db()
will create empty tables in the hotel schema if it runs first.
"""
from __future__ import annotations

import os
import sys

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

TABLES = [
    "inventory",
    "sales",
    "rooms",
    "expenses",
    "debtors",
    "debtor_payments",
    "transfers",
    "users",
    "settings",
]

TARGET_SCHEMA = os.getenv("HOTEL_SCHEMA", "hotel85")


def main() -> None:
    raw_url = os.getenv("DATABASE_URL", "")
    if not raw_url:
        print("ERROR: DATABASE_URL is not set.", file=sys.stderr)
        sys.exit(1)
    url = raw_url.replace("postgres://", "postgresql://", 1)

    print(f"Connecting to database…")
    engine = create_engine(url)

    with engine.connect() as conn:
        # 1. Count rows before migration
        before: dict[str, int] = {}
        for table in TABLES:
            try:
                count = conn.execute(text(f"SELECT COUNT(*) FROM public.{table}")).scalar()
                before[table] = int(count or 0)
            except Exception:
                before[table] = -1  # table doesn't exist in public

        tables_to_move = [t for t in TABLES if before[t] >= 0]
        print(f"\nTables found in public schema: {tables_to_move}")
        print(f"Row counts: {before}")

        if not tables_to_move:
            print("\nNo tables found in public schema. Nothing to migrate.")
            return

        # 2. Create target schema
        print(f"\nCreating schema '{TARGET_SCHEMA}'…")
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA}"'))
        conn.commit()

        # 3. Move each table
        print(f"Moving tables to '{TARGET_SCHEMA}' schema…")
        for table in tables_to_move:
            try:
                conn.execute(text(f'ALTER TABLE public."{table}" SET SCHEMA "{TARGET_SCHEMA}"'))
                print(f"  ✓ {table}")
            except Exception as e:
                print(f"  ✗ {table}: {e}", file=sys.stderr)
                conn.rollback()
                print("\nMigration aborted — rolled back all changes.", file=sys.stderr)
                sys.exit(1)
        conn.commit()

        # 4. Verify row counts
        print("\nVerifying row counts…")
        all_ok = True
        for table in tables_to_move:
            after_count = conn.execute(
                text(f'SELECT COUNT(*) FROM "{TARGET_SCHEMA}"."{table}"')
            ).scalar()
            after_count = int(after_count or 0)
            status = "✓" if after_count == before[table] else "✗ MISMATCH"
            print(f"  {status}  {table}: {before[table]} → {after_count}")
            if after_count != before[table]:
                all_ok = False

        if not all_ok:
            print("\nERROR: Row count mismatch detected. Check the database manually.", file=sys.stderr)
            sys.exit(1)

        print(f"\n✅ Migration complete! All {len(tables_to_move)} tables moved to '{TARGET_SCHEMA}'.")
        print(f"\nNext steps:")
        print(f"  1. Set HOTEL_SCHEMA={TARGET_SCHEMA} in your Railway environment variables")
        print(f"  2. Redeploy the bot")


if __name__ == "__main__":
    main()
