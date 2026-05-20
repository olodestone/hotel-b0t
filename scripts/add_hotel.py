"""
add_hotel.py — Register a new hotel so the multi-bot runner picks it up on next restart.

Usage:
    python scripts/add_hotel.py --schema kings_inn --token 1234567:AAAxxx --admin-id 98765432

What it does:
  1. Creates the PostgreSQL schema for the hotel (e.g. kings_inn)
  2. Creates all hotel tables inside that schema
  3. Registers the hotel in public.hotels with its bot token

After running, restart the Railway service — the new bot starts automatically.
"""
from __future__ import annotations

import argparse
import os
import sys

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import database as db


def main() -> None:
    parser = argparse.ArgumentParser(description="Register a new hotel for the multi-bot runner.")
    parser.add_argument("--schema", required=True, help="Unique slug, e.g. kings_inn")
    parser.add_argument("--token", required=True, help="Telegram bot token from @BotFather")
    parser.add_argument("--admin-id", dest="admin_id", help="Telegram user ID of the hotel admin")
    args = parser.parse_args()

    schema = args.schema.strip().lower().replace(" ", "_")
    token = args.token.strip()
    admin_ids = args.admin_id.strip() if args.admin_id else ""

    if not os.getenv("DATABASE_URL"):
        print("ERROR: DATABASE_URL is not set.", file=sys.stderr)
        sys.exit(1)

    print(f"Setting up schema '{schema}'…")
    db.init_db(schema=schema, token=token)

    print("Registering hotel config…")
    db.add_hotel_config(schema=schema, token=token, admin_ids=admin_ids)

    print(f"\n✅ Hotel '{schema}' registered successfully.")
    print("Restart the Railway service — the new bot will start automatically.")


if __name__ == "__main__":
    main()
