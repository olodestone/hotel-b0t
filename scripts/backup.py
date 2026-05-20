"""
backup.py — Per-schema pg_dump backup for hotel bot deployments.

Dumps only this hotel's schema (not the entire database) and saves it
locally. Optional: upload to Backblaze B2 by setting B2_* env vars.

Usage:
  python scripts/backup.py

Schedule on Railway:
  Add a separate "Cron Job" service in Railway pointing to this script,
  configured to run nightly (e.g. 0 2 * * *).

  Or add to bot.py's APScheduler job queue to run alongside the daily report.

Environment variables:
  DATABASE_URL   — PostgreSQL connection string (required)
  HOTEL_SCHEMA   — Hotel schema to back up (required)
  BACKUP_DIR     — Local directory for dump files (default: ./backups)
  B2_KEY_ID      — Backblaze B2 application key ID (optional)
  B2_APP_KEY     — Backblaze B2 application key (optional)
  B2_BUCKET_NAME — Backblaze B2 bucket name (optional)
"""
from __future__ import annotations

import gzip
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DATABASE_URL  = os.getenv("DATABASE_URL", "")
HOTEL_SCHEMA  = os.getenv("HOTEL_SCHEMA", "")
BACKUP_DIR    = Path(os.getenv("BACKUP_DIR", "./backups"))
B2_KEY_ID     = os.getenv("B2_KEY_ID", "")
B2_APP_KEY    = os.getenv("B2_APP_KEY", "")
B2_BUCKET     = os.getenv("B2_BUCKET_NAME", "")


def run_dump() -> Path:
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL is not set.", file=sys.stderr)
        sys.exit(1)
    if not HOTEL_SCHEMA:
        print("ERROR: HOTEL_SCHEMA is not set.", file=sys.stderr)
        sys.exit(1)

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = BACKUP_DIR / f"{HOTEL_SCHEMA}_{stamp}.sql.gz"

    url = DATABASE_URL.replace("postgres://", "postgresql://", 1)

    print(f"Dumping schema '{HOTEL_SCHEMA}' → {out_file} …")
    dump_proc = subprocess.Popen(
        ["pg_dump", f"--schema={HOTEL_SCHEMA}", url],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = dump_proc.communicate()
    if dump_proc.returncode != 0:
        print(f"ERROR: pg_dump failed:\n{stderr.decode()}", file=sys.stderr)
        sys.exit(1)

    with gzip.open(out_file, "wb") as f:
        f.write(stdout)

    size_kb = out_file.stat().st_size // 1024
    print(f"✓ Dump complete: {out_file} ({size_kb} KB)")
    return out_file


def upload_b2(file: Path) -> None:
    try:
        from b2sdk.v2 import B2Api, InMemoryAccountInfo  # type: ignore
    except ImportError:
        print("b2sdk not installed — skipping B2 upload. Run: pip install b2sdk")
        return

    print(f"Uploading to Backblaze B2 bucket '{B2_BUCKET}' …")
    info = InMemoryAccountInfo()
    api  = B2Api(info)
    api.authorize_account("production", B2_KEY_ID, B2_APP_KEY)
    bucket = api.get_bucket_by_name(B2_BUCKET)
    bucket.upload_local_file(
        local_file=str(file),
        file_name=f"hotel-backups/{file.name}",
    )
    print(f"✓ Uploaded: hotel-backups/{file.name}")


def prune_local(keep: int = 7) -> None:
    """Keep only the most recent `keep` local dumps for this schema."""
    dumps = sorted(BACKUP_DIR.glob(f"{HOTEL_SCHEMA}_*.sql.gz"))
    for old in dumps[:-keep]:
        old.unlink()
        print(f"  removed old backup: {old.name}")


def main() -> None:
    dump_file = run_dump()

    if B2_KEY_ID and B2_APP_KEY and B2_BUCKET:
        upload_b2(dump_file)
    else:
        print("B2 credentials not set — backup saved locally only.")

    prune_local(keep=7)
    print("Done.")


if __name__ == "__main__":
    main()
