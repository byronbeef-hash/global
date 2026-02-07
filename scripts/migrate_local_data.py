#!/usr/bin/env python3
"""Migrate existing local CSV data into Supabase.

Usage:
    # Set env vars first:
    export SUPABASE_URL=https://your-project.supabase.co
    export SUPABASE_KEY=your-anon-key

    # Run from the cattle_scraper_prod directory:
    python scripts/migrate_local_data.py /path/to/cattle_contacts.csv

    # Or use the default path (the comprehensive export):
    python scripts/migrate_local_data.py
"""

import csv
import os
import sys
from pathlib import Path

# Add parent to path so we can import app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

from supabase import create_client


def migrate(csv_path: str, supabase_url: str, supabase_key: str) -> None:
    """Read CSV and upsert all records into Supabase contacts table."""
    client = create_client(supabase_url, supabase_key)

    # Read CSV
    print(f"Reading {csv_path}...")
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"Found {len(rows)} rows in CSV")

    # Deduplicate by email (keep the row with more data)
    by_email: dict[str, dict] = {}
    for row in rows:
        email = row.get("email", "").strip().lower()
        if not email:
            continue

        if email not in by_email:
            by_email[email] = row
        else:
            # Keep the row with more non-empty fields
            existing_filled = sum(1 for v in by_email[email].values() if v)
            new_filled = sum(1 for v in row.values() if v)
            if new_filled > existing_filled:
                by_email[email] = row

    unique_records = list(by_email.values())
    print(f"Unique emails after dedup: {len(unique_records)}")

    # Upsert in batches
    batch_size = 100
    total_upserted = 0
    errors = 0

    for i in range(0, len(unique_records), batch_size):
        batch = unique_records[i:i + batch_size]

        # Clean records for Supabase
        clean_batch = []
        for row in batch:
            clean = {
                "email": row.get("email", "").strip().lower(),
                "farm_name": row.get("farm_name", ""),
                "owner_name": row.get("owner_name", ""),
                "phone": row.get("phone", ""),
                "address": row.get("address", ""),
                "city": row.get("city", ""),
                "state": row.get("state", ""),
                "zip_code": row.get("zip_code", ""),
                "website": row.get("website", ""),
                "facebook": row.get("facebook", ""),
                "instagram": row.get("instagram", ""),
                "cattle_type": row.get("cattle_type", ""),
                "breed": row.get("breed", ""),
                "head_count": row.get("head_count", ""),
                "source_url": row.get("source_url", ""),
            }
            if clean["email"]:
                clean_batch.append(clean)

        if not clean_batch:
            continue

        try:
            result = client.table("contacts").upsert(
                clean_batch,
                on_conflict="email",
            ).execute()
            total_upserted += len(clean_batch)
            print(f"  Batch {i // batch_size + 1}: upserted {len(clean_batch)} records (total: {total_upserted})")
        except Exception as e:
            errors += 1
            print(f"  Batch {i // batch_size + 1} ERROR: {e}")

    print(f"\nMigration complete!")
    print(f"  Total upserted: {total_upserted}")
    print(f"  Errors: {errors}")

    # Verify
    result = client.table("contacts").select("id", count="exact").execute()
    print(f"  Contacts in Supabase: {result.count}")


def main():
    # Get Supabase credentials
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")

    if not supabase_url or not supabase_key:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY environment variables must be set.")
        print("  export SUPABASE_URL=https://your-project.supabase.co")
        print("  export SUPABASE_KEY=your-anon-key")
        sys.exit(1)

    # Get CSV path
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    else:
        # Default: look for the comprehensive export
        default_paths = [
            Path(__file__).parent.parent.parent / "cattle_scraper" / "data" / "output" / "comprehensive_email_list.csv",
            Path(__file__).parent.parent.parent / "cattle_scraper" / "data" / "output" / "cattle_contacts.csv",
        ]
        csv_path = None
        for p in default_paths:
            if p.exists():
                csv_path = str(p)
                break

        if not csv_path:
            print("ERROR: No CSV file found. Provide the path as an argument:")
            print("  python scripts/migrate_local_data.py /path/to/cattle_contacts.csv")
            sys.exit(1)

    if not Path(csv_path).exists():
        print(f"ERROR: File not found: {csv_path}")
        sys.exit(1)

    migrate(csv_path, supabase_url, supabase_key)


if __name__ == "__main__":
    main()
