"""
One-time backfill of NYS BOE contributions into Neon from the bulk downloads.

    DATABASE_URL=... python3 scripts/load_boe_to_neon.py [--base ~/Desktop/boe-contributions]

Expects, under --base:
    ALL_REPORTS_StateCandidate/*.csv   (candidates filing in their own name)
    ALL_REPORTS_StateCommittee/*.csv   (all committees — almost all of the data)
    commcand/COMMCAND.CSV              (filer reference)

Keeps itemized Schedule A rows only. Safe to re-run: existing rows are skipped.
See boe_common.py for the file layout and why the key isn't TRANS_NUMBER alone.
"""
import csv
import glob
import os
import sys
import time

import psycopg2

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import boe_common  # noqa: E402

csv.field_size_limit(10**9)


def main():
    base = os.path.expanduser("~/Desktop/boe-contributions")
    if "--base" in sys.argv:
        base = os.path.expanduser(sys.argv[sys.argv.index("--base") + 1])
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    started = time.time()

    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.nys_boe_contributions') IS NOT NULL")
        existed = cur.fetchone()[0]
    # First load: create indexes at the end. Later runs keep them in place.
    boe_common.ensure_schema(conn, with_indexes=existed)

    filer_files = [p for p in glob.glob(os.path.join(base, "commcand", "*")) if p.lower().endswith(".csv")]
    for path in filer_files:
        with open(path, encoding="utf-8", errors="replace", newline="") as f:
            n = boe_common.load_filers(conn, csv.reader(f))
        print(f"Loaded {n:,} filers from {os.path.basename(path)}")

    total_seen = total_in = total_skip = 0
    for folder in ("ALL_REPORTS_StateCandidate", "ALL_REPORTS_StateCommittee"):
        files = sorted(p for p in glob.glob(os.path.join(base, folder, "**", "*"), recursive=True)
                       if p.lower().endswith(".csv"))
        for path in files:
            name = os.path.basename(path)
            print(f"\nProcessing {folder}/{name} ({os.path.getsize(path) / 1e6:,.0f} MB)...")
            with open(path, encoding="utf-8", errors="replace", newline="") as f:
                seen, ins, skip = boe_common.load_rows(conn, csv.reader(f), name)
            print(f"  Done: {seen:,} Schedule A rows — {ins:,} inserted, {skip:,} already present")
            total_seen += seen; total_in += ins; total_skip += skip

    print("\nBuilding indexes...")
    boe_common.build_indexes(conn)

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), MIN(election_year), MAX(election_year), SUM(amount) FROM nys_boe_contributions")
        n, lo, hi, dollars = cur.fetchone()
    conn.close()
    print(f"\n✅ Complete in {(time.time() - started) / 60:.1f} min: {total_in:,} inserted, {total_skip:,} skipped. "
          f"Table now has {n:,} rows, {lo}–{hi}, ${float(dollars or 0):,.0f} total.")


if __name__ == "__main__":
    main()
