"""
Refresh nys_boe_contributions from newer NYS BOE bulk files. Idempotent: rows
already loaded (same filer, year, filing period and transaction number) are
skipped, so the full ALL_REPORTS files or a single period's file both work.

    # Files downloaded in a browser (zip, csv, or a folder of them):
    DATABASE_URL=... python3 scripts/boe_quarterly_refresh.py ~/Downloads/ALL_REPORTS.zip
    DATABASE_URL=... python3 scripts/boe_quarterly_refresh.py ~/Desktop/boe-contributions

    # Or URLs, comma-separated (what the GitHub Action passes):
    DATABASE_URL=... BOE_ZIP_URLS=https://... python3 scripts/boe_quarterly_refresh.py

Where the files come from: https://publicreporting.elections.ny.gov/
DownloadCampaignFinanceData/DownloadCampaignFinanceData → Data Type
"Disclosure Report", then Report Year + Report Type (or All / All), and
"Filer Data" for the filer reference.

About automating the download (checked 2026-10-01): the site is behind
Cloudflare bot protection and returns 403 to scripted clients, including for
the documented commcand.zip link, and the report download needs a browser
session (a SetSessions call before DownloadZipFile). There is no stable public
URL to put in BOE_ZIP_URLS today, so the scheduled run exits with an error
that says so — treat it as the quarterly reminder to download and run this
locally. If BOE publishes direct links later, set the BOE_ZIP_URLS variable.
"""
import csv
import io
import os
import sys
import tempfile
import zipfile

import psycopg2

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import boe_common  # noqa: E402

csv.field_size_limit(10**9)


def is_filer_file(name: str) -> bool:
    return "commcand" in os.path.basename(name).lower()


def load_csv(conn, stream, name: str, totals: dict) -> None:
    reader = csv.reader(io.TextIOWrapper(stream, encoding="utf-8", errors="replace", newline=""))
    if is_filer_file(name):
        n = boe_common.load_filers(conn, reader)
        print(f"  {name}: {n:,} filers")
        return
    seen, ins, skip = boe_common.load_rows(conn, reader, os.path.basename(name), log=lambda m: print(m))
    print(f"  {name}: {seen:,} Schedule A rows — {ins:,} new, {skip:,} already present")
    totals["seen"] += seen
    totals["new"] += ins


def load_zip(conn, zf: zipfile.ZipFile, label: str, totals: dict) -> None:
    """ALL_REPORTS.zip holds one zip per filer type, each holding a CSV."""
    for info in zf.infolist():
        lower = info.filename.lower()
        if lower.endswith(".csv"):
            with zf.open(info) as f:
                load_csv(conn, f, info.filename, totals)
        elif lower.endswith(".zip"):
            with zf.open(info) as f, tempfile.TemporaryFile() as tmp:
                while True:
                    chunk = f.read(1 << 20)
                    if not chunk:
                        break
                    tmp.write(chunk)
                tmp.seek(0)
                with zipfile.ZipFile(tmp) as inner:
                    load_zip(conn, inner, info.filename, totals)


def load_path(conn, path: str, totals: dict) -> None:
    path = os.path.expanduser(path)
    if os.path.isdir(path):
        # Prefer extracted CSVs; fall back to zips that have no CSV beside them.
        csvs = set()
        for root, _dirs, files in os.walk(path):
            for name in sorted(files):
                if name.lower().endswith(".csv"):
                    csvs.add(os.path.splitext(os.path.join(root, name))[0].lower())
                    load_path(conn, os.path.join(root, name), totals)
        for root, _dirs, files in os.walk(path):
            for name in sorted(files):
                full = os.path.join(root, name)
                if name.lower().endswith(".zip") and os.path.splitext(full)[0].lower() not in csvs:
                    load_path(conn, full, totals)
    elif path.lower().endswith(".zip"):
        print(f"Loading {path}...")
        with zipfile.ZipFile(path) as zf:
            load_zip(conn, zf, path, totals)
    elif path.lower().endswith(".csv"):
        print(f"Loading {path}...")
        with open(path, "rb") as f:
            load_csv(conn, f, path, totals)
    else:
        print(f"Skipping {path} (not a zip, csv or folder)")


def download(url: str, dest) -> None:
    import requests
    resp = requests.get(url, stream=True, timeout=300, headers={"User-Agent": "pythia-boe-refresh/1.0"})
    if resp.status_code == 403:
        raise SystemExit(
            f"\n❌ {url} returned 403.\n"
            "The BOE site blocks scripted downloads (Cloudflare). Download the files in a browser from\n"
            "https://publicreporting.elections.ny.gov/DownloadCampaignFinanceData/DownloadCampaignFinanceData\n"
            "and run:  DATABASE_URL=... python3 scripts/boe_quarterly_refresh.py <downloaded.zip>")
    resp.raise_for_status()
    if "html" in (resp.headers.get("content-type") or "").lower():
        raise SystemExit(f"\n❌ {url} returned a web page, not a zip. See the note at the top of this script.")
    for chunk in resp.iter_content(1 << 20):
        dest.write(chunk)


def main() -> None:
    paths = [a for a in sys.argv[1:] if not a.startswith("--")]
    urls = [u.strip() for u in (os.environ.get("BOE_ZIP_URLS") or "").split(",") if u.strip()]
    if not paths and not urls:
        raise SystemExit(
            "❌ Nothing to load. Pass downloaded files/folders as arguments, or set BOE_ZIP_URLS.\n"
            "The BOE site has no stable scripted download link today — see the note at the top of\n"
            "scripts/boe_quarterly_refresh.py. Download in a browser, then run this script on the files.")

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    boe_common.ensure_schema(conn)
    totals = {"seen": 0, "new": 0}
    for path in paths:
        load_path(conn, path, totals)
    for url in urls:
        print(f"Downloading {url}...")
        with tempfile.TemporaryFile() as tmp:
            download(url, tmp)
            tmp.seek(0)
            with zipfile.ZipFile(tmp) as zf:
                load_zip(conn, zf, url, totals)

    with conn.cursor() as cur:
        cur.execute("ANALYZE nys_boe_contributions")
        cur.execute("SELECT COUNT(*), MIN(election_year), MAX(election_year) FROM nys_boe_contributions")
        n, lo, hi = cur.fetchone()
    conn.commit()
    conn.close()
    print(f"\n✅ Refresh complete: {totals['new']:,} new of {totals['seen']:,} Schedule A rows read. "
          f"Table has {n:,} rows, {lo}–{hi}.")


if __name__ == "__main__":
    main()
