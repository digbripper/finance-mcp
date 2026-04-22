"""
Builds a SQLite voter database from the NYS voter file.
Filters to active NYC voters only (no vote-count minimum).
Indexes on LASTNAME and (LASTNAME, FIRSTNAME) for fast lookup.

Run locally: python3 build_voter_db.py
Output: nyc_voters.db (~300-500MB)
"""

import csv, os, sqlite3, time

NYC_COUNTIES    = {"03", "24", "31", "41", "43"}
COUNTY_NAMES    = {"03":"Bronx","24":"Kings","31":"Manhattan","41":"Queens","43":"Staten Island"}
ACTIVE_STATUSES = {"A", "AM", "AF", "AP", "AU"}

INPUT  = "/Volumes/External St/NY Voter Data/ALLNYVOTERS20260420/ALLNYVOTERS20260420.txt"
OUTPUT = os.path.expanduser("~/finance-mcp-deploy/nyc_voters.db")

def count_elections(history_str):
    if not history_str:
        return 0, 0
    events = [e.strip() for e in history_str.split(";") if e.strip()]
    ge = sum(1 for e in events if "general election" in e.lower())
    pr = sum(1 for e in events if "primary" in e.lower())
    return ge, pr

print(f"Building SQLite voter DB from {INPUT}")
print(f"Output: {OUTPUT}")
t0 = time.time()

# Remove old DB if exists
if os.path.exists(OUTPUT):
    os.remove(OUTPUT)
    print("Removed existing DB")

con = sqlite3.connect(OUTPUT)
cur = con.cursor()

cur.executescript("""
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=NORMAL;

    CREATE TABLE voters (
        sboeid       TEXT PRIMARY KEY,
        lastname     TEXT NOT NULL,
        firstname    TEXT NOT NULL,
        middlename   TEXT,
        dob          TEXT,
        party        TEXT,
        address      TEXT,
        city         TEXT,
        zip          TEXT,
        county_code  TEXT,
        county_name  TEXT,
        cd           TEXT,
        sd           TEXT,
        ad           TEXT,
        regdate      TEXT,
        status       TEXT,
        ge_votes     INTEGER DEFAULT 0,
        primary_votes INTEGER DEFAULT 0,
        voter_score  INTEGER DEFAULT 0
    );
""")

BATCH = 10000
batch = []
total = kept = 0

with open(INPUT, "r", encoding="latin-1") as f:
    reader = csv.reader(f)
    for row in reader:
        total += 1
        if total % 200000 == 0:
            elapsed = time.time() - t0
            print(f"  {total:,} rows | {kept:,} kept | {elapsed:.0f}s elapsed...", end="\r")

        if len(row) < 47:
            continue
        if row[23].strip() not in NYC_COUNTIES:
            continue
        if row[41].strip() not in ACTIVE_STATUSES:
            continue

        county = row[23].strip()
        history = row[46].strip() if len(row) > 46 else ""
        ge, pr = count_elections(history)

        house   = row[4].strip()
        pre_dir = row[6].strip()
        street  = row[7].strip()
        post_dir= row[8].strip()
        apt_type= row[9].strip()
        apt_num = row[10].strip()
        addr    = " ".join(x for x in [house, pre_dir, street, post_dir] if x)
        if apt_num:
            addr += f" {apt_type} {apt_num}".strip()

        sboeid = row[45].strip()
        if not sboeid:
            continue

        batch.append((
            sboeid,
            row[0].strip().upper(),    # lastname
            row[1].strip().upper(),    # firstname
            row[2].strip().upper(),    # middlename
            row[19].strip(),           # dob
            row[21].strip(),           # party
            addr,
            row[12].strip(),           # city
            row[13].strip(),           # zip
            county,
            COUNTY_NAMES.get(county, ""),
            row[28].strip(),           # cd
            row[29].strip(),           # sd
            row[30].strip(),           # ad
            row[37].strip(),           # regdate
            row[41].strip(),           # status
            ge, pr, ge + pr,
        ))
        kept += 1

        if len(batch) >= BATCH:
            cur.executemany(
                "INSERT OR IGNORE INTO voters VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                batch
            )
            con.commit()
            batch = []

if batch:
    cur.executemany(
        "INSERT OR IGNORE INTO voters VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        batch
    )
    con.commit()

print(f"\n\nBuilding indexes...")
cur.executescript("""
    CREATE INDEX idx_lastname       ON voters(lastname);
    CREATE INDEX idx_lastname_first ON voters(lastname, firstname);
    CREATE INDEX idx_party          ON voters(party);
    CREATE INDEX idx_zip            ON voters(zip);
""")
con.close()

elapsed = time.time() - t0
import subprocess
size = subprocess.run(["ls", "-lah", OUTPUT], capture_output=True, text=True).stdout
print(f"Done in {elapsed:.0f}s")
print(f"Total rows:        {total:,}")
print(f"Active NYC voters: {kept:,}")
print(f"Database: {size}")
