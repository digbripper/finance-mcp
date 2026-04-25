"""
Builds a SQLite voter database from the NYS voter file.
Filters to active NYC voters with 2+ general elections.
Includes council_district, ge_years, primary_years columns.

Run: python3 build_voter_db.py
Output: nyc_voters.db
"""

import csv, os, re, sqlite3, subprocess, time

NYC_COUNTIES    = {"03", "24", "31", "41", "43"}
COUNTY_NAMES    = {"03": "Bronx", "24": "Kings", "31": "Manhattan",
                   "41": "Queens", "43": "Staten Island"}
ACTIVE_STATUSES = {"A", "AM", "AF", "AP", "AU"}
GE_THRESHOLD    = 2

INPUT  = "/Volumes/External St/NY Voter Data/ALLNYVOTERS20260420/ALLNYVOTERS20260420.txt"
OUTPUT = os.path.expanduser("~/finance-mcp-deploy/nyc_voters.db")

def parse_election_years(history_str):
    if not history_str:
        return [], []
    ge_years = set()
    primary_years = set()
    for event in history_str.split(";"):
        event = event.strip()
        if not event:
            continue
        m = re.search(r'\b(20\d{2})\b', event)
        if not m:
            continue
        year = m.group(1)
        el = event.lower()
        if "general election" in el or el.startswith("ge("):
            ge_years.add(year)
        elif any(x in el for x in ["primary", "presidential primary"]):
            primary_years.add(year)
    return sorted(ge_years, reverse=True), sorted(primary_years, reverse=True)

print(f"Building SQLite voter DB")
print(f"Input:  {INPUT}")
print(f"Output: {OUTPUT}")
t0 = time.time()

if os.path.exists(OUTPUT):
    os.remove(OUTPUT)
    print("Removed existing DB")

con = sqlite3.connect(OUTPUT)
cur = con.cursor()

cur.executescript("""
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=NORMAL;
    PRAGMA cache_size=-64000;

    CREATE TABLE voters (
        sboeid            TEXT PRIMARY KEY,
        lastname          TEXT NOT NULL,
        firstname         TEXT NOT NULL,
        middlename        TEXT,
        dob               TEXT,
        party             TEXT,
        address           TEXT,
        city              TEXT,
        zip               TEXT,
        county_code       TEXT,
        county_name       TEXT,
        cd                TEXT,
        sd                TEXT,
        ad                TEXT,
        council_district  TEXT,
        regdate           TEXT,
        status            TEXT,
        ge_votes          INTEGER DEFAULT 0,
        primary_votes     INTEGER DEFAULT 0,
        voter_score       INTEGER DEFAULT 0,
        ge_years          TEXT,
        primary_years     TEXT
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

        county  = row[23].strip()
        history = row[46].strip() if len(row) > 46 else ""
        ge_years, primary_years = parse_election_years(history)
        if len(ge_years) < GE_THRESHOLD:
            continue

        house    = row[4].strip()
        pre_dir  = row[6].strip()
        street   = row[7].strip()
        post_dir = row[8].strip()
        apt_type = row[9].strip()
        apt_num  = row[10].strip()
        addr     = " ".join(x for x in [house, pre_dir, street, post_dir] if x)
        if apt_num:
            addr += f" {apt_type} {apt_num}".strip()

        sboeid = row[45].strip()
        if not sboeid:
            continue

        ge_count = len(ge_years)
        prim_count = len(primary_years)

        batch.append((
            sboeid,
            row[0].strip().upper(),
            row[1].strip().upper(),
            row[2].strip().upper(),
            row[19].strip(),
            row[21].strip(),
            addr,
            row[12].strip(),
            row[13].strip(),
            county,
            COUNTY_NAMES.get(county, ""),
            row[28].strip(),    # cd congressional
            row[29].strip(),    # sd state senate
            row[30].strip(),    # ad assembly
            row[25].strip(),    # council_district (LD)
            row[37].strip(),
            row[41].strip(),
            ge_count,
            prim_count,
            ge_count + prim_count,
            ",".join(ge_years),
            ",".join(primary_years),
        ))
        kept += 1

        if len(batch) >= BATCH:
            cur.executemany(
                "INSERT OR IGNORE INTO voters VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                batch
            )
            con.commit()
            batch = []

if batch:
    cur.executemany(
        "INSERT OR IGNORE INTO voters VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        batch
    )
    con.commit()

print(f"\n\nBuilding indexes...")
cur.executescript("""
    CREATE INDEX idx_lastname           ON voters(lastname);
    CREATE INDEX idx_lastname_first     ON voters(lastname, firstname);
    CREATE INDEX idx_county_score       ON voters(county_code, voter_score);
    CREATE INDEX idx_county_party_score ON voters(county_code, party, voter_score);
    CREATE INDEX idx_county_ad          ON voters(county_code, ad);
    CREATE INDEX idx_county_sd          ON voters(county_code, sd);
    CREATE INDEX idx_county_cd          ON voters(county_code, cd);
    CREATE INDEX idx_council_district   ON voters(council_district);
    CREATE INDEX idx_zip                ON voters(zip);
""")
con.close()

elapsed = time.time() - t0
size = subprocess.run(["ls", "-lah", OUTPUT], capture_output=True, text=True).stdout
print(f"Done in {elapsed:.0f}s")
print(f"Total rows scanned: {total:,}")
print(f"Active NYC voters:  {kept:,}")
print(f"Database: {size}")
