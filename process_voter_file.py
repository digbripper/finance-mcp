"""
Processes NYS voter file -> lean NYC voter CSV for finance MCP.
Filters to: active NYC voters who voted in 2+ general elections.

Output fields:
  SBOEID, LASTNAME, FIRSTNAME, DOB, PARTY, OTHER_PARTY,
  ADDRESS, CITY, ZIP, COUNTY_CODE, COUNTY_NAME,
  CD, SD, AD, COUNCIL_DISTRICT, REGDATE, STATUS,
  GE_VOTES, PRIMARY_VOTES, VOTER_SCORE,
  GE_YEARS, PRIMARY_YEARS

Run: python3 process_voter_file.py
"""

import csv, os, re, subprocess

NYC_COUNTIES    = {"03", "24", "31", "41", "43"}
COUNTY_NAMES    = {"03": "Bronx", "24": "Kings", "31": "Manhattan",
                   "41": "Queens", "43": "Staten Island"}
ACTIVE_STATUSES = {"A", "AM", "AF", "AP", "AU"}

INPUT  = "/Volumes/External St/NY Voter Data/ALLNYVOTERS20260420/ALLNYVOTERS20260420.txt"
OUTPUT = os.path.expanduser("~/finance-mcp-deploy/nyc_super_voters.csv")

GE_THRESHOLD = 2   # voted in at least 2 general elections

def parse_election_years(history_str: str) -> tuple[list[str], list[str]]:
    """
    Parse VoterHistory string, return (ge_years, primary_years) as
    sorted descending lists of 4-digit year strings.
    e.g. ["2025","2024","2023"], ["2024","2022"]
    """
    if not history_str:
        return [], []
    ge_years:      set[str] = set()
    primary_years: set[str] = set()
    for event in history_str.split(";"):
        event = event.strip()
        if not event:
            continue
        # Extract 4-digit year (2000+)
        m = re.search(r'\b(20\d{2})\b', event)
        if not m:
            continue
        year = m.group(1)
        el = event.lower()
        if "general election" in el or el.startswith("ge("):
            ge_years.add(year)
        elif any(x in el for x in ["primary", "presidential primary"]):
            primary_years.add(year)
    return (sorted(ge_years, reverse=True),
            sorted(primary_years, reverse=True))

print(f"Processing {INPUT}")
print(f"Output:    {OUTPUT}")
print(f"Filter:    active NYC voters with {GE_THRESHOLD}+ general elections")

total = kept = skipped_county = skipped_status = skipped_votes = 0

with open(INPUT, "r", encoding="latin-1") as infile, \
     open(OUTPUT, "w", newline="") as outfile:

    writer = csv.writer(outfile)
    writer.writerow([
        "SBOEID", "LASTNAME", "FIRSTNAME", "DOB", "PARTY", "OTHER_PARTY",
        "ADDRESS", "CITY", "ZIP", "COUNTY_CODE", "COUNTY_NAME",
        "CD", "SD", "AD", "COUNCIL_DISTRICT", "REGDATE", "STATUS",
        "GE_VOTES", "PRIMARY_VOTES", "VOTER_SCORE",
        "GE_YEARS", "PRIMARY_YEARS",
    ])

    reader = csv.reader(infile)
    for row in reader:
        total += 1
        if total % 200000 == 0:
            print(f"  {total:,} rows | {kept:,} kept...", end="\r")

        if len(row) < 47:
            continue

        county  = row[23].strip()
        status  = row[41].strip()
        history = row[46].strip() if len(row) > 46 else ""

        if county not in NYC_COUNTIES:
            skipped_county += 1
            continue
        if status not in ACTIVE_STATUSES:
            skipped_status += 1
            continue

        ge_years, primary_years = parse_election_years(history)
        if len(ge_years) < GE_THRESHOLD:
            skipped_votes += 1
            continue

        # Build address
        house    = row[4].strip()
        pre_dir  = row[6].strip()
        street   = row[7].strip()
        post_dir = row[8].strip()
        apt_type = row[9].strip()
        apt_num  = row[10].strip()
        address  = " ".join(x for x in [house, pre_dir, street, post_dir] if x)
        if apt_num:
            address += f" {apt_type} {apt_num}".strip()

        ge_count      = len(ge_years)
        primary_count = len(primary_years)
        voter_score   = ge_count + primary_count

        writer.writerow([
            row[45].strip(),                    # SBOEID
            row[0].strip(),                     # LASTNAME
            row[1].strip(),                     # FIRSTNAME
            row[19].strip(),                    # DOB
            row[21].strip(),                    # PARTY
            row[22].strip(),                    # OTHER_PARTY
            address,                            # ADDRESS
            row[12].strip(),                    # CITY
            row[13].strip(),                    # ZIP
            county,                             # COUNTY_CODE
            COUNTY_NAMES.get(county, ""),       # COUNTY_NAME
            row[28].strip(),                    # CD (congressional)
            row[29].strip(),                    # SD (state senate)
            row[30].strip(),                    # AD (assembly)
            row[25].strip(),                    # COUNCIL_DISTRICT (LD = local legislative)
            row[37].strip(),                    # REGDATE
            status,                             # STATUS
            ge_count,                           # GE_VOTES
            primary_count,                      # PRIMARY_VOTES
            voter_score,                        # VOTER_SCORE
            ",".join(ge_years),                 # GE_YEARS  e.g. "2025,2024,2023"
            ",".join(primary_years),            # PRIMARY_YEARS
        ])
        kept += 1

print(f"\n\nDone!")
print(f"  Total rows:            {total:,}")
print(f"  Skipped (non-NYC):     {skipped_county:,}")
print(f"  Skipped (inactive):    {skipped_status:,}")
print(f"  Skipped (< {GE_THRESHOLD} GE):       {skipped_votes:,}")
print(f"  Kept:                  {kept:,}")
result = subprocess.run(["ls", "-lah", OUTPUT], capture_output=True, text=True)
print(result.stdout)
