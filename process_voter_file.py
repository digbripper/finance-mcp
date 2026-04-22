"""
Processes NYS voter file -> lean NYC super-voter CSV for finance MCP.
Filters to: NYC counties only + active voters + voted in 4+ general elections.
"""

import csv, sys, re, os, subprocess

NYC_COUNTIES = {"03", "24", "31", "41", "43"}
COUNTY_NAMES = {"03":"Bronx","24":"Kings","31":"Manhattan","41":"Queens","43":"Staten Island"}
ACTIVE_STATUSES = {"A", "AM", "AF", "AP", "AU"}

def count_election_types(history_str):
    if not history_str:
        return 0, 0
    events = [e.strip() for e in history_str.split(";") if e.strip()]
    ge, primary = 0, 0
    for event in events:
        el = event.lower()
        if "general election" in el or el.startswith("ge("):
            ge += 1
        elif "primary" in el:
            primary += 1
    return ge, primary

input_file = "/Volumes/External St/NY Voter Data/ALLNYVOTERS20260420/ALLNYVOTERS20260420.txt"
output_file = os.path.expanduser("~/finance-mcp-deploy/nyc_super_voters.csv")

GE_THRESHOLD = 4

print(f"Processing {input_file}...")
total = kept = skipped_county = skipped_status = skipped_votes = 0

with open(input_file, "r", encoding="latin-1") as infile, \
     open(output_file, "w", newline="") as outfile:

    writer = csv.writer(outfile)
    writer.writerow([
        "SBOEID","LASTNAME","FIRSTNAME","DOB","PARTY","OTHER_PARTY",
        "ADDRESS","CITY","ZIP","COUNTY_CODE","COUNTY_NAME",
        "CD","SD","AD","REGDATE","STATUS",
        "GE_VOTES","PRIMARY_VOTES","VOTER_SCORE"
    ])

    reader = csv.reader(infile)
    for row in reader:
        total += 1
        if total % 100000 == 0:
            print(f"  {total:,} rows processed, {kept:,} kept...", end="\r")

        if len(row) < 47:
            continue

        county = row[23].strip()
        status = row[41].strip()
        history = row[46].strip() if len(row) > 46 else ""

        if county not in NYC_COUNTIES:
            skipped_county += 1
            continue

        if status not in ACTIVE_STATUSES:
            skipped_status += 1
            continue

        ge_votes, primary_votes = count_election_types(history)
        if ge_votes < GE_THRESHOLD:
            skipped_votes += 1
            continue

        house    = row[4].strip()
        pre_dir  = row[6].strip()
        street   = row[7].strip()
        post_dir = row[8].strip()
        apt_type = row[9].strip()
        apt_num  = row[10].strip()
        address  = " ".join(x for x in [house, pre_dir, street, post_dir] if x)
        if apt_num:
            address += f" {apt_type} {apt_num}".strip()

        writer.writerow([
            row[45].strip(),
            row[0].strip(),
            row[1].strip(),
            row[19].strip(),
            row[21].strip(),
            row[22].strip(),
            address,
            row[12].strip(),
            row[13].strip(),
            county,
            COUNTY_NAMES.get(county, ""),
            row[28].strip(),
            row[29].strip(),
            row[30].strip(),
            row[37].strip(),
            status,
            ge_votes,
            primary_votes,
            ge_votes + primary_votes,
        ])
        kept += 1

print(f"\n\nDone!")
print(f"  Total rows:           {total:,}")
print(f"  Skipped (non-NYC):    {skipped_county:,}")
print(f"  Skipped (inactive):   {skipped_status:,}")
print(f"  Skipped (< {GE_THRESHOLD} GE):      {skipped_votes:,}")
print(f"  Kept (NYC super voters): {kept:,}")
result = subprocess.run(["ls", "-lah", output_file], capture_output=True, text=True)
print(result.stdout)
