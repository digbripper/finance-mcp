"""
NYS Board of Elections contribution data → Neon (nys_boe_contributions).

Shared by the Finance MCP server (name normalization for lookups), the
one-time backfill (scripts/load_boe_to_neon.py) and the quarterly refresh
(scripts/boe_quarterly_refresh.py). Plain Python 3.9+, psycopg2 only.

The bulk files from publicreporting.elections.ny.gov have NO header row and 59
columns. The published FileFormatReference.pdf lists 56 and is out of date:
the real files add FILER_PREVIOUS_ID after FILER_ID and COUNTY_DESC after the
election type, so everything from FILING_ABBREV on sits later than documented.
Positions below were verified against the 2026-09 download (every one of
11.9M rows has 59 columns).

Facts about the data that shaped the schema:
  - TRANS_NUMBER is NOT unique. Different filers reuse numbers (29,737 rows)
    and one filer can repeat a number across filing periods (4,595), so the
    key is (filer_id, election_year, filing_abbrev, trans_number).
  - OWED_AMT on Schedule A is not a pledge: where present (4% of rows) it
    equals ORG_AMT in 99.99% of cases. It is stored but never added to amount.
  - R_LIABILITY is blank on Schedule A; R_LOBBYIST is 'Y' on only ~230 rows.
"""
from __future__ import annotations

import csv
import io
import re

# 0-based positions in the 59-column bulk file.
COL = dict(
    filer_id=0, cand_comm_name=2, election_year=3, report_type=4,
    filing_abbrev=6, filing_desc=7, filing_sched_abbrev=10,
    trans_number=13, sched_date=15, contrib_type=17,
    entity_name=24, first_name=25, middle_name=26, last_name=27,
    address=28, city=29, state=30, zip=31,
    owed_amt=35, amount=36, r_itemized=39, r_liability=40,
    is_lobbyist=49, employer=52, occupation=53,
)
EXPECTED_COLUMNS = 59

# Filer reference file (COMMCAND.CSV), also headerless.
FILER_COL = dict(filer_id=0, filer_name=1, compliance_type=2, filer_type=3, filer_status=4,
                 committee_type=5, office_desc=6, district=7, county_desc=8)

INSERT_COLUMNS = [
    "filer_id", "cand_comm_name", "election_year", "report_type", "filing_abbrev",
    "filing_sched_abbrev", "filing_desc", "sched_date", "contrib_type",
    "entity_name", "first_name", "middle_name", "last_name", "full_name", "last_name_norm",
    "address", "city", "state", "zip", "amount", "owed_amt", "trans_number",
    "is_lobbyist", "employer", "occupation", "r_liability", "source_file",
]
CONFLICT_KEY = "(filer_id, election_year, filing_abbrev, trans_number)"

TABLE_DDL = """
CREATE TABLE IF NOT EXISTS nys_boe_contributions (
  id                  BIGSERIAL PRIMARY KEY,

  -- Recipient (in every row — no join needed)
  filer_id            TEXT NOT NULL,
  cand_comm_name      TEXT NOT NULL,

  -- Filing metadata
  election_year       INTEGER NOT NULL,
  report_type         TEXT,        -- election type: State/Local, ...
  filing_abbrev       TEXT NOT NULL DEFAULT '',  -- disclosure period letter (A-L)
  filing_sched_abbrev TEXT,        -- 'A' = monetary contributions from individuals & partnerships
  filing_desc         TEXT,        -- disclosure period name (32-Day Pre-Primary, ...)
  sched_date          DATE,

  -- Contributor identity
  contrib_type        TEXT,        -- Individual / Partnership including LLPs / ...
  entity_name         TEXT,        -- org name if entity donor
  first_name          TEXT,
  middle_name         TEXT,
  last_name           TEXT,
  full_name           TEXT,        -- name parts joined, or entity_name
  last_name_norm      TEXT,        -- lower-case, punctuation and Jr/Sr/III stripped; lookup key

  -- Contributor address
  address             TEXT,
  city                TEXT,
  state               TEXT,
  zip                 TEXT,

  -- Transaction amounts
  amount              NUMERIC(14,2),            -- ORG_AMT: the contribution
  owed_amt            NUMERIC(14,2) DEFAULT 0,  -- OWED_AMT: duplicates amount where present; do not add

  trans_number        TEXT NOT NULL,            -- NOT unique on its own; see the constraint

  -- Donor context
  is_lobbyist         BOOLEAN,     -- R_LOBBYIST
  employer            TEXT,
  occupation          TEXT,
  r_liability         BOOLEAN,     -- R_LIABILITY (blank on Schedule A)

  -- Source tracking
  source_file         TEXT,
  created_at          TIMESTAMPTZ DEFAULT NOW(),

  CONSTRAINT nys_boe_contributions_key UNIQUE (filer_id, election_year, filing_abbrev, trans_number)
);

CREATE TABLE IF NOT EXISTS nys_boe_filers (
  filer_id        TEXT PRIMARY KEY,
  filer_name      TEXT,
  compliance_type TEXT,
  filer_type      TEXT,
  filer_status    TEXT,
  committee_type  TEXT,
  office_desc     TEXT,
  district        TEXT,
  county_desc     TEXT
);
"""

# Built after the initial load (much faster than maintaining them row by row).
INDEX_DDL = [
    "CREATE INDEX IF NOT EXISTS idx_boe_last_name_norm ON nys_boe_contributions(last_name_norm)",
    "CREATE INDEX IF NOT EXISTS idx_boe_full_name  ON nys_boe_contributions(LOWER(full_name))",
    "CREATE INDEX IF NOT EXISTS idx_boe_recipient  ON nys_boe_contributions(LOWER(cand_comm_name))",
    "CREATE INDEX IF NOT EXISTS idx_boe_filer      ON nys_boe_contributions(filer_id)",
    "CREATE INDEX IF NOT EXISTS idx_boe_year       ON nys_boe_contributions(election_year)",
    "CREATE INDEX IF NOT EXISTS idx_boe_amount     ON nys_boe_contributions(amount DESC)",
    "CREATE INDEX IF NOT EXISTS idx_boe_lobbyist   ON nys_boe_contributions(is_lobbyist) WHERE is_lobbyist = TRUE",
    "ANALYZE nys_boe_contributions",
]

_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v", "esq", "md", "phd", "dds", "cpa"}


def norm_last_name(name: str | None) -> str:
    """'Costin Jr' → 'costin', "O'Brien" → 'obrien', 'De La Rosa' → 'de la rosa'."""
    s = re.sub(r"[.,'’`]", "", (name or "").lower())
    s = re.sub(r"[^a-z0-9 -]", " ", s)
    tokens = [t for t in s.split() if t not in _SUFFIXES]
    return " ".join(tokens)


def _amount(value: str) -> str:
    try:
        return f"{float(value.replace(',', '').strip()):.2f}" if value and value.strip() else ""
    except ValueError:
        return ""


def _flag(value: str) -> str:
    v = (value or "").strip().upper()
    return "t" if v == "Y" else "f" if v == "N" else ""


def _date(value: str) -> str:
    v = (value or "").strip()[:10]
    return v if re.fullmatch(r"(19[89]\d|20\d\d)-\d\d-\d\d", v) else ""


def map_row(row: list[str], source_file: str) -> list[str] | None:
    """One bulk-file row → values in INSERT_COLUMNS order (strings; '' = NULL),
    or None unless it is an itemized Schedule A contribution."""
    if len(row) < 54:
        return None
    g = lambda key: row[COL[key]].strip()
    if g("filing_sched_abbrev").upper() != "A" or g("r_itemized").upper() != "Y":
        return None
    trans, filer, year = g("trans_number"), g("filer_id"), g("election_year")
    if not trans or not filer or not year.isdigit():
        return None
    first, middle, last, entity = g("first_name"), g("middle_name"), g("last_name"), g("entity_name")
    full_name = " ".join(p for p in (first, middle, last) if p) if last else entity
    return [
        filer, g("cand_comm_name") or "(unnamed filer)", year, g("report_type"), g("filing_abbrev") or "-",
        "A", g("filing_desc"), _date(g("sched_date")), g("contrib_type"),
        entity, first, middle, last, full_name, norm_last_name(last),
        g("address"), g("city"), g("state")[:2], g("zip")[:10],
        _amount(g("amount")) or "0.00", _amount(g("owed_amt")) or "0.00", trans,
        _flag(g("is_lobbyist")), g("employer"), g("occupation"), _flag(g("r_liability")), source_file,
    ]


def ensure_schema(conn, with_indexes: bool = True) -> None:
    with conn.cursor() as cur:
        cur.execute(TABLE_DDL)
        if with_indexes:
            for ddl in INDEX_DDL:
                cur.execute(ddl)
    conn.commit()


def build_indexes(conn, log=print) -> None:
    for ddl in INDEX_DDL:
        log(f"  {ddl.split(' ON ')[0]}...")
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()


def load_rows(conn, rows, source_file: str, chunk: int = 50_000, log=print) -> tuple[int, int, int]:
    """
    Stream csv rows (lists) into nys_boe_contributions. Idempotent: rows whose
    (filer, year, period, trans number) already exist are skipped.
    Returns (schedule_a_rows, inserted, skipped).
    """
    cols = ", ".join(INSERT_COLUMNS)
    with conn.cursor() as cur:
        # A real table rather than TEMP: Neon's pooled connections don't keep
        # session state between transactions.
        cur.execute("DROP TABLE IF EXISTS nys_boe_stage")
        cur.execute(f"CREATE UNLOGGED TABLE nys_boe_stage AS SELECT {cols} FROM nys_boe_contributions WITH NO DATA")
    conn.commit()

    seen = inserted = 0
    buf = io.StringIO()
    writer = csv.writer(buf)
    pending = 0

    def flush():
        nonlocal inserted, pending, buf, writer
        if not pending:
            return
        buf.seek(0)
        with conn.cursor() as cur:
            cur.copy_expert(f"COPY nys_boe_stage ({cols}) FROM STDIN WITH (FORMAT csv, NULL '')", buf)
            cur.execute(f"""
                INSERT INTO nys_boe_contributions ({cols})
                SELECT {cols} FROM nys_boe_stage
                ON CONFLICT {CONFLICT_KEY} DO NOTHING
            """)
            inserted += cur.rowcount
            cur.execute("TRUNCATE nys_boe_stage")
        conn.commit()
        buf = io.StringIO()
        writer = csv.writer(buf)
        pending = 0

    for row in rows:
        mapped = map_row(row, source_file)
        if mapped is None:
            continue
        writer.writerow(mapped)
        seen += 1
        pending += 1
        if pending >= chunk:
            flush()
            log(f"  {seen:,} Schedule A rows, {inserted:,} inserted, {seen - inserted:,} already present")
    flush()
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS nys_boe_stage")
    conn.commit()
    return seen, inserted, seen - inserted


def load_filers(conn, rows) -> int:
    from psycopg2.extras import execute_values
    c = FILER_COL
    batch = {}
    for row in rows:
        if len(row) < 9 or not row[c["filer_id"]].strip():
            continue
        g = lambda key: row[c[key]].strip() or None
        batch[row[c["filer_id"]].strip()] = (
            row[c["filer_id"]].strip(), g("filer_name"), g("compliance_type"), g("filer_type"),
            g("filer_status"), g("committee_type"), g("office_desc"), g("district"), g("county_desc"))
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO nys_boe_filers
                (filer_id, filer_name, compliance_type, filer_type, filer_status,
                 committee_type, office_desc, district, county_desc)
            VALUES %s
            ON CONFLICT (filer_id) DO UPDATE SET
                filer_name = EXCLUDED.filer_name, compliance_type = EXCLUDED.compliance_type,
                filer_type = EXCLUDED.filer_type, filer_status = EXCLUDED.filer_status,
                committee_type = EXCLUDED.committee_type, office_desc = EXCLUDED.office_desc,
                district = EXCLUDED.district, county_desc = EXCLUDED.county_desc
        """, list(batch.values()), page_size=2000)
    conn.commit()
    return len(batch)
