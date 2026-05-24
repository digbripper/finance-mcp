"""
finance_mcp_server.py  — SSE transport edition
================================================
Remote MCP server for campaign finance enrichment.
Runs on Railway and connects via mcp-remote.

Data sources:
  - NYC CFB  (city-level, live API)
  - FEC      (federal, live API)
  - NYS BOE  (state-level, bundled parsed_contributions.csv)

Auth: every request must include  X-API-Key: <MCP_API_KEY>
"""

import asyncio
import csv
import json
import logging
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from mcp import types
from rapidfuzz import fuzz, process
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Mount, Route

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─── Config (lazy — never crash at import time) ───────────────────────────────

def _cfg(key: str, default: str = "") -> str:
    return os.environ.get(key, default)

def _require(key: str) -> str:
    val = os.environ.get(key, "")
    if not val:
        raise RuntimeError(f"Required env var {key!r} is not set")
    return val

CFB_BASE             = "https://data.cityofnewyork.us/resource"
FEC_BASE             = "https://api.open.fec.gov/v1"
CFB_CONTRIBUTIONS_ID = "k3cd-yu9d"
MATCH_THRESHOLD      = 82

BOE_CSV_PATH = Path(__file__).parent / "nys_boe_data" / "parsed_contributions.csv"

# ─── NYS BOE CSV cache ────────────────────────────────────────────────────────

_boe_rows:   list[dict] = []
_boe_loaded: bool       = False

def _load_boe_csv():
    global _boe_rows, _boe_loaded
    if _boe_loaded:
        return
    if not BOE_CSV_PATH.exists():
        log.warning(f"NYS BOE CSV not found at {BOE_CSV_PATH}")
        _boe_loaded = True
        return
    with open(BOE_CSV_PATH, newline="", encoding="utf-8") as f:
        _boe_rows = list(csv.DictReader(f))
    log.info(f"Loaded {len(_boe_rows):,} NYS BOE contributions")
    _boe_loaded = True

def boe_donors_to(candidate_name: str, limit: int = 100) -> list[dict]:
    _load_boe_csv()
    if not _boe_rows:
        return []
    norm_target = normalize(candidate_name)
    results = []
    for row in _boe_rows:
        cname = (row.get("candidate_name") or "").strip()
        if cname and fuzz.token_sort_ratio(normalize(cname), norm_target) >= MATCH_THRESHOLD:
            results.append(row)
            if len(results) >= limit:
                break
    return results

def boe_donations_by(donor_name: str, limit: int = 100) -> list[dict]:
    _load_boe_csv()
    if not _boe_rows:
        return []
    norm_target = normalize(donor_name)
    results = []
    for row in _boe_rows:
        cname = (row.get("contributor_name") or "").strip()
        if cname and fuzz.token_sort_ratio(normalize(cname), norm_target) >= MATCH_THRESHOLD:
            results.append(row)
            if len(results) >= limit:
                break
    return results


# ─── BOE donor name index (built lazily for fast voter cross-reference) ───────

_boe_donor_index: dict[str, list[dict]] = {}  # lastname_upper -> [rows]
_boe_index_built: bool = False

def _build_boe_donor_index():
    """Build a lastname-keyed index of BOE donor rows for fast lookup."""
    global _boe_donor_index, _boe_index_built
    if _boe_index_built:
        return
    _load_boe_csv()
    for row in _boe_rows:
        raw = (row.get("contributor_name") or "").strip().upper()
        if not raw:
            continue
        # Handle "LAST, FIRST" and "FIRST LAST" formats
        if "," in raw:
            last = raw.split(",")[0].strip()
        else:
            parts = raw.split()
            last = parts[-1] if parts else ""
        if last:
            _boe_donor_index.setdefault(last, []).append(row)
    log.info(f"BOE donor index built: {len(_boe_donor_index):,} unique last names")
    _boe_index_built = True

def boe_donations_by_voter(lastname: str, firstname: str) -> list[dict]:
    """Fast BOE lookup for a voter by last + first name."""
    _build_boe_donor_index()
    candidates = _boe_donor_index.get(lastname.upper(), [])
    if not candidates:
        return []
    first_up = firstname.upper()
    results = []
    for row in candidates:
        raw = (row.get("contributor_name") or "").strip().upper()
        # Check first name appears in contributor string
        if first_up[:4] in raw or raw.startswith(first_up[:4]):
            results.append(row)
    return results

# ─── DB helpers ───────────────────────────────────────────────────────────────

def get_db():
    return psycopg2.connect(_require("DATABASE_URL"), cursor_factory=psycopg2.extras.RealDictCursor)

def get_all_contacts() -> list[dict]:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    p.id::text,
                    p.full_name,
                    p.first_name,
                    p.last_name,
                    p.notes,
                    string_agg(DISTINCT o.name, ', ') AS orgs,
                    string_agg(DISTINCT po.job_title, ', ') AS titles
                FROM people_person p
                LEFT JOIN people_personorganization po ON p.id = po.person_id
                LEFT JOIN organizations_organization o ON po.organization_id = o.id
                WHERE p.is_active = TRUE
                GROUP BY p.id, p.full_name, p.first_name, p.last_name, p.notes
            """)
            return [dict(r) for r in cur.fetchall()]

def write_finance_note(person_id: str, note: str):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT notes FROM people_person WHERE id = %s", (person_id,))
            row = cur.fetchone()
            if not row:
                return
            existing = row["notes"] or ""
            if note[:60] in existing:
                return
            sep = "\n\n" if existing else ""
            cur.execute(
                "UPDATE people_person SET notes = %s, updated_at = NOW() WHERE id = %s",
                (existing + sep + note, person_id)
            )
        conn.commit()

def write_relationship(from_id: str, to_id: str, rel_type: str, context: str, notes: str) -> bool:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM people_personrelationship
                WHERE from_person_id = %s AND to_person_id = %s AND relationship_type = %s
            """, (from_id, to_id, rel_type))
            if cur.fetchone():
                return False
            cur.execute("""
                INSERT INTO people_personrelationship
                  (id, from_person_id, to_person_id, relationship_type,
                   context, notes, is_primary, is_active, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, FALSE, TRUE, NOW(), NOW())
            """, (str(uuid.uuid4()), from_id, to_id, rel_type, context, notes))
        conn.commit()
        return True

# ─── Name utilities ───────────────────────────────────────────────────────────

_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "esq", "phd", "md", "cpa", "dr"}

def normalize(name: str) -> str:
    name = re.sub(r"[^\w\s]", " ", (name or "").lower())
    return " ".join(t for t in name.split() if t not in _SUFFIXES).strip()

def build_index(contacts: list[dict]) -> tuple[dict, list[str]]:
    index = {}
    for c in contacts:
        display = c.get("full_name") or f"{c.get('first_name', '')} {c.get('last_name', '')}".strip()
        key = normalize(display)
        if key:
            index[key] = {**c, "_display": display}
    return index, list(index.keys())

def best_match(name: str, index: dict, keys: list[str]) -> tuple[Optional[dict], float]:
    norm = normalize(name)
    if not norm:
        return None, 0.0
    result = process.extractOne(norm, keys, scorer=fuzz.token_sort_ratio, score_cutoff=MATCH_THRESHOLD)
    if not result:
        return None, 0.0
    return index[result[0]], result[1]

# ─── Influence ranking — table setup & rollback ───────────────────────────────

import math as _math

# DDL for each object, in dependency order
_INFLUENCE_DDL: list[tuple[str, str]] = [
    ("_finance_migrations", """
        CREATE TABLE IF NOT EXISTS _finance_migrations (
            id              SERIAL PRIMARY KEY,
            migration_name  VARCHAR(100) UNIQUE NOT NULL,
            applied_at      TIMESTAMPTZ DEFAULT NOW(),
            rollback_sql    TEXT NOT NULL
        )
    """),
    ("people_voter_enrichment", """
        CREATE TABLE IF NOT EXISTS people_voter_enrichment (
            id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            person_id               UUID NOT NULL REFERENCES people_person(id) ON DELETE CASCADE,
            sboeid                  VARCHAR(20),
            party_code              VARCHAR(10),
            party_label             VARCHAR(50),
            voter_score             INTEGER DEFAULT 0,
            ge_votes                INTEGER DEFAULT 0,
            primary_votes           INTEGER DEFAULT 0,
            ge_years                TEXT    DEFAULT '',
            primary_years           TEXT    DEFAULT '',
            off_year_years          TEXT    DEFAULT '',
            assembly_district       VARCHAR(10),
            state_senate_district   VARCHAR(10),
            congressional_district  VARCHAR(10),
            county_code             VARCHAR(5),
            county_name             VARCHAR(50),
            voter_address           VARCHAR(255),
            voter_city              VARCHAR(100),
            voter_zip               VARCHAR(10),
            match_confidence        INTEGER DEFAULT 0,
            matched_at              TIMESTAMPTZ DEFAULT NOW(),
            updated_at              TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(person_id)
        )
    """),
    ("idx_pve_zip",    "CREATE INDEX IF NOT EXISTS idx_pve_zip    ON people_voter_enrichment(voter_zip)"),
    ("idx_pve_ad",     "CREATE INDEX IF NOT EXISTS idx_pve_ad     ON people_voter_enrichment(assembly_district)"),
    ("idx_pve_sd",     "CREATE INDEX IF NOT EXISTS idx_pve_sd     ON people_voter_enrichment(state_senate_district)"),
    ("idx_pve_cd",     "CREATE INDEX IF NOT EXISTS idx_pve_cd     ON people_voter_enrichment(congressional_district)"),
    ("idx_pve_county", "CREATE INDEX IF NOT EXISTS idx_pve_county ON people_voter_enrichment(county_code)"),
    ("people_influence_scores", """
        CREATE TABLE IF NOT EXISTS people_influence_scores (
            id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            person_id           UUID NOT NULL REFERENCES people_person(id) ON DELETE CASCADE,
            institutional_score NUMERIC(5,2) DEFAULT 0,
            financial_score     NUMERIC(5,2) DEFAULT 0,
            lobbying_score      NUMERIC(5,2) DEFAULT 0,
            network_score       NUMERIC(5,2) DEFAULT 0,
            engagement_score    NUMERIC(5,2) DEFAULT 0,
            composite_score     NUMERIC(5,2) DEFAULT 0,
            component_breakdown JSONB,
            algorithm_version   VARCHAR(20)  DEFAULT 'v1.0',
            computed_at         TIMESTAMPTZ  DEFAULT NOW(),
            UNIQUE(person_id)
        )
    """),
    ("idx_pis_composite", "CREATE INDEX IF NOT EXISTS idx_pis_composite ON people_influence_scores(composite_score DESC)"),
    ("organizations_990_data", """
        CREATE TABLE IF NOT EXISTS organizations_990_data (
            id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            organization_id  UUID NOT NULL REFERENCES organizations_organization(id) ON DELETE CASCADE,
            ein              VARCHAR(20),
            legal_name       VARCHAR(500),
            total_revenue    BIGINT,
            total_assets     BIGINT,
            total_expenses   BIGINT,
            num_employees    INTEGER,
            ntee_code        VARCHAR(20),
            fiscal_year      INTEGER,
            match_confidence INTEGER DEFAULT 0,
            fetched_at       TIMESTAMPTZ DEFAULT NOW(),
            updated_at       TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(organization_id)
        )
    """),
    ("idx_990_revenue", "CREATE INDEX IF NOT EXISTS idx_990_revenue ON organizations_990_data(total_revenue DESC NULLS LAST)"),
    ("organizations_union_data", """
        CREATE TABLE IF NOT EXISTS organizations_union_data (
            id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            organization_id   UUID NOT NULL REFERENCES organizations_organization(id) ON DELETE CASCADE,
            file_number       VARCHAR(20),
            legal_name        VARCHAR(500),
            affiliation       VARCHAR(100),
            state             VARCHAR(5),
            city              VARCHAR(100),
            total_receipts    BIGINT,
            total_disbursements BIGINT,
            total_assets      BIGINT,
            membership_count  INTEGER,
            report_year       INTEGER,
            form_type         VARCHAR(10),
            match_confidence  INTEGER DEFAULT 0,
            fetched_at        TIMESTAMPTZ DEFAULT NOW(),
            updated_at        TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(organization_id)
        )
    """),
    ("idx_union_receipts", "CREATE INDEX IF NOT EXISTS idx_union_receipts ON organizations_union_data(total_receipts DESC NULLS LAST)"),
]

_INFLUENCE_ROLLBACK_SQL = (
    "DROP TABLE IF EXISTS organizations_union_data CASCADE; "
    "DROP TABLE IF EXISTS organizations_990_data CASCADE; "
    "DROP TABLE IF EXISTS people_influence_scores CASCADE; "
    "DROP TABLE IF EXISTS people_voter_enrichment CASCADE; "
    "DROP TABLE IF EXISTS _finance_migrations CASCADE;"
)

def setup_influence_tables() -> dict:
    """
    Create the three influence-ranking tables.
    Safe to call multiple times — all statements use IF NOT EXISTS.
    Returns a summary of what was done and what to do next.
    """
    with get_db() as conn:
        for _name, ddl in _INFLUENCE_DDL:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()

        # Record the migration so rollback SQL is always available in the DB
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO _finance_migrations (migration_name, rollback_sql)
                VALUES ('001_influence_ranking_tables', %s)
                ON CONFLICT (migration_name) DO NOTHING
            """, (_INFLUENCE_ROLLBACK_SQL,))
        conn.commit()

    log.info("setup_influence_tables: all tables and indexes created")
    return {
        "status": "ok",
        "tables_created": [
            "_finance_migrations",
            "people_voter_enrichment",
            "people_influence_scores",
            "organizations_990_data",
            "organizations_union_data",
        ],
        "next_steps": [
            "1. Call enrich_voter_data           — matches contacts to voter file",
            "2. Call fetch_990_data              — fetches nonprofit 990 revenue data",
            "3. Call fetch_union_data            — fetches union LM-2 receipts from DOL OLMS",
            "4. Call compute_influence_scores    — computes v1 scores",
            "5. Call rank_influential_people     — query results with optional geo filter",
        ],
        "to_undo_everything": "Call rollback_influence_tables with confirm=true",
    }


def rollback_influence_tables() -> dict:
    """
    DROP all three influence-ranking tables and every row in them.
    Does NOT touch any existing Pythia tables.
    """
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(_INFLUENCE_ROLLBACK_SQL)
        conn.commit()
    log.info("rollback_influence_tables: all influence tables dropped")
    return {
        "status": "ok",
        "dropped": [
            "people_influence_scores",
            "people_voter_enrichment",
            "_finance_migrations",
        ],
        "message": "All influence ranking data has been removed. Existing Pythia tables are untouched.",
    }


# ─── Influence ranking — voter enrichment ────────────────────────────────────

def _tables_exist() -> tuple[bool, bool]:
    """Return (voter_enrichment_exists, influence_scores_exists)."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_name IN ('people_voter_enrichment', 'people_influence_scores')
            """)
            found = {r["table_name"] for r in cur.fetchall()}
    return ("people_voter_enrichment" in found, "people_influence_scores" in found)


def enrich_voter_data_batch(limit: int = 0) -> dict:
    """
    Match every active Pythia contact to the NYC voter file by name.
    Writes party, voter score, districts, and address into people_voter_enrichment.
    Safe to re-run — uses INSERT … ON CONFLICT DO UPDATE (upsert).
    """
    # ── Stage 1: get contacts ─────────────────────────────────────────────────
    try:
        contacts = get_all_contacts()
    except Exception as e:
        return {"error": f"Failed to load contacts: {e}"}

    if limit and limit > 0:
        contacts = contacts[:limit]

    if not contacts:
        return {"error": "No active contacts found in Pythia."}

    # ── Stage 2: open write connection ────────────────────────────────────────
    try:
        conn = get_db()
    except Exception as e:
        return {"error": f"DB connection failed: {e}"}

    matched, unmatched, errors = 0, 0, 0
    BATCH = 50

    try:
        batch_n = 0
        for i, contact in enumerate(contacts):
            pid  = contact.get("id", "")
            name = (contact.get("full_name") or "").strip()
            if not name or not pid:
                unmatched += 1
                continue

            try:
                voter = lookup_voter(name)

                if voter:
                    v_first = (voter.get("firstname") or voter.get("first") or "").title()
                    v_last  = (voter.get("lastname")  or voter.get("last")  or "").title()
                    v_full  = f"{v_first} {v_last}".strip()
                    conf    = int(fuzz.token_sort_ratio(normalize(name), normalize(v_full)))
                    party_code = voter.get("party") or voter.get("party_code") or ""

                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO people_voter_enrichment (
                                person_id, sboeid, party_code, party_label,
                                voter_score, ge_votes, primary_votes,
                                ge_years, primary_years, off_year_years,
                                assembly_district, state_senate_district, congressional_district,
                                county_code, county_name,
                                voter_address, voter_city, voter_zip,
                                match_confidence, matched_at, updated_at
                            ) VALUES (%s,%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s, %s,%s,%s, %s, NOW(),NOW())
                            ON CONFLICT (person_id) DO UPDATE SET
                                sboeid=%s, party_code=%s, party_label=%s,
                                voter_score=%s, ge_votes=%s, primary_votes=%s,
                                ge_years=%s, primary_years=%s, off_year_years=%s,
                                assembly_district=%s, state_senate_district=%s, congressional_district=%s,
                                county_code=%s, county_name=%s,
                                voter_address=%s, voter_city=%s, voter_zip=%s,
                                match_confidence=%s, updated_at=NOW()
                        """, (
                            # INSERT values
                            pid,
                            voter.get("sboeid","") or "",
                            party_code,
                            PARTY_LABELS.get(party_code, party_code),
                            int(voter.get("voter_score",0) or 0),
                            int(voter.get("ge_votes",0) or voter.get("general_elections_voted",0) or 0),
                            int(voter.get("primary_votes",0) or voter.get("primaries_voted",0) or 0),
                            voter.get("ge_years","") or "",
                            voter.get("primary_years","") or "",
                            voter.get("off_year_years","") or "",
                            voter.get("ad","") or voter.get("assembly_district","") or "",
                            voter.get("sd","") or voter.get("state_senate_district","") or "",
                            voter.get("cd","") or voter.get("congressional_district","") or "",
                            voter.get("county_code","") or "",
                            voter.get("county_name","") or voter.get("county","") or "",
                            voter.get("address","") or "",
                            voter.get("city","") or "",
                            voter.get("zip","") or "",
                            conf,
                            # ON CONFLICT UPDATE values (same order, no person_id)
                            voter.get("sboeid","") or "",
                            party_code,
                            PARTY_LABELS.get(party_code, party_code),
                            int(voter.get("voter_score",0) or 0),
                            int(voter.get("ge_votes",0) or voter.get("general_elections_voted",0) or 0),
                            int(voter.get("primary_votes",0) or voter.get("primaries_voted",0) or 0),
                            voter.get("ge_years","") or "",
                            voter.get("primary_years","") or "",
                            voter.get("off_year_years","") or "",
                            voter.get("ad","") or voter.get("assembly_district","") or "",
                            voter.get("sd","") or voter.get("state_senate_district","") or "",
                            voter.get("cd","") or voter.get("congressional_district","") or "",
                            voter.get("county_code","") or "",
                            voter.get("county_name","") or voter.get("county","") or "",
                            voter.get("address","") or "",
                            voter.get("city","") or "",
                            voter.get("zip","") or "",
                            conf,
                        ))
                    matched += 1
                    batch_n += 1
                    if batch_n >= BATCH:
                        conn.commit()
                        batch_n = 0
                        log.info(f"enrich_voter_data: {i+1}/{len(contacts)}, matched={matched}")
                else:
                    unmatched += 1

            except Exception as e:
                log.warning(f"enrich_voter_data contact error for {name!r}: {e}")
                errors += 1
                try:
                    conn.rollback()  # clear aborted transaction so next insert works
                except Exception:
                    pass

        conn.commit()

    except Exception as e:
        log.error(f"enrich_voter_data_batch outer error: {e}", exc_info=True)
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": str(e), "type": type(e).__name__, "matched_before_error": matched}
    finally:
        try:
            conn.close()
        except Exception:
            pass

    log.info(f"enrich_voter_data complete: matched={matched} unmatched={unmatched} errors={errors}")
    return {
        "status":          "ok",
        "total_processed": len(contacts),
        "matched":         matched,
        "unmatched":       unmatched,
        "errors":          errors,
        "match_rate_pct":  round(matched / max(len(contacts), 1) * 100, 1),
        "message": (
            f"{matched} of {len(contacts)} contacts matched to the voter file. "
            "Next step: call compute_influence_scores."
        ),
    }


# ─── Influence ranking — v1 scoring ──────────────────────────────────────────

def _parse_first_dollar(text: str) -> float:
    """Extract the first $N,NNN amount from a string. Returns 0.0 if none found."""
    m = re.search(r"\$([0-9,]+)", text or "")
    if not m:
        return 0.0
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return 0.0


def compute_influence_scores_batch() -> dict:
    """
    Compute v1 influence scores for every active Pythia contact.
    Five components:
      institutional (35%) — org influence tier + role seniority
      financial     (25%) — campaign donations given/received
      lobbying      (20%) — being targeted by lobbyists
      network       (15%) — total relationship connections
      engagement    (5%)  — voter score / civic participation
    Scores are written to people_influence_scores (upsert — safe to re-run).
    """
    WEIGHTS = dict(institutional=0.35, financial=0.25,
                   lobbying=0.20, network=0.15, engagement=0.05)

    # ── Stage 1: verify tables exist ──────────────────────────────────────────
    try:
        conn = get_db()
    except Exception as e:
        return {"error": f"DB connection failed: {e}"}

    try:
        # ── Stage 2: load all scoring data in one connection ──────────────────
        with conn.cursor() as cur:
            cur.execute("""
                SELECT p.id::text AS person_id, p.full_name,
                    MIN(o.influence_tier) AS best_tier,
                    MAX(CASE WHEN rt.is_decision_maker THEN 1 ELSE 0 END) AS is_decision_maker,
                    MAX(COALESCE(rt.seniority_level, 0)) AS max_seniority,
                    COUNT(DISTINCT CASE WHEN o.influence_tier = 1 THEN o.id END) AS tier1_orgs
                FROM people_person p
                LEFT JOIN people_personorganization po ON po.person_id = p.id AND po.is_current = TRUE
                LEFT JOIN organizations_organization o ON o.id = po.organization_id AND o.influence_tier IS NOT NULL
                LEFT JOIN people_roletype rt ON rt.id = po.role_type_id
                WHERE p.is_active = TRUE
                GROUP BY p.id, p.full_name
            """)
            inst_map = {r["person_id"]: dict(r) for r in cur.fetchall()}

        with conn.cursor() as cur:
            cur.execute("""
                SELECT from_person_id::text AS person_id,
                    COUNT(*) AS donation_count,
                    COUNT(DISTINCT to_person_id) AS unique_recipients,
                    STRING_AGG(notes, '|||') AS all_notes
                FROM people_personrelationship
                WHERE relationship_type = 'Campaign Donor' AND is_active = TRUE
                GROUP BY from_person_id
            """)
            donor_map = {r["person_id"]: dict(r) for r in cur.fetchall()}

        with conn.cursor() as cur:
            cur.execute("""
                SELECT to_person_id::text AS person_id,
                    COUNT(DISTINCT from_person_id) AS unique_donors
                FROM people_personrelationship
                WHERE relationship_type = 'Campaign Donor' AND is_active = TRUE
                GROUP BY to_person_id
            """)
            recvd_map = {r["person_id"]: dict(r) for r in cur.fetchall()}

        with conn.cursor() as cur:
            cur.execute("""
                SELECT to_person_id::text AS person_id,
                    COUNT(*) AS lobbying_count,
                    STRING_AGG(notes, '|||') AS all_notes
                FROM people_personrelationship
                WHERE relationship_type IN ('Lobbyist', 'Lobbying Client') AND is_active = TRUE
                GROUP BY to_person_id
            """)
            lobby_map = {r["person_id"]: dict(r) for r in cur.fetchall()}

        with conn.cursor() as cur:
            cur.execute("""
                SELECT person_id::text, COUNT(*) AS total_connections
                FROM (
                    SELECT from_person_id AS person_id FROM people_personrelationship WHERE is_active = TRUE
                    UNION ALL
                    SELECT to_person_id   FROM people_personrelationship WHERE is_active = TRUE
                ) sides
                JOIN people_person p ON p.id = person_id AND p.is_active = TRUE
                GROUP BY person_id
            """)
            net_map = {r["person_id"]: dict(r) for r in cur.fetchall()}

        try:
            with conn.cursor() as cur:
                cur.execute("SELECT person_id::text, voter_score, ge_votes, primary_votes FROM people_voter_enrichment")
                voter_map = {r["person_id"]: dict(r) for r in cur.fetchall()}
        except Exception:
            voter_map = {}  # table may be empty or not yet populated

        # ── 990 revenue per person (best org they're currently at) ─────────
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT po.person_id::text,
                        GREATEST(
                            MAX(COALESCE(n.total_revenue, 0)),
                            MAX(COALESCE(u.total_receipts, 0))
                        ) AS best_financial_metric
                    FROM people_personorganization po
                    LEFT JOIN organizations_990_data n
                        ON n.organization_id = po.organization_id
                        AND n.match_confidence > 70
                    LEFT JOIN organizations_union_data u
                        ON u.organization_id = po.organization_id
                        AND u.match_confidence > 70
                    WHERE po.is_current = TRUE
                      AND (
                          (n.total_revenue IS NOT NULL AND n.total_revenue > 0)
                          OR (u.total_receipts IS NOT NULL AND u.total_receipts > 0)
                      )
                    GROUP BY po.person_id
                """)
                revenue_990_map = {r["person_id"]: int(r["best_financial_metric"])
                                   for r in cur.fetchall()}
        except Exception:
            revenue_990_map = {}  # tables may not exist yet

        with conn.cursor() as cur:
            cur.execute("SELECT id::text AS person_id, full_name FROM people_person WHERE is_active = TRUE")
            all_people = list(cur.fetchall())

        log.info(f"compute_influence_scores: loaded data for {len(all_people)} contacts")

        # ── Stage 3: compute scores in Python ─────────────────────────────────
        scored = []
        for person in all_people:
            pid  = person["person_id"]
            name = person["full_name"] or ""

            inst = inst_map.get(pid, {})
            don  = donor_map.get(pid, {})
            recv = recvd_map.get(pid, {})
            lob  = lobby_map.get(pid, {})
            net  = net_map.get(pid, {})
            vot  = voter_map.get(pid)

            # ── Institutional score ────────────────────────────────────────
            tier      = inst.get("best_tier")
            tier_base = {1: 80, 2: 50, 3: 25}.get(tier, 5) if tier else 5
            # 990 revenue-based score — takes precedence over manual tier if higher
            revenue_990    = revenue_990_map.get(pid)
            revenue_score  = _revenue_to_institutional_score(revenue_990)
            institutional_base = max(float(tier_base), revenue_score)
            seniority_bonus   = min(15, int(inst.get("max_seniority", 0) or 0) * 2)
            decision_bonus    = 15 if inst.get("is_decision_maker") else 0
            multi_tier1_bonus = min(10, max(0, int(inst.get("tier1_orgs", 0) or 0) - 1) * 5)
            institutional_score = min(100.0, institutional_base + seniority_bonus + decision_bonus + multi_tier1_bonus)

            total_donated = 0.0
            for chunk in (don.get("all_notes") or "").split("|||"):
                total_donated += _parse_first_dollar(chunk)
            amount_score    = min(60.0, _math.log10(total_donated + 1) * 15)
            breadth_score   = min(25.0, int(don.get("unique_recipients", 0) or 0) * 5)
            recipient_score = min(15.0, int(recv.get("unique_donors", 0) or 0) * 2)
            financial_score = min(100.0, amount_score + breadth_score + recipient_score)

            total_comp = 0.0
            for chunk in (lob.get("all_notes") or "").split("|||"):
                m = re.search(r"Compensation:\s*\$([0-9,]+)", chunk)
                try:
                    total_comp += float(m.group(1).replace(",", "")) if m else _parse_first_dollar(chunk)
                except (ValueError, AttributeError):
                    pass
            lob_count_score = min(50.0, int(lob.get("lobbying_count", 0) or 0) * 5)
            lob_comp_score  = min(50.0, _math.log10(total_comp + 1) * 12) if total_comp > 0 else 0.0
            lobbying_score  = min(100.0, lob_count_score + lob_comp_score)

            total_conn    = int(net.get("total_connections", 0) or 0)
            network_score = min(100.0, _math.log10(total_conn + 1) * 30) if total_conn > 0 else 0.0

            if vot:
                vs    = int(vot.get("voter_score", 0) or 0)
                ge_v  = int(vot.get("ge_votes", 0) or 0)
                pri_v = int(vot.get("primary_votes", 0) or 0)
                engagement_score = min(100.0, min(50.0, (vs/30.0)*50) + min(25.0, ge_v*2.0) + min(25.0, pri_v*3.0))
            else:
                engagement_score = 0.0

            base    = (WEIGHTS["institutional"] * institutional_score + WEIGHTS["financial"] * financial_score
                       + WEIGHTS["lobbying"] * lobbying_score + WEIGHTS["network"] * network_score
                       + WEIGHTS["engagement"] * engagement_score)
            strong  = sum(1 for s in [institutional_score, financial_score, lobbying_score, network_score] if s > 60)
            composite_score = min(100.0, base * (1.0 + 0.15 * max(0, strong - 1)))

            breakdown = {
                "institutional": round(institutional_score, 2),
                "financial":     round(financial_score, 2),
                "lobbying":      round(lobbying_score, 2),
                "network":       round(network_score, 2),
                "engagement":    round(engagement_score, 2),
                "raw": {
                    "best_tier":         tier,
                    "is_decision_maker": bool(inst.get("is_decision_maker")),
                    "revenue_990":       revenue_990,
                    "total_donated":     round(total_donated, 2),
                    "donation_count":    int(don.get("donation_count", 0) or 0),
                    "unique_donors_in":  int(recv.get("unique_donors", 0) or 0),
                    "lobbying_filings":  int(lob.get("lobbying_count", 0) or 0),
                    "total_connections": total_conn,
                    "voter_score":       int(vot.get("voter_score", 0) or 0) if vot else None,
                },
            }
            scored.append({
                "person_id": pid, "name": name,
                "institutional_score": round(institutional_score, 2),
                "financial_score":     round(financial_score, 2),
                "lobbying_score":      round(lobbying_score, 2),
                "network_score":       round(network_score, 2),
                "engagement_score":    round(engagement_score, 2),
                "composite_score":     round(composite_score, 2),
                "breakdown":           breakdown,
            })

        # ── Stage 4: bulk upsert all scores in one round trip ─────────────────────────
        rows = [
            (
                r["person_id"],
                r["institutional_score"], r["financial_score"],
                r["lobbying_score"],      r["network_score"],
                r["engagement_score"],    r["composite_score"],
                json.dumps(r["breakdown"]),
            )
            for r in scored
        ]
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO people_influence_scores (
                    person_id,
                    institutional_score, financial_score, lobbying_score,
                    network_score, engagement_score, composite_score,
                    component_breakdown, algorithm_version, computed_at
                ) VALUES %s
                ON CONFLICT (person_id) DO UPDATE SET
                    institutional_score = EXCLUDED.institutional_score,
                    financial_score     = EXCLUDED.financial_score,
                    lobbying_score      = EXCLUDED.lobbying_score,
                    network_score       = EXCLUDED.network_score,
                    engagement_score    = EXCLUDED.engagement_score,
                    composite_score     = EXCLUDED.composite_score,
                    component_breakdown = EXCLUDED.component_breakdown,
                    algorithm_version   = EXCLUDED.algorithm_version,
                    computed_at         = NOW()
                """,
                rows,
                template="(%s,%s,%s,%s,%s,%s,%s,%s,'v1.0',NOW())",
                page_size=500,
            )
        conn.commit()
        log.info(f"compute_influence_scores: bulk upserted {len(scored)} rows")

        top10 = sorted(scored, key=lambda x: -x["composite_score"])[:10]
        log.info(f"compute_influence_scores complete: {len(scored)} contacts scored")
        return {
            "status":          "ok",
            "contacts_scored": len(scored),
            "algorithm":       "v1.0",
            "weights":         WEIGHTS,
            "top_10_preview": [
                {"name": r["name"], "composite": r["composite_score"],
                 "inst": r["institutional_score"], "fin": r["financial_score"],
                 "lob":  r["lobbying_score"],       "net": r["network_score"],
                 "eng":  r["engagement_score"]}
                for r in top10
            ],
            "next_step": "Call rank_influential_people to query results with optional geographic filters.",
        }

    except Exception as e:
        log.error(f"compute_influence_scores_batch failed: {e}", exc_info=True)
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": str(e), "type": type(e).__name__}
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ─── Influence ranking — query ────────────────────────────────────────────────

_BOROUGH_COUNTY = {
    "manhattan": "31", "new york": "31",
    "brooklyn":  "24", "kings":    "24",
    "queens":    "41",
    "bronx":     "03",
    "staten island": "43", "richmond": "43",
}

def rank_influential_people(
    zip_code: str               = "",
    borough: str                = "",
    assembly_district: str      = "",
    state_senate_district: str  = "",
    congressional_district: str = "",
    limit: int                  = 25,
    min_score: float            = 0.0,
) -> list[dict]:
    """
    Query stored influence scores with optional geographic and score filters.
    Geographic filters use voter-file addresses from people_voter_enrichment.
    Returns ranked list, most influential first.
    """
    _, pis_exists = _tables_exist()
    if not pis_exists:
        return [{"error": "Table people_influence_scores not found. Run setup_influence_tables first."}]

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM people_influence_scores")
            if (cur.fetchone() or {}).get("n", 0) == 0:
                return [{"error": "No scores yet. Run compute_influence_scores first."}]

    has_geo  = any([zip_code, borough, assembly_district,
                    state_senate_district, congressional_district])
    filters  = []
    params: list = []

    if zip_code:
        filters.append("pve.voter_zip = %s")
        params.append(zip_code.strip())
    if borough:
        cc = _BOROUGH_COUNTY.get(borough.lower().strip())
        if cc:
            filters.append("pve.county_code = %s")
            params.append(cc)
    if assembly_district:
        filters.append("CAST(LTRIM(COALESCE(pve.assembly_district,'0'),'0') AS TEXT) = %s")
        params.append(str(assembly_district).lstrip("0") or "0")
    if state_senate_district:
        filters.append("CAST(LTRIM(COALESCE(pve.state_senate_district,'0'),'0') AS TEXT) = %s")
        params.append(str(state_senate_district).lstrip("0") or "0")
    if congressional_district:
        filters.append("CAST(LTRIM(COALESCE(pve.congressional_district,'0'),'0') AS TEXT) = %s")
        params.append(str(congressional_district).lstrip("0") or "0")
    if min_score > 0:
        filters.append("pis.composite_score >= %s")
        params.append(min_score)

    # For geo queries use INNER JOIN so unmatched contacts are excluded.
    # For citywide (no geo filter) use LEFT JOIN so everyone appears.
    voter_join  = "JOIN" if has_geo else "LEFT JOIN"
    where_extra = ("AND " + " AND ".join(filters)) if filters else ""

    params.append(limit)

    sql = f"""
        WITH ranked AS (
            SELECT
                p.id::text                   AS person_id,
                p.full_name,
                pis.composite_score,
                pis.institutional_score,
                pis.financial_score,
                pis.lobbying_score,
                pis.network_score,
                pis.engagement_score,
                pis.component_breakdown::text AS breakdown_json,
                pis.computed_at,
                pve.voter_zip,
                pve.assembly_district,
                pve.state_senate_district,
                pve.congressional_district,
                pve.county_name,
                pve.party_label,
                pve.voter_score,
                pve.voter_address,
                pve.voter_city
            FROM people_influence_scores pis
            JOIN people_person p ON p.id = pis.person_id AND p.is_active = TRUE
            {voter_join} people_voter_enrichment pve ON pve.person_id = pis.person_id
            WHERE 1=1 {where_extra}
            ORDER BY pis.composite_score DESC
            LIMIT %s
        )
        SELECT
            r.*,
            STRING_AGG(DISTINCT o.name,     ', ' ORDER BY o.name) AS orgs,
            MIN(o.influence_tier)                                   AS best_tier,
            STRING_AGG(DISTINCT po.job_title, ', ')                 AS titles
        FROM ranked r
        LEFT JOIN people_personorganization po
            ON po.person_id::text = r.person_id AND po.is_current = TRUE
        LEFT JOIN organizations_organization o ON o.id = po.organization_id
        GROUP BY
            r.person_id, r.full_name, r.composite_score,
            r.institutional_score, r.financial_score, r.lobbying_score,
            r.network_score, r.engagement_score, r.breakdown_json,
            r.computed_at, r.voter_zip, r.assembly_district,
            r.state_senate_district, r.congressional_district,
            r.county_name, r.party_label, r.voter_score,
            r.voter_address, r.voter_city
        ORDER BY r.composite_score DESC
    """

    results = []
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for row in cur.fetchall():
                d = dict(row)
                # Parse JSON breakdown back to dict for clean output
                if d.get("breakdown_json"):
                    try:
                        d["component_breakdown"] = json.loads(d["breakdown_json"])
                    except Exception:
                        d["component_breakdown"] = {}
                    del d["breakdown_json"]
                results.append(d)

    return results


# ─── IRS 990 data — ProPublica Nonprofit Explorer ────────────────────────────

# Keywords that identify government or for-profit entities that don't file 990s.
# These are skipped during 990 fetching to avoid wasted API calls.
_GOVT_KEYWORDS = (
    "u.s. senate", "us senate", "united states senate",
    "u.s. house", "us house", "house of representatives",
    "nyc ", "city of new york", "new york city",
    "office of the mayor", "office of the new york city mayor",
    "mayor's office", "borough president",
    "city council", "city planning",
    "state of new york", "nys ", "new york state",
    "governor", "state comptroller", "attorney general",
    "department of ", "dept. of ", "office of ",
    "nypd", "fdny", "police department", "fire department",
    "federal ", "congress", "white house",
    "supreme court", "district court",
)

def _is_government_entity(org_name: str) -> bool:
    """Return True if org is a government body or for-profit that won't have a 990."""
    n = org_name.lower().strip()
    return any(kw in n for kw in _GOVT_KEYWORDS)


def _revenue_to_institutional_score(revenue: int | None) -> float:
    """
    Convert annual nonprofit revenue (from IRS 990) to an institutional score (0-100).
    Uses a log-like scale so score differences between small orgs are meaningful.
    """
    if not revenue or revenue <= 0:
        return 0.0
    if revenue >= 1_000_000_000:   # $1B+
        return 95.0
    if revenue >= 500_000_000:     # $500M – $1B
        return 88.0
    if revenue >= 100_000_000:     # $100M – $500M
        return 80.0
    if revenue >= 50_000_000:      # $50M – $100M
        return 70.0
    if revenue >= 10_000_000:      # $10M – $50M
        return 55.0
    if revenue >= 5_000_000:       # $5M – $10M
        return 40.0
    if revenue >= 1_000_000:       # $1M – $5M
        return 25.0
    return 10.0


def _search_propublica_990(org_name: str) -> dict | None:
    """
    Search ProPublica Nonprofit Explorer API for an organization and return
    its most recent 990 filing data (revenue, assets, NTEE code, etc.).
    Returns None if no confident match is found.
    Makes at most 2 API calls: one search, one detail fetch.
    """
    if not org_name or not org_name.strip():
        return None

    # Strip legal suffixes that confuse the search
    clean = org_name.strip()
    for sfx in (", Inc.", ", Inc", ", LLC", ", Corp.", ", Corp",
                " Foundation", " Institute", " Association"):
        if clean.endswith(sfx):
            clean = clean[: -len(sfx)].strip()

    try:
        # ── Search (NY state first, broader if needed) ─────────────────────
        search_url = "https://projects.propublica.org/nonprofits/api/v2/search.json"
        resp = requests.get(search_url, params={"q": clean, "state[id]": "NY"},
                            timeout=15, headers={"User-Agent": "research/public-data"})
        candidates = resp.json().get("organizations", []) if resp.ok else []

        if not candidates:
            resp2 = requests.get(search_url, params={"q": clean},
                                 timeout=15, headers={"User-Agent": "research/public-data"})
            candidates = resp2.json().get("organizations", []) if resp2.ok else []

        if not candidates:
            return None

        # ── Fuzzy-match to pick best candidate ─────────────────────────────
        best, best_score = None, 0
        for c in candidates[:10]:
            score = fuzz.token_sort_ratio(normalize(org_name), normalize(c.get("name", "")))
            if score > best_score:
                best_score, best = score, c

        if best_score < 75 or not best:
            return None

        # ── Fetch detail for the matched EIN ───────────────────────────────
        ein = str(best.get("ein", "")).replace("-", "").strip()
        if not ein:
            return None

        import time as _time
        _time.sleep(0.3)   # polite pause between search and detail call

        det = requests.get(
            f"https://projects.propublica.org/nonprofits/api/v2/organizations/{ein}.json",
            timeout=15, headers={"User-Agent": "research/public-data"},
        )
        if not det.ok:
            return None

        data     = det.json()
        org_info = data.get("organization", {})
        filings  = [f for f in data.get("filings_with_data", [])
                    if (f.get("totrevenue") or 0) > 0]

        if not filings:
            return None

        latest = max(filings, key=lambda f: f.get("tax_prd_yr", 0))

        return {
            "ein":            ein,
            "legal_name":     org_info.get("name") or best.get("name", ""),
            "total_revenue":  int(latest.get("totrevenue", 0) or 0),
            "total_assets":   int(latest.get("totassetsend", 0) or latest.get("totassets", 0) or 0),
            "total_expenses": int(latest.get("totfuncexpns", 0) or 0),
            "num_employees":  int(latest.get("totemploy", 0) or 0),
            "ntee_code":      str(org_info.get("ntee_code") or best.get("ntee_code") or ""),
            "fiscal_year":    int(latest.get("tax_prd_yr", 0) or 0),
            "match_confidence": best_score,
        }

    except Exception as e:
        log.warning(f"ProPublica 990 lookup error for {org_name!r}: {e}")
        return None


def fetch_990_data_batch(limit: int = 50, force: bool = False) -> dict:
    """
    Fetch IRS 990 data for Pythia organizations via ProPublica Nonprofit Explorer.
    Stores annual revenue, assets, and NTEE category for each matched org in
    organizations_990_data. This data is used by compute_influence_scores to
    objectively weight institutional authority by org budget size.

    By default processes only orgs with no existing 990 record (safe to re-run).
    Set force=True to re-fetch already-processed orgs.
    Limit defaults to 50 per call (~40-50s); run multiple times for full coverage.
    Government entities and for-profits are automatically skipped.
    """
    # ── Check table exists ────────────────────────────────────────────────────
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'organizations_990_data' AND table_schema = 'public'
            """)
            if not cur.fetchone():
                conn.close()
                return {"error": "Table organizations_990_data not found. Run setup_influence_tables first."}
        conn.close()
    except Exception as e:
        return {"error": f"DB connection error: {e}"}

    # ── Load orgs to process ──────────────────────────────────────────────────
    try:
        conn = get_db()
        with conn.cursor() as cur:
            if force:
                cur.execute("""
                    SELECT id::text, name, industry
                    FROM organizations_organization
                    WHERE name IS NOT NULL AND name != ''
                    ORDER BY influence_tier NULLS LAST, name
                """)
            else:
                cur.execute("""
                    SELECT o.id::text, o.name, o.industry
                    FROM organizations_organization o
                    WHERE o.name IS NOT NULL AND o.name != ''
                      AND NOT EXISTS (
                          SELECT 1 FROM organizations_990_data n
                          WHERE n.organization_id = o.id
                      )
                    ORDER BY o.influence_tier NULLS LAST, o.name
                """)
            orgs = list(cur.fetchall())
        conn.close()
    except Exception as e:
        return {"error": f"Failed to load orgs: {e}"}

    if limit and limit > 0:
        orgs = orgs[:limit]

    if not orgs:
        return {
            "status": "ok",
            "message": "All organizations already have 990 data. Use force=true to re-fetch.",
            "matched": 0, "skipped_government": 0, "no_match": 0, "errors": 0,
            "remaining_unprocessed": 0,
        }

    matched, skipped_gov, no_match, errors = 0, 0, 0, 0

    import time as _time

    try:
        conn = get_db()
        for i, org in enumerate(orgs):
            org_id   = org["id"]
            org_name = (org["name"] or "").strip()

            if not org_name:
                no_match += 1
                continue

            # Skip government entities and for-profits
            if _is_government_entity(org_name):
                skipped_gov += 1
                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO organizations_990_data
                                (organization_id, match_confidence, fetched_at, updated_at)
                            VALUES (%s, -1, NOW(), NOW())
                            ON CONFLICT (organization_id) DO NOTHING
                        """, (org_id,))
                    conn.commit()
                except Exception:
                    pass
                continue

            # Search ProPublica (makes 1-2 HTTP calls)
            result = _search_propublica_990(org_name)

            try:
                with conn.cursor() as cur:
                    if result and result.get("total_revenue", 0) > 0:
                        cur.execute("""
                            INSERT INTO organizations_990_data (
                                organization_id, ein, legal_name,
                                total_revenue, total_assets, total_expenses,
                                num_employees, ntee_code, fiscal_year,
                                match_confidence, fetched_at, updated_at
                            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
                            ON CONFLICT (organization_id) DO UPDATE SET
                                ein            = EXCLUDED.ein,
                                legal_name     = EXCLUDED.legal_name,
                                total_revenue  = EXCLUDED.total_revenue,
                                total_assets   = EXCLUDED.total_assets,
                                total_expenses = EXCLUDED.total_expenses,
                                num_employees  = EXCLUDED.num_employees,
                                ntee_code      = EXCLUDED.ntee_code,
                                fiscal_year    = EXCLUDED.fiscal_year,
                                match_confidence = EXCLUDED.match_confidence,
                                updated_at     = NOW()
                        """, (
                            org_id,
                            result["ein"],
                            result["legal_name"],
                            result["total_revenue"],
                            result["total_assets"],
                            result["total_expenses"],
                            result["num_employees"],
                            result["ntee_code"],
                            result["fiscal_year"],
                            result["match_confidence"],
                        ))
                        matched += 1
                    else:
                        # Record "tried but no match" to skip on future runs
                        cur.execute("""
                            INSERT INTO organizations_990_data
                                (organization_id, match_confidence, fetched_at, updated_at)
                            VALUES (%s, 0, NOW(), NOW())
                            ON CONFLICT (organization_id) DO NOTHING
                        """, (org_id,))
                        no_match += 1
                conn.commit()

            except Exception as e:
                log.warning(f"fetch_990_data DB write error for {org_name!r}: {e}")
                errors += 1
                try:
                    conn.rollback()
                except Exception:
                    pass

            # Polite rate limiting — ~0.5s between orgs
            if i < len(orgs) - 1:
                _time.sleep(0.5)

            if (i + 1) % 10 == 0:
                log.info(f"fetch_990_data: {i+1}/{len(orgs)} processed, "
                         f"matched={matched} skipped={skipped_gov} no_match={no_match}")

    except Exception as e:
        log.error(f"fetch_990_data_batch outer error: {e}", exc_info=True)
        return {"error": str(e), "type": type(e).__name__, "matched_before_error": matched}
    finally:
        try:
            conn.close()
        except Exception:
            pass

    # Count how many orgs are still unprocessed
    try:
        c2 = get_db()
        with c2.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS n FROM organizations_organization o
                WHERE o.name IS NOT NULL AND o.name != ''
                  AND NOT EXISTS (SELECT 1 FROM organizations_990_data n WHERE n.organization_id = o.id)
            """)
            row = cur.fetchone()
            remaining = int(row["n"]) if row else 0
        c2.close()
    except Exception:
        remaining = -1

    nonprofit_orgs = len(orgs) - skipped_gov
    return {
        "status":              "ok",
        "total_processed":     len(orgs),
        "matched":             matched,
        "skipped_government":  skipped_gov,
        "no_match_found":      no_match,
        "errors":              errors,
        "match_rate_pct":      round(matched / max(nonprofit_orgs, 1) * 100, 1),
        "remaining_unprocessed": remaining,
        "message": (
            f"Matched {matched} orgs to 990 data. "
            + (f"{remaining} orgs still unprocessed — run again to continue. " if remaining > 0 else "All orgs processed. ")
            + "Run compute_influence_scores to update rankings."
        ),
    }


# ─── 990 background fetch (runs inside Railway, immune to MCP session issues) ─

import threading as _threading
from datetime import datetime, timezone as _tz

_990_bg_lock   = _threading.Lock()
_990_bg_status: dict = {
    "running":     False,
    "started_at":  None,
    "finished_at": None,
    "processed":   0,
    "matched":     0,
    "skipped":     0,
    "no_match":    0,
    "errors":      0,
    "remaining":   -1,
    "last_error":  None,
}


def _fetch_990_background_worker() -> None:
    """
    Long-running background thread that exhausts all remaining orgs.
    Calls fetch_990_data_batch(100) in a loop until nothing is left.
    Runs inside the Railway process — no MCP connection required.
    """
    import time as _t
    global _990_bg_status

    _990_bg_status["started_at"]  = datetime.now(_tz.utc).isoformat()
    _990_bg_status["finished_at"] = None
    _990_bg_status["processed"]   = 0
    _990_bg_status["matched"]     = 0
    _990_bg_status["skipped"]     = 0
    _990_bg_status["no_match"]    = 0
    _990_bg_status["errors"]      = 0
    _990_bg_status["remaining"]   = -1
    _990_bg_status["last_error"]  = None

    log.info("990 background worker started")
    try:
        while _990_bg_status["running"]:
            result = fetch_990_data_batch(limit=100, force=False)

            if result.get("error"):
                _990_bg_status["last_error"] = result["error"]
                log.error(f"990 bg worker batch error: {result['error']}")
                break

            _990_bg_status["processed"] += result.get("total_processed", 0)
            _990_bg_status["matched"]   += result.get("matched", 0)
            _990_bg_status["skipped"]   += result.get("skipped_government", 0)
            _990_bg_status["no_match"]  += result.get("no_match_found", 0)
            _990_bg_status["errors"]    += result.get("errors", 0)
            _990_bg_status["remaining"]  = result.get("remaining_unprocessed", 0)

            log.info(
                f"990 bg worker: cumulative processed={_990_bg_status['processed']} "
                f"matched={_990_bg_status['matched']} remaining={_990_bg_status['remaining']}"
            )

            if _990_bg_status["remaining"] == 0:
                log.info("990 bg worker: all orgs processed — done")
                break

            _t.sleep(2)  # Brief pause between batches

    except Exception as e:
        log.error(f"990 background worker fatal error: {e}", exc_info=True)
        _990_bg_status["last_error"] = str(e)
    finally:
        _990_bg_status["running"]     = False
        _990_bg_status["finished_at"] = datetime.now(_tz.utc).isoformat()
        log.info(f"990 background worker finished: {_990_bg_status}")


def start_990_background_fetch() -> dict:
    """
    Start fetching IRS 990 data for ALL remaining orgs in a background thread.
    Returns immediately — the fetch runs inside the Railway server process for
    the next 15-25 minutes, completely independent of the MCP session.
    Call get_990_fetch_status to monitor progress.
    """
    global _990_bg_status

    with _990_bg_lock:
        if _990_bg_status["running"]:
            return {
                "status":  "already_running",
                "progress": dict(_990_bg_status),
                "message": "Background fetch is already running. Call get_990_fetch_status to check progress.",
            }

        _990_bg_status["running"] = True
        t = _threading.Thread(
            target=_fetch_990_background_worker,
            daemon=True,
            name="fetch-990-bg",
        )
        t.start()

    log.info("990 background fetch thread started")
    return {
        "status":  "started",
        "message": (
            "IRS 990 fetch running in the background inside Railway. "
            "All remaining organizations will be processed over the next 15-25 minutes. "
            "This is completely independent of the MCP session — no timeouts, no reconnection issues. "
            "Call get_990_fetch_status to monitor progress, or check the database directly: "
            "SELECT COUNT(*) FROM organizations_990_data WHERE total_revenue > 0"
        ),
    }


def get_990_fetch_status() -> dict:
    """Return current status of the background 990 fetch."""
    status = dict(_990_bg_status)

    # Also pull live counts from DB for accuracy
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*)                                                   AS total_processed,
                    COUNT(CASE WHEN total_revenue > 0 THEN 1 END)             AS matched,
                    COUNT(CASE WHEN match_confidence = -1 THEN 1 END)         AS skipped_govt,
                    COUNT(CASE WHEN match_confidence = 0  THEN 1 END)         AS no_match
                FROM organizations_990_data
            """)
            row = cur.fetchone()
            if row:
                status["db_total_processed"] = int(row["total_processed"])
                status["db_matched"]         = int(row["matched"])
                status["db_skipped_govt"]    = int(row["skipped_govt"])
                status["db_no_match"]        = int(row["no_match"])

            cur.execute("""
                SELECT COUNT(*) AS n FROM organizations_organization o
                WHERE o.name IS NOT NULL AND o.name != ''
                  AND NOT EXISTS (SELECT 1 FROM organizations_990_data n WHERE n.organization_id = o.id)
            """)
            row2 = cur.fetchone()
            status["db_remaining_unprocessed"] = int(row2["n"]) if row2 else -1
        conn.close()
    except Exception as e:
        status["db_error"] = str(e)

    return status



# ─── Union LM-2 data — DOL developer API (api.dol.gov/V1/ELORS) ─────────────
#
# The DOL OLMS public-facing web app (olmsapps.dol.gov/olpdr) has no usable
# REST API — all endpoints return 404 or require the Angular app session.
# The official DOL developer API is the correct access path.
# Requires DOL_API_KEY env var (free key at developer.dol.gov).

_DOL_API_BASE  = "https://api.dol.gov/V1"
_DOL_API_KEY   = None   # loaded lazily from env

# In-memory cache: all NY LM-2 union records, loaded once per process lifetime
_olms_lm2_cache:        list[dict] = []
_olms_lm2_cache_loaded: bool       = False


def _get_dol_key() -> str | None:
    global _DOL_API_KEY
    if _DOL_API_KEY is None:
        _DOL_API_KEY = os.environ.get("DOL_API_KEY", "").strip() or None
    return _DOL_API_KEY


def _load_olms_lm2_data(force: bool = False) -> list[dict]:
    """
    Download all NY-state LM-2 union records from the DOL developer API and
    cache them in memory.  Subsequent calls return the cache instantly.
    Typical NY dataset: ~500-1 000 records, loads in 2-5 seconds.
    """
    global _olms_lm2_cache, _olms_lm2_cache_loaded

    if _olms_lm2_cache_loaded and not force:
        return _olms_lm2_cache

    key = _get_dol_key()
    if not key:
        log.warning("DOL_API_KEY not set — union LM-2 data unavailable")
        return []

    all_records: list[dict] = []
    page_size   = 200
    skip        = 0

    # Try both state field name variants in one pass; stop on first success
    state_filters = ["State eq 'NY'", "STATE eq 'NY'", "mailSt eq 'NY'"]
    working_filter: str | None = None

    for sf in state_filters:
        try:
            resp = requests.get(
                f"{_DOL_API_BASE}/ELORS/lm2FinalData",
                params={"KEY": key, "$filter": sf, "$top": 1, "$format": "json"},
                timeout=20,
            )
            log.debug(f"DOL API probe filter={sf!r} status={resp.status_code} "
                      f"body[:200]={resp.text[:200]!r}")
            if resp.ok:
                body = resp.json()
                records = _dol_extract_records(body)
                if records is not None:   # empty list is OK — filter worked
                    working_filter = sf
                    break
        except Exception as exc:
            log.debug(f"DOL API filter probe {sf!r} failed: {exc}")

    if working_filter is None:
        log.error("DOL API: could not determine correct state filter field name")
        return []

    # Full paginated download
    while True:
        try:
            resp = requests.get(
                f"{_DOL_API_BASE}/ELORS/lm2FinalData",
                params={
                    "KEY":     key,
                    "$filter": working_filter,
                    "$top":    page_size,
                    "$skip":   skip,
                    "$format": "json",
                },
                timeout=30,
            )
            if not resp.ok:
                log.error(f"DOL API error {resp.status_code}: {resp.text[:200]}")
                break
            records = _dol_extract_records(resp.json())
            if not records:
                break
            all_records.extend(records)
            log.info(f"DOL API: {len(all_records)} NY LM-2 records loaded so far")
            if len(records) < page_size:
                break
            skip += page_size
        except Exception as exc:
            log.error(f"DOL API pagination error at skip={skip}: {exc}")
            break

    _olms_lm2_cache        = all_records
    _olms_lm2_cache_loaded = True
    log.info(f"DOL API: cached {len(all_records)} NY LM-2 records")
    return all_records


def _dol_extract_records(body: dict | list) -> list[dict] | None:
    """
    Extract the list of records from a DOL API response.
    Returns None if the body doesn't look like a valid API response,
    empty list if the filter worked but returned no rows.
    """
    if isinstance(body, list):
        return body
    # OData v3: {"d": {"results": [...]}}
    if "d" in body and isinstance(body["d"], dict):
        return body["d"].get("results", [])
    # OData v4: {"value": [...]}
    if "value" in body and isinstance(body["value"], list):
        return body["value"]
    # Flat dict with single record
    if any(k in body for k in ("UNION_NAME", "union_name", "UnionName", "NAME")):
        return [body]
    return None


def _looks_like_union(org_name: str) -> bool:
    """Return True if the org name suggests a labor union."""
    kws = (" local ", "local #", "local no.", "union", "afl-cio", " seiu", " cwa ",
           " ibew", " ufcw", " uaw ", " ibt ", " afscme", " aft ", "teamsters",
           "laborers", "ironworkers", "carpenters", "plumbers", "electricians",
           "painters", " iatse", " pba ", "police benevolent", "firefighters",
           " iaff", " 1199", "district council", "transport workers", " twu ",
           " dc37", " dc 37", "building service", "hotel employees",)
    n = org_name.lower()
    return any(kw in n for kw in kws)


def _int_from_dict(d: dict, *keys: str) -> int:
    """Extract the first non-zero int from a dict by trying multiple keys."""
    for k in keys:
        v = d.get(k)
        if v:
            try:
                return int(str(v).replace(",", "").strip())
            except (ValueError, TypeError):
                pass
    return 0


def _search_olms_lm2(org_name: str) -> dict | None:
    """
    Search the in-memory DOL LM-2 cache for a union matching org_name.
    Loads the cache from api.dol.gov on first call.
    Returns financial data dict or None if no confident match found.
    """
    records = _load_olms_lm2_data()
    if not records:
        return None

    best, best_score = None, 0
    for r in records:
        # DOL API field names vary slightly by dataset version — try all
        name = (r.get("UNION_NAME") or r.get("union_name") or r.get("UnionName")
                or r.get("NAME") or r.get("name") or "")
        score = fuzz.token_sort_ratio(normalize(org_name), normalize(name))
        if score > best_score:
            best_score, best = score, r

    if best_score < 75 or not best:
        return None

    def _i(*keys: str) -> int:
        return _int_from_dict(best, *keys)

    # Receipts — try every plausible field name
    receipts = _i("TOTRECEIPTS", "TOTALRECEIPTS", "total_receipts",
                  "TotalReceipts", "receipts", "RECEIPTS")
    assets   = _i("TOTASSETS", "TOTALASSETS", "total_assets",
                  "TotalAssets", "assets", "ASSETS")
    disburse = _i("TOTDISBURSE", "TOTALDISBURSEMENTS", "total_disbursements",
                  "TotalDisbursements", "disbursements", "DISBURSEMENTS")
    members  = _i("MEMBERS_AT_END_OF_PERIOD", "TOTMEMBERS", "total_members",
                  "TotalMembers", "members", "MEMBERS", "TOTALMEMBERS",
                  "MembersAtEndOfPeriod")
    year     = _i("PERIOD_OF_REPT", "YEAR", "year", "RPTYR",
                  "period_of_report", "PeriodOfReport")

    file_num  = str(best.get("FILE_NUM") or best.get("file_num")
                    or best.get("FileNum") or best.get("FILE_NUMBER") or "").strip()
    affil     = str(best.get("AFFILIATION") or best.get("affiliation")
                    or best.get("AFF_ABBR") or best.get("affAbbr") or "").strip()
    city      = str(best.get("CITY") or best.get("city") or "").strip()
    state     = str(best.get("STATE") or best.get("state")
                    or best.get("MAILST") or "NY").strip()
    form_type = str(best.get("FORM_TYPE") or best.get("form_type")
                    or best.get("FormType") or "LM2").strip()
    union_name = str(best.get("UNION_NAME") or best.get("union_name")
                     or best.get("UnionName") or best.get("NAME") or "").strip()

    return {
        "file_number":         file_num,
        "legal_name":          union_name,
        "affiliation":         affil,
        "state":               state,
        "city":                city,
        "total_receipts":      receipts,
        "total_disbursements": disburse,
        "total_assets":        assets,
        "membership_count":    members,
        "report_year":         year,
        "form_type":           form_type,
        "match_confidence":    best_score,
    }


def test_union_lookup(org_name: str) -> dict:
    """
    Diagnostic: load NY LM-2 data from DOL API and attempt a union lookup.
    Returns the raw first record from the API (so you can see field names),
    plus the match result for the given org name.
    Call this after setting DOL_API_KEY to confirm the API is working.
    """
    key = _get_dol_key()
    if not key:
        return {"error": "DOL_API_KEY environment variable not set on Railway"}

    # Show raw API response for one record so we can confirm field names
    raw_sample = None
    api_status = None
    try:
        resp = requests.get(
            f"{_DOL_API_BASE}/ELORS/lm2FinalData",
            params={"KEY": key, "$top": 1, "$format": "json"},
            timeout=15,
        )
        api_status = resp.status_code
        if resp.ok:
            records = _dol_extract_records(resp.json())
            raw_sample = records[0] if records else None
    except Exception as exc:
        api_status = str(exc)

    # Full lookup using cache
    cache = _load_olms_lm2_data()
    result = _search_olms_lm2(org_name)

    return {
        "query":           org_name,
        "api_status":      api_status,
        "cache_size":      len(cache),
        "raw_sample_keys": list(raw_sample.keys()) if raw_sample else None,
        "raw_sample":      raw_sample,
        "found":           result is not None,
        "receipts":        result.get("total_receipts", 0) if result else 0,
        "result":          result,
    }



def fetch_union_data_batch(limit: int = 50, force: bool = False) -> dict:
    """
    Fetch DOL OLMS LM-2/LM-3 union data for Pythia organizations.
    Skips government entities and orgs already matched via IRS 990.
    Safe to re-run; use force=True to re-fetch already-processed orgs.
    """
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("""SELECT 1 FROM information_schema.tables
                           WHERE table_name='organizations_union_data'
                           AND table_schema='public'""")
            if not cur.fetchone():
                conn.close()
                return {"error": "Run setup_influence_tables first."}
        conn.close()
    except Exception as e:
        return {"error": str(e)}

    try:
        conn = get_db()
        with conn.cursor() as cur:
            if force:
                cur.execute("""SELECT id::text, name FROM organizations_organization
                               WHERE name IS NOT NULL AND name != ''
                               ORDER BY name""")
            else:
                cur.execute("""
                    SELECT o.id::text, o.name
                    FROM organizations_organization o
                    WHERE o.name IS NOT NULL AND o.name != ''
                      AND NOT EXISTS (SELECT 1 FROM organizations_union_data u
                                      WHERE u.organization_id = o.id)
                      AND NOT EXISTS (SELECT 1 FROM organizations_990_data n
                                      WHERE n.organization_id = o.id
                                        AND n.total_revenue > 0
                                        AND n.match_confidence > 70)
                    ORDER BY
                        CASE WHEN lower(o.name) SIMILAR TO
                            '%%(local|union|afl|seiu|cwa|ibew|ufcw|uaw|ibt|'
                            || 'teamster|laborer|ironworker|carpenter|plumber|'
                            || 'electrician|painter|iatse|pba|firefighter|1199|'
                            || 'transport worker|twu|dc37|building service|hotel)%%'
                        THEN 0 ELSE 1 END,
                        o.name
                """)
            orgs = list(cur.fetchall())
        conn.close()
    except Exception as e:
        return {"error": f"Failed to load orgs: {e}"}

    if limit and limit > 0:
        orgs = orgs[:limit]
    if not orgs:
        return {"status": "ok",
                "message": "All orgs processed. Use force=true to re-fetch.",
                "matched": 0, "skipped": 0, "no_match": 0, "errors": 0}

    import time as _t
    matched, skipped, no_match, errors = 0, 0, 0, 0

    try:
        conn = get_db()
        for i, org in enumerate(orgs):
            org_id   = org["id"]
            org_name = (org["name"] or "").strip()

            if not org_name or _is_government_entity(org_name):
                skipped += 1
                try:
                    with conn.cursor() as cur:
                        cur.execute("""INSERT INTO organizations_union_data
                            (organization_id, match_confidence, fetched_at, updated_at)
                            VALUES (%s,-1,NOW(),NOW())
                            ON CONFLICT (organization_id) DO NOTHING""", (org_id,))
                    conn.commit()
                except Exception:
                    pass
                continue

            result = _search_olms_lm2(org_name)

            try:
                with conn.cursor() as cur:
                    if result and result.get("total_receipts", 0) > 0:
                        cur.execute("""
                            INSERT INTO organizations_union_data (
                                organization_id, file_number, legal_name, affiliation,
                                state, city,
                                total_receipts, total_disbursements, total_assets,
                                membership_count, report_year, form_type,
                                match_confidence, fetched_at, updated_at
                            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
                            ON CONFLICT (organization_id) DO UPDATE SET
                                file_number=EXCLUDED.file_number,
                                legal_name=EXCLUDED.legal_name,
                                affiliation=EXCLUDED.affiliation,
                                state=EXCLUDED.state, city=EXCLUDED.city,
                                total_receipts=EXCLUDED.total_receipts,
                                total_disbursements=EXCLUDED.total_disbursements,
                                total_assets=EXCLUDED.total_assets,
                                membership_count=EXCLUDED.membership_count,
                                report_year=EXCLUDED.report_year,
                                form_type=EXCLUDED.form_type,
                                match_confidence=EXCLUDED.match_confidence,
                                updated_at=NOW()
                        """, (
                            org_id,
                            result["file_number"],    result["legal_name"],
                            result["affiliation"],    result["state"],
                            result["city"],           result["total_receipts"],
                            result["total_disbursements"], result["total_assets"],
                            result["membership_count"], result["report_year"],
                            result["form_type"],      result["match_confidence"],
                        ))
                        matched += 1
                    else:
                        cur.execute("""INSERT INTO organizations_union_data
                            (organization_id, match_confidence, fetched_at, updated_at)
                            VALUES (%s,0,NOW(),NOW())
                            ON CONFLICT (organization_id) DO NOTHING""", (org_id,))
                        no_match += 1
                conn.commit()
            except Exception as e:
                log.warning(f"fetch_union_data DB error for {org_name!r}: {e}")
                errors += 1
                try: conn.rollback()
                except Exception: pass

            if i < len(orgs) - 1:
                _t.sleep(0.5)
            if (i + 1) % 10 == 0:
                log.info(f"fetch_union_data: {i+1}/{len(orgs)} "
                         f"matched={matched} no_match={no_match}")

    except Exception as e:
        log.error(f"fetch_union_data_batch error: {e}", exc_info=True)
        return {"error": str(e), "matched_before_error": matched}
    finally:
        try: conn.close()
        except Exception: pass

    return {
        "status": "ok",
        "total_processed": len(orgs),
        "matched": matched, "skipped": skipped,
        "no_match": no_match, "errors": errors,
        "match_rate_pct": round(matched / max(len(orgs) - skipped, 1) * 100, 1),
        "message": f"Matched {matched} orgs to OLMS union data.",
    }


# ─── Union data background fetch ─────────────────────────────────────────────

_union_bg_lock   = _threading.Lock()
_union_bg_status: dict = {
    "running": False, "started_at": None, "finished_at": None,
    "processed": 0, "matched": 0, "skipped": 0,
    "no_match": 0, "errors": 0, "last_error": None,
}


def _fetch_union_background_worker() -> None:
    import time as _t
    global _union_bg_status
    _union_bg_status.update(
        started_at=datetime.now(_tz.utc).isoformat(), finished_at=None,
        processed=0, matched=0, skipped=0, no_match=0, errors=0, last_error=None,
    )
    log.info("Union data background worker started")
    try:
        while _union_bg_status["running"]:
            result = fetch_union_data_batch(limit=100, force=False)
            if result.get("error"):
                _union_bg_status["last_error"] = result["error"]
                break
            _union_bg_status["processed"] += result.get("total_processed", 0)
            _union_bg_status["matched"]   += result.get("matched", 0)
            _union_bg_status["skipped"]   += result.get("skipped", 0)
            _union_bg_status["no_match"]  += result.get("no_match", 0)
            _union_bg_status["errors"]    += result.get("errors", 0)
            if result.get("total_processed", 0) == 0:
                break
            log.info(f"Union bg: processed={_union_bg_status['processed']} "
                     f"matched={_union_bg_status['matched']}")
            _t.sleep(2)
    except Exception as e:
        log.error(f"Union bg worker error: {e}", exc_info=True)
        _union_bg_status["last_error"] = str(e)
    finally:
        _union_bg_status["running"] = False
        _union_bg_status["finished_at"] = datetime.now(_tz.utc).isoformat()


def start_union_data_background_fetch() -> dict:
    """Start fetching OLMS union data for all remaining orgs in a background thread."""
    global _union_bg_status
    with _union_bg_lock:
        if _union_bg_status["running"]:
            return {"status": "already_running", "progress": dict(_union_bg_status)}
        _union_bg_status["running"] = True
        t = _threading.Thread(target=_fetch_union_background_worker,
                              daemon=True, name="fetch-union-bg")
        t.start()
    return {"status": "started",
            "message": "Union LM-2 fetch running in background. "
                       "Call get_union_fetch_status to monitor."}


def get_union_fetch_status() -> dict:
    """Return current status of the background union data fetch."""
    status = dict(_union_bg_status)
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("""SELECT COUNT(*) AS total,
                COUNT(CASE WHEN total_receipts > 0 THEN 1 END) AS matched,
                COUNT(CASE WHEN match_confidence=-1 THEN 1 END) AS skipped,
                COUNT(CASE WHEN match_confidence=0  THEN 1 END) AS no_match
                FROM organizations_union_data""")
            row = cur.fetchone()
            if row:
                status["db_total"] = int(row["total"])
                status["db_matched"] = int(row["matched"])
        conn.close()
    except Exception as e:
        status["db_error"] = str(e)
    return status

def cfb_donations_received(candidate_name: str, limit: int = 50) -> list[dict]:
    last = candidate_name.strip().split()[-1]
    try:
        resp = requests.get(
            f"{CFB_BASE}/{CFB_CONTRIBUTIONS_ID}.json",
            params={"$limit": limit,
                    "$where": f"upper(candidate_name) like upper('%{last}%') AND amount >= 500",
                    "$order": "amount DESC"},
            timeout=15
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.warning(f"CFB received error: {e}")
        return []

def cfb_donations_made(donor_name: str, limit: int = 50) -> list[dict]:
    last = donor_name.strip().split()[-1]
    try:
        resp = requests.get(
            f"{CFB_BASE}/{CFB_CONTRIBUTIONS_ID}.json",
            params={"$limit": limit,
                    "$where": f"upper(contributor_name) like upper('%{last}%') AND amount >= 250",
                    "$order": "amount DESC"},
            timeout=15
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.warning(f"CFB made error: {e}")
        return []

def fec_donations_to(candidate_name: str, limit: int = 50) -> list[dict]:
    key = _cfg("FEC_API_KEY", "DEMO_KEY")
    last = candidate_name.strip().split()[-1]
    try:
        cands = requests.get(f"{FEC_BASE}/candidates/search/",
            params={"api_key": key, "q": last, "state": "NY", "per_page": 3}, timeout=10
        ).json().get("results", [])
        params = {"api_key": key, "per_page": min(limit, 100), "contributor_state": "NY",
                  "min_amount": 500, "sort": "-contribution_receipt_amount", "sort_hide_null": True}
        if cands:
            cid = cands[0].get("principal_committees", [{}])[0].get("id", "")
            if cid:
                params["committee_id"] = cid
            else:
                return []  # can't query without a committee_id
        else:
            return []
        resp = requests.get(f"{FEC_BASE}/schedules/schedule_a/", params=params, timeout=15)
        resp.raise_for_status()
        return resp.json().get("results", [])
    except Exception as e:
        log.warning(f"FEC to error: {e}")
        return []

def fec_donations_by(donor_name: str, limit: int = 50) -> list[dict]:
    key = _cfg("FEC_API_KEY", "DEMO_KEY")
    last = donor_name.strip().split()[-1]
    try:
        time.sleep(0.3)
        resp = requests.get(f"{FEC_BASE}/schedules/schedule_a/",
            params={"api_key": key, "per_page": min(limit, 100), "contributor_name": last,
                    "contributor_state": "NY", "min_amount": 250,
                    "sort": "-contribution_receipt_amount"},
            timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("results", [])
    except Exception as e:
        log.warning(f"FEC by error: {e}")
        return []


# ─── NYC Lobbying (City Clerk eLobbyist, dataset fmf3-knd8) ──────────────────


def _extract_role_for_name(targets_str: str, last_name: str,
                            full_name: str = "") -> list[dict]:
    """
    Parse a semicolon-delimited lobbying targets string and extract the
    role/agency context for each entry matching the given person.
    Returns [{name_in_filing, role_in_filing, full_entry}].
    e.g. "Borough President - Brooklyn Evan Thies" ->
         {name: "Evan Thies", role: "Borough President - Brooklyn"}
    """
    import re as _re
    results = []
    entries = [e.strip() for e in targets_str.split(";") if e.strip()]
    last_lower = last_name.lower()

    for entry in entries:
        if last_lower not in entry.lower():
            continue
        matched_name = ""
        role = ""

        if full_name:
            parts = full_name.strip().split()
            if len(parts) >= 2:
                pattern = _re.compile(
                    _re.escape(parts[0]) + r"\s+" + _re.escape(parts[-1]),
                    _re.IGNORECASE
                )
                m = pattern.search(entry)
                if m:
                    role = entry[:m.start()].strip().rstrip(" -,").strip()
                    matched_name = entry[m.start():].strip()

        if not matched_name:
            idx = entry.lower().rfind(last_lower)
            if idx == -1:
                continue
            before_words = entry[:idx].strip().split()
            first_name = before_words[-1] if before_words else ""
            role = " ".join(before_words[:-1]).rstrip(" -,").strip() if before_words else ""
            matched_name = f"{first_name} {last_name}".strip() if first_name else last_name

        results.append({
            "name_in_filing": matched_name,
            "role_in_filing":  role or "Unknown",
            "full_entry":      entry.strip(),
        })
    return results

NYC_LOBBYING_ID = "fmf3-knd8"
NYC_OPEN_DATA   = "https://data.cityofnewyork.us/resource"

def _lobbying_params(last_name: str, year: str = "", limit: int = 200) -> dict:
    """Build SoQL params to find lobbying filings targeting a named official."""
    # The targets field contains strings like "NYC Council Members Julie Menin - District No. 5"
    # Search both lobbyist_targets and periodic_targets
    where = f"(upper(lobbyist_targets) like upper('%{last_name}%') OR upper(periodic_targets) like upper('%{last_name}%'))"
    if year:
        where += f" AND report_year='{year}'"
    return {"$where": where, "$limit": limit, "$order": "compensation_total DESC"}

def nyc_lobbying_targets(official_name: str, year: str = "", limit: int = 200) -> list[dict]:
    """
    Return lobbying filings that targeted a named official.
    Works for council members, commissioners, borough presidents, etc.
    Results are de-duplicated by client+lobbyist+year.
    """
    last = official_name.strip().split()[-1]
    url  = f"{NYC_OPEN_DATA}/{NYC_LOBBYING_ID}.json"
    try:
        resp = requests.get(url, params=_lobbying_params(last, year, limit), timeout=10)
        resp.raise_for_status()
        rows = resp.json()
        # Verify the name actually appears (last-name search can get false positives)
        norm_target = normalize(official_name)
        verified = []
        for row in rows:
            combined = " ".join([
                row.get("lobbyist_targets") or "",
                row.get("periodic_targets") or "",
            ]).lower()
            # Accept if last name appears in targets text
            if last.lower() in combined:
                verified.append(row)
        log.info(f"NYC lobbying: {len(verified)} filings found targeting {official_name} ({year or 'all years'})")
        return verified
    except Exception as e:
        log.warning(f"NYC lobbying API error: {e}")
        return []

def nyc_lobbying_by_client(client_name: str, year: str = "", limit: int = 100) -> list[dict]:
    """Return lobbying filings where client_name matches — shows who a person/org hired to lobby."""
    last = client_name.strip().split()[-1]
    url  = f"{NYC_OPEN_DATA}/{NYC_LOBBYING_ID}.json"
    try:
        where = f"upper(client_name) like upper('%{last}%')"
        if year:
            where += f" AND report_year='{year}'"
        resp = requests.get(url,
            params={"$where": where, "$limit": limit, "$order": "compensation_total DESC"},
            timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.warning(f"NYC lobbying by client error: {e}")
        return []

def _dedupe_lobbying(rows: list[dict], subject_name: str = "") -> list[dict]:
    """Collapse multiple period filings into one entry per client+lobbyist+year.
    Extracts role/position context for the subject from the targets string."""
    seen: dict[str, dict] = {}
    for row in rows:
        key = f"{row.get('client_name','')}|{row.get('lobbyist_name','')}|{row.get('report_year','')}"
        if key not in seen:
            # Extract how the subject is listed in this filing
            subject_roles: list[dict] = []
            if subject_name:
                last = subject_name.strip().split()[-1]
                for field in ["lobbyist_targets", "periodic_targets"]:
                    val = row.get(field) or ""
                    if val:
                        matches = _extract_role_for_name(val, last, subject_name)
                        subject_roles.extend(matches)
                # Dedupe roles
                seen_roles = set()
                unique_roles = []
                for r in subject_roles:
                    k = r["role_in_filing"]
                    if k not in seen_roles:
                        seen_roles.add(k)
                        unique_roles.append(r)
                subject_roles = unique_roles

            seen[key] = {
                "lobbyist_name":    (row.get("lobbyist_name") or "").strip(),
                "client_name":      (row.get("client_name") or "").strip(),
                "client_industry":  (row.get("client_industry") or "").strip(),
                "year":             (row.get("report_year") or "").strip(),
                "compensation":     0.0,
                "activities":       row.get("lobbyist_activities") or row.get("periodic_activities") or "",
                "lobbyist_po":      (row.get("lobbyist_po") or "").strip(),
                "client_po":        (row.get("client_po") or "").strip(),
                "subject_roles":    subject_roles,   # how the subject is listed
            }
        try:
            seen[key]["compensation"] += float(row.get("compensation_total") or 0)
        except (ValueError, TypeError):
            pass
    return sorted(seen.values(), key=lambda x: -x["compensation"])


# ─── NYS Lobbying (COELIG/JCOPE, data.ny.gov — annual datasets) ──────────────
# Each row = one contact with a covered official during a bi-monthly period.
# Key fields: party_name (official), principal_lobbyist_name, contractual_client_name,
#             lobbying_subjects, compensation, reporting_year, government_body

NYS_LOBBY_DATASETS = {
    "2025": "kn4r-wkd3",
    "2024": "erp5-6f4d",
    "2023": "th4u-mq5q",
    "2022": "2u7m-echw",
    "2021": "jy9e-nxib",
    "2020": "cuzx-2f5f",
}
NYS_OPEN_DATA = "https://data.ny.gov/resource"

# Map of well-known officials to their office titles as they appear in party_name.
# When a name alone returns nothing, we also search these aliases.
NYS_OFFICIAL_ALIASES: dict[str, list[str]] = {
    "hochul":    ["Executive Chamber", "Governor Kathy Hochul", "Governor's Office"],
    "cuomo":     ["Executive Chamber", "Governor Andrew Cuomo"],
    "adams":     ["Office of the Mayor", "Mayor Eric Adams"],
    "james":     ["Attorney General Letitia James", "Office of the Attorney General"],
    "dinapoli":  ["Office of the State Comptroller", "Comptroller Thomas DiNapoli"],
}

def nys_lobbying_targets(official_name: str, years: list[str] | None = None,
                         limit_per_year: int = 200) -> list[dict]:
    """
    Return NYS COELIG bi-monthly rows where party_name matches the official.
    Also searches office-title aliases for Governor/Mayor/AG-level officials.
    Searches the two most recent years by default.
    """
    if years is None:
        years = ["2025", "2024"]

    last = official_name.strip().split()[-1].lower()
    all_rows: list[dict] = []

    # Build list of search terms: last name + any known aliases
    search_terms = [official_name.strip().split()[-1]]  # last name
    for alias_key, aliases in NYS_OFFICIAL_ALIASES.items():
        if alias_key in last:
            search_terms.extend(aliases)

    for year in years:
        dataset_id = NYS_LOBBY_DATASETS.get(year)
        if not dataset_id:
            continue
        url = f"{NYS_OPEN_DATA}/{dataset_id}.json"
        seen_ids: set[str] = set()

        for term in search_terms:
            where = f"upper(party_name) like upper('%{term}%')"
            try:
                resp = requests.get(url,
                    params={"$where": where, "$limit": limit_per_year,
                            "$order": "compensation DESC"},
                    timeout=12)
                resp.raise_for_status()
                rows = resp.json()
                for r in rows:
                    uid = r.get("unique_id") or r.get("form_submission_id", "")
                    if uid and uid not in seen_ids:
                        seen_ids.add(uid)
                        r["_matched_alias"] = term
                        all_rows.append(r)
            except Exception as e:
                log.warning(f"NYS lobbying {year}/{term} error: {e}")

        log.info(f"NYS lobbying {year}: {len(seen_ids)} rows targeting {official_name}")

    return all_rows

def nys_lobbying_by_client(client_name: str, years: list[str] | None = None,
                            limit_per_year: int = 100) -> list[dict]:
    """Return NYS rows where contractual_client_name matches."""
    if years is None:
        years = ["2025", "2024"]
    last = client_name.strip().split()[-1]
    all_rows: list[dict] = []
    for year in years:
        dataset_id = NYS_LOBBY_DATASETS.get(year)
        if not dataset_id:
            continue
        url = f"{NYS_OPEN_DATA}/{dataset_id}.json"
        try:
            resp = requests.get(url,
                params={"$where": f"upper(contractual_client_name) like upper('%{last}%')",
                        "$limit": limit_per_year, "$order": "compensation DESC"},
                timeout=12)
            resp.raise_for_status()
            all_rows.extend(resp.json())
        except Exception as e:
            log.warning(f"NYS lobbying by client {year} error: {e}")
    return all_rows

def _dedupe_nys_lobbying(rows: list[dict]) -> list[dict]:
    """Collapse bi-monthly rows into one entry per lobbyist+client+official+year."""
    seen: dict[str, dict] = {}
    for row in rows:
        key = "|".join([
            row.get("principal_lobbyist_name", ""),
            row.get("contractual_client_name", ""),
            row.get("party_name", ""),
            row.get("reporting_year", ""),
        ])
        if key not in seen:
            seen[key] = {
                "lobbyist_name":   (row.get("principal_lobbyist_name") or "").strip(),
                "client_name":     (row.get("contractual_client_name") or "").strip(),
                "official_name":   (row.get("party_name") or "").strip(),
                "year":            (row.get("reporting_year") or "").strip(),
                "government_body": (row.get("government_body") or "").strip(),
                "subjects":        (row.get("lobbying_subjects") or "").strip(),
                "compensation":    0.0,
                "individual_lobbyists": (row.get("individual_lobbyist_name") or "").strip(),
            }
        try:
            seen[key]["compensation"] += float(row.get("compensation") or 0)
        except (ValueError, TypeError):
            pass
    return sorted(seen.values(), key=lambda x: -x["compensation"])



# ─── Federal officials lookup table ─────────────────────────────────────────
# Maps last name -> FEC candidate ID, committee IDs, LDA search term
# Add entries here as needed for frequent federal contacts.

FEDERAL_OFFICIALS: dict[str, dict] = {
    "schumer":    {"fec_id": "S8NY00082",
                   "fec_committees": ["C00346312"],  # Friends of Schumer
                   "lda_search": "Schumer"},
    "gillibrand": {"fec_id": "S0NY00410",
                   "fec_committees": ["C00413914"],  # Gillibrand for Senate
                   "lda_search": "Gillibrand"},
    "jeffries":   {"fec_id": "H2NY08135",
                   "fec_committees": [],
                   "lda_search": "Jeffries"},
    "ocasio":     {"fec_id": "H8NY14107",
                   "fec_committees": [],
                   "lda_search": "Ocasio-Cortez"},
    "nadler":     {"fec_id": "H0NY17033",
                   "fec_committees": [],
                   "lda_search": "Nadler"},
    "meeks":      {"fec_id": "H8NY06084",
                   "fec_committees": [],
                   "lda_search": "Meeks"},
}

LDA_BASE = "https://lda.senate.gov/api/v1"

# ─── LDA LD-203 contribution reports ─────────────────────────────────────────
# LD-203 = semi-annual reports where every registered lobbyist discloses
# every campaign contribution they made. This is the best way to find
# who's lobbying a specific federal official — lobbyists who gave them money
# are almost certainly also meeting with them.
#
# Strategy:
# 1. Fetch LD-203 filings where recipient_name matches the official
# 2. Filter client-side to contribution_items where honoree_name or payee_name matches
# 3. Group by registrant firm — aggregate contribution amounts
# 4. For each firm, pull their LD-2 quarterly filings to get active clients + issues
# 5. Result: "Firm X gave $Y while lobbying for clients A, B on issues C, D"

def lda_ld203_contributions_to(official_name: str,
                                years: list[int] | None = None,
                                limit_per_year: int = 200) -> list[dict]:
    """
    Fetch LD-203 filings mentioning this official, then filter to actual
    contribution_items that reference them by name or committee.
    Returns list of {registrant, lobbyist, honoree, payee, amount, date, year}.
    """
    last = official_name.strip().split()[-1].lower()

    # Skip state/local officials — no federal LDA presence
    if last in ("hochul", "adams", "james", "dinapoli", "mamdani", "cuomo",
                "menin", "brewer", "rivera", "osse", "marte"):
        log.info(f"LDA LD-203: {official_name} is state/local, skipping")
        return []

    if years is None:
        years = [2025, 2024, 2023]

    # Get known committee names to match against payee_name
    official_info = FEDERAL_OFFICIALS.get(last, {})
    committee_ids = official_info.get("fec_committees", [])
    lda_search    = official_info.get("lda_search", official_name.strip().split()[-1])

    all_items: list[dict] = []
    seen_keys: set[str] = set()

    for year in years:
        try:
            # recipient_name does a broad text search — we filter precisely client-side
            resp = requests.get(f"{LDA_BASE}/contributions/",
                params={
                    "recipient_name": lda_search,
                    "filing_year": year,
                    "limit": limit_per_year,
                },
                timeout=12,
                headers={"Accept": "application/json"})
            resp.raise_for_status()
            filings = resp.json().get("results", [])
            log.info(f"LDA LD-203 {year}: {len(filings)} raw filings for {lda_search}")

            for filing in filings:
                registrant = filing.get("registrant") or {}
                lobbyist   = filing.get("lobbyist") or {}
                reg_name   = (registrant.get("name") or "").strip()
                reg_id     = registrant.get("id")
                lob_first  = (lobbyist.get("first_name") or "").strip()
                lob_last   = (lobbyist.get("last_name") or "").strip()
                lob_name   = f"{lob_first} {lob_last}".strip()
                period     = filing.get("filing_period_display", "")

                for item in (filing.get("contribution_items") or []):
                    honoree = (item.get("honoree_name") or "").strip()
                    payee   = (item.get("payee_name") or "").strip()

                    # Accept if honoree name contains our search term
                    # or if payee matches a known committee
                    matched = lda_search.lower() in honoree.lower()
                    if not matched and committee_ids:
                        matched = any(cid.upper() in payee.upper() for cid in committee_ids)
                    if not matched:
                        # Fallback: last name in honoree
                        matched = last in honoree.lower()

                    if not matched:
                        continue

                    try:
                        amount = float(item.get("amount") or 0)
                    except (ValueError, TypeError):
                        amount = 0.0

                    key = f"{reg_name}|{lob_name}|{honoree}|{payee}|{year}"
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)

                    all_items.append({
                        "registrant_name": reg_name,
                        "registrant_id":   reg_id,
                        "lobbyist_name":   lob_name or reg_name,
                        "honoree_name":    honoree,
                        "payee_name":      payee,
                        "amount":          amount,
                        "date":            item.get("date") or "",
                        "period":          period,
                        "year":            year,
                        "contribution_type": item.get("contribution_type_display", "FECA"),
                    })

        except Exception as e:
            log.warning(f"LDA LD-203 error {year}: {e}")

    log.info(f"LDA LD-203: {len(all_items)} contribution items targeting {official_name}")
    return all_items


def lda_registrant_clients(registrant_id: int, year: int = 2024) -> list[dict]:
    """
    For a lobbying firm (by registrant_id), get their active LD-2 clients
    and the issues they're lobbying on. Returns list of {client, issues}.
    """
    try:
        resp = requests.get(f"{LDA_BASE}/filings/",
            params={
                "registrant_id": registrant_id,
                "filing_year":   year,
                "filing_type":   "Q1,Q2,Q3,Q4",
                "limit":         20,
            },
            timeout=10,
            headers={"Accept": "application/json"})
        resp.raise_for_status()
        filings = resp.json().get("results", [])

        # Dedupe clients with their issues
        clients: dict[str, set[str]] = {}
        for f in filings:
            client_name = ((f.get("client") or {}).get("name") or "").strip()
            if not client_name:
                continue
            if client_name not in clients:
                clients[client_name] = set()
            for act in (f.get("lobbying_activities") or []):
                issue = act.get("general_issue_code_display")
                if issue:
                    clients[client_name].add(issue)

        return [{"client": c, "issues": ", ".join(sorted(issues))}
                for c, issues in clients.items()]

    except Exception as e:
        log.warning(f"LDA registrant clients error: {e}")
        return []


def build_federal_lobbying_profile(contribution_items: list[dict],
                                   index: dict, keys: list[str],
                                   subject_id: str | None,
                                   person_name: str,
                                   findings: dict):
    """
    Group LD-203 items by registrant firm, enrich with their LD-2 clients,
    cross-reference against Pythia contacts, write relationships.
    Populates findings["federal_lobbied_by"] in place.
    """
    if not contribution_items:
        return

    # Group by registrant firm
    firms: dict[str, dict] = {}
    for item in contribution_items:
        reg = item["registrant_name"]
        if reg not in firms:
            firms[reg] = {
                "registrant_name": reg,
                "registrant_id":   item["registrant_id"],
                "total_amount":    0.0,
                "contributions":   [],
                "lobbyists":       set(),
                "years":           set(),
            }
        firms[reg]["total_amount"]  += item["amount"]
        firms[reg]["lobbyists"].add(item["lobbyist_name"])
        firms[reg]["years"].add(str(item["year"]))
        firms[reg]["contributions"].append({
            "lobbyist":   item["lobbyist_name"],
            "honoree":    item["honoree_name"],
            "payee":      item["payee_name"],
            "amount":     item["amount"],
            "date":       item["date"],
            "period":     item["period"],
            "type":       item["contribution_type"],
        })

    # For each firm, get their active clients (in parallel for speed)
    import concurrent.futures as _cf
    most_recent_year = max((item["year"] for item in contribution_items), default=2024)

    with _cf.ThreadPoolExecutor(max_workers=6) as pool:
        client_futures = {
            reg: pool.submit(lda_registrant_clients, data["registrant_id"], most_recent_year)
            for reg, data in firms.items()
            if data["registrant_id"]
        }

    for reg, data in sorted(firms.items(), key=lambda x: -x[1]["total_amount"]):
        clients = client_futures.get(reg)
        client_list = clients.result() if clients else []

        entry = {
            "registrant": reg,
            "total_amount": data["total_amount"],
            "years": sorted(data["years"]),
            "lobbyists": sorted(data["lobbyists"]),
            "contributions": sorted(data["contributions"], key=lambda x: -x["amount"])[:5],
            "active_clients": client_list[:10],
            "registrant_in_db": False,
            "registrant_person_id": None,
            "source": "Federal LDA LD-203",
        }

        # Match individual lobbyist names against contacts
        for lob in data["lobbyists"]:
            if not lob:
                continue
            lm, _ = best_match(lob, index, keys)
            if lm:
                entry["registrant_in_db"]    = True
                entry["registrant_person_id"] = lm["id"]
                entry["registrant_db_name"]   = lm["_display"]
                write_finance_note(lm["id"],
                    f"[LDA LD-203 {sorted(data['years'])[-1]}] Made contributions to {person_name}'s campaign "
                    f"while registered lobbyist at {reg}")
                if subject_id:
                    if write_relationship(lm["id"], subject_id, "Lobbyist",
                            f"LD-203 contributor to {person_name} ({sorted(data['years'])[-1]})",
                            f"${data['total_amount']:,.0f} total | {reg} | Federal LDA"):
                        findings["new_connections_written"] += 1
                break  # one match per firm is enough

        findings["federal_lobbied_by"].append(entry)


def lda_lobbying_targeting(official_name: str,
                            years: list[int] | None = None,
                            limit: int = 50) -> list[dict]:
    """
    Thin wrapper kept for backward compatibility.
    Now returns LD-203 contribution items (not LD-2 filings).
    """
    return lda_ld203_contributions_to(official_name, years)


def _dedupe_lda_filings(filings: list[dict]) -> list[dict]:
    """Kept for find_financial_path backward compat — pass through."""
    return filings


# ─── FEC improvements — committee-based donor lookup ─────────────────────────

def fec_get_committees(candidate_name: str) -> list[str]:
    """
    Return FEC committee IDs for a candidate.
    First checks FEDERAL_OFFICIALS table, then falls back to API search.
    """
    last = candidate_name.strip().split()[-1].lower()
    official = FEDERAL_OFFICIALS.get(last)

    # Use hardcoded committees if available and non-empty
    if official and official.get("fec_committees"):
        return official["fec_committees"]

    # Try to find via API search
    fec_key = _cfg("FEC_API_KEY", "DEMO_KEY")
    candidate_id = official.get("fec_id") if official else None

    if not candidate_id:
        # Search by name
        try:
            resp = requests.get(f"{FEC_BASE}/candidates/search/",
                params={"api_key": fec_key, "q": candidate_name, "office": "S,H", "state": "NY", "per_page": 3},
                timeout=10)
            resp.raise_for_status()
            results = resp.json().get("results", [])
            if results:
                candidate_id = results[0]["candidate_id"]
        except Exception as e:
            log.warning(f"FEC candidate search error: {e}")
            return []

    if not candidate_id:
        return []

    try:
        resp = requests.get(f"{FEC_BASE}/committees/",
            params={"api_key": fec_key, "candidate_id": candidate_id, "per_page": 10},
            timeout=10)
        resp.raise_for_status()
        return [c["committee_id"] for c in resp.json().get("results", [])]
    except Exception as e:
        log.warning(f"FEC committees lookup error: {e}")
        return []

def fec_top_donors(candidate_name: str, limit: int = 100) -> list[dict]:
    """
    Get top donors to a federal candidate by querying their FEC committees directly.
    Much more reliable than the name-based search in fec_donations_to().
    """
    fec_key = _cfg("FEC_API_KEY", "DEMO_KEY")
    committee_ids = fec_get_committees(candidate_name)

    if not committee_ids:
        log.warning(f"FEC: no committees found for {candidate_name}")
        return []

    all_donors: list[dict] = []
    # FEC schedule_a requires two_year_transaction_period — search the last 3 cycles
    import datetime as _dt
    current_year = _dt.datetime.now().year
    # FEC cycles are even years; get the current and two prior cycles
    cycle = current_year if current_year % 2 == 0 else current_year + 1
    cycles = [cycle, cycle - 2, cycle - 4]

    for committee_id in committee_ids[:3]:
        for two_year in cycles:
            try:
                resp = requests.get(f"{FEC_BASE}/schedules/schedule_a/",
                    params={
                        "api_key": fec_key,
                        "committee_id": committee_id,
                        "two_year_transaction_period": two_year,
                        "sort": "-contribution_receipt_amount",
                        "sort_hide_null": "true",
                        "per_page": min(limit // len(cycles) + 10, 100),
                        # omit is_individual — filter post-fetch to avoid API rejection
                    },
                    timeout=15)
                resp.raise_for_status()
                results = resp.json().get("results", [])
                log.info(f"FEC {committee_id} cycle {two_year}: {len(results)} donors")
                # Keep only individuals (entity_type = IND)
                all_donors.extend(
                    r for r in results
                    if (r.get("entity_type") or "").upper() in ("IND", "")
                )
            except Exception as e:
                log.warning(f"FEC schedule_a error {committee_id}/{two_year}: {e}")

    # Dedupe by contributor name, sum amounts
    donor_map: dict[str, dict] = {}
    for row in all_donors:
        name = (row.get("contributor_name") or "").strip()
        if not name:
            continue
        if name not in donor_map:
            donor_map[name] = {
                "contributor_name": name,
                "employer": (row.get("contributor_employer") or "").strip(),
                "occupation": (row.get("contributor_occupation") or "").strip(),
                "state": (row.get("contributor_state") or "").strip(),
                "total_amount": 0.0,
                "latest_date": "",
            }
        donor_map[name]["total_amount"] += float(row.get("contribution_receipt_amount") or 0)
        date = row.get("contribution_receipt_date") or ""
        if date > donor_map[name]["latest_date"]:
            donor_map[name]["latest_date"] = date

    return sorted(donor_map.values(), key=lambda x: -x["total_amount"])
# ─── LDA registrant cross-reference (in-memory from bundled CSV) ─────────────
# lda_registrants.csv is built by running lda_fetch.py locally and committing.
# Format: id, name, description, city, state (17k+ rows, ~1MB)

import re as _re_lda
import csv as _csv_lda

def _norm_lda(s: str) -> str:
    """Strip punctuation, upper-case, collapse whitespace."""
    return " ".join(_re_lda.sub(r"[^A-Z0-9 ]", " ", (s or "").upper()).split())

def _words_lda(s: str) -> set[str]:
    """Significant words (4+ chars) from a normalized string."""
    return {w for w in _norm_lda(s).split() if len(w) >= 4}

# Legal suffixes to strip before matching
_LDA_SUFFIXES = (
    " LLC", " LLP", " INC", " CORP", " LP", " PC", " PLLC", " PLCC",
    " LTD", " CO", " PA", " NA", " PLC", " NPC", " LC", " DBA",
)

def _strip_suffixes(name: str) -> str:
    n = name.upper().strip().rstrip(",").strip()
    changed = True
    while changed:
        changed = False
        for s in _LDA_SUFFIXES:
            if n.endswith(s):
                n = n[:-len(s)].strip().rstrip(",").strip()
                changed = True
    return n

# Build in-memory index at startup: word -> list of registrant dicts
# Each registrant has: id, name, description, city, state, _words (set)
_LDA_REGISTRANT_INDEX: dict[str, list[dict]] = {}
_LDA_REGISTRANT_LOADED = False

def _load_lda_registrants():
    global _LDA_REGISTRANT_LOADED
    if _LDA_REGISTRANT_LOADED:
        return

    # Try multiple paths: next to the script, cwd, /app (Railway default)
    _candidates = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "lda_registrants.csv"),
        os.path.join(os.getcwd(), "lda_registrants.csv"),
        "/app/lda_registrants.csv",
    ]
    # List /app contents so we can see what Railway actually deployed
    try:
        app_files = sorted(os.listdir("/app"))
        log.info(f"Files in /app: {app_files}")
    except Exception as e:
        log.info(f"Could not list /app: {e}")
    log.info(f"LDA CSV search paths: {_candidates}")
    csv_path = None
    for _candidate in _candidates:
        log.info(f"  checking {_candidate}: exists={os.path.exists(_candidate)}")
        if os.path.exists(_candidate):
            csv_path = _candidate
            break
    if csv_path is None:
        # Last resort: search the entire /app directory tree
        import glob as _glob
        matches = _glob.glob("/app/**/lda_registrants.csv", recursive=True) +                   _glob.glob("/home/**/ lda_registrants.csv", recursive=True)
        log.info(f"  glob fallback found: {matches}")
        if matches:
            csv_path = matches[0]
        else:
            log.warning("lda_registrants.csv not found in any expected path — LDA cross-reference disabled")
            _LDA_REGISTRANT_LOADED = True
            return

    count = 0
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in _csv_lda.DictReader(f):
            name = (row.get("name") or "").strip()
            if not name:
                continue
            # Skip stale/terminated registrants (description looks like a date)
            desc = (row.get("description") or "").strip()
            if _re_lda.match(r"^\d{1,2}/\d{1,2}/\d{2,4}$", desc):
                continue
            words = _words_lda(_strip_suffixes(name))
            entry = {
                "registrant_id":   row.get("id", ""),
                "registrant_name": name,
                "description":     desc,
                "city":            (row.get("city") or "").strip(),
                "state":           (row.get("state") or "").strip(),
                "_words":          words,
            }
            for word in words:
                _LDA_REGISTRANT_INDEX.setdefault(word, []).append(entry)
            count += 1

    log.info(f"Loaded {count} LDA registrants into memory index")
    _LDA_REGISTRANT_LOADED = True

# Per-call cache to avoid repeated lookups for the same employer
_lda_registrant_cache: dict[str, dict | None] = {}

def lda_lookup_registrant(employer_name: str) -> dict | None:
    """
    Check if an employer name matches a registered LDA lobbying firm.
    Uses bundled CSV + in-memory word index. No external API calls.
    Returns registrant dict or None.
    """
    if not employer_name or len(employer_name) < 4:
        return None

    key = employer_name.upper().strip()
    if key in _lda_registrant_cache:
        return _lda_registrant_cache[key]

    _load_lda_registrants()

    query_words = _words_lda(_strip_suffixes(employer_name))
    if not query_words:
        _lda_registrant_cache[key] = None
        return None

    # Find candidates that share at least one significant word
    candidates: dict[str, dict] = {}  # registrant_id -> entry
    for word in query_words:
        for entry in _LDA_REGISTRANT_INDEX.get(word, []):
            candidates[entry["registrant_id"]] = entry

    if not candidates:
        _lda_registrant_cache[key] = None
        return None

    # Score each candidate by word overlap
    best = None
    best_score = 0
    for entry in candidates.values():
        shared = query_words & entry["_words"]
        # Require overlap on at least 1 word, AND shared words must cover
        # at least half of the shorter name's words (reduces false positives)
        if not shared:
            continue
        min_len = min(len(query_words), len(entry["_words"]))
        coverage = len(shared) / max(min_len, 1)
        score = len(shared) + coverage  # weight both count and proportion
        if score > best_score:
            best_score = score
            best = entry

    # Require at least 0.5 coverage to avoid weak matches like
    # "BLOOMBERG LP" matching "BLOOM INNOVATIONS" on just "BLOOM"
    if best is None or best_score < 1.5:
        _lda_registrant_cache[key] = None
        return None

    result = {
        "registrant_id":   best["registrant_id"],
        "registrant_name": best["registrant_name"],
        "description":     best["description"],
        "city":            best["city"],
        "state":           best["state"],
    }
    _lda_registrant_cache[key] = result
    log.debug(f"LDA match: {employer_name!r} -> {best['registrant_name']!r} (score={best_score:.1f})")
    return result


def lda_enrich_donors(donor_rows: list[dict]) -> list[dict]:
    """
    For a list of FEC donor dicts (each with an 'employer' field),
    cross-reference each employer against the LDA registrants database.
    Enriches in-place and also fetches active clients for matched firms.
    Returns enriched list sorted by amount, lobbyist-linked donors first.
    """
    if not donor_rows:
        return donor_rows

    import concurrent.futures as _cf

    # Step 1: dedupe employers and look up all in parallel
    unique_employers = list({
        d.get("employer", "").strip()
        for d in donor_rows
        if d.get("employer", "").strip()
    })

    with _cf.ThreadPoolExecutor(max_workers=10) as pool:
        reg_futures = {
            emp: pool.submit(lda_lookup_registrant, emp)
            for emp in unique_employers
        }

    employer_to_registrant = {
        emp: fut.result()
        for emp, fut in reg_futures.items()
    }

    # Step 2: for matched registrants, fetch their active clients in parallel
    matched_registrants = {
        emp: reg for emp, reg in employer_to_registrant.items()
        if reg and reg.get("registrant_id")
    }

    with _cf.ThreadPoolExecutor(max_workers=6) as pool:
        client_futures = {
            emp: pool.submit(lda_registrant_clients,
                             reg["registrant_id"], 2024)
            for emp, reg in matched_registrants.items()
        }

    employer_to_clients = {
        emp: fut.result()
        for emp, fut in client_futures.items()
    }

    # Step 3: annotate donor rows
    for donor in donor_rows:
        emp = donor.get("employer", "").strip()
        reg = employer_to_registrant.get(emp)
        if reg:
            donor["is_lda_registrant"]    = True
            donor["lda_registrant_name"]  = reg["registrant_name"]
            donor["lda_registrant_id"]    = reg["registrant_id"]
            donor["lda_firm_description"] = reg.get("description", "")
            donor["lda_active_clients"]   = employer_to_clients.get(emp, [])[:8]
        else:
            donor["is_lda_registrant"]   = False
            donor["lda_active_clients"]  = []

    # Sort: LDA-matched donors first, then by amount descending
    return sorted(donor_rows,
                  key=lambda d: (not d.get("is_lda_registrant", False),
                                 -d.get("amount", 0)))


# ─── NYC Voter File (nyc_super_voters.csv — in-memory) ───────────────────────
# 416k active NYC voters who voted in 4+ general elections.
# 49MB CSV, loaded into memory at startup. Already deployed in Railway.

import csv as _vcsv
import sqlite3 as _sqlite3
import threading as _threading
_VOTER_DB_LOCK = _threading.Lock()

_VOTER_INDEX: dict[str, list[dict]] = {}   # kept for compat but no longer populated
_VOTER_LOADED = False
_voter_db_conn = None  # set by _init_voter_db once SQLite DB is ready
_SUPER_VOTERS_CSV_PATH: str | None = None  # path to CSV for disk-streaming queries
_voter_db_download_thread = None  # background download thread; join() to wait for it

PARTY_LABELS = {
    "DEM": "Democrat", "REP": "Republican", "CON": "Conservative",
    "WOR": "Working Families", "BLK": "No Party", "OTH": "Other",
    "GRE": "Green", "LBT": "Libertarian", "IND": "Independence",
}

# Compact voter index: list of tuples for fast filtering
# Fields: (county_code, party, ad, sd, cd, voter_score, last, first,
#           dob, address, city, zip, regdate, ge_votes, primary_votes, sboeid)
_VOTER_TUPLES: list[tuple] = []

def _load_voter_file():
    """Load super voters CSV into compact tuple list — ~35MB vs 200MB for full dicts."""
    global _VOTER_LOADED, _SUPER_VOTERS_CSV_PATH, _VOTER_TUPLES
    if _VOTER_LOADED:
        return
    for _candidate in [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "nyc_super_voters.csv"),
        os.path.join(os.getcwd(), "nyc_super_voters.csv"),
        "/app/nyc_super_voters.csv",
    ]:
        if os.path.exists(_candidate):
            _SUPER_VOTERS_CSV_PATH = _candidate
            break
    else:
        log.warning("nyc_super_voters.csv not found")
        _VOTER_LOADED = True
        return

    count = 0
    with open(_SUPER_VOTERS_CSV_PATH, newline="", encoding="utf-8") as f:
        for row in _vcsv.DictReader(f):
            _VOTER_TUPLES.append((
                row.get("COUNTY_CODE",""),          # 0
                row.get("PARTY",""),                # 1
                (row.get("AD","")).lstrip("0"),     # 2
                (row.get("SD","")).lstrip("0"),     # 3
                (row.get("CD","")).lstrip("0"),     # 4
                int(row.get("VOTER_SCORE") or 0),  # 5
                (row.get("LASTNAME") or "").strip().upper(),   # 6
                (row.get("FIRSTNAME") or "").strip().upper(),  # 7
                row.get("DOB",""),                  # 8
                row.get("ADDRESS",""),              # 9
                row.get("CITY",""),                 # 10
                row.get("ZIP",""),                  # 11
                row.get("REGDATE",""),              # 12
                int(row.get("GE_VOTES") or 0),     # 13
                int(row.get("PRIMARY_VOTES") or 0),# 14
                row.get("SBOEID",""),               # 15
                row.get("GE_YEARS",""),             # 16 e.g. "2025,2024,2023"
                row.get("PRIMARY_YEARS",""),        # 17 e.g. "2024,2022"
                row.get("OFF_YEAR_YEARS",""),       # 18 e.g. "2022,2019"
            ))
            count += 1
    log.info(f"Loaded {count:,} NYC super voters into compact index")
    _VOTER_LOADED = True

VOTER_DB_RELEASE_URL = (
    "https://github.com/digbripper/finance-mcp"
    "/releases/download/v1.0-voter-db/nyc_voters.db"
)
VOTER_DB_LOCAL_PATH = "/app/nyc_voters.db"

def _is_real_sqlite(path: str) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(6) == b"SQLite"
    except Exception:
        return False

def _has_year_columns(path: str) -> bool:
    """Check if the DB has ge_years column — confirms it is the v1.1 schema."""
    try:
        con = _sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        cols = [r[1] for r in con.execute("PRAGMA table_info(voters)").fetchall()]
        con.close()
        return "ge_years" in cols
    except Exception:
        return False

def _download_voter_db() -> bool:
    import urllib.request as _ur
    path = VOTER_DB_LOCAL_PATH
    tmp_path = path + ".tmp"
    if (os.path.exists(path) and _is_real_sqlite(path)
            and os.path.getsize(path) > 100_000_000
            and _has_year_columns(path)):
        log.info("Full voter DB already present (schema OK)")
        return True
    if os.path.exists(path):
        log.info("Voter DB present but outdated schema or wrong size — re-downloading...")
    log.info("Downloading full voter DB from GitHub Releases (~1GB)...")
    try:
        req = _ur.Request(VOTER_DB_RELEASE_URL,
                          headers={"User-Agent": "finance-mcp/1.0"})
        with _ur.urlopen(req, timeout=300) as resp, \
             open(tmp_path, "wb") as out:
            downloaded = 0
            while True:
                chunk = resp.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
                downloaded += len(chunk)
                if downloaded % (100 * 1024 * 1024) == 0:
                    log.info(f"  Downloaded {downloaded // (1024*1024)}MB...")
        # Atomic rename only after full download
        os.replace(tmp_path, path)
        log.info(f"Voter DB download complete ({downloaded // (1024*1024)}MB)")
        return True
    except Exception as e:
        log.error(f"Failed to download voter DB: {e}")
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        return False

def _open_voter_db():
    """Open the SQLite voter DB once downloaded."""
    global _voter_db_conn
    try:
        conn = _sqlite3.connect(
            f"file:{VOTER_DB_LOCAL_PATH}?mode=ro", uri=True,
            check_same_thread=False
        )
        conn.row_factory = _sqlite3.Row
        count = conn.execute("SELECT COUNT(*) FROM voters").fetchone()[0]
        _voter_db_conn = conn
        log.info(f"Full voter DB loaded: {count:,} active NYC voters")
    except Exception as e:
        log.error(f"Failed to open voter DB: {e}")

def _background_download():
    """Download voter DB in background — doesn't block server startup."""
    if _download_voter_db():
        _open_voter_db()
    else:
        log.warning("Full voter DB unavailable — lookup_voter uses super voter CSV only")

def _init_voter_db():
    """Load super voter CSV immediately, then download full DB in background."""
    _load_voter_file()  # always load CSV — fast, needed for find_super_voters

    # If DB already present, valid, and has year columns — open synchronously (fast path)
    if (os.path.exists(VOTER_DB_LOCAL_PATH) and _is_real_sqlite(VOTER_DB_LOCAL_PATH)
            and _has_year_columns(VOTER_DB_LOCAL_PATH)):
        _open_voter_db()
        return

    if os.path.exists(VOTER_DB_LOCAL_PATH) and _is_real_sqlite(VOTER_DB_LOCAL_PATH):
        # Old DB exists but lacks year columns — open it now so queries use SQLite instead of
        # the CSV tuple fallback. Background download will atomically replace it and call
        # _open_voter_db() again to swap the connection to the schema-correct version.
        log.info("Outdated voter DB (missing year columns) — opening for now, re-downloading in background...")
        _open_voter_db()
    else:
        log.info("Voter DB not present or invalid — downloading in background thread...")

    # Download in background — atomically replaces the file and re-opens the connection when done.
    # Save thread ref so find_super_voters can join() it rather than falling to CSV fallback.
    global _voter_db_download_thread
    import threading as _th
    t = _th.Thread(target=_background_download, daemon=True)
    _voter_db_download_thread = t
    t.start()


def lookup_voter(full_name: str, dob: str = "") -> dict | None:
    """
    Look up a person in the NYC voter DB by name.
    Uses SQLite if available, streams CSV as fallback.
    """
    parts = full_name.strip().upper().split()
    if not parts:
        return None
    last  = parts[-1]
    first = parts[0] if len(parts) >= 2 else ""

    # ── SQLite path (full 4.9M voter DB) ─────────────────────────────────────
    if _voter_db_conn is not None:
        try:
            with _VOTER_DB_LOCK:
                if dob:
                    row = _voter_db_conn.execute(
                        "SELECT * FROM voters WHERE lastname=? AND firstname=? AND dob=? LIMIT 1",
                        (last, first, dob)
                    ).fetchone()
                    if row:
                        return dict(row)
                rows = _voter_db_conn.execute(
                    "SELECT * FROM voters WHERE lastname=? AND firstname LIKE ? "
                    "ORDER BY voter_score DESC LIMIT 5",
                    (last, first[:3] + "%") if first else (last, "%")
                ).fetchall()
                if rows:
                    for r in rows:
                        if r["firstname"] == first:
                            return dict(r)
                    return dict(rows[0])
        except Exception as e:
            log.warning(f"Voter DB lookup error: {e}")

    # ── Tuple index fallback (416k super voters, fast in-memory scan) ──────────
    _load_voter_file()
    if not _VOTER_TUPLES:
        return None
    best, best_score = None, 0
    for t in _VOTER_TUPLES:
        if t[6] != last:
            continue
        score = 10
        if first:
            if t[7] == first:              score += 12
            elif t[7].startswith(first[:3]): score += 6
        if dob and t[8] == dob:            score += 20
        if score > best_score:
            best_score = score
            best = {"last": t[6], "first": t[7], "dob": t[8], "party": t[1],
                    "address": t[9], "city": t[10], "zip": t[11],
                    "county_code": t[0], "county": t[10],
                    "cd": t[4], "sd": t[3], "ad": t[2], "regdate": t[12],
                    "ge_votes": t[13], "primary_votes": t[14], "voter_score": t[5],
                    "sboeid": t[15],
                    "ge_years": t[16] if len(t) > 16 else "",
                    "primary_years": t[17] if len(t) > 17 else "",
                    "off_year_years": t[18] if len(t) > 18 else ""}
    return best if best_score >= 16 else None





# ─── Super voter finder ───────────────────────────────────────────────────────

COUNTY_CODES = {
    "manhattan": "31", "new york": "31", "ny": "31",
    "brooklyn": "24", "kings": "24",
    "queens": "41",
    "bronx": "03",
    "staten island": "43", "richmond": "43",
}

def find_super_voters(
    county: str = "brooklyn",
    min_voter_score: int = 10,
    party: str = "",
    assembly_district: str = "",
    state_senate_district: str = "",
    congressional_district: str = "",
    cross_reference_finance: bool = True,
    limit: int = 50,
) -> list[dict]:
    """
    Find high-engagement NYC voters. Uses SQLite DB if available, streams CSV otherwise.
    Filters by county, party, district, voter score. Cross-references BOE finance data.
    """
    _load_voter_file()

    # Resolve county name to code
    county_code = COUNTY_CODES.get(county.lower().strip())
    if not county_code:
        county_code = county if county in COUNTY_CODES.values() else None
    if not county_code:
        return [{"error": f"Unknown county: {county}. Use: manhattan, brooklyn, queens, bronx, staten island"}]

    party_map = {"DEMOCRAT": "DEM", "DEMOCRATIC": "DEM", "REPUBLICAN": "REP",
                 "WORKING FAMILIES": "WOR", "NO PARTY": "BLK", "INDEPENDENT": "BLK"}
    party_code = party_map.get(party.upper(), party.upper()[:3]) if party else ""

    ad_str = str(assembly_district).lstrip("0") if assembly_district else ""
    sd_str = str(state_senate_district).lstrip("0") if state_senate_district else ""
    cd_str = str(congressional_district).lstrip("0") if congressional_district else ""

    fetch_limit = limit * 3 if cross_reference_finance else limit
    rows = []

    # ── SQLite path ───────────────────────────────────────────────────────────
    global _voter_db_conn
    # If the background download thread is still running, join it so we don't
    # race past it and fall to the CSV tuple fallback (which has no year columns).
    # find_super_voters runs in a thread-pool executor so blocking here is safe.
    if _voter_db_conn is None and _voter_db_download_thread is not None and _voter_db_download_thread.is_alive():
        log.info("find_super_voters: voter DB still downloading — waiting up to 120s...")
        import time as _time
        _t0 = _time.time()
        _voter_db_download_thread.join(timeout=120)
        log.info(f"find_super_voters: download wait finished in {_time.time()-_t0:.1f}s, db_conn={_voter_db_conn is not None}")

    # Open DB on demand if file exists but connection not yet set (covers restart after crash)
    if _voter_db_conn is None and os.path.exists(VOTER_DB_LOCAL_PATH):
        if _is_real_sqlite(VOTER_DB_LOCAL_PATH):
            log.info(f"find_super_voters: opening voter DB on demand (size={os.path.getsize(VOTER_DB_LOCAL_PATH):,})")
            _open_voter_db()
        else:
            log.warning(f"find_super_voters: DB file exists but not valid SQLite (size={os.path.getsize(VOTER_DB_LOCAL_PATH)})")
    if _voter_db_conn is not None:
        try:
            sql = ("SELECT sboeid,lastname,firstname,dob,party,address,city,zip,"
                   "county_code,county_name,cd,sd,ad,regdate,ge_votes,primary_votes,voter_score,"
                   "ge_years,primary_years,off_year_years "
                   "FROM voters WHERE county_code=? AND voter_score>=?")
            params: list = [county_code, min_voter_score]
            if party_code:
                sql += " AND party=?"; params.append(party_code)
            if ad_str:
                sql += " AND CAST(LTRIM(ad,'0') AS TEXT)=?"; params.append(ad_str)
            if sd_str:
                sql += " AND CAST(LTRIM(sd,'0') AS TEXT)=?"; params.append(sd_str)
            if cd_str:
                sql += " AND CAST(LTRIM(cd,'0') AS TEXT)=?"; params.append(cd_str)
            sql += " ORDER BY voter_score DESC LIMIT ?"
            params.append(fetch_limit)
            with _VOTER_DB_LOCK:
                db_rows = _voter_db_conn.execute(sql, params).fetchall()
            for r in db_rows:
                rd = dict(r)  # convert once; sqlite3.Row has no .get()
                rows.append({
                    "last": rd.get("lastname", ""), "first": rd.get("firstname", ""),
                    "dob": rd.get("dob", ""), "party": rd.get("party", ""),
                    "address": rd.get("address", ""), "city": rd.get("city", ""), "zip": rd.get("zip", ""),
                    "county_code": rd.get("county_code", ""), "county": rd.get("county_name", ""),
                    "cd": rd.get("cd", ""), "sd": rd.get("sd", ""), "ad": rd.get("ad", ""),
                    "regdate": rd.get("regdate", ""), "ge_votes": rd.get("ge_votes", 0),
                    "primary_votes": rd.get("primary_votes", 0), "voter_score": rd.get("voter_score", 0),
                    "ge_years": rd.get("ge_years", ""),
                    "primary_years": rd.get("primary_years", ""),
                    "off_year_years": rd.get("off_year_years", ""),
                })
            log.info(f"find_super_voters (SQLite): {len(rows)} rows for {county}")
        except Exception as e:
            log.warning(f"find_super_voters SQLite error: {e}")

    # ── Tuple index fallback (in-memory, fast) ───────────────────────────────
    if not rows and _VOTER_TUPLES:
        for t in _VOTER_TUPLES:
            if t[0] != county_code: continue
            if t[5] < min_voter_score: continue
            if party_code and t[1] != party_code: continue
            if ad_str and t[2] != ad_str: continue
            if sd_str and t[3] != sd_str: continue
            if cd_str and t[4] != cd_str: continue
            rows.append({"last": t[6], "first": t[7], "dob": t[8], "party": t[1],
                         "address": t[9], "city": t[10], "zip": t[11],
                         "county_code": t[0], "county": "",
                         "cd": t[4], "sd": t[3], "ad": t[2], "regdate": t[12],
                         "ge_votes": t[13], "primary_votes": t[14], "voter_score": t[5],
                         "ge_years": t[16] if len(t) > 16 else "",
                         "primary_years": t[17] if len(t) > 17 else "",
                         "off_year_years": t[18] if len(t) > 18 else ""})
            if len(rows) >= fetch_limit:
                break
        rows.sort(key=lambda x: -x.get("voter_score", 0))
        log.info(f"find_super_voters (tuple index): {len(rows)} rows for {county}")

    if not rows:
        return [{"error": "Voter data not yet available — SQLite DB is still downloading. Try again in a few minutes."}]

    RECENCY_BONUS = {"2025": 3, "2024": 2, "2023": 1}
    def _recency_score(v):
        base = v.get("voter_score", 0)
        bonus = 0
        for f in ("ge_years", "primary_years", "off_year_years"):
            for yr in (v.get(f) or "").split(","):
                yr = yr.strip()
                if yr in RECENCY_BONUS:
                    bonus += RECENCY_BONUS[yr]
        return base + bonus * 0.5
    rows.sort(key=lambda x: -_recency_score(x))
    log.info(f"find_super_voters: {len(rows)} voters for {county} score>={min_voter_score} | db_conn={_voter_db_conn is not None} | db_exists={os.path.exists(VOTER_DB_LOCAL_PATH)}")

    if cross_reference_finance:
        _build_boe_donor_index()


    results = []
    for v in rows:
        party_label = PARTY_LABELS.get(v.get("party",""), v.get("party",""))
        entry = {
            "name": f"{(v.get('first') or '').title()} {(v.get('last') or '').title()}",
            "lastname": v.get("last",""),
            "firstname": v.get("first",""),
            "party": party_label,
            "party_code": v.get("party",""),
            "address": v.get("address",""),
            "city": v.get("city",""),
            "zip": v.get("zip",""),
            "assembly_district": v.get("ad",""),
            "state_senate_district": v.get("sd",""),
            "congressional_district": v.get("cd",""),
            "registered_since": v.get("regdate",""),
            "general_elections_voted": v.get("ge_votes",0),
            "primaries_voted": v.get("primary_votes",0),
            "voter_score": v.get("voter_score",0),
            "ge_years": v.get("ge_years",""),
            "primary_years": v.get("primary_years",""),
            "off_year_years": v.get("off_year_years",""),
            "total_donated": 0.0,
            "donation_count": 0,
            "top_candidates": [],
            "has_finance_history": False,
        }

        if cross_reference_finance:
            donations = boe_donations_by_voter(v.get("last",""), v.get("first",""))
            if donations:
                total = sum(float(d.get("amount") or 0) for d in donations)
                candidates = {}
                for d in donations:
                    cand = (d.get("candidate_name") or "").strip()
                    if cand:
                        candidates[cand] = candidates.get(cand, 0) + float(d.get("amount") or 0)
                top = sorted(candidates.items(), key=lambda x: -x[1])[:5]
                entry["total_donated"] = round(total, 2)
                entry["donation_count"] = len(donations)
                entry["top_candidates"] = [{"candidate": c, "amount": round(a, 2)} for c, a in top]
                entry["has_finance_history"] = total > 0

        results.append(entry)

    # If cross-referencing, sort by those with finance history first, then by voter score
    if cross_reference_finance:
        results.sort(key=lambda x: (-x["total_donated"], -x["voter_score"]))

    return results[:limit]


# ─── Person profile — works for anyone, Pythia or not ────────────────────────

def get_person_profile(person_name: str) -> dict:
    """
    Build a full profile for any person by name.
    Does NOT require them to be in the Pythia DB.
    Pulls from: voter file, BOE donations, CFB donations, NYC/NYS lobbying,
                FEC donors (if federal official), Pythia DB (if matched).
    """
    profile = {
        "name": person_name,
        "in_pythia": False,
        "pythia_id": None,
        "pythia_orgs": None,
        "voter_profile": None,
        "donations_made": [],        # campaigns they donated to
        "donations_received": [],    # donors to them (if official)
        "lobbied_by_nyc": [],
        "lobbied_by_nys": [],
        "boe_donor_summary": None,
    }

    # ── Fire all HTTP lookups in parallel ────────────────────────────────────
    import concurrent.futures as _cf
    with _cf.ThreadPoolExecutor(max_workers=6) as _pool:
        _f_voter    = _pool.submit(lookup_voter, person_name)
        _f_boe_made = _pool.submit(boe_donations_by, person_name)
        _f_cfb_made = _pool.submit(cfb_donations_made, person_name)
        _f_nyc_lob  = _pool.submit(nyc_lobbying_targets, person_name)
        _f_nys_lob  = _pool.submit(nys_lobbying_targets, person_name)
        _f_contacts = _pool.submit(get_all_contacts)

    # ── Voter file lookup ─────────────────────────────────────────────────────
    voter = _f_voter.result()
    if voter:
        party_label = PARTY_LABELS.get(
            voter.get("party", voter.get("party_code", "")),
            voter.get("party", voter.get("party_code", ""))
        )
        profile["voter_profile"] = {
            "registered_party":        party_label,
            "party_code":              voter.get("party", voter.get("party_code", "")),
            "address":                 voter.get("address", ""),
            "city":                    voter.get("city", ""),
            "zip":                     voter.get("zip", ""),
            "county":                  voter.get("county", voter.get("county_name", "")),
            "congressional_district":  voter.get("cd", ""),
            "state_senate_district":   voter.get("sd", ""),
            "assembly_district":       voter.get("ad", ""),
            "registered_since":        voter.get("regdate", ""),
            "general_elections_voted": voter.get("ge_votes", voter.get("general_elections_voted", 0)),
            "primaries_voted":         voter.get("primary_votes", voter.get("primaries_voted", 0)),
            "voter_score":             voter.get("voter_score", 0),
        }

    # ── Pythia DB lookup ──────────────────────────────────────────────────────
    try:
        contacts = _f_contacts.result()
        index, keys = build_index(contacts)
        subject, score = best_match(person_name, index, keys)
        if subject and score >= 82:
            profile["in_pythia"]    = True
            profile["pythia_id"]    = subject["id"]
            profile["pythia_orgs"]  = subject.get("orgs")
            profile["pythia_name"]  = subject.get("_display")
    except Exception as e:
        log.warning(f"Pythia lookup failed for {person_name}: {e}")

    # ── BOE donations MADE by this person ─────────────────────────────────────
    try:
        boe_rows = _f_boe_made.result() or []
        if boe_rows:
            rmap: dict[str, dict] = {}
            for row in boe_rows:
                c = (row.get("candidate_name") or "").strip()
                if c:
                    if c not in rmap:
                        rmap[c] = {"amount": 0.0,
                                   "year": row.get("election_year") or "",
                                   "source": "NYS BOE"}
                    rmap[c]["amount"] += float(row.get("amount") or 0)
            profile["donations_made"] = sorted(
                [{"candidate": c, **v} for c, v in rmap.items()],
                key=lambda x: -x["amount"]
            )
            total = sum(r["amount"] for r in profile["donations_made"])
            profile["boe_donor_summary"] = {
                "total_donated": round(total, 2),
                "num_candidates": len(rmap),
                "source": "NYS BOE",
            }
    except Exception as e:
        log.warning(f"BOE donations lookup failed: {e}")

    # ── CFB donations MADE ────────────────────────────────────────────────────
    try:
        cfb_rows = _f_cfb_made.result() or []
        for row in cfb_rows:
            c = (row.get("candidate_name") or "").strip()
            if c:
                profile["donations_made"].append({
                    "candidate": c,
                    "amount": float(row.get("amount") or 0),
                    "year": (row.get("date") or "")[:4],
                    "source": "NYC CFB",
                })
        profile["donations_made"].sort(key=lambda x: -x["amount"])
    except Exception as e:
        log.warning(f"CFB donations lookup failed: {e}")

    # ── NYC lobbying targeting this person ────────────────────────────────────
    try:
        nyc_rows = _f_nyc_lob.result() or []
        deduped = _dedupe_lobbying(nyc_rows, person_name)
        profile["lobbied_by_nyc"] = [
            {
                "lobbyist": r["lobbyist_name"],
                "client": r["client_name"],
                "year": r["year"],
                "compensation": r["compensation"],
                "activities": r["activities"][:150] if r["activities"] else "",
                "listed_as": "; ".join(
                    x["role_in_filing"] for x in r.get("subject_roles", [])
                    if x.get("role_in_filing") and x["role_in_filing"] != "Unknown"
                ) or "Lobbying target",
            }
            for r in deduped[:20]
        ]
    except Exception as e:
        log.warning(f"NYC lobbying lookup failed: {e}")

    # ── NYS lobbying targeting this person ────────────────────────────────────
    try:
        nys_rows = _f_nys_lob.result() or []
        nys_deduped = _dedupe_nys_lobbying(nys_rows)
        profile["lobbied_by_nys"] = [
            {"lobbyist": r["lobbyist_name"], "client": r["client_name"],
             "year": r["year"], "compensation": r["compensation"],
             "subjects": r["subjects"][:150]}
            for r in nys_deduped[:20]
        ]
    except Exception as e:
        log.warning(f"NYS lobbying lookup failed: {e}")

    return profile


# ─── Influential people finder ────────────────────────────────────────────────

def find_influential_in_area(
    zip_code: str = "",
    borough: str = "",
    assembly_district: str = "",
    state_senate_district: str = "",
    congressional_district: str = "",
    min_tier: int = 2,
    include_super_voters: bool = True,
    include_unmatched_pythia: bool = True,
    limit: int = 25,
) -> list[dict]:
    """
    Find influential people in a geographic area by combining:
    1. Pythia contacts at min_tier or better — voter file provides their home address/districts
    2. High-scoring super voters in the area who may not be in Pythia

    Returns merged list sorted by: influence tier, then voter score.
    """
    import concurrent.futures as _cf

    # ── Step 1: Pull Pythia contacts at requested tier ────────────────────────
    pythia_contacts = []
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT
                        p.id::text, p.full_name, p.first_name, p.last_name,
                        p.personal_address_line_1, p.personal_city,
                        p.personal_postal_code,
                        o.name  AS org_name,
                        o.influence_tier,
                        po.job_title
                    FROM people_person p
                    JOIN people_personorganization po ON po.person_id = p.id
                    JOIN organizations_organization o  ON o.id = po.organization_id
                    WHERE p.is_active = TRUE
                      AND po.is_current = TRUE
                      AND o.influence_tier <= %s
                      AND o.influence_tier IS NOT NULL
                      AND p.full_name IS NOT NULL
                      AND p.full_name != ''
                    ORDER BY o.influence_tier, p.full_name
                """, (min_tier,))
                pythia_contacts = [dict(r) for r in cur.fetchall()]
        log.info(f"find_influential_in_area: {len(pythia_contacts)} Pythia contacts at tier<={min_tier}")
    except Exception as e:
        log.warning(f"Pythia contacts fetch error: {e}")

    # ── Step 2: Voter-lookup each Pythia contact in parallel ──────────────────
    def _lookup_with_context(contact):
        name = contact.get("full_name", "").strip()
        if not name:
            return contact, None
        voter = lookup_voter(name)
        return contact, voter

    pythia_with_voter = []
    with _cf.ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(_lookup_with_context, c): c for c in pythia_contacts}
        for fut in _cf.as_completed(futures):
            try:
                contact, voter = fut.result()
                pythia_with_voter.append((contact, voter))
            except Exception:
                pythia_with_voter.append((futures[fut], None))

    # ── Step 3: Build district/zip filters ───────────────────────────────────
    zip_f  = zip_code.strip()
    ad_f   = str(assembly_district).lstrip("0") if assembly_district else ""
    sd_f   = str(state_senate_district).lstrip("0") if state_senate_district else ""
    cd_f   = str(congressional_district).lstrip("0") if congressional_district else ""
    bor_f  = borough.lower().strip()

    BOROUGH_COUNTIES = {
        "manhattan": "31", "new york": "31",
        "brooklyn": "24",  "kings": "24",
        "queens": "41",
        "bronx": "03",
        "staten island": "43", "richmond": "43",
    }
    county_f = BOROUGH_COUNTIES.get(bor_f, "")

    def _voter_matches(voter: dict | None) -> bool:
        """Check if a voter record matches the requested location filters."""
        if not voter:
            return False
        if zip_f   and voter.get("zip","").strip()                   != zip_f:  return False
        if county_f and voter.get("county_code","").strip()          != county_f: return False
        if ad_f    and voter.get("ad","").lstrip("0")                != ad_f:   return False
        if sd_f    and voter.get("sd","").lstrip("0")                != sd_f:   return False
        if cd_f    and voter.get("cd","").lstrip("0")                != cd_f:   return False
        return True

    has_location_filter = any([zip_f, county_f, ad_f, sd_f, cd_f])

    # ── Step 4: Build Pythia results ──────────────────────────────────────────
    results = []
    seen_names: set[str] = set()

    for contact, voter in sorted(
        pythia_with_voter,
        key=lambda x: (x[0].get("influence_tier") or 99,
                       -(x[1].get("voter_score", 0) if x[1] else 0))
    ):
        name = contact.get("full_name", "").strip()
        if not name or name in seen_names:
            continue

        voter_matches = _voter_matches(voter)

        # Include if: voter record matches filter, OR no location filter set,
        # OR include_unmatched_pythia and this is a meaningful tier contact
        if has_location_filter and not voter_matches:
            if not include_unmatched_pythia:
                continue
            # For unmatched, only include Tier 1 — they're important regardless of address
            if (contact.get("influence_tier") or 99) > 1:
                continue

        seen_names.add(name)
        entry = {
            "name":             name,
            "source":           "pythia",
            "influence_tier":   contact.get("influence_tier"),
            "org":              contact.get("org_name", ""),
            "job_title":        contact.get("job_title", ""),
            "pythia_id":        contact.get("id"),
            "in_pythia":        True,
            "voter_score":      voter.get("voter_score", 0) if voter else None,
            "ge_years":         voter.get("ge_years", "") if voter else "",
            "primary_years":    voter.get("primary_years", "") if voter else "",
            "party":            PARTY_LABELS.get(voter.get("party",""), voter.get("party","")) if voter else "",
            "address":          voter.get("address","") if voter else contact.get("personal_address_line_1",""),
            "zip":              voter.get("zip","") if voter else contact.get("personal_postal_code",""),
            "city":             voter.get("city","") if voter else contact.get("personal_city",""),
            "assembly_district":voter.get("ad","") if voter else "",
            "state_senate_district": voter.get("sd","") if voter else "",
            "congressional_district": voter.get("cd","") if voter else "",
            "voter_matched":    bool(voter),
            "address_source":   "voter_file" if voter else "pythia",
        }
        results.append(entry)

    log.info(f"find_influential_in_area: {len(results)} Pythia matches after location filter")

    # ── Step 5: Add super voters from the area not already in Pythia ─────────
    if include_super_voters and has_location_filter:
        voter_rows = find_super_voters(
            county=borough or "brooklyn",
            min_voter_score=10,
            assembly_district=assembly_district,
            state_senate_district=state_senate_district,
            congressional_district=congressional_district,
            cross_reference_finance=False,
            limit=50,
        )
        # If zip filter, apply it to voter results
        if zip_f:
            voter_rows = [v for v in voter_rows if v.get("zip","").strip() == zip_f]

        # Build a name set from Pythia results for deduplication
        pythia_name_set = {normalize(r["name"]) for r in results}

        for v in voter_rows:
            if isinstance(v, dict) and "error" in v:
                continue
            vname = f"{(v.get('first') or '').title()} {(v.get('last') or '').title()}".strip()
            if normalize(vname) in pythia_name_set:
                continue
            seen_names.add(vname)
            results.append({
                "name":             vname,
                "source":           "voter_file",
                "influence_tier":   None,
                "org":              "",
                "job_title":        "",
                "pythia_id":        None,
                "in_pythia":        False,
                "voter_score":      v.get("voter_score", 0),
                "ge_years":         v.get("ge_years", ""),
                "primary_years":    v.get("primary_years", ""),
                "party":            PARTY_LABELS.get(v.get("party",""), v.get("party","")),
                "address":          v.get("address",""),
                "zip":              v.get("zip",""),
                "city":             v.get("city",""),
                "assembly_district": v.get("ad",""),
                "state_senate_district": v.get("sd",""),
                "congressional_district": v.get("cd",""),
                "voter_matched":    True,
                "address_source":   "voter_file",
            })

    # ── Step 6: Final sort and trim ───────────────────────────────────────────
    RECENCY = {"2025": 3, "2024": 2, "2023": 1}
    def _score(r):
        tier_score = 10 - (r.get("influence_tier") or 10)   # tier 1 = 9, tier 2 = 8, None = 0
        voter_score = r.get("voter_score") or 0
        bonus = sum(RECENCY.get(yr.strip(), 0)
                    for f in ("ge_years","primary_years")
                    for yr in (r.get(f) or "").split(",") if yr.strip())
        return (tier_score * 100) + voter_score + bonus * 0.5

    results.sort(key=lambda x: -_score(x))
    log.info(f"find_influential_in_area: returning {min(limit, len(results))} of {len(results)} total")
    return results[:limit]

# ─── Core enrichment ──────────────────────────────────────────────────────────

def enrich_person(person_name: str) -> dict:
    import concurrent.futures as _cf

    contacts = get_all_contacts()
    index, keys = build_index(contacts)
    subject, _ = best_match(person_name, index, keys)
    subject_id = subject["id"] if subject else None

    findings = {
        "subject_name": person_name,
        "matched_in_db": bool(subject),
        "db_match_name": subject.get("_display") if subject else None,
        "db_orgs": subject.get("orgs") if subject else None,
        "known_donors_in_db": [],
        "known_recipients_in_db": [],
        "co_donors_in_db": [],
        "voter_profile": None,
        "lobbied_by": [],
        "nys_lobbied_by": [],
        "federal_donors": [],
        "federal_lobbied_by": [],
        "lobbying_clients_in_db": [],
        "new_connections_written": 0,
    }

    # 1. Who donated TO this person? — fetch all sources in parallel
    with _cf.ThreadPoolExecutor(max_workers=8) as pool:
        f_cfb_recv  = pool.submit(cfb_donations_received, person_name)
        # Skip legacy fec_donations_to for officials in FEDERAL_OFFICIALS table
        # (fec_top_donors handles them via committee ID; fec_donations_to would 422)
        _last_lower = person_name.strip().split()[-1].lower()
        _use_legacy_fec = _last_lower not in FEDERAL_OFFICIALS
        f_fec_recv  = pool.submit(fec_donations_to, person_name) if _use_legacy_fec else pool.submit(lambda: [])
        f_boe_recv  = pool.submit(boe_donors_to, person_name)
        f_cfb_made  = pool.submit(cfb_donations_made, person_name)
        f_fec_made  = pool.submit(fec_donations_by, person_name)
        f_boe_made  = pool.submit(boe_donations_by, person_name)
        f_nyc_lobby  = pool.submit(nyc_lobbying_targets, person_name)
        f_nys_lobby  = pool.submit(nys_lobbying_targets, person_name)
        # Federal LDA + FEC donors — only meaningful for federal officials
        f_fed_lobby  = pool.submit(lda_lobbying_targeting, person_name)
        f_fec_donors = pool.submit(fec_top_donors, person_name)

    all_received = []
    for row in (f_cfb_recv.result() or []):
        d = (row.get("contributor_name") or "").strip()
        if d: all_received.append({"donor_name": d, "amount": float(row.get("amount") or 0),
                                    "year": (row.get("date") or "")[:4], "source": "NYC CFB"})
    for row in (f_fec_recv.result() or []):
        d = (row.get("contributor_name") or "").strip()
        if d: all_received.append({"donor_name": d,
                                    "amount": float(row.get("contribution_receipt_amount") or 0),
                                    "year": (row.get("contribution_receipt_date") or "")[:4], "source": "FEC"})
    for row in (f_boe_recv.result() or []):
        d = (row.get("contributor_name") or "").strip()
        if d: all_received.append({"donor_name": d, "amount": float(row.get("amount") or 0),
                                    "year": row.get("election_year") or (row.get("date") or "")[:4],
                                    "source": "NYS BOE"})

    for item in all_received:
        dc, _ = best_match(item["donor_name"], index, keys)
        if not dc: continue
        findings["known_donors_in_db"].append({
            "name": dc["_display"], "person_id": dc["id"], "orgs": dc.get("orgs", ""),
            "amount": item["amount"], "source": item["source"], "year": item["year"],
        })
        write_finance_note(dc["id"],
            f"[{item['source']} {item['year']}] Donated ${item['amount']:,.0f} to {person_name} (auto-detected)")
        if subject_id and write_relationship(dc["id"], subject_id, "Campaign Donor",
                f"Donated to {person_name}",
                f"${item['amount']:,.0f} | Source: {item['source']} | {item['year']}"):
            findings["new_connections_written"] += 1

    # 2. Who did THIS person donate to? — use results fetched in parallel above
    rmap: dict[str, dict] = {}
    for row in (f_cfb_made.result() or []):
        c = (row.get("candidate_name") or "").strip()
        if c:
            if c not in rmap: rmap[c] = {"amount": 0, "year": (row.get("date") or "")[:4], "source": "NYC CFB"}
            rmap[c]["amount"] += float(row.get("amount") or 0)
    for row in (f_fec_made.result() or []):
        c = (row.get("committee_name") or "").strip()
        if c:
            if c not in rmap: rmap[c] = {"amount": 0, "year": (row.get("contribution_receipt_date") or "")[:4], "source": "FEC"}
            rmap[c]["amount"] += float(row.get("contribution_receipt_amount") or 0)
    for row in (f_boe_made.result() or []):
        c = (row.get("candidate_name") or "").strip()
        if c:
            if c not in rmap: rmap[c] = {"amount": 0, "year": row.get("election_year") or "", "source": "NYS BOE"}
            rmap[c]["amount"] += float(row.get("amount") or 0)

    for cname, info in sorted(rmap.items(), key=lambda x: -x[1]["amount"])[:15]:
        cc, _ = best_match(cname, index, keys)
        findings["known_recipients_in_db"].append({
            "candidate_name": cname, "in_db": bool(cc),
            "db_match": cc["_display"] if cc else None, **info,
        })
        if subject_id and cc:
            write_finance_note(subject_id,
                f"[{info['source']} {info['year']}] Donated ${info['amount']:,.0f} to {cname} (auto-detected)")
            if write_relationship(subject_id, cc["id"], "Campaign Donor",
                    f"Donated to {cname}",
                    f"${info['amount']:,.0f} | Source: {info['source']} | {info['year']}"):
                findings["new_connections_written"] += 1


    # ── Federal FEC donors — who gave to this person's campaign committees ────
    fec_donor_rows = f_fec_donors.result() or []
    log.info(f"FEC donors: {len(fec_donor_rows)} unique contributors — enriching with LDA cross-ref...")

    # Cross-reference each donor's employer against LDA registrants database.
    # Lobbyist-linked donors bubble to the top; each gets active client list.
    fec_donor_rows = lda_enrich_donors(fec_donor_rows)

    for donor in fec_donor_rows[:50]:
        dname = donor.get("contributor_name", "").strip()
        if not dname:
            continue
        dc, _ = best_match(dname, index, keys)
        entry = {
            "contributor_name":   dname,
            "employer":           donor.get("employer", ""),
            "occupation":         donor.get("occupation", ""),
            "state":              donor.get("state", ""),
            "amount":             donor.get("total_amount", 0.0),
            "latest_date":        donor.get("latest_date", ""),
            "in_db":              bool(dc),
            "person_id":          dc["id"] if dc else None,
            "db_name":            dc["_display"] if dc else None,
            "orgs":               dc.get("orgs", "") if dc else "",
            "source":             "FEC",
            # LDA cross-reference fields
            "is_lda_registrant":  donor.get("is_lda_registrant", False),
            "lda_registrant_name":donor.get("lda_registrant_name", ""),
            "lda_firm_description":donor.get("lda_firm_description", ""),
            "lda_active_clients": donor.get("lda_active_clients", []),
        }
        findings["federal_donors"].append(entry)
        if dc and subject_id:
            rel_note = f"${donor.get('total_amount',0):,.0f} | {donor.get('employer','')}"
            if donor.get("is_lda_registrant"):
                rel_note += f" [LDA: {donor.get('lda_registrant_name','')}]"
            write_finance_note(dc["id"],
                f"[FEC] Donated ${donor.get('total_amount',0):,.0f} to {person_name}'s campaign committee"
                + (f" — employer is registered LDA lobbyist: {donor.get('lda_registrant_name','')}"
                   if donor.get("is_lda_registrant") else ""))
            if write_relationship(dc["id"], subject_id, "Campaign Donor",
                    f"Donated to {person_name} (FEC)",
                    rel_note):
                findings["new_connections_written"] += 1

    # 3. Co-donors
    if rmap:
        top = max(rmap, key=lambda k: rmap[k]["amount"])
        seen: set[str] = set()
        for row in list(cfb_donations_received(top, limit=100)) + list(boe_donors_to(top)):
            d = (row.get("contributor_name") or "").strip()
            if not d or normalize(d) == normalize(person_name): continue
            m, _ = best_match(d, index, keys)
            if m and m["id"] not in seen:
                seen.add(m["id"])
                findings["co_donors_in_db"].append({
                    "name": m["_display"], "person_id": m["id"], "orgs": m.get("orgs", ""),
                    "shared_candidate": top, "their_amount": float(row.get("amount") or 0),
                    "source": "NYS BOE" if row.get("election_year") else "NYC CFB",
                })
                if subject_id and m["id"] != subject_id:
                    if write_relationship(subject_id, m["id"], "Co-Donor",
                            f"Co-donors to {top}", f"Both donated to {top} (auto-detected)"):
                        findings["new_connections_written"] += 1



    # ── Voter file cross-reference ────────────────────────────────────────────
    voter = lookup_voter(person_name)
    if voter:
        party_code  = voter.get("party") or voter.get("party_code") or ""
        party_label = PARTY_LABELS.get(party_code, party_code)
        findings["voter_profile"] = {
            "registered_party":       party_label,
            "party_code":             party_code,
            "address":                voter.get("address", ""),
            "city":                   voter.get("city", ""),
            "zip":                    voter.get("zip", ""),
            "county":                 voter.get("county") or voter.get("county_name", ""),
            "congressional_district": voter.get("cd", ""),
            "state_senate_district":  voter.get("sd", ""),
            "assembly_district":      voter.get("ad", ""),
            "registered_since":       voter.get("regdate", ""),
            "general_elections_voted":voter.get("ge_votes") or voter.get("general_elections_voted", 0),
            "primaries_voted":        voter.get("primary_votes") or voter.get("primaries_voted", 0),
            "voter_score":            voter.get("voter_score", 0),
            "ge_years":               voter.get("ge_years", ""),
            "primary_years":          voter.get("primary_years", ""),
            "off_year_years":         voter.get("off_year_years", ""),
            "sboeid":                 voter.get("sboeid", ""),
        }
        log.info(f"Voter match for {person_name}: {party_label}, "
                 f"score={voter['voter_score']}, GE={voter['ge_votes']}")
    else:
        log.info(f"No voter file match for {person_name}")

    # 4. Who lobbied THIS person? (NYC City Clerk eLobbyist data)
    try:
        log.info(f"Processing NYC lobbying data for {person_name}...")
        lobby_rows = f_nyc_lobby.result() or []
        log.info(f"Got {len(lobby_rows)} raw NYC lobbying rows")
        deduped_lobbying = _dedupe_lobbying(lobby_rows, person_name)
        log.info(f"Deduped to {len(deduped_lobbying)} unique client/lobbyist pairs")

        for item in deduped_lobbying[:50]:
            # Summarize how subject is listed in this filing
            roles = item.get("subject_roles", [])
            role_summary = "; ".join(
                r["role_in_filing"] for r in roles if r.get("role_in_filing") and r["role_in_filing"] != "Unknown"
            ) if roles else ""

            entry = {
                "lobbyist": item["lobbyist_name"],
                "client": item["client_name"],
                "client_industry": item["client_industry"],
                "year": item["year"],
                "compensation": item["compensation"],
                "activities": item["activities"][:200] if item["activities"] else "",
                "subject_listed_as": role_summary or "Lobbying target",
                "subject_roles_detail": roles,
                "lobbyist_in_db": False,
                "client_in_db": False,
                "lobbyist_person_id": None,
                "client_person_id": None,
            }

            # Match lobbyist principal against contacts
            lpo = item.get("lobbyist_po", "")
            if lpo:
                lm, _ = best_match(lpo, index, keys)
                if lm:
                    entry["lobbyist_in_db"] = True
                    entry["lobbyist_person_id"] = lm["id"]
                    entry["lobbyist_db_name"] = lm["_display"]
                    write_finance_note(lm["id"],
                        f"[NYC Lobbying {item['year']}] Lobbied {person_name} on behalf of {item['client_name']}")
                    if subject_id:
                        if write_relationship(lm["id"], subject_id, "Lobbyist",
                                f"Lobbied {person_name} ({item['year']})",
                                f"Client: {item['client_name']} | Compensation: ${item['compensation']:,.0f}"):
                            findings["new_connections_written"] += 1

            # Match client principal against contacts
            cpo = item.get("client_po", "")
            if cpo:
                cm, _ = best_match(cpo, index, keys)
                if cm:
                    entry["client_in_db"] = True
                    entry["client_person_id"] = cm["id"]
                    entry["client_db_name"] = cm["_display"]
                    write_finance_note(cm["id"],
                        f"[NYC Lobbying {item['year']}] Hired lobbyist to target {person_name} re: {item['activities'][:100]}")
                    if subject_id:
                        if write_relationship(cm["id"], subject_id, "Lobbying Client",
                                f"Hired lobbyist targeting {person_name} ({item['year']})",
                                f"Lobbyist: {item['lobbyist_name']} | Compensation: ${item['compensation']:,.0f}"):
                            findings["new_connections_written"] += 1

            findings["lobbied_by"].append(entry)


        # ── 4c. NYS state-level lobbying targeting this official ──────────────
        nys_rows = f_nys_lobby.result() or []
        nys_deduped = _dedupe_nys_lobbying(nys_rows)
        log.info(f"NYS deduped: {len(nys_deduped)} unique lobbyist/client pairs")

        for item in nys_deduped[:50]:
            entry = {
                "lobbyist": item["lobbyist_name"],
                "client": item["client_name"],
                "year": item["year"],
                "compensation": item["compensation"],
                "government_body": item["government_body"],
                "subjects": item["subjects"][:200] if item["subjects"] else "",
                "individual_lobbyists": item["individual_lobbyists"][:200] if item["individual_lobbyists"] else "",
                "lobbyist_in_db": False,
                "client_in_db": False,
                "lobbyist_person_id": None,
                "client_person_id": None,
                "source": "NYS COELIG",
            }

            # Match individual lobbyists (semicolon-separated "Last, First" names)
            for raw in item["individual_lobbyists"].split(";"):
                raw = raw.strip()
                if not raw:
                    continue
                # Convert "Last, First" -> "First Last" for matching
                parts = raw.split(",", 1)
                name = f"{parts[1].strip()} {parts[0].strip()}" if len(parts) == 2 else raw
                lm, _ = best_match(name, index, keys)
                if lm:
                    entry["lobbyist_in_db"] = True
                    entry["lobbyist_person_id"] = lm["id"]
                    entry["lobbyist_db_name"] = lm["_display"]
                    write_finance_note(lm["id"],
                        f"[NYS Lobbying {item['year']}] Lobbied {person_name} on behalf of {item['client_name']}")
                    if subject_id:
                        if write_relationship(lm["id"], subject_id, "Lobbyist",
                                f"Lobbied {person_name} ({item['year']})",
                                f"Client: {item['client_name']} | NYS COELIG | ${item['compensation']:,.0f}"):
                            findings["new_connections_written"] += 1
                    break  # one match per entry is enough

            # Match client name
            cm, _ = best_match(item["client_name"], index, keys)
            if cm:
                entry["client_in_db"] = True
                entry["client_person_id"] = cm["id"]
                entry["client_db_name"] = cm["_display"]
                write_finance_note(cm["id"],
                    f"[NYS Lobbying {item['year']}] Hired lobbyist to target {person_name} re: {item['subjects'][:100]}")
                if subject_id:
                    if write_relationship(cm["id"], subject_id, "Lobbying Client",
                            f"Hired lobbyist targeting {person_name} ({item['year']})",
                            f"Lobbyist: {item['lobbyist_name']} | NYS COELIG | ${item['compensation']:,.0f}"):
                        findings["new_connections_written"] += 1

            findings["nys_lobbied_by"].append(entry)


        # ── 4d. Federal LDA LD-203 — lobbyists who contributed to this official ───
        lda_items = f_fed_lobby.result() or []
        log.info(f"Federal LDA LD-203: {len(lda_items)} contribution items for {person_name}")
        build_federal_lobbying_profile(
            lda_items, index, keys, subject_id, person_name, findings
        )

        # Did this person/org hire lobbyists?
        client_rows = nyc_lobbying_by_client(person_name)
        if client_rows:
            for item in _dedupe_lobbying(client_rows)[:10]:
                findings["lobbying_clients_in_db"].append({
                    "as_client": item["client_name"],
                    "hired_lobbyist": item["lobbyist_name"],
                    "year": item["year"],
                    "compensation": item["compensation"],
                    "activities": item["activities"][:200] if item["activities"] else "",
                })
    except Exception as e:
        log.error(f"Lobbying lookup failed for {person_name}: {e}", exc_info=True)
        findings["lobbying_error"] = str(e)

    return findings

# ─── MCP Server ───────────────────────────────────────────────────────────────

mcp_server = Server("finance-enrichment")

@mcp_server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="lookup_finance_connections",
            description=(
                "Look up campaign finance AND lobbying connections for a named person using NYC CFB, FEC, NYS BOE, NYC City Clerk eLobbyist, NYS COELIG, and Federal LDA data. "
                "Call this automatically whenever a query involves political influence, access to an elected official, "
                "or background on a political figure. "
                "Returns: who donated to them, who they donated to, co-donors in your contacts database, AND who lobbied them at city (NYC), state (NYS COELIG), and federal (LDA) levels. "
                "Also writes new relationships back to the database in real time. "
                "ALWAYS call this for political figures before answering influence questions."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "person_name": {"type": "string", "description": "Full name of the politician or contact to look up"}
                },
                "required": ["person_name"]
            }
        ),
        types.Tool(
            name="get_person_profile",
            description=(
                "Get a comprehensive profile for ANY person by name — they do NOT need to be "
                "in the Pythia contacts database. Returns: voter registration (party, voting "
                "history, districts), campaign donations they made (BOE + CFB), lobbying "
                "activity targeting them (NYC + NYS), and Pythia DB match if one exists. "
                "Use this when you want a full picture of someone's political engagement and "
                "financial history without needing them to already be in the system."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "person_name": {"type": "string", "description": "Full name of the person"},
                },
                "required": ["person_name"]
            }
        ),
        types.Tool(
            name="find_super_voters",
            description=(
                "Find high-engagement NYC voters in a given county or district, "
                "cross-referenced with campaign finance data to surface those who are "
                "also significant donors. Useful for identifying politically active "
                "individuals with financial influence. "
                "Filter by county (manhattan/brooklyn/queens/bronx/staten island), "
                "party (DEM/REP/WOR etc), assembly district, senate district, "
                "congressional district, and minimum voter score. "
                "Returns voters sorted by donation total, with their voting history, "
                "party, address, districts, and top campaign contributions."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "county":        {"type": "string", "description": "NYC county: manhattan, brooklyn, queens, bronx, or staten island"},
                    "min_voter_score": {"type": "integer", "description": "Minimum voter score (GE + primary votes). Default 10."},
                    "party":         {"type": "string", "description": "Party filter: DEM, REP, WOR, BLK, CON, etc. Optional."},
                    "assembly_district": {"type": "string", "description": "Assembly district number. Optional."},
                    "state_senate_district": {"type": "string", "description": "State senate district number. Optional."},
                    "congressional_district": {"type": "string", "description": "Congressional district number. Optional."},
                    "cross_reference_finance": {"type": "boolean", "description": "Cross-reference with BOE donation data. Default true."},
                    "limit":         {"type": "integer", "description": "Max results to return. Default 50."},
                },
                "required": []
            }
        ),
        types.Tool(
            name="find_financial_path",
            description=(
                "Find whether two people share financial/donor connections — "
                "i.e. co-donors to the same candidate, or one donated to the other's allies. "
                "Use when asked about the relationship or connection between two people."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "person_a": {"type": "string", "description": "First person's name"},
                    "person_b": {"type": "string", "description": "Second person's name"},
                },
                "required": ["person_a", "person_b"]
            }
        ),
        types.Tool(
            name="find_influential_in_area",
            description=(
                "Find the most influential people in a specific geographic area by combining "
                "Pythia contacts (ranked by influence tier) with high-engagement voters from "
                "the voter file. Tier 1 and 2 Pythia contacts are looked up in the voter file "
                "to get their home address and district assignments. Also surfaces high-scoring "
                "super voters in the area who are not yet in Pythia. "
                "Filter by zip code, borough, assembly district, state senate district, or "
                "congressional district. Results ranked by influence tier first, then voter score. "
                "Use when asked about influential people in a neighborhood, zip code, or district."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "zip_code":               {"type": "string", "description": "5-digit zip code. Optional."},
                    "borough":                {"type": "string", "description": "NYC borough: manhattan, brooklyn, queens, bronx, staten island. Optional."},
                    "assembly_district":      {"type": "string", "description": "Assembly district number. Optional."},
                    "state_senate_district":  {"type": "string", "description": "State senate district number. Optional."},
                    "congressional_district": {"type": "string", "description": "Congressional district number. Optional."},
                    "min_tier":               {"type": "integer", "description": "Max influence tier to include (1=top only, 2=top two tiers, 3=all). Default 2."},
                    "include_super_voters":   {"type": "boolean", "description": "Also include high-scoring voters not in Pythia. Default true."},
                    "include_unmatched_pythia": {"type": "boolean", "description": "Include Tier 1 Pythia contacts even if not found in voter file. Default true."},
                    "limit":                  {"type": "integer", "description": "Max results. Default 25."},
                },
                "required": []
            }
        ),
        types.Tool(
            name="fetch_990_data",
            description=(
                "Fetch IRS Form 990 data (annual revenue, total assets, NTEE category) for Pythia "
                "organizations via the free ProPublica Nonprofit Explorer API. "
                "Matched revenue data is used by compute_influence_scores to objectively weight "
                "institutional authority by org budget size — replacing subjective manual tier "
                "assignments for nonprofits. "
                "Government entities (city agencies, elected officials) and for-profits are "
                "automatically skipped. "
                "Processes up to 50 orgs per call (~40-50 seconds); run multiple times until "
                "remaining_unprocessed reaches 0. Safe to re-run — skips already-fetched orgs. "
                "Run setup_influence_tables first. "
                "For bulk processing without session timeouts, use start_990_background_fetch instead."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "description": "Max orgs per call. Default 50."},
                    "force": {"type": "boolean", "description": "Re-fetch already-processed orgs. Default false."},
                },
                "required": [],
            },
        ),
        types.Tool(
            name="fetch_union_data",
            description=(
                "Fetch DOL OLMS LM-2/LM-3 union financial data (total receipts, assets, membership) "
                "for Pythia organizations via the DOL Office of Labor-Management Standards API. "
                "Union dues receipts feed directly into compute_influence_scores as the institutional "
                "weight for union orgs — covering UFT, DC 37, SEIU 32BJ, TWU Local 100, CWA, "
                "IBT, IUOE, PBA, and all other major NYC unions. "
                "Skips government entities and orgs already matched via IRS 990. "
                "Processes up to 50 orgs per call. Safe to re-run. "
                "Run setup_influence_tables first. "
                "For bulk processing, use start_union_data_background_fetch."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "description": "Max orgs per call. Default 50."},
                    "force": {"type": "boolean", "description": "Re-fetch already-processed orgs. Default false."},
                },
                "required": [],
            },
        ),
        types.Tool(
            name="test_union_lookup",
            description=(
                "Diagnostic tool: run a single OLMS lookup for a specific org name and return "
                "the full raw result. Use this immediately after first deploy to confirm the "
                "OLMS API is responding correctly before running the full fetch. "
                "Example: test_union_lookup('SEIU 32BJ')"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "org_name": {"type": "string", "description": "Union name to look up, e.g. 'DC 37' or 'UFT'"},
                },
                "required": ["org_name"],
            },
        ),
        types.Tool(
            name="start_union_data_background_fetch",
            description=(
                "Start fetching DOL OLMS union LM-2 data for all remaining orgs in a background thread. "
                "Returns immediately — immune to MCP session timeouts. "
                "Call get_union_fetch_status to monitor progress."
            ),
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        types.Tool(
            name="get_union_fetch_status",
            description="Return current status of the background union data fetch.",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        types.Tool(
            name="start_990_background_fetch",
            description=(
                "Start fetching IRS 990 data for ALL remaining organizations in a background thread "
                "running inside Railway. Returns immediately — no MCP session timeout possible. "
                "The fetch runs for 15-25 minutes processing all remaining orgs in 100-org batches. "
                "Use this instead of repeated fetch_990_data calls when MCP sessions are unstable. "
                "Call get_990_fetch_status to monitor progress."
            ),
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        types.Tool(
            name="get_990_fetch_status",
            description=(
                "Return the current status of the background 990 fetch, including live counts "
                "from the database (total processed, matched, remaining). "
                "Also works as a general 990 data coverage check even if no background fetch is running."
            ),
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        # ── Influence ranking tools ────────────────────────────────────────
        types.Tool(
            name="setup_influence_tables",
            description=(
                "One-time setup: creates the three database tables needed for influence scoring "
                "(people_voter_enrichment, people_influence_scores, _finance_migrations). "
                "Safe to call multiple times — uses IF NOT EXISTS. "
                "Call this FIRST before running any other influence ranking tools. "
                "To undo everything: call rollback_influence_tables with confirm=true."
            ),
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        types.Tool(
            name="rollback_influence_tables",
            description=(
                "DESTRUCTIVE: drops all three influence ranking tables and all data in them. "
                "Does NOT touch any existing Pythia tables (people_person, organizations_organization, etc.). "
                "Requires confirm=true to prevent accidental execution."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "confirm": {
                        "type": "boolean",
                        "description": "Must be true to execute. Anything else aborts with no changes."
                    }
                },
                "required": ["confirm"],
            },
        ),
        types.Tool(
            name="enrich_voter_data",
            description=(
                "Match every active Pythia contact to the NYC voter file by name, then store "
                "their party registration, voter score, assembly/senate/congressional districts, "
                "and home address in people_voter_enrichment. "
                "Safe to re-run — upserts on conflict. "
                "Run setup_influence_tables first. "
                "Use the optional limit parameter to test on a small batch before running on all contacts."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Max contacts to process. 0 or omit = all contacts (default)."
                    }
                },
                "required": [],
            },
        ),
        types.Tool(
            name="compute_influence_scores",
            description=(
                "Compute v1 influence scores for every active Pythia contact and store results "
                "in people_influence_scores. Five weighted components: "
                "institutional authority (35%), financial influence (25%), "
                "lobbying exposure (20%), network connections (15%), civic engagement (5%). "
                "Safe to re-run — updates existing scores. "
                "Run setup_influence_tables and enrich_voter_data first for best results. "
                "Also run lookup_finance_connections on key contacts before scoring so "
                "campaign finance and lobbying data is written to the database."
            ),
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        types.Tool(
            name="rank_influential_people",
            description=(
                "Return a ranked list of the most influential people based on stored influence scores. "
                "Filter by zip code, borough, assembly district, state senate district, or "
                "congressional district. No filter = full citywide ranking. "
                "Results include score breakdown, org affiliations, voter data, and district assignments. "
                "Run compute_influence_scores first to populate scores."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "zip_code": {
                        "type": "string",
                        "description": "5-digit zip code — filters to people whose voter-file address is in this zip."
                    },
                    "borough": {
                        "type": "string",
                        "description": "manhattan, brooklyn, queens, bronx, or staten island."
                    },
                    "assembly_district": {
                        "type": "string",
                        "description": "Assembly district number (e.g. '73')."
                    },
                    "state_senate_district": {
                        "type": "string",
                        "description": "State senate district number."
                    },
                    "congressional_district": {
                        "type": "string",
                        "description": "Congressional district number."
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results to return. Default 25."
                    },
                    "min_score": {
                        "type": "number",
                        "description": "Minimum composite score (0-100). Default 0."
                    },
                },
                "required": [],
            },
        ),
    ]

@mcp_server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    loop = asyncio.get_event_loop()
    log.info(f"Tool called: {name} args={arguments}")
    try:
        if name == "lookup_finance_connections":
            log.info(f"Finance lookup: {arguments['person_name']}")
            findings = await loop.run_in_executor(None, enrich_person, arguments["person_name"])
            return [types.TextContent(type="text", text=json.dumps(findings, indent=2, default=str))]

        elif name == "find_financial_path":
            log.info(f"Financial path: {arguments['person_a']} <-> {arguments['person_b']}")
            fa, fb = await asyncio.gather(
                loop.run_in_executor(None, enrich_person, arguments["person_a"]),
                loop.run_in_executor(None, enrich_person, arguments["person_b"]),
            )
            a_cands = {r["candidate_name"] for r in fa.get("known_recipients_in_db", [])}
            b_cands = {r["candidate_name"] for r in fb.get("known_recipients_in_db", [])}

            a_lobbied     = {r["client"] for r in fa.get("lobbied_by", [])}
            b_lobbied     = {r["client"] for r in fb.get("lobbied_by", [])}
            a_lobbied_nys = {r["client"] for r in fa.get("nys_lobbied_by", [])}
            b_lobbied_nys = {r["client"] for r in fb.get("nys_lobbied_by", [])}
            a_lobbied_fed = {r["registrant"] for r in fa.get("federal_lobbied_by", [])}
            b_lobbied_fed = {r["registrant"] for r in fb.get("federal_lobbied_by", [])}

            b_lobbied_a_nyc = [r for r in fa.get("lobbied_by", [])
                               if normalize(r.get("client","")) == normalize(arguments["person_b"])
                               or normalize(r.get("lobbyist","")) == normalize(arguments["person_b"])]
            b_lobbied_a_nys = [r for r in fa.get("nys_lobbied_by", [])
                               if normalize(r.get("client","")) == normalize(arguments["person_b"])
                               or normalize(r.get("lobbyist","")) == normalize(arguments["person_b"])]
            a_lobbied_b_nyc = [r for r in fb.get("lobbied_by", [])
                               if normalize(r.get("client","")) == normalize(arguments["person_a"])
                               or normalize(r.get("lobbyist","")) == normalize(arguments["person_a"])]
            a_lobbied_b_nys = [r for r in fb.get("nys_lobbied_by", [])
                               if normalize(r.get("client","")) == normalize(arguments["person_a"])
                               or normalize(r.get("lobbyist","")) == normalize(arguments["person_a"])]
            b_lobbied_a_fed = [r for r in fa.get("federal_lobbied_by", [])
                               if normalize(r.get("registrant","")) == normalize(arguments["person_b"])
                               or any(normalize(l) == normalize(arguments["person_b"])
                                      for l in r.get("lobbyists", []))]
            a_lobbied_b_fed = [r for r in fb.get("federal_lobbied_by", [])
                               if normalize(r.get("registrant","")) == normalize(arguments["person_a"])
                               or any(normalize(l) == normalize(arguments["person_a"])
                                      for l in r.get("lobbyists", []))]

            result = {
                "person_a": arguments["person_a"],
                "person_b": arguments["person_b"],
                "shared_donation_targets": list(a_cands & b_cands),
                "a_donated_to_b_allies": [r for r in fa.get("known_recipients_in_db", []) if r.get("in_db")],
                "b_donated_to_a_allies": [r for r in fb.get("known_recipients_in_db", []) if r.get("in_db")],
                "b_lobbied_a_nyc": b_lobbied_a_nyc,
                "a_lobbied_b_nyc": a_lobbied_b_nyc,
                "shared_nyc_lobbying_clients": list(a_lobbied & b_lobbied),
                "b_lobbied_a_nys": b_lobbied_a_nys,
                "a_lobbied_b_nys": a_lobbied_b_nys,
                "shared_nys_lobbying_clients": list(a_lobbied_nys & b_lobbied_nys),
                "b_lobbied_a_federal": b_lobbied_a_fed,
                "a_lobbied_b_federal": a_lobbied_b_fed,
                "shared_federal_lobbying_registrants": list(a_lobbied_fed & b_lobbied_fed),
            }
            return [types.TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

        elif name == "find_super_voters":
            log.info(f"find_super_voters: {arguments}")
            results = await loop.run_in_executor(None, lambda: find_super_voters(
                county=arguments.get("county", "brooklyn"),
                min_voter_score=int(arguments.get("min_voter_score", 10)),
                party=arguments.get("party", ""),
                assembly_district=arguments.get("assembly_district", ""),
                state_senate_district=arguments.get("state_senate_district", ""),
                congressional_district=arguments.get("congressional_district", ""),
                cross_reference_finance=bool(arguments.get("cross_reference_finance", True)),
                limit=int(arguments.get("limit", 50)),
            ))
            return [types.TextContent(type="text", text=json.dumps(results, indent=2, default=str))]

        elif name == "get_person_profile":
            log.info(f"get_person_profile: {arguments.get('person_name')}")
            profile = await loop.run_in_executor(
                None, get_person_profile, arguments["person_name"]
            )
            return [types.TextContent(type="text", text=json.dumps(profile, indent=2, default=str))]

        elif name == "find_influential_in_area":
            log.info(f"find_influential_in_area: {arguments}")
            results = await loop.run_in_executor(None, lambda: find_influential_in_area(
                zip_code=arguments.get("zip_code", ""),
                borough=arguments.get("borough", ""),
                assembly_district=arguments.get("assembly_district", ""),
                state_senate_district=arguments.get("state_senate_district", ""),
                congressional_district=arguments.get("congressional_district", ""),
                min_tier=int(arguments.get("min_tier", 2)),
                include_super_voters=bool(arguments.get("include_super_voters", True)),
                include_unmatched_pythia=bool(arguments.get("include_unmatched_pythia", True)),
                limit=int(arguments.get("limit", 25)),
            ))
            return [types.TextContent(type="text", text=json.dumps(results, indent=2, default=str))]

        else:
            # ── Influence ranking tools ────────────────────────────────────
            if name == "setup_influence_tables":
                log.info("setup_influence_tables called")
                result = await loop.run_in_executor(None, setup_influence_tables)
                return [types.TextContent(type="text", text=json.dumps(result, indent=2))]

            elif name == "rollback_influence_tables":
                if not arguments.get("confirm"):
                    return [types.TextContent(type="text", text=json.dumps(
                        {"error": "Aborted. Pass confirm=true to execute rollback."}))]
                log.info("rollback_influence_tables called — dropping tables")
                result = await loop.run_in_executor(None, rollback_influence_tables)
                return [types.TextContent(type="text", text=json.dumps(result, indent=2))]

            elif name == "fetch_990_data":
                lim   = int(arguments.get("limit", 50))
                force = bool(arguments.get("force", False))
                log.info(f"fetch_990_data called, limit={lim}, force={force}")
                result = await loop.run_in_executor(None, lambda: fetch_990_data_batch(lim, force))
                return [types.TextContent(type="text", text=json.dumps(result, indent=2))]

            elif name == "fetch_union_data":
                lim   = int(arguments.get("limit", 50))
                force = bool(arguments.get("force", False))
                log.info(f"fetch_union_data called, limit={lim}, force={force}")
                result = await loop.run_in_executor(None, lambda: fetch_union_data_batch(lim, force))
                return [types.TextContent(type="text", text=json.dumps(result, indent=2))]

            elif name == "test_union_lookup":
                org = arguments.get("org_name", "")
                log.info(f"test_union_lookup called: {org!r}")
                result = await loop.run_in_executor(None, lambda: test_union_lookup(org))
                return [types.TextContent(type="text", text=json.dumps(result, indent=2))]

            elif name == "start_union_data_background_fetch":
                log.info("start_union_data_background_fetch called")
                result = start_union_data_background_fetch()
                return [types.TextContent(type="text", text=json.dumps(result, indent=2))]

            elif name == "get_union_fetch_status":
                log.info("get_union_fetch_status called")
                result = await loop.run_in_executor(None, get_union_fetch_status)
                return [types.TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

            elif name == "start_990_background_fetch":
                log.info("start_990_background_fetch called")
                result = start_990_background_fetch()   # non-blocking, no executor needed
                return [types.TextContent(type="text", text=json.dumps(result, indent=2))]

            elif name == "get_990_fetch_status":
                log.info("get_990_fetch_status called")
                result = await loop.run_in_executor(None, get_990_fetch_status)
                return [types.TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

            elif name == "enrich_voter_data":
                lim = int(arguments.get("limit", 0))
                log.info(f"enrich_voter_data called, limit={lim}")
                result = await loop.run_in_executor(None, lambda: enrich_voter_data_batch(lim))
                return [types.TextContent(type="text", text=json.dumps(result, indent=2))]

            elif name == "compute_influence_scores":
                log.info("compute_influence_scores called")
                result = await loop.run_in_executor(None, compute_influence_scores_batch)
                return [types.TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

            elif name == "rank_influential_people":
                log.info(f"rank_influential_people called: {arguments}")
                result = await loop.run_in_executor(None, lambda: rank_influential_people(
                    zip_code               = arguments.get("zip_code", ""),
                    borough                = arguments.get("borough", ""),
                    assembly_district      = arguments.get("assembly_district", ""),
                    state_senate_district  = arguments.get("state_senate_district", ""),
                    congressional_district = arguments.get("congressional_district", ""),
                    limit                  = int(arguments.get("limit", 25)),
                    min_score              = float(arguments.get("min_score", 0)),
                ))
                return [types.TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

            else:
                return [types.TextContent(type="text", text=json.dumps({"error": f"Unknown tool: {name}"}))]

    except Exception as e:
        log.error(f"call_tool error in {name}: {e}", exc_info=True)
        return [types.TextContent(type="text", text=json.dumps({"error": str(e)}, indent=2))]


# ─── Starlette app ────────────────────────────────────────────────────────────

sse_transport = SseServerTransport("/messages/")

def _check_auth(request: Request) -> bool:
    expected = _cfg("MCP_API_KEY")
    if not expected:
        return True
    provided = request.headers.get("x-api-key") or request.query_params.get("api_key", "")
    return provided == expected

def _make_server() -> Server:
    """
    Create a FRESH Server instance for each SSE connection.

    mcp.server.Server stores _client_params and initialization state as
    instance variables. Calling .run() concurrently on the same instance
    causes initialization state to be shared across connections — the first
    connection's handshake completes the flag for all later connections,
    so they never finish their own InitializeRequest/InitializedNotification
    exchange and every tool call gets "Received request before initialization
    was complete".

    Solution: one Server instance per connection. The global mcp_server is
    kept only to register the handler functions via decorators; each actual
    SSE connection gets its own fresh copy.
    """
    s = Server("finance-enrichment")
    s.list_tools()(list_tools)
    s.call_tool()(call_tool)
    return s


async def handle_sse(request: Request):
    if not _check_auth(request):
        return Response("Unauthorized", status_code=401)
    log.info(f"SSE connection from {request.client}")
    conn_server = _make_server()   # ← fresh instance, no shared init state
    async with sse_transport.connect_sse(
        request.scope, request.receive, request._send
    ) as streams:
        await conn_server.run(streams[0], streams[1], conn_server.create_initialization_options())
    return Response()  # connect_sse managed the full response; return empty so Starlette doesn't raise TypeError

# /messages/ must be a raw ASGI app — handle_post_message sends its own response
# IMPORTANT: do NOT create a Request(scope, receive) here — it consumes the body,
# leaving handle_post_message with nothing to parse. Read auth from scope directly.
async def messages_asgi(scope, receive, send):
    """Raw ASGI wrapper with auth check that doesn't consume the request body."""
    expected = _cfg("MCP_API_KEY")
    if expected:
        # Read headers from scope without touching the body
        headers = {k.lower(): v for k, v in scope.get("headers", [])}
        api_key = headers.get(b"x-api-key", b"").decode()
        if not api_key:
            # Fallback: check query string
            qs = scope.get("query_string", b"").decode()
            for part in qs.split("&"):
                if part.startswith("api_key="):
                    api_key = part[8:]
                    break
        if api_key != expected:
            response = Response("Unauthorized", status_code=401)
            await response(scope, receive, send)
            return
    await sse_transport.handle_post_message(scope, receive, send)

async def healthcheck(request: Request):
    boe_status = f"{len(_boe_rows):,} rows loaded" if _boe_loaded else "not yet loaded"
    return Response(
        json.dumps({"status": "ok", "boe_csv": boe_status}),
        media_type="application/json"
    )

@asynccontextmanager
async def lifespan(app):
    log.info("=== finance-enrichment MCP server starting ===")
    log.info(f"DATABASE_URL set: {bool(os.environ.get('DATABASE_URL'))}")
    log.info(f"MCP_API_KEY set:  {bool(os.environ.get('MCP_API_KEY'))}")
    log.info(f"FEC_API_KEY set:  {bool(os.environ.get('FEC_API_KEY'))}")
    _load_boe_csv()
    # Eagerly load LDA registrants CSV and voter file
    _load_lda_registrants()
    _init_voter_db()
    # BOE donor index built lazily on first find_super_voters call
    log.info("=== Ready ===")
    yield

starlette_app = Starlette(
    lifespan=lifespan,
    routes=[
        Route("/health", healthcheck),
        Route("/sse", handle_sse),
        Mount("/messages/", app=messages_asgi),
    ],
)

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8000"))
    log.info(f"Listening on 0.0.0.0:{port}")
    uvicorn.run(starlette_app, host="0.0.0.0", port=port)
