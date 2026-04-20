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

# ─── Finance API calls ────────────────────────────────────────────────────────

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
            params["committee_id"] = cands[0].get("principal_committees", [{}])[0].get("id", "")
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

def _dedupe_lobbying(rows: list[dict]) -> list[dict]:
    """Collapse multiple period filings into one entry per client+lobbyist+year."""
    seen: dict[str, dict] = {}
    for row in rows:
        key = f"{row.get('client_name','')}|{row.get('lobbyist_name','')}|{row.get('report_year','')}"
        if key not in seen:
            seen[key] = {
                "lobbyist_name":    (row.get("lobbyist_name") or "").strip(),
                "client_name":      (row.get("client_name") or "").strip(),
                "client_industry":  (row.get("client_industry") or "").strip(),
                "year":             (row.get("report_year") or "").strip(),
                "compensation":     0.0,
                "activities":       row.get("lobbyist_activities") or row.get("periodic_activities") or "",
                "lobbyist_po":      (row.get("lobbyist_po") or "").strip(),
                "client_po":        (row.get("client_po") or "").strip(),
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
    "gillibrand": {"fec_id": "S4NY00145",
                   "fec_committees": [],
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
    for committee_id in committee_ids[:3]:  # cap at 3 committees
        try:
            resp = requests.get(f"{FEC_BASE}/schedules/schedule_a/",
                params={
                    "api_key": fec_key,
                    "committee_id": committee_id,
                    "sort": "-contribution_receipt_amount",
                    "sort_hide_null": True,
                    "per_page": min(limit, 100),
                    "is_individual": True,
                },
                timeout=15)
            resp.raise_for_status()
            results = resp.json().get("results", [])
            log.info(f"FEC committee {committee_id}: {len(results)} donors")
            all_donors.extend(results)
        except Exception as e:
            log.warning(f"FEC schedule_a error for {committee_id}: {e}")

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

# ─── Federal LDA Lobbying — who lobbied this official ────────────────────────

def lda_lobbying_targeting(official_name: str,
                            years: list[int] | None = None,
                            limit: int = 50) -> list[dict]:
    """
    Find federal LDA filings mentioning this official in specific_lobbying_issues.
    Uses the FEDERAL_OFFICIALS table to get the right search term.
    Skips state/local officials (Hochul, Adams) that don't appear in LDA.
    """
    last = official_name.strip().split()[-1].lower()
    official = FEDERAL_OFFICIALS.get(last)

    # State/local officials: no LDA presence
    if last in ("hochul", "adams", "james", "dinapoli", "mamdani", "cuomo"):
        log.info(f"LDA: {official_name} is state/local, skipping")
        return []

    search_term = official["lda_search"] if official else official_name.strip().split()[-1]

    if years is None:
        years = [2025, 2024]

    all_filings: list[dict] = []
    seen_uuids: set[str] = set()

    for year in years:
        try:
            resp = requests.get(f"{LDA_BASE}/filings/",
                params={
                    "specific_lobbying_issues": search_term,
                    "filing_year": year,
                    "filing_type": "Q1,Q2,Q3,Q4",
                    "limit": limit,
                },
                timeout=12,
                headers={"Accept": "application/json"})
            resp.raise_for_status()
            for f in resp.json().get("results", []):
                uid = f.get("filing_uuid") or str(f.get("id") or "")
                if uid and uid not in seen_uuids:
                    seen_uuids.add(uid)
                    all_filings.append(f)
            log.info(f"LDA {year}: {len(resp.json().get('results',[]))} filings for {search_term}")
        except Exception as e:
            log.warning(f"LDA targeting error {year}/{search_term}: {e}")

    log.info(f"LDA total: {len(all_filings)} filings for {official_name}")
    return all_filings

def _dedupe_lda_filings(filings: list[dict]) -> list[dict]:
    """Collapse LDA filings to one entry per registrant+client+year."""
    seen: dict[str, dict] = {}
    for f in filings:
        registrant = (f.get("registrant") or {}).get("name", "")
        client     = (f.get("client") or {}).get("name", "")
        year       = str(f.get("filing_year", ""))
        key        = f"{registrant}|{client}|{year}"

        if key not in seen:
            issues: set[str] = set()
            lobbyist_names: list[str] = []
            entities: set[str] = set()
            for act in (f.get("lobbying_activities") or []):
                if act.get("general_issue_code_display"):
                    issues.add(act["general_issue_code_display"])
                for lb in (act.get("lobbyists") or []):
                    lobj = lb.get("lobbyist") or {}
                    fn = lobj.get("first_name", "")
                    ln = lobj.get("last_name", "")
                    if fn or ln:
                        lobbyist_names.append(f"{fn} {ln}".strip())
                for ent in (act.get("government_entities") or []):
                    if ent.get("name"):
                        entities.add(ent["name"])
            seen[key] = {
                "registrant_name":    registrant,
                "client_name":        client,
                "year":               year,
                "filing_type":        f.get("filing_type_display", ""),
                "income":             float(f.get("income") or 0),
                "expenses":           float(f.get("expenses") or 0),
                "issues":             ", ".join(sorted(issues))[:300],
                "lobbyist_names":     list(set(lobbyist_names))[:10],
                "government_entities":list(entities)[:10],
            }
        else:
            seen[key]["income"]   += float(f.get("income") or 0)
            seen[key]["expenses"] += float(f.get("expenses") or 0)

    return sorted(seen.values(), key=lambda x: -(x["income"] + x["expenses"]))

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
        f_fec_recv  = pool.submit(fec_donations_to, person_name)
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
    log.info(f"FEC donors: {len(fec_donor_rows)} unique contributors")
    for donor in fec_donor_rows[:50]:
        dname = donor.get("contributor_name", "").strip()
        if not dname:
            continue
        dc, _ = best_match(dname, index, keys)
        entry = {
            "contributor_name": dname,
            "employer": donor.get("employer", ""),
            "occupation": donor.get("occupation", ""),
            "state": donor.get("state", ""),
            "amount": donor.get("total_amount", 0.0),
            "latest_date": donor.get("latest_date", ""),
            "in_db": bool(dc),
            "person_id": dc["id"] if dc else None,
            "db_name": dc["_display"] if dc else None,
            "orgs": dc.get("orgs", "") if dc else "",
            "source": "FEC",
        }
        findings["federal_donors"].append(entry)
        if dc and subject_id:
            write_finance_note(dc["id"],
                f"[FEC] Donated ${donor['total_amount']:,.0f} to {person_name}'s campaign committee")
            if write_relationship(dc["id"], subject_id, "Campaign Donor",
                    f"Donated to {person_name} (FEC)",
                    f"${donor['total_amount']:,.0f} | {donor.get('employer','')}"):
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


    # 4. Who lobbied THIS person? (NYC City Clerk eLobbyist data)
    try:
        log.info(f"Processing NYC lobbying data for {person_name}...")
        lobby_rows = f_nyc_lobby.result() or []
        log.info(f"Got {len(lobby_rows)} raw NYC lobbying rows")
        deduped_lobbying = _dedupe_lobbying(lobby_rows)
        log.info(f"Deduped to {len(deduped_lobbying)} unique client/lobbyist pairs")

        for item in deduped_lobbying[:50]:
            entry = {
                "lobbyist": item["lobbyist_name"],
                "client": item["client_name"],
                "client_industry": item["client_industry"],
                "year": item["year"],
                "compensation": item["compensation"],
                "activities": item["activities"][:200] if item["activities"] else "",
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


        # ── 4d. Federal LDA lobbying targeting this official ─────────────────
        lda_filings = f_fed_lobby.result() or []
        lda_deduped = _dedupe_lda_filings(lda_filings)
        log.info(f"Federal LDA deduped: {len(lda_deduped)} unique registrant/client pairs")

        for item in lda_deduped[:30]:
            entry = {
                "registrant": item["registrant_name"],
                "client": item["client_name"],
                "year": item["year"],
                "filing_type": item["filing_type"],
                "income": item["income"],
                "expenses": item["expenses"],
                "issues": item["issues"],
                "government_entities": item["government_entities"],
                "lobbyist_names": item["lobbyist_names"],
                "registrant_in_db": False,
                "client_in_db": False,
                "source": "Federal LDA",
            }

            # Match individual lobbyists against contacts
            for lname in item["lobbyist_names"]:
                lm, _ = best_match(lname, index, keys)
                if lm:
                    entry["registrant_in_db"] = True
                    entry["registrant_person_id"] = lm["id"]
                    entry["registrant_db_name"] = lm["_display"]
                    write_finance_note(lm["id"],
                        f"[Federal LDA {item['year']}] Lobbied on behalf of {item['client_name']}")
                    if subject_id:
                        if write_relationship(lm["id"], subject_id, "Lobbyist",
                                f"Federal lobbyist for {item['client_name']} ({item['year']})",
                                f"Income: ${item['income']:,.0f} | Issues: {item['issues'][:100]}"):
                            findings["new_connections_written"] += 1
                    break

            # Match client against contacts
            cm, _ = best_match(item["client_name"], index, keys)
            if cm:
                entry["client_in_db"] = True
                entry["client_person_id"] = cm["id"]
                entry["client_db_name"] = cm["_display"]
                if subject_id:
                    if write_relationship(cm["id"], subject_id, "Lobbying Client",
                            f"Federal lobbying client targeting {person_name} ({item['year']})",
                            f"Registrant: {item['registrant_name']} | Income: ${item['income']:,.0f}"):
                        findings["new_connections_written"] += 1

            findings["federal_lobbied_by"].append(entry)

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
    ]

@mcp_server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    loop = asyncio.get_event_loop()
    log.info(f"Tool called: {name} args={arguments}")
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

        # Shared lobbying: officials both people lobbied, or one lobbied the other
        a_lobbied      = {r["client"] for r in fa.get("lobbied_by", [])}
        b_lobbied      = {r["client"] for r in fb.get("lobbied_by", [])}
        a_lobbied_nys  = {r["client"] for r in fa.get("nys_lobbied_by", [])}
        b_lobbied_nys  = {r["client"] for r in fb.get("nys_lobbied_by", [])}
        a_lobbied_fed  = {r["client"] for r in fa.get("federal_lobbied_by", [])}
        b_lobbied_fed  = {r["client"] for r in fb.get("federal_lobbied_by", [])}

        # B lobbied A (B appears in A's lobbied_by list)
        b_lobbied_a_nyc = [r for r in fa.get("lobbied_by", [])
                           if normalize(r.get("client","")) == normalize(arguments["person_b"])
                           or normalize(r.get("lobbyist","")) == normalize(arguments["person_b"])]
        b_lobbied_a_nys = [r for r in fa.get("nys_lobbied_by", [])
                           if normalize(r.get("client","")) == normalize(arguments["person_b"])
                           or normalize(r.get("lobbyist","")) == normalize(arguments["person_b"])]
        # A lobbied B
        a_lobbied_b_nyc = [r for r in fb.get("lobbied_by", [])
                           if normalize(r.get("client","")) == normalize(arguments["person_a"])
                           or normalize(r.get("lobbyist","")) == normalize(arguments["person_a"])]
        a_lobbied_b_nys = [r for r in fb.get("nys_lobbied_by", [])
                           if normalize(r.get("client","")) == normalize(arguments["person_a"])
                           or normalize(r.get("lobbyist","")) == normalize(arguments["person_a"])]

        # Federal: did either person appear as lobbyist in the other's federal filings?
        b_lobbied_a_fed = [r for r in fa.get("federal_lobbied_by", [])
                           if normalize(r.get("client","")) == normalize(arguments["person_b"])
                           or any(normalize(l) == normalize(arguments["person_b"])
                                  for l in r.get("lobbyist_names", []))]
        a_lobbied_b_fed = [r for r in fb.get("federal_lobbied_by", [])
                           if normalize(r.get("client","")) == normalize(arguments["person_a"])
                           or any(normalize(l) == normalize(arguments["person_a"])
                                  for l in r.get("lobbyist_names", []))]

        result = {
            "person_a": arguments["person_a"],
            "person_b": arguments["person_b"],
            # Campaign finance connections
            "shared_donation_targets": list(a_cands & b_cands),
            "a_donated_to_b_allies": [r for r in fa.get("known_recipients_in_db", []) if r.get("in_db")],
            "b_donated_to_a_allies": [r for r in fb.get("known_recipients_in_db", []) if r.get("in_db")],
            # NYC lobbying connections
            "b_lobbied_a_nyc": b_lobbied_a_nyc,
            "a_lobbied_b_nyc": a_lobbied_b_nyc,
            "shared_nyc_lobbying_clients": list(a_lobbied & b_lobbied),
            # NYS lobbying connections
            "b_lobbied_a_nys": b_lobbied_a_nys,
            "a_lobbied_b_nys": a_lobbied_b_nys,
            "shared_nys_lobbying_clients": list(a_lobbied_nys & b_lobbied_nys),
            # Federal LDA connections
            "b_lobbied_a_federal": b_lobbied_a_fed,
            "a_lobbied_b_federal": a_lobbied_b_fed,
            "shared_federal_lobbying_clients": list(a_lobbied_fed & b_lobbied_fed),
        }
        return [types.TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
    return [types.TextContent(type="text", text=f"Unknown tool: {name}")]

# ─── Starlette app ────────────────────────────────────────────────────────────

sse_transport = SseServerTransport("/messages/")

def _check_auth(request: Request) -> bool:
    expected = _cfg("MCP_API_KEY")
    if not expected:
        return True
    provided = request.headers.get("x-api-key") or request.query_params.get("api_key", "")
    return provided == expected

async def handle_sse(request: Request):
    if not _check_auth(request):
        return Response("Unauthorized", status_code=401)
    log.info(f"SSE connection from {request.client}")
    async with sse_transport.connect_sse(
        request.scope, request.receive, request._send
    ) as streams:
        await mcp_server.run(streams[0], streams[1], mcp_server.create_initialization_options())

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
