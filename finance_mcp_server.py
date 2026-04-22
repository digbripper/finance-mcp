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

    # LDA strategy for senators/house members:
    # 1. Search by registrant_name containing the official's name (catches cases
    #    where the person IS a registered lobbyist themselves — rare but real)
    # 2. Fetch NY-client filings and filter by government_entities containing "SENATE"
    #    or "HOUSE" + last name match in the activity description
    # The specific_lobbying_issues field contains bill/issue text, NOT senator names.

    for year in years:
        # Search 1: NY clients lobbying in this year — filter for Senate/House contacts
        try:
            resp = requests.get(f"{LDA_BASE}/filings/",
                params={
                    "client_state": "NY",
                    "filing_year": year,
                    "filing_type": "Q1,Q2,Q3,Q4",
                    "limit": limit,
                },
                timeout=12,
                headers={"Accept": "application/json"})
            resp.raise_for_status()
            filings = resp.json().get("results", [])
            for f in filings:
                uid = f.get("filing_uuid") or str(f.get("id") or "")
                if uid in seen_uuids:
                    continue
                # Check if any lobbying activity targets this official by name
                # in government_entities or in the lobbyist names
                matched = False
                for act in (f.get("lobbying_activities") or []):
                    entities = " ".join(
                        (e.get("name") or "").lower()
                        for e in (act.get("government_entities") or [])
                    )
                    if search_term.lower() in entities:
                        matched = True
                        break
                if matched:
                    seen_uuids.add(uid)
                    all_filings.append(f)
            log.info(f"LDA {year} NY clients: {len(filings)} filings, {len(all_filings)} matched {search_term}")
        except Exception as e:
            log.warning(f"LDA targeting error {year}: {e}")

    log.info(f"LDA total: {len(all_filings)} filings targeting {official_name}")
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

_VOTER_INDEX: dict[str, list[dict]] = {}   # LASTNAME -> [voter dicts]
_VOTER_LOADED = False

PARTY_LABELS = {
    "DEM": "Democrat", "REP": "Republican", "CON": "Conservative",
    "WOR": "Working Families", "BLK": "No Party", "OTH": "Other",
    "GRE": "Green", "LBT": "Libertarian", "IND": "Independence",
}

def _load_voter_file():
    global _VOTER_LOADED
    if _VOTER_LOADED:
        return
    for _candidate in [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "nyc_super_voters.csv"),
        os.path.join(os.getcwd(), "nyc_super_voters.csv"),
        "/app/nyc_super_voters.csv",
    ]:
        if os.path.exists(_candidate):
            csv_path = _candidate
            break
    else:
        log.warning("nyc_super_voters.csv not found — voter lookup disabled")
        _VOTER_LOADED = True
        return
    count = 0
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in _vcsv.DictReader(f):
            last = (row.get("LASTNAME") or "").strip().upper()
            if not last:
                continue
            _VOTER_INDEX.setdefault(last, []).append({
                "sboeid":        row.get("SBOEID", ""),
                "last":          last,
                "first":         (row.get("FIRSTNAME") or "").strip().upper(),
                "dob":           row.get("DOB", ""),
                "party":         row.get("PARTY", ""),
                "address":       row.get("ADDRESS", ""),
                "city":          row.get("CITY", ""),
                "zip":           row.get("ZIP", ""),
                "county_code":   row.get("COUNTY_CODE", ""),
                "county":        row.get("COUNTY_NAME", ""),
                "cd":            row.get("CD", ""),
                "sd":            row.get("SD", ""),
                "ad":            row.get("AD", ""),
                "regdate":       row.get("REGDATE", ""),
                "ge_votes":      int(row.get("GE_VOTES") or 0),
                "primary_votes": int(row.get("PRIMARY_VOTES") or 0),
                "voter_score":   int(row.get("VOTER_SCORE") or 0),
            })
            count += 1
    log.info(f"Loaded {count:,} NYC super voters into memory index")
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

def _download_voter_db() -> bool:
    import urllib.request as _ur
    path = VOTER_DB_LOCAL_PATH
    if os.path.exists(path) and _is_real_sqlite(path):
        log.info("Full voter DB already present")
        return True
    log.info("Downloading full voter DB from GitHub Releases (~1GB)...")
    try:
        req = _ur.Request(VOTER_DB_RELEASE_URL,
                          headers={"User-Agent": "finance-mcp/1.0"})
        with _ur.urlopen(req, timeout=300) as resp, \
             open(path, "wb") as out:
            downloaded = 0
            while True:
                chunk = resp.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
                downloaded += len(chunk)
                if downloaded % (100 * 1024 * 1024) == 0:
                    log.info(f"  Downloaded {downloaded // (1024*1024)}MB...")
        log.info(f"Voter DB download complete ({downloaded // (1024*1024)}MB)")
        return True
    except Exception as e:
        log.error(f"Failed to download voter DB: {e}")
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

    # If DB already present and valid, open it synchronously (fast path)
    if os.path.exists(VOTER_DB_LOCAL_PATH) and _is_real_sqlite(VOTER_DB_LOCAL_PATH):
        _open_voter_db()
        return

    # Otherwise download in background so startup health check passes
    log.info("Voter DB not present — downloading in background thread...")
    import threading as _th
    t = _th.Thread(target=_background_download, daemon=True)
    t.start()


def lookup_voter(full_name: str, dob: str = "") -> dict | None:
    """
    Look up a person in the NYC voter DB by name.
    Returns best-matching voter record dict or None.
    Uses indexed SQL queries — fast even on 3.5M rows.
    """
    if _voter_db_conn is None:
        return None

    parts = full_name.strip().upper().split()
    if not parts:
        return None

    last  = parts[-1]
    first = parts[0] if len(parts) >= 2 else ""

    try:
        with _VOTER_DB_LOCK:
            if dob:
                # Exact match: last + first + DOB
                row = _voter_db_conn.execute(
                    "SELECT * FROM voters WHERE lastname=? AND firstname=? AND dob=? LIMIT 1",
                    (last, first, dob)
                ).fetchone()
                if row:
                    return dict(row)

            # Fuzzy: last + first prefix (first 3 chars)
            if first:
                rows = _voter_db_conn.execute(
                    "SELECT * FROM voters WHERE lastname=? AND firstname LIKE ? "
                    "ORDER BY voter_score DESC LIMIT 5",
                    (last, first[:3] + "%")
                ).fetchall()
            else:
                rows = _voter_db_conn.execute(
                    "SELECT * FROM voters WHERE lastname=? "
                    "ORDER BY voter_score DESC LIMIT 5",
                    (last,)
                ).fetchall()

            if not rows:
                return None

            # Among candidates, prefer exact first name match
            for r in rows:
                if r["firstname"] == first:
                    return dict(r)
            # Fallback: highest voter_score
            return dict(rows[0])

    except Exception as e:
        log.warning(f"Voter DB lookup error for {full_name!r}: {e}")
        return None



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
    Find high-engagement NYC voters using the in-memory super voter CSV index.
    Filters by county, party, district, voter score. Cross-references BOE finance data.
    """
    _load_voter_file()
    if not _VOTER_INDEX:
        return [{"error": "Super voter index not loaded"}]

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
    for voters_list in _VOTER_INDEX.values():
        for v in voters_list:
            if v.get("county_code") != county_code: continue
            if v.get("voter_score", 0) < min_voter_score: continue
            if party_code and v.get("party","") != party_code: continue
            if ad_str and v.get("ad","").lstrip("0") != ad_str: continue
            if sd_str and v.get("sd","").lstrip("0") != sd_str: continue
            if cd_str and v.get("cd","").lstrip("0") != cd_str: continue
            rows.append(v)
            if len(rows) >= fetch_limit:
                break
        if len(rows) >= fetch_limit:
            break

    rows.sort(key=lambda x: -x.get("voter_score", 0))
    log.info(f"find_super_voters: {len(rows)} voters for {county} score>={min_voter_score}")

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
            "total_donated": 0.0,
            "donation_count": 0,
            "top_candidates": [],
            "has_finance_history": False,
        }

        if cross_reference_finance:
            donations = boe_donations_by_voter(v["lastname"], v["firstname"])
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
        party_label = PARTY_LABELS.get(voter["party"], voter["party"])
        findings["voter_profile"] = {
            "registered_party":  party_label,
            "party_code":        voter["party"],
            "address":           voter["address"],
            "city":              voter["city"],
            "zip":               voter["zip"],
            "county":            voter["county"],
            "congressional_district": voter["cd"],
            "state_senate_district":  voter["sd"],
            "assembly_district":      voter["ad"],
            "registered_since":  voter["regdate"],
            "general_elections_voted": voter["ge_votes"],
            "primaries_voted":   voter["primary_votes"],
            "voter_score":       voter["voter_score"],
            "sboeid":            voter["sboeid"],
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
    # Eagerly load LDA registrants CSV and voter file
    _load_lda_registrants()
    _init_voter_db()
    _build_boe_donor_index()  # pre-warm for find_super_voters
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
