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
        resp = requests.get(url, params=_lobbying_params(last, year, limit), timeout=20)
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
            timeout=20)
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

# ─── Core enrichment ──────────────────────────────────────────────────────────

def enrich_person(person_name: str) -> dict:
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
        "lobbying_clients_in_db": [],
        "new_connections_written": 0,
    }

    # 1. Who donated TO this person?
    all_received = []
    for row in cfb_donations_received(person_name):
        d = (row.get("contributor_name") or "").strip()
        if d: all_received.append({"donor_name": d, "amount": float(row.get("amount") or 0),
                                    "year": (row.get("date") or "")[:4], "source": "NYC CFB"})
    for row in fec_donations_to(person_name):
        d = (row.get("contributor_name") or "").strip()
        if d: all_received.append({"donor_name": d,
                                    "amount": float(row.get("contribution_receipt_amount") or 0),
                                    "year": (row.get("contribution_receipt_date") or "")[:4], "source": "FEC"})
    for row in boe_donors_to(person_name):
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

    # 2. Who did THIS person donate to?
    rmap: dict[str, dict] = {}
    for row in cfb_donations_made(person_name):
        c = (row.get("candidate_name") or "").strip()
        if c:
            if c not in rmap: rmap[c] = {"amount": 0, "year": (row.get("date") or "")[:4], "source": "NYC CFB"}
            rmap[c]["amount"] += float(row.get("amount") or 0)
    for row in fec_donations_by(person_name):
        c = (row.get("committee_name") or "").strip()
        if c:
            if c not in rmap: rmap[c] = {"amount": 0, "year": (row.get("contribution_receipt_date") or "")[:4], "source": "FEC"}
            rmap[c]["amount"] += float(row.get("contribution_receipt_amount") or 0)
    for row in boe_donations_by(person_name):
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
    # ── 4a. Raw lobbying filings targeting this official ──────────────────────
    current_year = str(__import__("datetime").datetime.now().year)
    lobby_rows = nyc_lobbying_targets(person_name)
    deduped_lobbying = _dedupe_lobbying(lobby_rows)

    for item in deduped_lobbying[:50]:  # cap at 50 unique client/lobbyist pairs
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

    # ── 4b. Did this person (or their org) hire lobbyists? ────────────────────
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

    return findings

# ─── MCP Server ───────────────────────────────────────────────────────────────

mcp_server = Server("finance-enrichment")

@mcp_server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="lookup_finance_connections",
            description=(
                "Look up campaign finance AND lobbying connections for a named person using NYC CFB, FEC, NYS BOE, and NYC City Clerk eLobbyist data. "
                "Call this automatically whenever a query involves political influence, access to an elected official, "
                "or background on a political figure. "
                "Returns: who donated to them, who they donated to, co-donors in your contacts database, AND who lobbied them (lobbyist firm, client, compensation, subject matter). "
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
        result = {
            "person_a": arguments["person_a"], "person_b": arguments["person_b"],
            "shared_donation_targets": list(a_cands & b_cands),
            "a_donated_to_b_allies": [r for r in fa.get("known_recipients_in_db", []) if r.get("in_db")],
            "b_donated_to_a_allies": [r for r in fb.get("known_recipients_in_db", []) if r.get("in_db")],
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
