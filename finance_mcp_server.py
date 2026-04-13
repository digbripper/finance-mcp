"""
finance_mcp_server.py  — SSE transport edition
================================================
Remote MCP server for campaign finance enrichment.
Runs on Railway (or any host) and connects via mcp-remote.

Data sources:
  - NYC CFB  (city-level, live API)
  - FEC      (federal, live API)
  - NYS BOE  (state-level, bundled CSV — parsed_contributions.csv)

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

# ─── Config ────────────────────────────────────────────────────────────────────

DATABASE_URL = os.environ["DATABASE_URL"]
FEC_API_KEY  = os.getenv("FEC_API_KEY", "DEMO_KEY")
MCP_API_KEY  = os.environ["MCP_API_KEY"]   # required — set in Railway env vars

CFB_BASE             = "https://data.cityofnewyork.us/resource"
FEC_BASE             = "https://api.open.fec.gov/v1"
CFB_CONTRIBUTIONS_ID = "k3cd-yu9d"

MATCH_THRESHOLD = 82

# NYS BOE CSV lives next to this file (bundled in Docker image)
BOE_CSV_PATH = Path(__file__).parent / "nys_boe_data" / "parsed_contributions.csv"

# ─── NYS BOE CSV cache (loaded once at startup) ────────────────────────────────

_boe_rows: list[dict] = []
_boe_loaded = False

def _load_boe_csv():
    global _boe_rows, _boe_loaded
    if _boe_loaded:
        return
    if not BOE_CSV_PATH.exists():
        log.warning(f"NYS BOE CSV not found at {BOE_CSV_PATH} — state data unavailable")
        _boe_loaded = True
        return
    with open(BOE_CSV_PATH, newline="", encoding="utf-8") as f:
        _boe_rows = list(csv.DictReader(f))
    log.info(f"Loaded {len(_boe_rows):,} NYS BOE contributions from CSV")
    _boe_loaded = True

def boe_donors_to(candidate_name: str, limit: int = 100) -> list[dict]:
    """Return BOE rows where candidate_name fuzzy-matches."""
    _load_boe_csv()
    if not _boe_rows:
        return []
    norm_target = normalize(candidate_name)
    results = []
    for row in _boe_rows:
        cname = (row.get("candidate_name") or "").strip()
        if not cname:
            continue
        if fuzz.token_sort_ratio(normalize(cname), norm_target) >= MATCH_THRESHOLD:
            results.append(row)
        if len(results) >= limit:
            break
    return results

def boe_donations_by(donor_name: str, limit: int = 100) -> list[dict]:
    """Return BOE rows where contributor_name fuzzy-matches."""
    _load_boe_csv()
    if not _boe_rows:
        return []
    norm_target = normalize(donor_name)
    results = []
    for row in _boe_rows:
        cname = (row.get("contributor_name") or "").strip()
        if not cname:
            continue
        if fuzz.token_sort_ratio(normalize(cname), norm_target) >= MATCH_THRESHOLD:
            results.append(row)
        if len(results) >= limit:
            break
    return results

# ─── DB helpers ────────────────────────────────────────────────────────────────

def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

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
                WHERE from_person_id = %s AND to_person_id = %s
                  AND relationship_type = %s
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

# ─── Name utilities ─────────────────────────────────────────────────────────────

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

# ─── Live finance API calls ──────────────────────────────────────────────────────

def cfb_donations_received(candidate_name: str, limit: int = 50) -> list[dict]:
    last = candidate_name.strip().split()[-1]
    url = f"{CFB_BASE}/{CFB_CONTRIBUTIONS_ID}.json"
    params = {
        "$limit": limit,
        "$where": f"upper(candidate_name) like upper('%{last}%') AND amount >= 500",
        "$order": "amount DESC",
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.warning(f"CFB donations received error: {e}")
        return []

def cfb_donations_made(donor_name: str, limit: int = 50) -> list[dict]:
    last = donor_name.strip().split()[-1]
    url = f"{CFB_BASE}/{CFB_CONTRIBUTIONS_ID}.json"
    params = {
        "$limit": limit,
        "$where": f"upper(contributor_name) like upper('%{last}%') AND amount >= 250",
        "$order": "amount DESC",
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.warning(f"CFB donations made error: {e}")
        return []

def fec_donations_to(candidate_name: str, limit: int = 50) -> list[dict]:
    last = candidate_name.strip().split()[-1]
    url = f"{FEC_BASE}/schedules/schedule_a/"
    params = {
        "api_key": FEC_API_KEY,
        "per_page": min(limit, 100),
        "contributor_state": "NY",
        "min_amount": 500,
        "sort": "-contribution_receipt_amount",
        "sort_hide_null": True,
    }
    try:
        cand_resp = requests.get(
            f"{FEC_BASE}/candidates/search/",
            params={"api_key": FEC_API_KEY, "q": last, "state": "NY", "per_page": 3},
            timeout=10
        )
        cand_resp.raise_for_status()
        cands = cand_resp.json().get("results", [])
        if cands:
            params["committee_id"] = cands[0].get("principal_committees", [{}])[0].get("id", "")
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json().get("results", [])
    except Exception as e:
        log.warning(f"FEC donations to error: {e}")
        return []

def fec_donations_by(donor_name: str, limit: int = 50) -> list[dict]:
    last = donor_name.strip().split()[-1]
    url = f"{FEC_BASE}/schedules/schedule_a/"
    params = {
        "api_key": FEC_API_KEY,
        "per_page": min(limit, 100),
        "contributor_name": last,
        "contributor_state": "NY",
        "min_amount": 250,
        "sort": "-contribution_receipt_amount",
    }
    try:
        time.sleep(0.3)
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json().get("results", [])
    except Exception as e:
        log.warning(f"FEC donations by error: {e}")
        return []

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
        "new_connections_written": 0,
    }

    # ── 1. Who donated TO this person? (CFB + FEC + BOE) ──────────────────────
    all_received = []
    for row in cfb_donations_received(person_name):
        dname = (row.get("contributor_name") or "").strip()
        if not dname:
            continue
        all_received.append({
            "donor_name": dname,
            "amount": float(row.get("amount") or 0),
            "year": (row.get("date") or "")[:4],
            "source": "NYC CFB",
        })

    for row in fec_donations_to(person_name):
        dname = (row.get("contributor_name") or "").strip()
        if not dname:
            continue
        all_received.append({
            "donor_name": dname,
            "amount": float(row.get("contribution_receipt_amount") or 0),
            "year": (row.get("contribution_receipt_date") or "")[:4],
            "source": "FEC",
        })

    for row in boe_donors_to(person_name):
        dname = (row.get("contributor_name") or "").strip()
        if not dname:
            continue
        all_received.append({
            "donor_name": dname,
            "amount": float(row.get("amount") or 0),
            "year": row.get("election_year") or (row.get("date") or "")[:4],
            "source": "NYS BOE",
        })

    for item in all_received:
        donor_contact, score = best_match(item["donor_name"], index, keys)
        if not donor_contact:
            continue
        entry = {
            "name": donor_contact["_display"],
            "person_id": donor_contact["id"],
            "orgs": donor_contact.get("orgs", ""),
            "amount": item["amount"],
            "source": item["source"],
            "year": item["year"],
        }
        findings["known_donors_in_db"].append(entry)
        note = f"[{item['source']} {item['year']}] Donated ${item['amount']:,.0f} to {person_name} (auto-detected)"
        write_finance_note(donor_contact["id"], note)
        if subject_id:
            new = write_relationship(
                donor_contact["id"], subject_id,
                rel_type="Campaign Donor",
                context=f"Donated to {person_name}",
                notes=f"${item['amount']:,.0f} | Source: {item['source']} | {item['year']}"
            )
            if new:
                findings["new_connections_written"] += 1

    # ── 2. Who did THIS person donate to? (CFB + FEC + BOE) ───────────────────
    recipient_map: dict[str, dict] = {}

    for row in cfb_donations_made(person_name):
        cname = (row.get("candidate_name") or "").strip()
        if not cname:
            continue
        if cname not in recipient_map:
            recipient_map[cname] = {"amount": 0, "year": (row.get("date") or "")[:4], "source": "NYC CFB"}
        recipient_map[cname]["amount"] += float(row.get("amount") or 0)

    for row in fec_donations_by(person_name):
        cname = (row.get("committee_name") or "").strip()
        if not cname:
            continue
        if cname not in recipient_map:
            recipient_map[cname] = {"amount": 0, "year": (row.get("contribution_receipt_date") or "")[:4], "source": "FEC"}
        recipient_map[cname]["amount"] += float(row.get("contribution_receipt_amount") or 0)

    for row in boe_donations_by(person_name):
        cname = (row.get("candidate_name") or "").strip()
        if not cname:
            continue
        if cname not in recipient_map:
            recipient_map[cname] = {"amount": 0, "year": row.get("election_year") or "", "source": "NYS BOE"}
        recipient_map[cname]["amount"] += float(row.get("amount") or 0)

    for cname, info in sorted(recipient_map.items(), key=lambda x: -x[1]["amount"])[:15]:
        cand_contact, _ = best_match(cname, index, keys)
        entry = {
            "candidate_name": cname,
            "in_db": bool(cand_contact),
            "db_match": cand_contact["_display"] if cand_contact else None,
            **info,
        }
        findings["known_recipients_in_db"].append(entry)
        if subject_id and cand_contact:
            note = f"[{info['source']} {info['year']}] Donated ${info['amount']:,.0f} to {cname} (auto-detected)"
            write_finance_note(subject_id, note)
            new = write_relationship(
                subject_id, cand_contact["id"],
                rel_type="Campaign Donor",
                context=f"Donated to {cname}",
                notes=f"${info['amount']:,.0f} | Source: {info['source']} | {info['year']}"
            )
            if new:
                findings["new_connections_written"] += 1

    # ── 3. Co-donors ───────────────────────────────────────────────────────────
    if recipient_map:
        top_candidate = max(recipient_map, key=lambda k: recipient_map[k]["amount"])
        co_donors_seen: set[str] = set()

        for row in cfb_donations_received(top_candidate, limit=100):
            dname = (row.get("contributor_name") or "").strip()
            if not dname or normalize(dname) == normalize(person_name):
                continue
            match, _ = best_match(dname, index, keys)
            if match and match["id"] not in co_donors_seen:
                co_donors_seen.add(match["id"])
                findings["co_donors_in_db"].append({
                    "name": match["_display"],
                    "person_id": match["id"],
                    "orgs": match.get("orgs", ""),
                    "shared_candidate": top_candidate,
                    "their_amount": float(row.get("amount") or 0),
                    "source": "NYC CFB",
                })
                if subject_id and match["id"] != subject_id:
                    new = write_relationship(
                        subject_id, match["id"],
                        rel_type="Co-Donor",
                        context=f"Co-donors to {top_candidate}",
                        notes=f"Both donated to {top_candidate} (auto-detected)"
                    )
                    if new:
                        findings["new_connections_written"] += 1

        for row in boe_donors_to(top_candidate):
            dname = (row.get("contributor_name") or "").strip()
            if not dname or normalize(dname) == normalize(person_name):
                continue
            match, _ = best_match(dname, index, keys)
            if match and match["id"] not in co_donors_seen:
                co_donors_seen.add(match["id"])
                findings["co_donors_in_db"].append({
                    "name": match["_display"],
                    "person_id": match["id"],
                    "orgs": match.get("orgs", ""),
                    "shared_candidate": top_candidate,
                    "their_amount": float(row.get("amount") or 0),
                    "source": "NYS BOE",
                })
                if subject_id and match["id"] != subject_id:
                    new = write_relationship(
                        subject_id, match["id"],
                        rel_type="Co-Donor",
                        context=f"Co-donors to {top_candidate}",
                        notes=f"Both donated to {top_candidate} (NYS BOE)"
                    )
                    if new:
                        findings["new_connections_written"] += 1

    return findings

# ─── MCP Server ───────────────────────────────────────────────────────────────

app = Server("finance-enrichment")

@app.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="lookup_finance_connections",
            description=(
                "Look up campaign finance connections for a named person using NYC CFB, FEC, and NYS BOE data. "
                "Call this automatically whenever a query involves political influence, access to an elected official, "
                "or background on a political figure. "
                "Returns: who donated to them, who they donated to, and co-donors already in your contacts database. "
                "Also writes new relationships back to the database in real time. "
                "ALWAYS call this for political figures before answering influence questions."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "person_name": {
                        "type": "string",
                        "description": "Full name of the politician or contact to look up"
                    }
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

@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    loop = asyncio.get_event_loop()

    if name == "lookup_finance_connections":
        person_name = arguments["person_name"]
        log.info(f"Finance lookup: {person_name}")
        findings = await loop.run_in_executor(None, enrich_person, person_name)
        return [types.TextContent(type="text", text=json.dumps(findings, indent=2, default=str))]

    elif name == "find_financial_path":
        person_a = arguments["person_a"]
        person_b = arguments["person_b"]
        log.info(f"Financial path: {person_a} <-> {person_b}")
        findings_a, findings_b = await asyncio.gather(
            loop.run_in_executor(None, enrich_person, person_a),
            loop.run_in_executor(None, enrich_person, person_b),
        )
        a_cands = {r["candidate_name"] for r in findings_a.get("known_recipients_in_db", [])}
        b_cands = {r["candidate_name"] for r in findings_b.get("known_recipients_in_db", [])}
        result = {
            "person_a": person_a,
            "person_b": person_b,
            "shared_donation_targets": list(a_cands & b_cands),
            "a_donated_to_b_allies": [r for r in findings_a.get("known_recipients_in_db", []) if r.get("in_db")],
            "b_donated_to_a_allies": [r for r in findings_b.get("known_recipients_in_db", []) if r.get("in_db")],
        }
        return [types.TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

    return [types.TextContent(type="text", text=f"Unknown tool: {name}")]

# ─── Starlette app with auth middleware ───────────────────────────────────────

sse = SseServerTransport("/messages/")

async def handle_sse(request: Request):
    # Auth check
    api_key = request.headers.get("x-api-key") or request.query_params.get("api_key")
    if api_key != MCP_API_KEY:
        return Response("Unauthorized", status_code=401)

    async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
        await app.run(streams[0], streams[1], app.create_initialization_options())

async def handle_messages(request: Request):
    api_key = request.headers.get("x-api-key") or request.query_params.get("api_key")
    if api_key != MCP_API_KEY:
        return Response("Unauthorized", status_code=401)
    await sse.handle_post_message(request.scope, request.receive, request._send)

async def healthcheck(request: Request):
    boe_count = len(_boe_rows) if _boe_loaded else "not loaded"
    return Response(
        json.dumps({"status": "ok", "boe_rows": boe_count}),
        media_type="application/json"
    )

@asynccontextmanager
async def lifespan(app):
    log.info("Starting up — loading NYS BOE CSV...")
    _load_boe_csv()
    log.info("Ready.")
    yield

starlette_app = Starlette(
    lifespan=lifespan,
    routes=[
        Route("/health", healthcheck),
        Route("/sse", handle_sse),
        Mount("/messages/", app=sse.handle_post_message),
    ],
)

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    log.info(f"Starting finance-enrichment MCP server on port {port}")
    uvicorn.run(starlette_app, host="0.0.0.0", port=port)
