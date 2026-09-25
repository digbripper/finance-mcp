"""
Influence scoring v2.0 — shared by finance_mcp_server.py (compute_influence_scores
and the Phase 1/2 enrichment writes) and preview_v2.py (local dry run against
the DB). Plain Python 3.9+ with no dependencies beyond a psycopg2 connection
using RealDictCursor, so the preview runs exactly the code that deploys.

    composite = institutional × 0.85 + financial × 0.05 + network × 0.10
    (Tier 1 trusted advisors / major donors: 0.60 / 0.30 / 0.10)

Position dominates; donations and connections add to it. Institutional is the
elected office's score for elected officials; for everyone else it comes from
person tier, tier-basis floors and a small org-tier bonus, capped at 75.
(Weights chosen 2026-09-25 as the best fit to hand-set targets for known
people — see preview_v2.py.) Lobbying and engagement are no longer scored
(stored as 0) and there is no stacking bonus. Rows with algorithm_version
'politician_v1' (elected officials, scored by office in pythia-web
scripts/politician-scores.mjs) are never touched.
"""
from __future__ import annotations

import json
import math
import re
from collections import defaultdict

ALGORITHM_VERSION = "v2.0"
POLITICIAN_VERSION = "politician_v1"
WEIGHTS_V2 = dict(institutional=0.85, financial=0.05, network=0.10)

# Donors and advisors whose influence is money and relationships: financial
# gets real weight for them.
ADVISOR_BASES = ("trusted_advisor", "major_donor")
WEIGHTS_ADVISOR = dict(institutional=0.60, financial=0.30, network=0.10)

# Non-elected institutional score (2026-09-25): person tier and tier-basis
# floors, plus a small org-tier bonus, never above 75. No 990 revenue, no
# seniority or decision-maker bonuses — those stacked a nonprofit VP at a
# $82M org to 100.
NON_ELECTED_MAX = 75.0
TIER_BASIS_FLOORS = {
    "appointed_executive": 75.0,  # MTA / H+H chiefs — at the ceiling
    "trusted_advisor": 65.0,      # Emily Giske, Merryl Tisch
    "manual": 50.0,               # manually set tier 1
}
DEFAULT_FLOOR = 10.0
ORG_TIER_BONUS = {1: 10.0, 2: 5.0, 3: 2.0}

# Staff inherit none of their principal's position: a budget analyst in the
# Comptroller's Office is not the Comptroller. A staff title never earns an
# office's score; their real influence shows up in proximity.
STAFF_TITLE_KEYWORDS = [
    "director", "deputy director", "counsel", "chief of staff",
    "deputy chief", "analyst", "coordinator", "associate",
    "assistant", "manager", "specialist", "officer", "advisor",
    "adviser", "administrator", "commissioner", "deputy commissioner",
    "press secretary", "communications", "scheduler", "aide", "deputy",
]
# Titles that ARE the principal role…
_PRINCIPAL = re.compile(
    r"\b(chief executive|ceo|president|chair(man|woman|person)?|founder|partner|"
    r"executive director|commissioner|deputy mayor)\b")
# …unless they're the number two ("Deputy Commissioner", "Vice President",
# "Deputy Executive Director", "Deputy Borough President")…
_SUBORDINATE = re.compile(
    r"\b(deputy|assistant|associate|vice)\s+(\w+\s+){0,2}"
    r"(president|chair\w*|commissioner|director|partner)\b")
# …or explicitly work for someone.
_ALWAYS_STAFF = re.compile(
    r"chief of staff|press secretary|scheduler|\baide\b|\bto the\b|"
    r"\bto (senator|council ?member|assembly ?member|borough president|mayor|comptroller|governor)")


def is_staff_role(job_title: str) -> bool:
    t = (job_title or "").lower().strip()
    if not t:
        return False
    if _ALWAYS_STAFF.search(t):
        return True
    if _PRINCIPAL.search(t) and not _SUBORDINATE.search(t):
        return False
    return any(kw in t for kw in STAFF_TITLE_KEYWORDS)


# "Former ..." titles: the past role earns nothing. When the title names only
# a generic role ("Former CEO" at Loews) the org is the former employer and the
# whole affiliation is skipped; when it names another office ("Former NYC
# Comptroller" on a US House link) the org is current and still counts.
_FORMER = re.compile(r"\bformer\b|\bex-", re.I)
_ROLE_WORDS = {
    "ceo", "coo", "cfo", "evp", "svp", "vp", "president", "vice", "chief", "executive",
    "director", "officer", "of", "staff", "deputy", "senior", "counsel", "counselor",
    "manager", "chair", "chairman", "chairwoman", "chairperson", "founder", "co", "partner",
    "member", "head", "general", "to", "the", "and", "for", "a", "an", "at", "board",
    "managing", "principal", "associate", "assistant", "advisor", "adviser", "special",
}
_PLACE_WORDS = {"nyc", "new", "york", "city", "state", "us", "u", "s", "united", "states", "national"}


def current_title(job_title: str | None, org_name: str | None) -> str | None:
    """
    The part of a title that describes the person's current role at this org,
    '' if none, or None if the affiliation itself is a former one.
    Titles can hold several roles ("former state senator; of counsel").
    """
    parts = [p.strip() for p in re.split(r"[;|]", job_title or "") if p.strip()]
    current = [p for p in parts if not _FORMER.search(p)]
    if current or not parts:
        return "; ".join(current)
    org_tokens = set(re.findall(r"[a-z0-9]+", (org_name or "").lower())) - _PLACE_WORDS
    for p in parts:
        tokens = set(re.findall(r"[a-z0-9]+", _FORMER.sub(" ", p).lower()))
        named = tokens - _ROLE_WORDS - _PLACE_WORDS
        if not named or named & org_tokens:
            return None  # the former role was at this org
    return ""


# An org name counts as a government position only when the org IS the
# position ("US House of Representatives", "Queens Borough President"), not an
# office, campaign or candidacy attached to one ("NYC Mayor's Office of Talent
# and Workforce Development", "2025 NYC Mayoral Candidate").
_NOT_A_POSITION = re.compile(
    r"\boffice\b|'s office|candidate|campaign|committee|caucus|department|agency|"
    r"\bfor\b|friends of|mayoral", re.I)


# Appointed (not elected) entries in _GOV_POSITION_SCORES, by their first
# keyword. Appointees are non-elected: their position counts, capped at 75.
_APPOINTED_KEYS = {
    "secretary of state", "director of the fbi", "nypd commissioner", "schools chancellor",
    "mta chairman", "fire commissioner", "commissioner",
}


def _position(text: str, elected: bool) -> float:
    t = (text or "").lower()
    for keywords, score in _GOV_POSITION_SCORES:
        if (keywords[0] in _APPOINTED_KEYS) == elected:
            continue
        if any(kw in t for kw in keywords):
            return float(score)
    return 0.0


_SEEKING = re.compile(r"\b(candidate|nominee|running for)\b", re.I)


def position_scores(title: str, org_name: str | None) -> tuple[float, float]:
    """(elected, appointed) position score for one affiliation. Staff titles
    earn neither, nor do candidates ("Candidate" on a US House link); an org
    name counts only when the org IS the position."""
    if is_staff_role(title) or _SEEKING.search(title or ""):
        return 0.0, 0.0
    org = (org_name or "").replace("\u2019", "'")
    org_ok = bool(org) and not _NOT_A_POSITION.search(org)
    elected = max(_position(title, True), _position(org, True) if org_ok else 0.0)
    appointed = _position(title, False)
    return elected, appointed


def non_elected_institutional(person_tier, basis: str | None, org_tier) -> float:
    floor = TIER_BASIS_FLOORS.get(basis, DEFAULT_FLOOR)
    bonus = ORG_TIER_BONUS.get(org_tier, 0.0)
    if person_tier == 2:
        base = 35.0
    elif person_tier == 1 and basis not in TIER_BASIS_FLOORS:
        base = 50.0
    else:
        base = 10.0
    return min(NON_ELECTED_MAX, max(floor, base) + bonus)


def weights_for(person_tier, basis: str | None) -> dict:
    if person_tier == 1 and basis in ADVISOR_BASES:
        return WEIGHTS_ADVISOR
    return WEIGHTS_V2


# ─── Institutional helpers (moved from finance_mcp_server.py, unchanged) ─────

def _revenue_to_institutional_score(revenue: int | None) -> float:
    """
    Convert annual nonprofit / union revenue (from IRS 990 or LM-2) to an
    institutional score (0-100).  Log-like scale so small-org differences matter.
    """
    if not revenue or revenue <= 0:
        return 0.0
    if revenue >= 1_000_000_000:   return 95.0
    if revenue >= 500_000_000:     return 88.0
    if revenue >= 100_000_000:     return 80.0
    if revenue >= 50_000_000:      return 70.0
    if revenue >= 10_000_000:      return 55.0
    if revenue >= 5_000_000:       return 40.0
    if revenue >= 1_000_000:       return 25.0
    return 10.0


# ─── Government position institutional scores ─────────────────────────────────
#
# Elected officials and senior appointees get a fixed institutional base score
# based on their position title, bypassing the org influence_tier system (which
# doesn't cover government orgs).  Ordered from most specific → most general;
# first match wins.  Matching is case-insensitive substring on combined
# title + org string.

_GOV_POSITION_SCORES: list[tuple[list[str], float]] = [
    # Federal — leadership
    (["president of the united states", "potus"],                           100),
    (["vice president of the united states"],                                99),
    (["senate majority leader"],                                             97),
    (["senate minority leader"],                                             96),
    (["speaker of the house"],                                               96),
    (["senate majority whip", "senate minority whip"],                       93),
    (["house majority leader", "house minority leader"],                     93),
    (["house majority whip", "house minority whip"],                         91),
    # Federal — rank and file
    (["us senator", "united states senator", "senator from new york",
      "senator, new york", "senator, ny"],                                   90),
    (["us representative", "member of congress", "congressman",
      "congresswoman", "us house of representatives",
      "member, us house", "representative, ny"],                             83),
    # Federal — cabinet / agencies
    (["secretary of state", "secretary of defense",
      "secretary of the treasury", "secretary of labor",
      "secretary of commerce", "secretary of education",
      "secretary of health"],                                                88),
    (["director of the fbi", "director of the cia",
      "national security advisor"],                                          85),
    # NYS — leadership
    (["governor of new york", "nys governor",
      "governor, new york"],                                                 95),
    (["new york state attorney general", "nys attorney general",
      "attorney general of new york", "attorney general, new york"],         88),
    (["new york state comptroller", "nys comptroller",
      "state comptroller, new york"],                                        85),
    (["lieutenant governor", "lt. governor"],                                80),
    (["new york state senate majority leader",
      "nys senate majority leader"],                                         84),
    (["new york state senate minority leader",
      "nys senate minority leader"],                                         83),
    (["new york state assembly speaker", "nys assembly speaker"],            83),
    # NYS — rank and file
    (["new york state senator", "nys senator",
      "state senator, new york", "state senator"],                           74),
    (["assemblymember", "assembly member",
      "new york state assembly", "nys assembly"],                            68),
    # NYC — leadership
    (["mayor of new york", "nyc mayor",
      "mayor, new york city"],                                               92),
    (["new york city comptroller", "nyc comptroller",
      "comptroller, new york city", "comptroller, nyc",
      "nyc comptroller"],                                                    82),
    (["new york city public advocate", "nyc public advocate",
      "public advocate, new york"],                                          79),
    (["new york city council speaker", "nyc council speaker",
      "speaker of the city council"],                                        82),
    (["borough president"],                                                   77),
    # NYC — rank and file
    (["new york city council member", "nyc council member",
      "city council member", "council member, new york"],                    68),
    # NYC agencies — appointed officials
    (["nypd commissioner", "police commissioner",
      "commissioner of police"],                                             72),
    (["schools chancellor", "nyc schools chancellor",
      "chancellor of the new york city"],                                    75),
    (["mta chairman", "mta ceo", "mta president"],                           73),
    (["fire commissioner", "fdny commissioner"],                             70),
    # Generic fallback for named commissioner / director roles
    (["commissioner"],                                                        65),
]


def _government_position_score(titles: str, orgs: str) -> float:
    """
    Return the institutional base score for a government position, or 0.0 if
    the combined title + org string doesn't match any known government position.
    Matching is case-insensitive substring; first entry in _GOV_POSITION_SCORES wins.
    """
    combined = ((titles or "") + " || " + (orgs or "")).lower()
    for keywords, score in _GOV_POSITION_SCORES:
        if any(kw in combined for kw in keywords):
            return float(score)
    return 0.0


# ─── Financial v2.0 ──────────────────────────────────────────────────────────

def calculate_financial_score_v2(total_donated: float, unique_officials: int,
                                 max_single_donation: float, annual_totals: dict) -> float:
    """
    total_donated       — total lifetime donations
    unique_officials    — distinct recipients (officials / committees) given to
    max_single_donation — largest single donation
    annual_totals       — {year: total} for annual giving patterns

    No donation history at all scores 0, not the $0–$2,499 floor of 5: absence
    of data isn't evidence of a small donor.
    """
    total_donated = float(total_donated or 0)
    if total_donated <= 0:
        return 0.0

    # Base score from total donated (stepped so it doesn't all go to the ultra-wealthy)
    if total_donated >= 75_000:
        base = 90
    elif total_donated >= 40_000:
        base = 75
    elif total_donated >= 15_000:
        base = 55
    elif total_donated >= 5_000:
        base = 35
    elif total_donated >= 2_500:
        base = 15  # local influence only — gets proximity record elsewhere
    else:
        base = 5

    # Breadth bonus — giving to multiple officials = broad influence
    unique_officials = int(unique_officials or 0)
    if unique_officials >= 5:
        breadth_bonus = 15
    elif unique_officials >= 3:
        breadth_bonus = 8
    elif unique_officials >= 2:
        breadth_bonus = 3
    else:
        breadth_bonus = 0  # single official = proximity only, no broad boost

    # Single-donation spike — large single gift signals deep relationship
    max_single_donation = float(max_single_donation or 0)
    if max_single_donation >= 25_000:
        spike_bonus = 15
    elif max_single_donation >= 15_000:
        spike_bonus = 10
    elif max_single_donation >= 10_000:
        spike_bonus = 5
    else:
        spike_bonus = 0

    # Annual consistency bonus — giving consistently across multiple years
    active_years = sum(1 for v in (annual_totals or {}).values() if float(v or 0) >= 1_000)
    if active_years >= 5:
        consistency_bonus = 10
    elif active_years >= 3:
        consistency_bonus = 5
    else:
        consistency_bonus = 0

    return float(min(100.0, base + breadth_bonus + spike_bonus + consistency_bonus))


def composite_v2(institutional: float, financial: float, network: float,
                 weights: dict | None = None) -> float:
    w = weights or WEIGHTS_V2
    return round(min(100.0, w["institutional"] * float(institutional or 0)
                     + w["financial"] * float(financial or 0)
                     + w["network"] * float(network or 0)), 2)


def active_years(annual_totals: dict) -> int:
    return sum(1 for v in (annual_totals or {}).values() if float(v or 0) >= 1_000)


def _year(value) -> str | None:
    m = re.search(r"(19|20)\d{2}", str(value or ""))
    return m.group(0) if m else None


def profile_contribution_rows(boe_rows, cfb_rows, fec_rows, superpac_rows=None) -> dict:
    """Largest single contribution and {year: total} across raw contribution rows."""
    amounts: list[tuple[float, str | None]] = []
    for r in boe_rows or []:
        amounts.append((float(r.get("amount") or 0), _year(r.get("date") or r.get("election_year"))))
    for r in cfb_rows or []:
        amounts.append((float(r.get("amount") or 0), _year(r.get("date"))))
    for r in list(fec_rows or []) + list(superpac_rows or []):
        amounts.append((float(r.get("contribution_receipt_amount") or 0), _year(r.get("contribution_receipt_date"))))
    annual: dict[str, float] = defaultdict(float)
    for amt, yr in amounts:
        if yr:
            annual[yr] += amt
    return {
        "max_single_donation": round(max((a for a, _ in amounts), default=0.0), 2),
        "annual_totals": {y: round(v, 2) for y, v in sorted(annual.items())},
    }


def parse_donor_note(note: str) -> tuple[float, str | None]:
    """'$3,850 | Source: NYC CFB | 2012' → (3850.0, '2012'). Campaign Donor
    relationship notes hold the TOTAL given to one recipient and the year of
    the first gift (see enrich_person), not a single contribution."""
    m = re.search(r"\$([0-9,]+(?:\.\d+)?)", note or "")
    amount = float(m.group(1).replace(",", "")) if m else 0.0
    return amount, _year((note or "").split("|")[-1]) if "|" in (note or "") else None


def _num(v) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def financial_inputs(rel: dict | None, stored_raw: dict | None, summary: dict | None = None) -> dict:
    """
    Merge the three places donation data lives:
      - Campaign Donor relationships (lookup_finance_connections / background
        enrichment): per-recipient totals, but only for recipients who are
        people in Pythia.
      - person_donation_summary (pythia-web scripts/enrich-donations.mjs):
        the same tool's full recipient list, including the campaign
        committees that never become relationships. Per-recipient totals.
      - The stored score row's raw breakdown, when it came from confidence-
        checked Phase 2 enrichment (has 'confirmed_total', added 2026-06-11).
        Includes Super PAC and FEC money. Older raw totals without
        'confirmed_total' predate identity confirmation and aren't trusted.
    The sources overlap, so each input takes the largest value, not a sum.
    """
    rel = rel or {}
    raw = stored_raw if isinstance(stored_raw, dict) else {}
    trusted = "confirmed_total" in raw
    candidates = [
        ("relationships", _num(rel.get("total")), int(rel.get("recipients") or 0),
         _num(rel.get("max_single")), "per_recipient_total", rel.get("annual") or {}),
    ]
    if summary:
        candidates.append(("donation_summary", _num(summary.get("total_donated")),
                           int(summary.get("unique_recipients") or 0),
                           _num(summary.get("max_recipient_total")), "per_recipient_total",
                           summary.get("annual_totals") or {}))
    if trusted:
        candidates.append(("enrichment", _num(raw.get("total_donated")), int(raw.get("unique_recipients") or 0),
                           _num(raw.get("max_single_donation")), "single_contribution",
                           raw.get("annual_totals") or {}))

    total, recipients, max_single, max_basis = 0.0, 0, 0.0, None
    annual: dict[str, float] = {}
    sources = []
    for name, c_total, c_recips, c_max, c_basis, c_annual in candidates:
        if c_total > 0:
            sources.append(name)
        total = max(total, c_total)
        recipients = max(recipients, c_recips)
        if c_max > max_single:
            max_single, max_basis = c_max, c_basis
        for y, v in (c_annual or {}).items():
            annual[str(y)] = max(_num(annual.get(str(y))), _num(v))

    return {
        "total_donated": round(total, 2),
        "unique_recipients": recipients,
        "max_single_donation": round(max_single, 2),
        "max_single_basis": max_basis,
        "annual_totals": {y: round(_num(v), 2) for y, v in sorted(annual.items())},
        "financial_source": "+".join(sources) or None,
        "trusted_enrichment": trusted,
    }


# Phase 2 detail carried forward so the UI's donation line keeps working.
_CARRY_RAW = ("confirmed_total", "excluded_total", "excluded_count", "warning_total",
              "superpac_total", "company_pac_total", "pac_receipts_in", "donation_count",
              "name_common", "distinct_states")


def score_contacts(conn, person_ids: list[str] | None = None) -> dict:
    """
    Compute v2.0 scores for active contacts (all, or only person_ids). READS
    ONLY — returns rows for the caller to write (compute_influence_scores) or
    print (preview_v2.py).

    Returns {"scored": [row...], "politicians_skipped": n}; each row has
    person_id, name, *_score, composite_score, breakdown.
    """
    with conn.cursor() as cur:
        # Institutional inputs, one row per current affiliation so each
        # title is judged against its own org.
        cur.execute("""
            SELECT p.id::text AS person_id, p.full_name, p.influence_tier AS person_tier,
                   po.organization_id::text AS org_id, o.name AS org_name,
                   o.influence_tier AS org_tier, po.job_title,
                   COALESCE(rt.is_decision_maker, FALSE) AS is_decision_maker,
                   COALESCE(rt.seniority_level, 0) AS seniority
            FROM people_person p
            LEFT JOIN people_personorganization po
                   ON po.person_id = p.id AND po.is_current = TRUE
            LEFT JOIN organizations_organization o ON o.id = po.organization_id
            LEFT JOIN people_roletype rt ON rt.id = po.role_type_id
            WHERE p.is_active = TRUE
        """)
        inst_map: dict[str, dict] = {}
        for r in cur.fetchall():
            d = inst_map.setdefault(r["person_id"], {
                "full_name": r["full_name"], "person_tier": r["person_tier"], "affiliations": []})
            if r["org_id"] or r["job_title"]:
                d["affiliations"].append(dict(r))

        cur.execute("""
            SELECT from_person_id::text AS person_id, to_person_id::text AS to_id, notes
            FROM people_personrelationship
            WHERE relationship_type = 'Campaign Donor' AND is_active = TRUE
        """)
        rel_map: dict[str, dict] = {}
        for r in cur.fetchall():
            amount, year = parse_donor_note(r["notes"])
            d = rel_map.setdefault(r["person_id"], {"total": 0.0, "to": set(), "max_single": 0.0,
                                                    "annual": defaultdict(float), "count": 0})
            d["total"] += amount
            d["to"].add(r["to_id"])
            d["max_single"] = max(d["max_single"], amount)
            d["count"] += 1
            if year:
                d["annual"][year] += amount
        for d in rel_map.values():
            d["recipients"] = len(d.pop("to"))

        cur.execute("""
            SELECT to_person_id::text AS person_id, COUNT(DISTINCT from_person_id) AS unique_donors
            FROM people_personrelationship
            WHERE relationship_type = 'Campaign Donor' AND is_active = TRUE
            GROUP BY to_person_id
        """)
        recvd_map = {r["person_id"]: int(r["unique_donors"] or 0) for r in cur.fetchall()}

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
        net_map = {r["person_id"]: int(r["total_connections"] or 0) for r in cur.fetchall()}

        try:
            cur.execute("SELECT person_id::text, basis FROM person_tier_basis")
            basis_map = {r["person_id"]: r["basis"] for r in cur.fetchall()}
        except Exception:
            conn.rollback()
            basis_map = {}

        try:
            cur.execute("""
                SELECT person_id::text, total_donated, unique_recipients,
                       max_recipient_total, annual_totals
                FROM person_donation_summary
            """)
            summary_map = {r["person_id"]: dict(r) for r in cur.fetchall()}
        except Exception:
            conn.rollback()
            summary_map = {}

        cur.execute("""
            SELECT person_id::text, algorithm_version, component_breakdown
            FROM people_influence_scores
        """)
        stored = {}
        politicians = set()
        for r in cur.fetchall():
            if r["algorithm_version"] == POLITICIAN_VERSION:
                politicians.add(r["person_id"])
                continue
            cb = r["component_breakdown"]
            if isinstance(cb, str):
                try:
                    cb = json.loads(cb)
                except ValueError:
                    cb = {}
            stored[r["person_id"]] = (cb or {}).get("raw") or {}

    wanted = set(person_ids) if person_ids else None
    scored = []
    for pid, inst in inst_map.items():
        if pid in politicians:
            continue  # never overwrite politician_v1
        if wanted is not None and pid not in wanted:
            continue

        # ── Institutional ─────────────────────────────────────────────────────
        # Elected office → that office's score. Everyone else → person tier,
        # tier-basis floor and org-tier bonus (or an appointed position held
        # under a principal title), capped at 75. "Former" roles don't count.
        affs = []
        former_skipped = 0
        for a in inst["affiliations"]:
            title = current_title(a["job_title"], a["org_name"])
            if title is None:
                former_skipped += 1
                continue
            affs.append({**a, "job_title": title})
        elected = appointed = 0.0
        for a in affs:
            e, ap = position_scores(a["job_title"], a["org_name"])
            elected, appointed = max(elected, e), max(appointed, ap)
        tier = min((a["org_tier"] for a in affs if a["org_tier"]), default=None)
        basis = basis_map.get(pid)
        person_tier = inst.get("person_tier")
        if elected > 0:
            institutional, inst_source = elected, "elected_office"
        else:
            by_tier = non_elected_institutional(person_tier, basis, tier)
            institutional = min(NON_ELECTED_MAX, max(by_tier, appointed))
            inst_source = "appointed_position" if appointed > by_tier else "tier"
        weights = weights_for(person_tier, basis)

        # ── Financial ─────────────────────────────────────────────────────────
        rel = rel_map.get(pid)
        stored_raw = stored.get(pid)
        fin_in = financial_inputs(rel, stored_raw, summary_map.get(pid))
        financial = calculate_financial_score_v2(
            fin_in["total_donated"], fin_in["unique_recipients"],
            fin_in["max_single_donation"], fin_in["annual_totals"])

        # ── Network (unchanged) ───────────────────────────────────────────────
        total_conn = net_map.get(pid, 0)
        network = min(100.0, math.log10(total_conn + 1) * 30) if total_conn > 0 else 0.0

        composite = composite_v2(institutional, financial, network, weights)

        raw = {}
        if fin_in["trusted_enrichment"]:
            raw.update({k: stored_raw[k] for k in _CARRY_RAW if k in stored_raw})
        raw.update({
            "best_tier": tier,
            "former_affiliations_skipped": former_skipped,
            "person_tier": person_tier,
            "institutional_source": inst_source,
            "tier_basis": basis,
            "total_donated": fin_in["total_donated"],
            "unique_recipients": fin_in["unique_recipients"],
            "max_single_donation": fin_in["max_single_donation"],
            "max_single_basis": fin_in["max_single_basis"],
            "annual_totals": fin_in["annual_totals"],
            "active_years": active_years(fin_in["annual_totals"]),
            "financial_source": fin_in["financial_source"],
            "unique_donors_in": recvd_map.get(pid, 0),
            "total_connections": total_conn,
        })
        if "donation_count" not in raw:
            raw["donation_count"] = (rel or {}).get("count", 0)

        scored.append({
            "person_id": pid,
            "name": inst.get("full_name") or "",
            "institutional_score": round(institutional, 2),
            "financial_score": round(financial, 2),
            "lobbying_score": 0.0,
            "network_score": round(network, 2),
            "engagement_score": 0.0,
            "composite_score": composite,
            "breakdown": {
                "institutional": round(institutional, 2),
                "financial": round(financial, 2),
                "network": round(network, 2),
                "lobbying": 0.0,
                "engagement": 0.0,
                "weights": weights,
                "raw": raw,
            },
        })

    return {"scored": scored, "politicians_skipped": len(politicians)}
