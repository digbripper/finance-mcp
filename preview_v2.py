"""
Dry run of the v2.0 influence scoring against the live DB — runs the same
influence_v2.score_contacts() the server uses, writes nothing, and compares
with the stored scores.

    DATABASE_URL=... python3 preview_v2.py [--out scoring_preview.json]

Prints spot checks (named people, random council members, nonprofit
directors, people with no donation history), the score distribution before
and after, and the biggest movers; saves every row to --out.
"""
from __future__ import annotations

import json
import os
import random
import re
import statistics
import sys

import psycopg2
import psycopg2.extras

import math

from influence_v2 import POLITICIAN_VERSION, composite_v2, score_contacts

NAMED = ["Merryl Tisch", "Mitchell Katz", "Emily Giske", "Brad Lander", "Janno Lieber",
         "Chuck Schumer", "Zohran Mamdani", "James Tisch"]
COUNCIL = re.compile(r"council ?member", re.I)
STAFF = re.compile(r"chief of staff|director|aide|staff|deputy|liaison|counsel|analyst|scheduler|manager|former", re.I)
EXEC_DIRECTOR = re.compile(r"executive director", re.I)
GOVERNMENT = re.compile(r"\b(nyc|new york city|department|office of|council|senate|assembly|state of|mayor|borough|authority)\b", re.I)


def main() -> None:
    out = "scoring_preview.json"
    if "--out" in sys.argv:
        out = sys.argv[sys.argv.index("--out") + 1]
    conn = psycopg2.connect(os.environ["DATABASE_URL"], cursor_factory=psycopg2.extras.RealDictCursor)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id::text AS person_id, p.full_name, p.influence_tier AS person_tier, b.basis,
                   s.algorithm_version, s.composite_score::float AS composite,
                   s.institutional_score::float AS inst, s.financial_score::float AS fin,
                   s.network_score::float AS net, s.lobbying_score::float AS lob,
                   s.engagement_score::float AS eng,
                   STRING_AGG(DISTINCT po.job_title, '; ') AS titles,
                   STRING_AGG(DISTINCT o.name, '; ') AS orgs
            FROM people_person p
            LEFT JOIN people_influence_scores s ON s.person_id = p.id
            LEFT JOIN person_tier_basis b ON b.person_id = p.id
            LEFT JOIN people_personorganization po ON po.person_id = p.id AND po.is_current = TRUE
            LEFT JOIN organizations_organization o ON o.id = po.organization_id
            WHERE p.is_active = TRUE
            GROUP BY p.id, p.full_name, p.influence_tier, b.basis, s.algorithm_version, s.composite_score,
                     s.institutional_score, s.financial_score, s.network_score, s.lobbying_score, s.engagement_score
        """)
        people = {r["person_id"]: dict(r) for r in cur.fetchall()}

        # For comparison only: network without synthetic Co-Donor edges.
        cur.execute("""
            SELECT person_id::text, COUNT(*) AS n FROM (
                SELECT from_person_id AS person_id FROM people_personrelationship
                WHERE is_active = TRUE AND relationship_type <> 'Co-Donor'
                UNION ALL
                SELECT to_person_id FROM people_personrelationship
                WHERE is_active = TRUE AND relationship_type <> 'Co-Donor'
            ) x JOIN people_person p ON p.id = person_id AND p.is_active = TRUE
            GROUP BY person_id
        """)
        real_conn = {r["person_id"]: int(r["n"]) for r in cur.fetchall()}

    result = score_contacts(conn)
    conn.close()
    v2 = {r["person_id"]: r for r in result["scored"]}

    rows = []
    for pid, p in people.items():
        s = v2.get(pid)
        raw = (s or {}).get("breakdown", {}).get("raw", {})
        rows.append({
            **p,
            "v2": s["composite_score"] if s else None,
            "v2_inst": s["institutional_score"] if s else None,
            "v2_fin": s["financial_score"] if s else None,
            "v2_net": s["network_score"] if s else None,
            "total_donated": raw.get("total_donated"),
            "unique_recipients": raw.get("unique_recipients"),
            "max_single": raw.get("max_single_donation"),
            "active_years": raw.get("active_years"),
            "financial_source": raw.get("financial_source"),
            "politician": p["algorithm_version"] == POLITICIAN_VERSION,
            "staff_capped": raw.get("staff_capped"),
            "floor": raw.get("institutional_floor"),
            "org_tier": raw.get("best_tier"),
        })
        rc = real_conn.get(pid, 0)
        rows[-1]["v2_no_codonor"] = (composite_v2(s["institutional_score"], s["financial_score"],
                                                  min(100.0, math.log10(rc + 1) * 30) if rc else 0.0)
                                     if s else None)

    def fmt(v):
        return "—" if v is None else f"{v:.1f}"

    def line(r, label=None):
        name = (label or r["full_name"])[:26]
        tier = f"P{r['person_tier']}" if r["person_tier"] else "—"
        if r["politician"]:
            return f"  {name:<26} {fmt(r['composite']):>7}   [skip — politician_v1]{'':<14} {tier}"
        cur = r["composite"]
        change = "" if cur is None or r["v2"] is None else f"{r['v2'] - cur:+.1f}"
        flags = (" capped" if r["staff_capped"] else "") + (f" floor{r['floor']:.0f}" if r["floor"] else "")
        detail = (f"inst {fmt(r['v2_inst'])}{flags} fin {fmt(r['v2_fin'])} net {fmt(r['v2_net'])}"
                  f" | no-co-donor {fmt(r['v2_no_codonor'])}")
        money = f" ${r['total_donated']:,.0f}/{r['unique_recipients']}r" if r["total_donated"] else ""
        basis = f" [{r['basis']}]" if r["basis"] else ""
        return (f"  {name:<26} {('new' if cur is None else fmt(cur)):>7} {fmt(r['v2']):>8} {change:>7}  "
                f"{tier:<4} {detail}{money}{basis}")

    header = f"  {'NAME':<26} {'CURRENT':>7} {'V2.0':>8} {'CHANGE':>7}  TIER"
    rng = random.Random(2026)

    print("\nNamed people")
    print(header)
    for n in NAMED:
        first, last = n.split()[0], n.split()[-1]
        hits = [r for r in rows if re.search(rf"\b{first}\b", r["full_name"] or "", re.I)
                and re.search(rf"\b{last}\b", r["full_name"] or "", re.I)]
        if not hits:
            print(f"  {n:<26} (not found)")
        for r in hits:
            print(line(r))

    def section(title, pool, k=5):
        print(f"\n{title} ({len(pool)} total, {min(k, len(pool))} shown)")
        print(header)
        for r in rng.sample(pool, min(k, len(pool))):
            print(line(r))

    council = [r for r in rows if COUNCIL.search(r["titles"] or "") and not STAFF.search(r["titles"] or "")]
    section("Council members", council)
    comptroller_staff = [r for r in rows if not r["politician"] and re.search(r"comptroller", r["orgs"] or "", re.I)
                         and r["staff_capped"]]
    section("Comptroller's Office staff (capped)", comptroller_staff)
    nonprofit = [r for r in rows if not r["politician"] and EXEC_DIRECTOR.search(r["titles"] or "")
                 and not GOVERNMENT.search(r["orgs"] or "") and (r["total_donated"] or 0) > 0]
    section("Nonprofit executive directors with donations", nonprofit)
    no_money = [r for r in rows if not r["politician"] and r["v2"] is not None and not r["total_donated"]
                and not r["org_tier"]]
    section("No donations and no org tier", no_money)

    scored = [r for r in rows if not r["politician"] and r["v2"] is not None]
    existing = [r for r in scored if r["composite"] is not None]
    missing = [r for r in scored if r["composite"] is None]
    buckets = [(0, 10), (10, 20), (20, 30), (30, 40), (40, 50), (50, 60), (60, 70), (70, 101)]

    def hist(vals):
        return [sum(1 for v in vals if lo <= v < hi) for lo, hi in buckets]

    print(f"\nDistribution — {len(existing)} people with a v1.0 score today "
          f"(+ {len(missing)} with no score, shown separately)")
    print(f"  {'range':<8} {'v1.0 now':>9} {'v2.0':>7} {'unscored→v2.0':>14}")
    cur_h, new_h = hist([r["composite"] for r in existing]), hist([r["v2"] for r in existing])
    miss_h = hist([r["v2"] for r in missing])
    for (lo, hi), a, b, c in zip(buckets, cur_h, new_h, miss_h):
        print(f"  {f'{lo}-{min(hi, 100)}':<8} {a:>9} {b:>7} {c:>14}")
    if existing:
        print(f"  median   {statistics.median(r['composite'] for r in existing):>9.1f} "
              f"{statistics.median(r['v2'] for r in existing):>7.1f}"
              f"{statistics.median(r['v2'] for r in missing) if missing else 0:>15.1f}")
        up = sum(1 for r in existing if r["v2"] > r["composite"] + 0.05)
        down = sum(1 for r in existing if r["v2"] < r["composite"] - 0.05)
        print(f"  {up} go up, {down} go down, {len(existing) - up - down} unchanged")
        with_fin = sum(1 for r in scored if (r["v2_fin"] or 0) > 0)
        print(f"  financial > 0: {with_fin} of {len(scored)} people under v2.0 "
              f"(today: {sum(1 for r in existing if (r['fin'] or 0) > 0)})")

    movers = sorted(existing, key=lambda r: r["v2"] - r["composite"])
    print("\nBiggest drops")
    print(header)
    for r in movers[:8]:
        print(line(r))
    print("\nBiggest gains")
    print(header)
    for r in movers[::-1][:8]:
        print(line(r))
    print("\nTop 15 under v2.0 (non-politicians)")
    print(header)
    for r in sorted(scored, key=lambda r: -r["v2"])[:15]:
        print(line(r))

    def named(first, last):
        hit = [r for r in rows if re.search(rf"\b{first}\b", r["full_name"] or "", re.I)
               and re.search(rf"\b{last}\b", r["full_name"] or "", re.I) and not r["politician"]]
        return max(hit, key=lambda r: r["v2"] or 0) if hit else None

    def target(label, value, lo, hi):
        if value is None:
            print(f"  - {label}: no data")
            return
        print(f"  - {label} {lo}–{hi}: {'✓' if lo <= value <= hi else '✗'} ({value:.1f})")

    def median(rs):
        vals = [r["v2"] for r in rs if r["v2"] is not None]
        return statistics.median(vals) if vals else None

    print("\nTargets")
    for (first, last), (lo, hi) in {("Merryl", "Tisch"): (70, 80), ("Mitchell", "Katz"): (68, 72),
                                    ("Emily", "Giske"): (58, 65), ("Brad", "Lander"): (60, 65)}.items():
        r = named(first, last)
        target(f"{first} {last}", r and r["v2"], lo, hi)
    target("Comptroller's Office staff (median)", median(comptroller_staff), 25, 38)
    target("Nonprofit EDs with donations (median)", median(nonprofit), 35, 50)
    target("No donations, no org tier (median)", median(no_money), 5, 15)
    politicians_touched = [r for r in rows if r["politician"] and r["person_id"] in v2]
    print(f"  - Politicians unchanged: {'✓' if not politicians_touched else '✗'} "
          f"({result['politicians_skipped']} politician_v1 rows skipped)")

    with open(out, "w") as f:
        json.dump({
            "algorithm": "v2.0",
            "politicians_skipped": result["politicians_skipped"],
            "rows": rows,
        }, f, indent=1, default=str)
    print(f"\nSaved {len(rows)} rows to {out}")


if __name__ == "__main__":
    main()
