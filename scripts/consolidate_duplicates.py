#!/usr/bin/env python3
"""Consolidate duplicate organizations and fix FK violations.

Strategy:
  1. Group by normalize_name(); for each group with 2+ entries:
     - Pick primary (most data: prefer non-null url/amount/contact)
     - Merge others' data into primary (COALESCE-style)
     - Re-point FK references (grant_programs, foundation_focus_areas) to primary
     - Delete merged orgs
  2. Fix orphan grant_programs (FK violations)
  3. Fix URL collisions where same URL maps to multiple orgs
     - Keep URL only on primary (most data); blank others
"""
from __future__ import annotations
import re
import sqlite3
import json
from collections import defaultdict
from pathlib import Path

DB = Path("/Users/nishimura+/projects/apps/corporate-foundation-grants/corporate_research_grants.sqlite")

LEGAL_PREFIXES = [
    "公益財団法人", "一般財団法人", "公益社団法人", "一般社団法人",
    "特定非営利活動法人", "認定特定非営利活動法人",
    "ＮＰＯ法人", "NPO法人",
    "株式会社", "有限会社",
    "公益財団", "一般財団", "公益社団", "一般社団",
    # Abbreviated forms (often parenthesized)
    "（公財）", "(公財)", "（一財）", "(一財)",
    "（公社）", "(公社)", "（一社）", "(一社)",
    "（独法）", "(独法)", "（株）", "(株)", "（有）", "(有)",
    "（NPO）", "(NPO)", "（特非）", "(特非)",
]


def fullwidth_to_halfwidth(s: str) -> str:
    return s.translate(str.maketrans(
        "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ０１２３４５６７８９",
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
    ))


def normalize_name(name: str) -> str:
    if not name:
        return ""
    n = name.strip()
    # Convert full-width alpha/numeric to half-width
    n = fullwidth_to_halfwidth(n)
    # Strip social welfare and school legal prefixes
    extra_prefixes = ["社会福祉法人", "学校法人", "宗教法人", "医療法人",
                      "（社福）", "(社福)", "（学）", "(学)"]
    for _ in range(3):
        stripped = False
        for p in LEGAL_PREFIXES + extra_prefixes:
            if n.startswith(p):
                n = n[len(p):].strip()
                stripped = True
                break
        if not stripped:
            break
    # Strip trailing parenthetical NOT covering the entire string
    if not (n.startswith("（") or n.startswith("(")):
        n = re.sub(r"[（(].*?[)）]\s*$", "", n).strip()
    n = n.replace("　", "").replace(" ", "").replace("・", "").replace("・", "")
    n = n.replace("(", "").replace(")", "").replace("（", "").replace("）", "")
    return n.lower()


def field_richness(org: dict) -> int:
    """Score org by data completeness."""
    score = 0
    for f in ("url", "prefecture", "annual_grant_amount", "contact_email",
              "contact_phone", "contact_address", "description", "name_en"):
        if org.get(f):
            score += 1
    return score


def merge_metadata(meta1: str, meta2: str) -> str:
    """Merge two JSON metadata strings."""
    try:
        m1 = json.loads(meta1) if meta1 else {}
    except:
        m1 = {}
    try:
        m2 = json.loads(meta2) if meta2 else {}
    except:
        m2 = {}
    sources = set()
    for m in (m1, m2):
        s = m.get("source") or m.get("sources")
        if isinstance(s, list):
            sources.update(s)
        elif s:
            sources.add(s)
    merged = {**m2, **m1}
    if sources:
        merged["sources"] = sorted(sources)
        merged.pop("source", None)
    return json.dumps(merged, ensure_ascii=False)


def main():
    conn = sqlite3.connect(DB, timeout=60)
    conn.execute("PRAGMA busy_timeout = 60000")
    cur = conn.cursor()

    print("=== Step 1: Fix FK violations (delete orphan grant_programs) ===")
    cur.execute("""DELETE FROM grant_programs WHERE organization_id NOT IN (SELECT id FROM organizations)""")
    print(f"  Deleted orphan grant_programs: {cur.rowcount}")
    cur.execute("""DELETE FROM foundation_focus_areas WHERE organization_id NOT IN (SELECT id FROM organizations)""")
    print(f"  Deleted orphan foundation_focus_areas: {cur.rowcount}")
    conn.commit()

    print("\n=== Step 2: Group organizations by normalized name ===")
    cur.execute("SELECT id, name, name_en, type, foundation_subtype, legal_form, "
                "corporate_parent, prefecture, municipality, url, description, "
                "contact_phone, contact_email, contact_address, metadata, "
                "annual_grant_amount, jfc_rank, country_code, founder_name, "
                "established_year, koeki_id, total_assets, annual_grant_amount_history "
                "FROM organizations ORDER BY id")
    rows = cur.fetchall()
    cols = ["id", "name", "name_en", "type", "foundation_subtype", "legal_form",
            "corporate_parent", "prefecture", "municipality", "url", "description",
            "contact_phone", "contact_email", "contact_address", "metadata",
            "annual_grant_amount", "jfc_rank", "country_code", "founder_name",
            "established_year", "koeki_id", "total_assets", "annual_grant_amount_history"]
    orgs = [dict(zip(cols, r)) for r in rows]

    groups = defaultdict(list)
    for o in orgs:
        norm = normalize_name(o["name"])
        if norm:
            groups[norm].append(o)

    duplicate_groups = {k: v for k, v in groups.items() if len(v) > 1}
    print(f"  Unique normalized names: {len(groups)}")
    print(f"  Duplicate groups: {len(duplicate_groups)}")
    print(f"  Total duplicate rows: {sum(len(v) for v in duplicate_groups.values())}")

    print("\n=== Step 3: Merge duplicates ===")
    merged_count = 0
    fk_updates = 0
    for norm, group in duplicate_groups.items():
        # Pick primary: highest field_richness, prefer one with 公益財団法人 prefix
        group.sort(key=lambda o: (
            -field_richness(o),
            0 if "公益" in (o["name"] or "") else 1,
            o["id"],
        ))
        primary = group[0]
        secondaries = group[1:]

        # Merge secondaries' data into primary
        for sec in secondaries:
            updates = []
            params = []
            for f in ("name_en", "prefecture", "municipality", "url", "description",
                      "contact_phone", "contact_email", "contact_address",
                      "annual_grant_amount", "jfc_rank", "founder_name",
                      "established_year", "koeki_id", "total_assets",
                      "corporate_parent", "annual_grant_amount_history"):
                if not primary.get(f) and sec.get(f):
                    updates.append(f"{f} = ?")
                    params.append(sec[f])
                    primary[f] = sec[f]

            # Merge metadata
            merged_meta = merge_metadata(primary["metadata"] or "", sec["metadata"] or "")
            updates.append("metadata = ?")
            params.append(merged_meta)
            primary["metadata"] = merged_meta

            # Subtype: if primary is 'other' and secondary is more specific, take secondary
            if primary["foundation_subtype"] in (None, "", "other") and sec["foundation_subtype"] not in (None, "", "other"):
                updates.append("foundation_subtype = ?")
                params.append(sec["foundation_subtype"])
                primary["foundation_subtype"] = sec["foundation_subtype"]

            params.append(primary["id"])
            cur.execute(
                f"UPDATE organizations SET {', '.join(updates)}, updated_at=datetime('now','localtime') WHERE id = ?",
                params,
            )

            # Re-point FK references
            cur.execute("UPDATE grant_programs SET organization_id = ? WHERE organization_id = ?",
                        (primary["id"], sec["id"]))
            fk_updates += cur.rowcount
            # foundation_focus_areas has UNIQUE on (organization_id, category_id):
            # Delete secondary entries that conflict with primary's existing entries
            cur.execute(
                """DELETE FROM foundation_focus_areas
                   WHERE organization_id = ?
                     AND category_id IN (
                       SELECT category_id FROM foundation_focus_areas WHERE organization_id = ?
                     )""",
                (sec["id"], primary["id"]),
            )
            cur.execute("UPDATE foundation_focus_areas SET organization_id = ? WHERE organization_id = ?",
                        (primary["id"], sec["id"]))
            fk_updates += cur.rowcount

            # Delete secondary
            cur.execute("DELETE FROM organizations WHERE id = ?", (sec["id"],))
            merged_count += 1
    conn.commit()
    print(f"  Merged organizations: {merged_count}")
    print(f"  FK references updated: {fk_updates}")

    print("\n=== Step 4: Fix URL collisions (same URL on multiple orgs) ===")
    cur.execute("SELECT url, COUNT(*) c FROM organizations WHERE url IS NOT NULL AND url != '' GROUP BY url HAVING c > 1")
    collisions = cur.fetchall()
    print(f"  URL collision groups: {len(collisions)}")
    url_blanked = 0
    for url, _ in collisions:
        cur.execute("SELECT id, name, foundation_subtype, annual_grant_amount, contact_email, contact_phone "
                    "FROM organizations WHERE url = ? ORDER BY id", (url,))
        cands = cur.fetchall()
        # Score by data richness, keep URL on best one
        scored = sorted(cands, key=lambda r: -sum(1 for v in r[2:] if v))
        keep = scored[0][0]
        for r in scored[1:]:
            cur.execute("UPDATE organizations SET url = NULL, updated_at=datetime('now','localtime') WHERE id = ?",
                        (r[0],))
            url_blanked += 1
    conn.commit()
    print(f"  URLs cleared from non-primary entries: {url_blanked}")

    print("\n=== Step 5: Final verification ===")
    cur.execute("SELECT COUNT(*) FROM organizations")
    print(f"  organizations: {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM organizations WHERE url IS NOT NULL AND url != ''")
    print(f"  with url: {cur.fetchone()[0]}")

    cur.execute("PRAGMA foreign_key_check")
    fk_violations = cur.fetchall()
    print(f"  FK violations: {len(fk_violations)}")

    # Check duplicates remaining
    cur.execute("SELECT name FROM organizations")
    name_norms = defaultdict(int)
    for (n,) in cur.fetchall():
        name_norms[normalize_name(n)] += 1
    remaining_dups = sum(1 for c in name_norms.values() if c > 1)
    print(f"  Remaining duplicate name groups: {remaining_dups}")

    conn.close()


if __name__ == "__main__":
    main()
