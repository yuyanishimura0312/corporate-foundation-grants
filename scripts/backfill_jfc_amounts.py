#!/usr/bin/env python3
"""
Backfill jfc_rank and annual_grant_amount from JFC top100 data
using normalized fuzzy name matching.
"""
from __future__ import annotations
import json, re, sqlite3
from pathlib import Path

ROOT = Path("/Users/nishimura+/projects/apps/corporate-foundation-grants")
DB = ROOT / "corporate_research_grants.sqlite"
JFC_TOP100 = ROOT / "data" / "jfc_top100_amounts.json"


def normalize(name: str) -> str:
    if not name:
        return ""
    s = re.sub(r"^(公益|一般|特定非営利活動|特例)?(財団法人|社団法人)?\s*", "", name)
    s = re.sub(r"(財団|基金|振興会|奨学会|事業団|研究会|学会|協会|機構|センター)$", "", s)
    s = s.replace("　", "").replace(" ", "")
    s = s.replace("公益", "").replace("法人", "")
    return s.lower()


def main():
    with open(JFC_TOP100) as f:
        jfc = json.load(f)

    matched_existing = jfc.get("matched", [])
    unmatched = jfc.get("unmatched", [])
    print(f"JFC matched (already): {len(matched_existing)}")
    print(f"JFC unmatched: {len(unmatched)}")

    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("SELECT id, name, jfc_rank, annual_grant_amount FROM organizations")
    orgs = cur.fetchall()
    norm_index = {}
    for rid, name, rank, amt in orgs:
        norm_index.setdefault(normalize(name), []).append((rid, name, rank, amt))

    new_matches = 0
    update_count = 0

    # Apply existing matches first to ensure jfc_rank present
    for m in matched_existing:
        db_name = m.get("db_name", "")
        rank = m.get("rank")
        amount = m.get("amount")
        for cand_id, cand_name, cur_rank, cur_amt in norm_index.get(normalize(db_name), []):
            new_rank = cur_rank or rank
            new_amt = cur_amt or amount
            if new_rank != cur_rank or new_amt != cur_amt:
                cur.execute(
                    "UPDATE organizations SET jfc_rank=?, annual_grant_amount=?, updated_at=datetime('now','localtime') WHERE id=?",
                    (new_rank, new_amt, cand_id),
                )
                update_count += 1

    # Apply unmatched with fuzzy name matching
    for u in unmatched:
        if isinstance(u, dict):
            top100_name = u.get("top100_name", "") or u.get("name", "")
            rank = u.get("rank")
            amount = u.get("amount")
        else:
            continue

        if not top100_name:
            continue
        norm = normalize(top100_name)
        if norm in norm_index:
            cands = norm_index[norm]
            # Take first match
            cand_id, cand_name, cur_rank, cur_amt = cands[0]
            new_rank = cur_rank or rank
            new_amt = cur_amt or amount
            cur.execute(
                "UPDATE organizations SET jfc_rank=?, annual_grant_amount=?, updated_at=datetime('now','localtime') WHERE id=?",
                (new_rank, new_amt, cand_id),
            )
            new_matches += 1
            print(f"  matched: rank={rank} {top100_name} -> {cand_name}")

    conn.commit()

    cur.execute("SELECT COUNT(*) FROM organizations WHERE jfc_rank IS NOT NULL")
    total_with_rank = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM organizations WHERE annual_grant_amount IS NOT NULL")
    total_with_amount = cur.fetchone()[0]

    print(f"\nJFC backfill complete:")
    print(f"  New unmatched->matched: {new_matches}")
    print(f"  Total updates (incl existing): {update_count + new_matches}")
    print(f"  Organizations with jfc_rank: {total_with_rank}")
    print(f"  Organizations with annual_grant_amount: {total_with_amount}")
    conn.close()


if __name__ == "__main__":
    main()
