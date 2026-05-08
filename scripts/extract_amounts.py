#!/usr/bin/env python3
"""Extract annual_grant_amount from various sources:
1. Codex Phase 4-7 extracted JSON files (already exist)
2. organizations.description (koeki purpose) — heuristic regex
3. JFC top100 amounts — apply remaining matches
"""
from __future__ import annotations
import json
import re
import sqlite3
from pathlib import Path

ROOT = Path("/Users/nishimura+/projects/apps/corporate-foundation-grants")
DB = ROOT / "corporate_research_grants.sqlite"


def parse_yen(s: str) -> int | None:
    if not s:
        return None
    s = s.replace("，", ",").replace(",", "").strip()
    s = s.translate(str.maketrans("０１２３４５６７８９", "0123456789"))
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*億\s*([0-9]+(?:\.[0-9]+)?)?\s*(?:千)?万?\s*円", s)
    if m:
        oku = float(m.group(1)) * 100_000_000
        mn = float(m.group(2)) * 10_000 if m.group(2) else 0
        return int(oku + mn)
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*億\s*円", s)
    if m:
        return int(float(m.group(1)) * 100_000_000)
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*千万\s*円", s)
    if m:
        return int(float(m.group(1)) * 10_000_000)
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*百万\s*円", s)
    if m:
        return int(float(m.group(1)) * 1_000_000)
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*万\s*円", s)
    if m:
        return int(float(m.group(1)) * 10_000)
    return None


def normalize_name(name: str) -> str:
    if not name:
        return ""
    s = re.sub(r"^(公益|一般|特定非営利活動|特例)?(財団法人|社団法人)?\s*", "", name)
    s = re.sub(r"(財団|基金|振興会|奨学会|事業団|研究会|学会|協会|機構|センター)$", "", s)
    return s.replace("　", "").replace(" ", "").replace("・", "").replace("ー", "").lower()


def main():
    conn = sqlite3.connect(DB, timeout=60)
    conn.execute("PRAGMA busy_timeout = 60000")
    cur = conn.cursor()

    cur.execute("SELECT id, name, description, annual_grant_amount FROM organizations")
    orgs = cur.fetchall()

    # 1. Apply Codex Phase 4-7 extracted amounts (those that haven't been applied)
    codex_files = [
        ROOT / "research_results" / "codex_phase4_extracted.json",
        ROOT / "research_results" / "codex_phase5_extracted.json",
        ROOT / "research_results" / "codex_phase6_extracted.json",
    ]
    codex_amounts = {}  # norm_name -> amounts list
    for f in codex_files:
        if not f.exists():
            continue
        with open(f) as fp:
            data = json.load(fp)
        for k, fdata in data.get("foundations", {}).items():
            n = normalize_name(k)
            if n and fdata.get("amounts"):
                codex_amounts.setdefault(n, []).extend(
                    a for a in fdata["amounts"] if 10_000_000 <= a <= 100_000_000_000
                )

    db_index = {normalize_name(name): {"id": rid, "amount": amt}
                for rid, name, _, amt in orgs}

    codex_added = 0
    for n, amts in codex_amounts.items():
        if n in db_index and not db_index[n]["amount"]:
            amts.sort()
            amt = amts[len(amts) // 2]  # median
            cur.execute(
                "UPDATE organizations SET annual_grant_amount = ?, updated_at=datetime('now','localtime') WHERE id = ?",
                (amt, db_index[n]["id"]),
            )
            codex_added += 1
            db_index[n]["amount"] = amt
    print(f"Codex amounts applied: {codex_added}")

    # 2. Extract from description text
    cur.execute("""SELECT id, name, description FROM organizations
                   WHERE annual_grant_amount IS NULL
                     AND description IS NOT NULL AND description != ''""")
    desc_targets = cur.fetchall()
    print(f"Description targets: {len(desc_targets)}")

    desc_added = 0
    for rid, name, desc in desc_targets:
        # Look for grant amount keywords near a yen value
        # Patterns: "助成総額", "年間助成", "助成額", "事業費"
        patterns = [
            r"(助成総額|年間助成|助成額|事業費|助成事業費)[：:\s]*([0-9０-９,，]+(?:\.[0-9０-９]+)?\s*(?:億|千万|百万|万)?\s*円)",
            r"年[間額計]?\s*([0-9０-９,，]+(?:\.[0-9０-９]+)?\s*(?:億|千万|百万|万)?\s*円)\s*(?:程度|を|の助成)",
        ]
        amount = None
        for pat in patterns:
            m = re.search(pat, desc)
            if m:
                amount_str = m.group(2) if len(m.groups()) >= 2 else m.group(1)
                amount = parse_yen(amount_str)
                if amount and 1_000_000 <= amount <= 100_000_000_000:
                    break
                amount = None
        if amount:
            cur.execute(
                "UPDATE organizations SET annual_grant_amount = ?, updated_at=datetime('now','localtime') WHERE id = ?",
                (amount, rid),
            )
            desc_added += 1

    print(f"Description amounts extracted: {desc_added}")

    conn.commit()
    cur.execute("SELECT COUNT(*) FROM organizations WHERE annual_grant_amount IS NOT NULL")
    total_with_amount = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM organizations")
    total = cur.fetchone()[0]
    print(f"\nTotal with amount: {total_with_amount}/{total} ({total_with_amount/total*100:.1f}%)")
    conn.close()


if __name__ == "__main__":
    main()
