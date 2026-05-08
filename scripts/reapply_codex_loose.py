#!/usr/bin/env python3
"""Re-apply Codex Phase 4-7 extracted URL/amount data with looser fuzzy matching.

The original apply_codex_data.py used strict normalization. Many candidates
didn't match because of suffix variations (財団 vs 財団法人 etc.) or compound
names. This version uses substring matching for higher recall.
"""
from __future__ import annotations
import json
import re
import sqlite3
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path("/Users/nishimura+/projects/apps/corporate-foundation-grants")
DB = ROOT / "corporate_research_grants.sqlite"


def normalize_loose(name: str) -> str:
    """Looser normalization for fuzzy matching."""
    if not name:
        return ""
    s = re.sub(r"^(公益|一般|特定非営利活動|認定特定非営利活動|特例)?(財団法人|社団法人)?\s*", "", name)
    s = re.sub(r"^(\(公財\)|（公財）|\(一財\)|（一財）|\(公社\)|（公社）|\(一社\)|（一社）|\(独\)|（独）)\s*", "", s)
    s = re.sub(r"(財団|基金|振興会|奨学会|事業団|研究会|学会|協会|機構|センター|株式会社|会社|法人|財団法人|社団法人)$", "", s)
    s = re.sub(r"[（(].*?[)）]", "", s)  # remove all parentheses
    s = s.replace("　", "").replace(" ", "").replace("・", "").replace("-", "").replace("ー", "")
    return s.lower().strip()


def is_credible_url(url: str) -> bool:
    try:
        domain = urlparse(url).netloc.lower()
    except:
        return False
    blocked = ["wikipedia", "facebook", "twitter", "x.com", "linkedin",
               "instagram", "youtube", "note.com", "ameblo", "blogspot",
               "qiita", "zenn", "medium", "koeki-info.go.jp", "jfc.or.jp",
               "jyosei-navi", "fields.canpan", "canpan.info", "nta.go.jp",
               "prtimes", "atpress", "amazon.co.jp", "rakuten",
               "google.com", "google.co.jp", "yahoo"]
    if any(b in domain for b in blocked):
        return False
    return any(domain.endswith(t) for t in [".or.jp", ".jp", ".org", ".ac.jp", ".co.jp", ".com"])


def main():
    conn = sqlite3.connect(DB, timeout=60)
    conn.execute("PRAGMA busy_timeout = 60000")
    cur = conn.cursor()

    # Build all DB orgs by loose-norm
    cur.execute("SELECT id, name, url, annual_grant_amount FROM organizations")
    db_index = {}
    for rid, name, url, amt in cur.fetchall():
        n = normalize_loose(name)
        if n and len(n) >= 2:
            db_index.setdefault(n, []).append({"id": rid, "name": name, "url": url, "amount": amt})

    # Load all Codex data
    codex_files = [
        ROOT / "research_results" / "codex_phase4_extracted.json",
        ROOT / "research_results" / "codex_phase5_extracted.json",
        ROOT / "research_results" / "codex_phase6_extracted.json",
    ]
    all_foundations = {}
    for f in codex_files:
        if not f.exists():
            continue
        with open(f) as fp:
            data = json.load(fp)
        for k, fdata in data.get("foundations", {}).items():
            n = normalize_loose(k)
            if not n:
                continue
            if n in all_foundations:
                # Merge
                for u in fdata.get("urls", []):
                    if u not in all_foundations[n]["urls"]:
                        all_foundations[n]["urls"].append(u)
                for a in fdata.get("amounts", []):
                    if a not in all_foundations[n]["amounts"]:
                        all_foundations[n]["amounts"].append(a)
            else:
                all_foundations[n] = {
                    "name": fdata.get("name", k),
                    "urls": list(fdata.get("urls", [])),
                    "amounts": list(fdata.get("amounts", [])),
                }
    print(f"Codex unique foundations (loose-norm): {len(all_foundations)}")

    url_added = 0
    amount_added = 0
    matched_orgs = set()

    for n, cdata in all_foundations.items():
        # Try exact loose match
        candidates = db_index.get(n, [])
        # Substring fallback
        if not candidates and len(n) >= 4:
            for db_n, entries in db_index.items():
                if (n in db_n or db_n in n) and len(min(n, db_n, key=len)) >= 4:
                    candidates.extend(entries)

        for entry in candidates:
            if entry["id"] in matched_orgs:
                continue
            updates = []
            params = []
            if not entry["url"] and cdata["urls"]:
                for url in cdata["urls"]:
                    if is_credible_url(url):
                        # Strip trailing slash for consistency
                        url = url.rstrip("/").lower()
                        updates.append("url=?")
                        params.append(url)
                        url_added += 1
                        break
            if not entry["amount"] and cdata["amounts"]:
                amts = sorted([a for a in cdata["amounts"] if 10_000_000 <= a <= 100_000_000_000])
                if amts:
                    updates.append("annual_grant_amount=?")
                    params.append(amts[len(amts) // 2])
                    amount_added += 1
            if updates:
                params.append(entry["id"])
                cur.execute(
                    f"UPDATE organizations SET {', '.join(updates)}, updated_at=datetime('now','localtime') WHERE id=?",
                    params,
                )
                matched_orgs.add(entry["id"])
                break  # only update one entry per Codex foundation

    conn.commit()
    cur.execute("SELECT COUNT(*) FROM organizations WHERE url IS NOT NULL AND url != ''")
    url_total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM organizations WHERE annual_grant_amount IS NOT NULL")
    amt_total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM organizations")
    total = cur.fetchone()[0]

    print(f"\nLoose match results:")
    print(f"  URL added: {url_added}")
    print(f"  Amount added: {amount_added}")
    print(f"  Matched orgs: {len(matched_orgs)}")
    print(f"\nTotal with URL: {url_total}/{total} ({url_total/total*100:.1f}%)")
    print(f"Total with amount: {amt_total}/{total} ({amt_total/total*100:.1f}%)")
    conn.close()


if __name__ == "__main__":
    main()
