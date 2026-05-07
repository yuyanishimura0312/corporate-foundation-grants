#!/usr/bin/env python3
"""Apply Codex Phase 5 extracted data to DB.

For matched foundations (by normalized name):
  - Backfill URL (when missing and Codex has a candidate)
  - Backfill annual_grant_amount (when missing and Codex has reasonable value)

For unmatched Codex foundations:
  - Skip (quality check needed before adding new orgs from extraction)
"""
from __future__ import annotations
import json, re, sqlite3
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "corporate_research_grants.sqlite"
EXTRACTED = ROOT / "research_results" / "codex_phase5_extracted.json"


def normalize_name(name: str) -> str:
    if not name:
        return ""
    s = re.sub(r"^(公益|一般|特定非営利活動|特例)?(財団法人|社団法人)?\s*", "", name)
    s = re.sub(r"(財団|基金|振興会|奨学会|事業団|研究会|学会|協会|機構|センター)$", "", s)
    s = s.replace("　", "").replace(" ", "").replace("公益", "").replace("法人", "")
    return s.lower()


def is_credible_url(url: str, foundation_name: str) -> bool:
    """Check if URL is plausibly the foundation's official site."""
    try:
        domain = urlparse(url).netloc.lower()
    except Exception:
        return False
    blocked = [
        "wikipedia", "facebook", "twitter", "x.com", "linkedin",
        "instagram", "youtube", "note.com", "ameblo", "hatena",
        "blogspot", "qiita", "zenn", "medium",
        "koeki-info.go.jp",  # 公益法人info itself
        "jfc.or.jp",  # JFC search results
        "jyosei-navi", "fields.canpan",
    ]
    if any(b in domain for b in blocked):
        return False
    # Prefer .or.jp / .jp / .org / .ac.jp domains
    if any(domain.endswith(t) for t in [".or.jp", ".jp", ".org", ".ac.jp"]):
        return True
    return False


def main():
    if not EXTRACTED.exists():
        print(f"Extracted file not found: {EXTRACTED}")
        return

    with open(EXTRACTED) as f:
        data = json.load(f)
    foundations = data.get("foundations", {})
    print(f"Codex extracted: {len(foundations)} foundations")

    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("SELECT id, name, url, annual_grant_amount FROM organizations")
    db_index = {}
    for rid, name, url, amt in cur.fetchall():
        db_index[normalize_name(name)] = {"id": rid, "name": name, "url": url, "amount": amt}

    url_added = amount_added = matched = unmatched = 0
    for norm, fdata in foundations.items():
        ckey = normalize_name(norm)
        if ckey in db_index:
            matched += 1
            entry = db_index[ckey]
            updates = []
            params = []

            # URL backfill
            if not entry["url"] and fdata.get("urls"):
                for url in fdata["urls"]:
                    if is_credible_url(url, fdata["name"]):
                        updates.append("url=?")
                        params.append(url)
                        url_added += 1
                        break

            # Amount backfill — be conservative, only if amount > 10M and < 100B
            if not entry["amount"] and fdata.get("amounts"):
                amounts_filtered = [a for a in fdata["amounts"]
                                     if 10_000_000 <= a <= 100_000_000_000]
                if amounts_filtered:
                    # Take median to avoid outliers
                    amounts_filtered.sort()
                    amt = amounts_filtered[len(amounts_filtered) // 2]
                    updates.append("annual_grant_amount=?")
                    params.append(amt)
                    amount_added += 1

            if updates:
                params.append(entry["id"])
                cur.execute(
                    f"UPDATE organizations SET {', '.join(updates)}, updated_at=datetime('now','localtime') WHERE id=?",
                    params,
                )
        else:
            unmatched += 1

    conn.commit()
    conn.close()

    print(f"\nMatched to existing: {matched}")
    print(f"Unmatched (potential new): {unmatched}")
    print(f"URL backfilled: {url_added}")
    print(f"Amount backfilled: {amount_added}")


if __name__ == "__main__":
    main()
