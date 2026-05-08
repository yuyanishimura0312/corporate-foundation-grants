#!/usr/bin/env python3
"""Fix URL collisions: same URL pointing to multiple foundations.

Strategy:
  For each URL collision group, score each org by:
    - Domain ↔ name match (e.g., 'mitsubishi-zaidan.jp' matches '三菱財団' via mitsubishi)
    - JFC rank presence (authoritative)
    - Data richness (amount, awardees, etc.)
  Keep URL on best org, normalize URLs (strip trailing slash), clear from others.
"""
from __future__ import annotations
import re
import sqlite3
from collections import defaultdict
from urllib.parse import urlparse

DB = "/Users/nishimura+/projects/apps/corporate-foundation-grants/corporate_research_grants.sqlite"

# Domain → likely foundation name keywords
DOMAIN_NAME_HINTS = {
    "af-info.or.jp": ["旭硝子", "asahi", "agc"],
    "toyotafound.or.jp": ["トヨタ", "toyota"],
    "mitsubishi-zaidan.jp": ["三菱"],
    "nakatani-foundation.jp": ["中谷"],
    "takeda-sci.or.jp": ["武田"],
    "sumitomo.or.jp": ["住友"],
    "inamori-f.or.jp": ["稲盛"],
    "secomzaidan.jp": ["セコム"],
    "ueharazaidan.or.jp": ["上原"],
    "kajima-f.or.jp": ["鹿島"],
    "terumozaidan.or.jp": ["テルモ"],
    "shimadzu.co.jp": ["島津"],
    "konica-minolta-fdn.or.jp": ["コニカミノルタ"],
    "rohm-music-fdn.or.jp": ["ローム"],
    "nipponfoundation.or.jp": ["日本財団"],
    "honda-fdn.or.jp": ["ホンダ", "本田"],
    "kao-foundation.or.jp": ["花王"],
    "lotte-foundation.or.jp": ["ロッテ"],
    "panasonic.com": ["パナソニック", "松下"],
    "suntory.com": ["サントリー"],
    "fields.canpan.info": [],  # generic aggregator, blank URL on all
    "koeki-info.go.jp": [],
}


def normalize_url(url: str) -> str:
    """Normalize URL for collision detection."""
    if not url:
        return ""
    u = url.strip().rstrip("/").lower()
    # Strip trailing slash and trailing path 'index.html'
    u = re.sub(r"/index\.html?$", "", u)
    return u


def domain_of(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").lower().lstrip("www.")
    except:
        return ""


def score_org(name: str, url: str, jfc_rank: int, amount: int) -> int:
    """Score: higher = more likely the legitimate URL owner."""
    score = 0
    domain = domain_of(url)
    hints = DOMAIN_NAME_HINTS.get(domain.replace("www.", ""), None)
    if hints is None:
        # Try without www prefix
        for k, v in DOMAIN_NAME_HINTS.items():
            if k in domain:
                hints = v
                break
    if hints is not None:
        for h in hints:
            if h.lower() in (name or "").lower() or h in (name or ""):
                score += 100
                break
    if jfc_rank:
        score += 50
    if amount:
        score += 10
    if name:
        score += min(len(name), 20)
    return score


def main():
    conn = sqlite3.connect(DB, timeout=60)
    conn.execute("PRAGMA busy_timeout = 60000")
    cur = conn.cursor()

    # Step 1: Normalize URLs
    cur.execute("SELECT id, url FROM organizations WHERE url IS NOT NULL AND url != ''")
    rows = cur.fetchall()
    norm_count = 0
    for rid, url in rows:
        nu = normalize_url(url)
        if nu != url:
            cur.execute("UPDATE organizations SET url = ?, updated_at=datetime('now','localtime') WHERE id = ?",
                        (nu, rid))
            norm_count += 1
    conn.commit()
    print(f"URLs normalized: {norm_count}")

    # Step 2: Find DOMAIN-level collisions and resolve
    # (verify_db checks by domain, not full URL)
    cur.execute("""
        SELECT id, name, url, jfc_rank, annual_grant_amount FROM organizations
        WHERE url IS NOT NULL AND url != ''
    """)
    by_domain = defaultdict(list)
    for rid, name, url, rank, amount in cur.fetchall():
        d = domain_of(url).replace("www.", "")
        by_domain[d].append((rid, name, url, rank, amount))
    collisions = [(d, len(orgs)) for d, orgs in by_domain.items() if len(orgs) > 1]
    print(f"\nDomain-level collision groups: {len(collisions)}")

    cleared = 0
    blanked_aggregator = 0
    for domain, _ in collisions:
        # If aggregator domain, blank URL on all
        if any(agg in domain for agg in ["canpan", "koeki-info", "scj.go.jp", "wikipedia",
                                          "prtimes", "atpress", "nta.go.jp"]):
            for rid, _, _, _, _ in by_domain[domain]:
                cur.execute("UPDATE organizations SET url = NULL, updated_at=datetime('now','localtime') WHERE id = ?",
                            (rid,))
                blanked_aggregator += 1
            continue

        # Score each org and keep URL only on highest scorer
        cands = by_domain[domain]
        scored = sorted(cands, key=lambda r: -score_org(r[1], r[2], r[3] or 0, r[4] or 0))
        keep_id = scored[0][0]
        for rid, _, _, _, _ in scored[1:]:
            cur.execute("UPDATE organizations SET url = NULL, updated_at=datetime('now','localtime') WHERE id = ?",
                        (rid,))
            cleared += 1

    conn.commit()
    print(f"  URLs cleared (collision resolution): {cleared}")
    print(f"  URLs cleared (aggregator domains): {blanked_aggregator}")

    # Step 3: Verify domain-level
    cur.execute("SELECT id, name, url FROM organizations WHERE url IS NOT NULL AND url != ''")
    by_dom = defaultdict(int)
    for _, _, url in cur.fetchall():
        d = domain_of(url).replace("www.", "")
        by_dom[d] += 1
    remaining = sum(1 for c in by_dom.values() if c > 1)
    print(f"\n  Remaining domain collisions: {remaining}")

    cur.execute("SELECT COUNT(*) FROM organizations WHERE url IS NOT NULL AND url != ''")
    print(f"  Total orgs with URL: {cur.fetchone()[0]}")
    conn.close()


if __name__ == "__main__":
    main()
