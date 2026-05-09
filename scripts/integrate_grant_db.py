#!/usr/bin/env python3
"""CFG ↔ Grant DB の対応関係を構築して cross_db_mapping に投入"""
from __future__ import annotations
import re
import sqlite3
import time
import uuid
from pathlib import Path
from urllib.parse import urlparse

CFG_DB = Path("/Users/nishimura+/projects/apps/corporate-foundation-grants/corporate_research_grants.sqlite")
GRANT_DB = Path("/Users/nishimura+/projects/apps/grant-db/grant_db.sqlite")

LEGAL_PREFIXES = [
    "公益財団法人", "一般財団法人", "公益社団法人", "一般社団法人",
    "特定非営利活動法人", "認定特定非営利活動法人",
    "ＮＰＯ法人", "NPO法人", "株式会社", "有限会社",
    "公益財団", "一般財団", "公益社団", "一般社団",
    "（公財）", "(公財)", "（一財）", "(一財)",
    "（公社）", "(公社)", "（一社）", "(一社)",
    "（独法）", "(独法)", "（株）", "(株)", "（有）", "(有)",
    "社会福祉法人", "学校法人", "独立行政法人", "国立研究開発法人",
]


def fullwidth_to_halfwidth(s: str) -> str:
    return s.translate(str.maketrans(
        "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ",
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
    ))


def normalize_name(name: str) -> str:
    if not name:
        return ""
    n = fullwidth_to_halfwidth(name.strip())
    for _ in range(3):
        stripped = False
        for p in LEGAL_PREFIXES:
            if n.startswith(p):
                n = n[len(p):].strip()
                stripped = True
                break
        if not stripped:
            break
    n = re.sub(r"[（(].*?[)）]\s*$", "", n).strip()
    n = n.replace("　", "").replace(" ", "").replace("・", "")
    n = n.replace("(", "").replace(")", "").replace("（", "").replace("）", "")
    return n.lower()


def url_host(url: str) -> str:
    if not url:
        return ""
    try:
        h = urlparse(url).hostname or ""
    except Exception:
        return ""
    return h.lower().replace("www.", "")


def main():
    started = time.time()
    cfg = sqlite3.connect(CFG_DB, timeout=60)
    cfg.execute("PRAGMA busy_timeout = 60000")
    gdb = sqlite3.connect(GRANT_DB, timeout=60)

    # Load all
    cfg_cur = cfg.cursor()
    cfg_cur.execute("SELECT id, name, url FROM organizations")
    cfg_orgs = cfg_cur.fetchall()
    print(f"CFG organizations: {len(cfg_orgs)}")

    gdb_cur = gdb.cursor()
    gdb_cur.execute("SELECT id, name, url FROM organizations")
    gdb_orgs = gdb_cur.fetchall()
    print(f"Grant DB organizations: {len(gdb_orgs)}")

    # Build indices
    cfg_by_norm = {}
    cfg_by_url = {}
    for cid, name, url in cfg_orgs:
        nm = normalize_name(name)
        if nm:
            cfg_by_norm.setdefault(nm, []).append((cid, name))
        h = url_host(url or "")
        if h:
            cfg_by_url.setdefault(h, []).append((cid, name))

    gdb_by_norm = {}
    gdb_by_url = {}
    for gid, name, url in gdb_orgs:
        nm = normalize_name(name)
        if nm:
            gdb_by_norm.setdefault(nm, []).append((gid, name))
        h = url_host(url or "")
        if h:
            gdb_by_url.setdefault(h, []).append((gid, name))

    # Match
    matches = []  # (cfg_id, gdb_id, method, confidence, cfg_name, gdb_name)
    matched_cfg_ids = set()
    matched_gdb_ids = set()

    # 1) Exact normalized name
    for nm, cfg_list in cfg_by_norm.items():
        if nm in gdb_by_norm:
            for cid, cn in cfg_list:
                if cid in matched_cfg_ids:
                    continue
                for gid, gn in gdb_by_norm[nm]:
                    if gid in matched_gdb_ids:
                        continue
                    matches.append((cid, gid, "name_normalized", 0.95, cn, gn))
                    matched_cfg_ids.add(cid)
                    matched_gdb_ids.add(gid)
                    break

    # 2) URL host match (skip aggregator hosts)
    skip_hosts = {
        "wikipedia.org", "ja.wikipedia.org", "facebook.com", "twitter.com",
        "x.com", "youtube.com", "koeki-info.go.jp", "jfc.or.jp",
        "jyosei-navi.jfc.or.jp", "fields.canpan.info", "canpan.info",
        "nta.go.jp",
    }
    for host, cfg_list in cfg_by_url.items():
        if host in skip_hosts:
            continue
        if host in gdb_by_url:
            for cid, cn in cfg_list:
                if cid in matched_cfg_ids:
                    continue
                for gid, gn in gdb_by_url[host]:
                    if gid in matched_gdb_ids:
                        continue
                    matches.append((cid, gid, "url", 0.85, cn, gn))
                    matched_cfg_ids.add(cid)
                    matched_gdb_ids.add(gid)
                    break

    # 3) Substring match (loose, high recall)
    for nm, cfg_list in cfg_by_norm.items():
        if not nm or len(nm) < 4:
            continue
        for cid, cn in cfg_list:
            if cid in matched_cfg_ids:
                continue
            # Search loose substring in gdb
            for gnm, gdb_list in gdb_by_norm.items():
                if not gnm or len(gnm) < 4:
                    continue
                if (nm in gnm or gnm in nm) and len(min(nm, gnm, key=len)) >= 4:
                    for gid, gn in gdb_list:
                        if gid in matched_gdb_ids:
                            continue
                        matches.append((cid, gid, "name_substring", 0.70, cn, gn))
                        matched_cfg_ids.add(cid)
                        matched_gdb_ids.add(gid)
                        break
                    break

    print(f"\nMatches found: {len(matches)}")
    print(f"  by normalized name: {sum(1 for m in matches if m[2] == 'name_normalized')}")
    print(f"  by URL host:        {sum(1 for m in matches if m[2] == 'url')}")
    print(f"  by substring:       {sum(1 for m in matches if m[2] == 'name_substring')}")
    print(f"\nUnmatched CFG: {len(cfg_orgs) - len(matched_cfg_ids)}")
    print(f"Unmatched Grant DB: {len(gdb_orgs) - len(matched_gdb_ids)}")

    # Save to cross_db_mapping (clear old, insert new)
    cfg_cur.execute("DELETE FROM cross_db_mapping")
    cfg_cur.executemany(
        """INSERT OR IGNORE INTO cross_db_mapping
           (cfg_id, grant_db_id, match_method, match_confidence, cfg_name, grant_db_name, created_at)
           VALUES (?, ?, ?, ?, ?, ?, datetime('now','localtime'))""",
        matches,
    )

    # Log to integration_metadata
    cfg_cur.execute(
        """INSERT INTO integration_metadata
           (id, operation, cfg_count, grant_db_count, matched_count,
            unmatched_cfg, unmatched_gdb, duration_ms, started_at, completed_at)
           VALUES (?, 'full_sync', ?, ?, ?, ?, ?, ?, ?,
                   datetime('now','localtime'))""",
        (str(uuid.uuid4()), len(cfg_orgs), len(gdb_orgs), len(matches),
         len(cfg_orgs) - len(matched_cfg_ids), len(gdb_orgs) - len(matched_gdb_ids),
         int((time.time() - started) * 1000),
         time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(started))),
    )

    cfg.commit()
    cfg.close()
    gdb.close()

    print(f"\nDone in {time.time() - started:.1f}s")


if __name__ == "__main__":
    main()
