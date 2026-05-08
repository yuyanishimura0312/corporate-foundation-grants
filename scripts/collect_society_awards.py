#!/usr/bin/env python3
"""Collect academic-society award programs into the CFG database.

Source population
-----------------
日本学術会議協力学術研究団体 (~2,000) のうち、研究奨励賞・若手研究者賞・
優秀論文賞・学会賞を運営している主要学会 ~150 をキュレートし、
``data/societies/societies_seed.json`` に格納している。本スクリプトは
シードリストを起点に以下を行う:

1. 各学会公式サイトの awards URL（既知）を ``cache/societies/<slug>.html``
   に取得（rate-limit 1req/3sec, robots.txt 尊重, 404/timeout は許容）。
2. 既存 ``organizations`` テーブルとの重複検出: ``normalize_name`` で
   "公益社団法人XX" 等の法人格プレフィクスを剥がして照合。
3. 新規学会は ``type='foundation'``, ``foundation_subtype='academic'``,
   ``legal_form`` (`公益社団法人` / `一般社団法人` 等) で INSERT。
   既存学会は欠損フィールドを補完 UPDATE。
4. シード内の各 award を ``grant_programs`` に upsert。

実行
----
::

    # ドライラン (DBは触らずスクレイプのみ)
    python scripts/collect_society_awards.py --dry-run

    # スクレイプは行わずシードのみ DB 反映
    python scripts/collect_society_awards.py --no-fetch

    # フル実行
    python scripts/collect_society_awards.py
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
import sys
import time
import unicodedata
import urllib.parse
import urllib.robotparser
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "corporate_research_grants.sqlite"
SEED = ROOT / "data" / "societies" / "societies_seed.json"
CACHE_DIR = ROOT / "cache" / "societies"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

USER_AGENT = (
    "MiratukuBot/1.0 (Foundation Grants Research; "
    "+contact:dialoguebar@gmail.com) python-requests"
)
INTERVAL_SEC = 3.0
TIMEOUT = (15, 30)  # connect / read

LOG = logging.getLogger("collect_society_awards")

# --------------------------------------------------------------------------- #
# Normalization helpers
# --------------------------------------------------------------------------- #
LEGAL_PREFIX_RE = re.compile(
    r"^(公益|一般|特定非営利活動|特例)?(財団法人|社団法人)?\s*"
)


def normalize_name(name: str) -> str:
    """Drop legal-form prefixes / spaces / case for fuzzy matching.

    Mirrors :func:`scripts.import_umin.normalize_name` so de-duplication
    is consistent across importers.
    """
    if not name:
        return ""
    s = unicodedata.normalize("NFKC", name)
    s = LEGAL_PREFIX_RE.sub("", s)
    s = s.replace("　", "").replace(" ", "")
    return s.lower()


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def new_id(prefix: str = "") -> str:
    return f"{prefix}{uuid.uuid4().hex[:12]}"


# --------------------------------------------------------------------------- #
# HTTP layer (cache + rate limit + robots)
# --------------------------------------------------------------------------- #
_last_request_at: dict[str, float] = {}
_robots_cache: dict[str, urllib.robotparser.RobotFileParser] = {}


def _robots_allows(url: str) -> bool:
    try:
        parsed = urllib.parse.urlparse(url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        rp = _robots_cache.get(origin)
        if rp is None:
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(f"{origin}/robots.txt")
            try:
                rp.read()
            except Exception as exc:  # noqa: BLE001
                LOG.debug("robots fetch failed for %s: %s", origin, exc)
                _robots_cache[origin] = rp
                return True
            _robots_cache[origin] = rp
        return rp.can_fetch(USER_AGENT, url)
    except Exception:  # noqa: BLE001
        return True


def _respect_rate_limit(host: str) -> None:
    now = time.monotonic()
    last = _last_request_at.get(host, 0.0)
    delta = now - last
    if delta < INTERVAL_SEC:
        time.sleep(INTERVAL_SEC - delta)
    _last_request_at[host] = time.monotonic()


def fetch_awards_page(slug: str, url: str) -> Optional[str]:
    """Fetch an awards URL with caching. Returns text or None on failure."""
    if not url:
        return None
    cache_path = CACHE_DIR / f"{slug}.html"
    if cache_path.exists() and cache_path.stat().st_size > 200:
        try:
            return cache_path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            return cache_path.read_bytes().decode("utf-8", errors="replace")

    if not _robots_allows(url):
        LOG.warning("robots.txt disallows %s", url)
        return None

    host = urllib.parse.urlparse(url).netloc
    _respect_rate_limit(host)
    try:
        LOG.info("GET %s", url)
        resp = requests.get(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept-Language": "ja,en;q=0.8",
            },
            timeout=TIMEOUT,
            allow_redirects=True,
        )
        resp.raise_for_status()
        # Heuristic encoding detection
        if not resp.encoding or resp.encoding.lower() in ("iso-8859-1",):
            resp.encoding = resp.apparent_encoding or "utf-8"
        text = resp.text
        cache_path.write_text(text, encoding="utf-8")
        return text
    except requests.exceptions.RequestException as exc:
        LOG.warning("fetch failed for %s: %s", url, exc)
        return None


# --------------------------------------------------------------------------- #
# Seed loading
# --------------------------------------------------------------------------- #
def load_seed() -> list[dict]:
    if not SEED.exists():
        raise FileNotFoundError(f"seed file missing: {SEED}")
    data = json.loads(SEED.read_text(encoding="utf-8"))
    return list(data.get("societies", []))


# --------------------------------------------------------------------------- #
# DB upsert
# --------------------------------------------------------------------------- #
def find_existing_org(
    conn: sqlite3.Connection, name: str
) -> Optional[tuple[str, str]]:
    """Return (id, name) if any organizations row matches by normalized name."""
    target = normalize_name(name)
    if not target:
        return None
    cur = conn.execute("SELECT id, name FROM organizations")
    for oid, oname in cur.fetchall():
        if normalize_name(oname) == target:
            return oid, oname
    return None


def upsert_organization(
    conn: sqlite3.Connection, society: dict, dry_run: bool = False
) -> tuple[str, bool]:
    """Insert or update organizations row. Returns (id, is_new)."""
    name = society["name"]
    existing = find_existing_org(conn, name)
    metadata = {
        "source": "society_awards",
        "field": society.get("field"),
        "subfield": society.get("subfield"),
        "awards_url": society.get("awards_url"),
        "scj_cooperating_society": True,
    }
    meta_json = json.dumps(metadata, ensure_ascii=False)

    if existing:
        oid, _ = existing
        if dry_run:
            return oid, False
        # Merge metadata: keep existing keys, add ours where missing.
        cur = conn.execute(
            "SELECT metadata, foundation_subtype, legal_form, url FROM organizations WHERE id=?",
            (oid,),
        )
        row = cur.fetchone()
        prior_meta = {}
        if row and row[0]:
            try:
                prior_meta = json.loads(row[0])
            except json.JSONDecodeError:
                prior_meta = {}
        merged = {**metadata, **prior_meta}
        merged.setdefault("scj_cooperating_society", True)
        merged_json = json.dumps(merged, ensure_ascii=False)
        conn.execute(
            """UPDATE organizations SET
                   name_en=COALESCE(name_en, ?),
                   url=COALESCE(NULLIF(url,''), ?),
                   foundation_subtype=COALESCE(foundation_subtype, ?),
                   legal_form=COALESCE(legal_form, ?),
                   metadata=?,
                   updated_at=?
               WHERE id=?""",
            (
                society.get("name_en") or None,
                society.get("url") or None,
                "academic",
                society.get("legal_form") or None,
                merged_json,
                now_str(),
                oid,
            ),
        )
        return oid, False

    # New org
    oid = new_id("org_")
    if dry_run:
        return oid, True
    conn.execute(
        """INSERT INTO organizations (
               id, name, name_en, type, url, foundation_subtype, legal_form,
               country_code, metadata, created_at, updated_at
           ) VALUES (?, ?, ?, 'foundation', ?, 'academic', ?, 'JP', ?, ?, ?)""",
        (
            oid,
            name,
            society.get("name_en") or None,
            society.get("url") or None,
            society.get("legal_form") or None,
            meta_json,
            now_str(),
            now_str(),
        ),
    )
    return oid, True


def upsert_program(
    conn: sqlite3.Connection,
    organization_id: str,
    award: dict,
    society: dict,
    dry_run: bool = False,
) -> tuple[str, bool]:
    """Insert or update a grant_programs row. Returns (id, is_new)."""
    name = award["name"]
    cur = conn.execute(
        "SELECT id FROM grant_programs WHERE organization_id=? AND name=? LIMIT 1",
        (organization_id, name),
    )
    row = cur.fetchone()
    metadata = {
        "source": "society_awards",
        "award_type": award.get("type"),
        "target": award.get("target"),
        "field": society.get("field"),
        "subfield": society.get("subfield"),
    }
    meta_json = json.dumps(metadata, ensure_ascii=False)
    purpose = (
        f"{society['name']}が運営する{award.get('type','学会')}賞。"
        f"対象: {award.get('target','会員')}"
    )

    if row:
        pid = row[0]
        if dry_run:
            return pid, False
        conn.execute(
            """UPDATE grant_programs SET
                   description=COALESCE(NULLIF(description,''), ?),
                   purpose=COALESCE(NULLIF(purpose,''), ?),
                   category=COALESCE(category, 'research'),
                   subcategories=COALESCE(NULLIF(subcategories,''), ?),
                   source_url=COALESCE(NULLIF(source_url,''), ?),
                   metadata=?,
                   updated_at=?
               WHERE id=?""",
            (
                purpose,
                purpose,
                society.get("subfield"),
                society.get("awards_url") or society.get("url"),
                meta_json,
                now_str(),
                pid,
            ),
        )
        return pid, False

    pid = new_id("prog_")
    if dry_run:
        return pid, True
    conn.execute(
        """INSERT INTO grant_programs (
               id, organization_id, name, description, purpose, category,
               subcategories, is_recurring, source_url, metadata,
               created_at, updated_at
           ) VALUES (?, ?, ?, ?, ?, 'research', ?, 1, ?, ?, ?, ?)""",
        (
            pid,
            organization_id,
            name,
            purpose,
            purpose,
            society.get("subfield"),
            society.get("awards_url") or society.get("url"),
            meta_json,
            now_str(),
            now_str(),
        ),
    )
    return pid, True


# --------------------------------------------------------------------------- #
# Pipeline
# --------------------------------------------------------------------------- #
def backup_db() -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bak = DB.with_suffix(f".sqlite.bak.society_{ts}")
    bak.write_bytes(DB.read_bytes())
    return bak


def run(
    *,
    dry_run: bool = False,
    no_fetch: bool = False,
    limit: Optional[int] = None,
) -> dict:
    seed = load_seed()
    if limit:
        seed = seed[:limit]
    LOG.info("loaded %d societies from seed", len(seed))

    if not dry_run:
        bak = backup_db()
        LOG.info("DB backup -> %s", bak.name)

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    stats = {
        "societies_total": len(seed),
        "fetched": 0,
        "fetch_failed": 0,
        "orgs_inserted": 0,
        "orgs_updated": 0,
        "programs_inserted": 0,
        "programs_updated": 0,
        "awards_total": 0,
    }

    for soc in seed:
        slug = soc.get("slug") or normalize_name(soc.get("name", ""))[:30]
        if not no_fetch and soc.get("awards_url"):
            html = fetch_awards_page(slug, soc["awards_url"])
            if html:
                stats["fetched"] += 1
            else:
                stats["fetch_failed"] += 1

        oid, is_new_org = upsert_organization(conn, soc, dry_run=dry_run)
        if is_new_org:
            stats["orgs_inserted"] += 1
        else:
            stats["orgs_updated"] += 1

        for award in soc.get("awards", []) or []:
            stats["awards_total"] += 1
            _, is_new_prog = upsert_program(
                conn, oid, award, soc, dry_run=dry_run
            )
            if is_new_prog:
                stats["programs_inserted"] += 1
            else:
                stats["programs_updated"] += 1

    if not dry_run:
        conn.commit()

    # Final totals (post-commit)
    cur = conn.execute("SELECT COUNT(*) FROM organizations")
    stats["db_total_orgs"] = cur.fetchone()[0]
    cur = conn.execute("SELECT COUNT(*) FROM grant_programs")
    stats["db_total_programs"] = cur.fetchone()[0]
    cur = conn.execute(
        "SELECT COUNT(*) FROM organizations WHERE foundation_subtype='academic'"
    )
    stats["db_academic_orgs"] = cur.fetchone()[0]
    cur = conn.execute(
        "SELECT COUNT(*) FROM grant_programs "
        "WHERE metadata LIKE '%\"source\": \"society_awards\"%'"
    )
    stats["db_society_programs"] = cur.fetchone()[0]

    conn.close()
    return stats


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="DBを更新しない (スクレイプ・解析のみ)",
    )
    parser.add_argument(
        "--no-fetch",
        action="store_true",
        help="HTTPフェッチを行わない (シードのみDB反映)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="先頭N学会のみ処理 (デバッグ用)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="DEBUGログを有効化"
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    stats = run(
        dry_run=args.dry_run, no_fetch=args.no_fetch, limit=args.limit
    )
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
