#!/usr/bin/env python3
"""
discover_urls.py — 研究助成財団DB公式URL自動探索

対象: organizations WHERE url IS NULL OR url = ''
手法（優先順）:
  1) DuckDuckGo HTML検索（API不要、レート制限緩い）
     - クエリA: "<財団名> 公式 site:.or.jp"
     - クエリB: "<財団名> 公式"
     - クエリC: "<財団名>"
  2) 検証: ドメインが .or.jp/.jp/.org、トップページのtitle/h1に財団名一致
  3) UPDATE organizations.url, metadata.url_source, metadata.url_discovered_at

バッチ処理: --limit で件数制御、metadata.url_attempted_at により再開可能
レート制限: 1req/3sec、検索エンジンキューと検証用HTTPキューを分離
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

try:
    from ddgs import DDGS
except Exception:
    from duckduckgo_search import DDGS  # type: ignore

DB_PATH = Path("/Users/nishimura+/projects/apps/corporate-foundation-grants/corporate_research_grants.sqlite")

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_0) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

# 信頼できないドメイン（一般的に財団公式ではないもの）
BLOCKED_DOMAINS = {
    "wikipedia.org", "ja.wikipedia.org", "en.wikipedia.org",
    "facebook.com", "twitter.com", "x.com", "linkedin.com",
    "instagram.com", "youtube.com", "note.com", "ameblo.jp",
    "hatena.ne.jp", "blogspot.com",
    "koeki-info.go.jp",
    "jyosei-navi.jfc.or.jp",
    "fields.canpan.info",
    "canpan.info",
    "nta.go.jp",
    "hojin-bangou.nta.go.jp",
    "houjin-bangou.nta.go.jp",
    "nikkei.com", "asahi.com", "yomiuri.co.jp", "mainichi.jp",
    "amazon.co.jp", "rakuten.co.jp",
    "google.com", "google.co.jp", "yahoo.co.jp", "yahoo.com",
    "prtimes.jp", "atpress.ne.jp",
    "duckduckgo.com",
    "scholar.google.com",
}

# 財団名から法人格プレフィックスを除去するためのパターン
LEGAL_PREFIXES = [
    "公益財団法人", "一般財団法人", "公益社団法人", "一般社団法人",
    "特定非営利活動法人", "認定特定非営利活動法人",
    "ＮＰＯ法人", "NPO法人",
    "株式会社", "有限会社",
    "公益財団", "一般財団", "公益社団", "一般社団",
]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_name(name: str) -> str:
    """財団名から法人格プレフィックスを取り除き、検索用に正規化"""
    n = name.strip()
    # 全角→半角の簡易処理
    n = n.replace("　", " ")
    for p in LEGAL_PREFIXES:
        if n.startswith(p):
            n = n[len(p):].strip()
            break
    # 末尾の「（）」内コメントを除去
    n = re.sub(r"[（(].*?[)）]\s*$", "", n).strip()
    return n


def core_keywords(name: str) -> list[str]:
    """財団名照合用キーワード抽出"""
    base = normalize_name(name)
    kws = {base, name.strip()}
    # 「○○財団」「○○記念財団」「○○振興財団」等の主要要素
    m = re.match(r"^(.+?)(財団|奨学会|記念会|振興会|事業団|基金|協会|会|学会|研究所|研究会|機構|センター)$", base)
    if m:
        kws.add(m.group(1))
    return [k for k in kws if k and len(k) >= 2]


def get_metadata(row_meta: str | None) -> dict:
    if not row_meta:
        return {}
    try:
        return json.loads(row_meta)
    except Exception:
        return {}


def domain_score(url: str) -> int:
    """ドメインの信頼度スコア（高いほど公式の可能性が高い）"""
    host = (urlparse(url).hostname or "").lower()
    score = 0
    if host.endswith(".or.jp"):
        score += 5
    elif host.endswith(".go.jp"):
        score += 4
    elif host.endswith(".ac.jp"):
        score += 3
    elif host.endswith(".jp"):
        score += 2
    elif host.endswith(".org"):
        score += 2
    elif host.endswith(".com") or host.endswith(".net"):
        score += 1
    # サブパスが浅い（=トップページ寄り）と加点
    path = urlparse(url).path or "/"
    if path in ("", "/"):
        score += 1
    return score


def is_blocked(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    if not host:
        return True
    for bad in BLOCKED_DOMAINS:
        if host == bad or host.endswith("." + bad):
            return True
    return False


def to_root(url: str) -> str:
    p = urlparse(url)
    if not p.scheme or not p.hostname:
        return url
    return f"{p.scheme}://{p.hostname}"


def fetch(url: str, timeout: int = 12) -> requests.Response | None:
    try:
        r = requests.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept-Language": "ja,en;q=0.7"},
            timeout=timeout,
            allow_redirects=True,
        )
        return r
    except Exception:
        return None


def page_signals(url: str) -> tuple[str, str]:
    """トップページの (title, h1text) を取得"""
    r = fetch(url)
    if not r or r.status_code >= 400:
        return "", ""
    # encoding fallback
    if not r.encoding or r.encoding.lower() in ("iso-8859-1", "ascii"):
        r.encoding = r.apparent_encoding or "utf-8"
    try:
        soup = BeautifulSoup(r.text, "html.parser")
    except Exception:
        return "", ""
    title = (soup.title.string if soup.title and soup.title.string else "") or ""
    h1 = ""
    h1_tag = soup.find("h1")
    if h1_tag:
        h1 = h1_tag.get_text(" ", strip=True)
    return title.strip(), h1.strip()


def verify_match(url: str, name: str) -> tuple[bool, str]:
    """
    URLが財団公式と判定できるか検証
    - トップページのtitle/h1に「財団名のコアキーワード」が含まれる場合のみ確定
    - サブページや短すぎるキーワード（2文字以下）は不採用
    """
    if is_blocked(url):
        return False, "blocked_domain"
    kws = [k for k in core_keywords(name) if len(k) >= 3]
    if not kws:
        return False, "no_strong_keyword"
    title, h1 = page_signals(to_root(url))
    blob = f"{title} {h1}"
    if not blob.strip():
        return False, "no_top_signal"
    for kw in kws:
        if kw in blob:
            return True, f"title/h1 contains '{kw}'"
    return False, f"no_keyword_in_title (title={title[:40]})"


def ddg_search(query: str, max_results: int = 8) -> list[dict]:
    try:
        with DDGS() as ddgs:
            return list(ddgs.text(query, max_results=max_results, region="jp-jp"))
    except Exception as e:
        print(f"  [WARN] DDG search failed: {e}", file=sys.stderr)
        return []


def discover_for(name: str) -> tuple[str | None, str | None, str | None]:
    """
    Returns: (url, source, note)
    """
    base = normalize_name(name)
    queries = [
        (f'"{base}" 公式 site:or.jp', "ddg_orjp"),
        (f'"{base}" 公式', "ddg_official"),
        (f"{base} 助成", "ddg_grant"),
    ]
    seen_hosts: set[str] = set()
    candidates: list[tuple[int, str, str]] = []  # (score, url, source)

    for q, src in queries:
        results = ddg_search(q, max_results=8)
        time.sleep(5)  # rate limit per search (DDG returns 'No results' under load)
        for r in results:
            url = r.get("href") or r.get("url") or ""
            if not url or is_blocked(url):
                continue
            host = (urlparse(url).hostname or "").lower()
            if host in seen_hosts:
                continue
            seen_hosts.add(host)
            sc = domain_score(url)
            candidates.append((sc, url, src))
        if not results:
            continue

    # 高スコア順でソート
    candidates.sort(key=lambda x: -x[0])

    for sc, url, src in candidates[:6]:
        ok, note = verify_match(url, name)
        time.sleep(1.5)  # rate limit per HTTP fetch
        if ok:
            return to_root(url), src, note
    return None, None, "no_verified_candidate"


def run(limit: int, dry_run: bool = False):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id, name, metadata
        FROM organizations
        WHERE (url IS NULL OR url = '')
        ORDER BY name
        """
    )
    all_rows = cur.fetchall()
    print(f"[INFO] candidates with no URL: {len(all_rows)}", flush=True)

    pending = []
    for row in all_rows:
        meta = get_metadata(row["metadata"])
        if meta.get("url_attempted_at"):
            # 既に試行済み → スキップ（再試行したい場合は別フラグ）
            continue
        pending.append(row)

    print(f"[INFO] not yet attempted: {len(pending)}", flush=True)
    print(f"[INFO] processing first {limit} ...", flush=True)

    found = 0
    not_found = 0
    by_source: dict[str, int] = {}
    started = time.time()

    for i, row in enumerate(pending[:limit], 1):
        name = row["name"]
        org_id = row["id"]
        meta = get_metadata(row["metadata"])
        print(f"[{i}/{limit}] {name}", flush=True)

        try:
            url, source, note = discover_for(name)
        except Exception as e:
            url, source, note = None, None, f"error:{e}"

        meta["url_attempted_at"] = now_iso()
        if url:
            meta["url_source"] = source
            meta["url_discovered_at"] = now_iso()
            meta["url_verify_note"] = note
            found += 1
            by_source[source or "unknown"] = by_source.get(source or "unknown", 0) + 1
            print(f"    -> {url}  [src={source}, {note}]", flush=True)
            if not dry_run:
                cur.execute(
                    "UPDATE organizations SET url=?, metadata=?, updated_at=? WHERE id=?",
                    (url, json.dumps(meta, ensure_ascii=False), now_iso(), org_id),
                )
        else:
            meta["url_verify_note"] = note or "not_found"
            not_found += 1
            print(f"    -> NOT FOUND ({note})", flush=True)
            if not dry_run:
                cur.execute(
                    "UPDATE organizations SET metadata=?, updated_at=? WHERE id=?",
                    (json.dumps(meta, ensure_ascii=False), now_iso(), org_id),
                )

        # コミットは10件ごと（中断・再開を強くするため）
        if i % 10 == 0 and not dry_run:
            conn.commit()

    if not dry_run:
        conn.commit()
    elapsed = time.time() - started
    print("---")
    print(f"processed: {min(limit, len(pending))}")
    print(f"found:     {found}")
    print(f"not_found: {not_found}")
    rate = (found / max(1, found + not_found)) * 100
    print(f"success rate: {rate:.1f}%")
    print(f"sources: {by_source}")
    print(f"elapsed: {elapsed:.0f}s")
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Discover official URLs for foundations")
    ap.add_argument("--limit", type=int, default=100, help="batch size (default 100)")
    ap.add_argument("--dry-run", action="store_true", help="don't write to DB")
    args = ap.parse_args()
    run(limit=args.limit, dry_run=args.dry_run)
