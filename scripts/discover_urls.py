#!/usr/bin/env python3
"""
discover_urls.py — 研究助成財団DB公式URL自動探索 (Phase 7 強化版)

対象: organizations WHERE url IS NULL OR url = ''
手法（優先順）:
  0) 推定URL直接試行 (heuristic guess)
     - 財団名のローマ字/英語成分から `<core>-foundation.or.jp`, `<core>.or.jp`,
       `<core>foundation.or.jp`, `<core>.jp`, `<core>.org` などを直接HTTPで試す
     - title/h1にキーワードが含まれていれば確定
  1) DuckDuckGo HTML検索（API不要、レート制限緩い）
     - クエリA: "<財団名> 公式 site:.or.jp"
     - クエリB: "<財団名> 公式"
     - クエリC: "<財団名> 助成"
  2) 検証: ドメインが .or.jp/.jp/.org、トップページのtitle/h1に財団名一致
  3) UPDATE organizations.url, metadata.url_source, metadata.url_discovered_at

バッチ処理: --limit で件数制御、metadata.url_attempted_at により再開可能
レート制限: 2req/3sec、検索エンジンキューと検証用HTTPキューを分離
DB競合回避: PRAGMA busy_timeout=300000 + retry logic
"""
from __future__ import annotations

import argparse
import json
import random
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

HTTP_TIMEOUT = 8  # seconds
HTTP_MAX_REDIRECTS = 5
RATE_SLEEP = 0.7  # ~3req/2sec

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

# 財団名末尾の語尾パターン（推定URLのコア抽出用）
SUFFIX_PATTERN = re.compile(
    r"(財団|奨学会|記念会|振興会|事業団|基金|協会|学会|研究所|研究会|機構|センター|"
    r"ファウンデーション|ファンデーション|フェローシップ|ミュージアム|"
    r"Foundation|FOUNDATION|foundation)$"
)

# よく使われる「記念」「育英」等の修飾語（除いてドメインのコア部を作る）
DECORATOR_WORDS = ["記念", "育英", "学術", "振興", "国際", "公益", "総合", "技術", "科学", "教育", "研究"]

# カナ→英字の簡易対応（よく登場する企業由来の財団名）
KANA_TO_LATIN = {
    "ロームミュージックファンデーション": "rohmmusicfdn",
    "ローム": "rohm",
    "トヨタ": "toyota",
    "トヨタ自動車": "toyota",
    "ホンダ": "honda",
    "ニッサン": "nissan",
    "ソニー": "sony",
    "パナソニック": "panasonic",
    "三菱": "mitsubishi",
    "三菱重工": "mhi",
    "三井": "mitsui",
    "住友": "sumitomo",
    "サントリー": "suntory",
    "キヤノン": "canon",
    "キャノン": "canon",
    "リコー": "ricoh",
    "富士通": "fujitsu",
    "富士フイルム": "fujifilm",
    "東芝": "toshiba",
    "日立": "hitachi",
    "村田": "murata",
    "京セラ": "kyocera",
    "オムロン": "omron",
    "デンソー": "denso",
    "セイコー": "seiko",
    "シャープ": "sharp",
    "コニカミノルタ": "konicaminolta",
    "オリンパス": "olympus",
    "ニコン": "nikon",
    "アサヒ": "asahi",
    "キリン": "kirin",
    "サッポロ": "sapporo",
    "明治": "meiji",
    "森永": "morinaga",
    "ヤクルト": "yakult",
    "資生堂": "shiseido",
    "花王": "kao",
    "ライオン": "lion",
    "セコム": "secom",
    "リクルート": "recruit",
    "ベネッセ": "benesse",
    "野村": "nomura",
    "大和": "daiwa",
    "みずほ": "mizuho",
    "ＳＯＭＰＯ": "sompo",
    "SOMPO": "sompo",
    "ＳＧＨ": "sgh",
    "SGH": "sgh",
    "ロッテ": "lotte",
    "PwC": "pwc",
    "ＰｗＣ": "pwc",
    "りそな": "resona",
    "稲盛": "inamori",
    "稲盛財団": "inamori",
    "日本生命": "nihonseimei",
    "鉄道弘済会": "kousaikai",
    "日本財団": "nipponfoundation",
    "笹川": "sasakawa",
    "東洋アルミ": "toyoaluminium",
    "牧": "boku",
    "齋藤": "saito",
    "斎藤": "saito",
    "萩原": "hagiwara",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_name(name: str) -> str:
    """財団名から法人格プレフィックスを取り除き、検索用に正規化"""
    n = name.strip()
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
    m = re.match(r"^(.+?)(財団|奨学会|記念会|振興会|事業団|基金|協会|会|学会|研究所|研究会|機構|センター)$", base)
    if m:
        kws.add(m.group(1))
    return [k for k in kws if k and len(k) >= 2]


def latin_core(name: str) -> list[str]:
    """財団名から推定ドメイン用のローマ字コア候補を生成"""
    base = normalize_name(name)
    base_no_suffix = SUFFIX_PATTERN.sub("", base).strip()

    candidates: list[str] = []

    # 既知マッピング（前方一致優先）
    for jp, en in sorted(KANA_TO_LATIN.items(), key=lambda x: -len(x[0])):
        if jp in name or jp in base or jp in base_no_suffix:
            candidates.append(en)
            break

    # アルファベット・数字を直接抽出
    ascii_part = re.sub(r"[^A-Za-z0-9]", "", base_no_suffix)
    if ascii_part and len(ascii_part) >= 3:
        candidates.append(ascii_part.lower())

    # 全角英数→半角に変換して再抽出
    def zen_to_han(s: str) -> str:
        return s.translate(str.maketrans(
            "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ０１２３４５６７８９",
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
        ))
    han = zen_to_han(base_no_suffix)
    han_ascii = re.sub(r"[^A-Za-z0-9]", "", han)
    if han_ascii and len(han_ascii) >= 3 and han_ascii.lower() not in [c.lower() for c in candidates]:
        candidates.append(han_ascii.lower())

    # 重複排除しつつ順序保持
    seen: set[str] = set()
    out: list[str] = []
    for c in candidates:
        c = c.strip().lower()
        if c and c not in seen and 3 <= len(c) <= 30:
            seen.add(c)
            out.append(c)
    return out


def guess_urls(name: str) -> list[str]:
    """財団名から推定URLを生成"""
    cores = latin_core(name)
    urls: list[str] = []
    for core in cores:
        for tmpl in (
            "https://www.{c}-foundation.or.jp",
            "https://{c}-foundation.or.jp",
            "https://www.{c}foundation.or.jp",
            "https://{c}foundation.or.jp",
            "https://www.{c}.or.jp",
            "https://{c}.or.jp",
            "https://www.{c}foundation.org",
            "https://www.{c}-foundation.org",
            "https://www.{c}.jp",
            "https://{c}.jp",
        ):
            urls.append(tmpl.format(c=core))
    # 重複排除
    return list(dict.fromkeys(urls))


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


_session = requests.Session()
_session.max_redirects = HTTP_MAX_REDIRECTS


def fetch(url: str, timeout: int = HTTP_TIMEOUT) -> requests.Response | None:
    try:
        r = _session.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept-Language": "ja,en;q=0.7"},
            timeout=timeout,
            allow_redirects=True,
        )
        return r
    except Exception:
        return None


def page_signals(url: str) -> tuple[str, str, int]:
    """トップページの (title, h1text, status_code) を取得"""
    r = fetch(url)
    if not r:
        return "", "", 0
    if r.status_code >= 400:
        return "", "", r.status_code
    if not r.encoding or r.encoding.lower() in ("iso-8859-1", "ascii"):
        r.encoding = r.apparent_encoding or "utf-8"
    try:
        soup = BeautifulSoup(r.text, "html.parser")
    except Exception:
        return "", "", r.status_code
    title = (soup.title.string if soup.title and soup.title.string else "") or ""
    h1 = ""
    h1_tag = soup.find("h1")
    if h1_tag:
        h1 = h1_tag.get_text(" ", strip=True)
    return title.strip(), h1.strip(), r.status_code


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
    title, h1, status = page_signals(to_root(url))
    if status == 0:
        return False, "no_response"
    if status >= 400:
        return False, f"http_{status}"
    blob = f"{title} {h1}"
    if not blob.strip():
        return False, "no_top_signal"
    for kw in kws:
        if kw in blob:
            return True, f"title/h1 contains '{kw}'"
    return False, f"no_keyword_in_title (title={title[:40]})"


def head_alive(url: str, timeout: int = 5) -> bool:
    """HEADで素早く到達可能か判定。HEADが拒否されたらGETにフォールバック"""
    try:
        r = _session.head(
            url,
            headers={"User-Agent": USER_AGENT, "Accept-Language": "ja,en;q=0.7"},
            timeout=timeout,
            allow_redirects=True,
        )
        if r.status_code < 400:
            return True
        # 405や403はGETで再試行
        if r.status_code in (403, 405, 501):
            r = _session.get(
                url,
                headers={"User-Agent": USER_AGENT, "Accept-Language": "ja,en;q=0.7"},
                timeout=timeout,
                allow_redirects=True,
                stream=True,
            )
            r.close()
            return r.status_code < 400
        return False
    except Exception:
        return False


def try_guessed_urls(name: str) -> tuple[str | None, str | None]:
    """推定URLを順に試して最初に検証成功したものを返す。
    高速化: まずHEADで存在確認、200系のみフル検証する。"""
    for url in guess_urls(name):
        time.sleep(0.4)  # 軽いレート制限
        if not head_alive(url, timeout=5):
            continue
        time.sleep(0.4)
        ok, note = verify_match(url, name)
        if ok:
            return to_root(url), f"guess: {note}"
    return None, None


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
    # Phase 0: heuristic URL guess (最速・コスト低)
    url, note = try_guessed_urls(name)
    if url:
        return url, "guess", note

    base = normalize_name(name)
    queries = [
        (f'"{base}" 公式 site:or.jp', "ddg_orjp"),
        (f'"{base}" 公式', "ddg_official"),
    ]
    seen_hosts: set[str] = set()
    candidates: list[tuple[int, str, str]] = []

    # 1問目で候補が拾えたら2問目はスキップ（時間節約）
    for q, src in queries:
        results = ddg_search(q, max_results=6)
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
        if candidates:
            break
        time.sleep(1.5)  # rate limit between searches

    candidates.sort(key=lambda x: -x[0])

    for sc, url, src in candidates[:3]:
        time.sleep(RATE_SLEEP)
        ok, note = verify_match(url, name)
        if ok:
            return to_root(url), src, note
    return None, None, "no_verified_candidate"


# --- DB write helpers (busy retry) -----------------------------------------

def db_execute_with_retry(conn: sqlite3.Connection, sql: str, params: tuple,
                           max_retries: int = 8) -> None:
    """Execute SQL with retry on database lock."""
    for attempt in range(max_retries):
        try:
            conn.execute(sql, params)
            return
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() or "busy" in str(e).lower():
                wait = min(30, 2 ** attempt) + random.uniform(0, 1)
                print(f"  [DB] locked, retrying in {wait:.1f}s (attempt {attempt+1}/{max_retries})", file=sys.stderr)
                time.sleep(wait)
                continue
            raise
    raise RuntimeError(f"DB write failed after {max_retries} retries")


def db_commit_with_retry(conn: sqlite3.Connection, max_retries: int = 8) -> None:
    for attempt in range(max_retries):
        try:
            conn.commit()
            return
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() or "busy" in str(e).lower():
                wait = min(30, 2 ** attempt) + random.uniform(0, 1)
                print(f"  [DB] commit locked, retrying in {wait:.1f}s ({attempt+1}/{max_retries})", file=sys.stderr)
                time.sleep(wait)
                continue
            raise
    raise RuntimeError("DB commit failed after retries")


def run(limit: int, dry_run: bool = False):
    # timeout=120秒で接続、busy_timeout=300000(=300秒) PRAGMAも併用
    conn = sqlite3.connect(DB_PATH, timeout=120.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 300000")
    conn.execute("PRAGMA journal_mode = WAL")
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id, name, metadata
        FROM organizations
        WHERE (url IS NULL OR url = '')
          AND legal_form IN ('公益財団法人','一般財団法人','公益社団法人','一般社団法人')
          AND name LIKE '%財団%'
        ORDER BY annual_grant_amount DESC NULLS LAST, name
        """
    )
    all_rows = cur.fetchall()
    print(f"[INFO] candidates with no URL: {len(all_rows)}", flush=True)

    pending = []
    for row in all_rows:
        meta = get_metadata(row["metadata"])
        if meta.get("url_attempted_at"):
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
        elapsed = time.time() - started
        rate_so_far = (found / max(1, i - 1) * 100) if i > 1 else 0
        print(f"[{i}/{limit}] ({elapsed:.0f}s, found={found}, rate={rate_so_far:.1f}%) {name}", flush=True)

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
                db_execute_with_retry(
                    conn,
                    "UPDATE organizations SET url=?, metadata=?, updated_at=? WHERE id=?",
                    (url, json.dumps(meta, ensure_ascii=False), now_iso(), org_id),
                )
        else:
            meta["url_verify_note"] = note or "not_found"
            not_found += 1
            print(f"    -> NOT FOUND ({note})", flush=True)
            if not dry_run:
                db_execute_with_retry(
                    conn,
                    "UPDATE organizations SET metadata=?, updated_at=? WHERE id=?",
                    (json.dumps(meta, ensure_ascii=False), now_iso(), org_id),
                )

        # コミットは10件ごと
        if i % 10 == 0 and not dry_run:
            db_commit_with_retry(conn)

    if not dry_run:
        db_commit_with_retry(conn)
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
