#!/usr/bin/env python3
"""
extract_wikipedia_urls.py — Wikipedia から財団公式URLを抽出する

対象: organizations WHERE url IS NULL OR url = ''
データソース:
  1. Wikipedia opensearch API
       https://ja.wikipedia.org/w/api.php?action=opensearch&search=<query>
       上位5候補から最も合致するページを選択
  2. Wikipedia REST summary API（存在検証 + ページタイトル正規化）
       https://ja.wikipedia.org/api/rest_v1/page/summary/<title>
  3. Wikipedia 本文 HTML（infobox + 外部リンクから公式URLを抽出）
       https://ja.wikipedia.org/wiki/<title>
       <a rel="mw:ExtLink"> 由来の外部リンクを優先採用

検証ルール:
  - 許可ドメイン: .or.jp / .ac.jp / .go.jp / .jp / .org
  - ブロックリスト: wikipedia/wikidata/blog/twitter/facebook/canpan/nta.go.jp 他
  - 同じドメインに複数財団を紐付けない（簡易ドメインスコア + 既存使用ドメインの除外）
  - ページのタイトル/抜粋に財団名のコアキーワードが含まれることを確認

レート制限: 1 req/sec（Wikipedia API friendly）
キャッシュ: cache/wikipedia/<sha1(title)>.json で本文HTMLとsummaryを保存
DB書き込み: PRAGMA busy_timeout=120000 + retry logic
"""
from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlparse

import requests
from bs4 import BeautifulSoup

DB_PATH = Path("/Users/nishimura+/projects/apps/corporate-foundation-grants/corporate_research_grants.sqlite")
CACHE_DIR = Path("/Users/nishimura+/projects/apps/corporate-foundation-grants/cache/wikipedia")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

USER_AGENT = (
    "MiratukuFoundationDB/1.0 "
    "(https://github.com/yuyanishimura0312; dialoguebar@gmail.com) "
    "python-requests"
)

WIKI_OPENSEARCH = "https://ja.wikipedia.org/w/api.php"
WIKI_REST_SUMMARY = "https://ja.wikipedia.org/api/rest_v1/page/summary/"
WIKI_PAGE = "https://ja.wikipedia.org/wiki/"

HTTP_TIMEOUT = 10
RATE_SLEEP = 1.0  # 1req/sec

# ---------- Domain rules ---------------------------------------------------

ALLOWED_TLDS = (".or.jp", ".ac.jp", ".go.jp", ".jp", ".org")

BLOCKED_DOMAINS = {
    "wikipedia.org", "ja.wikipedia.org", "en.wikipedia.org",
    "wikidata.org", "wikimedia.org", "commons.wikimedia.org",
    "wikisource.org",
    "facebook.com", "twitter.com", "x.com", "linkedin.com",
    "instagram.com", "youtube.com", "note.com", "ameblo.jp",
    "hatena.ne.jp", "blogspot.com", "blog.goo.ne.jp",
    "fields.canpan.info", "canpan.info",
    "nta.go.jp", "houjin-bangou.nta.go.jp", "hojin-bangou.nta.go.jp",
    "koeki-info.go.jp", "scj.go.jp",
    "geohack.toolforge.org", "toolforge.org",
    "isni.org", "viaf.org", "d-nb.info", "id.loc.gov",
    "catalogue.bnf.fr", "data.bnf.fr", "id.ndl.go.jp", "nla.gov.au",
    "ci.nii.ac.jp", "cir.nii.ac.jp", "idref.fr", "lux.collections.yale.edu",
    "aleph.nkp.cz", "creativecommons.org",
    "google.com", "google.co.jp", "yahoo.co.jp", "yahoo.com",
    "amazon.co.jp", "rakuten.co.jp",
    "duckduckgo.com",
    "prtimes.jp", "atpress.ne.jp",
    "scholar.google.com",
    "nikkei.com", "asahi.com", "yomiuri.co.jp", "mainichi.jp",
}

BLOCKED_KEYWORDS = ("blog", "wiki")  # ホスト名にこれが含まれていたら除外

LEGAL_PREFIXES = [
    "公益財団法人", "一般財団法人", "公益社団法人", "一般社団法人",
    "特定非営利活動法人", "認定特定非営利活動法人",
    "ＮＰＯ法人", "NPO法人",
    "公益財団", "一般財団", "公益社団", "一般社団",
]


# ---------- Utility --------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_name(name: str) -> str:
    n = (name or "").strip().replace("　", " ")
    for p in LEGAL_PREFIXES:
        if n.startswith(p):
            n = n[len(p):].strip()
            break
    n = re.sub(r"[（(].*?[)）]\s*$", "", n).strip()
    return n


def core_keywords(name: str) -> list[str]:
    base = normalize_name(name)
    kws = {base, name.strip()}
    m = re.match(
        r"^(.+?)(財団|奨学会|記念会|振興会|事業団|基金|協会|学会|研究所|研究会|機構|センター)$",
        base,
    )
    if m:
        kws.add(m.group(1))
    return [k for k in kws if k and len(k) >= 2]


def domain_of(url: str) -> str:
    return (urlparse(url).hostname or "").lower().lstrip(".")


def domain_root(url: str) -> str:
    """Return the domain stripped of leading 'www.' for de-duplication."""
    h = domain_of(url)
    if h.startswith("www."):
        h = h[4:]
    return h


def is_blocked(url: str) -> bool:
    host = domain_of(url)
    if not host:
        return True
    for bad in BLOCKED_DOMAINS:
        if host == bad or host.endswith("." + bad):
            return True
    for kw in BLOCKED_KEYWORDS:
        if kw in host:
            return True
    return False


def is_allowed_tld(url: str) -> bool:
    host = domain_of(url)
    return any(host.endswith(t) for t in ALLOWED_TLDS)


def domain_score(url: str) -> int:
    host = domain_of(url)
    score = 0
    if host.endswith(".or.jp"):
        score += 6
    elif host.endswith(".go.jp"):
        score += 4
    elif host.endswith(".ac.jp"):
        score += 4
    elif host.endswith(".jp"):
        score += 3
    elif host.endswith(".org"):
        score += 3
    path = urlparse(url).path or "/"
    if path in ("", "/"):
        score += 1
    return score


def to_root(url: str) -> str:
    p = urlparse(url)
    if not p.scheme or not p.hostname:
        return url
    return f"{p.scheme}://{p.hostname}"


def cache_path(title: str, kind: str) -> Path:
    h = hashlib.sha1(title.encode("utf-8")).hexdigest()[:16]
    safe = re.sub(r"[^A-Za-z0-9_-]", "_", title)[:60]
    return CACHE_DIR / f"{safe}_{h}.{kind}.json"


def load_cache(p: Path):
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_cache(p: Path, data) -> None:
    try:
        p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


# ---------- HTTP -----------------------------------------------------------

_session = requests.Session()
_session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "ja,en;q=0.7"})


def http_get(url: str, params: dict | None = None) -> requests.Response | None:
    try:
        r = _session.get(url, params=params, timeout=HTTP_TIMEOUT)
        return r
    except Exception:
        return None


# ---------- Wikipedia API --------------------------------------------------

def opensearch(query: str) -> list[str]:
    """Top 5 candidate page titles for the given query."""
    cp = cache_path(f"opensearch::{query}", "json")
    cached = load_cache(cp)
    if cached is not None:
        return cached
    time.sleep(RATE_SLEEP)
    r = http_get(
        WIKI_OPENSEARCH,
        params={"action": "opensearch", "search": query, "limit": 5, "format": "json"},
    )
    titles: list[str] = []
    if r and r.status_code == 200:
        try:
            data = r.json()
            if isinstance(data, list) and len(data) >= 2:
                titles = [t for t in data[1] if isinstance(t, str)]
        except Exception:
            pass
    save_cache(cp, titles)
    return titles


def fetch_summary(title: str) -> dict | None:
    cp = cache_path(f"summary::{title}", "json")
    cached = load_cache(cp)
    if cached is not None:
        return cached
    time.sleep(RATE_SLEEP)
    r = http_get(WIKI_REST_SUMMARY + quote(title, safe=""))
    if not r or r.status_code != 200:
        save_cache(cp, {})
        return {}
    try:
        data = r.json()
    except Exception:
        data = {}
    save_cache(cp, data)
    return data


def fetch_page_html(title: str) -> str:
    cp = cache_path(f"html::{title}", "json")
    cached = load_cache(cp)
    if isinstance(cached, dict) and "html" in cached:
        return cached["html"]
    time.sleep(RATE_SLEEP)
    r = http_get(WIKI_PAGE + quote(title, safe=""))
    html = r.text if r and r.status_code == 200 else ""
    save_cache(cp, {"html": html})
    return html


# ---------- Page parsing ---------------------------------------------------

INFOBOX_LABEL_PATTERN = re.compile(r"(公式サイト|公式ウェブサイト|公式ホームページ|ウェブサイト|外部リンク)")


def extract_external_links(html: str) -> list[str]:
    """Return external links from the article body, in document order."""
    if not html:
        return []
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return []
    body = soup.find("div", id="mw-content-text") or soup
    urls: list[str] = []
    seen: set[str] = set()

    # 1) Infobox の「公式サイト」「ウェブサイト」行を最優先
    for tbl in body.find_all("table", class_=re.compile(r"infobox")):
        for tr in tbl.find_all("tr"):
            th = tr.find("th")
            if not th:
                continue
            label = th.get_text(" ", strip=True)
            if INFOBOX_LABEL_PATTERN.search(label):
                for a in tr.find_all("a", href=True):
                    href = a["href"]
                    if href.startswith("http"):
                        if href not in seen:
                            seen.add(href)
                            urls.append(href)

    # 2) class に external を含む a タグ（rel="mw:ExtLink" を含む）
    for a in body.find_all("a", href=True):
        href = a["href"]
        if not href.startswith("http"):
            continue
        cls = a.get("class") or []
        rel = a.get("rel") or []
        rel_str = " ".join(rel) if isinstance(rel, list) else str(rel)
        if any("external" in c for c in cls) or "mw:ExtLink" in rel_str:
            if href not in seen:
                seen.add(href)
                urls.append(href)

    return urls


# ---------- Domain collision tracker ---------------------------------------

class DomainTracker:
    """Track which domains are already used so we don't assign the same domain
    to multiple foundations."""

    def __init__(self, conn: sqlite3.Connection):
        self.used: set[str] = set()
        cur = conn.cursor()
        cur.execute("SELECT url FROM organizations WHERE url IS NOT NULL AND url != ''")
        for (url,) in cur.fetchall():
            d = domain_root(url)
            if d:
                self.used.add(d)

    def is_taken(self, url: str) -> bool:
        return domain_root(url) in self.used

    def claim(self, url: str) -> None:
        d = domain_root(url)
        if d:
            self.used.add(d)


# ---------- Discovery ------------------------------------------------------

def select_best_title(name: str, candidates: list[str]) -> str | None:
    """Pick the candidate Wikipedia title most likely to refer to this foundation."""
    if not candidates:
        return None
    base = normalize_name(name)
    kws = core_keywords(name)
    # Prefer candidates that include 「財団」 or any core keyword
    scored: list[tuple[int, str]] = []
    for title in candidates:
        sc = 0
        if base and base in title:
            sc += 10
        if "財団" in title or "基金" in title or "奨学" in title:
            sc += 3
        for kw in kws:
            if len(kw) >= 3 and kw in title:
                sc += 2
        # Disambiguation pages typically end with 「(財団)」or「(曖昧さ回避)」
        if "曖昧さ回避" in title:
            sc -= 5
        scored.append((sc, title))
    scored.sort(key=lambda x: -x[0])
    if scored[0][0] <= 0:
        return None
    return scored[0][1]


def verify_summary_matches(summary: dict, name: str) -> bool:
    """Confirm the Wikipedia article describes the target foundation."""
    if not summary:
        return False
    if summary.get("type") in ("disambiguation",):
        return False
    title = summary.get("title", "")
    extract = summary.get("extract", "")
    blob = f"{title} {extract}"
    if not blob.strip():
        return False
    base = normalize_name(name)
    kws = core_keywords(name)
    # 強キーワード: 4文字以上で本文に含まれていれば良い
    for kw in kws:
        if len(kw) >= 4 and kw in blob:
            return True
    # 弱キーワード: タイトル+抜粋の両方に含まれている
    if base and base in title and base in extract:
        return True
    return False


def verify_html_matches(html: str, name: str) -> bool:
    """Backup verification: read HTML page directly and check for foundation
    name in the title or first paragraph. Used when REST summary returns a
    redirected (parent company) page."""
    if not html:
        return False
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return False
    title_tag = soup.find("title")
    title = title_tag.get_text(" ", strip=True) if title_tag else ""
    h1 = soup.find("h1", id="firstHeading")
    h1_text = h1.get_text(" ", strip=True) if h1 else ""
    body = soup.find("div", id="mw-content-text")
    paragraphs = []
    if body:
        for p in body.find_all("p", limit=3):
            paragraphs.append(p.get_text(" ", strip=True))
    blob = " ".join([title, h1_text] + paragraphs)
    if not blob.strip():
        return False
    kws = core_keywords(name)
    for kw in kws:
        if len(kw) >= 4 and kw in blob:
            return True
    base = normalize_name(name)
    if base and base in h1_text:
        return True
    return False


def pick_official_url(name: str, links: list[str], tracker: DomainTracker) -> tuple[str | None, str | None]:
    """Pick the best official-looking URL from a list of external links."""
    if not links:
        return None, None
    candidates: list[tuple[int, str]] = []
    for raw in links:
        if is_blocked(raw):
            continue
        if not is_allowed_tld(raw):
            continue
        root = to_root(raw)
        if tracker.is_taken(root):
            continue
        sc = domain_score(root)
        # 検索結果側の語感: ホスト名に foundation/zaidan/fdn が入るとボーナス
        host = domain_of(root)
        for hint in ("foundation", "zaidan", "ザイダン", "fdn", "kikin", "kyokai"):
            if hint in host:
                sc += 2
                break
        candidates.append((sc, root))
    if not candidates:
        return None, None
    candidates.sort(key=lambda x: -x[0])
    best_score, best_url = candidates[0]
    return best_url, f"score={best_score}"


def discover_for(name: str, tracker: DomainTracker) -> tuple[str | None, str | None]:
    """Return (url, note) using Wikipedia."""
    base = normalize_name(name)
    if not base:
        return None, "empty_name"

    queries: list[str] = []
    # 法人格プレフィックスを除いた base から検索（記事タイトルは法人格抜きが多い）
    queries.append(base)
    if name.strip() != base:
        queries.append(name.strip())
    # 「財団」を含まない場合は付与した検索も試す
    if "財団" not in base and "基金" not in base:
        queries.append(base + " 財団")

    chosen_title: str | None = None
    verification_note = ""
    for q in queries:
        candidates = opensearch(q)
        title = select_best_title(name, candidates)
        if not title:
            continue
        # Step A: REST summary 検証（リダイレクトで親会社になることがある）
        summary = fetch_summary(title) or {}
        if verify_summary_matches(summary, name):
            chosen_title = title
            verification_note = "summary_match"
            break
        # Step B: HTMLを直接フェッチして検証（summary失敗時のフォールバック）
        html = fetch_page_html(title)
        if verify_html_matches(html, name):
            chosen_title = title
            verification_note = "html_match"
            break

    if not chosen_title:
        return None, "no_wikipedia_match"

    html = fetch_page_html(chosen_title)
    links = extract_external_links(html)
    if not links:
        return None, f"no_external_links (title={chosen_title})"
    url, note = pick_official_url(name, links, tracker)
    if not url:
        return None, f"no_qualifying_link (title={chosen_title})"
    return url, f"wiki:{chosen_title} | {verification_note} | {note}"


# ---------- DB helpers -----------------------------------------------------

def db_execute_with_retry(conn: sqlite3.Connection, sql: str, params: tuple,
                          max_retries: int = 8) -> None:
    for attempt in range(max_retries):
        try:
            conn.execute(sql, params)
            return
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() or "busy" in str(e).lower():
                wait = min(30, 2 ** attempt) + random.uniform(0, 1)
                print(f"  [DB] locked, retry in {wait:.1f}s ({attempt + 1}/{max_retries})", file=sys.stderr)
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
                print(f"  [DB] commit locked, retry in {wait:.1f}s ({attempt + 1}/{max_retries})", file=sys.stderr)
                time.sleep(wait)
                continue
            raise
    raise RuntimeError("DB commit failed after retries")


def get_metadata(row_meta) -> dict:
    if not row_meta:
        return {}
    try:
        return json.loads(row_meta)
    except Exception:
        return {}


# ---------- Main -----------------------------------------------------------

def run(limit: int, dry_run: bool = False, skip_attempted: bool = True):
    conn = sqlite3.connect(DB_PATH, timeout=120.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 120000")
    conn.execute("PRAGMA journal_mode = WAL")
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id, name, metadata
        FROM organizations
        WHERE (url IS NULL OR url = '')
        ORDER BY annual_grant_amount DESC NULLS LAST, name
        """
    )
    all_rows = cur.fetchall()
    print(f"[INFO] candidates with no URL: {len(all_rows)}", flush=True)

    pending: list[sqlite3.Row] = []
    for row in all_rows:
        meta = get_metadata(row["metadata"])
        if skip_attempted and meta.get("wikipedia_attempted_at"):
            continue
        pending.append(row)

    print(f"[INFO] not yet attempted (wikipedia): {len(pending)}", flush=True)
    target = pending[:limit]
    print(f"[INFO] processing first {len(target)} ...", flush=True)

    tracker = DomainTracker(conn)
    found = 0
    not_found = 0
    by_reason: dict[str, int] = {}
    started = time.time()

    for i, row in enumerate(target, 1):
        name = row["name"]
        org_id = row["id"]
        meta = get_metadata(row["metadata"])
        elapsed = time.time() - started
        rate = (found / max(1, i - 1) * 100) if i > 1 else 0
        print(f"[{i}/{len(target)}] ({elapsed:.0f}s, found={found}, rate={rate:.1f}%) {name}", flush=True)

        try:
            url, note = discover_for(name, tracker)
        except Exception as e:
            url, note = None, f"error:{e}"

        meta["wikipedia_attempted_at"] = now_iso()
        if url:
            meta["url_source"] = "wikipedia"
            meta["url_discovered_at"] = now_iso()
            meta["url_verify_note"] = note
            tracker.claim(url)
            found += 1
            print(f"    -> {url}  [{note}]", flush=True)
            if not dry_run:
                db_execute_with_retry(
                    conn,
                    "UPDATE organizations SET url=?, metadata=?, updated_at=? WHERE id=?",
                    (url, json.dumps(meta, ensure_ascii=False), now_iso(), org_id),
                )
        else:
            meta["wikipedia_verify_note"] = note or "not_found"
            reason = (note or "unknown").split(" (")[0]
            by_reason[reason] = by_reason.get(reason, 0) + 1
            not_found += 1
            print(f"    -> NOT FOUND ({note})", flush=True)
            if not dry_run:
                db_execute_with_retry(
                    conn,
                    "UPDATE organizations SET metadata=?, updated_at=? WHERE id=?",
                    (json.dumps(meta, ensure_ascii=False), now_iso(), org_id),
                )

        if i % 10 == 0 and not dry_run:
            db_commit_with_retry(conn)

    if not dry_run:
        db_commit_with_retry(conn)

    elapsed = time.time() - started
    print("---")
    print(f"processed: {len(target)}")
    print(f"found:     {found}")
    print(f"not_found: {not_found}")
    rate = (found / max(1, found + not_found)) * 100
    print(f"success rate: {rate:.1f}%")
    print(f"failure reasons: {by_reason}")
    print(f"elapsed: {elapsed:.0f}s")
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Extract official URLs via Wikipedia")
    ap.add_argument("--limit", type=int, default=200, help="batch size (default 200)")
    ap.add_argument("--dry-run", action="store_true", help="don't write to DB")
    ap.add_argument("--retry-attempted", action="store_true",
                    help="re-process orgs already attempted")
    args = ap.parse_args()
    run(limit=args.limit, dry_run=args.dry_run, skip_attempted=not args.retry_attempted)
