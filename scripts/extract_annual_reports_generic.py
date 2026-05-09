#!/usr/bin/env python3
"""Generic annual-report PDF crawler for koeki-info-derived foundations.

Strategy
--------
1. Pull foundations from organizations that have annual_grant_amount IS NULL
   and a homepage URL (or one of the metadata-derived URL hints).
2. For each foundation:
   a) Visit the homepage and look for typical disclosure-page links
      (情報公開, ディスクロージャー, 財務, 事業報告, 公表事項, IR).
   b) On each candidate page, scrape *.pdf links whose names look like an
      annual report (jigyohokoku, kessan, houkoku, FinancialStatement, etc).
   c) Download each candidate PDF (with on-disk cache) and parse via the
      existing extract_annual_reports.parse_pdf function.
3. Persist the latest detected annual_grant_amount + total_assets and a
   5-year history JSON back into the organizations row.
4. Rate limit 1 req / 3 sec, robots.txt-tolerant (respects 4xx/5xx by
   skipping); write to DB in batches with busy_timeout=300000ms.

This script is idempotent: re-running uses cached PDFs and only updates
columns when a new value is detected.

Usage
-----
    python3 scripts/extract_annual_reports_generic.py --limit 200
    python3 scripts/extract_annual_reports_generic.py --offset 200 --limit 200
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Iterable
from urllib.parse import urljoin, urlparse

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
from extract_annual_reports import parse_pdf  # type: ignore  # noqa: E402

DB_PATH = ROOT / "corporate_research_grants.sqlite"
CACHE_DIR = ROOT / "cache" / "annual_reports_generic"
DATA_DIR = ROOT / "data"
OUTPUT_FILE = DATA_DIR / "annual_grant_extraction_results_generic.json"
LOG_FILE = ROOT / "logs" / "extract_generic.log"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36 "
    "(research-bot; +contact: dialoguebar@gmail.com)"
)
RATE_LIMIT_SEC = 3.0
HTTP_TIMEOUT = 45

# Disclosure-page hints — the link text or URL substrings that typically lead
# to annual-report PDFs. We expand outward from the homepage one hop deep.
DISCLOSURE_HINTS = (
    "disclosure", "zaimu", "zaim", "finance", "financial",
    "about/data", "about/report", "ir/", "/ir.html",
    "houkoku", "kaiji", "kouhyou", "kouhyo", "report", "houji",
    "jigyou", "jigyo", "info/", "joho",
)
DISCLOSURE_TEXT_HINTS = (
    "情報公開", "ディスクロージャー", "財務", "事業報告", "決算",
    "公表事項", "公表", "公開資料", "IR", "情報開示",
    "貸借対照表", "正味財産", "資産", "情報",
)
PDF_HREF_RE = re.compile(
    r"href\s*=\s*[\"']([^\"']+\.pdf[^\"']*)[\"']", re.IGNORECASE
)
GENERIC_LINK_RE = re.compile(
    r"<a[^>]*href\s*=\s*[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
    re.IGNORECASE | re.DOTALL,
)
ANNUAL_KEYWORDS = (
    "jigyohokoku", "jigyou", "report", "houkoku", "annual",
    "FinancialStatement", "kessan", "decisionofaccount",
    "balance", "networth", "business", "shomi", "taishakutaisho",
)
YEAR_RE = re.compile(r"(20\d{2}|令和\s*\d+|R\s*\d+)")
REIWA_TO_YEAR = {1: 2019, 2: 2020, 3: 2021, 4: 2022, 5: 2023, 6: 2024, 7: 2025}

TARGET_YEARS = [2024, 2023, 2022, 2021, 2020]

# ---------------------------------------------------------------------------
_session = requests.Session()
_session.headers.update({"User-Agent": USER_AGENT})
_last_request_at = 0.0


def http_get(
    url: str, expect_pdf: bool = False, max_retries: int = 2,
) -> Optional[bytes]:
    """Rate-limited GET. Returns bytes or None on any failure."""
    global _last_request_at
    for attempt in range(max_retries + 1):
        wait = RATE_LIMIT_SEC - (time.monotonic() - _last_request_at)
        if wait > 0:
            time.sleep(wait)
        try:
            r = _session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
            _last_request_at = time.monotonic()
            if r.status_code != 200:
                return None
            if expect_pdf and not r.content[:5].startswith(b"%PDF"):
                return None
            return r.content
        except requests.RequestException:
            _last_request_at = time.monotonic()
            if attempt >= max_retries:
                return None
            time.sleep(2 + attempt)
    return None


def fetch_html(url: str) -> str:
    b = http_get(url)
    if not b:
        return ""
    try:
        # Best-effort decode (sjis→cp932→utf-8 fallbacks)
        for enc in ("utf-8", "cp932", "shift_jis", "euc_jp"):
            try:
                return b.decode(enc)
            except UnicodeDecodeError:
                continue
        return b.decode("utf-8", errors="replace")
    except Exception:
        return ""


def _name_keywords(name: str) -> list[str]:
    """Extract distinctive keyword(s) from a foundation name for matching.

    Returns several short candidates to match against homepage HTML.
    """
    cleaned = re.sub(
        r"(公益財団法人|一般財団法人|公益社団法人|一般社団法人|財団法人|社団法人|"
        r"特定非営利活動法人)",
        "",
        name,
    ).strip()
    cleaned = cleaned.replace("　", "").replace(" ", "")
    if not cleaned:
        return []
    # Strip generic suffixes for better keyword extraction
    core = re.sub(
        r"(科学振興財団|科学振興会|学術振興財団|学術振興会|奨学財団|奨学会|"
        r"教育財団|教育振興財団|文化財団|記念財団|記念会|振興会|振興財団|"
        r"財団|社団|協会|機構|基金)$",
        "",
        cleaned,
    ).strip()
    cands: list[str] = []
    if core and len(core) >= 2:
        cands.append(core[:8])
        if len(core) >= 4:
            cands.append(core[:4])
        cands.append(core[:2])
    if cleaned and cleaned != core:
        cands.append(cleaned[:8])
    # Dedup preserving order
    seen = set()
    out = []
    for c in cands:
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out


def homepage_matches(home_url: str, name: str) -> tuple[bool, str]:
    """Return (matches, html). Heuristic guard against URL pointing to a
    completely different organisation."""
    html = fetch_html(home_url)
    if not html:
        return False, ""
    keys = _name_keywords(name)
    if not keys:
        return True, html  # cannot evaluate — assume OK
    # Strip HTML tags for cheaper text search
    text = re.sub(r"<[^>]+>", " ", html)
    for k in keys:
        if k in text:
            return True, html
    return False, html


def discover_disclosure_pages(home_url: str, name: str) -> tuple[list[str], str]:
    """Return (candidate URLs, reason-if-skipped)."""
    matches, html = homepage_matches(home_url, name)
    if not matches:
        return [], "homepage-name-mismatch"
    if not html:
        return [], "fetch-failed"
    parsed = urlparse(home_url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    candidates: list[str] = [home_url]
    seen = {home_url}

    for m in GENERIC_LINK_RE.finditer(html):
        href, text = m.group(1), re.sub(r"<[^>]+>", "", m.group(2))
        text = text.strip()
        href_l = href.lower()
        if any(k in href_l for k in DISCLOSURE_HINTS) or any(
            k in text for k in DISCLOSURE_TEXT_HINTS
        ):
            absolute = urljoin(home_url, href)
            if absolute.startswith(origin) and absolute not in seen:
                seen.add(absolute)
                candidates.append(absolute)
                if len(candidates) >= 8:
                    break

    # Also try common conventional paths
    for tail in ("/disclosure/", "/disclosure.html", "/about/disclosure/",
                 "/about/disclosure.html", "/zaimu.html", "/zaimu/",
                 "/finance/", "/about/data/", "/about/data.html",
                 "/info/", "/ir/", "/joho.html", "/joho/"):
        guess = origin + tail
        if guess not in seen:
            seen.add(guess)
            candidates.append(guess)

    return candidates[:12], ""


def find_pdfs_on_page(page_url: str) -> dict[int, list[str]]:
    """Return {year:int -> [pdf_url]} found on a single page."""
    html = fetch_html(page_url)
    if not html:
        return {}
    found: dict[int, list[str]] = {}

    for href in PDF_HREF_RE.findall(html):
        absolute = urljoin(page_url, href)
        lower = absolute.lower()
        if not any(k.lower() in lower for k in ANNUAL_KEYWORDS):
            continue

        year: Optional[int] = None
        ym = re.search(r"20(\d{2})", absolute)
        if ym:
            year = 2000 + int(ym.group(1))
        if year is None:
            rm = re.search(r"[rR](\d{1,2})", absolute)
            if rm:
                year = REIWA_TO_YEAR.get(int(rm.group(1)))
        if year is None:
            continue
        if year not in TARGET_YEARS:
            continue
        found.setdefault(year, [])
        if absolute not in found[year]:
            found[year].append(absolute)

    return found


def cache_pdf(slug: str, key: str, url: str) -> Optional[Path]:
    sub = CACHE_DIR / slug
    sub.mkdir(parents=True, exist_ok=True)
    fname = sub / f"{key}.pdf"
    if fname.exists() and fname.stat().st_size > 1024:
        return fname
    payload = http_get(url, expect_pdf=True)
    if not payload:
        return None
    fname.write_bytes(payload)
    return fname


def safe_slug(org_id: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", org_id.lower())


# ---------------------------------------------------------------------------
def process_org(
    org_id: str,
    name: str,
    home_url: str,
    log,
) -> dict:
    print(f"\n[ {name} ]  ({org_id})  {home_url}")
    log.write(f"[ {name} ] {home_url}\n")

    if not home_url.startswith(("http://", "https://")):
        home_url = "https://" + home_url

    pages, skip_reason = discover_disclosure_pages(home_url, name)
    if skip_reason:
        print(f"  skipped: {skip_reason}")
        return {"id": org_id, "name": name, "skipped": skip_reason}
    print(f"  disclosure-page candidates: {len(pages)}")

    pdfs: dict[int, list[str]] = {}
    for page_url in pages:
        more = find_pdfs_on_page(page_url)
        for y, urls in more.items():
            pdfs.setdefault(y, [])
            for u in urls:
                if u not in pdfs[y]:
                    pdfs[y].append(u)
        if sum(len(v) for v in pdfs.values()) >= 12:
            break

    if not pdfs:
        print("  no PDFs discovered")
        return {"id": org_id, "name": name, "pdfs": 0, "extracted": False}

    print(f"  PDF candidates: {sorted(pdfs.keys(), reverse=True)}")

    history: list[dict] = []
    asset_latest: Optional[int] = None
    snippets: dict = {}
    needs_ocr = False
    slug = safe_slug(org_id)

    for year in sorted(pdfs.keys(), reverse=True):
        urls = pdfs[year]
        year_grant: Optional[int] = None
        year_assets: Optional[int] = None
        year_snip: dict = {}
        for idx, url in enumerate(urls):
            key = f"{year}" if idx == 0 else f"{year}_{idx}"
            try:
                path = cache_pdf(slug, key, url)
            except Exception as e:
                log.write(f"  cache_pdf error {url}: {e}\n")
                continue
            if not path:
                continue
            try:
                parsed = parse_pdf(path)
            except Exception as e:
                log.write(f"  parse_pdf error {path}: {e}\n")
                continue
            if not parsed.get("extractable"):
                if "image-based" in (parsed.get("error") or ""):
                    needs_ocr = True
                continue
            if year_grant is None and parsed["annual_grant_amount"]:
                year_grant = parsed["annual_grant_amount"]
            if year_assets is None and parsed["total_assets"]:
                year_assets = parsed["total_assets"]
            if not year_snip and parsed["snippets"]:
                year_snip = parsed["snippets"]

        if year_grant:
            history.append({"year": year, "amount": year_grant})
            print(f"    {year}: grant={year_grant:,}円  assets={year_assets or '-'}")
        else:
            print(f"    {year}: no grant amount detected ({len(urls)} pdf(s))")
        if asset_latest is None and year_assets:
            asset_latest = year_assets
        if not snippets:
            snippets = year_snip

    annual = history[0]["amount"] if history else None
    return {
        "id": org_id,
        "name": name,
        "home_url": home_url,
        "pdfs": sum(len(v) for v in pdfs.values()),
        "annual_grant_amount": annual,
        "total_assets": asset_latest,
        "history": history,
        "needs_ocr": needs_ocr,
        "snippets": snippets,
    }


def update_db(
    conn: sqlite3.Connection,
    org_id: str,
    annual: Optional[int],
    assets: Optional[int],
    history: list[dict],
    dry_run: bool,
) -> dict:
    cur = conn.execute(
        "SELECT annual_grant_amount, total_assets, "
        "annual_grant_amount_history, metadata "
        "FROM organizations WHERE id = ?",
        (org_id,),
    )
    row = cur.fetchone()
    if not row:
        return {"error": "not found"}
    cur_ann, cur_ass, cur_hist, cur_meta = row
    sets: list[str] = []
    params: list = []
    delta: dict = {}
    if annual and annual != cur_ann:
        sets.append("annual_grant_amount = ?")
        params.append(annual)
        delta["updated_annual"] = True
    if assets and assets != cur_ass:
        sets.append("total_assets = ?")
        params.append(assets)
        delta["updated_assets"] = True
    if history:
        hj = json.dumps(history, ensure_ascii=False)
        if hj != cur_hist:
            sets.append("annual_grant_amount_history = ?")
            params.append(hj)
            delta["updated_history"] = True
    if (annual or assets) and not dry_run:
        # Tag metadata source
        try:
            md = json.loads(cur_meta) if cur_meta else {}
        except Exception:
            md = {}
        md["financial_source"] = "koeki_financial_extraction"
        md["financial_extracted_at"] = (
            datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        )
        sets.append("metadata = ?")
        params.append(json.dumps(md, ensure_ascii=False))
    if sets and not dry_run:
        sets.append("updated_at = ?")
        params.append(datetime.now(timezone.utc).replace(tzinfo=None).isoformat())
        params.append(org_id)
        conn.execute(
            f"UPDATE organizations SET {', '.join(sets)} WHERE id = ?",
            params,
        )
    return delta


def select_targets(conn: sqlite3.Connection) -> list[tuple]:
    """Return list of (id, name, url) targets in priority order:
    1. id starts with 'koeki_' (公益法人info由来)  AND has URL
    2. legal_form is one of the four foundation/association forms
    3. annual_grant_amount IS NULL
    """
    sql = (
        "SELECT id, name, url, COALESCE(json_extract(metadata, '$.research_score'), 0) AS score "
        "FROM organizations "
        "WHERE annual_grant_amount IS NULL "
        "  AND legal_form IN ('公益財団法人','一般財団法人','公益社団法人','一般社団法人') "
        "  AND url IS NOT NULL AND url != '' "
        "ORDER BY "
        "  CASE WHEN id LIKE 'koeki_%' THEN 0 ELSE 1 END, "
        "  score DESC, "
        "  name "
    )
    return list(conn.execute(sql))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--commit-every", type=int, default=20)
    args = parser.parse_args()

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH, timeout=300.0)
    conn.execute("PRAGMA busy_timeout = 300000")

    rows = select_targets(conn)
    print(f"Total candidates: {len(rows)}")

    rows = rows[args.offset:]
    if args.limit:
        rows = rows[: args.limit]
    print(f"Processing batch: offset={args.offset} limit={args.limit} "
          f"(actual={len(rows)})")

    results: list[dict] = []
    if OUTPUT_FILE.exists():
        try:
            results = json.loads(OUTPUT_FILE.read_text())
        except Exception:
            results = []

    summary = {
        "processed": 0,
        "annual_extracted": 0,
        "assets_extracted": 0,
        "needs_ocr": 0,
        "no_pdfs": 0,
        "fetch_failed": 0,
        "skipped_name_mismatch": 0,
    }

    with LOG_FILE.open("a", encoding="utf-8") as log:
        log.write(f"\n=== run {datetime.now().isoformat()} "
                  f"offset={args.offset} limit={args.limit} ===\n")
        for i, (org_id, name, home_url, _score) in enumerate(rows):
            summary["processed"] += 1
            try:
                res = process_org(org_id, name, home_url, log)
            except Exception as e:
                tb = traceback.format_exc()
                log.write(f"  EXCEPTION {org_id}: {e}\n{tb}\n")
                print(f"  EXCEPTION {org_id}: {e}")
                results.append({"id": org_id, "name": name, "error": str(e)})
                continue

            if res.get("skipped"):
                summary["skipped_name_mismatch"] += 1
                results.append(res)
                continue
            if not res.get("pdfs"):
                summary["no_pdfs"] += 1
            if res.get("needs_ocr"):
                summary["needs_ocr"] += 1
            if res.get("annual_grant_amount"):
                summary["annual_extracted"] += 1
            if res.get("total_assets"):
                summary["assets_extracted"] += 1

            try:
                delta = update_db(
                    conn,
                    org_id,
                    res.get("annual_grant_amount"),
                    res.get("total_assets"),
                    res.get("history") or [],
                    args.dry_run,
                )
                res["delta"] = delta
            except Exception as e:
                log.write(f"  DB write error {org_id}: {e}\n")
                res["db_error"] = str(e)

            results.append(res)

            if (i + 1) % args.commit_every == 0:
                conn.commit()
                OUTPUT_FILE.write_text(
                    json.dumps(results, ensure_ascii=False, indent=2)
                )
                print(f"  -- checkpoint @ {i+1}: {summary}")

        conn.commit()

    OUTPUT_FILE.write_text(json.dumps(results, ensure_ascii=False, indent=2))
    print(f"\n=== Final Summary ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")
    print(f"  Output: {OUTPUT_FILE}")
    print(f"  Log:    {LOG_FILE}")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
