#!/usr/bin/env python3
"""Import UMIN 研究費補助機関データベース into the CFG ``organizations`` table.

Source: https://center6.umin.ac.jp/cgi-open-bin/josei/select/index.cgi
        (618 institutions, listed via ``kikan_cd=NNNNNNNN`` per row)

Pipeline:
1. Fetch the index page (which already contains the full institution list);
   POST the year form to allow choosing a fiscal year. Cache as
   ``cache/umin/index_<year>.html``.
2. Extract every ``kikan_cd`` plus institution name in display order.
3. For each institution, fetch the detail page (``?kikan_cd=NNNNNNNN``) and
   parse the metadata table: 法人格, 機関名称, 機関名読み, 機関英名,
   郵便番号, 住所, ＴＥＬ, ＦＡＸ, 趣旨・経緯等, ＵＲＬ, Ｅ－ＭＡＩＬ,
   役員, 事務局責任者, plus the embedded grant block (対象内容/対象研究者
   etc.). Cache as ``cache/umin/<kikan_cd>.html``.
4. Match each institution against existing CFG ``organizations`` by
   ``normalize_name`` (same logic as ``import_koeki.py``); update missing
   fields when a match is found, otherwise insert a new ``foundation`` row
   tagged with ``metadata.source='umin'``.

Rate limit: 1 req / 3 sec. Robots.txt at center6.umin.ac.jp returns 401
(Basic Auth) so it is unfetchable; the institution detail pages themselves
declare ``<meta name="robots" content="index,follow">``, so we proceed with
the conservative 3-sec interval.
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
import uuid
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "corporate_research_grants.sqlite"
CACHE_DIR = ROOT / "cache" / "umin"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

BASE = "https://center6.umin.ac.jp/cgi-open-bin/josei/select/index.cgi"
USER_AGENT = (
    "MiratukuBot/1.0 (Foundation Grants Research; "
    "+contact:dialoguebar@gmail.com) python-requests"
)
INTERVAL_SEC = 3.0
TIMEOUT = (30, 60)

LOG = logging.getLogger("import_umin")

PREFECTURES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

_last_request_at = 0.0


# --------------------------------------------------------------------------- #
# Reused logic from import_koeki.py (kept inline so this script is self-contained)
# --------------------------------------------------------------------------- #
def normalize_name(name: str) -> str:
    """Drop legal-form prefixes / spaces / case for fuzzy matching."""
    if not name:
        return ""
    s = unicodedata.normalize("NFKC", name)
    s = re.sub(r"^(公益|一般|特定非営利活動|特例)?(財団法人|社団法人)?\s*", "", s)
    s = s.replace("　", "").replace(" ", "")
    return s.lower()


def detect_legal_form(name: str, raw_legal: str = "") -> Optional[str]:
    src = (raw_legal or "") + " " + (name or "")
    if "公益財団法人" in src:
        return "公益財団法人"
    if "公益社団法人" in src:
        return "公益社団法人"
    if "一般財団法人" in src:
        return "一般財団法人"
    if "一般社団法人" in src:
        return "一般社団法人"
    if "特定非営利活動法人" in src:
        return "特定非営利活動法人"
    if "株式会社" in src:
        return "株式会社"
    return "その他"


def detect_subtype(name: str, purpose: str, target: str = "") -> str:
    """Pick a foundation_subtype.

    UMIN is a medical/research-grant directory, so the prior pattern is
    biased toward medical/academic content. We keep ``import_koeki.py``'s
    ordering and only nudge medical phrasing toward academic when no
    stronger signal applies.
    """
    blob = " ".join([name or "", purpose or "", target or ""])
    if any(k in blob for k in ["国際", "世界", "アジア", "ユネスコ", "UNESCO", "海外"]):
        return "intl"
    if any(k in blob for k in ["大学", "学会", "学術振興会", "学院", "研究会"]):
        return "academic"
    if any(k in blob for k in ["医学", "医療", "看護", "薬学", "病院", "がん", "難病", "医科"]):
        # Medical / clinical foundations: classify as academic (research-oriented)
        return "academic"
    if any(k in blob for k in ["記念", "奨学", "篤志", "賞"]):
        return "individual"
    if any(k in blob for k in ["市民", "ボランティア", "市民基金"]):
        return "ngo"
    if any(k in blob for k in ["振興", "技術", "科学", "産業"]):
        return "corporate"
    return "other"


def detect_prefecture(address: str) -> Optional[str]:
    if not address:
        return None
    s = unicodedata.normalize("NFKC", address)
    for pref in PREFECTURES:
        if pref in s[:10]:
            return pref
    return None


# --------------------------------------------------------------------------- #
# HTTP layer with on-disk cache + rate limit
# --------------------------------------------------------------------------- #
def _respect_rate_limit() -> None:
    global _last_request_at
    now = time.monotonic()
    delta = now - _last_request_at
    if delta < INTERVAL_SEC:
        time.sleep(INTERVAL_SEC - delta)
    _last_request_at = time.monotonic()


def _http_get(url: str) -> str:
    _respect_rate_limit()
    LOG.info("GET %s", url)
    resp = requests.get(
        url,
        headers={"User-Agent": USER_AGENT, "Accept-Language": "ja,en;q=0.8"},
        timeout=TIMEOUT,
        allow_redirects=True,
    )
    resp.raise_for_status()
    resp.encoding = resp.encoding or "utf-8"
    return resp.text


def _http_post(url: str, data: dict) -> str:
    _respect_rate_limit()
    LOG.info("POST %s %s", url, data)
    resp = requests.post(
        url,
        data=data,
        headers={"User-Agent": USER_AGENT, "Accept-Language": "ja,en;q=0.8"},
        timeout=TIMEOUT,
        allow_redirects=True,
    )
    resp.raise_for_status()
    resp.encoding = resp.encoding or "utf-8"
    return resp.text


def fetch_index(year: int = 2026, use_cache: bool = True) -> str:
    """Fetch the institution-list page for a fiscal year (POST form submit)."""
    cache_path = CACHE_DIR / f"index_{year}.html"
    if use_cache and cache_path.exists() and cache_path.stat().st_size > 1000:
        return cache_path.read_text(encoding="utf-8")
    html = _http_post(BASE, {"nendo": str(year), "serv": "", ".submit": "表示"})
    cache_path.write_text(html, encoding="utf-8")
    return html


def fetch_detail(kikan_cd: str, use_cache: bool = True) -> str:
    cache_path = CACHE_DIR / f"{kikan_cd}.html"
    if use_cache and cache_path.exists() and cache_path.stat().st_size > 500:
        return cache_path.read_text(encoding="utf-8")
    url = f"{BASE}?kikan_cd={kikan_cd}"
    html = _http_get(url)
    cache_path.write_text(html, encoding="utf-8")
    return html


# --------------------------------------------------------------------------- #
# Parsers
# --------------------------------------------------------------------------- #
KIKAN_CD_RE = re.compile(r'kikan_cd=(\d{6,10})')


def parse_index(html: str) -> list[tuple[str, str]]:
    """Return list of (kikan_cd, name) preserving display order."""
    soup = BeautifulSoup(html, "html.parser")
    seen: dict[str, str] = {}
    for a in soup.select("a[href*='kikan_cd=']"):
        m = KIKAN_CD_RE.search(a.get("href") or "")
        if not m:
            continue
        cd = m.group(1)
        if cd in seen:
            continue
        seen[cd] = (a.get_text(strip=True) or "").strip()
    return list(seen.items())


def _td_text(tr) -> str:
    """Concatenate visible text of all <td> inside a <tr> with linebreaks."""
    cells = tr.find_all("td")
    if not cells:
        return ""
    parts = []
    for td in cells:
        # Replace <br> with newlines before extracting text.
        for br in td.find_all("br"):
            br.replace_with("\n")
        parts.append(td.get_text("\n").strip())
    text = "\n".join(p for p in parts if p)
    # Collapse runs of blank lines, trim each line.
    text = re.sub(r"[ \t　]+\n", "\n", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def _td_first_link(tr) -> Optional[str]:
    cells = tr.find_all("td")
    for td in cells:
        a = td.find("a", href=True)
        if a:
            return a["href"].strip()
    return None


# Map of <th> label -> canonical key. We normalize the label first
# (NFKC + strip + collapse whitespace) so half/full-width quirks are absorbed.
LABEL_MAP = {
    "機関ID": "kikan_id",
    "年度": "fiscal_year",
    "法人格": "legal_form_raw",
    "機関名称": "name",
    "機関名読み": "name_kana",
    "機関英名": "name_en",
    "郵便番号": "postal_code",
    "住所": "address",
    "TEL": "phone",
    "FAX": "fax",
    "趣旨・経緯等": "purpose",
    "URL": "url",
    "E-MAIL": "email",
    "役員": "officers",
    "事務局責任者": "secretariat",
    # Grant block
    "助成金名称": "grant_name",
    "助成区分": "grant_category",
    "対象内容": "target_content",
    "関連URL": "related_url",
    "対象研究者": "target_researcher",
    "募集時期": "application_period",
    "助成件数": "grant_count",
    "助成金額": "grant_amount",
    "助成期間": "grant_period",
}


def _norm_label(s: str) -> str:
    """Aggressive label normalization: NFKC, strip punctuation/spaces."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = s.replace(" ", "").replace("　", "").replace(":", "").replace("：", "")
    # Some labels appear as "ＴＥＬ" -> NFKC -> "TEL"; that's fine.
    return s


def parse_detail(html: str, kikan_cd: str) -> dict:
    """Extract institution metadata + first grant block from a detail page."""
    soup = BeautifulSoup(html, "html.parser")
    out: dict = {"kikan_cd": kikan_cd}
    # Two tables on page: the institution table, then a grant table.
    for tr in soup.select("tr"):
        th = tr.find("th")
        if not th:
            continue
        label = _norm_label(th.get_text(strip=True))
        key = LABEL_MAP.get(label)
        if not key:
            continue
        val = _td_text(tr)
        # For URL fields, prefer the href if present
        if key in ("url", "related_url"):
            href = _td_first_link(tr)
            if href:
                val = href
        # Don't clobber the institution-level fields with the grant table's
        # 関連URL etc. (different keys -> safe). But "URL" (key='url') only
        # appears in the institution table.
        if val:
            out[key] = val
    return out


# --------------------------------------------------------------------------- #
# DB integration
# --------------------------------------------------------------------------- #
def upsert_organization(
    conn: sqlite3.Connection,
    existing: dict[str, dict],
    rec: dict,
    counters: dict,
) -> None:
    name = (rec.get("name") or "").strip()
    if not name:
        counters["skip_no_name"] += 1
        return

    # Sanitize obvious noise
    address = (rec.get("address") or "").strip()
    phone = (rec.get("phone") or "").strip()
    fax = (rec.get("fax") or "").strip()
    email = (rec.get("email") or "").strip()
    url = (rec.get("url") or "").strip()
    purpose = (rec.get("purpose") or "").strip()
    postal = (rec.get("postal_code") or "").strip()
    name_en = (rec.get("name_en") or "").strip() or None
    legal_raw = (rec.get("legal_form_raw") or "").strip()
    target = (rec.get("target_content") or "") + " " + (rec.get("target_researcher") or "")

    legal_form = detect_legal_form(name, legal_raw)
    subtype = detect_subtype(name, purpose, target)
    prefecture = detect_prefecture(address)

    # Compose a useful contact_address that includes the postal code
    if postal and address and postal not in address:
        contact_address = f"〒{postal} {address}"
    else:
        contact_address = address or None

    metadata = {
        "source": "umin",
        "kikan_cd": rec.get("kikan_cd"),
        "kikan_id": rec.get("kikan_id"),
        "name_kana": rec.get("name_kana"),
        "fiscal_year": rec.get("fiscal_year"),
        "officers": rec.get("officers"),
        "secretariat": rec.get("secretariat"),
        "grant_sample": {
            "name": rec.get("grant_name"),
            "category": rec.get("grant_category"),
            "target_content": rec.get("target_content"),
            "target_researcher": rec.get("target_researcher"),
            "application_period": rec.get("application_period"),
            "grant_count": rec.get("grant_count"),
            "grant_amount": rec.get("grant_amount"),
            "grant_period": rec.get("grant_period"),
            "related_url": rec.get("related_url"),
        },
    }
    metadata = {k: v for k, v in metadata.items() if v}
    metadata["grant_sample"] = {
        k: v for k, v in (metadata.get("grant_sample") or {}).items() if v
    } or None
    metadata = {k: v for k, v in metadata.items() if v}

    norm = normalize_name(name)
    cur = conn.cursor()
    if norm in existing:
        ex = existing[norm]
        # Build a coalesce-style update so we only fill blanks.
        sets, params = [], []
        if url:
            sets.append("url = COALESCE(NULLIF(url,''), ?)"); params.append(url)
        if phone:
            sets.append("contact_phone = COALESCE(NULLIF(contact_phone,''), ?)"); params.append(phone)
        if email:
            sets.append("contact_email = COALESCE(NULLIF(contact_email,''), ?)"); params.append(email)
        if contact_address:
            sets.append("contact_address = COALESCE(NULLIF(contact_address,''), ?)"); params.append(contact_address)
        if prefecture:
            sets.append("prefecture = COALESCE(NULLIF(prefecture,''), ?)"); params.append(prefecture)
        if purpose:
            sets.append("description = COALESCE(NULLIF(description,''), ?)"); params.append(purpose)
        if legal_form:
            sets.append("legal_form = COALESCE(legal_form, ?)"); params.append(legal_form)
        if subtype:
            sets.append("foundation_subtype = COALESCE(foundation_subtype, ?)"); params.append(subtype)
        if name_en:
            sets.append("name_en = COALESCE(NULLIF(name_en,''), ?)"); params.append(name_en)

        # Merge metadata: keep prior keys, add UMIN keys without overwriting.
        existing_meta_raw = ex.get("metadata") or "{}"
        try:
            existing_meta = json.loads(existing_meta_raw) if existing_meta_raw else {}
        except json.JSONDecodeError:
            existing_meta = {}
        merged_meta = dict(existing_meta)
        umin_block = {k: v for k, v in metadata.items() if k != "source"}
        merged_meta.setdefault("umin", umin_block)
        # Track that UMIN saw this record, even if existing source is e.g. koeki
        sources = merged_meta.get("sources") or [existing_meta.get("source")] if existing_meta.get("source") else []
        sources = [s for s in sources if s]
        if "umin" not in sources:
            sources.append("umin")
        merged_meta["sources"] = sources
        sets.append("metadata = ?")
        params.append(json.dumps(merged_meta, ensure_ascii=False))

        if sets:
            sets.append("updated_at = datetime('now','localtime')")
            params.append(ex["id"])
            cur.execute(
                f"UPDATE organizations SET {', '.join(sets)} WHERE id=?", params
            )
            counters["updated"] += 1
        else:
            counters["skip_nochange"] += 1
        return

    # New record
    new_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO organizations (
            id, name, name_en, type, foundation_subtype, legal_form,
            prefecture, url, description,
            contact_phone, contact_email, contact_address,
            metadata, country_code, created_at, updated_at
        ) VALUES (?, ?, ?, 'foundation', ?, ?, ?, ?, ?, ?, ?, ?, ?, 'JP',
                  datetime('now','localtime'), datetime('now','localtime'))
        """,
        (
            new_id, name, name_en, subtype, legal_form, prefecture,
            url or None, purpose or None, phone or None, email or None,
            contact_address,
            json.dumps(metadata, ensure_ascii=False),
        ),
    )
    counters["new"] += 1
    existing[norm] = {"id": new_id, "metadata": json.dumps(metadata, ensure_ascii=False)}


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--year", type=int, default=2026, help="Fiscal year for the index page (default 2026)")
    p.add_argument("--limit", type=int, default=None, help="Limit institutions processed (for smoke testing)")
    p.add_argument("--no-cache", action="store_true", help="Force re-fetch (ignore cache)")
    p.add_argument("--dry-run", action="store_true", help="Parse only; do not write to DB")
    p.add_argument("--sample", action="store_true",
                   help="Skip network entirely; use sample data to validate the script logic")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.sample:
        return _run_sample(args)

    # 1. Index
    try:
        index_html = fetch_index(args.year, use_cache=not args.no_cache)
    except Exception as exc:
        LOG.error("Failed to fetch index page: %s", exc)
        LOG.error("Run with --sample to validate script logic without network.")
        return 2
    listings = parse_index(index_html)
    LOG.info("Parsed %d institutions from index page", len(listings))
    if args.limit:
        listings = listings[: args.limit]

    # 2. DB existing
    if not DB.exists():
        LOG.error("DB not found: %s", DB)
        return 2
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("SELECT id, name, metadata FROM organizations")
    existing: dict[str, dict] = {}
    for rid, name, meta in cur.fetchall():
        existing[normalize_name(name)] = {"id": rid, "metadata": meta or "{}"}
    LOG.info("Existing CFG organizations: %d", len(existing))

    # 3. Detail loop
    counters = {"new": 0, "updated": 0, "skip_nochange": 0, "skip_no_name": 0, "errors": 0}
    parsed_count = 0
    for i, (cd, name_hint) in enumerate(listings, 1):
        try:
            detail_html = fetch_detail(cd, use_cache=not args.no_cache)
        except Exception as exc:
            LOG.warning("[%d/%d] %s fetch failed: %s", i, len(listings), cd, exc)
            counters["errors"] += 1
            continue
        rec = parse_detail(detail_html, cd)
        if not rec.get("name"):
            rec["name"] = name_hint
        parsed_count += 1
        if args.dry_run:
            LOG.info("[%d/%d] %s %s", i, len(listings), cd, rec.get("name"))
            continue
        upsert_organization(conn, existing, rec, counters)
        if i % 25 == 0:
            conn.commit()
            LOG.info("[%d/%d] progress: %s", i, len(listings), counters)

    if not args.dry_run:
        conn.commit()
    cur.execute("SELECT COUNT(*) FROM organizations")
    total = cur.fetchone()[0]
    conn.close()

    print()
    print("=== UMIN import summary ===")
    print(f"Year                  : {args.year}")
    print(f"Index institutions    : {len(listings)}")
    print(f"Detail pages parsed   : {parsed_count}")
    print(f"New organizations     : {counters['new']}")
    print(f"Updated existing      : {counters['updated']}")
    print(f"Skipped (no diff)     : {counters['skip_nochange']}")
    print(f"Skipped (no name)     : {counters['skip_no_name']}")
    print(f"Errors                : {counters['errors']}")
    print(f"Total CFG orgs now    : {total}")
    return 0


# --------------------------------------------------------------------------- #
# Sample-data fallback (offline smoke test)
# --------------------------------------------------------------------------- #
SAMPLE_RECORDS = [
    {
        "kikan_cd": "00017770",
        "kikan_id": "00013380",
        "fiscal_year": "2026",
        "legal_form_raw": "公益財団法人",
        "name": "日本フィランソロピック財団",
        "name_kana": "にほんふぃらんそろぴっくざいだん",
        "postal_code": "105-0004",
        "address": "東京都港区新橋１丁目１−１３　アーバンネット内幸町ビル３階",
        "phone": "050-3521-0160",
        "purpose": "日本フィランソロピック財団は、寄附者のご寄附で財団内にさまざまな基金を設立します。",
        "url": "https://np-foundation.or.jp",
        "email": "info@np-foundation.or.jp",
        "officers": "代表理事:岸本　和久",
        "secretariat": "専務理事・事務局長:長谷川 攝",
        "grant_name": "第３回がん研究フロンティア基金",
        "grant_category": "研究助成",
        "target_content": "がんの新たな予防・診断・治療に資する基礎研究",
        "target_researcher": "国内の大学、研究機関または医療機関のいずれかに所属する若手研究者",
        "grant_amount": "総額１億円",
    },
    {
        "kikan_cd": "00099999",
        "kikan_id": "00099999",
        "fiscal_year": "2026",
        "legal_form_raw": "一般社団法人",
        "name": "サンプル医学研究振興会",
        "address": "京都府京都市中京区サンプル通り1-1",
        "phone": "075-000-0000",
        "purpose": "医学研究の振興と人材育成を目的とする",
        "url": "https://example.invalid/",
        "email": "info@example.invalid",
    },
]


def _run_sample(args) -> int:
    LOG.info("Running in --sample mode (no network).")
    if not DB.exists():
        LOG.error("DB not found: %s", DB)
        return 2
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("SELECT id, name, metadata FROM organizations")
    existing: dict[str, dict] = {}
    for rid, name, meta in cur.fetchall():
        existing[normalize_name(name)] = {"id": rid, "metadata": meta or "{}"}
    LOG.info("Existing CFG organizations: %d", len(existing))

    counters = {"new": 0, "updated": 0, "skip_nochange": 0, "skip_no_name": 0, "errors": 0}
    if args.dry_run:
        for rec in SAMPLE_RECORDS:
            LOG.info("dry: %s", rec["name"])
    else:
        for rec in SAMPLE_RECORDS:
            upsert_organization(conn, existing, rec, counters)
        conn.commit()
    cur.execute("SELECT COUNT(*) FROM organizations")
    total = cur.fetchone()[0]
    conn.close()
    print()
    print("=== UMIN sample-mode summary ===")
    print(f"Sample records        : {len(SAMPLE_RECORDS)}")
    print(f"New organizations     : {counters['new']}")
    print(f"Updated existing      : {counters['updated']}")
    print(f"Skipped (no diff)     : {counters['skip_nochange']}")
    print(f"Total CFG orgs now    : {total}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
