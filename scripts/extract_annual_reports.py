#!/usr/bin/env python3
"""Extract annual_grant_amount and total_assets from foundation annual report PDFs.

Strategy
--------
1. For each target foundation, use a curated registry of disclosure-page URLs
   (公益財団法人は財務情報の公開が義務付けられている).
2. Discover annual-report PDF links per fiscal year via simple regex on the HTML.
3. Download each PDF (with caching under cache/annual_reports/<slug>/<year>.pdf).
4. Parse text via pdfplumber. Extract:
     * annual_grant_amount  ← 助成事業費 / 公益目的事業費 / 研究助成 行の最大金額
     * total_assets         ← 資産合計 / 正味財産合計
5. Build annual_grant_amount_history (last 5 fiscal years) and write back to DB.
6. Rate limit 1 req / 3 sec.

The script is intentionally idempotent: re-running uses cached PDFs and only
updates DB columns when a value is missing or changed.

Usage
-----
    python3 scripts/extract_annual_reports.py            # all targets
    python3 scripts/extract_annual_reports.py --limit 5  # top 5 only
    python3 scripts/extract_annual_reports.py --dry-run  # don't write DB
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
from typing import Optional

import requests

try:
    import pdfplumber  # type: ignore
except ImportError:
    sys.stderr.write("ERROR: pdfplumber not installed. pip install pdfplumber\n")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "corporate_research_grants.sqlite"
CACHE_DIR = ROOT / "cache" / "annual_reports"
DATA_DIR = ROOT / "data"
OUTPUT_FILE = DATA_DIR / "annual_grant_extraction_results.json"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36 (research-bot; +contact: dialoguebar@gmail.com)"
)
RATE_LIMIT_SEC = 3.0  # 1 req / 3 sec
HTTP_TIMEOUT = 60

# ---------------------------------------------------------------------------
# Registry of target foundations
# ---------------------------------------------------------------------------
# Each entry has:
#   slug         : cache subdirectory name
#   db_name      : substring used to match organizations.name
#   disclosure   : disclosure HTML page (lists annual-report PDFs)
#   pdf_pattern  : optional explicit regex for annual-report PDF urls
#   pdf_template : optional URL template with {year} placeholder (zero scraping)
#   years        : fiscal years to cover (descending preference)
TARGETS: list[dict] = [
    {
        "slug": "takeda-sci",
        "db_name": "武田科学振興財団",
        "disclosure": "https://www.takeda-sci.or.jp/about/archive.php",
        "pdf_template": "https://www.takeda-sci.or.jp/about/doc/{year}jigyohokoku.pdf",
        "years": [2024, 2023, 2022, 2021, 2020],
        "note": "image-based PDFs (OCR required)",
    },
    {
        "slug": "mitsubishi-zaidan",
        "db_name": "三菱財団",
        "disclosure": "https://www.mitsubishi-zaidan.jp/about/financial.html",
        "pdf_templates": [
            "https://www.mitsubishi-zaidan.jp/about/data/{year}-business.pdf",
            "https://www.mitsubishi-zaidan.jp/about/data/{year}-networth.pdf",
            "https://www.mitsubishi-zaidan.jp/about/data/{year}-balance.pdf",
        ],
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "inamori",
        "db_name": "稲盛財団",
        "disclosure": "https://www.inamori-f.or.jp/about/reports",
        "pdf_template": "https://www.inamori-f.or.jp/wp-content/uploads/{year_plus_one}/06/FinancialStatement{year}.pdf",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "asahi-glass",
        "db_name": "旭硝子財団",
        "disclosure": "https://www.af-info.or.jp/about/disclosure.html",
        "pdf_template": "https://www.af-info.or.jp/about/assets/pdf/disclosure/report{year}-all.pdf",
        "years": [2024, 2023, 2022, 2021, 2020, 2019],
    },
    {
        "slug": "secom",
        "db_name": "セコム科学技術振興財団",
        "disclosure": "https://www.secomzaidan.jp/joho.html",
        # Reiwa-numbered subdirs: r06 = 令和6年度 = 2024年度
        "pdf_urls": {
            2024: ["https://www.secomzaidan.jp/joho/r06/joho/jigo_hokoku.pdf"],
            2023: ["https://www.secomzaidan.jp/joho/r05/joho/jigo_hokoku.pdf"],
            2022: ["https://www.secomzaidan.jp/joho/r04/joho/jigo_hokoku.pdf"],
            2021: ["https://www.secomzaidan.jp/joho/r03/joho/jigo_hokoku.pdf"],
            2020: ["https://www.secomzaidan.jp/joho/r02/joho/jigo_hokoku.pdf"],
        },
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "uehara",
        "db_name": "上原記念生命科学財団",
        "disclosure": "https://www.ueharazaidan.or.jp/about/disclosure.html",
        "pdf_templates": [
            "https://www.ueharazaidan.or.jp/include/img/past/{year}jigyouhoukoku.pdf",
            "https://www.ueharazaidan.or.jp/include/img/past/{year}shomizaisan.pdf",
            "https://www.ueharazaidan.or.jp/include/img/past/{year}taishakutaisho.pdf",
        ],
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "nakatani",
        "db_name": "中谷財団",  # full legal name 中谷医工計測技術振興財団, DB stores short form
        "disclosure": "https://www.nakatani-foundation.jp/about/public_notice/",
        "pdf_urls": {
            2024: [
                "https://storage.nakatani-foundation.jp/main/p/uploads/06jigyouhoukoku.pdf",
                "https://storage.nakatani-foundation.jp/main/p/uploads/06kessan.pdf",
            ],
            2023: [
                "https://storage.nakatani-foundation.jp/main/p/uploads/05jigyouhoukoku.pdf",
                "https://storage.nakatani-foundation.jp/main/p/uploads/05kessan.pdf",
            ],
            2022: [
                "https://storage.nakatani-foundation.jp/main/p/uploads/04jigyouhoukoku.pdf",
                "https://storage.nakatani-foundation.jp/main/p/uploads/04kessan.pdf",
            ],
            2021: [
                "https://storage.nakatani-foundation.jp/main/p/uploads/03jigyouhoukoku.pdf",
                "https://storage.nakatani-foundation.jp/main/p/uploads/03kessan.pdf",
            ],
            2020: [
                "https://www.nakatani-foundation.jp/wp-content/uploads/02jigyouhoukoku.pdf",
                "https://www.nakatani-foundation.jp/wp-content/uploads/02kessan.pdf",
            ],
        },
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "terumo",
        "db_name": "テルモ生命科学振興財団",
        "disclosure": "https://www.terumozaidan.or.jp/disclosure/",
        "pdf_template": "https://www.terumozaidan.or.jp/disclosure/pdf/fy{year}_01_note.pdf",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "toyota-found",
        "db_name": "トヨタ財団",
        "disclosure": "https://www.toyotafound.or.jp/about/data/",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "murata",
        "db_name": "村田学術振興・教育財団",
        "disclosure": "https://corporate.murata.com/ja-jp/group/zaidan",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    # ------------------------------------------------------------------
    # Expansion batch 2: JFC top-81 + major medium foundations
    # Curated 2026-05. Each entry uses HTML-scrape fallback through the
    # disclosure page to find PDFs (scrape regex matches 'jigyohokoku',
    # 'report', 'houkoku', 'kessan', 'FinancialStatement' substrings).
    # ------------------------------------------------------------------
    {
        "slug": "naito-kagaku",
        "db_name": "内藤記念科学振興財団",
        "disclosure": "https://www.naito-f.or.jp/jp/about/disclosure.php",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "mochida-kinen",
        "db_name": "持田記念医学薬学振興財団",
        "disclosure": "https://www.mochidazaidan.or.jp/about/disclosure.html",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "ichimura-kiyoshi",
        "db_name": "市村清新技術財団",
        "disclosure": "https://www.sgkz.or.jp/disclosure/",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "canon-zaidan",
        "db_name": "キヤノン財団",
        "disclosure": "https://jp.foundation.canon/about/disclosure.html",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "tateishi-kagaku",
        "db_name": "立石科学技術振興財団",
        "disclosure": "https://www.tateisi-f.org/about/finance/",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "funai",
        "db_name": "船井情報科学振興財団",
        "disclosure": "https://www.funaifoundation.jp/finance.html",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "suzuki-zaidan",
        "db_name": "スズキ財団",
        "disclosure": "https://www.suzukifound.jp/disclosure/",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "iwatani-naoji",
        "db_name": "岩谷直治記念財団",
        "disclosure": "https://www.iwatani-foundation.or.jp/disclosure.html",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "ogasawara-toshiaki",
        "db_name": "小笠原敏晶記念財団",
        "disclosure": "https://www.ogasawarafound.or.jp/disclosure/",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "denki-tsushin",
        "db_name": "電気通信普及財団",
        "disclosure": "https://www.taf.or.jp/about/data.html",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "kobayashi",
        "db_name": "小林財団",
        "disclosure": "https://www.kobayashi-foundation.or.jp/disclosure",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "honjo",
        "db_name": "本庄国際奨学財団",
        "disclosure": "https://www.hisf.or.jp/about/disclosure/",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "hoso-bunka",
        "db_name": "放送文化基金",
        "disclosure": "https://www.hbf.or.jp/about/disclosure",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "konica-minolta",
        "db_name": "コニカミノルタ科学技術振興財団",
        "disclosure": "https://www.konicaminolta.com/foundation/about/disclosure.html",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "fujifilm",
        "db_name": "富士フイルム・グリーンファンド",
        "disclosure": "https://www.fujifilm.com/jp/ja/about/sustainability/society/foundation",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "japan-securities",
        "db_name": "日本証券奨学財団",
        "disclosure": "https://www.jssf.or.jp/financial/",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "nomura",
        "db_name": "野村財団",
        "disclosure": "https://www.nomurafoundation.or.jp/about/disclosure/",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "kao",
        "db_name": "花王芸術・科学財団",
        "disclosure": "https://www.kao-foundation.or.jp/about/disclosure.html",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "kao-fuji",
        "db_name": "花王みんなの森づくり",
        "disclosure": "https://www.kao-foundation.or.jp/disclosure/",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "hattori-houkoukai",
        "db_name": "服部報公会",
        "disclosure": "https://hattori-hokokai.or.jp/disclosure.html",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "hayao-nakayama",
        "db_name": "中山隼雄科学技術文化財団",
        "disclosure": "https://www.nakayama-zaidan.or.jp/disclosure",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "yamato",
        "db_name": "ヤマト福祉財団",
        "disclosure": "https://www.yamato-fukushi.jp/disclosure/",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "toyobo",
        "db_name": "東洋紡バイオテクノロジー研究財団",
        "disclosure": "https://www.toyobo.co.jp/foundation/disclosure.html",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "ishibashi",
        "db_name": "石橋財団",
        "disclosure": "https://www.ishibashi-foundation.or.jp/about/disclosure/",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "fukutake",
        "db_name": "福武教育文化振興財団",
        "disclosure": "https://www.fukutake.or.jp/about/disclosure/",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "saneyoshi",
        "db_name": "実吉奨学会",
        "disclosure": "https://www.saneyoshi-shogakukai.or.jp/about/disclosure.html",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "shinohara-yoshiko",
        "db_name": "篠原欣子記念財団",
        "disclosure": "https://ysmf.or.jp/disclosure/",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "otsuka-toshimi",
        "db_name": "大塚敏美育英奨学財団",
        "disclosure": "https://www.otsuka-toshimi.or.jp/about/disclosure.html",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "mitsubishi-ufj-trust",
        "db_name": "三菱ＵＦＪ信託奨学財団",
        "disclosure": "https://www.muft.or.jp/about/disclosure",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "nikki-saneyoshi",
        "db_name": "日揮・実吉奨学会",
        "disclosure": "https://www.nikki-saneyoshi.or.jp/disclosure.html",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "heiwa-nakajima",
        "db_name": "平和中島財団",
        "disclosure": "https://hnf.jp/disclosure/",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "hakuhodo-edu",
        "db_name": "博報堂教育財団",
        "disclosure": "https://www.hakuhodofoundation.or.jp/zaidan/zaim/",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "hirose",
        "db_name": "ヒロセ財団",
        "disclosure": "https://www.hirose-foundation.or.jp/disclosure/",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "haccho",
        "db_name": "発酵研究所",
        "disclosure": "https://www.ifo.or.jp/about/disclosure.html",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "ono-yakuhin",
        "db_name": "小野薬品研究助成",
        "disclosure": "https://www.ono-pharma.com/foundation/disclosure",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "asahi-shukoukai",
        "db_name": "朝日新聞文化財団",
        "disclosure": "https://www.asahizaidan.or.jp/disclosure/",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "nakamura-sekizenkai",
        "db_name": "中村積善会",
        "disclosure": "https://www.nakamura-sekizenkai.or.jp/disclosure.html",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "iketani",
        "db_name": "池谷科学技術振興財団",
        "disclosure": "https://www.iketani.or.jp/disclosure/",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "uchida-yokokai",
        "db_name": "内田洋行教育研究助成",
        "disclosure": "https://uchida.co.jp/foundation/disclosure",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "iijima-tojuro",
        "db_name": "飯島藤十郎記念食品科学振興財団",
        "disclosure": "https://www.iijima-kinenzaidan.or.jp/disclosure.html",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "odakyu",
        "db_name": "小田急財団",
        "disclosure": "https://www.odakyu-foundation.jp/disclosure/",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "morino-kinenzaidan",
        "db_name": "森野記念財団",
        "disclosure": "https://morinozaidan.or.jp/about/disclosure",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "kashima-shoten",
        "db_name": "鹿島学術振興財団",
        "disclosure": "https://kashimafound.org/profile/disclosure/",
        "pdf_template": "https://kashimafound.org/profile/data/jigyohokoku{year_short}.pdf",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "sumitomo-zaidan",
        "db_name": "住友財団",
        "disclosure": "https://www.sumitomo.or.jp/zaimu.htm",
        "pdf_templates": [
            "https://www.sumitomo.or.jp/Act/{year}_jigyo.pdf",
            "https://www.sumitomo.or.jp/Act/{year}_kessan.pdf",
        ],
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "kakehashi",
        "db_name": "化学及血清療法研究所",
        "disclosure": "https://www.kaketsuken.org/disclosure/",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "tetsudo-kosaikai",
        "db_name": "鉄道弘済会",
        "disclosure": "https://www.kousaikai.or.jp/disclosure.html",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
    {
        "slug": "nihon-seimei",
        "db_name": "日本生命財団",
        "disclosure": "https://nihonseimei-zaidan.or.jp/disclosure.html",
        "years": [2024, 2023, 2022, 2021, 2020],
    },
]


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
_session = requests.Session()
_session.headers.update({"User-Agent": USER_AGENT})


def http_get(url: str, expect_pdf: bool = False) -> Optional[bytes]:
    """GET with retries and content-type check. Returns bytes or None."""
    try:
        time.sleep(RATE_LIMIT_SEC)
        r = _session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
        if r.status_code != 200:
            print(f"      HTTP {r.status_code}: {url}")
            return None
        if expect_pdf and not r.content.startswith(b"%PDF"):
            print(f"      not a PDF: {url}")
            return None
        return r.content
    except requests.RequestException as e:
        print(f"      request error: {e} :: {url}")
        return None


# ---------------------------------------------------------------------------
# PDF discovery
# ---------------------------------------------------------------------------
PDF_HREF_RE = re.compile(
    r"href\s*=\s*[\"']([^\"']+\.pdf[^\"']*)[\"']", re.IGNORECASE
)

# Heuristics for matching annual-report PDFs by URL/anchor text
ANNUAL_KEYWORDS = (
    "jigyohokoku", "report", "houkoku", "annual",
    "FinancialStatement", "kessan", "decisionofaccount",
)
YEAR_RE = re.compile(r"(20\d{2}|令和\s*\d+|R\s*\d+|\d{4})")


def discover_pdfs(target: dict) -> dict[int, list[str]]:
    """Return mapping {fiscal_year:int -> [absolute_pdf_url, ...]}.

    Many foundations split their annual disclosure across multiple PDFs
    (事業報告書 / 決算書 / 貸借対照表 / 正味財産増減計算書). We collect them
    all and let the parser pick the first one that yields data.

    Tries the explicit `pdf_template` first, then falls back to scraping the
    disclosure HTML page for *.pdf links and matching their year + keywords.
    """
    found: dict[int, list[str]] = {}

    def _add(y: int, url: str) -> None:
        found.setdefault(y, [])
        if url not in found[y]:
            found[y].append(url)

    # 0) Direct per-year mappings (str or list)
    direct = target.get("pdf_urls") or {}
    for y, val in direct.items():
        if isinstance(val, str):
            _add(y, val)
        else:
            for u in val:
                _add(y, u)

    def _fmt(tpl: str, y: int) -> str:
        # Support: {year}=2024, {year_plus_one}=2025, {year_short}=24, {reiwa}=6
        return tpl.format(
            year=y,
            year_plus_one=y + 1,
            year_short=f"{y % 100:02d}",
            reiwa=y - 2018,
        )

    # 1) Template-based generation (single template only — for full reports)
    template = target.get("pdf_template")
    if template:
        for y in target["years"]:
            _add(y, _fmt(template, y))

    # Optional per-target list of templates for multi-file disclosures
    for tpl in target.get("pdf_templates", []) or []:
        for y in target["years"]:
            _add(y, _fmt(tpl, y))

    # 2) HTML scrape fallback / supplement.
    # If the primary disclosure URL 404s, try a few common alternative paths
    # at the same origin: /about/, /info/, /disclosure/, /zaimu/, /finance/.
    from urllib.parse import urljoin, urlparse

    def _scrape(url: str) -> str:
        b = http_get(url)
        return b.decode("utf-8", errors="replace") if b else ""

    primary = target["disclosure"]
    html = _scrape(primary)
    base_for_join = primary

    if not html:
        parsed = urlparse(primary)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        for alt in ("/about/", "/info/", "/disclosure/", "/zaimu/", "/finance/", "/about/disclosure/", "/profile/disclosure/"):
            alt_url = origin + alt
            if alt_url == primary:
                continue
            html = _scrape(alt_url)
            if html:
                base_for_join = alt_url
                break

    if html:
        parsed = urlparse(base_for_join)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        for href in PDF_HREF_RE.findall(html):
            # Some sites write site-root-relative paths as bare relative
            # ("jp/about/report/foo.pdf"). If the href looks like it starts
            # with a top-level dir of the site path, also try origin-rooted.
            candidates = [urljoin(base_for_join, href)]
            if not href.startswith(("/", "http://", "https://")):
                # treat as if rooted at origin
                candidates.append(origin + "/" + href.lstrip("./"))
            for absolute in candidates:
                lower = absolute.lower()
                if not any(k.lower() in lower for k in ANNUAL_KEYWORDS):
                    continue
                ym = re.search(r"20(\d{2})", absolute)
                if not ym:
                    continue
                year = 2000 + int(ym.group(1))
                if year not in target["years"]:
                    continue
                _add(year, absolute)

    return found


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------
def cache_pdf(target_slug: str, cache_key, url: str) -> Optional[Path]:
    """Download (or load cached) PDF and return the local path.

    cache_key may be int (year) or str (e.g. '2024_1' for multi-pdf year).
    """
    cache_subdir = CACHE_DIR / target_slug
    cache_subdir.mkdir(parents=True, exist_ok=True)
    fname = cache_subdir / f"{cache_key}.pdf"
    if fname.exists() and fname.stat().st_size > 1024:
        return fname
    payload = http_get(url, expect_pdf=True)
    if not payload:
        return None
    fname.write_bytes(payload)
    return fname


# ---------------------------------------------------------------------------
# PDF parsing
# ---------------------------------------------------------------------------
# A "yen number" is a sequence of digits with comma thousands separators only
# (no internal whitespace). Capturing whitespace caused multi-column rows to
# concatenate into nonsensical 24-digit values.
#
# Examples that match:  259,953,314    138,666,718,893    1,533,349,483
# Examples that DON'T:  259,953,314 1,533,349,483   (split into two)
NUM_RE = re.compile(r"(?<![0-9０-９.,，])([0-9０-９](?:[0-9０-９]{0,2}[,，][0-9０-９]{3})+|[0-9０-９]{4,13})(?![0-9０-９.,，])")
# Ordered by priority — first hit wins per line. Each entry is the substring
# that must appear at the START of the line (after table marks/numbering),
# avoiding false matches in prose.
#
# Important nuance: 「研究助成事業」 as a P&L line item ≠ 「研究助成」 in prose.
# Strict-prefix matching plus a noise filter (must not contain Japanese
# narrative particles like 「を」「は」「が」 between keyword and the number)
# rules out narrative text.
GRANT_LINE_KEYWORDS_STRICT = (
    "助成事業費",
    "公益目的事業費",
    "助成金支出",
    "事業費計",
    "研究助成事業",
    "事業費",  # generic but only at line start
    "助成事業",  # generic fallback; still requires line-start
)
# Narrative-tolerant fallback patterns (last resort).
# Match "研究助成金の支払総額は X億Y万円" etc.
GRANT_NARRATIVE_RE = re.compile(
    r"(?:研究助成金の支払総額|助成金総額|助成金支出総額)[はをが、]*([0-9０-９,，]{1,4}億[0-9０-９,，]{0,5}万?円?|[0-9０-９,，]{4,}\s*円)"
)
# For total assets we want the bare 「資産合計」 row, NOT 「負債及び正味財産合計」.
# Use exact-line matching: the line must START with the keyword (after spaces).
ASSET_LINE_PRIMARY = ("資産合計", "総資産")
ASSET_LINE_FALLBACK = ("正味財産合計",)


def _zenkaku_to_int(s: str) -> Optional[int]:
    """Normalize digits, strip separators, return int or None."""
    trans = str.maketrans("０１２３４５６７８９，", "0123456789,")
    s = s.translate(trans).replace(",", "").replace(" ", "").strip()
    if not s.isdigit():
        return None
    return int(s)


def _parse_oku_man_yen(s: str) -> Optional[int]:
    """Parse Japanese narrative amounts like '5億6,710万円' to integer yen."""
    trans = str.maketrans("０１２３４５６７８９，", "0123456789,")
    s = s.translate(trans).replace(",", "").replace(" ", "").strip()
    m = re.match(r"^(\d+)億(\d+)万円?$", s)
    if m:
        return int(m.group(1)) * 100_000_000 + int(m.group(2)) * 10_000
    m = re.match(r"^(\d+)億円?$", s)
    if m:
        return int(m.group(1)) * 100_000_000
    m = re.match(r"^(\d+)万円?$", s)
    if m:
        return int(m.group(1)) * 10_000
    m = re.match(r"^(\d+)円?$", s)
    if m:
        v = int(m.group(1))
        if v >= 1_000_000:
            return v
    return None


def _line_numbers(line: str, floor: int = 1_000_000) -> list[int]:
    """Return all yen amounts on a line, in left-to-right order."""
    nums: list[int] = []
    for m in NUM_RE.findall(line):
        v = _zenkaku_to_int(m)
        if v is not None and v >= floor:
            nums.append(v)
    return nums


def _largest_number(line: str, floor: int = 1_000_000) -> Optional[int]:
    nums = _line_numbers(line, floor)
    return max(nums) if nums else None


def _starts_with_keyword(line: str, keyword: str, exclusions: tuple[str, ...] = ()) -> bool:
    """True iff the line, after stripping leading whitespace and table marks,
    starts with the keyword and is NOT prefixed by any of the exclusions."""
    stripped = line.lstrip(" 　\t□■・(（")
    if any(ex in stripped[: stripped.find(keyword) + len(keyword)] and stripped.find(ex) < stripped.find(keyword)
           for ex in exclusions if ex in stripped and keyword in stripped):
        return False
    return stripped.startswith(keyword) or (
        # Allow a small label cell before the keyword in tables, e.g. "Ⅰ 資産合計"
        re.match(rf"^[ⅠⅡⅢⅣⅤ１-９0-9.\s]{{0,4}}{re.escape(keyword)}", stripped) is not None
    )


def parse_pdf(path: Path) -> dict:
    """Open a PDF and extract grant amount + asset hints."""
    out: dict = {
        "annual_grant_amount": None,
        "total_assets": None,
        "snippets": {},
        "pages": 0,
        "extractable": False,
    }
    try:
        with pdfplumber.open(path) as pdf:
            out["pages"] = len(pdf.pages)
            full_text_parts: list[str] = []
            for p in pdf.pages:
                t = p.extract_text() or ""
                full_text_parts.append(t)
            full = "\n".join(full_text_parts)
    except Exception as e:
        out["error"] = str(e)
        return out

    if len(full.strip()) < 200:
        out["error"] = "no extractable text (likely image-based PDF)"
        return out

    out["extractable"] = True

    # Many Japanese financial PDFs render labels as wide-spaced characters,
    # e.g.「研 究 助 成 事 業」. Normalize a SECOND copy with internal Japanese
    # whitespace squeezed, and search BOTH versions per line so numbers stay
    # intact while keyword matching succeeds.
    raw_lines = full.split("\n")
    lines = []  # (raw, condensed)
    for raw in raw_lines:
        # Squeeze whitespace between Japanese-script characters only,
        # leaving number/ASCII whitespace intact.
        condensed = re.sub(
            r"([一-龯ぁ-ゟァ-ヿ々])[ 　\t]+(?=[一-龯ぁ-ゟァ-ヿ々])",
            r"\1",
            raw,
        )
        lines.append((raw, condensed))

    # ---- annual_grant_amount ----------------------------------------
    # Strategy:
    #  (1) STRICT line-start keyword match on P&L items.
    #  (2) Fallback: narrative pattern "研究助成金の支払総額は X億Y万円".
    grant_candidates: list[tuple[int, str, str]] = []
    for raw, condensed in lines:
        # Use condensed form for keyword matching, raw form for number extraction.
        for kw in GRANT_LINE_KEYWORDS_STRICT:
            if _starts_with_keyword(condensed, kw):
                nums = _line_numbers(raw, floor=10_000_000)
                if nums:
                    # Largest number on the line — works for both two-column
                    # 当年度/前年度 and multi-column 内訳/合計 layouts.
                    v = max(nums)
                    grant_candidates.append((v, kw, raw.strip()))
                break
    if grant_candidates:
        # Prefer the highest-priority keyword first; ties broken by larger sum.
        priority = {kw: i for i, kw in enumerate(GRANT_LINE_KEYWORDS_STRICT)}
        grant_candidates.sort(key=lambda x: (priority.get(x[1], 99), -x[0]))
        best = grant_candidates[0]
        out["annual_grant_amount"] = best[0]
        out["snippets"]["grant"] = f"[{best[1]}] {best[2][:160]}"
    else:
        # Narrative fallback
        for raw, condensed in lines:
            m = GRANT_NARRATIVE_RE.search(condensed)
            if m:
                v = _parse_oku_man_yen(m.group(1))
                if v:
                    out["annual_grant_amount"] = v
                    out["snippets"]["grant"] = f"[narrative] {raw.strip()[:160]}"
                    break

    # ---- total_assets ------------------------------------------------
    # Strict: must START with the keyword to avoid 「負債及び正味財産合計」.
    # First number on the line is the current period (BS layout convention).
    asset_value: Optional[int] = None
    asset_label: str = ""
    asset_line: str = ""
    for raw, condensed in lines:
        for kw in ASSET_LINE_PRIMARY:
            if _starts_with_keyword(condensed, kw):
                nums = _line_numbers(raw, floor=10_000_000)
                if nums:
                    if asset_value is None or nums[0] > asset_value:
                        asset_value = nums[0]
                        asset_label = kw
                        asset_line = raw.strip()
                break
    if asset_value is None:
        for raw, condensed in lines:
            for kw in ASSET_LINE_FALLBACK:
                if _starts_with_keyword(condensed, kw):
                    nums = _line_numbers(raw, floor=10_000_000)
                    if nums:
                        if asset_value is None or nums[0] > asset_value:
                            asset_value = nums[0]
                            asset_label = kw
                            asset_line = raw.strip()
                    break
    if asset_value:
        out["total_assets"] = asset_value
        out["snippets"]["assets"] = f"[{asset_label}] {asset_line[:160]}"

    return out


# ---------------------------------------------------------------------------
# DB writes
# ---------------------------------------------------------------------------
def find_org_id(conn: sqlite3.Connection, db_name_substr: str) -> Optional[str]:
    """Find a single matching organizations.id, preferring 公益財団法人 prefix."""
    cur = conn.execute(
        "SELECT id, name, annual_grant_amount FROM organizations "
        "WHERE name LIKE ? ORDER BY "
        "  CASE WHEN name LIKE '公益財団法人%' THEN 0 ELSE 1 END, "
        "  COALESCE(annual_grant_amount, 0) DESC LIMIT 1",
        (f"%{db_name_substr}%",),
    )
    row = cur.fetchone()
    return row[0] if row else None


def update_org(
    conn: sqlite3.Connection,
    org_id: str,
    annual: Optional[int],
    assets: Optional[int],
    history: list[dict],
    dry_run: bool,
) -> dict:
    """Update DB columns. Returns a delta report."""
    cur = conn.execute(
        "SELECT annual_grant_amount, total_assets, annual_grant_amount_history "
        "FROM organizations WHERE id = ?",
        (org_id,),
    )
    row = cur.fetchone()
    if not row:
        return {"error": "org not found"}
    cur_annual, cur_assets, cur_hist = row

    sets: list[str] = []
    params: list = []
    delta = {"updated_annual": False, "updated_assets": False, "updated_history": False}

    if annual and annual != cur_annual:
        sets.append("annual_grant_amount = ?")
        params.append(annual)
        delta["updated_annual"] = True
        delta["annual_old"] = cur_annual
        delta["annual_new"] = annual

    if assets and assets != cur_assets:
        sets.append("total_assets = ?")
        params.append(assets)
        delta["updated_assets"] = True
        delta["assets_old"] = cur_assets
        delta["assets_new"] = assets

    if history:
        history_json = json.dumps(history, ensure_ascii=False)
        if history_json != cur_hist:
            sets.append("annual_grant_amount_history = ?")
            params.append(history_json)
            delta["updated_history"] = True
            delta["history_years"] = [h["year"] for h in history]

    if sets and not dry_run:
        sets.append("updated_at = ?")
        params.append(datetime.now(timezone.utc).replace(tzinfo=None).isoformat())
        params.append(org_id)
        conn.execute(
            f"UPDATE organizations SET {', '.join(sets)} WHERE id = ?",
            params,
        )
        conn.commit()

    return delta


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def process_target(target: dict, conn: sqlite3.Connection, dry_run: bool) -> dict:
    print(f"\n[ {target['db_name']} ]  ({target['slug']})")
    pdfs = discover_pdfs(target)
    if not pdfs:
        print("  no PDFs discovered")
        return {"target": target["db_name"], "pdfs": 0, "extracted": 0}

    print(f"  PDFs candidates: {sorted(pdfs.keys(), reverse=True)}")
    history: list[dict] = []
    asset_latest: Optional[int] = None
    snippets: dict = {}
    extractable_pages = 0

    for year in sorted(pdfs.keys(), reverse=True):
        urls = pdfs[year] if isinstance(pdfs[year], list) else [pdfs[year]]
        # Merge results across all PDFs for this fiscal year — typically the
        # 事業報告書 has narrative, the 決算書 has the P&L numbers.
        year_grant: Optional[int] = None
        year_assets: Optional[int] = None
        year_snip: dict = {}
        for idx, url in enumerate(urls):
            cache_key = f"{year}" if idx == 0 else f"{year}_{idx}"
            path = cache_pdf(target["slug"], cache_key, url)
            if not path:
                continue
            parsed = parse_pdf(path)
            if not parsed.get("extractable"):
                continue
            extractable_pages += parsed["pages"]
            if year_grant is None and parsed["annual_grant_amount"]:
                year_grant = parsed["annual_grant_amount"]
            if year_assets is None and parsed["total_assets"]:
                year_assets = parsed["total_assets"]
            if not year_snip and parsed["snippets"]:
                year_snip = parsed["snippets"]

        if year_grant:
            history.append({"year": year, "amount": year_grant})
            print(
                f"    {year}: grant={year_grant:,}円 "
                f"assets={year_assets or '-'}  ({len(urls)} pdf(s))"
            )
        else:
            print(f"    {year}: no grant amount detected ({len(urls)} pdf(s))")
        if asset_latest is None and year_assets:
            asset_latest = year_assets
        if not snippets:
            snippets = year_snip

    # Latest year amount becomes annual_grant_amount
    annual = history[0]["amount"] if history else None

    org_id = find_org_id(conn, target["db_name"])
    if not org_id:
        print(f"  no DB org match for '{target['db_name']}'")
        return {
            "target": target["db_name"],
            "pdfs": len(pdfs),
            "history_years": [h["year"] for h in history],
            "annual_grant_amount": annual,
            "total_assets": asset_latest,
            "snippets": snippets,
            "db_match": False,
        }

    delta = update_org(conn, org_id, annual, asset_latest, history, dry_run)
    print(f"  DB org_id={org_id} delta={delta}")

    return {
        "target": target["db_name"],
        "pdfs": len(pdfs),
        "history_years": [h["year"] for h in history],
        "annual_grant_amount": annual,
        "total_assets": asset_latest,
        "snippets": snippets,
        "db_match": True,
        "delta": delta,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=0, help="process only first N targets")
    parser.add_argument("--target", help="substring match against db_name; process only matches")
    parser.add_argument("--dry-run", action="store_true", help="don't write to DB")
    parser.add_argument(
        "--skip-extracted",
        action="store_true",
        help="skip targets whose annual_grant_amount is already populated in DB",
    )
    parser.add_argument(
        "--offset", type=int, default=0,
        help="skip first N targets (used to resume after partial runs)",
    )
    args = parser.parse_args()

    targets = TARGETS
    if args.target:
        needle = args.target
        targets = [t for t in TARGETS if needle in t["db_name"]]

    if args.skip_extracted:
        # Filter out targets whose DB row already has annual_grant_amount.
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        keep: list[dict] = []
        for t in targets:
            org_id = find_org_id(conn, t["db_name"])
            if not org_id:
                keep.append(t)
                continue
            cur = conn.execute(
                "SELECT annual_grant_amount, annual_grant_amount_history FROM organizations WHERE id = ?",
                (org_id,),
            )
            row = cur.fetchone()
            # Skip if both annual_grant_amount and history are already set.
            if row and row[0] and row[1]:
                continue
            keep.append(t)
        conn.close()
        targets = keep

    if args.offset:
        targets = targets[args.offset:]
    if args.limit:
        targets = targets[: args.limit]

    print(f"Targets: {len(targets)} foundations  dry_run={args.dry_run}")
    print(f"DB: {DB_PATH}")
    print(f"Cache: {CACHE_DIR}")

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    results: list[dict] = []
    for t in targets:
        # Open & close per target — pdfplumber's internal mmap/IO can interact
        # poorly with long-lived sqlite connections on macOS. Use a 30 s busy
        # timeout to tolerate concurrent readers/writers on the DB.
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        conn.execute("PRAGMA busy_timeout = 30000")
        try:
            results.append(process_target(t, conn, args.dry_run))
        except Exception as e:
            print(f"  EXCEPTION on {t['db_name']}: {e}")
            results.append({"target": t["db_name"], "error": str(e)})
        finally:
            conn.close()

    OUTPUT_FILE.write_text(json.dumps(results, ensure_ascii=False, indent=2))

    # Summary
    extracted = [r for r in results if r.get("annual_grant_amount")]
    updated_annual = [r for r in results if r.get("delta", {}).get("updated_annual")]
    updated_assets = [r for r in results if r.get("delta", {}).get("updated_assets")]
    updated_history = [r for r in results if r.get("delta", {}).get("updated_history")]

    print(f"\n=== Summary ===")
    print(f"Targets processed: {len(results)}")
    print(f"Annual grant amount extracted: {len(extracted)}")
    print(f"DB updates — annual: {len(updated_annual)}  assets: {len(updated_assets)}  history: {len(updated_history)}")
    print(f"Output: {OUTPUT_FILE}")

    return 0 if extracted else 1


if __name__ == "__main__":
    sys.exit(main())
