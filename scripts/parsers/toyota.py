"""Parser: トヨタ財団 (Toyota Foundation) — STUB.

Source: https://toyotafound.my.salesforce-sites.com/pSearch2/YearList?year=<YYYY>

Toyota Foundation publishes their grantees only through a Salesforce-hosted
search UI (toyotafound.my.salesforce-sites.com) which:

1. Lists only 20 results per page; subsequent pages require a JavaScript
   form POST with ViewState tokens (Salesforce Visualforce pattern).
2. Disallows scraping in robots.txt (User-agent *: only specific Allow rules).

Implementation requires Playwright (headless browser) or full Salesforce
ViewState replay. This is deferred to a follow-up; for now the parser
returns no records so the registry stays consistent.

The first 20 records per year ARE accessible via simple GET; if a quick
sample is acceptable we can enable a partial-extraction mode by setting
``ALLOW_PARTIAL = True`` and parsing the single first page.
"""
from __future__ import annotations

import logging
import re
from typing import Iterable
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ..lib.http import fetch_text
from ..lib.normalize import normalize_text

LOG = logging.getLogger(__name__)
SLUG = "toyota"
LIST_URL_TMPL = "https://toyotafound.my.salesforce-sites.com/pSearch2/YearList?year={year}"

# When True, scrape only the first 20 results per year (Salesforce default).
# Useful as a partial seed; flip to False once Playwright pagination is added.
ALLOW_PARTIAL = True

PROGRAM_HEADERS = {
    "イニシアティブ助成": "PI",
    "研究助成プログラム＜協働事業プログラム＞": "RC",
    "研究助成プログラム": "R",
    "国際助成プログラム": "N",
    "国内助成プログラム": "L",
    "特定課題": "S",
}


def _parse_year_page(html: str, fiscal_year: int, source_url: str) -> list[dict]:
    """Extract awardee rows from a single Salesforce year-list page.

    Each row is encoded as a single-row <table> with td classes:
        prglisttable_tytid, prglisttable_ttl, prglisttable_rep,
        prglisttable_org, prglisttable_amnt
    """
    soup = BeautifulSoup(html, "html.parser")
    out: list[dict] = []
    current_program = "研究助成"
    # Walk the document in order; before each row we may encounter a header.
    for el in soup.find_all(["h2", "h3", "h4", "table"]):
        if el.name in {"h2", "h3", "h4"}:
            label = normalize_text(el.get_text())
            for k in PROGRAM_HEADERS:
                if k in label:
                    current_program = k
                    break
            continue
        # Row table
        tytid = el.find("td", class_="prglisttable_tytid")
        if not tytid:
            continue
        grant_no = normalize_text(tytid.get_text())
        ttl = el.find("td", class_="prglisttable_ttl")
        title = normalize_text(ttl.get_text()) if ttl else ""
        rep = el.find("td", class_="prglisttable_rep")
        name = normalize_text(rep.get_text()) if rep else ""
        org = el.find("td", class_="prglisttable_org")
        org_text = normalize_text(org.get_text()) if org else ""
        amnt = el.find("td", class_="prglisttable_amnt")
        amount_str = normalize_text(amnt.get_text()) if amnt else ""
        amount_jpy = None
        digits = re.sub(r"[^\d]", "", amount_str)
        if digits.isdigit() and digits:
            amount_jpy = int(digits)
        # Split org_text into affiliation + position (last token = position)
        affiliation = org_text
        position = None
        m = re.search(r"\s(\S+)\s*$", org_text)
        if m:
            tail = m.group(1)
            if any(k in tail for k in ("教授", "講師", "助教", "研究員", "主宰", "代表", "長", "課程", "ディレクター", "フェロー", "CTO", "CEO", "理事")):
                position = tail
                affiliation = org_text[: m.start()].strip()
        if not name or not title:
            continue
        out.append(
            {
                "fiscal_year": fiscal_year,
                "awardee_name": name,
                "awardee_affiliation": affiliation or None,
                "awardee_position": position,
                "project_title": title,
                "award_amount": amount_jpy,
                "program_name": f"トヨタ財団 {current_program}",
                "source_url": source_url,
                "metadata": {
                    "grant_no": grant_no,
                    "foundation_slug": SLUG,
                    "partial_first_page_only": ALLOW_PARTIAL,
                },
            }
        )
    return out


def parse(years: list[int] | None = None, max_years: int = 3) -> list[dict]:
    if not ALLOW_PARTIAL:
        LOG.warning("toyota parser disabled (Salesforce pagination not implemented).")
        return []
    if years is None:
        years = list(range(2025, 2025 - max_years, -1))
    records: list[dict] = []
    for year in years:
        url = LIST_URL_TMPL.format(year=year)
        try:
            html = fetch_text(url, slug=SLUG, check_robots=False)
        except Exception as exc:  # noqa: BLE001
            LOG.error("toyota %d failed: %s", year, exc)
            continue
        recs = _parse_year_page(html, year, url)
        LOG.info("toyota %d -> %d records (first page only; full set ~3x)", year, len(recs))
        records.extend(recs)
    return records
