"""Parser: 稲盛財団 (Inamori Foundation) — 研究助成 recipients.

Source: https://www.inamori-f.or.jp/recipient/?year=<YYYY>
Year filter is via querystring (validated against the live site as of 2026-05).
Each year is paginated; each page lists ~24 recipients in ``div.human``
elements containing name, affiliation, and category tag.

Per recipient, a detail page may publish a project title, but for stage 1 the
list-level information is enough to populate ``grant_results``. Where the
project title is missing we fall back to a generic placeholder (``採択者``)
so the row still shows up under ``fiscal_year`` aggregations.
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
SLUG = "inamori"
LIST_URL = "https://www.inamori-f.or.jp/recipient/"

# Inamori publishes 7 disciplinary tags; map to our level-1 taxonomy IDs.
CATEGORY_MAP = {
    "数物系科学": "natural_science",
    "化学": "natural_science",
    "情報学": "engineering",
    "工学": "engineering",
    "生物学": "life_science",
    "生命科学": "life_science",
    "農学": "life_science",
    "医歯薬学": "life_science",
    "Science & Engineering": "natural_science",
    "材料科学": "engineering",
    "人文科学": "humanities_social",
    "社会科学": "humanities_social",
    "芸術": "arts_culture",
    "思想・歴史": "humanities_social",
    "言語・文学・芸術": "arts_culture",
}


def _parse_listing(html: str) -> tuple[list[dict], int]:
    """Return ``(records, max_page)``."""
    soup = BeautifulSoup(html, "html.parser")
    out: list[dict] = []
    for human in soup.select("div.human"):
        a = human.select_one("a[href]")
        if not a:
            continue
        detail_url = a.get("href", "").strip()
        name = normalize_text(human.select_one(".human__name").get_text() if human.select_one(".human__name") else "")
        # The second <p> inside .human__text is affiliation.
        ps = human.select(".human__text p")
        affiliation = normalize_text(ps[1].get_text()) if len(ps) >= 2 else None
        tags = [normalize_text(t.get_text()) for t in human.select(".tag span")]
        out.append(
            {
                "name": name,
                "affiliation": affiliation,
                "tags": tags,
                "detail_url": detail_url,
            }
        )
    max_page = 1
    for a in soup.select(".pagination--recipient a"):
        m = re.search(r"/page/(\d+)/", a.get("href", ""))
        if m:
            max_page = max(max_page, int(m.group(1)))
    return out, max_page


def _category_id(tags: list[str]) -> str | None:
    for t in tags:
        if t in CATEGORY_MAP:
            return CATEGORY_MAP[t]
    return None


def parse(years: list[int] | None = None, max_years: int = 5) -> list[dict]:
    if years is None:
        # Live site exposes 2018-2026 in the year filter. Take most recent N.
        years = list(range(2026, 2026 - max_years, -1))

    records: list[dict] = []
    for year in years:
        page = 1
        while True:
            if page == 1:
                url = f"{LIST_URL}?year={year}&s="
            else:
                url = f"{LIST_URL}page/{page}/?year={year}&s="
            try:
                html = fetch_text(url, slug=SLUG)
            except Exception as exc:  # noqa: BLE001
                LOG.error("inamori %d page %d failed: %s", year, page, exc)
                break
            items, max_page = _parse_listing(html)
            if not items:
                break
            for it in items:
                records.append(
                    {
                        "fiscal_year": year,
                        "awardee_name": it["name"],
                        "awardee_affiliation": it["affiliation"],
                        "project_title": "稲盛研究助成 採択者",  # list page has no per-row title
                        "program_name": "稲盛財団 研究助成",
                        "source_url": it["detail_url"],
                        "field_category_id": _category_id(it["tags"]),
                        "metadata": {
                            "tags": it["tags"],
                            "list_url": url,
                            "foundation_slug": SLUG,
                        },
                    }
                )
            LOG.info("inamori %d page %d -> %d items (of %d pages)", year, page, len(items), max_page)
            if page >= max_page:
                break
            page += 1
    return records
