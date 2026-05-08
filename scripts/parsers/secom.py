"""Parser: セコム科学技術振興財団 (Secom Science and Technology Foundation).

Source: https://www.secomzaidan.jp/kiroku.html
Per-year HTML pages:
    /kiroku_r06.html ... /kiroku_r02.html (Reiwa)
    /kiroku_h31.html ... /kiroku_h15.html (Heisei)

Each year page has multiple tables, one per acceptance cohort. Each table has
a 2-row pattern per awardee:

    Row A (3+ cells): [番号, 氏名, 助成額(年度別 + 累計)]
    Row B (3 cells):  [所属, 職名, 研究課題名]

The first 2-3 header rows describe column titles and are skipped.
"""
from __future__ import annotations

import logging
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ..lib.http import fetch_text
from ..lib.normalize import normalize_text, heisei_to_western

LOG = logging.getLogger(__name__)
SLUG = "secom"
INDEX_URL = "https://www.secomzaidan.jp/kiroku.html"

# Filename like kiroku_r06.html (Reiwa 6) or kiroku_h31.html (Heisei 31)
PAGE_RE = re.compile(r"^kiroku_(r\d{1,2}|h\d{1,2})\.html$")
NUM_RE = re.compile(r"^\s*\d+\s*$")
AMOUNT_RE = re.compile(r"^[\d,，／\s]*$")


def _discover_year_pages(html: str) -> list[tuple[int, str]]:
    """Return ``[(fiscal_year_western, absolute_url)]``."""
    soup = BeautifulSoup(html, "html.parser")
    out: list[tuple[int, str]] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        m = PAGE_RE.match(href)
        if not m:
            continue
        era_tag = m.group(1)
        # r06 -> 2024 (Reiwa 6 = 2024)
        if era_tag.startswith("r"):
            year_western = 2018 + int(era_tag[1:])
        else:  # h
            year_western = 1988 + int(era_tag[1:])
        out.append((year_western, urljoin(INDEX_URL, href)))
    out.sort(key=lambda t: -t[0])
    return out


def _parse_year_page(html: str, fiscal_year: int, source_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    out: list[dict] = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        # Build cell lists
        row_cells: list[list[str]] = []
        for r in rows:
            cells = [
                normalize_text(c.get_text(separator=" ", strip=True))
                for c in r.find_all(["td", "th"])
            ]
            row_cells.append(cells)

        # The header pattern is the first row containing "番号" and "申請者";
        # data starts after the next 1-2 sub-header rows.
        header_idx = None
        for i, cells in enumerate(row_cells):
            if any("番号" in c for c in cells) and any("申請者" in c for c in cells):
                header_idx = i
                break
        if header_idx is None:
            continue
        # Skip secondary header rows (氏名 / 所属 etc).
        i = header_idx + 1
        # Walk forward; data rows come in pairs (Row A + Row B).
        while i < len(row_cells):
            row_a = row_cells[i]
            if not row_a or not row_a[0] or not NUM_RE.match(row_a[0]):
                # Sub-header or empty; advance.
                i += 1
                continue
            # Row A: [num, name, ...amounts..., cumulative]
            num = row_a[0]
            name = row_a[1] if len(row_a) > 1 else ""
            cumulative = ""
            if len(row_a) >= 3:
                cumulative = row_a[-1]
            # Row B: [affiliation, position, title]
            row_b = row_cells[i + 1] if i + 1 < len(row_cells) else []
            affiliation = row_b[0] if len(row_b) >= 1 else ""
            position = row_b[1] if len(row_b) >= 2 else ""
            title = row_b[2] if len(row_b) >= 3 else ""
            if not name or not title:
                i += 1
                continue
            # Convert cumulative 万円 to JPY if numeric
            amount = None
            cum = cumulative.replace(",", "").replace("，", "").strip()
            if cum.isdigit():
                amount = int(cum) * 10_000
            out.append(
                {
                    "fiscal_year": fiscal_year,
                    "awardee_name": name,
                    "awardee_affiliation": affiliation or None,
                    "awardee_position": position or None,
                    "project_title": title,
                    "award_amount": amount,
                    "program_name": "セコム科学技術振興財団 一般研究助成",
                    "source_url": source_url,
                    "metadata": {
                        "no": num,
                        "raw_cumulative_man": cumulative,
                        "foundation_slug": SLUG,
                    },
                }
            )
            i += 2
    return out


def parse(years: list[int] | None = None, max_years: int = 3) -> list[dict]:
    index_html = fetch_text(INDEX_URL, slug=SLUG)
    pairs = _discover_year_pages(index_html)
    if years:
        pairs = [(y, u) for y, u in pairs if y in set(years)]
    else:
        pairs = pairs[:max_years]
    records: list[dict] = []
    for year, url in pairs:
        try:
            html = fetch_text(url, slug=SLUG)
        except Exception as exc:  # noqa: BLE001
            LOG.error("secom %d failed: %s", year, exc)
            continue
        recs = _parse_year_page(html, year, url)
        LOG.info("secom %d -> %d records", year, len(recs))
        records.extend(recs)
    return records
