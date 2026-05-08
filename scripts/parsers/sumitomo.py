"""Parser: 住友財団 (Sumitomo Foundation).

Source: https://www.sumitomo.or.jp/
Per-program / per-year HTML pages:
    html/kiso/kisotai<YYYY>.htm        基礎科学研究助成
    html/kankyo/kantaisyo<YYYY>.htm    環境研究助成

The HTML tables vary by year:

A) Multi-row form (kiso 2023-2024):
   col1: 研究テーマ | col2: 研究者氏名 | col3: 助成金額
   → next row(s) below: 所属機関 / 職位

B) Multi-row + 分野 form (kiso 2025+):
   col1: 分野 | col2: テーマ | col3: 研究者 | col4: 助成金額
   → next row(s) below: 所属 / 職位

C) Compact form (kiso 2022 and earlier, environment all years):
   col1: テーマ | col2: 氏名 + 所属 + 職位 (single cell) | col3: 額

Strategy: walk the table rows; whenever a row has >=3 cells with a numeric
amount in the last cell, treat it as a primary record and look ahead at
single-cell rows for affiliation / position metadata.
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
SLUG = "sumitomo"

# Programs to scrape. Each entry: (program_label, slug-prefix, page_template).
PROGRAMS: list[tuple[str, str, str]] = [
    (
        "住友財団 基礎科学研究助成",
        "kiso",
        "https://www.sumitomo.or.jp/html/kiso/kisotai{year}.htm",
    ),
    (
        "住友財団 環境研究助成",
        "kankyo",
        "https://www.sumitomo.or.jp/html/kankyo/kantaisyo{year}.htm",
    ),
]

AMOUNT_RE = re.compile(r"^\s*[\d,，]+\s*$")
POS_KEYWORDS = (
    "教授", "准教授", "助教", "講師", "研究員", "主任研究員", "上級研究員",
    "博士研究員", "チームリーダー", "ユニット長", "プロジェクトリーダー",
    "室長", "部長", "センター長", "ディレクター", "フェロー",
    "学長", "副学長", "学部長", "館長", "所長", "副所長",
    "医員", "医師", "教諭", "校長",
)
AFFIL_KEYWORDS = ("大学", "研究所", "研究院", "機構", "センター", "学院", "病院", "学校", "学園", "協会", "法人", "博物館")


def _is_amount(s: str) -> bool:
    return bool(AMOUNT_RE.match(s.replace(",", "").replace("，", "")))


def _to_jpy_from_man(s: str) -> int | None:
    digits = s.replace(",", "").replace("，", "").strip()
    if not digits.isdigit():
        return None
    return int(digits) * 10_000  # 万円 -> 円


def _looks_like_affiliation(s: str) -> bool:
    return any(k in s for k in AFFIL_KEYWORDS)


def _looks_like_position(s: str) -> bool:
    return any(k in s for k in POS_KEYWORDS) and not _looks_like_affiliation(s)


def _split_name_affiliation_position(cell: str) -> tuple[str, str | None, str | None]:
    """Compact form (env 2024, kiso 2022): split a single cell.

    Pattern: '<name> <kana?> <affiliation>... <position>[ 他N名]'
    """
    text = normalize_text(cell)
    if not text:
        return "", None, None
    # Try to peel a position from the end
    position = None
    affiliation = None
    # Strip trailing "他N名"
    text2 = re.sub(r"\s*他\s*\d+\s*名\s*$", "", text).strip()
    # Find last position keyword
    for kw in sorted(POS_KEYWORDS, key=len, reverse=True):
        m = re.search(rf"\s({kw})\s*$", text2)
        if m:
            position = kw
            text2 = text2[: m.start()].strip()
            break
    # Now text2 is "<name> <affiliation>...". Affiliation usually contains a
    # university/institute keyword; everything before that is the name.
    aff_match = None
    for kw in AFFIL_KEYWORDS:
        m = re.search(rf"\S*{kw}", text2)
        if m:
            aff_match = m
            break
    if aff_match:
        name = text2[: aff_match.start()].strip()
        affiliation = text2[aff_match.start() :].strip()
    else:
        name = text2.strip()
    return name, affiliation, position


def _scrape_page(url: str, fiscal_year: int, program_name: str) -> list[dict]:
    try:
        html = fetch_text(url, slug=SLUG)
    except Exception as exc:  # noqa: BLE001
        LOG.info("sumitomo %d %s skipped: %s", fiscal_year, url, exc)
        return []
    soup = BeautifulSoup(html, "html.parser")
    out: list[dict] = []
    current_program = program_name
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        # Pre-compute cell lists
        row_cells: list[list[str]] = []
        for r in rows:
            cells = [normalize_text(c.get_text(separator=" ", strip=True)) for c in r.find_all(["td", "th"])]
            row_cells.append(cells)

        i = 0
        while i < len(row_cells):
            cells = row_cells[i]
            # Update current_program from a header row that mentions a section
            if len(cells) == 1:
                txt = cells[0]
                if any(k in txt for k in ("一般研究", "課題研究", "若手研究者")):
                    # carry the section
                    section = txt.split("（")[0].strip()
                    current_program = f"{program_name} / {section}"
                i += 1
                continue
            if len(cells) < 3:
                i += 1
                continue
            # Detect data row by a numeric amount in the last cell
            amount_cell = cells[-1]
            if not _is_amount(amount_cell):
                i += 1
                continue
            amount_jpy = _to_jpy_from_man(amount_cell)

            # Number of leading classification cells before theme: 1 if first cell is a 分野 label
            # Heuristic: themes are long text containing kanji; 分野 cells are short (<= 6 chars)
            offset = 0
            if len(cells) >= 4 and len(cells[0]) <= 8 and len(cells[1]) > 8:
                offset = 1  # first cell is 分野
            theme = cells[offset]
            researcher_cell = cells[offset + 1] if len(cells) > offset + 1 else ""
            # researcher_cell may be:
            #  - just name (multi-row form): then look ahead
            #  - "name affiliation position" compact form
            name, affiliation, position = "", None, None
            if researcher_cell:
                if _looks_like_affiliation(researcher_cell):
                    # compact form
                    name, affiliation, position = _split_name_affiliation_position(researcher_cell)
                else:
                    # multi-row form: name only
                    # Strip trailing "他N名"
                    name = re.sub(r"\s*他\s*\d+\s*名\s*$", "", researcher_cell).strip()
                    # Look ahead at next 1-3 single-cell rows for affiliation / position
                    j = i + 1
                    extras: list[str] = []
                    while j < len(row_cells) and len(row_cells[j]) == 1 and j - i <= 3:
                        extras.append(row_cells[j][0])
                        j += 1
                    for x in extras:
                        x_clean = re.sub(r"\s*他\s*\d+\s*名\s*$", "", x).strip()
                        if _looks_like_affiliation(x_clean) and not affiliation:
                            affiliation = x_clean
                        elif _looks_like_position(x_clean) and not position:
                            position = x_clean
                        elif not position and any(k in x_clean for k in POS_KEYWORDS):
                            position = x_clean
            if not name or not theme:
                i += 1
                continue
            out.append(
                {
                    "fiscal_year": fiscal_year,
                    "awardee_name": name,
                    "awardee_affiliation": affiliation,
                    "awardee_position": position,
                    "project_title": theme,
                    "award_amount": amount_jpy,
                    "program_name": current_program,
                    "source_url": url,
                    "metadata": {"foundation_slug": SLUG, "raw_amount_man": amount_cell},
                }
            )
            i += 1
    return out


def parse(years: list[int] | None = None, max_years: int = 3) -> list[dict]:
    if years is None:
        years = list(range(2026, 2026 - max_years, -1))
    records: list[dict] = []
    for year in years:
        for program_name, _slug, tmpl in PROGRAMS:
            url = tmpl.format(year=year)
            recs = _scrape_page(url, year, program_name)
            if recs:
                LOG.info("sumitomo %d %s -> %d", year, program_name, len(recs))
            records.extend(recs)
    return records
