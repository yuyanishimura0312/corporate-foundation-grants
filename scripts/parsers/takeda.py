"""Parser: 武田科学振興財団 (Takeda Science Foundation).

Source: https://www.takeda-sci.or.jp/research/list.php
Year list PDFs: ``/research/doc/<year>_list.pdf`` (2012-2025).

Each PDF is structured as a sequence of program sections. Each section has:
    <program_name (heading line)>
    氏 名 / 所属機関・職位 / 研 究 題 目  (column header)
    <table rows>           ← parsed via pdfplumber.extract_tables()
    計NN件                  ← section terminator

Strategy:
- Discover year PDFs from the index page.
- For each PDF, extract per-page text to find section headers, then use
  ``page.extract_tables()`` to recover (name, affiliation+position, title)
  rows. Section header detection uses the line directly preceding the
  ``氏 名 ... 研 究 題 目`` header row, and pages that lack a fresh section
  inherit the previous section's program name.
"""
from __future__ import annotations

import io
import logging
import re
from typing import Iterator
from urllib.parse import urljoin

import pdfplumber
from bs4 import BeautifulSoup

from ..lib.http import fetch, fetch_text
from ..lib.normalize import normalize_text

LOG = logging.getLogger(__name__)
SLUG = "takeda"
INDEX_URL = "https://www.takeda-sci.or.jp/research/list.php"

# Map program names to amounts based on Takeda's published guidelines.
# Used as a fallback when a per-row amount cannot be parsed from the PDF.
PROGRAM_AMOUNT_HINTS = {
    "武田報彰医学研究助成": 30_000_000,
    "ハイリスク新興感染症研究助成": 10_000_000,
    "ハイリスク新興感染症研究": 10_000_000,
    "生命科学研究助成": 10_000_000,
    "医学系研究助成": 2_000_000,
    "医学系研究継続助成": 3_000_000,
    "薬学系研究助成": 2_000_000,
    "薬学系研究継続助成": 3_000_000,
    "ライフサイエンス研究助成": 2_000_000,
    "ライフサイエンス研究継続助成": 3_000_000,
    "特定研究助成": None,  # per-institution range (32-50M yen); table not parsed here
    "ビジョナリーリサーチ助成": 2_000_000,
    "ビジョナリーリサーチ継続助成(ホップ)": 5_000_000,
    "ビジョナリーリサーチ継続助成(ステップ)": 10_000_000,
    "ビジョナリーリサーチ継続助成(ジャンプ)": 30_000_000,
    "中学校・高等学校理科教育振興助成": 300_000,
    "理科教育振興助成": 300_000,
}
# FY2025 raised the science-education grant from 300k to 500k yen (published overview page).
PROGRAM_AMOUNT_BY_YEAR = {("理科教育振興助成", 2025): 500_000, ("中学校・高等学校理科教育振興助成", 2025): 500_000}


HEADER_RE = re.compile(r"氏\s*名\s+所属機関[・･]職位\s+研\s*究\s*題\s*目")
SECTION_TERMINATOR_RE = re.compile(r"計\s*\d+\s*件")
PROGRAM_TITLE_LINE_RE = re.compile(
    r"^(?:武田報彰医学研究助成|ハイリスク新興感染症研究(?:助成)?|"
    r"生命科学研究助成|医学系研究(?:継続)?助成|薬学系研究(?:継続)?助成|"
    r"ライフサイエンス研究(?:継続)?助成|特定研究助成|"
    r"ビジョナリーリサーチ(?:継続)?助成(?:.*)?|"
    r"(?:中学校[・･]高等学校)?理科教育振興助成)\s*(?:[（(].*?[)）])?\s*$"
)


def _discover_year_pdfs(html: str) -> list[tuple[int, str]]:
    """Return ``[(fiscal_year, absolute_url), ...]`` from the list.php page.

    Filename pattern: ``/research/doc/<YYYY>_list.pdf``.
    """
    soup = BeautifulSoup(html, "html.parser")
    out: list[tuple[int, str]] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        m = re.search(r"/(\d{4})_list\.pdf$", href)
        if not m:
            continue
        year = int(m.group(1))
        out.append((year, urljoin(INDEX_URL, href)))
    out.sort(key=lambda x: -x[0])
    return out


# Heading spellings differ between years; store one program name per program so a
# re-run does not create duplicate grant_programs rows.
PROGRAM_CANONICAL = {
    "ハイリスク新興感染症研究": "ハイリスク新興感染症研究助成",
    "中学校・高等学校理科教育振興助成": "理科教育振興助成",
}


def _amount_hint(program: str, year: int | None = None) -> int | None:
    # Longest key first so that e.g. the (ステップ)/(ホップ)/(ジャンプ) variants are not
    # swallowed by a shorter prefix (the old first-substring-wins loop gave all
    # continuation grants the same amount).
    for key in sorted(PROGRAM_AMOUNT_HINTS, key=len, reverse=True):
        if key in program:
            return PROGRAM_AMOUNT_BY_YEAR.get((key, year), PROGRAM_AMOUNT_HINTS[key])
    return None


def _split_affiliation_position(cell: str) -> tuple[str, str | None]:
    """Heuristically split affiliation cell into (affiliation, position).

    Takeda layout: affiliation may span multiple lines; the position keyword
    (教授/准教授/助教/講師/研究員/部長/室長/特任XX/客員XX/etc.) tends to land
    at the very end of the cell. We extract the trailing token after the last
    whitespace if it matches a known position marker.
    """
    text = re.sub(r"\s+", " ", normalize_text(cell))
    if not text:
        return "", None
    POS = (
        r"(?:特任|客員|招聘|招へい|特命|特定)?"
        r"(?:教授|准教授|助教(?:授)?|講師|研究員|主任研究員|主席研究員|"
        r"上級研究員|博士研究員|チームリーダー|ユニット長|室長|部長|"
        r"センター長|診療助教|医員|医師|主席?|主任|専任講師|"
        r"プロジェクトリーダー|グループリーダー|フェロー|ディレクター)"
    )
    m = re.search(rf"\s({POS}(?:\([^)]*\))?)\s*$", text)
    if m:
        position = m.group(1).strip()
        affiliation = text[: m.start()].strip()
        return affiliation, position
    return text, None


def _iter_sections(pdf_bytes: bytes) -> Iterator[tuple[str, list[list[str]]]]:
    """Iterate ``(program_name, rows)`` across pages.

    Each yielded ``rows`` is a list of ``[name, affiliation_cell, title_cell]``.

    The program of a table is the nearest program heading line *above it on the
    page* (by y position); a table with no heading above it on its page belongs
    to the last heading of the previous page.  Sub-headings such as
    ``<がん領域(基礎)>`` are not program headings.  (The earlier version took the
    line directly before the column header and stopped at the first match on a
    page, which mislabelled every table after a sub-heading and every second
    table on a page -- 2023-11-23 correction, see cfg/takeda-fix/notes.md.)
    """
    current_program: str | None = None
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            headings = []
            for ln in page.extract_text_lines():
                txt = normalize_text(ln["text"]).strip()
                if PROGRAM_TITLE_LINE_RE.match(txt):
                    headings.append((ln["top"], txt))
            tables = page.find_tables()
            for tb in sorted(tables, key=lambda x: x.bbox[1]):
                prog = None
                for top, txt in headings:
                    if top <= tb.bbox[1]:
                        prog = txt
                if prog is not None:
                    current_program = prog
                tbl = tb.extract()
                rows = [r for r in tbl if r and any((c or "").strip() for c in r)]
                if not rows:
                    continue
                # Awardee tables have exactly 3 columns
                # (氏名 / 所属機関・職位 / 研究題目). Skip stats / TOC tables
                # which have 4+ cols (e.g. プログラム名 / 応募件数 / 採択件数 / 採択率).
                if max(len(r) for r in rows) != 3:
                    continue
                header = rows[0]
                if header and HEADER_RE.search(" ".join(c or "" for c in header)):
                    rows = rows[1:]
                else:
                    # Tables without the awardee header are not awardee lists
                    # (e.g. 年度別実績テーブル at the end of the PDF).
                    continue
                if not rows:
                    continue
                if current_program is None:
                    current_program = "研究助成"
                yield current_program, rows
            # a heading below the last table of the page starts the next page's section
            if headings and (not tables or headings[-1][0] > max(t.bbox[1] for t in tables)):
                current_program = headings[-1][1]


def _parse_pdf(pdf_bytes: bytes, year: int, source_url: str) -> list[dict]:
    out: list[dict] = []
    for program, rows in _iter_sections(pdf_bytes):
        program = PROGRAM_CANONICAL.get(program, program)
        amount = _amount_hint(program, year)
        for row in rows:
            if len(row) < 3:
                continue
            name = normalize_text(row[0] or "")
            aff_cell = row[1] or ""
            title = normalize_text((row[2] or "").replace("\n", ""))
            if not name or not title:
                continue
            # Filter out section terminator stragglers.
            if SECTION_TERMINATOR_RE.fullmatch(name):
                continue
            affiliation, position = _split_affiliation_position(aff_cell)
            out.append(
                {
                    "fiscal_year": year,
                    "awardee_name": name,
                    "awardee_affiliation": affiliation or None,
                    "awardee_position": position,
                    "project_title": title,
                    "award_amount": amount,
                    "program_name": f"武田科学振興財団 {program}",
                    "source_url": source_url,
                    "metadata": {"section": program, "foundation_slug": SLUG},
                }
            )
    return out


def parse(years: list[int] | None = None, max_years: int = 5) -> list[dict]:
    """Return awardee records.

    Args:
        years: Specific fiscal years to fetch (e.g. ``[2024, 2023]``).
        max_years: When ``years`` is None, take the most recent N years.
    """
    html = fetch_text(INDEX_URL, slug=SLUG)
    pairs = _discover_year_pdfs(html)
    if years:
        pairs = [(y, u) for y, u in pairs if y in set(years)]
    else:
        pairs = pairs[:max_years]

    records: list[dict] = []
    for year, url in pairs:
        try:
            pdf_bytes = fetch(url, slug=SLUG, binary=True)
            recs = _parse_pdf(pdf_bytes, year, url)
            LOG.info("takeda %d -> %d records", year, len(recs))
            records.extend(recs)
        except Exception as exc:  # noqa: BLE001
            LOG.error("takeda %d failed: %s", year, exc)
    return records
