"""Parser: 旭硝子財団 (Asahi Glass Foundation).

Source: https://www.af-info.or.jp/research/awardees.html
Per-year PDFs:
    research/assets/pdf/awardees/<YYYY>adoptionlist.pdf

Each PDF has clean tables (extractable via pdfplumber):
- Domestic awardees: 6 columns
    [No, 所属機関名, 職位, 氏名, 研究課題, 助成額(千円)]
- Overseas awardees: 5 columns
    [No, 所属機関名, 氏名, 研究課題, 上段千円/下段USD]

Section headers appear as page-level text like
    "(１) 化学・生命分野 研究奨励 ５１件"
captured between tables and used as program names.
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
SLUG = "asahi-glass"
INDEX_URL = "https://www.af-info.or.jp/research/awardees.html"

PDF_HREF_RE = re.compile(r"awardees/(\d{4})adoptionlist\.pdf$")

# Section line: "(１) 化学・生命分野 研究奨励 ５１件" — full-width digits in parens
SECTION_RE = re.compile(
    r"^[（(]\s*[０-９0-9]+\s*[）)]\s*(?P<name>.+?)\s*[０-９0-9]+\s*件\s*$"
)
# Overseas section: 大学名（国） NN件
OVERSEAS_RE = re.compile(r"^(?P<inst>[^\s（(]+(?:[（(][^)）]+[)）])?)\s+[０-９0-9]+\s*件\s*$")
# Header row of awardee tables
HEADER_DOMESTIC_RE = re.compile(r"所属機関名.*職位.*氏名.*研究課題")
HEADER_OVERSEAS_RE = re.compile(r"所属機関名.*氏名.*研究課題")


def _discover_year_pdfs(html: str) -> list[tuple[int, str]]:
    soup = BeautifulSoup(html, "html.parser")
    out: list[tuple[int, str]] = []
    for a in soup.select("a[href]"):
        m = PDF_HREF_RE.search(a.get("href", ""))
        if not m:
            continue
        year = int(m.group(1))
        url = urljoin(INDEX_URL, a["href"])
        out.append((year, url))
    out.sort(key=lambda t: -t[0])
    # de-dupe
    seen: set[int] = set()
    uniq: list[tuple[int, str]] = []
    for y, u in out:
        if y in seen:
            continue
        seen.add(y)
        uniq.append((y, u))
    return uniq


def _split_affiliation_position_from_cell(cell: str) -> tuple[str, str | None]:
    """Cell pattern: '東北大学\n大学院薬学研究科' (affiliation, no position)."""
    text = normalize_text(cell.replace("\n", " "))
    return text, None


def _to_amount_jpy(amount_thousand_yen: str) -> int | None:
    """Convert '3,000' (thousand yen) to 3_000_000 JPY."""
    if not amount_thousand_yen:
        return None
    s = amount_thousand_yen.replace(",", "").replace("，", "").strip()
    if not s.isdigit():
        return None
    return int(s) * 1000


def _iter_sections_and_tables(pdf_bytes: bytes) -> Iterator[tuple[str, str, list[list[str]]]]:
    """Yield (program_name, table_kind, rows) tuples.

    Strategy: assemble (top_y, kind, payload) records per page, ordered
    vertically. Each section header advances ``current_program`` for all
    subsequent tables on the same or later pages.

    table_kind: 'domestic' (6 cols) or 'overseas' (5 cols).
    """
    current_program = "助成研究"
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            # 1) Collect all section headers with their y position from page.chars
            #    extract_words gives per-line y positions; cheaper: use extract_text_lines
            page_events: list[tuple[float, str, object]] = []
            try:
                lines = page.extract_text_lines() or []
            except Exception:
                lines = []
            for ln in lines:
                txt = normalize_text(ln.get("text", ""))
                if not txt:
                    continue
                y = ln.get("top", 0.0)
                m = SECTION_RE.match(txt)
                if m:
                    page_events.append((y, "section", m.group("name").strip()))
                    continue
                if "海外研究助成" in txt and re.search(r"[０-９0-9]+\s*件", txt) is None:
                    page_events.append((y, "section", "海外研究助成"))
                    continue
                m2 = OVERSEAS_RE.match(txt)
                if m2:
                    inst = m2.group("inst")
                    page_events.append((y, "section", f"海外研究助成 / {inst}"))
                    continue

            # 2) Tables with their y position via page.find_tables()
            try:
                tables = page.find_tables() or []
            except Exception:
                tables = []
            for tbl in tables:
                bbox = tbl.bbox  # (x0, top, x1, bottom)
                top_y = bbox[1]
                rows = tbl.extract()
                if not rows:
                    continue
                rows = [r for r in rows if r and any((c or "").strip() for c in r)]
                if not rows:
                    continue
                page_events.append((top_y, "table", rows))

            # 3) Sort by y position (top to bottom)
            page_events.sort(key=lambda e: e[0])

            # 4) Walk events; tables inherit the latest section header
            for _, kind, payload in page_events:
                if kind == "section":
                    current_program = payload
                    continue
                rows = payload
                ncols = max(len(r) for r in rows)
                header_text = " ".join((c or "") for c in rows[0])
                has_domestic_header = HEADER_DOMESTIC_RE.search(header_text)
                has_overseas_header = HEADER_OVERSEAS_RE.search(header_text)
                # Continuation tables on pages 2+ may start directly with data
                # (no header row). Detect via first cell looking like a number.
                first_cell = (rows[0][0] or "").strip()
                is_data_first = first_cell.isdigit()
                if has_domestic_header and ncols >= 6:
                    yield current_program, "domestic", rows[1:]
                elif has_overseas_header and ncols >= 5:
                    yield current_program, "overseas", rows[1:]
                elif is_data_first and ncols >= 6:
                    yield current_program, "domestic", rows
                elif is_data_first and ncols == 5:
                    yield current_program, "overseas", rows


def _parse_pdf(pdf_bytes: bytes, year: int, source_url: str) -> list[dict]:
    out: list[dict] = []
    for program, kind, rows in _iter_sections_and_tables(pdf_bytes):
        for row in rows:
            cells = [normalize_text((c or "").replace("\n", " ")) for c in row]
            if kind == "domestic":
                if len(cells) < 6:
                    continue
                _, affiliation, position, name, title, amount_str = cells[:6]
                if not name or not title:
                    continue
                # Skip totals row (e.g. "計", or aggregated)
                if name in {"", "計", "合計"}:
                    continue
                out.append(
                    {
                        "fiscal_year": year,
                        "awardee_name": name,
                        "awardee_affiliation": affiliation or None,
                        "awardee_position": position or None,
                        "project_title": title,
                        "award_amount": _to_amount_jpy(amount_str),
                        "program_name": f"旭硝子財団 {program}",
                        "source_url": source_url,
                        "metadata": {"section": program, "foundation_slug": SLUG},
                    }
                )
            elif kind == "overseas":
                if len(cells) < 5:
                    continue
                _, affiliation, name, title, amount_str = cells[:5]
                if not name or not title:
                    continue
                if name in {"", "計", "合計"}:
                    continue
                # Overseas amount cell contains "上段千円 / 下段USD" - take first number.
                m = re.search(r"([\d,]+)", amount_str)
                amount_jpy = None
                if m:
                    digits = m.group(1).replace(",", "")
                    if digits.isdigit():
                        amount_jpy = int(digits) * 1000
                out.append(
                    {
                        "fiscal_year": year,
                        "awardee_name": name,
                        "awardee_affiliation": affiliation or None,
                        "awardee_position": None,
                        "project_title": title,
                        "award_amount": amount_jpy,
                        "program_name": f"旭硝子財団 {program}",
                        "source_url": source_url,
                        "metadata": {"section": program, "foundation_slug": SLUG, "overseas": True},
                    }
                )
    return out


def parse(years: list[int] | None = None, max_years: int = 3) -> list[dict]:
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
            LOG.info("asahi-glass %d -> %d records", year, len(recs))
            records.extend(recs)
        except Exception as exc:  # noqa: BLE001
            LOG.error("asahi-glass %d failed: %s", year, exc)
    return records
