"""Parser: 上原記念生命科学財団 (Uehara Memorial Foundation).

Source: https://www.ueharazaidan.or.jp/grant/grantor.html
Per-year PDFs (most recent 2015-2025):
    /include/img/past/<YYYY>_joseikin.pdf       研究助成金 (q5,000,000 / 件)
    /include/img/past/<YYYY>_suishin.pdf        研究推進特別奨励金
    /include/img/past/<YYYY>_shoreikin.pdf      研究奨励金
    /include/img/past/<YYYY>_sympo.pdf          国際シンポジウム開催助成金

Each PDF has 4-column tables:
    [研究者名, 所属機関, 役職, 研究テーマ]
The header (一行目) repeats on every page.
Some PDFs (kaigai etc.) live under /include/img/<YYYY>NN/<YYYY>kaigai.pdf —
the directory year encodes the grant year (e.g. 2026/04/2025kaigai.pdf).
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
SLUG = "uehara"
INDEX_URL = "https://www.ueharazaidan.or.jp/grant/grantor.html"

# Map filename suffix → program name + per-grant amount (JPY) when fixed.
PROGRAM_INFO = {
    "joseikin": ("研究助成金", 5_000_000),
    "suishin": ("研究推進特別奨励金", None),
    "shoreikin": ("研究奨励金", None),
    "sympo": ("国際シンポジウム開催助成金", None),
    "kaigai": ("海外留学助成金", None),
    "wakate-kaigai": ("若手海外留学支援金", None),
    "rainichi": ("来日研究生助成金", None),
}

# Match URL patterns
PDF_RE = re.compile(
    r"/include/img/(?:past|\d{4}/\d{2})/(?P<year>\d{4})[_-]?(?P<kind>joseikin|suishin|shoreikin|sympo|kaigai|wakate-kaigai|rainichi)\.pdf$",
    re.IGNORECASE,
)
# Section header: "（A）領域" "（B）領域" etc — used as a sub-program label.
SECTION_RE = re.compile(r"^[（(]\s*([A-Z])\s*[)）]\s*領域\s*$")


def _discover_pdfs(html: str) -> list[tuple[int, str, str]]:
    """Return ``[(fiscal_year, kind, absolute_url), ...]``."""
    soup = BeautifulSoup(html, "html.parser")
    out: list[tuple[int, str, str]] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        m = PDF_RE.search(href)
        if not m:
            continue
        year = int(m.group("year"))
        kind = m.group("kind").lower()
        url = urljoin(INDEX_URL, href)
        out.append((year, kind, url))
    out.sort(key=lambda t: (-t[0], t[1]))
    # de-dupe (year, kind)
    seen: set[tuple[int, str]] = set()
    uniq: list[tuple[int, str, str]] = []
    for y, k, u in out:
        if (y, k) in seen:
            continue
        seen.add((y, k))
        uniq.append((y, k, u))
    return uniq


def _parse_pdf(
    pdf_bytes: bytes,
    year: int,
    program_name: str,
    fixed_amount: int | None,
    source_url: str,
) -> list[dict]:
    out: list[dict] = []
    section: str | None = None
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            # Find section header from page text
            text = page.extract_text() or ""
            for ln in text.split("\n"):
                ln = normalize_text(ln)
                m = SECTION_RE.match(ln)
                if m:
                    section = m.group(1)
                    break
            try:
                tables = page.find_tables() or []
            except Exception:
                tables = []
            for tbl in tables:
                rows = tbl.extract() or []
                rows = [r for r in rows if r and any((c or "").strip() for c in r)]
                if not rows:
                    continue
                # Skip header row if present
                first = " ".join((c or "") for c in rows[0])
                if "研究者名" in first and "所属機関" in first:
                    rows = rows[1:]
                for row in rows:
                    cells = [normalize_text((c or "").replace("\n", " ")) for c in row]
                    if len(cells) < 4:
                        continue
                    name, affiliation, position, title = cells[:4]
                    if not name or not title:
                        continue
                    if name in {"研究者名"}:
                        continue
                    out.append(
                        {
                            "fiscal_year": year,
                            "awardee_name": name,
                            "awardee_affiliation": affiliation or None,
                            "awardee_position": position or None,
                            "project_title": title,
                            "award_amount": fixed_amount,
                            "program_name": f"上原記念生命科学財団 {program_name}"
                            + (f" ({section}領域)" if section else ""),
                            "source_url": source_url,
                            "metadata": {
                                "section": section,
                                "foundation_slug": SLUG,
                            },
                        }
                    )
    return out


def parse(years: list[int] | None = None, max_years: int = 3) -> list[dict]:
    html = fetch_text(INDEX_URL, slug=SLUG)
    pairs = _discover_pdfs(html)
    if years:
        target = set(years)
        pairs = [t for t in pairs if t[0] in target]
    else:
        # Pick the most recent N distinct years; include all kinds for each.
        seen: list[int] = []
        kept: list[tuple[int, str, str]] = []
        for t in pairs:
            if t[0] not in seen:
                if len(seen) >= max_years:
                    break
                seen.append(t[0])
            kept.append(t)
        pairs = kept

    records: list[dict] = []
    for year, kind, url in pairs:
        program_label, fixed_amount = PROGRAM_INFO.get(kind, (kind, None))
        try:
            pdf_bytes = fetch(url, slug=SLUG, binary=True)
            recs = _parse_pdf(pdf_bytes, year, program_label, fixed_amount, url)
            LOG.info("uehara %d %s -> %d", year, kind, len(recs))
            records.extend(recs)
        except Exception as exc:  # noqa: BLE001
            LOG.error("uehara %d %s failed: %s", year, kind, exc)
    return records
