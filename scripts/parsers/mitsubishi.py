"""Parser: 三菱財団 (Mitsubishi Foundation).

Source: https://www.mitsubishi-zaidan.jp/support/list.html
Per-year PDFs:
    list/<YYYY>-natural.pdf       (自然科学研究助成・一般)
    list/<YYYY>-natural-y.pdf     (自然科学・若手)
    list/<YYYY>-natural-sp.pdf    (自然科学・特別)
    list/<YYYY>-humanities.pdf    (人文科学研究助成)
    list/h<NN>-natural.pdf        (Heisei legacy)
    list/h<NN>-humanities.pdf

PDF row layout::

    <affiliation>             ← line 1
    <title-line-1>            ← line 2
    <num> <pref> <amount>円    ← line 3 (sometimes contains a fragment of title)
    <position> <title-line-2> ← line 4 (position prefix on this line)
    <name (kana)>             ← line 5

Strategy: rolling 5-line state machine that anchors on the
``<num> <prefecture> <amount>円`` line, then reaches forward/backward to
collect the surrounding rows.
"""
from __future__ import annotations

import io
import logging
import re
from typing import Iterable
from urllib.parse import urljoin

import pdfplumber
from bs4 import BeautifulSoup

from ..lib.http import fetch, fetch_text
from ..lib.normalize import normalize_text, parse_amount_jpy, heisei_to_western

LOG = logging.getLogger(__name__)
SLUG = "mitsubishi"
INDEX_URL = "https://www.mitsubishi-zaidan.jp/support/list.html"

NUM_PREF_AMOUNT_RE = re.compile(
    r"^\s*(\d{1,3})\s+(\S+?[都道府県])\s+(.*?)([\d,]+)\s*円\s*$"
)

POSITION_TOKENS = (
    "特任教授", "客員教授", "招聘教授", "招へい教授", "特命教授", "特定教授",
    "准教授", "特任准教授", "客員准教授", "教授",
    "助教授", "講師", "特任講師", "専任講師",
    "助教", "特任助教", "客員助教", "特定助教",
    "研究員", "特任研究員", "主任研究員", "主席研究員", "上級研究員", "博士研究員",
    "チームリーダー", "ユニット長", "プロジェクトリーダー",
    "室長", "部長", "センター長", "ディレクター", "フェロー",
    "学長", "副学長", "学部長", "館長", "所長", "副所長", "館員",
    "医員", "医師", "診療助教", "主任", "主席",
    "教諭", "校長", "副校長",
)
POS_RE = re.compile(r"^\s*(" + "|".join(map(re.escape, POSITION_TOKENS)) + r")(?:\s|$)")

NAME_KANA_RE = re.compile(r"^(?P<name>.+?)\s*[（(](?P<kana>[^（()]+)[)）]\s*$")

PROGRAM_NAME_RE = re.compile(r"【\s*(?P<name>.+?)\s*】")


def _discover_year_pdfs(html: str) -> list[tuple[int, str, str]]:
    """Return ``[(fiscal_year, kind, absolute_url)]``.

    ``kind`` ∈ {``natural``, ``natural-y``, ``natural-sp``, ``humanities``}.
    """
    soup = BeautifulSoup(html, "html.parser")
    out: list[tuple[int, str, str]] = []
    for a in soup.select("a[href$='.pdf']"):
        href = a.get("href", "")
        m = re.search(
            r"list/(?P<year>(?:h\d{1,2}|20\d{2}))-"
            r"(?P<kind>natural(?:-y|-sp)?|humanities)(?:-y|-sp)?\.pdf$",
            href,
        )
        if not m:
            continue
        ystr = m.group("year")
        if ystr.startswith("h"):
            fy = heisei_to_western(ystr) or 0
        else:
            fy = int(ystr)
        out.append((fy, m.group("kind"), urljoin(INDEX_URL, href)))
    return sorted(out, key=lambda t: (-t[0], t[1]))


def _iter_records_from_text(
    text: str,
    *,
    fiscal_year: int,
    program_name: str,
    source_url: str,
) -> Iterable[dict]:
    """Walk page text line-by-line, emitting one record per anchor line."""
    raw_lines = [l for l in text.split("\n") if l.strip()]
    lines = [normalize_text(l) for l in raw_lines]
    current_program = program_name
    i = 0
    while i < len(lines):
        line = lines[i]
        m = PROGRAM_NAME_RE.search(line)
        if m:
            current_program = f"{program_name} / {m.group('name').strip()}"
            i += 1
            continue
        anchor = NUM_PREF_AMOUNT_RE.match(line)
        if not anchor:
            i += 1
            continue
        # Anchor line: <num> <pref> [title-fragment] <amount>円
        amount = parse_amount_jpy(line)
        title_inline = anchor.group(3).strip()  # often empty
        # Look back two lines for affiliation + title-1
        ctx_back = lines[max(0, i - 4) : i]
        # Look forward for position+title-2 and name(kana)
        ctx_fwd = lines[i + 1 : min(len(lines), i + 6)]

        # Affiliation = nearest non-empty line before that doesn't start with
        # a position token and doesn't itself match an anchor pattern.
        affiliation = None
        title_lines: list[str] = []
        for prev in reversed(ctx_back):
            if NUM_PREF_AMOUNT_RE.match(prev):
                break
            if POS_RE.match(prev):
                continue
            if NAME_KANA_RE.match(prev):
                break
            # Heuristic: affiliation usually contains 大学 / 研究所 / センター /
            # 学校 / 機構 / 学院 / 法人.
            if re.search(r"大学|研究所|研究院|学院|機構|センター|博物館|学校|学園|協会|法人|病院", prev):
                affiliation = prev
                break
            else:
                title_lines.insert(0, prev)
        if title_inline:
            title_lines.append(title_inline)

        position = None
        name = None
        kana = None
        for fwd in ctx_fwd:
            pos_m = POS_RE.match(fwd)
            if pos_m and position is None:
                position = pos_m.group(1)
                rest = fwd[pos_m.end():].strip()
                if rest:
                    title_lines.append(rest)
                continue
            nk = NAME_KANA_RE.match(fwd)
            if nk:
                name = nk.group("name").strip()
                kana = nk.group("kana").strip()
                break
            # If line is neither position nor name(kana), it is a title fragment.
            if not NUM_PREF_AMOUNT_RE.match(fwd):
                title_lines.append(fwd)
        title = "".join(title_lines).strip()

        if name and title:
            yield {
                "fiscal_year": fiscal_year,
                "awardee_name": name,
                "awardee_affiliation": affiliation,
                "awardee_position": position,
                "project_title": title,
                "award_amount": amount,
                "program_name": current_program,
                "source_url": source_url,
                "metadata": {"kana": kana, "foundation_slug": SLUG},
            }
        i += 1


def _parse_pdf(
    pdf_bytes: bytes,
    fiscal_year: int,
    program_name: str,
    source_url: str,
) -> list[dict]:
    text_pages: list[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text_pages.append(page.extract_text() or "")
    full_text = "\n".join(text_pages)
    return list(
        _iter_records_from_text(
            full_text,
            fiscal_year=fiscal_year,
            program_name=program_name,
            source_url=source_url,
        )
    )


KIND_TO_PROGRAM = {
    "natural": "三菱財団 自然科学研究助成",
    "natural-y": "三菱財団 自然科学研究助成（若手）",
    "natural-sp": "三菱財団 自然科学研究助成（特別）",
    "humanities": "三菱財団 人文科学研究助成",
}


def parse(years: list[int] | None = None, max_years: int = 3) -> list[dict]:
    html = fetch_text(INDEX_URL, slug=SLUG)
    pairs = _discover_year_pdfs(html)
    if years:
        pairs = [t for t in pairs if t[0] in set(years)]
    else:
        # Most recent N distinct years.
        seen: list[int] = []
        kept: list[tuple[int, str, str]] = []
        for t in pairs:
            if t[0] not in seen:
                seen.append(t[0])
                if len(seen) > max_years:
                    break
            if t[0] in seen[:max_years]:
                kept.append(t)
        pairs = kept

    records: list[dict] = []
    for year, kind, url in pairs:
        if year <= 0:
            continue
        program_name = KIND_TO_PROGRAM.get(kind, f"三菱財団 {kind}")
        try:
            pdf_bytes = fetch(url, slug=SLUG, binary=True)
            recs = _parse_pdf(pdf_bytes, year, program_name, url)
            LOG.info("mitsubishi %d %s -> %d records", year, kind, len(recs))
            records.extend(recs)
        except Exception as exc:  # noqa: BLE001
            LOG.error("mitsubishi %d %s failed: %s", year, kind, exc)
    return records
