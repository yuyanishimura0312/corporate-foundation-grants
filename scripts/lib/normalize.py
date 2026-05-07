"""Text normalization helpers for awardee records."""
from __future__ import annotations

import re
import unicodedata


_AMOUNT_RE = re.compile(r"([0-9,，]+)\s*円")


def normalize_text(s: str) -> str:
    """NFKC normalize, collapse whitespace, trim."""
    if s is None:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_amount_jpy(s: str) -> int | None:
    """Parse '5,000,000円' -> 5000000. Returns None on failure."""
    if not s:
        return None
    m = _AMOUNT_RE.search(s)
    if not m:
        return None
    digits = m.group(1).replace(",", "").replace("，", "")
    try:
        return int(digits)
    except ValueError:
        return None


def normalize_affiliation(s: str) -> str:
    """Light cleanup of affiliation strings (e.g. "東 京 大 学" -> "東京大学")."""
    s = normalize_text(s)
    # Heuristic: drop trailing position words for separate `position` field.
    return s


def heisei_to_western(year_str: str) -> int | None:
    """Convert "h24" / "平成24" / "令和2" / "2020" to a fiscal-year int."""
    s = normalize_text(year_str).lower()
    m = re.match(r"^(20\d\d|19\d\d)$", s)
    if m:
        return int(m.group(1))
    m = re.match(r"^h(\d{1,2})$", s) or re.match(r"^平成(\d{1,2})$", s)
    if m:
        return 1988 + int(m.group(1))
    m = re.match(r"^r(\d{1,2})$", s) or re.match(r"^令和(\d{1,2})$", s)
    if m:
        return 2018 + int(m.group(1))
    return None
