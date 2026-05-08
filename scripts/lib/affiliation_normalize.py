"""Normalize affiliation strings (Japanese institution names)."""
from __future__ import annotations
import re

# Common university aliases → canonical name
UNIV_ALIAS = {
    "東大": "東京大学", "東京大学院": "東京大学", "京大": "京都大学",
    "阪大": "大阪大学", "東北大": "東北大学", "名大": "名古屋大学",
    "北大": "北海道大学", "九大": "九州大学", "東工大": "東京工業大学",
    "東京工業大": "東京工業大学", "早大": "早稲田大学", "慶大": "慶應義塾大学",
    "慶応": "慶應義塾大学", "慶應大学": "慶應義塾大学",
    "Tokyo University": "東京大学", "University of Tokyo": "東京大学",
    "Kyoto University": "京都大学", "Osaka University": "大阪大学",
    "Tohoku University": "東北大学", "Nagoya University": "名古屋大学",
}

# Suffix patterns to strip (research center, graduate school, faculty)
SUFFIX_PATTERNS = [
    r"大学院.*$", r"大学院[\s].*$",
    r"研究科.*$", r"医学部.*$", r"工学部.*$", r"理学部.*$", r"農学部.*$",
    r"教育学部.*$", r"文学部.*$", r"経済学部.*$", r"法学部.*$",
    r"総合.*研究科.*$", r"研究院.*$", r"院.*$",
]
PREFIX_PATTERNS = [
    r"^国立大学法人\s*", r"^公立大学法人\s*", r"^学校法人\s*",
    r"^独立行政法人\s*", r"^国立研究開発法人\s*",
]


def normalize_affiliation(name: str) -> str:
    """Return a canonical institution name."""
    if not name:
        return ""
    s = name.strip()
    # Strip prefixes
    for p in PREFIX_PATTERNS:
        s = re.sub(p, "", s).strip()
    # Apply alias mapping (longest match first)
    for alias in sorted(UNIV_ALIAS.keys(), key=len, reverse=True):
        if alias in s:
            return UNIV_ALIAS[alias]
    # If contains 大学, try to extract just the university name (strip suffixes)
    if "大学" in s and not any(s.endswith(suf) for suf in ["大学", "大学院"]):
        # Extract up to "大学" + optional 院
        m = re.match(r"(.+?大学(?:院)?)", s)
        if m:
            base = m.group(1)
            # If base ends with 大学院, return without 院
            if base.endswith("大学院"):
                base = base[:-1]
            return base
    # Otherwise, strip suffix patterns
    for p in SUFFIX_PATTERNS:
        s = re.sub(p, "", s).strip()
    return s.strip()
