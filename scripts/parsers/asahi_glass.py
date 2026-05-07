"""Parser: 旭硝子財団 (Asahi Glass Foundation) — STUB.

Source: https://af-info.or.jp/research/result.html
Status: Not implemented yet. The site mixes per-program PDFs and HTML tables;
once the parsing strategy is finalized, replace ``parse()`` below with the
real implementation following the Mitsubishi/Takeda pattern.
"""
from __future__ import annotations

import logging

LOG = logging.getLogger(__name__)
SLUG = "asahi-glass"


def parse(years=None, max_years: int = 3):
    LOG.warning(
        "asahi-glass parser is a stub. Implement against "
        "https://af-info.or.jp/research/result.html and return records."
    )
    return []
