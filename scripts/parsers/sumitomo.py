"""Parser: 住友財団 (Sumitomo Foundation) — STUB.

Source: https://www.sumitomo.or.jp/result.html
Status: Not implemented yet. Sumitomo publishes per-year PDFs by program
(基礎科学 / 環境 / 国際交流 / アジア諸国). Implementation should follow the
Mitsubishi parser pattern (per-program PDF + line state machine).
"""
from __future__ import annotations

import logging

LOG = logging.getLogger(__name__)
SLUG = "sumitomo"


def parse(years=None, max_years: int = 3):
    LOG.warning(
        "sumitomo parser is a stub. Implement against "
        "https://www.sumitomo.or.jp/result.html and return records."
    )
    return []
