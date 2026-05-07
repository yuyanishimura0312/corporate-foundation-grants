"""pdfplumber wrapper for extracting awardee tables.

Extracts page text page-by-page so that parsers can apply foundation-specific
regex/state machines to recover (氏名 / 所属 / 課題 / 金額) tuples.
"""
from __future__ import annotations

import io
import logging
from typing import Iterator

import pdfplumber

LOG = logging.getLogger(__name__)


def extract_pages(pdf_bytes: bytes) -> Iterator[str]:
    """Yield extracted text per page (skipping empty pages)."""
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for i, page in enumerate(pdf.pages):
            try:
                text = page.extract_text() or ""
            except Exception as exc:  # noqa: BLE001
                LOG.warning("page %d extract failed: %s", i, exc)
                text = ""
            if text.strip():
                yield text


def extract_all_text(pdf_bytes: bytes) -> str:
    """Concatenate all pages into one string with form-feed separators."""
    return "\f".join(extract_pages(pdf_bytes))
