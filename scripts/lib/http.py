"""HTTP fetcher with on-disk caching, polite User-Agent and rate limiting.

Cache layout: ``ROOT/cache/awardees/<slug>/<basename>``
where basename mirrors the URL (path-encoded). The HTTP layer does not
parse content; binary responses (PDFs) are written byte-faithful.

Policy:
- Default rate limit: 1 request / 3 seconds (per process).
- Respects ``robots.txt`` via :mod:`urllib.robotparser` when ``check_robots``
  is True (default).
- 30s connect / 60s read timeout.
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
import time
import urllib.parse
import urllib.robotparser
from pathlib import Path
from typing import Optional

import requests

LOG = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[2]
CACHE_ROOT = ROOT / "cache" / "awardees"

USER_AGENT = (
    "MiratukuBot/1.0 (Foundation Grants Research; "
    "+contact:dialoguebar@gmail.com) python-requests"
)

DEFAULT_INTERVAL_SEC = 3.0
_last_request_at: dict[str, float] = {}
_robots_cache: dict[str, urllib.robotparser.RobotFileParser] = {}


def _safe_filename(url: str) -> str:
    """Map a URL to a stable filename (path + sha8 of full URL)."""
    parsed = urllib.parse.urlparse(url)
    base = os.path.basename(parsed.path) or "index.html"
    base = re.sub(r"[^A-Za-z0-9._-]", "_", base)
    h = hashlib.sha1(url.encode("utf-8")).hexdigest()[:8]
    stem, _, ext = base.rpartition(".")
    if not ext or len(ext) > 5:
        return f"{base}_{h}"
    return f"{stem}_{h}.{ext}"


def _robots_allows(url: str) -> bool:
    """Lightweight robots.txt check; failures fall back to permissive."""
    try:
        parsed = urllib.parse.urlparse(url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        rp = _robots_cache.get(origin)
        if rp is None:
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(f"{origin}/robots.txt")
            try:
                rp.read()
            except Exception as exc:  # noqa: BLE001
                LOG.debug("robots fetch failed for %s: %s", origin, exc)
                _robots_cache[origin] = rp  # cache empty
                return True
            _robots_cache[origin] = rp
        return rp.can_fetch(USER_AGENT, url)
    except Exception:  # noqa: BLE001
        return True


def _respect_rate_limit(host: str, interval: float) -> None:
    """Sleep so that this host has not been hit within ``interval`` seconds."""
    now = time.monotonic()
    last = _last_request_at.get(host, 0.0)
    delta = now - last
    if delta < interval:
        time.sleep(interval - delta)
    _last_request_at[host] = time.monotonic()


def fetch(
    url: str,
    *,
    slug: str,
    binary: bool = False,
    use_cache: bool = True,
    interval: float = DEFAULT_INTERVAL_SEC,
    check_robots: bool = True,
    timeout: tuple[int, int] = (30, 60),
) -> bytes:
    """Fetch a URL with caching. Returns the raw byte payload.

    Use ``binary=True`` for PDFs and other non-text resources (changes nothing
    about the bytes returned but is documented for clarity).
    """
    cache_dir = CACHE_ROOT / slug
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / _safe_filename(url)

    if use_cache and cache_path.exists() and cache_path.stat().st_size > 0:
        LOG.debug("cache hit %s", cache_path)
        return cache_path.read_bytes()

    if check_robots and not _robots_allows(url):
        raise PermissionError(f"robots.txt disallows fetching {url}")

    host = urllib.parse.urlparse(url).netloc
    _respect_rate_limit(host, interval)

    LOG.info("GET %s", url)
    resp = requests.get(
        url,
        headers={"User-Agent": USER_AGENT, "Accept-Language": "ja,en;q=0.8"},
        timeout=timeout,
        allow_redirects=True,
    )
    resp.raise_for_status()
    data = resp.content
    cache_path.write_bytes(data)
    return data


def fetch_text(url: str, *, slug: str, **kw) -> str:
    """Fetch and decode as UTF-8 text (best-effort)."""
    raw = fetch(url, slug=slug, **kw)
    # Most Japanese foundation sites are UTF-8; fall back gracefully.
    for enc in ("utf-8", "cp932", "shift_jis"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")
