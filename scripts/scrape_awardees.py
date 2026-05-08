#!/usr/bin/env python3
"""Scrape past awardees and call history from foundation websites.

Each foundation has a parser module under ``scripts/parsers/<slug>.py`` that
exposes a ``parse(years=None, max_years=N)`` function returning awardee
records. Records are upserted into ``grant_results`` (with ``grant_calls`` and
``grant_programs`` rows materialized lazily as needed).

Usage:
  python3 scripts/scrape_awardees.py --foundation takeda --year 2024
  python3 scripts/scrape_awardees.py --foundation takeda --max-years 3
  python3 scripts/scrape_awardees.py --all-priority1 --max-years 2
  python3 scripts/scrape_awardees.py --list
  python3 scripts/scrape_awardees.py --status
"""
from __future__ import annotations

import argparse
import importlib
import json
import logging
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

# Make ``scripts`` importable as a package so that ``scripts.parsers`` and
# ``scripts.lib`` resolve regardless of how the script is invoked.
THIS_FILE = Path(__file__).resolve()
SCRIPTS_DIR = THIS_FILE.parent
ROOT = SCRIPTS_DIR.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.lib.upsert import resolve_organization_id, upsert_results  # noqa: E402

DB = ROOT / "corporate_research_grants.sqlite"

LOG = logging.getLogger("scrape_awardees")


# Foundation registry. ``module`` is the parser module under scripts.parsers.
# ``org_patterns`` are tried (in order) against ``organizations.name`` LIKE
# matching to find the corresponding row.
PARSERS: dict[str, dict] = {
    "takeda": {
        "name": "武田科学振興財団",
        "url": "https://www.takeda-sci.or.jp/research/list.php",
        "module": "scripts.parsers.takeda",
        "org_patterns": ["武田科学振興財団", "武田科学"],
        "priority": 1,
        "note": "年度別採択者一覧PDFを提供、生命科学・医学領域、JFC top5",
    },
    "mitsubishi": {
        "name": "三菱財団",
        "url": "https://www.mitsubishi-zaidan.jp/support/list.html",
        "module": "scripts.parsers.mitsubishi",
        "org_patterns": ["三菱財団"],
        "priority": 1,
        "note": "自然科学・人文科学・社会福祉、各カテゴリPDF採択結果",
    },
    "inamori": {
        "name": "稲盛財団",
        "url": "https://www.inamori-f.or.jp/recipient/",
        "module": "scripts.parsers.inamori",
        "org_patterns": ["稲盛財団"],
        "priority": 1,
        "note": "JFC top12, 京都賞も運営",
    },
    "asahi-glass": {
        "name": "旭硝子財団",
        "url": "https://www.af-info.or.jp/research/awardees.html",
        "module": "scripts.parsers.asahi_glass",
        "org_patterns": ["旭硝子財団"],
        "priority": 1,
        "note": "自然科学・人文社会科学・環境、年度PDF表抽出",
    },
    "sumitomo": {
        "name": "住友財団",
        "url": "https://www.sumitomo.or.jp/",
        "module": "scripts.parsers.sumitomo",
        "org_patterns": ["住友財団"],
        "priority": 1,
        "note": "基礎科学・環境（年度別HTML、kisotaiYYYY/kantaisyoYYYY）",
    },
    "uehara": {
        "name": "上原記念生命科学財団",
        "url": "https://www.ueharazaidan.or.jp/grant/grantor.html",
        "module": "scripts.parsers.uehara",
        "org_patterns": ["上原記念生命科学財団"],
        "priority": 1,
        "note": "JFC top12、研究助成金/奨励金/海外留学等のPDF",
    },
    "secom": {
        "name": "セコム科学技術振興財団",
        "url": "https://www.secomzaidan.jp/kiroku.html",
        "module": "scripts.parsers.secom",
        "org_patterns": ["セコム科学技術振興財団"],
        "priority": 1,
        "note": "年度別HTML（kiroku_rNN/hNN）、2行ペア構造",
    },
}


def cmd_list() -> None:
    print(f"Registered foundations: {len(PARSERS)}\n")
    for slug, meta in sorted(PARSERS.items(), key=lambda x: x[1]["priority"]):
        mod_path = SCRIPTS_DIR / Path(*meta["module"].split(".")[1:]).with_suffix(".py")
        impl = "OK" if mod_path.exists() else "—"
        print(f"  P{meta['priority']} [{impl}] {slug:14} {meta['name']}")
        print(f"    {meta['url']}")
        print(f"    {meta['note']}")


def cmd_status() -> None:
    if not DB.exists():
        print(f"DB not found: {DB}")
        return
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM grant_results")
        total = cur.fetchone()[0]
        print(f"grant_results rows: {total}")
        cur.execute(
            """SELECT fiscal_year, COUNT(*) FROM grant_results
               WHERE fiscal_year IS NOT NULL
               GROUP BY fiscal_year ORDER BY fiscal_year DESC LIMIT 15"""
        )
        print("\nBy fiscal_year (top 15):")
        for y, c in cur.fetchall():
            print(f"  {y}: {c}")
        cur.execute(
            """SELECT o.name, COUNT(*) AS n
                 FROM grant_results r
                 JOIN grant_calls   c ON c.id = r.call_id
                 JOIN grant_programs p ON p.id = c.program_id
                 JOIN organizations o ON o.id = p.organization_id
                GROUP BY o.name ORDER BY n DESC LIMIT 15"""
        )
        print("\nBy foundation (top 15):")
        for name, n in cur.fetchall():
            print(f"  {n:5d}  {name}")
    except sqlite3.OperationalError as e:
        print(f"Error: {e}")
    conn.close()


def _run_parser(
    slug: str,
    years: list[int] | None,
    max_years: int,
    dry_run: bool,
) -> tuple[int, int, int]:
    """Run one parser and upsert its records.

    Returns ``(record_count, inserted, updated)``.
    """
    meta = PARSERS[slug]
    module_name = meta["module"]
    LOG.info("=== %s (%s) ===", slug, meta["name"])
    mod = importlib.import_module(module_name)
    if not hasattr(mod, "parse"):
        raise RuntimeError(f"{module_name} does not expose parse()")
    records = mod.parse(years=years, max_years=max_years)
    LOG.info("%s: collected %d records", slug, len(records))
    if dry_run:
        for r in records[:5]:
            LOG.info("  sample: %s", r)
        return len(records), 0, 0

    if not records:
        return 0, 0, 0
    # Always write a snapshot so a DB-locked failure does not lose work.
    snap_dir = ROOT / "cache" / "awardees" / slug
    snap_dir.mkdir(parents=True, exist_ok=True)
    snap = snap_dir / f"records_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    snap.write_text(json.dumps(records, ensure_ascii=False, indent=2))
    LOG.info("%s: snapshot %s", slug, snap)

    try:
        conn = sqlite3.connect(DB, timeout=300.0)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 300000")
        org_id = resolve_organization_id(conn, meta["org_patterns"])
        if not org_id:
            LOG.error(
                "%s: could not find organizations.id for patterns %s",
                slug, meta["org_patterns"],
            )
            conn.close()
            return len(records), 0, 0
        inserted, updated = upsert_results(conn, org_id, records)
        conn.close()
        LOG.info("%s: inserted=%d updated=%d", slug, inserted, updated)
        return len(records), inserted, updated
    except sqlite3.OperationalError as exc:
        LOG.error(
            "%s: DB write failed (%s). Records preserved at %s. "
            "Re-run with --import-snapshot to retry once DB is free.",
            slug, exc, snap,
        )
        return len(records), 0, 0


def cmd_import_snapshot(snapshot_path: Path) -> None:
    """Import a previously-written records JSON snapshot into ``grant_results``."""
    records = json.loads(snapshot_path.read_text())
    if not records:
        print("snapshot empty")
        return
    # Foundation slug is recorded in metadata.foundation_slug.
    slug = (records[0].get("metadata") or {}).get("foundation_slug")
    if slug not in PARSERS:
        print(f"unknown slug in snapshot: {slug}")
        sys.exit(1)
    meta = PARSERS[slug]
    conn = sqlite3.connect(DB, timeout=300.0)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 300000")
    org_id = resolve_organization_id(conn, meta["org_patterns"])
    if not org_id:
        print(f"organizations.id not found for {slug}")
        sys.exit(1)
    ins, upd = upsert_results(conn, org_id, records)
    conn.close()
    print(f"{slug}: inserted={ins} updated={upd}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Foundation awardees scraper")
    parser.add_argument("--list", action="store_true", help="List registered parsers")
    parser.add_argument("--status", action="store_true", help="Show DB coverage stats")
    parser.add_argument("--foundation", type=str, help="Run single parser by slug")
    parser.add_argument("--all-priority1", action="store_true", help="Run all priority-1 parsers")
    parser.add_argument(
        "--year", type=int, action="append",
        help="Restrict to specific fiscal year(s); can be passed multiple times.",
    )
    parser.add_argument("--max-years", type=int, default=3, help="Most recent N years (default 3)")
    parser.add_argument("--dry-run", action="store_true", help="Parse but do not upsert")
    parser.add_argument(
        "--import-snapshot", type=Path,
        help="Import a JSON snapshot produced by an earlier run into grant_results.",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.list:
        cmd_list()
        return
    if args.status:
        cmd_status()
        return
    if args.import_snapshot:
        cmd_import_snapshot(args.import_snapshot)
        return

    targets: list[str]
    if args.foundation:
        if args.foundation not in PARSERS:
            print(f"Unknown foundation: {args.foundation}")
            sys.exit(1)
        targets = [args.foundation]
    elif args.all_priority1:
        targets = [s for s, m in PARSERS.items() if m["priority"] == 1]
    else:
        parser.print_help()
        return

    summary: list[tuple[str, int, int, int]] = []
    for slug in targets:
        try:
            n, ins, upd = _run_parser(slug, args.year, args.max_years, args.dry_run)
            summary.append((slug, n, ins, upd))
        except Exception as exc:  # noqa: BLE001
            LOG.exception("%s failed: %s", slug, exc)
            summary.append((slug, 0, 0, 0))

    print("\n=== Summary ===")
    print(f"{'foundation':14}  records  inserted  updated")
    for slug, n, ins, upd in summary:
        print(f"{slug:14}  {n:7d}  {ins:8d}  {upd:7d}")


if __name__ == "__main__":
    main()
