#!/usr/bin/env python3
"""
Scrape past awardees and call history from foundation websites.

This is a scaffold for incremental build-out of grant_results data.
Each foundation gets its own parser registered in PARSERS dict.

Usage:
  python3 scripts/scrape_awardees.py --foundation takeda
  python3 scripts/scrape_awardees.py --all
  python3 scripts/scrape_awardees.py --list

Implementation strategy:
  - Use requests + BeautifulSoup (no JavaScript rendering needed for most foundations)
  - Cache fetched HTML to ~/projects/apps/corporate-foundation-grants/cache/awardees/
  - Insert into grant_results table (call_id resolved via fuzzy match on program name + year)
  - Each parser returns: List[{fiscal_year, awardee_name, awardee_affiliation, project_title, award_amount}]
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "corporate_research_grants.sqlite"
CACHE = ROOT / "cache" / "awardees"

# Mapping: foundation_slug -> (foundation_name_pattern, parser_fn)
# Parsers should be implemented incrementally per foundation.
# The 13 foundations below are top JFC-ranked + best-documented public awardee lists.

PARSERS: dict[str, dict] = {
    "takeda": {
        "name": "武田科学振興財団",
        "url": "https://www.takeda-sci.or.jp/business/award.php",
        "note": "年度別採択者一覧PDFを提供、生命科学・医学領域、JFC top5",
        "priority": 1,
    },
    "mitsubishi": {
        "name": "三菱財団",
        "url": "https://www.mitsubishi-zaidan.jp/grants/results/",
        "note": "自然科学・人文科学・社会福祉、各カテゴリPDF採択結果",
        "priority": 1,
    },
    "inamori": {
        "name": "稲盛財団",
        "url": "https://www.inamori-f.or.jp/research_grant/results/",
        "note": "JFC top12, 京都賞も運営",
        "priority": 1,
    },
    "asahi-glass": {
        "name": "旭硝子財団",
        "url": "https://af-info.or.jp/research/result.html",
        "note": "自然科学・人文社会科学・環境",
        "priority": 1,
    },
    "sumitomo": {
        "name": "住友財団",
        "url": "https://www.sumitomo.or.jp/result.html",
        "note": "基礎科学・環境・国際交流",
        "priority": 1,
    },
    "toyota": {
        "name": "トヨタ財団",
        "url": "https://www.toyotafound.or.jp/grant_results/",
        "note": "国際助成・研究助成、社会課題志向",
        "priority": 2,
    },
    "secom": {
        "name": "セコム科学技術振興財団",
        "url": "https://www.secomzaidan.jp/result.html",
        "note": "JFC上位、年間助成5億超",
        "priority": 2,
    },
    "kazima": {
        "name": "鹿島学術振興財団",
        "url": "https://www.kajima-f.or.jp/grant-projects/research-grant/result/",
        "note": "全学術分野、最大500万/件",
        "priority": 2,
    },
    "shimadzu": {
        "name": "島津科学技術振興財団",
        "url": "https://www.shimadzu.co.jp/aboutus/ssf/",
        "note": "計測科学・分析化学",
        "priority": 2,
    },
    "ueno": {
        "name": "上原記念生命科学財団",
        "url": "https://www.ueharazaidan.or.jp/result/",
        "note": "生命科学領域、JFC上位",
        "priority": 2,
    },
    "nakatani": {
        "name": "中谷医工計測技術振興財団",
        "url": "https://www.nakatani-foundation.jp/result.html",
        "note": "医工計測、JFC top21",
        "priority": 2,
    },
    "telmo": {
        "name": "テルモ生命科学振興財団",
        "url": "https://www.terumozaidan.or.jp/result/",
        "note": "生命科学、JFC top53",
        "priority": 3,
    },
    "ihi": {
        "name": "IHI若手研究助成",
        "url": "https://www.ihi.co.jp/csr/foundation/",
        "note": "工学系若手、example実装テスト用",
        "priority": 3,
    },
}


def cmd_list():
    """List registered parsers."""
    print(f"Registered foundations: {len(PARSERS)}\n")
    for slug, meta in sorted(PARSERS.items(), key=lambda x: x[1]["priority"]):
        impl = "✓" if (Path(__file__).parent / "parsers" / f"{slug}.py").exists() else "—"
        print(f"  P{meta['priority']} [{impl}] {slug:20} {meta['name']}")
        print(f"    {meta['url']}")
        print(f"    {meta['note']}")


def cmd_status():
    """Show DB grant_results coverage."""
    if not DB.exists():
        print(f"DB not found: {DB}")
        return
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM grant_results")
        total = cur.fetchone()[0]
        print(f"grant_results rows: {total}")
        cur.execute("""
            SELECT fiscal_year, COUNT(*) FROM grant_results
            WHERE fiscal_year IS NOT NULL
            GROUP BY fiscal_year ORDER BY fiscal_year DESC LIMIT 10
        """)
        print("\nBy fiscal_year (top 10):")
        for y, c in cur.fetchall():
            print(f"  {y}: {c}")
    except sqlite3.OperationalError as e:
        print(f"Error: {e}")
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Foundation awardees scraper")
    parser.add_argument("--list", action="store_true", help="List registered parsers")
    parser.add_argument("--status", action="store_true", help="Show DB coverage stats")
    parser.add_argument("--foundation", type=str, help="Run single parser by slug")
    parser.add_argument("--all", action="store_true", help="Run all priority-1 parsers")
    args = parser.parse_args()

    if args.list:
        cmd_list()
        return
    if args.status:
        cmd_status()
        return
    if args.foundation or args.all:
        print("ERROR: parser implementations are not yet wired.")
        print("Implement parsers/<slug>.py per the foundation pages, then register a callable.")
        print("See cmd_list() for documented foundations and starter URLs.")
        sys.exit(1)
    parser.print_help()


if __name__ == "__main__":
    main()
