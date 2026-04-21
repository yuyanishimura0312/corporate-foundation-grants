#!/usr/bin/env python3
"""
Collector: 公益法人information (koeki-info.go.jp)
Search for public interest foundations with research grant programs.

Strategy:
1. Search koeki-info.go.jp with category "学術・科学技術" + type "公益財団法人"
2. Extract foundation list with basic info
3. For each foundation, get available business reports (事業報告書)
4. Parse PDF reports for grant amounts

This script handles Step 1-2 (foundation discovery).
"""
import requests
import re
import json
import time
from pathlib import Path
from bs4 import BeautifulSoup

BASE_URL = "https://www.koeki-info.go.jp/pictis-info"
SEARCH_URL = f"{BASE_URL}/csa0001!show"
OUTPUT_DIR = Path(__file__).parent.parent / "data"
OUTPUT_FILE = OUTPUT_DIR / "koeki_foundations.json"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) research-bot/1.0",
    "Accept-Language": "ja,en;q=0.9",
})


def get_search_page():
    """Load search page and extract form tokens."""
    resp = SESSION.get(SEARCH_URL)
    resp.encoding = "utf-8"
    soup = BeautifulSoup(resp.text, "html.parser")

    # Extract hidden form fields
    form = soup.find("form")
    tokens = {}
    if form:
        for inp in form.find_all("input", {"type": "hidden"}):
            name = inp.get("name", "")
            val = inp.get("value", "")
            if name:
                tokens[name] = val

    return soup, tokens


def search_foundations(tokens, keyword="", category_code="01", page=1):
    """
    Search for public interest foundations.
    category_code: 01=学術・科学技術の振興
    """
    data = {**tokens}
    # Form parameters for search
    data.update({
        "event": "search",
        "keyword": keyword,
        "houjinKubun": "01",  # 公益財団法人
        "jigyouMokutekiCd": category_code,  # 学術・科学技術
    })

    resp = SESSION.post(SEARCH_URL, data=data)
    resp.encoding = "utf-8"
    return BeautifulSoup(resp.text, "html.parser")


def parse_search_results(soup):
    """Parse search results page to extract foundation list."""
    foundations = []
    # Look for result table rows
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) >= 3:
                name_cell = cells[0]
                link = name_cell.find("a")
                name = name_cell.get_text(strip=True)
                href = link.get("href", "") if link else ""

                # Extract other fields
                address = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                admin = cells[2].get_text(strip=True) if len(cells) > 2 else ""

                if "公益財団法人" in name or "財団" in name:
                    foundations.append({
                        "name": name,
                        "address": address,
                        "admin_agency": admin,
                        "detail_url": href,
                    })
    return foundations


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=== 公益法人information Collector ===")
    print("1. Loading search page...")
    soup, tokens = get_search_page()

    if not tokens:
        print("WARNING: No form tokens found. Site structure may have changed.")
        print("Page title:", soup.title.string if soup.title else "N/A")
        # Save page for debugging
        debug_file = OUTPUT_DIR / "koeki_search_debug.html"
        debug_file.write_text(str(soup), encoding="utf-8")
        print(f"Debug HTML saved to {debug_file}")
        return

    print(f"Found {len(tokens)} form tokens")
    print("2. Searching for 公益財団法人 with 学術・科学技術...")

    results_soup = search_foundations(tokens)
    foundations = parse_search_results(results_soup)

    print(f"Found {len(foundations)} foundations")

    # Save raw results for debugging
    debug_file = OUTPUT_DIR / "koeki_results_debug.html"
    debug_file.write_text(str(results_soup), encoding="utf-8")
    print(f"Results HTML saved to {debug_file}")

    if foundations:
        with open(OUTPUT_FILE, "w") as f:
            json.dump(foundations, f, ensure_ascii=False, indent=2)
        print(f"Saved to {OUTPUT_FILE}")

    for f in foundations[:10]:
        print(f"  {f['name']} | {f['address']} | {f['admin_agency']}")


if __name__ == "__main__":
    main()
