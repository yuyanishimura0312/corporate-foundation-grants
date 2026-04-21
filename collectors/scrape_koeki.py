#!/usr/bin/env python3
"""
Scrape all 5,674 public interest foundations from koeki-info.go.jp
using Playwright, then filter for corporate research grant foundations.
"""
import json, time, re, sys
from pathlib import Path
from playwright.sync_api import sync_playwright

OUTPUT_DIR = Path(__file__).parent.parent / "data"
OUTPUT_FILE = OUTPUT_DIR / "koeki_all_foundations.json"
FILTERED_FILE = OUTPUT_DIR / "koeki_research_foundations.json"

# Keywords indicating research grant activity in foundation purpose
RESEARCH_KEYWORDS = [
    "研究", "学術", "科学", "助成", "奨励", "振興", "技術",
    "医学", "医療", "薬学", "バイオ", "環境", "工学",
    "教育", "奨学", "留学",
]

# Keywords indicating corporate origin in foundation name
CORPORATE_KEYWORDS = [
    "トヨタ", "マツダ", "日産", "ホンダ", "ソニー", "パナソニック",
    "日立", "東芝", "NEC", "富士通", "キヤノン", "村田",
    "武田", "アステラス", "小野薬品", "テルモ", "花王", "小林製薬",
    "三菱", "住友", "野村", "大和証券", "みずほ", "りそな",
    "日本生命", "太陽生命", "大同生命", "SOMPO", "損保", "セコム",
    "明治安田", "丸紅", "三井", "ニッポンハム", "サントリー",
    "アサヒ", "ロッテ", "稲盛", "旭硝子", "AGC", "電通",
    "JKA", "ＪＫＡ", "JR", "ＪＲ", "NEXCO", "ENEOS",
    "ローム", "ホソカワ", "エフピコ", "大塚", "ベネッセ",
    "PwC", "デロイト", "SMBC", "全国銀行", "日本郵便", "ゆうちょ",
    "コスメトロジー", "鹿島", "清水建設", "大林", "竹中",
    "リクルート", "Yahoo", "楽天", "ソフトバンク", "NTT",
    "KDDI", "日本電気", "オムロン", "ファナック", "キーエンス",
    "ブリヂストン", "デンソー", "豊田", "TOTO", "LIXIL",
    "三島海雲", "岩谷", "倉田", "鉄鋼", "軽金属",
    "長瀬", "加藤", "中谷", "先進医薬", "牧誠",
    "萩原", "齋藤", "白珪", "大川", "市村", "杉浦",
    "セゾン", "ヤマハ", "カシオ", "ニコン", "オリンパス",
    "資生堂", "コニカ", "富士フイルム", "日本ペイント",
    "住友電工", "古河", "信越", "東レ", "帝人",
    "旭化成", "三菱ケミカル", "三菱重工", "IHI", "川崎重工",
    "日本製鉄", "JFE", "神戸製鋼", "日揮", "千代田化工",
]


def extract_entries_from_page(page):
    """Extract foundation entries from the current search results page."""
    return page.evaluate("""() => {
        const text = document.body.innerText;
        const entries = [];

        // Find all foundation entry blocks using the detail link pattern
        const detailBtns = document.querySelectorAll('a, button');
        const detailPositions = [];
        detailBtns.forEach(el => {
            if (el.textContent.trim() === '詳細') {
                detailPositions.push(el);
            }
        });

        // Parse each entry from the full text
        // Split on foundation name pattern (starts at line beginning)
        const lines = text.split('\\n');
        let current = null;

        for (let i = 0; i < lines.length; i++) {
            const line = lines[i].trim();

            // New foundation entry
            if (/^公益財団法[人⼈]/.test(line) && line.length > 6 && !line.match(/^公益財団法[人⼈]\\d/)) {
                if (current && current.name) {
                    entries.push(current);
                }
                current = {name: line.replace(/\\s+/g, ''), purpose: '', admin: '', address: ''};
            }

            if (!current) continue;

            // Parse fields
            if (line.startsWith('法人の目的')) {
                current.purpose = line.replace('法人の目的', '').trim();
            }
            if (line.match(/^行政庁/)) {
                const parts = line.split('\\t');
                for (let j = 0; j < parts.length; j++) {
                    if (parts[j] === '行政庁' && j+1 < parts.length) current.admin = parts[j+1].trim();
                    if (parts[j] === '住所' && j+1 < parts.length) current.address = parts[j+1].trim();
                }
            }
        }
        if (current && current.name) entries.push(current);

        return entries;
    }""")


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    all_foundations = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )
        page = context.new_page()

        print("1. Loading search page...")
        page.goto("https://www.koeki-info.go.jp/pictis-info/csa0001!show", timeout=30000)
        page.wait_for_load_state("networkidle")

        print("2. Setting search conditions (公益財団法人)...")
        # Open 検索条件2 法人区分
        page.click("text=検索条件2")  # Enable condition
        page.click("text=開く >> nth=0")  # Open section
        time.sleep(0.5)

        # Check 公益財団法人 checkbox
        page.evaluate("""() => {
            const cbs = document.querySelectorAll('input[name="corpDivision"]');
            cbs.forEach(cb => { if (cb.value === '20') cb.checked = true; });
        }""")

        print("3. Searching...")
        page.click("button:has-text('検索') >> nth=1")
        page.wait_for_load_state("networkidle")
        time.sleep(1)

        # Get total count
        total_text = page.evaluate("() => document.body.innerText.match(/公益財団法人\\n(\\d+)/)?.[1] || '0'")
        total = int(total_text)
        print(f"   Found {total} foundations")

        print("4. Loading first results page...")
        page.click("button:has-text('一覧表示') >> nth=1")
        page.wait_for_load_state("networkidle")
        time.sleep(1)

        # Set display to 100 per page
        page.evaluate("""() => {
            const sel = document.querySelector('select, [class*="count"]');
            if (sel) { sel.value = '100'; sel.dispatchEvent(new Event('change')); }
        }""")
        time.sleep(0.5)

        total_pages = (total + 99) // 100
        print(f"   {total_pages} pages to scrape (100 per page)")

        for page_num in range(1, total_pages + 1):
            print(f"   Page {page_num}/{total_pages}...", end=" ", flush=True)

            entries = extract_entries_from_page(page)
            all_foundations.extend(entries)
            print(f"{len(entries)} entries (total: {len(all_foundations)})")

            # Save progress every 10 pages
            if page_num % 10 == 0:
                with open(OUTPUT_FILE, "w") as f:
                    json.dump(all_foundations, f, ensure_ascii=False, indent=2)

            # Navigate to next page
            if page_num < total_pages:
                try:
                    page.click("text=次のページ", timeout=5000)
                    page.wait_for_load_state("networkidle")
                    time.sleep(1)
                except Exception as e:
                    print(f"   Navigation error: {e}")
                    break

        browser.close()

    # Save all foundations
    print(f"\n5. Saving {len(all_foundations)} foundations...")
    with open(OUTPUT_FILE, "w") as f:
        json.dump(all_foundations, f, ensure_ascii=False, indent=2)

    # Filter for research-related corporate foundations
    research_foundations = []
    for f in all_foundations:
        purpose = f.get("purpose", "")
        name = f.get("name", "")

        is_research = any(kw in purpose or kw in name for kw in RESEARCH_KEYWORDS)
        is_corporate = any(kw in name for kw in CORPORATE_KEYWORDS)

        if is_research:
            f["is_corporate"] = is_corporate
            f["research_score"] = sum(1 for kw in RESEARCH_KEYWORDS if kw in purpose or kw in name)
            research_foundations.append(f)

    # Sort by research relevance
    research_foundations.sort(key=lambda x: -x["research_score"])

    with open(FILTERED_FILE, "w") as f:
        json.dump(research_foundations, f, ensure_ascii=False, indent=2)

    corporate_research = [f for f in research_foundations if f["is_corporate"]]

    print(f"\n=== Results ===")
    print(f"Total foundations: {len(all_foundations)}")
    print(f"Research-related: {len(research_foundations)}")
    print(f"Corporate research: {len(corporate_research)}")
    print(f"\nSaved: {OUTPUT_FILE}")
    print(f"Filtered: {FILTERED_FILE}")

    # Show top corporate research foundations
    print(f"\n--- Top Corporate Research Foundations ---")
    for f in corporate_research[:20]:
        print(f"  [{f['research_score']}] {f['name']}")
        print(f"      {f['purpose'][:80]}...")
        print()


if __name__ == "__main__":
    main()
