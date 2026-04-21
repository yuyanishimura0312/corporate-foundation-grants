#!/usr/bin/env python3
"""
Scrape 助成財団センター (jyosei-navi.jfc.or.jp) for research grant programs.
Uses Playwright to navigate Angular SPA.

Strategy: Search "研究助成" + each field category separately to stay under result limit,
then deduplicate.
"""
import json, time, re
from pathlib import Path
from playwright.sync_api import sync_playwright

OUTPUT_DIR = Path(__file__).parent.parent / "data"
OUTPUT_FILE = OUTPUT_DIR / "jfc_research_programs.json"

# Field categories (事業分野) with their checkbox indices
# idx 0-4: 事業形態1 (研究助成=0, 事業活動=1, 奨学=2, 表彰=3, その他=4)
# idx 5-18: 事業形態2
# idx 19-41: 事業分野
FIELD_CATEGORIES = [
    (19, "物理科学"),
    (20, "地球科学"),
    (21, "生命科学"),
    (22, "工学"),
    (23, "医学"),
    (24, "形式科学"),
    (25, "農学"),
    (26, "自然科学その他"),
    (27, "人文科学"),
    (28, "社会科学"),
    (29, "環境"),
    (30, "教育・スポーツ"),
    (31, "福祉"),
    (32, "保健・医療"),
    (33, "文化・芸術"),
    (34, "国際"),
    (35, "公共"),
    (36, "人権"),
    (37, "災害・防災"),
    (38, "就労支援"),
    (39, "地域開発"),
    (40, "起業支援"),
    (41, "その他分野"),
]


def extract_programs(page):
    """Extract program entries from current results page."""
    return page.evaluate("""() => {
        const entries = [];
        // Find all program links/entries in the result list
        const links = document.querySelectorAll('a');
        const programLinks = [];
        links.forEach(a => {
            const href = a.getAttribute('href') || '';
            if (href.includes('/assist/view/') || href.includes('/search/assist/')) {
                programLinks.push({
                    text: a.textContent.trim(),
                    href: a.href,
                    id: href.match(/(\\d+)/)?.[1] || '',
                });
            }
        });

        // Also try to extract from the page text structure
        const text = document.body.innerText;
        const lines = text.split('\\n').map(l => l.trim()).filter(l => l);

        // Look for program entries in the DOM
        const items = document.querySelectorAll('[class*="item"], [class*="list"] > div, [class*="result"] > div');
        const domItems = [];
        items.forEach(item => {
            const t = item.textContent.trim();
            if (t.length > 20 && t.length < 500) {
                domItems.push(t.substring(0, 200));
            }
        });

        return {programLinks, domItems: domItems.slice(0, 5), lineCount: lines.length};
    }""")


def search_and_collect(page, field_idx, field_name):
    """Navigate to search, select research grants + specific field, collect results."""
    page.goto("https://jyosei-navi.jfc.or.jp/search/", wait_until="networkidle")
    time.sleep(1)

    # Check 研究助成 (idx=0) + specific field
    page.evaluate(f"""() => {{
        const cbs = document.querySelectorAll('input[type="checkbox"]');
        cbs[0].click(); // 研究助成
        cbs[{field_idx}].click(); // {field_name}
    }}""")
    time.sleep(0.3)

    # Click この条件で検索する
    page.evaluate("""() => {
        const btns = document.querySelectorAll('button');
        btns[4].click();
    }""")
    page.wait_for_load_state("networkidle")
    time.sleep(2)

    # Get count
    count_text = page.evaluate("""() => {
        const m = document.body.innerText.match(/(\\d+)\\s*件/);
        return m ? m[1] : '0';
    }""")
    count = int(count_text)

    if count == 0:
        return []

    # Check if results are displayed or we need to re-search with fewer results
    # If "検索結果が多すぎます" appears, we can't get results
    too_many = page.evaluate("""() => {
        return document.body.innerText.includes('多すぎます');
    }""")

    if too_many:
        print(f"    Too many results ({count}), skipping display")
        return [{"field": field_name, "count": count, "status": "too_many"}]

    # Wait for results to render
    time.sleep(1)

    # Extract program data from the page
    programs = []
    page_num = 1

    while True:
        data = page.evaluate("""() => {
            const results = [];
            // Try multiple selector strategies for the Angular SPA

            // Strategy 1: Find links to program detail pages
            const links = document.querySelectorAll('a[href*="/assist/view/"], a[href*="/search/search/assist/view/"]');
            links.forEach(a => {
                const href = a.getAttribute('href') || '';
                const id = href.match(/(\\d+)$/)?.[1] || '';
                const text = a.textContent.trim();
                if (text && id) {
                    results.push({name: text, id: id, href: a.href});
                }
            });

            // Strategy 2: Parse from structured DOM
            if (results.length === 0) {
                const containers = document.querySelectorAll('.search-result-list .item, .program-list .item, [class*="result-item"]');
                containers.forEach(c => {
                    const link = c.querySelector('a');
                    results.push({
                        name: (link || c).textContent.trim().substring(0, 100),
                        id: link ? (link.href.match(/(\\d+)$/)?.[1] || '') : '',
                        href: link ? link.href : '',
                    });
                });
            }

            // Strategy 3: Parse from visible text
            if (results.length === 0) {
                const bodyText = document.body.innerText;
                // Look for org name pattern followed by program name
                const sections = bodyText.split(/(?=公益財団法人|一般財団法人|公益社団法人)/);
                sections.forEach(sec => {
                    const name = sec.split('\\n')[0]?.trim();
                    if (name && name.length > 6 && name.length < 100) {
                        results.push({name, id: '', href: ''});
                    }
                });
            }

            // Check for next page
            const hasNext = !!document.querySelector('a[aria-label="Next"], [class*="next"], a:has-text("次")');

            return {results, hasNext, resultText: document.body.innerText.substring(0, 2000)};
        }""")

        if data["results"]:
            for r in data["results"]:
                r["field"] = field_name
            programs.extend(data["results"])

        # Try to find program info from text if no links found
        if not data["results"] and page_num == 1:
            # Parse the text directly
            text = data.get("resultText", "")
            orgs = re.findall(r'((?:公益|一般)(?:財団|社団)法人\S+)', text)
            for org in orgs:
                programs.append({"name": org, "id": "", "href": "", "field": field_name})

        if not data.get("hasNext") or page_num >= 20:
            break

        # Click next
        try:
            page.click('a[aria-label="Next"], [class*="next"]', timeout=3000)
            page.wait_for_load_state("networkidle")
            time.sleep(1)
            page_num += 1
        except:
            break

    return programs


def get_program_detail(page, url):
    """Get detailed info about a specific program."""
    try:
        page.goto(url, wait_until="networkidle", timeout=15000)
        time.sleep(1)

        return page.evaluate("""() => {
            const text = document.body.innerText;
            const result = {};

            // Extract fields
            const fields = ['団体名', '事業名', '助成金額', '助成件数', '募集期間',
                          '事業分野', '事業形態', '事業内容', '応募資格', 'URL'];

            for (const field of fields) {
                const regex = new RegExp(field + '[：:\\s]+(.+?)(?=\\n|$)');
                const match = text.match(regex);
                if (match) result[field] = match[1].trim();
            }

            // Also get full text for parsing
            result._fullText = text.substring(0, 3000);
            return result;
        }""")
    except Exception as e:
        return {"error": str(e)}


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    all_programs = []
    field_counts = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )
        page = context.new_page()

        print("=== 助成財団センター Scraper ===")
        print(f"Searching 研究助成 across {len(FIELD_CATEGORIES)} field categories...\n")

        for idx, (cb_idx, field_name) in enumerate(FIELD_CATEGORIES):
            print(f"[{idx+1}/{len(FIELD_CATEGORIES)}] {field_name}...", end=" ", flush=True)

            try:
                programs = search_and_collect(page, cb_idx, field_name)
                field_counts[field_name] = len(programs)
                all_programs.extend(programs)
                print(f"{len(programs)} programs")
            except Exception as e:
                print(f"ERROR: {e}")

            time.sleep(1)  # Be polite

        # Deduplicate by program ID (if available) or name
        seen = set()
        unique_programs = []
        for prog in all_programs:
            key = prog.get("id") or prog.get("name", "")
            if key and key not in seen:
                seen.add(key)
                unique_programs.append(prog)

        print(f"\n=== Results ===")
        print(f"Total programs found: {len(all_programs)}")
        print(f"Unique programs: {len(unique_programs)}")
        print(f"\nBy field:")
        for field, count in sorted(field_counts.items(), key=lambda x: -x[1]):
            print(f"  {field}: {count}")

        # If we got program URLs, fetch details for a sample
        programs_with_urls = [p for p in unique_programs if p.get("href")]
        if programs_with_urls:
            print(f"\nFetching details for {min(20, len(programs_with_urls))} programs...")
            for prog in programs_with_urls[:20]:
                detail = get_program_detail(page, prog["href"])
                prog.update(detail)
                time.sleep(0.5)

        browser.close()

    # Save
    with open(OUTPUT_FILE, "w") as f:
        json.dump(unique_programs, f, ensure_ascii=False, indent=2)

    print(f"\nSaved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
