#!/usr/bin/env python3
"""
Scrape 助成財団センター by searching each corporate foundation by name.
Uses the 団体名 search field to find programs for known foundations.
"""
import json, time, re, sqlite3
from pathlib import Path
from playwright.sync_api import sync_playwright

DB_PATH = Path(__file__).parent.parent / "corporate_research_grants.sqlite"
OUTPUT_DIR = Path(__file__).parent.parent / "data"
OUTPUT_FILE = OUTPUT_DIR / "jfc_matched_programs.json"


def get_foundation_names():
    """Get all foundation names from our DB."""
    db = sqlite3.connect(str(DB_PATH))
    rows = db.execute("SELECT name FROM organizations").fetchall()
    db.close()
    names = []
    for r in rows:
        # Clean name for search (remove 公益財団法人 prefix)
        name = r[0]
        clean = name.replace("公益財団法人", "").replace("一般財団法人", "")
        clean = clean.replace("（公財）", "").replace("（一財）", "").strip()
        # Use shorter search terms for better matching
        if len(clean) > 4:
            names.append({"full": name, "search": clean[:12]})  # First 12 chars
        else:
            names.append({"full": name, "search": clean})
    return names


def search_foundation(page, search_term):
    """Search for a foundation by name and extract programs."""
    page.goto("https://jyosei-navi.jfc.or.jp/search/", wait_until="networkidle")
    time.sleep(1)

    # Check 研究助成 + enter 団体名
    page.evaluate(f"""() => {{
        const cbs = document.querySelectorAll('input[type="checkbox"]');
        cbs[0].click(); // 研究助成

        // Find the 団体名 input field
        const inputs = document.querySelectorAll('input[type="text"]');
        // The second text input should be 団体名 (first is keyword)
        for (const inp of inputs) {{
            const label = inp.closest('tr, .form-group, .row')?.textContent || '';
            if (label.includes('団体名') || inp.placeholder?.includes('団体')) {{
                inp.value = '{search_term}';
                inp.dispatchEvent(new Event('input'));
                break;
            }}
        }}

        // Click search
        const btns = document.querySelectorAll('button');
        btns[4].click(); // この条件で検索する
    }}""")

    page.wait_for_load_state("networkidle")
    time.sleep(2)

    # Extract results
    result = page.evaluate("""() => {
        const text = document.body.innerText;
        const countMatch = text.match(/(\\d+)\\s*件/);
        const count = countMatch ? parseInt(countMatch[1]) : 0;
        const tooMany = text.includes('多すぎます');

        // Try to find program entries
        const programs = [];
        const links = document.querySelectorAll('a');
        links.forEach(a => {
            const href = a.getAttribute('href') || '';
            if (href.includes('/assist/view/') || href.includes('/assist/detail/')) {
                programs.push({
                    name: a.textContent.trim(),
                    href: a.href,
                    id: href.match(/(\\d+)$/)?.[1] || '',
                });
            }
        });

        // Parse from text if no links
        if (programs.length === 0 && !tooMany && count > 0 && count <= 50) {
            // Look for structured entries
            const sections = text.split(/\\n(?=.*財団|.*振興|.*記念)/);
            sections.forEach(sec => {
                const line = sec.split('\\n')[0]?.trim();
                if (line && line.length > 5 && line.length < 80 && !line.includes('検索')) {
                    programs.push({name: line, href: '', id: ''});
                }
            });
        }

        return {count, tooMany, programs, excerpt: text.substring(0, 800)};
    }""")

    return result


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    foundations = get_foundation_names()
    print(f"=== 助成財団センター Name Search ===")
    print(f"Searching {len(foundations)} foundations...\n")

    all_results = []
    matched = 0
    not_found = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )
        page = context.new_page()

        for i, f in enumerate(foundations):
            if i > 0 and i % 20 == 0:
                print(f"\n  Progress: {i}/{len(foundations)} ({matched} matched, {not_found} not found)\n")

            print(f"  [{i+1}/{len(foundations)}] {f['search'][:15]}...", end=" ", flush=True)

            try:
                result = search_foundation(page, f["search"])
                count = result.get("count", 0)

                if count > 0:
                    matched += 1
                    entry = {
                        "foundation_name": f["full"],
                        "search_term": f["search"],
                        "program_count": count,
                        "programs": result.get("programs", []),
                        "too_many": result.get("tooMany", False),
                    }
                    all_results.append(entry)
                    print(f"FOUND {count} programs")
                else:
                    not_found += 1
                    print(f"not found")

            except Exception as e:
                print(f"ERROR: {str(e)[:50]}")
                not_found += 1

            time.sleep(0.5)  # Rate limit

            # Save progress every 50
            if i > 0 and i % 50 == 0:
                with open(OUTPUT_FILE, "w") as fo:
                    json.dump(all_results, fo, ensure_ascii=False, indent=2)

        browser.close()

    # Save final
    with open(OUTPUT_FILE, "w") as fo:
        json.dump(all_results, fo, ensure_ascii=False, indent=2)

    print(f"\n=== Final Results ===")
    print(f"Searched: {len(foundations)} foundations")
    print(f"Found in JFC: {matched}")
    print(f"Not found: {not_found}")
    print(f"Total programs: {sum(r['program_count'] for r in all_results)}")
    print(f"\nSaved: {OUTPUT_FILE}")

    # Show top matches
    print(f"\n--- Top Matches ---")
    for r in sorted(all_results, key=lambda x: -x["program_count"])[:20]:
        print(f"  {r['foundation_name']}: {r['program_count']} programs")


if __name__ == "__main__":
    main()
