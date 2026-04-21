#!/usr/bin/env python3
"""
Collect annual grant expenditure data from corporate foundation websites.
Targets the top foundations by program count and known major grant-makers.

Strategy:
1. For each foundation, search for annual report / 事業報告 / 助成実績 pages
2. Extract annual grant expenditure amounts
3. Store in database

For foundations without web data, use koeki-info detail page data.
"""
import sqlite3, json, re, time
from pathlib import Path
from playwright.sync_api import sync_playwright

DB_PATH = Path(__file__).parent.parent / "corporate_research_grants.sqlite"
OUTPUT_DIR = Path(__file__).parent.parent / "data"
OUTPUT_FILE = OUTPUT_DIR / "annual_grant_amounts.json"

# Known foundation URLs and their annual report / grant results pages
# Manually curated for top foundations
FOUNDATION_URLS = {
    "トヨタ財団": "https://www.toyotafound.or.jp/about/data/",
    "三菱財団": "https://www.mitsubishi-zaidan.jp/support/",
    "武田科学振興財団": "https://www.takeda-sci.or.jp/business/",
    "稲盛財団": "https://www.inamori-f.or.jp/research_grant/",
    "住友財団": "https://www.sumitomo.or.jp/activities/",
    "マツダ財団": "https://mzaidan.mazda.co.jp/results/",
    "花王芸術・科学財団": "https://www.kao-foundation.or.jp/assist/science/",
    "ニッポンハム食の未来財団": "https://www.miraizaidan.or.jp/grant/",
    "旭硝子財団": "https://www.af-info.or.jp/research/",
    "ロッテ財団": "https://www.lotte-isf.or.jp/promotion/",
    "野村財団": "https://www.nomurafoundation.or.jp/social/grant/",
    "日本生命財団": "https://www.nihonseimei-zaidan.or.jp/jyosei/",
    "電気通信普及財団": "https://www.taf.or.jp/grant/",
    "セコム科学技術振興財団": "https://www.secomzaidan.jp/grant/",
    "大和証券福祉財団": "https://www.daiwa-grp.jp/dsf/grant/",
    "パナソニック教育財団": "https://www.pef.or.jp/school/grant/",
    "三島海雲記念財団": "https://www.mishima-kaiun.or.jp/assist/",
    "先進医薬研究振興財団": "https://www.smf.or.jp/",
    "ホソカワ粉体工学振興財団": "https://www.kona.or.jp/",
    "日立財団": "https://www.hitachi-zaidan.org/activities/",
    "東レ科学振興会": "https://www.toray-sf.or.jp/grant/",
    "カシオ科学振興財団": "https://www.casio.co.jp/csr/zaidan/",
    "岩谷直治記念財団": "https://www.iwatani-foundation.or.jp/",
    "コニカミノルタ科学技術振興財団": "https://www.konicaminolta.jp/about/csr/contribution/zaidan/",
    "サントリー文化財団": "https://www.suntory.co.jp/sfnd/research/",
    "中谷財団": "https://www.nakatani-foundation.jp/grant/",
    "アサヒグループ財団": "https://www.asahigroup-foundation.com/academic/",
    "加藤記念バイオサイエンス振興財団": "https://www.katokinen.or.jp/",
    "村田学術振興・教育財団": "https://corporate.murata.com/ja-jp/group/zaidan",
    "ソニー教育財団": "https://www.sony-ef.or.jp/",
}


def extract_financial_data(page, url, foundation_name):
    """Visit a foundation's website and extract grant amount data."""
    try:
        page.goto(url, timeout=15000, wait_until="domcontentloaded")
        time.sleep(2)

        result = page.evaluate("""() => {
            const text = document.body.innerText;

            // Look for monetary amounts (Japanese yen patterns)
            const amounts = [];

            // Pattern: XX億XX万円, X,XXX万円, etc.
            const amtPatterns = [
                /(\d[\d,]*)\s*億\s*(\d[\d,]*)\s*万\s*円/g,
                /(\d[\d,]*)\s*億円/g,
                /(\d[\d,]*)\s*万\s*円/g,
                /(\d[\d,.]*)\s*百万円/g,
            ];

            for (const pat of amtPatterns) {
                let match;
                while ((match = pat.exec(text)) !== null) {
                    const context = text.substring(Math.max(0, match.index - 50), match.index + match[0].length + 30);
                    amounts.push({
                        value: match[0],
                        context: context.replace(/\\n/g, ' ').trim(),
                    });
                }
            }

            // Look for grant count patterns
            const counts = [];
            const countPat = /(\d+)\s*件/g;
            let match;
            while ((match = countPat.exec(text)) !== null) {
                const context = text.substring(Math.max(0, match.index - 40), match.index + match[0].length + 20);
                if (context.includes('助成') || context.includes('採択') || context.includes('交付')) {
                    counts.push({value: match[0], context: context.replace(/\\n/g, ' ').trim()});
                }
            }

            // Look for year/fiscal year references near amounts
            const yearPats = text.match(/(令和\d+年度|20\d{2}年度|R\d年度)/g) || [];

            // Look for specific keywords indicating annual totals
            const hasAnnualReport = text.includes('事業報告') || text.includes('年次報告') || text.includes('活動報告');
            const hasGrantResults = text.includes('助成実績') || text.includes('助成件数') || text.includes('助成金額');
            const hasFinancials = text.includes('事業費') || text.includes('公益目的事業') || text.includes('助成金支出');

            return {
                amounts: amounts.slice(0, 15),
                counts: counts.slice(0, 10),
                years: [...new Set(yearPats)].slice(0, 5),
                hasAnnualReport,
                hasGrantResults,
                hasFinancials,
                excerpt: text.substring(0, 1500),
            };
        }""")

        return result
    except Exception as e:
        return {"error": str(e)}


def parse_amount_yen(text):
    """Convert Japanese yen text to integer."""
    text = text.replace(",", "").replace(".", "")
    oku_match = re.match(r"(\d+)億(\d+)万円", text)
    if oku_match:
        return int(oku_match.group(1)) * 100_000_000 + int(oku_match.group(2)) * 10_000
    oku_only = re.match(r"(\d+)億円", text)
    if oku_only:
        return int(oku_only.group(1)) * 100_000_000
    man_match = re.match(r"(\d+)万円", text)
    if man_match:
        return int(man_match.group(1)) * 10_000
    hyaku_match = re.match(r"(\d+)百万円", text)
    if hyaku_match:
        return int(hyaku_match.group(1)) * 1_000_000
    return 0


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        print(f"=== Annual Report Data Collector ===")
        print(f"Targeting {len(FOUNDATION_URLS)} foundations\n")

        for i, (name, url) in enumerate(FOUNDATION_URLS.items()):
            print(f"  [{i+1}/{len(FOUNDATION_URLS)}] {name}...", end=" ", flush=True)

            data = extract_financial_data(page, url, name)

            if data.get("error"):
                print(f"ERROR: {data['error'][:40]}")
                results.append({"name": name, "url": url, "error": data["error"]})
                continue

            # Determine best amount estimate
            best_amount = None
            best_context = ""
            for amt in data.get("amounts", []):
                ctx = amt["context"].lower()
                if any(kw in ctx for kw in ["助成", "研究", "支出", "交付", "事業費", "総額"]):
                    val = parse_amount_yen(amt["value"])
                    if val > 0 and (best_amount is None or val > best_amount):
                        best_amount = val
                        best_context = amt["context"]

            entry = {
                "name": name,
                "url": url,
                "estimated_annual_amount": best_amount,
                "amount_context": best_context,
                "all_amounts": data.get("amounts", []),
                "grant_counts": data.get("counts", []),
                "fiscal_years": data.get("years", []),
                "has_annual_report": data.get("hasAnnualReport", False),
                "has_grant_results": data.get("hasGrantResults", False),
            }
            results.append(entry)

            if best_amount:
                print(f"FOUND {best_amount/10000:.0f}万円 ({best_context[:40]})")
            elif data.get("amounts"):
                print(f"{len(data['amounts'])} amounts found (no grant-specific)")
            else:
                print(f"no amounts")

            time.sleep(1)

        browser.close()

    # Save results
    with open(OUTPUT_FILE, "w") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # Summary
    found = [r for r in results if r.get("estimated_annual_amount")]
    total = sum(r["estimated_annual_amount"] for r in found)
    print(f"\n=== Results ===")
    print(f"Processed: {len(results)} foundations")
    print(f"Amount found: {len(found)} foundations")
    print(f"Total estimated annual grants: {total/100000000:.1f}億円")
    print(f"\nSaved: {OUTPUT_FILE}")

    print(f"\n--- Top amounts ---")
    for r in sorted(found, key=lambda x: -x["estimated_annual_amount"])[:15]:
        print(f"  {r['name']}: {r['estimated_annual_amount']/10000:.0f}万円")
        print(f"    {r['amount_context'][:60]}")


if __name__ == "__main__":
    main()
