#!/usr/bin/env python3
"""研究助成 領域×金額 ランドスケープ算出 — 研究助成戦略検討用。
   primary_field(領域) × annual_grant_amount / total_assets を集計。
   カバレッジ(金額保有率)を明示。研究関連度high に絞った集計も出す。"""
import sqlite3, json
DB = "corporate_research_grants.sqlite"
c = sqlite3.connect(DB); c.row_factory = sqlite3.Row
NAMEJA = {r["id"]: r["name_ja"] for r in c.execute("SELECT id,name_ja FROM foundation_categories WHERE level=1")}

def agg(where=""):
    rows = c.execute("""
      SELECT primary_field AS f,
        COUNT(*) AS n,
        SUM(annual_grant_amount IS NOT NULL) AS n_amt,
        CAST(SUM(COALESCE(annual_grant_amount,0)) AS INTEGER) AS amt,
        CAST(AVG(annual_grant_amount) AS INTEGER) AS avg_amt,
        CAST(SUM(COALESCE(total_assets,0)) AS INTEGER) AS assets
      FROM organizations
      WHERE primary_field IS NOT NULL %s
      GROUP BY primary_field ORDER BY amt DESC""" % where).fetchall()
    return rows

def show(title, rows):
    print("\n=== %s ===" % title)
    print("%-18s %6s %6s %18s %16s %18s" % ("領域", "団体数", "金額有", "年間助成額合計", "平均助成額", "総資産合計"))
    tot_n = tot_amt = tot_assets = 0
    for r in rows:
        print("%-18s %6d %6d %18s %16s %18s" % (
            NAMEJA.get(r["f"], r["f"]), r["n"], r["n_amt"] or 0,
            "{:,}".format(r["amt"] or 0), "{:,}".format(r["avg_amt"] or 0), "{:,}".format(r["assets"] or 0)))
        tot_n += r["n"]; tot_amt += r["amt"] or 0; tot_assets += r["assets"] or 0
    print("%-18s %6d %6s %18s %16s %18s" % ("計", tot_n, "", "{:,}".format(tot_amt), "", "{:,}".format(tot_assets)))

allrows = agg()
show("全団体 領域×金額 ランドスケープ", allrows)
show("研究関連度 high のみ", agg("AND research_relevance='high'"))

# coverage note
n_amt = c.execute("SELECT COUNT(*) FROM organizations WHERE annual_grant_amount IS NOT NULL").fetchone()[0]
n_field = c.execute("SELECT COUNT(*) FROM organizations WHERE primary_field IS NOT NULL").fetchone()[0]
both = c.execute("SELECT COUNT(*) FROM organizations WHERE annual_grant_amount IS NOT NULL AND primary_field IS NOT NULL").fetchone()[0]
print("\nカバレッジ: 領域分類 %d/4893 (%.0f%%) / 金額保有 %d / 両方 %d" % (n_field, 100*n_field/4893, n_amt, both))
print("※金額保有率が低いため、金額合計は現時点の下限。Phase3収集の継続で精緻化される。")
out = {"landscape_all": [dict(r) for r in allrows]}
json.dump(out, open("research_results/grant_landscape.json", "w"), ensure_ascii=False, indent=1)
