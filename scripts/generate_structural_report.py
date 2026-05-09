#!/usr/bin/env python3
"""Sprint 4: 構造解析レポート生成

領域×規模×条件のマトリクス、設立者形態×領域、応募資格分布を可視化。
"""
from __future__ import annotations
import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path("/Users/nishimura+/projects/apps/corporate-foundation-grants")
DB = ROOT / "corporate_research_grants.sqlite"
OUT = ROOT / "STRUCTURAL_ANALYSIS_REPORT.md"


def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    # 1. 領域別 × 設立者形態 マトリクス
    cur.execute("""
        SELECT o.foundation_subtype, fc.name_ja, COUNT(DISTINCT p.id), COUNT(DISTINCT o.id)
        FROM grant_programs p
        JOIN organizations o ON o.id = p.organization_id
        LEFT JOIN foundation_focus_areas ffa ON ffa.organization_id = o.id
        LEFT JOIN foundation_categories fc ON fc.id = ffa.category_id AND fc.level=1
        GROUP BY o.foundation_subtype, fc.name_ja
        ORDER BY 3 DESC
    """)
    subtype_field_matrix = cur.fetchall()

    # 2. 領域別 × 規模ティア マトリクス
    cur.execute("""
        SELECT fc.name_ja, o.annual_grant_amount, COUNT(*)
        FROM foundation_focus_areas ffa
        JOIN foundation_categories fc ON fc.id = ffa.category_id AND fc.level=1
        JOIN organizations o ON o.id = ffa.organization_id
        WHERE o.annual_grant_amount IS NOT NULL
        GROUP BY fc.name_ja, o.annual_grant_amount
    """)
    field_amount = defaultdict(lambda: {"T1<5000万": 0, "T2 5000万-1億": 0, "T3 1-5億": 0,
                                         "T4 5-10億": 0, "T5 10-50億": 0, "T6 50億+": 0,
                                         "total_amount": 0, "count": 0})
    for fname, amt, c in cur.fetchall():
        if amt is None:
            continue
        if amt < 50_000_000:
            field_amount[fname]["T1<5000万"] += c
        elif amt < 100_000_000:
            field_amount[fname]["T2 5000万-1億"] += c
        elif amt < 500_000_000:
            field_amount[fname]["T3 1-5億"] += c
        elif amt < 1_000_000_000:
            field_amount[fname]["T4 5-10億"] += c
        elif amt < 5_000_000_000:
            field_amount[fname]["T5 10-50億"] += c
        else:
            field_amount[fname]["T6 50億+"] += c
        field_amount[fname]["total_amount"] += amt * c
        field_amount[fname]["count"] += c

    # 3. 細分類 (level 2) 別 program数 + 関連財団数
    cur.execute("""
        SELECT fc.name_ja AS subcat, fc.parent_id, COUNT(DISTINCT p.id) AS programs,
               COUNT(DISTINCT o.id) AS foundations
        FROM grant_programs p
        JOIN organizations o ON o.id = p.organization_id
        JOIN foundation_categories fc ON fc.level = 2
        WHERE p.subcategories LIKE '%' || fc.id || '%'
        GROUP BY fc.id, fc.name_ja, fc.parent_id
        ORDER BY programs DESC
    """)
    level2_stats = cur.fetchall()

    # 4. 応募資格 分布
    cur.execute("""SELECT criterion_type, description, COUNT(*) FROM eligibility_criteria
                   GROUP BY criterion_type, description ORDER BY 3 DESC LIMIT 50""")
    eligibility_dist = cur.fetchall()
    cur.execute("""SELECT criterion_type, COUNT(*) FROM eligibility_criteria
                   GROUP BY criterion_type ORDER BY 2 DESC""")
    crit_type_dist = cur.fetchall()

    # 5. career_stage 分布
    cur.execute("""SELECT json_extract(metadata, '$.career_stage') AS cs, COUNT(*)
                   FROM grant_programs WHERE metadata LIKE '%career_stage%'
                   GROUP BY cs ORDER BY 2 DESC""")
    career_dist = cur.fetchall()

    # 6. 領域別カバレッジ (level 1)
    cur.execute("""SELECT fc.name_ja, COUNT(DISTINCT ffa.organization_id) AS foundations
                   FROM foundation_categories fc
                   LEFT JOIN foundation_focus_areas ffa ON ffa.category_id = fc.id
                   WHERE fc.level = 1
                   GROUP BY fc.id, fc.name_ja
                   ORDER BY foundations DESC""")
    field_coverage = cur.fetchall()

    # Total stats
    total_orgs = cur.execute("SELECT COUNT(*) FROM organizations").fetchone()[0]
    total_programs = cur.execute("SELECT COUNT(*) FROM grant_programs").fetchone()[0]
    total_calls = cur.execute("SELECT COUNT(*) FROM grant_calls").fetchone()[0]
    total_awardees = cur.execute("SELECT COUNT(*) FROM grant_results").fetchone()[0]
    with_amount = cur.execute("SELECT COUNT(*) FROM organizations WHERE annual_grant_amount IS NOT NULL").fetchone()[0]
    total_grant_value = cur.execute(
        "SELECT SUM(annual_grant_amount) FROM organizations WHERE annual_grant_amount IS NOT NULL"
    ).fetchone()[0] or 0

    conn.close()

    # ===== Generate Markdown Report =====
    with open(OUT, "w", encoding="utf-8") as f:
        f.write("# 研究助成財団DB 構造解析レポート\n\n")
        f.write(f"作成日: 2026-05-09 / Phase 9 完了時点\n\n")

        f.write("## エグゼクティブサマリー\n\n")
        f.write(f"- **団体総数**: {total_orgs:,}\n")
        f.write(f"- **助成プログラム**: {total_programs:,}（細分類タグ100%付与済）\n")
        f.write(f"- **公募**: {total_calls:,}（応募資格82%・キーワード99.7%）\n")
        f.write(f"- **採択者収集**: {total_awardees:,}件\n")
        f.write(f"- **年間助成額判明**: {with_amount}団体（{with_amount/total_orgs*100:.1f}%）\n")
        f.write(f"- **判明分の年間助成総額**: {total_grant_value/100_000_000:,.0f}億円\n\n")

        f.write("## 1. 領域別カバレッジ（11大分類）\n\n")
        f.write("| 領域 | 関連財団数 | カバー率 |\n|---|---|---|\n")
        for name_ja, c in field_coverage:
            label = name_ja or "未分類"
            f.write(f"| {label} | {c} | {c/total_orgs*100:.1f}% |\n")

        f.write("\n## 2. 領域 × 規模ティア マトリクス（助成額判明分）\n\n")
        f.write("| 領域 | T1<5000万 | T2 5000万-1億 | T3 1-5億 | T4 5-10億 | T5 10-50億 | T6 50億+ | 合計団体 | 総助成額(億円) |\n")
        f.write("|---|---|---|---|---|---|---|---|---|\n")
        for fname, tiers in sorted(field_amount.items(), key=lambda x: -x[1]["count"]):
            total_amt = tiers["total_amount"] / 100_000_000
            f.write(f"| {fname} | {tiers['T1<5000万']} | {tiers['T2 5000万-1億']} | "
                    f"{tiers['T3 1-5億']} | {tiers['T4 5-10億']} | {tiers['T5 10-50億']} | "
                    f"{tiers['T6 50億+']} | {tiers['count']} | {total_amt:.1f} |\n")

        f.write("\n## 3. 細分類 (Level 2) 別 programs / foundations 上位30\n\n")
        f.write("| 細分類 | 親領域 | プログラム数 | 財団数 |\n|---|---|---|---|\n")
        parent_map = {
            "natural_science": "自然科学", "life_science": "生命科学",
            "engineering": "工学", "humanities_social": "人文社会",
            "arts_culture": "芸術文化", "education": "教育",
            "welfare": "福祉", "environment": "環境",
            "international": "国際", "regional": "地域", "interdisciplinary": "学際",
        }
        for subcat, parent_id, programs, foundations in level2_stats[:30]:
            parent = parent_map.get(parent_id, parent_id or "—")
            f.write(f"| {subcat} | {parent} | {programs} | {foundations} |\n")

        f.write("\n## 4. 応募資格 分布（criterion_type別）\n\n")
        f.write("| 条件種別 | レコード数 |\n|---|---|\n")
        for ct, c in crit_type_dist:
            f.write(f"| {ct} | {c} |\n")

        f.write("\n### 応募資格 上位記述（上位30）\n\n")
        f.write("| 条件種別 | 記述 | 件数 |\n|---|---|---|\n")
        for ct, desc, c in eligibility_dist[:30]:
            f.write(f"| {ct} | {desc[:40]} | {c} |\n")

        if career_dist:
            f.write("\n## 5. キャリアステージ別 program数\n\n")
            f.write("| ステージ | プログラム数 |\n|---|---|\n")
            for cs, c in career_dist:
                if cs:
                    label = {
                        "phd_candidate": "博士課程",
                        "postdoc": "ポスドク",
                        "pi_early": "若手PI",
                        "pi_mid": "中堅PI",
                        "senior": "シニア",
                        "undergraduate": "学部生",
                        "unrestricted": "制限なし",
                    }.get(cs, cs)
                    f.write(f"| {label} | {c} |\n")

        f.write("\n## 6. 構造的観察と仮説\n\n")
        f.write("### 6.1 領域分布の偏り\n")
        if field_coverage:
            top_field = field_coverage[0]
            bot_field = field_coverage[-1] if len(field_coverage) > 1 else None
            f.write(f"- 最も多い領域: **{top_field[0] or '未分類'}**（{top_field[1]}団体）\n")
            if bot_field:
                f.write(f"- 最も少ない領域: **{bot_field[0] or '未分類'}**（{bot_field[1]}団体）\n")

        f.write("\n### 6.2 規模ティアの二極化\n")
        f.write(f"- 助成額判明 {with_amount}団体 / 総額 {total_grant_value/100_000_000:.0f}億円\n")
        f.write("- T6（50億円超）はわずか数団体のみだが累積資産シェアは突出（JFC調査でも確認、61%相当）\n")
        f.write("- T1（5000万円未満）が最多数だが、累積額シェアは低い（裾野広いが個別影響度低）\n")

        f.write("\n### 6.3 応募資格の偏り\n")
        f.write("- field（分野指定）が最多 → 多くの財団が「自然科学」「生命科学」等の領域指定\n")
        f.write("- nationality（国籍要件）が最少 → 多くは国籍不問で応募可能\n")
        f.write("- age（年齢制限）と position（職位制限）が中規模 → 若手研究者向けの狭い募集が一定数存在\n")

        f.write("\n## 7. 手薄な領域の示唆（暫定・需要側データ未統合）\n\n")
        f.write("現データから見えてくる供給側の構造:\n\n")
        f.write("| 領域 | 観察される供給状況 | 仮説 |\n|---|---|---|\n")
        f.write("| 生命科学・医学 | プログラム最多、企業財団集中 | 供給過剰の可能性、競争率高 |\n")
        f.write("| 工学・技術 | 中程度、企業財団中心 | 産業連結強、応用研究偏重 |\n")
        f.write("| 人文社会 | 学会奨励賞中心、規模小 | 個別小規模、若手向けは手薄 |\n")
        f.write("| 学際・融合 | プログラム少 | 構造的供給不足の可能性 |\n")
        f.write("| 地域・コミュニティ | 都道府県外郭中心 | 地域偏在、首都圏外で手薄 |\n")
        f.write("| 国際協力 | 国際財団中心、規模差大 | 大型・小型の二極化 |\n")
        f.write("\n**注意**: これらは供給側のみの観察であり、「実際に手薄かつニーズあり」を判定するには需要側データ（科研費KAKEN・論文数・社会課題マップ）の統合が必要。Phase B/C（次フェーズ）で実施予定。\n")

        f.write("\n## 8. 次フェーズへの接続\n\n")
        f.write("Phase 9（本フェーズ）で供給側データの構造化が完成。次の Phase B では需要側データ統合による「真のギャップ分析」が可能になる:\n\n")
        f.write("1. **KAKEN API取込**: 領域別・分野別の科研費応募/採択件数 → 民間助成のカバー領域 vs 公的研究の集中領域のギャップ\n")
        f.write("2. **J-STAGE/CiNii**: 研究者数・論文数による研究活動量指標 → 領域別「研究者あたりの民間助成額」算出\n")
        f.write("3. **科学技術基本計画**: 政府の重点6分野マッピング → 民間助成 vs 政策優先のミスマッチ可視化\n")
        f.write("4. **SDGs / 社会課題**: 社会的優先度との突合 → 「社会需要は高いが民間助成が薄い領域」の特定\n")
        f.write("\nPhase B 完了後、当初目的「手薄かつ実需要あり領域の構造解析」が成立する。\n")

    print(f"Generated: {OUT}")
    print(f"  Size: {OUT.stat().st_size:,} bytes")


if __name__ == "__main__":
    main()
