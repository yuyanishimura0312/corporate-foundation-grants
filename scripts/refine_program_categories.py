#!/usr/bin/env python3
"""S1.3: foundation_categoriesに level 2-3階層を追加し、grant_programsに細分類タグを付与"""
from __future__ import annotations
import json
import re
import sqlite3
from pathlib import Path

DB = Path("/Users/nishimura+/projects/apps/corporate-foundation-grants/corporate_research_grants.sqlite")

# 11大分類（既存）→ 中分類のマッピング（新規追加）
LEVEL2 = {
    "natural_science": [
        ("ns_chem", "化学", "Chemistry", ["化学", "高分子", "ポリマー", "触媒", "有機", "無機"]),
        ("ns_phys", "物理学", "Physics", ["物理", "量子", "光", "レーザー", "プラズマ"]),
        ("ns_bio", "生物学", "Biology", ["生物", "生態", "分子生物"]),
        ("ns_geo", "地学・地球科学", "Geology", ["地学", "地球", "地質", "地震"]),
        ("ns_astro", "天文学・宇宙", "Astronomy", ["天文", "宇宙", "素粒子"]),
        ("ns_math", "数学", "Mathematics", ["数学", "数理"]),
    ],
    "life_science": [
        ("ls_med", "医学", "Medicine", ["医学", "医療", "臨床", "病理"]),
        ("ls_pharm", "薬学", "Pharmacology", ["薬学", "創薬", "製薬", "医薬"]),
        ("ls_genome", "ゲノム・遺伝学", "Genomics", ["ゲノム", "遺伝", "DNA", "RNA"]),
        ("ls_neuro", "脳科学・神経", "Neuroscience", ["脳", "神経", "認知"]),
        ("ls_immune", "免疫・感染症", "Immunology", ["免疫", "感染", "ウイルス", "細菌"]),
        ("ls_cancer", "がん研究", "Cancer", ["がん", "癌", "腫瘍"]),
        ("ls_regen", "再生医療", "Regenerative", ["再生", "iPS", "幹細胞"]),
        ("ls_basic", "生命科学基礎", "Basic Life Sciences", ["生命", "細胞", "タンパク"]),
    ],
    "engineering": [
        ("eng_mech", "機械工学", "Mechanical", ["機械", "機構", "ロボット", "ロボティクス"]),
        ("eng_elec", "電気電子", "Electrical", ["電気", "電子", "半導体", "回路"]),
        ("eng_info", "情報工学・AI", "Information & AI", ["情報", "AI", "機械学習", "深層", "アルゴリズム", "ソフトウェア"]),
        ("eng_chem", "化学工学", "Chemical Engineering", ["化学工学", "プロセス工学"]),
        ("eng_civil", "土木建築", "Civil & Architecture", ["土木", "建築", "都市", "構造", "建設"]),
        ("eng_mater", "材料工学", "Materials", ["材料", "金属", "セラミック", "複合材料"]),
        ("eng_energy", "エネルギー工学", "Energy", ["エネルギー", "電池", "太陽電池", "燃料電池", "原子力"]),
        ("eng_aero", "航空宇宙工学", "Aerospace", ["航空", "宇宙工学", "ロケット"]),
    ],
    "humanities_social": [
        ("hs_econ", "経済・経営", "Economics", ["経済", "経営", "金融", "経済学"]),
        ("hs_socio", "社会学", "Sociology", ["社会学", "社会"]),
        ("hs_psych", "心理学", "Psychology", ["心理"]),
        ("hs_polit", "政治・法学", "Political Science", ["政治", "法学", "法律"]),
        ("hs_hist", "歴史・人類学", "History & Anthropology", ["歴史", "人類学", "考古"]),
        ("hs_phil", "哲学・思想", "Philosophy", ["哲学", "思想", "倫理"]),
        ("hs_lang", "言語・文学", "Language & Literature", ["言語", "文学"]),
        ("hs_edu_research", "教育学", "Education Research", ["教育学"]),
    ],
    "arts_culture": [
        ("ac_music", "音楽", "Music", ["音楽"]),
        ("ac_visual", "美術・視覚", "Visual Arts", ["美術", "絵画", "彫刻"]),
        ("ac_perform", "舞台・演劇", "Performing Arts", ["演劇", "舞台", "ダンス"]),
        ("ac_film", "映像・写真", "Film & Photo", ["映像", "映画", "写真"]),
        ("ac_culture", "文化財・民俗", "Cultural Heritage", ["文化財", "民俗", "伝統"]),
    ],
    "education": [
        ("ed_school", "学校教育", "School Education", ["学校", "義務教育", "教育"]),
        ("ed_higher", "高等教育", "Higher Education", ["高等教育", "大学", "大学院"]),
        ("ed_intl_exchange", "国際教育", "International Education", ["国際教育", "留学"]),
        ("ed_scholarship", "奨学・育英", "Scholarship", ["奨学", "育英"]),
    ],
    "welfare": [
        ("wf_health", "保健医療", "Health", ["保健", "公衆衛生", "看護"]),
        ("wf_social", "社会福祉", "Social Welfare", ["社会福祉", "福祉"]),
        ("wf_aging", "高齢者・介護", "Aging", ["高齢", "介護", "認知症"]),
        ("wf_disability", "障害者支援", "Disability Support", ["障害", "ハンディキャップ"]),
        ("wf_child", "児童・若者", "Children & Youth", ["児童", "青少年", "若者"]),
    ],
    "environment": [
        ("env_climate", "気候変動", "Climate", ["気候", "温暖化", "CO2", "炭素"]),
        ("env_biodiv", "生物多様性", "Biodiversity", ["生物多様性", "生態系"]),
        ("env_pollution", "環境汚染", "Pollution", ["汚染", "公害"]),
        ("env_renewable", "再生可能エネルギー", "Renewable Energy", ["再生可能", "再エネ"]),
        ("env_sustain", "持続可能性", "Sustainability", ["持続可能", "サステナブル", "SDGs"]),
        ("env_forest", "森林・水資源", "Forest & Water", ["森林", "水資源", "海洋"]),
    ],
    "international": [
        ("intl_exchange", "国際交流", "Cultural Exchange", ["国際交流"]),
        ("intl_dev", "国際協力・開発", "Development Cooperation", ["国際協力", "途上国", "開発"]),
        ("intl_peace", "平和・外交", "Peace & Diplomacy", ["平和", "外交"]),
        ("intl_research", "国際共同研究", "International Research", ["国際共同研究", "国際研究"]),
    ],
    "regional": [
        ("reg_dev", "地域振興", "Regional Development", ["地域振興", "地方創生"]),
        ("reg_industry", "産業振興", "Industry Promotion", ["産業振興", "中小企業"]),
        ("reg_culture", "地域文化", "Regional Culture", ["地域文化"]),
    ],
    "interdisciplinary": [
        ("inter_emerging", "新興融合領域", "Emerging Cross-disciplinary", ["融合", "学際"]),
        ("inter_humanities_sci", "人文社会×科学", "STEAM Integration", []),
    ],
}


def main():
    conn = sqlite3.connect(DB, timeout=60)
    conn.execute("PRAGMA busy_timeout = 60000")
    cur = conn.cursor()

    # Step 1: Insert level 2 categories
    print("=== Step 1: Insert level 2 categories ===")
    inserted = 0
    for parent_id, children in LEVEL2.items():
        for cid, name_ja, name_en, kws in children:
            cur.execute("""INSERT OR IGNORE INTO foundation_categories
                (id, parent_id, level, name_ja, name_en, description, sort_order, created_at)
                VALUES (?, ?, 2, ?, ?, ?, ?, datetime('now','localtime'))""",
                (cid, parent_id, name_ja, name_en, ",".join(kws), 100))
            if cur.rowcount > 0:
                inserted += 1
    conn.commit()
    print(f"  Level 2 inserted: {inserted}")

    # Step 2: Tag programs with level 2 categories
    print("\n=== Step 2: Tag programs with level 2 ===")
    cur.execute("""SELECT p.id, p.name, p.description, p.purpose, p.category, o.name AS org_name
                   FROM grant_programs p
                   JOIN organizations o ON o.id = p.organization_id""")
    programs = cur.fetchall()
    print(f"  programs: {len(programs)}")

    tagged = 0
    for pid, name, desc, purpose, top_cat, org_name in programs:
        text = " ".join(filter(None, [name, desc, purpose, org_name]))
        # Map category to level1 id
        cat_map = {
            "research": ["natural_science", "life_science", "engineering",
                        "humanities_social", "interdisciplinary"],
            "education": ["education"],
            "environment": ["environment"],
            "international": ["international"],
            "social": ["humanities_social", "welfare"],
            "welfare": ["welfare"],
            "culture": ["arts_culture"],
        }
        candidate_l1 = cat_map.get(top_cat, list(LEVEL2.keys()))

        # Find best level 2 match
        matches = []
        for l1 in candidate_l1:
            for cid, name_ja, _, kws in LEVEL2.get(l1, []):
                if any(kw in text for kw in kws):
                    matches.append(cid)

        if matches:
            # Insert into foundation_focus_areas (organization × category) — but we want program-level
            # Use grant_programs.subcategories to store
            subcats = ",".join(matches[:5])
            cur.execute(
                "UPDATE grant_programs SET subcategories=?, updated_at=datetime('now','localtime') WHERE id=?",
                (subcats, pid),
            )
            tagged += 1

            # Also propagate to foundation_focus_areas
            cur.execute("SELECT organization_id FROM grant_programs WHERE id = ?", (pid,))
            org_id = cur.fetchone()[0]
            for cid in matches[:3]:
                cur.execute("""INSERT OR IGNORE INTO foundation_focus_areas
                    (organization_id, category_id, weight, is_primary, source, created_at)
                    VALUES (?, ?, 0.7, 0, 'inferred_from_program', datetime('now','localtime'))""",
                    (org_id, cid))

    conn.commit()
    print(f"  programs tagged with level 2: {tagged}")

    # Final
    cur.execute("SELECT COUNT(*) FROM grant_programs WHERE subcategories IS NOT NULL AND subcategories != ''")
    a = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM grant_programs")
    t = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM foundation_focus_areas")
    f = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT organization_id) FROM foundation_focus_areas")
    fo = cur.fetchone()[0]
    print(f"\nFinal:")
    print(f"  programs with subcategories: {a}/{t} ({a/t*100:.1f}%)")
    print(f"  foundation_focus_areas: {f} entries / {fo} orgs")
    conn.close()


if __name__ == "__main__":
    main()
