#!/usr/bin/env python3
"""Sprint 2.2: 残プログラムへの細分類タグ付け — 70% → 100%目標

戦略:
- grant_programs.subcategories が未設定の162件に対して
- ソース: organization.foundation_subtype + organization.name + program.name + program.description
  + program.metadata + organization.metadata.umin.grant_sample
- キーワードマッチで複数候補→上位3つを採用（出現頻度+特異性で順位）

タグ語彙: 既存DBに登場する59個の subcategory コード（hs_* / ns_* / ls_* / eng_* / ed_* / wf_* / intl_* / inter_*）
　＋ 自由形日本語ラベル（音響、人工知能、森林科学等）も拡張に含む

busy_timeout=300秒で並行処理対応。
"""
from __future__ import annotations

import json
import re
import sqlite3
import sys
from collections import Counter
from pathlib import Path

DB = Path("/Users/nishimura+/projects/apps/corporate-foundation-grants/corporate_research_grants.sqlite")
BUSY_TIMEOUT_MS = 300_000


# Keyword → canonical subcategory tag.
# Order: longer/specific phrases before shorter/general so the longest match wins.
TAG_RULES: list[tuple[str, str]] = [
    # ---------- Life sciences ----------
    ("再生医療", "ls_regen"),
    ("再生医学", "ls_regen"),
    ("ゲノム", "ls_genome"),
    ("免疫学", "ls_immune"),
    ("免疫", "ls_immune"),
    ("脳科学", "ls_neuro"),
    ("神経科学", "ls_neuro"),
    ("脳", "ls_neuro"),
    ("神経", "ls_neuro"),
    ("がん", "ls_cancer"),
    ("癌", "ls_cancer"),
    ("腫瘍", "ls_cancer"),
    ("オンコロジー", "ls_cancer"),
    ("薬学", "ls_pharm"),
    ("製薬", "ls_pharm"),
    ("創薬", "ls_pharm"),
    ("医薬", "ls_pharm"),
    ("臨床医学", "ls_med"),
    ("基礎医学", "ls_basic"),
    ("医学", "ls_med"),
    ("医療", "ls_med"),
    ("生命科学", "ls_basic"),
    ("ライフサイエンス", "ls_basic"),
    ("バイオ", "ls_basic"),

    # ---------- Natural sciences ----------
    ("天文", "ns_astro"),
    ("宇宙物理", "ns_astro"),
    ("宇宙", "ns_astro"),
    ("地球科学", "ns_geo"),
    ("地学", "ns_geo"),
    ("地質", "ns_geo"),
    ("気象", "ns_geo"),
    ("数学", "ns_math"),
    ("化学", "ns_chem"),
    ("物理学", "ns_phys"),
    ("物理", "ns_phys"),
    ("生物学", "ns_bio"),

    # ---------- Engineering ----------
    ("人工知能", "eng_info"),
    ("AI", "eng_info"),
    ("機械学習", "eng_info"),
    ("情報学", "eng_info"),
    ("情報科学", "eng_info"),
    ("情報通信", "eng_info"),
    ("コンピュータ", "eng_info"),
    ("計算機", "eng_info"),
    ("ソフトウェア", "eng_info"),
    ("通信", "eng_info"),
    ("情報", "eng_info"),
    ("航空", "eng_aero"),
    ("ロケット", "eng_aero"),
    ("土木", "eng_civil"),
    ("建築", "eng_civil"),
    ("建設", "eng_civil"),
    ("材料工学", "eng_mater"),
    ("素材", "eng_mater"),
    ("ナノ", "eng_mater"),
    ("化学工学", "eng_chem"),
    ("エネルギー工学", "eng_energy"),
    ("エネルギー", "eng_energy"),
    ("電力", "eng_energy"),
    ("電気工学", "eng_elec"),
    ("電子工学", "eng_elec"),
    ("電気", "eng_elec"),
    ("電子", "eng_elec"),
    ("機械工学", "eng_mech"),
    ("ロボット", "eng_mech"),
    ("メカ", "eng_mech"),
    ("精密", "eng_mech"),
    ("制御", "eng_mech"),
    ("自動車", "eng_mech"),

    # ---------- Humanities & Social sciences ----------
    ("社会学", "hs_socio"),
    ("社会科学", "hs_socio"),
    ("経済学", "hs_econ"),
    ("経済", "hs_econ"),
    ("経営学", "hs_econ"),
    ("経営", "hs_econ"),
    ("ビジネス", "hs_econ"),
    ("政治学", "hs_polit"),
    ("法学", "hs_polit"),
    ("法律", "hs_polit"),
    ("法政策", "hs_polit"),
    ("政治", "hs_polit"),
    ("行政", "hs_polit"),
    ("歴史学", "hs_hist"),
    ("歴史", "hs_hist"),
    ("史学", "hs_hist"),
    ("哲学", "hs_phil"),
    ("倫理", "hs_phil"),
    ("思想", "hs_phil"),
    ("言語学", "hs_lang"),
    ("言語", "hs_lang"),
    ("文学", "hs_lang"),
    ("心理学", "hs_psych"),
    ("心理", "hs_psych"),
    ("教育研究", "hs_edu_research"),

    # ---------- Education ----------
    ("学校教育", "ed_school"),
    ("初等教育", "ed_school"),
    ("中等教育", "ed_school"),
    ("学校", "ed_school"),
    ("高等教育", "ed_higher"),
    ("大学教育", "ed_higher"),
    ("奨学", "ed_scholarship"),
    ("スカラシップ", "ed_scholarship"),
    ("教育", "ed_school"),

    # ---------- Welfare / health ----------
    ("高齢", "wf_aging"),
    ("介護", "wf_aging"),
    ("認知症", "wf_aging"),
    ("社会福祉", "wf_social"),
    ("障害", "wf_social"),
    ("障がい", "wf_social"),
    ("青少年", "wf_social"),
    ("子ども", "wf_social"),
    ("児童", "wf_social"),
    ("健康", "wf_health"),
    ("保健", "wf_health"),
    ("予防", "wf_health"),
    ("福祉", "wf_social"),

    # ---------- International ----------
    ("国際交流", "intl_exchange"),
    ("国際協力", "intl_exchange"),
    ("国際開発", "intl_dev"),
    ("途上国", "intl_dev"),
    ("国際研究", "intl_research"),
    ("海外研究", "intl_research"),
    ("国際", "intl_exchange"),

    # ---------- Interdisciplinary ----------
    ("学際", "inter_emerging"),
    ("分野横断", "inter_emerging"),
    ("融合領域", "inter_emerging"),
    ("新領域", "inter_emerging"),
    ("萌芽", "inter_emerging"),

    # ---------- Arts & Culture ----------
    ("メディアアート", "arts"),
    ("デザイン", "arts"),
    ("展覧会", "arts"),
    ("展示", "arts"),
    ("美術", "arts"),
    ("芸術", "arts"),
    ("アート", "arts"),
    ("音楽", "arts"),
    ("公演", "arts"),
    ("文化財", "culture"),
    ("文化", "culture"),
    ("民俗", "culture"),
    ("伝統", "culture"),
    ("出版", "culture"),
    ("刊行", "culture"),
    ("メディア", "culture"),
    ("ジャーナリズム", "culture"),

    # ---------- Environment & sustainability ----------
    ("環境保全", "environment"),
    ("環境", "environment"),
    ("生態系", "environment"),
    ("生物多様性", "environment"),
    ("気候変動", "environment"),
    ("脱炭素", "environment"),
    ("再生可能", "environment"),
    ("自然保護", "environment"),
    ("森林", "environment"),
    ("海洋", "environment"),
    ("水資源", "environment"),
    ("リサイクル", "environment"),
    ("廃棄物", "environment"),
    ("エコ", "environment"),

    # ---------- Food / agriculture ----------
    ("食料", "food_agri"),
    ("食品", "food_agri"),
    ("食の", "food_agri"),
    ("農業", "food_agri"),
    ("水産", "food_agri"),
    ("畜産", "food_agri"),
    ("園芸", "food_agri"),

    # ---------- Wellbeing / loneliness ----------
    ("ウェルビーイング", "wellbeing"),
    ("孤独", "wellbeing"),
    ("孤立", "wellbeing"),
    ("生きがい", "wellbeing"),
    ("リハビリテーション", "ls_med"),

    # ---------- Disaster / safety ----------
    ("防災", "disaster"),
    ("災害", "disaster"),
    ("復興", "disaster"),
    ("安全", "disaster"),

    # ---------- Industry / technology generic ----------
    ("技術研究", "eng_general"),
    ("ものづくり", "eng_general"),
    ("製造業", "eng_general"),
    ("産業", "eng_general"),
]


# Foundation/program name fallback — when nothing else matches, infer broad tag.
NAME_FALLBACK_RULES: list[tuple[str, str]] = [
    ("デザイン", "arts"),
    ("芸術", "arts"),
    ("音楽", "arts"),
    ("美術", "arts"),
    ("文化", "culture"),
    ("メディア", "culture"),
    ("食", "food_agri"),
    ("農", "food_agri"),
    ("環境", "environment"),
    ("エコ", "environment"),
    ("鉄鋼", "eng_mater"),
    ("学術", "inter_emerging"),
    ("教育", "ed_school"),
    ("奨学", "ed_scholarship"),
    ("こども", "wf_social"),
    ("子ども", "wf_social"),
    ("ベネッセ", "ed_school"),
    ("トヨタ", "hs_socio"),
    ("ロッテ", "ls_basic"),
    ("PwC", "hs_econ"),
    ("ＮＥＸＣＯ", "eng_civil"),
    ("NEXCO", "eng_civil"),
    ("郵便", "hs_socio"),
]


def infer_tags(text: str) -> list[str]:
    """Return up to 3 subcategory tags ranked by match score.
    Score = sum of (occurrences * specificity bonus for longer keywords).
    """
    if not text:
        return []
    tag_scores: Counter[str] = Counter()
    text_lower = text  # Japanese; no case issues. Keep raw.
    for kw, tag in TAG_RULES:
        cnt = text_lower.count(kw)
        if cnt:
            # Specificity bonus: longer keywords contribute more
            bonus = min(len(kw), 6)
            tag_scores[tag] += cnt * bonus
    # Sort by score desc, then by first appearance order (Counter preserves)
    ranked = [t for t, _ in tag_scores.most_common()]
    return ranked[:3]


def main():
    if not DB.exists():
        print(f"DB not found: {DB}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(DB, timeout=300)
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA journal_mode = WAL")
    cur = conn.cursor()

    cur.execute(
        """SELECT p.id, p.name, p.description, p.purpose, p.metadata,
                  p.subcategories,
                  o.name AS o_name, o.foundation_subtype, o.description AS o_desc, o.metadata AS o_meta
             FROM grant_programs p
             JOIN organizations o ON p.organization_id = o.id"""
    )
    programs = cur.fetchall()
    print(f"Total programs: {len(programs)}")

    updated = 0
    skipped_already_tagged = 0
    no_match = 0
    distrib: Counter[str] = Counter()

    for (pid, p_name, p_desc, p_purpose, p_meta, p_subs,
         o_name, o_subtype, o_desc, o_meta) in programs:
        # Skip programs that already have subcategories
        if p_subs and p_subs.strip() and p_subs.strip() != "[]":
            skipped_already_tagged += 1
            continue

        # Compose text from program + organization fields
        parts = [p_name, p_desc, p_purpose, o_name, o_desc]

        # program metadata: target/field/subfield strings
        if p_meta:
            try:
                pm = json.loads(p_meta)
                for k in ("target", "field", "subfield", "award_type"):
                    v = pm.get(k)
                    if v:
                        parts.append(str(v))
            except (ValueError, TypeError):
                pass

        # organization metadata: umin grant_sample
        if o_meta:
            try:
                om = json.loads(o_meta)
                gs = (om.get("umin") or {}).get("grant_sample") or {}
                for k in ("target_content", "target_researcher", "name", "category"):
                    v = gs.get(k)
                    if v:
                        parts.append(str(v))
            except (ValueError, TypeError):
                pass

        text = " ".join([s for s in parts if s])
        tags = infer_tags(text)

        if not tags:
            # Fallback: foundation/program name based broad tag
            haystack = " ".join([s for s in (p_name, o_name) if s])
            for kw, tag in NAME_FALLBACK_RULES:
                if kw in haystack:
                    tags = [tag]
                    break

        if not tags:
            no_match += 1
            continue

        new_value = ",".join(tags)
        cur.execute(
            "UPDATE grant_programs SET subcategories = ?, updated_at = datetime('now','localtime') WHERE id = ?",
            (new_value, pid),
        )
        updated += 1
        for t in tags:
            distrib[t] += 1

    conn.commit()

    cur.execute(
        """SELECT COUNT(*) FROM grant_programs
            WHERE subcategories IS NOT NULL AND subcategories != '' AND subcategories != '[]'"""
    )
    cov = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM grant_programs")
    total = cur.fetchone()[0]

    print("\n=== Results ===")
    print(f"Programs already tagged (skipped): {skipped_already_tagged}")
    print(f"Programs newly tagged this run: {updated}")
    print(f"Programs with no keyword match: {no_match}")
    print(f"Subcategory coverage: {cov}/{total} ({cov/total*100:.1f}%)")
    if distrib:
        print("\nNewly added tag distribution (top 20):")
        for t, c in distrib.most_common(20):
            print(f"  {t}: {c}")

    conn.close()


if __name__ == "__main__":
    main()
