#!/usr/bin/env python3
"""領域分類器 — deterministic keyword classification of each foundation into the 11 level-1
   research/grant domains, from name+description. Additive column primary_field (+ method).
   For the 研究助成 領域×金額 landscape. Marked heuristic; codex/evidence classification is authoritative."""
import sqlite3, json
DB = "corporate_research_grants.sqlite"

# level-1 field -> keyword list (order = priority on ties handled by score then this order)
FIELDS = [
    ("life_science",     ["医学", "医科", "生命", "薬学", "ゲノム", "脳", "神経科学", "免疫", "がん", "癌", "再生医療", "バイオ", "生物医学", "臨床", "創薬", "病態", "健康科学", "老化"]),
    ("natural_science",  ["自然科学", "化学", "物理", "数学", "天文", "地球科学", "基礎科学", "理学", "素粒子", "結晶"]),
    ("engineering",      ["工学", "技術振興", "機械", "電気", "電子", "情報", "ＡＩ", "人工知能", "材料", "エネルギー", "航空", "宇宙", "ロボット", "ものづくり", "化学工学", "土木", "建築", "科学技術", "半導体", "通信"]),
    ("humanities_social",["人文", "社会科学", "経済", "法学", "政治", "心理", "歴史", "哲学", "言語", "社会学", "会計", "金融研究"]),
    ("arts_culture",     ["芸術", "美術", "音楽", "舞台", "演劇", "映像", "写真", "文化財", "伝統文化", "工芸", "文芸", "デザイン", "美術館", "文化振興"]),
    ("education",        ["教育", "奨学", "育英", "人材育成", "学校", "スカラシップ", "修学"]),
    ("welfare",          ["福祉", "介護", "障害", "障がい", "児童", "高齢", "社会福祉", "保健", "ボランティア", "医療現場", "こども", "子ども", "母子"]),
    ("environment",      ["環境", "気候", "生物多様", "森林", "再生可能", "持続可能", "自然保護", "緑化", "地球温暖", "海洋保全", "水資源", "エコ"]),
    ("international",    ["国際交流", "国際協力", "国際理解", "開発途上", "平和", "異文化", "海外", "国際親善", "グローバル"]),
    ("regional",         ["地域", "地方", "まちづくり", "産業振興", "コミュニティ", "地場", "郷土", "振興機構"]),
    ("interdisciplinary",["学際", "融合", "分野横断", "総合研究"]),
]

def classify(name, desc):
    t = (name or "") + " " + (desc or "")
    best, bestscore = None, 0
    scores = {}
    for field, kws in FIELDS:
        s = sum(1 for k in kws if k in t)
        if s: scores[field] = s
        if s > bestscore:
            best, bestscore = field, s
    return best, scores

c = sqlite3.connect(DB)
for col in ("primary_field", "primary_field_method"):
    if col not in [r[1] for r in c.execute("PRAGMA table_info(organizations)")]:
        c.execute("ALTER TABLE organizations ADD COLUMN %s TEXT" % col)

# authoritative first: if a foundation already has evidence-based focus_area (inferred_from_program),
# use its primary (is_primary or max weight) level-1 as primary_field with method='evidence'
auth = {}
for oid, cat in c.execute("""
    SELECT ffa.organization_id, fc.id FROM foundation_focus_areas ffa
    JOIN foundation_categories fc ON fc.id=ffa.category_id AND fc.level=1
    WHERE ffa.source='inferred_from_program'
    ORDER BY ffa.is_primary DESC, ffa.weight DESC""").fetchall():
    auth.setdefault(oid, cat)  # first = primary

from collections import Counter
cnt = Counter(); method_cnt = Counter()
for oid, name, desc in c.execute("SELECT id,name,description FROM organizations").fetchall():
    if oid in auth:
        pf, method = auth[oid], "evidence"
    else:
        pf, _ = classify(name, desc)
        method = "keyword" if pf else None
    if pf:
        c.execute("UPDATE organizations SET primary_field=?, primary_field_method=? WHERE id=?", (pf, method, oid))
        cnt[pf] += 1; method_cnt[method] += 1
c.commit()
print("primary_field assigned:", sum(cnt.values()), "/ 4893")
print("by method:", dict(method_cnt))
print("by field:", json.dumps(cnt.most_common(), ensure_ascii=False))
