#!/usr/bin/env python3
"""戦略分析v2 — fable是正版。企業財団(corporate)限定・分野は科研費リンク非依存のテーマ分類・
   ミッション適合領域内での追加性・軸の交差(分野×機関×キャリア×属性)。追加性を実行可能な形で。"""
import sqlite3, re, unicodedata, json
from collections import Counter
CFG = "corporate_research_grants.sqlite"
RID = "/Users/nishimura+/projects/research/researcher-intelligence-db/data/rid.db"
c = sqlite3.connect(CFG); c.row_factory = sqlite3.Row; rid = sqlite3.connect(RID)
def nn(s): return re.sub(r'[\s　・]', '', unicodedata.normalize("NFKC", s or ""))
out = {}

# 企業財団(corporate/group)の研究助成individual採択者を対象(fable是正: subtypeフィルタを明示)
rows = c.execute("""SELECT r.awardee_name, r.awardee_affiliation, r.awardee_position, r.project_title, r.rid_field, o.foundation_subtype
    FROM grant_results r JOIN grant_calls gc ON r.call_id=gc.id JOIN grant_programs p ON gc.program_id=p.id
    JOIN organizations o ON p.organization_id=o.id
    WHERE r.grant_type='research_individual' AND o.foundation_subtype IN ('corporate','group')""").fetchall()
out["n_corporate_awardees"] = len(rows)

# テーマ分類(project_title→分野・科研費リンク非依存=fable交絡A是正)。生命医学系を細分
THEME = [
    ("がん・腫瘍", ["がん","癌","腫瘍","白血病","悪性"]),
    ("免疫・感染症", ["免疫","感染","ウイルス","細菌","ワクチン","炎症"]),
    ("脳・神経科学", ["脳","神経","認知","精神","ニューロン","シナプス"]),
    ("遺伝・ゲノム・分子生物", ["遺伝子","ゲノム","DNA","RNA","タンパク","蛋白","分子生物","エピゲノム","染色体"]),
    ("再生・幹細胞", ["再生","幹細胞","iPS","組織工学"]),
    ("創薬・薬学", ["創薬","薬剤","薬物","製剤","薬効"]),
    ("代謝・生理・臨床医学", ["代謝","糖尿","循環器","心臓","腎","肝","生理","臨床","診断","治療"]),
    ("化学・材料", ["化学","触媒","高分子","材料","結晶","合成","分子設計"]),
    ("物理・数理", ["物理","量子","素粒子","光","レーザー","数理","数学","統計力学"]),
    ("情報・AI・計算", ["情報","AI","人工知能","機械学習","計算","アルゴリズム","データ","ロボット"]),
    ("エネルギー・環境技術", ["エネルギー","電池","太陽","水素","脱炭素","環境","再生可能"]),
    ("工学・デバイス", ["工学","機械","電気","電子","デバイス","センサ","半導体","回路","構造"]),
    ("農・食・生物資源", ["農","食","植物","作物","水産","森林","生態","微生物"]),
    ("地球・宇宙・海洋", ["地球","宇宙","天文","海洋","地質","気象","惑星"]),
    ("人文・社会・その他", ["経済","社会","歴史","哲学","文学","法","政治","心理","教育","芸術","文化"]),
]
def theme(t):
    t = t or ""
    for name, kws in THEME:
        if any(k in t for k in kws): return name
    return "その他・未分類"
theme_dist = Counter(theme(r["project_title"]) for r in rows)
out["theme_field_dist"] = theme_dist.most_common()

# 機関ティア(fable是正: baselineとして科研費側の同ティア研究者比を併記)
KYUTEI = {"東京大学","京都大学","大阪大学","東北大学","名古屋大学","九州大学","北海道大学"}
def univ(a):
    a=nn(a); m=re.match(r'^(.+?大学)',a) or re.match(r'^(.+?(研究所|機構|センター|大学校|高専))',a); return m.group(1) if m else None
def tier(a):
    u=univ(a)
    if not u: return "その他/不明"
    if u in KYUTEI: return "旧帝大7"
    if u.endswith("大学"): return "非旧帝大(地方国立・私立等)"
    return "研究機関等"
fnd_tier = Counter(tier(r["awardee_affiliation"]) for r in rows)
kk_tier = Counter()
for (ins,) in rid.execute("SELECT institute_name FROM rid_identity WHERE institute_name IS NOT NULL"):
    kk_tier[tier(ins)] += 1
# 追加性: 財団助成シェア vs 科研費研究者シェア(ティア)
Ft = sum(v for k,v in fnd_tier.items() if k!="その他/不明"); Kt = sum(v for k,v in kk_tier.items() if k!="その他/不明")
tier_gap = []
for t in ["旧帝大7","非旧帝大(地方国立・私立等)","研究機関等"]:
    fs = 100*fnd_tier.get(t,0)/Ft; ks = 100*kk_tier.get(t,0)/Kt
    tier_gap.append({"tier": t, "fnd_pct": round(fs,1), "kaken_pct": round(ks,1), "gap": round(fs-ks,1)})
out["tier_additionality"] = tier_gap

# キャリア段階(充足分)
pos = Counter()
YOUNG = ("助教","助手","ポスドク","博士","特任助","研究員","講師")
young=0; total_pos=0
for r in rows:
    p = r["awardee_position"]
    if not p: continue
    total_pos += 1
    pn = re.sub(r'[（(].*','',nn(p))[:8]; pos[pn]+=1
    if any(y in nn(p) for y in YOUNG): young+=1
out["career_dist"] = pos.most_common(10)
out["young_ratio"] = round(100*young/total_pos,1) if total_pos else 0

# 属性: 女性研究者向け公募・若手targeting(eligibilityから・実行可能性の証拠)
out["gender_targeted_calls"] = c.execute("SELECT COUNT(DISTINCT call_id) FROM eligibility_criteria WHERE criterion_type='gender' AND (description LIKE '%女性%限定%' OR description LIKE '%女性研究者%')").fetchone()[0]
out["age_targeted_calls"] = c.execute("SELECT COUNT(DISTINCT call_id) FROM eligibility_criteria WHERE criterion_type='age' AND description NOT LIKE '%なし%' AND description NOT LIKE '%不問%'").fetchone()[0]
out["total_research_calls"] = c.execute("SELECT COUNT(DISTINCT gc.id) FROM grant_calls gc JOIN grant_programs p ON gc.program_id=p.id JOIN organizations o ON p.organization_id=o.id WHERE o.foundation_subtype IN ('corporate','group')").fetchone()[0]

# 交差セル(fable是正: 分野×ティア×キャリアの実装可能な単位)。生命医学系×非旧帝大×若手 等
cross = Counter()
for r in rows:
    th = theme(r["project_title"]); ti = tier(r["awardee_affiliation"]); p = nn(r["awardee_position"] or "")
    yng = "若手" if any(y in p for y in YOUNG) else ("中堅以上" if p else "職位不明")
    cross[(th, ti, yng)] += 1
out["cross_top"] = [{"field":k[0],"tier":k[1],"career":k[2],"n":v} for k,v in cross.most_common(20)]

json.dump(out, open("research_results/strategic_impact_v2.json","w"), ensure_ascii=False, indent=1)
print("企業財団(corporate/group)研究助成採択者:", len(rows))
print("\n=== テーマ分野分布(科研費リンク非依存・全採択者) ===")
for k,v in theme_dist.most_common(12): print("  %-20s %d (%.1f%%)"%(k,v,100*v/len(rows)))
print("\n=== 機関ティア追加性(財団シェア vs 科研費研究者シェア) ===")
for x in tier_gap: print("  %-24s 財団%.1f%% vs 母集団%.1f%% gap%+.1f"%(x["tier"],x["fnd_pct"],x["kaken_pct"],x["gap"]))
print("\n=== キャリア: 若手比率 %.1f%% ==="%out["young_ratio"], out["career_dist"][:6])
print("\n=== 属性targeting(実行可能性) ===")
print("  女性限定公募:", out["gender_targeted_calls"], "/ 年齢制限公募:", out["age_targeted_calls"], "/ 全研究助成公募:", out["total_research_calls"])
print("\n=== 交差セル TOP12(分野×ティア×キャリア=実装単位) ===")
for x in out["cross_top"][:12]: print("  %-18s × %-22s × %-8s : %d"%(x["field"],x["tier"],x["career"],x["n"]))
