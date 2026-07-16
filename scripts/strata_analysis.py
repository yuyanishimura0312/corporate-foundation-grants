#!/usr/bin/env python3
"""研究層 詳細解析 Part1-3 (CFG × RID 31.1万人)。
   Part1 詳細分野: 分野×テーマ の微細セル(西洋医学×がん 等) + 採択者の副専門分野(学際)
   Part2 採択研究者との連関: 分野別に採択者の h_index/論文数/共著/若手率/主要機関
   Part3 ×31.1万人: 分野ごとに 被覆率(採択distinct/全国母集団) と 選抜性(採択h vs 母集団h)
   全数実測・捏造ゼロ。本体rid(<9億)のみ属性接地(サブは属性希薄)。金額不使用。"""
import sqlite3, re, unicodedata, json
from collections import Counter, defaultdict
CFG = "corporate_research_grants.sqlite"
RID = "/Users/nishimura+/projects/research/researcher-intelligence-db/data/rid.db"
con = sqlite3.connect(RID); con.row_factory = sqlite3.Row
con.execute("ATTACH ? AS cfg", (CFG,))
c = con

def nn(s): return re.sub(r'[\s　・]', '', unicodedata.normalize("NFKC", s or ""))

# ---- テーマ辞書(field_domain と整合・偽友是正済) ----
THEMES = [
  ("創薬・治療・診断", ["創薬","薬剤","治療","診断","医薬","製剤","ドラッグ","バイオマーカー","抗体医薬"]),
  ("免疫・感染・炎症", ["免疫","感染","ウイルス","細菌","炎症","ワクチン","抗体","アレルギー","自己免疫"]),
  ("脳・神経・認知", ["脳","神経","認知","シナプス","ニューロン","神経変性","記憶","精神","うつ","睡眠"]),
  ("がん・腫瘍", ["がん","癌","腫瘍","白血病","悪性","転移"]),
  ("ゲノム・遺伝子・オミクス", ["ゲノム","遺伝子","DNA","RNA","エピゲノム","オミクス","ゲノム編集","CRISPR","変異"]),
  ("材料・ナノ・触媒", ["ナノ","材料","触媒","高分子","二次元","薄膜","超分子","液晶","結晶"]),
  ("タンパク質・構造・酵素", ["タンパク質","蛋白","構造解析","酵素","立体構造","ペプチド"]),
  ("AI・機械学習・データ駆動", ["AI","人工知能","機械学習","深層学習","ニューラルネット","データ駆動","計算科学","アルゴリズム"]),
  ("エネルギー・脱炭素", ["脱炭素","カーボン","水素","太陽","電池","蓄電","燃料","再生可能"]),
  ("老化・長寿", ["老化","加齢","寿命","フレイル","抗加齢","サルコペニア"]),
  ("再生・幹細胞", ["再生","幹細胞","iPS","オルガノイド"]),
  ("微生物・腸内・発酵", ["微生物","腸内","細菌叢","マイクロバイオーム","発酵","菌"]),
  ("地域・福祉・社会課題", ["地域","福祉","高齢者","介護","コミュニティ","貧困","子育て","障害者","まちづくり"]),
  ("食・農・水産・植物", ["食品","農業","作物","水産","栽培","植物","家畜","品種"]),
]
def themes_of(t):
    t = t or ""; return [n for n, kws in THEMES if any(k in t for k in kws)]

# ================= CFG採択者(research_individual) =================
rows = c.execute("""SELECT g.rid_field, g.project_title, g.awardee_affiliation, g.awardee_position, g.rid_base_id
  FROM cfg.grant_results g JOIN cfg.grant_calls gc ON g.call_id=gc.id JOIN cfg.grant_programs p ON gc.program_id=p.id
  JOIN cfg.organizations o ON p.organization_id=o.id
  WHERE g.grant_type='research_individual' AND o.foundation_subtype IN ('corporate','group')""").fetchall()
N = len(rows)
matched = [r for r in rows if r["rid_field"]]

# ---- Part1: 分野×テーマ 微細セル ----
cell = Counter()
for r in matched:
    for th in themes_of(r["project_title"]):
        cell[(r["rid_field"], th)] += 1
fine_cells = [{"field": k[0], "theme": k[1], "n": v} for k, v in cell.most_common(20)]

# ---- Part1b: 採択者の副専門分野(学際性) ----
# 本体rid採択者の base_researcher_id → rid → secondary field
funded_bids = tuple({r["rid_base_id"] for r in matched if r["rid_base_id"] and r["rid_base_id"] < 900000000})
sec_pairs = Counter()
n_with_sec = 0
if funded_bids:
    q = """SELECT pri.agd_field_ja pri_f, sec.agd_field_ja sec_f
           FROM rid_identity ri
           JOIN rid_agd_field pri ON pri.rid=ri.rid
           JOIN rid_agd_field_secondary sec ON sec.rid=ri.rid
           WHERE ri.base_researcher_id IN (%s)""" % ",".join("?"*len(funded_bids))
    for pr, sr in c.execute(q, funded_bids):
        if pr and sr and pr != sr:
            sec_pairs[(pr, sr)] += 1; n_with_sec += 1
sec_top = [{"primary": k[0], "secondary": k[1], "n": v} for k, v in sec_pairs.most_common(12)]

# ================= Part2/3: 分野別 採択者属性 × 母集団 =================
KYUTEI = {"東京大学","京都大学","大阪大学","東北大学","名古屋大学","九州大学","北海道大学"}
def univ(a):
    a = nn(a); a = re.sub(r'^(国立大学法人|公立大学法人|学校法人)', '', a)
    m = re.match(r'^(.+?大学)', a) or re.match(r'^(.+?(研究所|機構|センター|大学校|高専))', a)
    return m.group(1) if m else None
YOUNG = ("助教","助手","ポスドク","博士","特任助","講師")
def is_young(p):
    p = nn(p or "")
    return (any(y in p for y in YOUNG) and "主任" not in p and "上席" not in p) if p else None

# 母集団 h_index 中央値(分野別) — rid_agd_field × rid_claims(h_index_real)
def median(v):
    v = sorted(v); n = len(v)
    return None if not n else (v[n//2] if n % 2 else (v[n//2-1]+v[n//2])/2)

# 全国母集団: 分野別 distinct研究者数 + h中央値
pop_cnt = {}; pop_hmed = {}
for f, cnt in c.execute("SELECT agd_field_ja, COUNT(DISTINCT rid) FROM rid_agd_field GROUP BY agd_field_ja"):
    if f: pop_cnt[f] = cnt
pop_h = defaultdict(list)
for f, h in c.execute("""SELECT af.agd_field_ja, cl.value_num FROM rid_agd_field af
        JOIN rid_claims cl ON cl.rid=af.rid AND cl.predicate='h_index_real' AND cl.value_num IS NOT NULL"""):
    if f: pop_h[f].append(h)
for f, v in pop_h.items(): pop_hmed[f] = round(median(v), 1)

# CFG採択者 分野別: distinct研究者・h/論文/共著 平均・若手率・機関
by_field = defaultdict(list)
for r in matched: by_field[r["rid_field"]].append(r)

# 本体採択者 base_researcher_id → rid → attributes を一括取得
attr = {}  # rid -> dict
if funded_bids:
    q2 = """SELECT ri.base_researcher_id bid, ri.rid,
            (SELECT value_num FROM rid_claims WHERE rid=ri.rid AND predicate='h_index_real' AND value_num IS NOT NULL LIMIT 1) h,
            cm.works_count, cm.coauthor_count
            FROM rid_identity ri LEFT JOIN rid_collab_measured cm ON cm.rid=ri.rid
            WHERE ri.base_researcher_id IN (%s)""" % ",".join("?"*len(funded_bids))
    for bid, rid_, h, wc, ca in c.execute(q2, funded_bids):
        attr[bid] = {"h": h, "works": wc, "coauthor": ca}

profiles = []
for f, rs in sorted(by_field.items(), key=lambda kv: -len(kv[1]))[:12]:
    bids = {r["rid_base_id"] for r in rs if r["rid_base_id"] and r["rid_base_id"] < 900000000}
    n_dist = len(bids)
    hs = [attr[b]["h"] for b in bids if b in attr and attr[b]["h"] is not None]
    works = [attr[b]["works"] for b in bids if b in attr and attr[b]["works"] is not None]
    coas = [attr[b]["coauthor"] for b in bids if b in attr and attr[b]["coauthor"] is not None]
    youngs = [is_young(r["awardee_position"]) for r in rs if is_young(r["awardee_position"]) is not None]
    insts = Counter(univ(r["awardee_affiliation"]) for r in rs if univ(r["awardee_affiliation"]))
    pool = pop_cnt.get(f, 0)
    fund_hmed = round(median(hs), 1) if hs else None
    profiles.append({
        "field": f, "n_award": len(rs), "n_researchers": n_dist,
        "pool_311k_hontai": pool,
        "coverage_pct": round(100*n_dist/pool, 2) if pool else None,
        "funded_h_med": fund_hmed, "pop_h_med": pop_hmed.get(f),
        "h_uplift": round(fund_hmed - pop_hmed[f], 1) if (fund_hmed and pop_hmed.get(f)) else None,
        "avg_works": round(sum(works)/len(works)) if works else None,
        "avg_coauthor": round(sum(coas)/len(coas)) if coas else None,
        "young_pct": round(100*sum(youngs)/len(youngs), 1) if youngs else None,
        "top_insts": [{"u": u, "n": v} for u, v in insts.most_common(3)],
    })

# 全体サマリ(選抜効果)
all_funded_h = [attr[b]["h"] for b in attr if attr[b]["h"] is not None]
summary = {
    "n_award": N, "n_matched": len(matched),
    "n_researchers_hontai": len(funded_bids),
    "funded_h_med": round(median(all_funded_h), 1) if all_funded_h else None,
    "pop_h_med": round(median([h for v in pop_h.values() for h in v]), 1),
    "n_with_secondary": n_with_sec,
}

out = {"summary": summary, "fine_cells": fine_cells, "secondary_pairs": sec_top, "profiles": profiles}
json.dump(out, open("research_results/strata_part123.json", "w"), ensure_ascii=False, indent=1)

print("採択 %d件 / 接地 %d / 本体研究者 %d" % (N, len(matched), len(funded_bids)))
print("選抜効果: 採択h中央 %s vs 母集団h中央 %s" % (summary["funded_h_med"], summary["pop_h_med"]))
print("\n=== Part1 分野×テーマ 微細セル TOP12 ===")
for x in fine_cells[:12]: print("  %-8s × %-16s %d" % (x["field"], x["theme"], x["n"]))
print("\n=== Part1b 採択者の主×副専門(学際) TOP8 (副field保有 %d) ===" % n_with_sec)
for x in sec_top[:8]: print("  %-8s → %-8s %d" % (x["primary"], x["secondary"], x["n"]))
print("\n=== Part2/3 分野別プロファイル ===")
for p in profiles:
    print("  %-8s 採択%3d(研究者%3d) 被覆%s%% h採択%s/母集団%s(+%s) 論文%s 若手%s%%" % (
        p["field"], p["n_award"], p["n_researchers"], p["coverage_pct"],
        p["funded_h_med"], p["pop_h_med"], p["h_uplift"], p["avg_works"], p["young_pct"]))