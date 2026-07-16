#!/usr/bin/env python3
"""学術分野 詳細解析 — 15区分より深く、AGD55分野 × 代表性比 × 金額 × 財団専門化 × サブテーマ。
   企業財団(corporate/group)研究助成individual採択者。科研費母集団(rid.db本体)と55分野で照合。
   fable是正を継承: 分野序列は方向性・RIDマッチ60%の測定バイアスを明示。全数実測・捏造ゼロ。"""
import sqlite3, re, unicodedata, json
from collections import Counter, defaultdict
CFG = "corporate_research_grants.sqlite"
RID = "/Users/nishimura+/projects/research/researcher-intelligence-db/data/rid.db"
c = sqlite3.connect(CFG); c.row_factory = sqlite3.Row
rid = sqlite3.connect(RID)

# --- 1. 企業財団助成の AGD55 分野分布(生rid_field) ---
rows = c.execute("""SELECT r.rid_field, r.project_title, r.awardee_affiliation, r.awardee_position,
    r.award_amount, r.rid_base_id, o.name AS fname, o.id AS fid
  FROM grant_results r JOIN grant_calls gc ON r.call_id=gc.id JOIN grant_programs p ON gc.program_id=p.id
  JOIN organizations o ON p.organization_id=o.id
  WHERE r.grant_type='research_individual' AND o.foundation_subtype IN ('corporate','group')""").fetchall()
N_all = len(rows)                       # 採択レコード(件)数
matched = [r for r in rows if r["rid_field"]]
N_m = len(matched)                      # rid_field付き件数
cfg_field = Counter(r["rid_field"] for r in matched)  # 件数ベース分布(グラフ用)

# distinct 研究者(人数)基準 — fable指摘: 件を人と呼ばない
distinct_researchers = len({r["rid_base_id"] for r in matched if r["rid_base_id"]})
from collections import Counter as _C
_win = _C(r["rid_base_id"] for r in matched if r["rid_base_id"])
multi_winners = sum(1 for v in _win.values() if v >= 2)
# 分野別 distinct 研究者(人数)
cfg_field_people = defaultdict(set)
for r in matched:
    if r["rid_base_id"]: cfg_field_people[r["rid_field"]].add(r["rid_base_id"])
cfg_people_cnt = {f: len(s) for f, s in cfg_field_people.items()}
P_tot = sum(cfg_people_cnt.values())    # 分野横断合計(重複研究者は分野ごとに数える)

# --- 2. 科研費母集団 AGD55 分布(distinct研究者) ---
kaken = Counter()
for f, cnt in rid.execute("SELECT agd_field_ja, COUNT(DISTINCT rid) FROM rid_agd_field GROUP BY agd_field_ja"):
    if f: kaken[f] = cnt
K_tot = sum(kaken.values())

# --- 3. 代表性比(財団シェア / 科研費シェア) — 両側とも distinct研究者基準に統一(fable是正) ---
rep = []
for f, pc in cfg_people_cnt.items():
    if pc < 8: continue  # 極小分野は比が不安定
    cfg_s = 100 * pc / P_tot            # 人数ベース分野シェア
    kk = kaken.get(f, 0)
    kk_s = 100 * kk / K_tot if kk else 0
    ratio = (cfg_s / kk_s) if kk_s > 0 else None
    rep.append({"field": f, "cfg_n": pc, "cfg_share": round(cfg_s, 2),
                "kaken_n": kk, "kaken_share": round(kk_s, 2),
                "ratio": round(ratio, 2) if ratio else None,
                "gap_pt": round(cfg_s - kk_s, 2)})
over = sorted([x for x in rep if x["ratio"]], key=lambda z: -z["ratio"])
under = sorted([x for x in rep if x["ratio"]], key=lambda z: z["ratio"])

# --- 4. 分野別 助成金額(award_amount 充填のみ・中央値) ---
def med(v):
    v = sorted(v); n = len(v)
    return None if not n else (v[n//2] if n % 2 else (v[n//2-1]+v[n//2])/2)
amt_by = defaultdict(list)
for r in matched:
    if r["award_amount"] and r["award_amount"] > 0:
        amt_by[r["rid_field"]].append(r["award_amount"])
amt_rows = []
for f, cc in cfg_field.most_common():
    v = amt_by.get(f, [])
    if len(v) >= 10:
        amt_rows.append({"field": f, "n_amt": len(v), "median": int(med(v)),
                         "avg": int(sum(v)/len(v))})
amt_rows.sort(key=lambda z: -z["median"])

# --- 5. 財団の分野専門化(各分野を最も多く助成する財団 top3 + 集中度) ---
fld_fnd = defaultdict(Counter)
for r in matched:
    fld_fnd[r["rid_field"]][r["fname"]] += 1
spec = []
for f, cc in cfg_field.most_common(16):
    fc = fld_fnd[f]
    tot = sum(fc.values())
    top = fc.most_common(3)
    # HHI(専門化=1財団集中度)
    hhi = sum((v/tot)**2 for v in fc.values())
    spec.append({"field": f, "total": tot, "n_foundations": len(fc),
                 "hhi": round(hhi, 3),
                 "top": [{"name": n, "n": v, "share": round(100*v/tot, 1)} for n, v in top]})

# --- 6. 生命医学系のサブテーマ分解(臨床/生物系の課題名を細分) ---
SUBTHEME = {
  "医学系(西洋医学)": {"src": {"西洋医学"}, "sub": [
      ("がん・腫瘍", ["がん","癌","腫瘍","白血病","悪性"]), ("循環器・代謝", ["循環器","心臓","心筋","糖尿","動脈","高血圧","腎","肝"]),
      ("脳・神経・精神", ["脳","神経","認知","精神","うつ","睡眠"]), ("免疫・感染", ["免疫","感染","ウイルス","炎症","アレルギー"]),
      ("再生・幹細胞", ["再生","幹細胞","iPS","組織"]), ("加齢・発生", ["加齢","老化","胎盤","卵","妊娠","発生"])]},
  "生物学": {"src": {"生物学","生態学"}, "sub": [
      ("分子・細胞", ["分子","細胞","遺伝子","ゲノム","タンパク","酵素"]), ("植物・微生物", ["植物","作物","微生物","菌","藻"]),
      ("動物・行動", ["動物","昆虫","魚","行動","進化"]), ("生態・多様性", ["生態","多様性","群集","環境"])]},
  "化学": {"src": {"化学","生化学"}, "sub": [
      ("有機・合成", ["有機","合成","触媒","反応","分子設計"]), ("高分子・材料", ["高分子","材料","ポリマー","結晶","ナノ"]),
      ("生化学・代謝", ["生化学","代謝","タンパク","酵素","脂質"]), ("物理化学・電気化学", ["電解","電池","光化学","分光","界面"])]},
}
def norm(s): return s or ""
subtheme = {}
for label, cfg_ in SUBTHEME.items():
    bucket = Counter(); unc = 0; base = 0
    for r in matched:
        if r["rid_field"] in cfg_["src"]:
            base += 1; t = norm(r["project_title"]); hit = None
            for name, kws in cfg_["sub"]:
                if any(k in t for k in kws): hit = name; break
            if hit: bucket[hit] += 1
            else: unc += 1
    subtheme[label] = {"base": base, "dist": bucket.most_common(), "unclassified": unc}

# --- 7. 財団ランドスケープ(1財団の寡占構造) ---
fnd_counter = Counter(r["fname"] for r in rows)
top_fnd = fnd_counter.most_common(8)
takeda_n = sum(v for n, v in fnd_counter.items() if "武田" in n)
amt_takeda = sum(1 for r in matched if r["award_amount"] and r["award_amount"] > 0 and "武田" in r["fname"])
amt_total = sum(1 for r in matched if r["award_amount"] and r["award_amount"] > 0)
# 武田の金額分布(一律でないことを明示・fable是正)
takeda_amt = Counter(r["award_amount"] for r in matched
                     if r["award_amount"] and r["award_amount"] > 0 and "武田" in r["fname"])
amt10m = sum(1 for r in matched if r["award_amount"] == 10000000)
landscape = {"top_foundations": [{"name": n, "n": v, "share": round(100*v/N_all, 1)} for n, v in top_fnd],
             "takeda_n": takeda_n, "takeda_share": round(100*takeda_n/N_all, 1),
             "amt_filled": amt_total, "amt_takeda_share": round(100*amt_takeda/amt_total, 1) if amt_total else 0,
             "takeda_amt_dist": [{"amt": a, "n": v} for a, v in takeda_amt.most_common(6)],
             "amt10m_share": round(100*amt10m/amt_total, 1) if amt_total else 0}

# --- 8. 財団性格別マッチ率(過少代表=測定バイアスの実証・fable是正) ---
STEM_KW = ["医学","薬","科学","化学","工学","技術","理学","生命","バイオ","エネル","材料","医薬","病態","代謝"]
CULT_KW = ["文化","芸術","スポーツ","社会教育","地域","音楽","美術","財団法人日本芸術","スポーツ振興"]
def kind(n):
    if any(k in n for k in CULT_KW): return "culture"
    if any(k in n for k in STEM_KW): return "stem"
    return "other"
kind_tot = Counter(); kind_matched = Counter()
for r in rows:
    k = kind(r["fname"]); kind_tot[k] += 1
    if r["rid_field"]: kind_matched[k] += 1
match_by_kind = {k: {"total": kind_tot[k], "matched": kind_matched[k],
                     "rate": round(100*kind_matched[k]/kind_tot[k], 1) if kind_tot[k] else 0}
                 for k in ["stem", "culture", "other"]}

out = {"n_all": N_all, "n_matched": N_m, "match_rate": round(100*N_m/N_all, 1),
       "distinct_researchers": distinct_researchers, "multi_winners": multi_winners,
       "kaken_total": K_tot, "agd_fields_in_cfg": len(cfg_field),
       "field_dist_agd55": cfg_field.most_common(),
       "representation": rep, "over_rep": over[:12], "under_rep": under[:12],
       "amount_by_field": amt_rows, "specialization": spec, "subtheme": subtheme,
       "landscape": landscape, "match_by_kind": match_by_kind}
json.dump(out, open("research_results/field_deep.json", "w"), ensure_ascii=False, indent=1)

print("企業財団採択者 %d / RIDマッチ %d (%.1f%%) / 出現AGD分野 %d" % (N_all, N_m, 100*N_m/N_all, len(cfg_field)))
print("\n=== AGD55 分野分布 TOP15 ===")
for f, cc in cfg_field.most_common(15): print("  %-10s %4d (%.1f%%)" % (f, cc, 100*cc/N_m))
print("\n=== 過剰代表 TOP8 (財団シェア/科研費シェア) ===")
for x in over[:8]: print("  %-10s ratio %.2f  財団%.2f%% vs 科研費%.2f%% (n=%d)" % (x["field"], x["ratio"], x["cfg_share"], x["kaken_share"], x["cfg_n"]))
print("\n=== 過少代表 TOP8 ===")
for x in under[:8]: print("  %-10s ratio %.2f  財団%.2f%% vs 科研費%.2f%% (n=%d)" % (x["field"], x["ratio"], x["cfg_share"], x["kaken_share"], x["cfg_n"]))
print("\n=== 分野別 助成金額 中央値 TOP8(n>=10) ===")
for x in amt_rows[:8]: print("  %-10s 中央値%s万 (n=%d)" % (x["field"], "{:,}".format(x["median"]//10000), x["n_amt"]))
print("\n=== 分野専門化(HHI高=1財団集中) TOP6 ===")
for x in sorted(spec, key=lambda z:-z["hhi"])[:6]:
    print("  %-10s HHI %.2f  財団数%d  筆頭=%s(%.0f%%)" % (x["field"], x["hhi"], x["n_foundations"], x["top"][0]["name"][:18], x["top"][0]["share"]))
print("\n=== サブテーマ(西洋医学) ===")
st = subtheme["医学系(西洋医学)"]
for name, cc in st["dist"]: print("  %-14s %d (%.0f%%)" % (name, cc, 100*cc/st["base"]))
print("  未分類 %d" % st["unclassified"])