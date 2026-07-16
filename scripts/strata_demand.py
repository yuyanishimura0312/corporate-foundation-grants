#!/usr/bin/env python3
"""Part4 本来助成されると良い研究層 — 需要(テーマ連携) vs 供給(CFG助成) のギャップ。
   需要 = NGF施策テーマ(200) + GVC産業テーマ(145) + GVC社会テーマ(194) の分野別 leverage。
   供給 = CFG企業財団の分野別 代表性比(distinct基準・field_deep.json) と 被覆率(strata_part123)。
   産業創出プロセス仮説(ICFM/VCPM) = 統合点/基盤完全性が律速 → 横断的enabling分野の重要性(定性)。
   全数実測・捏造ゼロ。需要側(NGF/GVC)はRIDマッチに非依存=人社の測定バイアスの影響を受けない。"""
import sqlite3, json
NGF = "/Users/nishimura+/projects/research/nextgen-forms-db/data/ngf.db"
GVC = "/Users/nishimura+/projects/research/global-vc-investment-db/data/vc.db"

# ---- 需要側: NGF 施策テーマ leverage(分野別 distinct施策テーマ数) ----
ngf = sqlite3.connect(NGF)
policy = {}
for f, n, prim in ngf.execute("""SELECT agd_field_ja, COUNT(DISTINCT nextgen_id), SUM(role='primary')
        FROM nextgen_researcher_link GROUP BY agd_field_ja"""):
    if f: policy[f] = {"themes": n, "primary": prim or 0}

# ---- 需要側: GVC 産業/社会テーマ leverage(分野→sector→theme) ----
gvc = sqlite3.connect(GVC)
gvc_lev = {}
for f, ind, soc in gvc.execute("""SELECT sa.agd_field,
        COUNT(DISTINCT CASE WHEN tr.target_type='industry' THEN tr.target_id END),
        COUNT(DISTINCT CASE WHEN tr.target_type='social' THEN tr.target_id END)
        FROM sector_agd sa JOIN theme_relation tr ON sa.sector_name=tr.sector_name
        GROUP BY sa.agd_field"""):
    if f: gvc_lev[f] = {"industry": ind or 0, "social": soc or 0}

# ---- 供給側: CFG 代表性比(distinct基準) + 被覆率 ----
fd = json.load(open("research_results/field_deep.json"))
rep = {x["field"]: x for x in fd["representation"]}
p123 = json.load(open("research_results/strata_part123.json"))
cov = {p["field"]: p for p in p123["profiles"]}

# ---- 統合: 全分野の 需要 vs 供給 ----
fields = set(policy) | set(gvc_lev) | set(rep)
merged = []
for f in fields:
    pol = policy.get(f, {}).get("themes", 0)
    ind = gvc_lev.get(f, {}).get("industry", 0)
    soc = gvc_lev.get(f, {}).get("social", 0)
    demand = pol + ind + soc               # 総テーマ leverage(政策+産業+社会)
    r = rep.get(f)
    ratio = r["ratio"] if r else None       # CFG代表性比(1=母集団並/>1=厚い/<1=薄い)
    cfg_share = r["cfg_share"] if r else 0
    merged.append({"field": f, "policy": pol, "industry": ind, "social": soc,
                   "demand": demand, "cfg_ratio": ratio, "cfg_share": cfg_share,
                   "coverage": cov.get(f, {}).get("coverage_pct")})

# 需要順位 + 供給不足度
maxd = max(m["demand"] for m in merged) or 1
for m in merged:
    m["demand_norm"] = round(m["demand"]/maxd, 2)
# 「本来助成されると良い層」= 需要高(demand上位) かつ 供給薄(ratio<1 or ratio欠測=構造的未到達)
by_demand = sorted(merged, key=lambda z: -z["demand"])
# under-served: demand 上位20位以内 かつ (ratio is None or ratio < 0.8)
underserved = [m for m in by_demand[:22] if (m["cfg_ratio"] is None or m["cfg_ratio"] < 0.8)]
# over-served(参考): demand低め かつ ratio高い
overserved = sorted([m for m in merged if m["cfg_ratio"] and m["cfg_ratio"] > 1.3], key=lambda z: -z["cfg_ratio"])

out = {"merged": merged, "by_demand": by_demand[:20], "underserved": underserved,
       "overserved": overserved[:8],
       "note": "需要=NGF施策+GVC産業+GVC社会 テーマleverage(RIDマッチ非依存)。供給=CFG代表性比(distinct)。"}
json.dump(out, open("research_results/strata_demand.json", "w"), ensure_ascii=False, indent=1)

print("=== 需要(テーマleverage=政策+産業+社会) 上位15分野 ===")
print("  %-10s 政策 産業 社会 計  | CFG代表性比 被覆%%" % "分野")
for m in by_demand[:15]:
    rr = ("%.2f" % m["cfg_ratio"]) if m["cfg_ratio"] else "測定不能"
    cc = ("%.2f" % m["coverage"]) if m["coverage"] else "—"
    print("  %-10s %3d %3d %3d %3d | %-8s %s" % (m["field"], m["policy"], m["industry"], m["social"], m["demand"], rr, cc))
print("\n=== 本来助成されると良い層(需要高×供給薄 ratio<0.8 or 測定不能) ===")
for m in underserved:
    rr = ("比%.2f" % m["cfg_ratio"]) if m["cfg_ratio"] else "企業財団ほぼ未到達(測定不能)"
    print("  %-10s テーマ計%3d (政策%d/産業%d/社会%d) — %s" % (m["field"], m["demand"], m["policy"], m["industry"], m["social"], rr))
print("\n=== 参考: 供給厚い層(ratio>1.3・生命科学ミッション適合) ===")
for m in overserved[:6]:
    print("  %-10s 代表性比%.2f テーマ計%d" % (m["field"], m["cfg_ratio"], m["demand"]))