#!/usr/bin/env python3
"""研究領域 深掘り解析 — AGD分野(discipline)の一段下=実質的な「研究テーマ領域」で解析。
   (A) 分野を横断する先端研究テーマ(課題名ベース・多重マッチ許容) × どのAGD分野に跨るか
   (B) 上位AGD分野ごとの深掘りプロファイル(領域テーマ + 財団エコロジー + 機関ティア + キャリア)
   (C) 少数財団依存の脆弱分野(orphan/vulnerability)
   企業財団(corporate/group) research_individual。課題名は全件充填。捏造ゼロ・全数実測。
   ※金額は武田寡占アーティファクトのため不使用。分野比較は行わない。"""
import sqlite3, re, unicodedata, json
from collections import Counter, defaultdict
CFG = "corporate_research_grants.sqlite"
c = sqlite3.connect(CFG); c.row_factory = sqlite3.Row

rows = c.execute("""SELECT r.rid_field, r.project_title, r.awardee_affiliation, r.awardee_position,
    r.rid_base_id, o.name AS fname
  FROM grant_results r JOIN grant_calls gc ON r.call_id=gc.id JOIN grant_programs p ON gc.program_id=p.id
  JOIN organizations o ON p.organization_id=o.id
  WHERE r.grant_type='research_individual' AND o.foundation_subtype IN ('corporate','group')""").fetchall()
N = len(rows)

# --- 先端研究テーマ タクソノミ(AGD分野を横断する実質的領域・多重マッチ許容) ---
THEMES = [
  ("AI・機械学習・データ駆動", ["AI","人工知能","機械学習","深層学習","ディープラーニング","ニューラルネット","データ駆動","データサイエンス","計算科学","シミュレーション","アルゴリズム","予測モデル"]),
  ("ゲノム・遺伝子・オミクス", ["ゲノム","遺伝子","DNA","RNA","エピゲノム","トランスクリプト","オミクス","ゲノム編集","CRISPR","染色体","エクソン","変異"]),
  ("がん・腫瘍", ["がん","癌","腫瘍","白血病","悪性","転移","肉腫","メラノーマ"]),
  ("免疫・感染・炎症", ["免疫","感染","ウイルス","細菌","炎症","ワクチン","抗体","アレルギー","自己免疫","マクロファージ","T細胞"]),
  ("脳・神経・認知", ["脳","神経","認知","シナプス","ニューロン","神経変性","記憶","精神","うつ","睡眠","グリア","脳梗塞"]),
  ("再生・幹細胞", ["再生","幹細胞","iPS","ES細胞","オルガノイド","組織工学","分化誘導"]),
  ("老化・長寿", ["老化","加齢","寿命","老年","フレイル","抗加齢","エイジング","サルコペニア"]),
  ("タンパク質・構造・酵素", ["タンパク質","蛋白","構造解析","酵素","結晶","立体構造","フォールディング","ペプチド"]),
  ("創薬・治療・診断", ["創薬","薬剤","治療","診断","医薬","製剤","ドラッグ","バイオマーカー","抗体医薬"]),
  ("量子・光・レーザー", ["量子","レーザー","光子","フォトニクス","スピン","超伝導","トポロジ","素粒子"]),
  ("材料・ナノ・触媒", ["ナノ","材料","触媒","高分子","二次元","メタマテリアル","薄膜","結晶成長","超分子","液晶"]),
  ("エネルギー・脱炭素", ["脱炭素","カーボン","水素","太陽","電池","蓄電","燃料","再生可能","二酸化炭素","アンモニア"]),
  ("環境・気候・生態", ["気候変動","環境保全","生態系","生物多様性","温暖化","水質","大気","森林","海洋汚染","マイクロプラ"]),
  ("ロボット・自律・センシング", ["ロボット","自動運転","ドローン","自律移動","アクチュエータ","バイオセンサ","ウェアラブル","ヒューマノイド","マニピュレータ"]),
  ("半導体・デバイス・回路", ["半導体","デバイス","集積回路","トランジスタ","MEMS","電子回路","パワー半導体"]),
  ("微生物・腸内・発酵", ["微生物","腸内","細菌叢","マイクロバイオーム","発酵","菌","酵母","バクテリア"]),
  ("食・農・水産・植物", ["食品","農業","作物","水産","栽培","植物","収穫","家畜","品種","昆虫"]),
  ("防災・地震・レジリエンス", ["防災","地震","災害","津波","豪雨","レジリエンス","減災","耐震"]),
  # 地域・福祉: "障害"(=腎/神経/睡眠/気分/発達障害の病態)・"子ども"(=基礎神経科学)のみ除外し
  # 障害者/子育て等の福祉文脈語へ置換("地域"は地域医療/地域包括ケア等で正当なので維持)(fable是正)
  ("地域・福祉・社会課題", ["地域","福祉","高齢者","介護","コミュニティ","貧困","子育て","児童福祉","障害者","過疎","まちづくり","生活困窮"]),
  # 宇宙: bare"月"(=ヶ月/半月板/N月)を除外(fable是正: 偽陽性21%)
  ("宇宙・天文・惑星", ["宇宙","天文","惑星","人工衛星","銀河","ブラックホール","太陽系","天体","恒星"]),
]
def norm(s): return s or ""
def themes_of(title):
    t = norm(title); return [name for name, kws in THEMES if any(k in t for k in kws)]

# --- (A) 先端テーマ prevalence(多重マッチ) + 跨るAGD分野 ---
theme_ct = Counter()
theme_fields = defaultdict(Counter)
any_theme = 0
for r in rows:
    ths = themes_of(r["project_title"])
    if ths: any_theme += 1
    for th in ths:
        theme_ct[th] += 1
        if r["rid_field"]: theme_fields[th][r["rid_field"]] += 1
theme_rows = []
for th, cnt in theme_ct.most_common():
    topf = theme_fields[th].most_common(3)
    theme_rows.append({"theme": th, "n": cnt, "share": round(100*cnt/N, 1),
                       "top_fields": [{"f": f, "n": v} for f, v in topf],
                       "n_fields": len(theme_fields[th])})

# --- 機関ティア / キャリア ヘルパ ---
KYUTEI = {"東京大学","京都大学","大阪大学","東北大学","名古屋大学","九州大学","北海道大学"}
def nn(s): return re.sub(r'[\s　・]', '', unicodedata.normalize("NFKC", s or ""))
def univ(a):
    a = nn(a); a = re.sub(r'^(国立大学法人|公立大学法人|学校法人)', '', a)
    m = re.match(r'^(.+?大学)', a) or re.match(r'^(.+?(研究所|機構|センター|大学校|高専))', a)
    return m.group(1) if m else None
def tier(a):
    u = univ(a)
    if not u: return None
    if u in KYUTEI: return "旧帝大7"
    if u.endswith("大学"): return "非旧帝大"
    return "研究機関等"
YOUNG = ("助教","助手","ポスドク","博士","特任助","講師")
def is_young(p):
    p = nn(p or "")
    if not p: return None
    return any(y in p for y in YOUNG) and "主任" not in p and "上席" not in p

# --- (B) 上位AGD分野ごとの深掘りプロファイル ---
by_field = defaultdict(list)
for r in rows:
    if r["rid_field"]: by_field[r["rid_field"]].append(r)
profiles = []
for f, rs in sorted(by_field.items(), key=lambda kv: -len(kv[1]))[:10]:
    n = len(rs)
    fnd = Counter(r["fname"] for r in rs); tot = sum(fnd.values())
    hhi = sum((v/tot)**2 for v in fnd.values())
    top_fnd = fnd.most_common(1)[0]
    tiers = Counter(tier(r["awardee_affiliation"]) for r in rs if tier(r["awardee_affiliation"]))
    tt = sum(tiers.values())
    young = [is_young(r["awardee_position"]) for r in rs if is_young(r["awardee_position"]) is not None]
    # 分野内の先端テーマ top5
    fth = Counter()
    for r in rs:
        for th in themes_of(r["project_title"]): fth[th] += 1
    profiles.append({
        "field": f, "n": n, "n_foundations": len(fnd), "hhi": round(hhi, 3),
        "top_foundation": top_fnd[0].replace("公益財団法人","").replace("一般財団法人",""),
        "top_foundation_share": round(100*top_fnd[1]/tot, 1),
        "nonkyutei_pct": round(100*tiers.get("非旧帝大",0)/tt, 1) if tt else None,
        "kyutei_pct": round(100*tiers.get("旧帝大7",0)/tt, 1) if tt else None,
        "young_pct": round(100*sum(young)/len(young), 1) if young else None,
        "top_themes": [{"t": t, "n": v} for t, v in fth.most_common(5)],
    })

# --- (C) 少数財団依存の脆弱分野(orphan/vulnerability) ---
vuln = []
for f, rs in by_field.items():
    n = len(rs)
    if n < 20: continue
    fnd = Counter(r["fname"] for r in rs); tot = sum(fnd.values())
    top = fnd.most_common(1)[0]
    hhi = sum((v/tot)**2 for v in fnd.values())
    vuln.append({"field": f, "n": n, "n_foundations": len(fnd), "hhi": round(hhi, 3),
                 "top_share": round(100*top[1]/tot, 1),
                 "top_foundation": top[0].replace("公益財団法人","").replace("一般財団法人","")})
vuln_by_fewfund = sorted(vuln, key=lambda z: z["n_foundations"])[:10]
vuln_by_hhi = sorted(vuln, key=lambda z: -z["hhi"])[:10]

out = {"n": N, "any_theme_pct": round(100*any_theme/N, 1),
       "themes": theme_rows, "profiles": profiles,
       "vuln_fewfund": vuln_by_fewfund, "vuln_hhi": vuln_by_hhi}
json.dump(out, open("research_results/field_domain.json", "w"), ensure_ascii=False, indent=1)

print("研究助成 %d件 / 先端テーマ何か該当 %.1f%%" % (N, 100*any_theme/N))
print("\n=== (A) 分野横断 先端研究テーマ prevalence TOP15 ===")
for x in theme_rows[:15]:
    tf = "・".join("%s%d" % (a["f"], a["n"]) for a in x["top_fields"][:3])
    print("  %-22s %4d (%.1f%%)  跨り%d分野  主分野:%s" % (x["theme"], x["n"], x["share"], x["n_fields"], tf))
print("\n=== (B) 上位分野 深掘りプロファイル ===")
for p in profiles:
    th = "・".join("%s%d" % (t["t"], t["n"]) for t in p["top_themes"][:3])
    print("  %-8s n=%4d 財団%3d HHI%.2f 筆頭%s%.0f%% 非旧帝%s%% 若手%s%%\n           テーマ: %s" % (
        p["field"], p["n"], p["n_foundations"], p["hhi"], p["top_foundation"][:12], p["top_foundation_share"],
        p["nonkyutei_pct"], p["young_pct"], th))
print("\n=== (C) 少数財団依存の脆弱分野(財団数少ない順) ===")
for x in vuln_by_fewfund[:8]:
    print("  %-8s n=%d 財団%d 筆頭%s%.0f%%" % (x["field"], x["n"], x["n_foundations"], x["top_foundation"][:14], x["top_share"]))