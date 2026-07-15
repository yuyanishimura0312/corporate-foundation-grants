#!/usr/bin/env python3
"""タスク2-4 — 3階層領域(プログラム/研究者/テーマ) × 学術分野照合 + A-E 全成果物。
   A:財団領域MAP B:科研費照合 C:大学分布 D:応募要項パターン E:金額分布。全て実測・JSON出力。"""
import sqlite3, re, unicodedata, json
from collections import Counter, defaultdict
CFG = "corporate_research_grants.sqlite"
RID = "/Users/nishimura+/projects/research/researcher-intelligence-db/data/rid.db"
OUT = "research_results/grant_field_map.json"

# AGD 55分野 → CFG 11大分類 マッピング
AGD11 = {
    "西洋医学":"life_science","東洋医学":"life_science","アーユルヴェーダ":"life_science","生化学":"life_science",
    "生理学":"life_science","薬学":"life_science","生命工学":"life_science","生物学":"natural_science",
    "生態学":"environment","化学":"natural_science","物理学":"natural_science","数学":"natural_science",
    "天文学":"natural_science","地球科学":"natural_science","地質学":"natural_science","気象学":"natural_science",
    "統計学":"natural_science","計量学":"natural_science","分類学":"natural_science","系統理論":"natural_science",
    "機械工学":"engineering","電気工学":"engineering","土木工学":"engineering","建築学":"engineering",
    "情報理論":"engineering","計算機科学":"engineering","経済学":"humanities_social","経営学":"humanities_social",
    "社会学":"humanities_social","政治学":"humanities_social","法学":"humanities_social","心理学":"humanities_social",
    "人類学":"humanities_social","地理学":"humanities_social","歴史学":"humanities_social","哲学":"humanities_social",
    "倫理学":"humanities_social","神学":"humanities_social","論理学":"humanities_social","認知科学":"humanities_social",
    "言語学":"humanities_social","文学":"arts_culture","古典学":"humanities_social","修辞学":"humanities_social",
    "美学":"arts_culture","芸術理論":"arts_culture","音楽学":"arts_culture","記号論":"humanities_social",
    "意味論":"humanities_social","解釈学":"humanities_social","翻訳学":"humanities_social","コミュニケーション":"humanities_social",
    "コミュニケーション論":"humanities_social","教育学":"education","農学":"natural_science","研究方法論":"interdisciplinary",
}
NAMEJA11 = {"natural_science":"自然科学","life_science":"生命科学・医学","engineering":"工学・技術",
    "humanities_social":"人文社会科学","arts_culture":"芸術・文化","education":"教育・人材育成","welfare":"福祉・健康",
    "environment":"環境","international":"国際交流・協力","regional":"地域","interdisciplinary":"学際・融合"}

def nn(s): return re.sub(r'[\s　・]', '', unicodedata.normalize("NFKC", s or ""))
PREF = "北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
# 主要大学→都道府県(全国分布用・主要機関のみ静的マップ+住所推定)
UNIV_PREF = {"東京大学":"東京都","京都大学":"京都府","大阪大学":"大阪府","東北大学":"宮城県","名古屋大学":"愛知県",
"九州大学":"福岡県","北海道大学":"北海道","筑波大学":"茨城県","慶應義塾大学":"東京都","早稲田大学":"東京都",
"東京科学大学":"東京都","東京工業大学":"東京都","神戸大学":"兵庫県","広島大学":"広島県","岡山大学":"岡山県",
"金沢大学":"石川県","千葉大学":"千葉県","新潟大学":"新潟県","熊本大学":"熊本県","長崎大学":"長崎県",
"信州大学":"長野県","岐阜大学":"岐阜県","三重大学":"三重県","静岡大学":"静岡県","山口大学":"山口県",
"鹿児島大学":"鹿児島県","徳島大学":"徳島県","愛媛大学":"愛媛県","富山大学":"富山県","群馬大学":"群馬県",
"埼玉大学":"埼玉県","横浜国立大学":"神奈川県","横浜市立大学":"神奈川県","大阪公立大学":"大阪府",
"名古屋工業大学":"愛知県","豊橋技術科学大学":"愛知県","理化学研究所":"埼玉県","産業技術総合研究所":"茨城県",
"東京理科大学":"東京都","日本大学":"東京都","近畿大学":"大阪府","立命館大学":"京都府","同志社大学":"京都府",
"順天堂大学":"東京都","東京医科歯科大学":"東京都","昭和大学":"東京都","日本医科大学":"東京都",
"山形大学":"山形県","弘前大学":"青森県","岩手大学":"岩手県","秋田大学":"秋田県","福島県立医科大学":"福島県",
"宇都宮大学":"栃木県","茨城大学":"茨城県","山梨大学":"山梨県","福井大学":"福井県","鳥取大学":"鳥取県",
"島根大学":"島根県","香川大学":"香川県","高知大学":"高知県","佐賀大学":"佐賀県","大分大学":"大分県",
"宮崎大学":"宮崎県","琉球大学":"沖縄県","滋賀大学":"滋賀県","奈良女子大学":"奈良県","和歌山大学":"和歌山県"}

def univ(a):
    a = nn(a)
    m = re.match(r'^(.+?大学)', a) or re.match(r'^(.+?(研究所|機構|センター|大学校|高専|工業高等専門学校))', a)
    return m.group(1) if m else None
def pref_of(u, addr=None):
    if not u: return None
    if u in UNIV_PREF: return UNIV_PREF[u]
    for k, v in UNIV_PREF.items():
        if k in u: return v
    if addr:
        m = re.match(r'^(' + PREF + r')', unicodedata.normalize("NFKC", addr))
        if m: return m.group(1)
    return None

# theme classifier: project_title → 11-cat (keyword)
THEME_KW = [
    ("life_science", ["がん","癌","腫瘍","免疫","細胞","遺伝子","ゲノム","タンパク","蛋白","神経","脳","疾患","病態","治療","診断","医療","臨床","創薬","薬剤","ウイルス","感染","代謝","再生","幹細胞","抗体","分子生物","生理","内科","外科","精神","認知症","糖尿病"]),
    ("natural_science", ["化学反応","触媒","分子","原子","量子","物理","素粒子","超伝導","結晶","材料科学","半導体","光","レーザー","数理","数学","統計","天文","宇宙","地球","気候変動","地震","火山","海洋","大気"]),
    ("engineering", ["工学","機械","電気","電子","情報","AI","人工知能","ロボット","制御","通信","ネットワーク","エネルギー","材料","構造","土木","建築","デバイス","センサ","回路","アルゴリズム","計算機","ソフトウェア"]),
    ("environment", ["環境","生態","生物多様","森林","気候","再生可能","持続可能","水質","大気汚染","脱炭素","カーボン","水資源","河川","流域","保全"]),
    ("humanities_social", ["経済","経営","社会","法","政治","心理","歴史","哲学","文化人類","地域社会","教育制度","貧困","格差","労働","政策","国際関係","言語"]),
    ("arts_culture", ["芸術","美術","音楽","文学","デザイン","舞台","映像","文化財","伝統文化"]),
    ("welfare", ["福祉","介護","障害","高齢","児童","保健","看護","ケア"]),
]
def theme_cat(title):
    t = title or ""
    for cat, kws in THEME_KW:
        if any(k in t for k in kws): return cat
    return None

c = sqlite3.connect(CFG); c.row_factory = sqlite3.Row
out = {}

# === タスク3: KAKEN(科研費23.5万) 学術領域分布 ===
rid = sqlite3.connect(RID)
kaken55 = Counter(); kaken11 = Counter()
for (fj,) in rid.execute("SELECT agd_field_ja FROM rid_agd_field"):
    kaken55[fj] += 1; kaken11[AGD11.get(fj, "interdisciplinary")] += 1
out["kaken_field55"] = kaken55.most_common()
out["kaken_field11"] = kaken11.most_common()

# === タスク2 + A: 3階層 財団領域MAP(research_individual) ===
rows = c.execute("""SELECT r.rid_field, r.project_title, r.awardee_affiliation, r.award_amount, r.metadata, o.primary_field, o.name fname
    FROM grant_results r JOIN grant_calls gc ON r.call_id=gc.id JOIN grant_programs p ON gc.program_id=p.id JOIN organizations o ON p.organization_id=o.id
    WHERE r.grant_type='research_individual'""").fetchall()
L1_program = Counter(); L2_researcher55 = Counter(); L2_researcher11 = Counter(); L3_theme = Counter()
for r in rows:
    if r["primary_field"]: L1_program[r["primary_field"]] += 1
    if r["rid_field"]:
        L2_researcher55[r["rid_field"]] += 1; L2_researcher11[AGD11.get(r["rid_field"], "interdisciplinary")] += 1
    tc = theme_cat(r["project_title"])
    if tc: L3_theme[tc] += 1
out["foundation_map"] = {
    "n_records": len(rows),
    "L1_program11": L1_program.most_common(),          # プログラム階層(財団の助成領域)
    "L2_researcher55": L2_researcher55.most_common(),   # 研究者階層(AGD55・RID連携)
    "L2_researcher11": L2_researcher11.most_common(),   # 研究者階層(11大分類)
    "L3_theme11": L3_theme.most_common(),               # テーマ階層(課題名分類)
}

# === B: 財団 vs 科研費 領域MAP 照合(11大分類・正規化%) ===
def pct(counter):
    counter = counter.items() if hasattr(counter,"items") else counter
    tot = sum(n for _, n in counter) or 1
    return {k: round(100*n/tot, 2) for k, n in counter}
fnd11 = pct(L2_researcher11); kk11 = pct(kaken11)
comp = []
for cat in NAMEJA11:
    comp.append({"field": NAMEJA11[cat], "foundation_pct": fnd11.get(cat, 0), "kaken_pct": kk11.get(cat, 0),
                 "diff": round(fnd11.get(cat, 0) - kk11.get(cat, 0), 2)})
out["comparison_11"] = sorted(comp, key=lambda x: -x["foundation_pct"])

# === C: 大学分布(財団助成・全国+大学別) ===
fnd_univ = Counter(); fnd_pref = Counter()
for r in rows:
    u = univ(r["awardee_affiliation"])
    if u: fnd_univ[u] += 1; p = pref_of(u); (fnd_pref.update([p]) if p else None)
out["foundation_univ_top"] = fnd_univ.most_common(30)
out["foundation_pref"] = fnd_pref.most_common()
# KAKEN大学分布(rid_identity.institute_name)
kk_univ = Counter(); kk_pref = Counter()
for (ins,) in rid.execute("SELECT institute_name FROM rid_identity WHERE institute_name IS NOT NULL"):
    u = univ(ins)
    if u: kk_univ[u] += 1; p = pref_of(u); (kk_pref.update([p]) if p else None)
out["kaken_univ_top"] = kk_univ.most_common(30)
out["kaken_pref"] = kk_pref.most_common()

# === D: 応募要項パターン ===
elig = {}
for typ in ("age", "nationality", "position", "affiliation_type", "field"):
    elig[typ] = c.execute("SELECT description,COUNT(*) n FROM eligibility_criteria WHERE criterion_type=? AND description!='' GROUP BY description ORDER BY n DESC LIMIT 10", (typ,)).fetchall()
    elig[typ] = [(r[0], r[1]) for r in elig[typ]]
out["eligibility_patterns"] = elig

# === E: 金額分布 ===
# 個別採択額: award_amount(検証済) + metadata amount_hint(codex推定・別ラベル)
amts = [r["award_amount"] for r in rows if r["award_amount"] and r["award_amount"] > 0]
amts_hint = []
for r in rows:
    if not (r["award_amount"] and r["award_amount"] > 0):
        try:
            m = json.loads(dict(r).get("metadata") or "{}") if "metadata" in r.keys() else {}
        except Exception:
            m = {}
        h = m.get("amount_hint_unverified")
        if isinstance(h, int) and h > 0: amts_hint.append(h)
def bucket(a):
    if a < 500000: return "<50万"
    if a < 1000000: return "50-100万"
    if a < 3000000: return "100-300万"
    if a < 5000000: return "300-500万"
    if a < 10000000: return "500-1000万"
    return "1000万+"
amt_dist = Counter(bucket(a) for a in amts)
order = ["<50万","50-100万","100-300万","300-500万","500-1000万","1000万+"]
out["amount_award_dist"] = [(k, amt_dist.get(k, 0)) for k in order]
out["amount_award_stats"] = {"n": len(amts), "min": min(amts) if amts else 0, "max": max(amts) if amts else 0,
                             "median": sorted(amts)[len(amts)//2] if amts else 0}
hint_dist = Counter(bucket(a) for a in amts_hint)
out["amount_hint_dist"] = [(k, hint_dist.get(k, 0)) for k in order]
out["amount_hint_stats"] = {"n": len(amts_hint), "median": sorted(amts_hint)[len(amts_hint)//2] if amts_hint else 0}
# 財団年間助成額
fa = [r[0] for r in c.execute("SELECT annual_grant_amount FROM organizations WHERE annual_grant_amount>0").fetchall()]
def fbucket(a):
    if a < 50000000: return "<5000万"
    if a < 100000000: return "5000万-1億"
    if a < 500000000: return "1-5億"
    if a < 1000000000: return "5-10億"
    return "10億+"
fo = ["<5000万","5000万-1億","1-5億","5-10億","10億+"]
fd = Counter(fbucket(a) for a in fa)
out["amount_foundation_dist"] = [(k, fd.get(k, 0)) for k in fo]

# === E拡張: program単位 1件あたり助成額分布(codex収集・492件) ===
pa = c.execute("SELECT amount_per_award, num_awards_per_year FROM grant_amounts WHERE amount_per_award>0").fetchall()
pamts = [r[0] for r in pa]
def pbucket(a):
    if a < 500000: return "<50万"
    if a < 1000000: return "50-100万"
    if a < 2000000: return "100-200万"
    if a < 3000000: return "200-300万"
    if a < 5000000: return "300-500万"
    if a < 10000000: return "500-1000万"
    return "1000万+"
porder = ["<50万","50-100万","100-200万","200-300万","300-500万","500-1000万","1000万+"]
pd = Counter(pbucket(a) for a in pamts)
out["amount_per_award_program_dist"] = [(k, pd.get(k, 0)) for k in porder]
out["amount_per_award_program_stats"] = {"n": len(pamts), "median": sorted(pamts)[len(pamts)//2] if pamts else 0,
    "min": min(pamts) if pamts else 0, "max": max(pamts) if pamts else 0}

json.dump(out, open(OUT, "w"), ensure_ascii=False, indent=1)
# print summary
print("=== A: 財団領域MAP (n=%d research_individual) ===" % len(rows))
print("L2研究者(11大分類):", [(NAMEJA11.get(k,k), n) for k, n in L2_researcher11.most_common()])
print("\n=== B: 財団 vs 科研費 照合(上位・%) ===")
for x in out["comparison_11"][:6]: print("  %-12s 財団%.1f%% 科研費%.1f%% 差%+.1f" % (x["field"], x["foundation_pct"], x["kaken_pct"], x["diff"]))
print("\n=== C: 財団助成 大学分布 top8 ===")
for u, n in fnd_univ.most_common(8): print("  %-12s %d" % (u, n))
print("財団 都道府県 top8:", fnd_pref.most_common(8))
print("\n=== E: 個別採択額分布 ===", out["amount_award_dist"], "median", out["amount_award_stats"]["median"])
print("saved:", OUT)
