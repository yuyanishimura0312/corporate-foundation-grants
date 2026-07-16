#!/usr/bin/env python3
"""戦略分析 — 企業財団が最大インパクト(追加性)を起こすために集中支援すべき研究者カテゴリー特定。
   財団助成 vs 科研費(研究者母集団)のギャップを 分野×地域×キャリア×機関 で定量化。全数実測。"""
import sqlite3, re, unicodedata, json
from collections import Counter
CFG = "corporate_research_grants.sqlite"
RID = "/Users/nishimura+/projects/research/researcher-intelligence-db/data/rid.db"
c = sqlite3.connect(CFG); rid = sqlite3.connect(RID)
def nn(s): return re.sub(r'[\s　・]', '', unicodedata.normalize("NFKC", s or ""))
AGD11 = json.load(open("research_results/grant_field_map.json"))  # reuse mapping via comparison
NAME11 = {"natural_science":"自然科学","life_science":"生命科学・医学","engineering":"工学・技術","humanities_social":"人文社会科学","arts_culture":"芸術・文化","education":"教育・人材育成","welfare":"福祉・健康","environment":"環境","international":"国際交流・協力","regional":"地域","interdisciplinary":"学際・融合"}
out = {}

# ========== 1. 分野の需給ギャップ(企業財団は理系に偏るが、母集団比の助成強度で見る) ==========
# 財団助成 55分野別件数(企業財団のみ=foundation_subtype corporate/group + 研究助成individual)
fnd55 = Counter()
for (f,) in c.execute("""SELECT r.rid_field FROM grant_results r JOIN grant_calls gc ON r.call_id=gc.id
    JOIN grant_programs p ON gc.program_id=p.id JOIN organizations o ON p.organization_id=o.id
    WHERE r.grant_type='research_individual' AND r.rid_field IS NOT NULL"""):
    fnd55[f] += 1
# 科研費 55分野別研究者数(母集団)
kk55 = Counter()
for (f,) in rid.execute("SELECT agd_field_ja FROM rid_agd_field"):
    kk55[f] += 1
# 助成強度 = 財団助成件数 / 科研費研究者数(1000人あたり) — 低い=手薄=追加性大
Ftot = sum(fnd55.values()); Ktot = sum(kk55.values())
intensity = []
for f in kk55:
    if kk55[f] < 500: continue  # 母集団が薄い分野は除外(安定性)
    fs = 100*fnd55.get(f,0)/Ftot; ks = 100*kk55[f]/Ktot
    inten = 1000*fnd55.get(f,0)/kk55[f]  # 研究者1000人あたり財団助成件数
    intensity.append({"field": f, "fnd_grants": fnd55.get(f,0), "kaken_researchers": kk55[f],
                      "fnd_share": round(fs,2), "kaken_share": round(ks,2), "gap": round(fs-ks,2), "intensity_per1000": round(inten,2)})
intensity.sort(key=lambda x: x["intensity_per1000"])
out["field_intensity"] = intensity

# ========== 2. 地域の需給ギャップ(財団助成 vs 研究者母集団を都道府県で) ==========
PREF = "北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
UNIV_PREF = json.loads(open("research_results/univ_pref.json").read()) if __import__("os").path.exists("research_results/univ_pref.json") else {}
# 主要大学→県(grant_field_map.pyと同一の静的マップを再利用するため簡易内蔵)
UP = {"東京大学":"東京都","京都大学":"京都府","大阪大学":"大阪府","東北大学":"宮城県","名古屋大学":"愛知県","九州大学":"福岡県","北海道大学":"北海道","筑波大学":"茨城県","慶應義塾大学":"東京都","早稲田大学":"東京都","東京科学大学":"東京都","東京工業大学":"東京都","神戸大学":"兵庫県","広島大学":"広島県","岡山大学":"岡山県","金沢大学":"石川県","千葉大学":"千葉県","新潟大学":"新潟県","熊本大学":"熊本県","長崎大学":"長崎県","信州大学":"長野県","岐阜大学":"岐阜県","三重大学":"三重県","静岡大学":"静岡県","山口大学":"山口県","鹿児島大学":"鹿児島県","徳島大学":"徳島県","愛媛大学":"愛媛県","富山大学":"富山県","群馬大学":"群馬県","埼玉大学":"埼玉県","横浜国立大学":"神奈川県","横浜市立大学":"神奈川県","大阪公立大学":"大阪府","名古屋工業大学":"愛知県","理化学研究所":"埼玉県","東京理科大学":"東京都","日本大学":"東京都","近畿大学":"大阪府","立命館大学":"京都府","同志社大学":"京都府","順天堂大学":"東京都","昭和大学":"東京都","山形大学":"山形県","弘前大学":"青森県","岩手大学":"岩手県","秋田大学":"秋田県","宇都宮大学":"栃木県","茨城大学":"茨城県","山梨大学":"山梨県","福井大学":"福井県","鳥取大学":"鳥取県","島根大学":"島根県","香川大学":"香川県","高知大学":"高知県","佐賀大学":"佐賀県","大分大学":"大分県","宮崎大学":"宮崎県","琉球大学":"沖縄県","滋賀大学":"滋賀県","和歌山大学":"和歌山県"}
def univ(a):
    a=nn(a); m=re.match(r'^(.+?大学)',a) or re.match(r'^(.+?(研究所|機構|センター|大学校|高専))',a); return m.group(1) if m else None
def pref_of(u):
    if not u: return None
    if u in UP: return UP[u]
    for k,v in UP.items():
        if k in u: return v
    return None
fnd_pref = Counter(); kk_pref = Counter()
for (a,) in c.execute("SELECT awardee_affiliation FROM grant_results WHERE grant_type='research_individual' AND awardee_affiliation IS NOT NULL"):
    p=pref_of(univ(a)); (fnd_pref.update([p]) if p else None)
for (ins,) in rid.execute("SELECT institute_name FROM rid_identity WHERE institute_name IS NOT NULL"):
    p=pref_of(univ(ins)); (kk_pref.update([p]) if p else None)
Fp=sum(fnd_pref.values()); Kp=sum(kk_pref.values())
pref_gap=[]
for p in kk_pref:
    if kk_pref[p]<200: continue
    fs=100*fnd_pref.get(p,0)/Fp; ks=100*kk_pref[p]/Kp
    inten=1000*fnd_pref.get(p,0)/kk_pref[p]
    pref_gap.append({"pref":p,"fnd":fnd_pref.get(p,0),"kaken":kk_pref[p],"fnd_share":round(fs,2),"kaken_share":round(ks,2),"gap":round(fs-ks,2),"intensity":round(inten,2)})
pref_gap.sort(key=lambda x:x["intensity"])
out["pref_intensity"]=pref_gap

# ========== 3. キャリア段階(採択者の職位分布) ==========
pos=Counter()
for (p,) in c.execute("SELECT awardee_position FROM grant_results WHERE grant_type='research_individual' AND awardee_position IS NOT NULL AND awardee_position!=''"):
    pn=re.sub(r'[（(].*','',nn(p))[:8]; pos[pn]+=1
out["career_positions"]=pos.most_common(12)

# ========== 4. 機関ティア(旧帝大 vs 地方国立 vs 私立) ==========
KYUTEI={"東京大学","京都大学","大阪大学","東北大学","名古屋大学","九州大学","北海道大学"}
tier=Counter()
for (a,) in c.execute("SELECT awardee_affiliation FROM grant_results WHERE grant_type='research_individual' AND awardee_affiliation IS NOT NULL"):
    u=univ(a)
    if not u: tier["その他/不明"]+=1
    elif u in KYUTEI: tier["旧帝大7"]+=1
    elif "大学" in u: tier["その他大学(地方国立・私立等)"]+=1
    else: tier["研究機関等"]+=1
out["institution_tier"]=tier.most_common()

json.dump(out, open("research_results/strategic_impact.json","w"), ensure_ascii=False, indent=1)
# print
print("=== 1. 分野別 助成強度(研究者1000人あたり財団助成件数・低い=手薄=追加性大) ===")
print("【手薄TOP8=追加性大】")
for x in intensity[:8]: print("  %-8s 強度%.1f (財団%d件/科研費%d人・gap%+.1f)"%(x["field"],x["intensity_per1000"],x["fnd_grants"],x["kaken_researchers"],x["gap"]))
print("【厚いTOP5=既に集中】")
for x in intensity[-5:]: print("  %-8s 強度%.1f (財団%d件/科研費%d人・gap%+.1f)"%(x["field"],x["intensity_per1000"],x["fnd_grants"],x["kaken_researchers"],x["gap"]))
print("\n=== 2. 地域別 助成強度(手薄=地方の追加性大) ===")
for x in pref_gap[:10]: print("  %-5s 強度%.1f (財団%d/科研費%d人・gap%+.1f)"%(x["pref"],x["intensity"],x["fnd"],x["kaken"],x["gap"]))
print("\n=== 3. キャリア段階 ===", out["career_positions"][:8])
print("\n=== 4. 機関ティア ===", out["institution_tier"])
