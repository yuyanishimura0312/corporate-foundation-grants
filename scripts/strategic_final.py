#!/usr/bin/env python3
"""戦略分析 最終版 — 強化分類器(未分類18.8%)で分野別まで精緻化 + ティア代表性ギャップ + 交差。
   企業財団6,289採択者。fable是正済(corporate限定・科研費非依存分野・honest labeling)。"""
import sqlite3, re, unicodedata, json
from collections import Counter
CFG = "corporate_research_grants.sqlite"
RID = "/Users/nishimura+/projects/research/researcher-intelligence-db/data/rid.db"
c = sqlite3.connect(CFG); c.row_factory = sqlite3.Row; rid = sqlite3.connect(RID)
def nn(s): return re.sub(r'[\s　・]', '', unicodedata.normalize("NFKC", s or ""))

# 強化分類器を読み込み(classify_theme_v2 の RID2FINE + THEME を再利用)
import importlib.util
spec = importlib.util.spec_from_file_location("ctv2", "scripts/classify_theme_v2.py")
# 直接importせず定義を再現(exec回避のため必要な辞書だけコピー)
RID2FINE = {"西洋医学":"臨床医学","東洋医学":"臨床医学","生理学":"生理・代謝","薬学":"創薬・薬学","生化学":"生化学・分子生物","生命工学":"生命工学","生物学":"生物学・生態","生態学":"生物学・生態","化学":"化学・材料","物理学":"物理・数理","数学":"物理・数理","統計学":"物理・数理","天文学":"地球・宇宙","地球科学":"地球・宇宙","地質学":"地球・宇宙","気象学":"地球・宇宙","機械工学":"工学・デバイス","電気工学":"工学・デバイス","土木工学":"建築・土木","建築学":"建築・土木","情報理論":"情報・AI","計算機科学":"情報・AI","農学":"農・食・生物","経済学":"人文・社会","経営学":"人文・社会","社会学":"人文・社会","政治学":"人文・社会","法学":"人文・社会","心理学":"人文・社会","人類学":"人文・社会","地理学":"人文・社会","歴史学":"人文・社会","哲学":"人文・社会","倫理学":"人文・社会","文学":"人文・社会","言語学":"人文・社会","教育学":"教育","美学":"芸術文化","芸術理論":"芸術文化","音楽学":"芸術文化"}
THEME = [("臨床医学",["がん","癌","腫瘍","白血病","悪性","循環器","心臓","心筋","腎","肝臓","肝","糖尿","生活習慣病","高血圧","動脈","診断","治療","病態","疾患","症","患者","医療","臨床","外科","内科","病院","加齢","老化","胎盤","卵子","妊娠"]),("免疫・感染",["免疫","感染","ウイルス","細菌","ワクチン","炎症","抗体","アレルギー","マクロファージ","T細胞","自己免疫"]),("脳・神経",["脳","神経","認知","精神","ニューロン","シナプス","記憶","睡眠","うつ","脳梗塞","グリア","神経変性"]),("生化学・分子生物",["遺伝子","ゲノム","DNA","RNA","タンパク","蛋白","ペプチド","分子生物","エピゲノム","染色体","酵素","翻訳","転写","発現","代謝経路"]),("再生・幹細胞",["再生","幹細胞","iPS","ES細胞","組織工学","オルガノイド","分化"]),("創薬・薬学",["創薬","薬剤","薬物","製剤","薬効","医薬","化合物","ドラッグ","天然物"]),("生理・代謝",["代謝","生理","ホルモン","栄養","レニン","アンジオテンシン","脂質","エネルギー代謝"]),("化学・材料",["化学","触媒","高分子","材料","結晶","合成","重合","電解","分子設計","ナノ","錯体","有機","無機","ポリマー","ヘリセン","ラジカル","キラル","分子","化合","反応","超分子","液晶"]),("物理・数理",["物理","量子","素粒子","光","レーザー","数理","数学","統計力学","スピン","磁性","超伝導","トポロジ","対称性","振動"]),("情報・AI",["情報","AI","人工知能","機械学習","深層学習","計算科学","アルゴリズム","データ科学","ロボット","ソフトウェア","ネットワーク","暗号","量子計算"]),("工学・デバイス",["工学","機械","電気","電子","デバイス","センサ","半導体","回路","制御","マイクロ","MEMS","アクチュエータ","駆動"]),("建築・土木",["建築","都市","まちづくり","住宅","土木","構造物","建物","景観","居住","メタボリズム","木造","耐震","インフラ","公共施設","空間"]),("エネ・環境",["エネルギー","電池","太陽","水素","燃料","脱炭素","カーボン","環境","再生可能","廃棄物","リサイクル","水質","大気"]),("農・食・生物",["農","食品","食料","植物","作物","水産","森林","生態","微生物","昆虫","動物","魚","アザラシ","進化","多様性","品種"]),("地球・宇宙",["地球","宇宙","天文","海洋","地質","気象","惑星","気候","地震","火山","グリーンランド","極域"]),("人文・社会",["経済","社会","歴史","哲学","文学","法","政治","心理","教育","芸術","文化","言語","宗教","特許","ツーリズム","観光","記憶","災害","イノベーション","政策","貧困","地域社会","労働"])]
def theme(t):
    t=t or ""
    for name,kws in THEME:
        if any(k in t for k in kws): return name
    return None
def field(rid_field, title):
    if rid_field and rid_field in RID2FINE: return RID2FINE[rid_field]
    return theme(title) or "その他・未分類"
KYUTEI={"東京大学","京都大学","大阪大学","東北大学","名古屋大学","九州大学","北海道大学"}
def univ(a):
    a=nn(a); a=re.sub(r'^(国立大学法人|公立大学法人|学校法人)','',a)
    m=re.match(r'^(.+?大学)',a) or re.match(r'^(.+?(研究所|機構|センター|大学校|高専))',a); return m.group(1) if m else None
def tier(a):
    u=univ(a)
    if not u: return None
    if u in KYUTEI: return "旧帝大7"
    if u.endswith("大学"): return "非旧帝大"
    return "研究機関等"
YOUNG=("助教","助手","ポスドク","博士","特任助","講師")
def young(p):
    p=nn(p or "")
    if not p: return None
    return "若手" if any(y in p for y in YOUNG) and "主任" not in p and "上席" not in p else "中堅以上"

rows=c.execute("""SELECT r.rid_field,r.project_title,r.awardee_affiliation,r.awardee_position
  FROM grant_results r JOIN grant_calls gc ON r.call_id=gc.id JOIN grant_programs p ON gc.program_id=p.id
  JOIN organizations o ON p.organization_id=o.id WHERE r.grant_type='research_individual' AND o.foundation_subtype IN ('corporate','group')""").fetchall()
out={"n":len(rows)}
# 分野分布(精緻)
fd=Counter(field(r["rid_field"],r["project_title"]) for r in rows)
out["field_dist"]=fd.most_common()
# 分野×ティア(非旧帝大の追加性を分野別に)
ft=Counter()
for r in rows:
    f=field(r["rid_field"],r["project_title"]); t=tier(r["awardee_affiliation"])
    if t: ft[(f,t)]+=1
# 各分野の非旧帝大比率(低い=旧帝大偏重=非旧帝大若手に追加余地)
field_tier=[]
for f in [x for x,_ in fd.most_common(12) if x!="その他・未分類"]:
    kyu=ft.get((f,"旧帝大7"),0); hik=ft.get((f,"非旧帝大"),0); ken=ft.get((f,"研究機関等"),0)
    tot=kyu+hik+ken
    if tot<30: continue
    field_tier.append({"field":f,"total":tot,"旧帝大%":round(100*kyu/tot,1),"非旧帝大%":round(100*hik/tot,1),"研究機関%":round(100*ken/tot,1)})
out["field_tier"]=field_tier
# 分野×ティア×若手 交差(実装単位・未分類/不明を除く)
cross=Counter()
for r in rows:
    f=field(r["rid_field"],r["project_title"]); t=tier(r["awardee_affiliation"]); y=young(r["awardee_position"])
    if f!="その他・未分類" and t and y: cross[(f,t,y)]+=1
out["cross"]=[{"field":k[0],"tier":k[1],"career":k[2],"n":v} for k,v in cross.most_common(15)]
# 非旧帝大×若手の分野別(集中支援の実装候補)
target=Counter()
for r in rows:
    f=field(r["rid_field"],r["project_title"]); t=tier(r["awardee_affiliation"]); y=young(r["awardee_position"])
    if f!="その他・未分類" and t=="非旧帝大" and y=="若手": target[f]+=1
out["target_nonkyutei_young"]=target.most_common(12)
json.dump(out,open("research_results/strategic_final.json","w"),ensure_ascii=False,indent=1)
print("企業財団採択者",len(rows),"/ 分野分布(精緻・未分類18.8%):")
for k,v in fd.most_common(10): print("  %-16s %d (%.1f%%)"%(k,v,100*v/len(rows)))
print("\n=== 分野別 機関ティア構成(非旧帝大%が低い=旧帝大偏重=追加余地大) ===")
for x in sorted(field_tier,key=lambda z:z["非旧帝大%"])[:8]: print("  %-16s 旧帝%.0f%% 非旧帝%.0f%% (n=%d)"%(x["field"],x["旧帝大%"],x["非旧帝大%"],x["total"]))
print("\n=== 集中支援 実装候補: 非旧帝大×若手 の分野別TOP ===")
for k,v in target.most_common(10): print("  %-16s %d件"%(k,v))
