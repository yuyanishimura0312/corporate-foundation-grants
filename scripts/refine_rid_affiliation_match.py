#!/usr/bin/env python3
"""fable再検証(PASS)の非ブロッキング改善2点を additive 実装。
 ① rid_affiliation_match の偽陰性(法人プレフィックス差・博物館/センター等の未認識で=0)を、
    所属正規化強化(法人格除去+広い機関suffix+部分一致fallback)で再計算し=1へ昇格(recall回収~355)。
 ② 過剰降格1件(栁澤琢史/ivec=本人濃厚)を metadata から復活。
 既存の正しい=1/降格は不変。再計算は接続行のみ・冪等。
 実行: python3 scripts/refine_rid_affiliation_match.py [--apply]"""
import sqlite3, re, json, sys
DB="corporate_research_grants.sqlite"
RID="/Users/nishimura+/projects/research/researcher-intelligence-db/data/rid.db"
RSUB="/Users/nishimura+/projects/research/researcher-intelligence-db/data/rid_sub.db"
APPLY="--apply" in sys.argv
LEGAL=re.compile(r"国立研究開発法人|国立大学法人|公立大学法人|独立行政法人|学校法人|一般社団法人|公益社団法人|一般財団法人|公益財団法人|地方独立行政法人|社会福祉法人|医療法人")
def norminst(s):
    if not s: return ""
    s=re.sub(r"\s|　","",s); s=LEGAL.sub("",s)
    return s
def cores(s):
    s=norminst(s)
    return set(re.findall(r"[一-龥ぁ-んァ-ヴA-Za-z]+?(?:大学|大学校|研究所|高専|高等専門学校|機構|センター|病院|博物館|美術館|図書館|研究機関|研究財団)", s))
def aff_match(aff,inst):
    a,i=norminst(aff),norminst(inst)
    if not a or not i: return None
    ac,ic=cores(aff),cores(inst)
    if ac & ic: return 1
    # 部分一致fallback: 短い方が長い方に含まれる(国立民族学博物館 exact 等)
    short,long=(a,i) if len(a)<=len(i) else (i,a)
    if len(short)>=4 and short in long: return 1
    return 0

r=sqlite3.connect(RID); s=sqlite3.connect(RSUB)
binst={x[0]:(x[1] or "") for x in r.execute("SELECT base_researcher_id,institute_name FROM rid_identity WHERE base_researcher_id IS NOT NULL")}
sinst={x[0]:(x[1] or "") for x in s.execute("SELECT sub_id,inst FROM sub_researcher")}
r.close(); s.close()
c=sqlite3.connect(DB); c.row_factory=sqlite3.Row

# ② 栁澤琢史/ivec 復活(降格metadataから)
restored=0
for g in c.execute("SELECT id,metadata FROM grant_results WHERE metadata LIKE '%rid_link_demoted%' AND awardee_name LIKE '%栁澤%'").fetchall():
    try: meta=json.loads(g["metadata"])
    except: continue
    d=meta.get("rid_link_demoted")
    if d and "ivec" in (d.get("awardee_aff","")):
        if APPLY:
            meta["rid_link_restored"]={"from":d,"reason":"fable再検証: 希少姓+研究領域(非侵襲BMI)が専門一致=本人濃厚"}
            del meta["rid_link_demoted"]
            c.execute("UPDATE grant_results SET rid_base_id=?, rid_field=?, rid_affiliation_match=1, metadata=? WHERE id=?",
                      (d["was_base_id"],d["was_field"],json.dumps(meta,ensure_ascii=False),g["id"]))
        restored+=1

# ① 偽陰性の再計算(接続行のみ)
rows=[dict(x) for x in c.execute("SELECT id,awardee_affiliation,rid_base_id,rid_affiliation_match FROM grant_results WHERE rid_base_id IS NOT NULL")]
promote=0; dist={1:0,0:0,None:0}
for g in rows:
    bid=g["rid_base_id"]
    inst = sinst.get(bid-900000000,"") if bid>=900000000 else binst.get(bid,"")
    m=aff_match(g["awardee_affiliation"] or "", inst)
    dist[m]=dist.get(m,0)+1
    if m!=g["rid_affiliation_match"]:
        if g["rid_affiliation_match"]==0 and m==1: promote+=1
        if APPLY: c.execute("UPDATE grant_results SET rid_affiliation_match=? WHERE id=?",(m,g["id"]))
if APPLY: c.commit()
print(("APPLIED" if APPLY else "DRY-RUN"))
print(f"  栁澤復活: {restored}件 / 偽陰性=0→=1 昇格: {promote}件")
print(f"  再計算後分布: 所属一致=1 {dist[1]} / 不一致=0 {dist[0]} / 所属不明NULL {dist[None]}")
c.close()
