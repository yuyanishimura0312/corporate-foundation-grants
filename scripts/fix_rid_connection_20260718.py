#!/usr/bin/env python3
"""fable接続監査(2026-07-18)の是正。CFG採択者×研究者DB31.1万人版の誤接続を additive/可逆で是正。
 (A) 全接続に rid_affiliation_match 列(1=所属core一致 / 0=不一致 / NULL=所属不明) を付与。
     学術利用は rid_affiliation_match=1 に絞れる(fable推奨)。
 (B) 別人確定を降格: 非学術所属(企業/小中高/病院)の採択者→学術研究者(大学/研究所)への接続は
     氏名のみ一致の別人濃厚 → rid_base_id/rid_field を NULL化(metadataに降格前値を保全=可逆)。
 改称・統合(東京科学大/昭和医科大 等)は所属不一致でも別人でないので降格しない。additive・捏造しない。
 実行: python3 scripts/fix_rid_connection_20260718.py [--apply]"""
import sqlite3, re, json, sys, datetime
DB="corporate_research_grants.sqlite"
RID="/Users/nishimura+/projects/research/researcher-intelligence-db/data/rid.db"
RSUB="/Users/nishimura+/projects/research/researcher-intelligence-db/data/rid_sub.db"
APPLY="--apply" in sys.argv
def cores(s):
    if not s: return set()
    return set(re.findall(r"[一-龥ぁ-んァ-ヴA-Za-z]+?(?:大学|大学校|研究所|高専|高等専門学校|機構|研究センター|病院|医療センター)", re.sub(r"\s|　","",s)))
NONACAD=re.compile(r"株式会社|（株）|\(株\)|小学校|中学校|高等学校|高校|クリニック|診療所")
ACAD=re.compile(r"大学|研究所|高専|機構|研究センター")

r=sqlite3.connect(RID); s=sqlite3.connect(RSUB)
binst={x[0]:(x[1] or "") for x in r.execute("SELECT base_researcher_id,institute_name FROM rid_identity WHERE base_researcher_id IS NOT NULL")}
sinst={x[0]:(x[1] or "") for x in s.execute("SELECT sub_id,inst FROM sub_researcher")}
r.close(); s.close()

c=sqlite3.connect(DB); c.row_factory=sqlite3.Row
cols=[x[1] for x in c.execute("PRAGMA table_info(grant_results)")]
if "rid_affiliation_match" not in cols and APPLY:
    c.execute("ALTER TABLE grant_results ADD COLUMN rid_affiliation_match INTEGER")
NOW=datetime.datetime.now().isoformat(timespec="seconds")
rows=[dict(x) for x in c.execute("SELECT id,awardee_affiliation,rid_base_id,rid_field,metadata FROM grant_results WHERE rid_base_id IS NOT NULL")]
plan={"match1":0,"match0":0,"match_null":0,"demoted":0}
for g in rows:
    bid=g["rid_base_id"]; aff=g["awardee_affiliation"] or ""
    inst = sinst.get(bid-900000000,"") if bid>=900000000 else binst.get(bid,"")
    ac,ic=cores(aff),cores(inst)
    if not aff.strip(): m=None; plan["match_null"]+=1
    elif ac & ic: m=1; plan["match1"]+=1
    else: m=0; plan["match0"]+=1
    # 降格: 非学術所属→学術研究者・所属core非一致 = 別人確定
    demote = (m==0 and NONACAD.search(aff) and ACAD.search(inst))
    if APPLY:
        if demote:
            try: meta=json.loads(g["metadata"]) if g["metadata"] else {}
            except: meta={}
            meta["rid_link_demoted"]={"was_base_id":bid,"was_field":g["rid_field"],"reason":"non_academic_awardee->academic_researcher (fable別人確定)","awardee_aff":aff[:60],"rid_inst":inst[:60],"ts":NOW}
            c.execute("UPDATE grant_results SET rid_base_id=NULL, rid_field=NULL, rid_affiliation_match=NULL, metadata=? WHERE id=?",(json.dumps(meta,ensure_ascii=False),g["id"]))
        else:
            c.execute("UPDATE grant_results SET rid_affiliation_match=? WHERE id=?",(m,g["id"]))
    if demote: plan["demoted"]+=1
if APPLY: c.commit()
print(("APPLIED" if APPLY else "DRY-RUN"), json.dumps(plan,ensure_ascii=False))
if APPLY:
    tot=c.execute("SELECT COUNT(*) FROM grant_results WHERE rid_base_id IS NOT NULL").fetchone()[0]
    m1=c.execute("SELECT COUNT(*) FROM grant_results WHERE rid_affiliation_match=1").fetchone()[0]
    print(f"接続 {tot} / うち所属一致(rid_affiliation_match=1) {m1} = 学術利用推奨サブセット")
c.close()
