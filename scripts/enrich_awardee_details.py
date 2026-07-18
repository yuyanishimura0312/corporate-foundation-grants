#!/usr/bin/env python3
"""採択者 詳細補填 投入(Phase5)。collect_awardee_details.py の staging を、source_url+氏名で
   既存 grant_results に照合し、空欄(award_amount/awardee_position/project_abstract/award_period_*)のみ
   additive 充填。新規作成・上書きしない。捏造ゼロ・per-person金額健全性gate。
   Usage: python3 scripts/enrich_awardee_details.py [--apply]"""
import sqlite3, json, re, sys, datetime
DB="corporate_research_grants.sqlite"
STAGING="research_results/codex_awardee_details_staging.json"
APPLY="--apply" in sys.argv
def norm(s): return re.sub(r"\s|　","",s or "")
# per-person 助成額の妥当域: 1万円〜3億円(個人研究助成の常識的上限)。超過は review。
AMOUNT_MIN=10_000; AMOUNT_MAX=300_000_000

staging=json.load(open(STAGING))
c=sqlite3.connect(DB); c.row_factory=sqlite3.Row
NOW=datetime.datetime.now().isoformat(timespec="seconds")
plan={"amount":0,"position":0,"abstract":0,"period":0,"matched":0,"unmatched":0,"amount_flagged":0}
review=[]

for url,rec in staging.items():
    cx=rec.get("codex")
    if not cx: continue
    for aw in cx.get("awardees",[]):
        nm=norm(aw.get("awardee_name"))
        if not nm: continue
        # source_url + 正規化氏名 で既存採択者を照合
        cands=c.execute("SELECT id,award_amount,awardee_position,project_abstract,award_period_start,metadata,project_title FROM grant_results WHERE source_url=?",(url,)).fetchall()
        target=None
        for r in cands:
            if norm(r["project_title"]) and aw.get("project_title") and norm(r["project_title"])==norm(aw.get("project_title")):
                pass
        # 氏名一致(source_url内)。同名複数なら課題名も一致するものを優先
        matches=[r for r in cands if norm(c.execute("SELECT awardee_name FROM grant_results WHERE id=?",(r["id"],)).fetchone()[0])==nm]
        if len(matches)>1 and aw.get("project_title"):
            ti=norm(aw["project_title"]); m2=[r for r in matches if norm(r["project_title"] or "")==ti]
            if m2: matches=m2
        if not matches: plan["unmatched"]+=1; continue
        r=matches[0]; plan["matched"]+=1
        sets={}; prov={}
        amt=aw.get("amount_jpy")
        if r["award_amount"] is None and isinstance(amt,int) and amt>0:
            if AMOUNT_MIN<=amt<=AMOUNT_MAX:
                sets["award_amount"]=amt; prov["amount_src"]=url; plan["amount"]+=1
            else:
                plan["amount_flagged"]+=1; review.append({"url":url,"name":aw.get("awardee_name"),"amount":amt,"reason":"out_of_per_person_range"})
        pos=aw.get("position")
        if (r["awardee_position"] is None or r["awardee_position"]=="") and pos:
            sets["awardee_position"]=pos; plan["position"]+=1
        ab=aw.get("project_abstract")
        if (r["project_abstract"] is None or r["project_abstract"]=="") and ab and len(ab)>10:
            sets["project_abstract"]=ab; plan["abstract"]+=1
        ps=aw.get("period_start"); pe=aw.get("period_end")
        if r["award_period_start"] is None and (ps or pe):
            sets["award_period_start"]=ps; sets["award_period_end"]=pe; plan["period"]+=1
        if sets and APPLY:
            try: meta=json.loads(r["metadata"]) if r["metadata"] else {}
            except: meta={}
            meta.setdefault("detail_enrich",{}).update({**prov,"ts":NOW,"src":url})
            sets["metadata"]=json.dumps(meta,ensure_ascii=False)
            cols=",".join(f"{k}=?" for k in sets)
            c.execute(f"UPDATE grant_results SET {cols} WHERE id=?",(*sets.values(),r["id"]))

if APPLY:
    c.commit()
    json.dump(review,open("research_results/awardee_detail_review.json","w"),ensure_ascii=False,indent=1)
print(("APPLIED" if APPLY else "DRY-RUN"), json.dumps(plan,ensure_ascii=False))
if review: print(f"金額 per-person範囲外 flag: {len(review)}件(非投入)")
c.close()
