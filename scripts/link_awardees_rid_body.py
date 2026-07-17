#!/usr/bin/env python3
"""採択者→本体RID(rid_identity 235,521) 追加接続 + rid_field backfill。
   additive-only: rid_base_id が NULL の行のみ新規接続 / rid_field が NULL の行のみ補填。既存値は不変。
   保守的マッチ(捏造ゼロ):
     (1) 氏名 name_norm 完全一致 かつ 同名唯一(homonym_count=1)     -> 確実接続
     (2) 同名複数だが 所属(awardee_affiliation) の機関トークンが RID institute_name と一意一致 -> 接続
   企業/団体/海外/非大学は本体RID対象外(接続しない)。provenance を metadata に記録。
   実行: python3 scripts/link_awardees_rid_body.py [--apply]  (未指定=dry-run)"""
import sqlite3, re, json, sys, datetime
CFG="corporate_research_grants.sqlite"
RID="/Users/nishimura+/projects/research/researcher-intelligence-db/data/rid.db"
APPLY="--apply" in sys.argv

def norm(s):
    return re.sub(r"\s+","",s).replace("　","") if s else ""
def inst_tokens(s):
    if not s: return set()
    return set(re.findall(r"[一-龥ぁ-んァ-ヴA-Za-z]+?(?:大学|研究所|高専|機構|センター|病院|大学院)", norm(s)))

# --- RID本体 index ---
r=sqlite3.connect(RID)
rid_name={}          # name_norm -> [(base_id, institute, homonym_count, rid)]
for rid_,bid,nm,inst,hc in r.execute(
    "SELECT rid,base_researcher_id,name_norm,institute_name,homonym_count FROM rid_identity WHERE base_researcher_id IS NOT NULL"):
    if nm: rid_name.setdefault(nm,[]).append((bid,inst or "",hc or 1,rid_))
# base_id -> 最上位confidence の agd_field
field_of={}
for bid,fld in r.execute("""
    SELECT i.base_researcher_id, af.agd_field_ja FROM rid_identity i
    JOIN rid_agd_field af ON af.rid=i.rid
    WHERE i.base_researcher_id IS NOT NULL
    GROUP BY i.base_researcher_id
    HAVING af.confidence=MAX(af.confidence)"""):
    field_of.setdefault(bid,fld)
r.close()

c=sqlite3.connect(CFG); c.row_factory=sqlite3.Row
rows=[dict(x) for x in c.execute(
  "SELECT id,awardee_name,awardee_affiliation,awardee_type,rid_base_id,rid_field FROM grant_results")]

new_links=[]      # (id, base_id, field, method)
field_only=[]     # (id, field)  既接続だがfield欠落
for g in rows:
    if g["rid_base_id"] is None:
        nm=norm(g["awardee_name"]); cands=rid_name.get(nm)
        if not cands: continue
        pick=None; method=None
        if len(cands)==1 and cands[0][2]==1:
            pick=cands[0]; method="name_unique"
        else:
            atk=inst_tokens(g["awardee_affiliation"])
            hits=[cd for cd in cands if atk and (atk & inst_tokens(cd[1]))]
            if len(hits)==1:
                pick=hits[0]; method="name_affiliation"
        if pick:
            new_links.append((g["id"], pick[0], field_of.get(pick[0]), method))
    else:
        if not g["rid_field"]:
            f=field_of.get(g["rid_base_id"])
            if f: field_only.append((g["id"], f))

print(f"未接続からの新規接続候補 : {len(new_links)}  (name_unique={sum(1 for x in new_links if x[3]=='name_unique')} / name_affiliation={sum(1 for x in new_links if x[3]=='name_affiliation')})")
print(f"  うち rid_field も付与   : {sum(1 for x in new_links if x[2])}")
print(f"既接続の rid_field 補填   : {len(field_only)}")

if not APPLY:
    print("\n[dry-run] --apply で書き込み。サンプル:")
    for x in new_links[:5]:
        g=next(r for r in rows if r['id']==x[0])
        print(f"  {g['awardee_name']} @ {g['awardee_affiliation'][:20] if g['awardee_affiliation'] else ''} -> base {x[1]} [{x[2]}] ({x[3]})")
    c.close(); sys.exit()

ts=datetime.datetime.now().isoformat(timespec="seconds")
prov=json.dumps({"rid_link":{"src":"rid_identity","ts":ts}},ensure_ascii=False)
n1=n2=0
for gid,bid,fld,method in new_links:
    cur=c.execute("UPDATE grant_results SET rid_base_id=?, rid_field=COALESCE(rid_field,?), "
              "metadata=COALESCE(NULLIF(metadata,''), ?) WHERE id=? AND rid_base_id IS NULL",
              (bid,fld,prov,gid)); n1+=cur.rowcount
for gid,fld in field_only:
    cur=c.execute("UPDATE grant_results SET rid_field=? WHERE id=? AND (rid_field IS NULL OR rid_field='')",
              (fld,gid)); n2+=cur.rowcount
c.commit()
print(f"\n[applied] 新規接続 {n1} 行 / rid_field補填 {n2} 行")
tot=c.execute("SELECT COUNT(*) FROM grant_results").fetchone()[0]
linked=c.execute("SELECT COUNT(*) FROM grant_results WHERE rid_base_id IS NOT NULL").fetchone()[0]
fld=c.execute("SELECT COUNT(*) FROM grant_results WHERE rid_field IS NOT NULL AND rid_field<>''").fetchone()[0]
print(f"接続率 {linked}/{tot} = {100*linked/tot:.1f}%  | rid_field {fld}/{tot} = {100*fld/tot:.1f}%")
c.close()
