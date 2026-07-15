#!/usr/bin/env python3
"""タスク1 — 採択者・役員・審査員 を研究者DB 31.1万人版(本体235,521 ∪ サブ76,054)に接続。
   AGD 55分野を付与。役員/審査員は研究者である者のみ連携(企業役員等は非連携=正)。"""
import sqlite3, re, unicodedata
from collections import defaultdict
CFG = "corporate_research_grants.sqlite"
RID = "/Users/nishimura+/projects/research/researcher-intelligence-db/data/rid.db"
SUB = "/Users/nishimura+/projects/research/researcher-intelligence-db/data/rid_sub.db"

def nn(s): return re.sub(r'[\s　・]', '', unicodedata.normalize("NFKC", s or ""))
def bi(a):
    a = nn(a); m = re.match(r'^(.+?大学)', a) or re.match(r'^(.+?(研究所|機構|センター|大学校|病院|高専))', a)
    return m.group(1) if m else a[:10]

# --- build unified 31.1万 index: name_norm -> [(source, key, inst, field)] ---
idx = defaultdict(list)
rid = sqlite3.connect(RID)
field_main = {}
for bid, fj in rid.execute("SELECT r.base_researcher_id, f.agd_field_ja FROM rid_agd_field f JOIN rid_identity r ON r.rid=f.rid"):
    field_main.setdefault(bid, fj)
for bid, nm, inst in rid.execute("SELECT base_researcher_id,name_ja,institute_name FROM rid_identity WHERE name_ja IS NOT NULL"):
    idx[nn(nm)].append(("main", bid, inst or "", field_main.get(bid)))
n_main = sum(len(v) for v in idx.values())
sub = sqlite3.connect(SUB)
field_sub = {}
for sid, fj in sub.execute("SELECT sub_id, agd_field_ja FROM sub_field_class"):
    field_sub.setdefault(sid, fj)
SUB_OFFSET = 900_000_000
for sid, nm, inst in sub.execute("SELECT sub_id,name,inst FROM sub_researcher WHERE name IS NOT NULL"):
    idx[nn(nm)].append(("sub", SUB_OFFSET + sid, inst or "", field_sub.get(sid)))
print("31.1万 index: main=%d sub=%d 総エントリ=%d uniq名=%d" % (n_main, sum(len(v) for v in idx.values()) - n_main, sum(len(v) for v in idx.values()), len(idx)))

def match(name, affil):
    cands = idx.get(nn(name), [])
    if len(cands) == 1: return cands[0]
    if len(cands) > 1 and affil:
        af = bi(affil)
        m = [c for c in cands if af and (af in nn(c[2]) or nn(c[2])[:6] in af)]
        if len(m) == 1: return m[0]
    return None

c = sqlite3.connect(CFG)
# --- awardees: extend to 31.1万 (add sub matches) ---
c.execute("UPDATE grant_results SET rid_base_id=NULL, rid_field=NULL")  # rebuild fully on 31.1万
aw_linked = aw_sub = 0
for gid, nm, af in c.execute("SELECT id,awardee_name,awardee_affiliation FROM grant_results WHERE awardee_name IS NOT NULL").fetchall():
    r = match(nm, af)
    if r:
        c.execute("UPDATE grant_results SET rid_base_id=?, rid_field=? WHERE id=?", (r[1], r[3], gid))
        aw_linked += 1
        if r[0] == "sub": aw_sub += 1
# --- officers/reviewers: add rid columns + link (researcher only) ---
for col in ("rid_base_id", "rid_field"):
    if col not in [x[1] for x in c.execute("PRAGMA table_info(foundation_officers)")]:
        c.execute("ALTER TABLE foundation_officers ADD COLUMN %s %s" % (col, "INTEGER" if col == "rid_base_id" else "TEXT"))
of_linked = of_sub = 0
for oid, nm, af in c.execute("SELECT id,person_name,affiliation FROM foundation_officers WHERE person_name IS NOT NULL").fetchall():
    r = match(nm, af)
    if r:
        c.execute("UPDATE foundation_officers SET rid_base_id=?, rid_field=? WHERE id=?", (r[1], r[3], oid))
        of_linked += 1
        if r[0] == "sub": of_sub += 1
c.commit()
aw_tot = c.execute("SELECT COUNT(*) FROM grant_results WHERE awardee_name IS NOT NULL").fetchone()[0]
of_tot = c.execute("SELECT COUNT(*) FROM foundation_officers").fetchone()[0]
rv_tot = c.execute("SELECT COUNT(*) FROM foundation_officers WHERE role='reviewer'").fetchone()[0]
rv_link = c.execute("SELECT COUNT(*) FROM foundation_officers WHERE role='reviewer' AND rid_base_id IS NOT NULL").fetchone()[0]
print("採択者 RID連携 %d/%d (%.0f%%) うちサブ%d" % (aw_linked, aw_tot, 100*aw_linked/aw_tot, aw_sub))
print("役員   RID連携 %d/%d (%.0f%%) うちサブ%d (企業役員等は非連携=正)" % (of_linked, of_tot, 100*of_linked/of_tot, of_sub))
print("審査員 RID連携 %d/%d (%.0f%%)" % (rv_link, rv_tot, 100*rv_link/max(rv_tot,1)))
