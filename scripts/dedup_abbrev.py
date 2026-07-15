#!/usr/bin/env python3
"""略記対応 重複統合 — （公財）等の略記 vs 公益財団法人 の形式差で生じた重複を統合。
   richな行を正本にし、権威フィールドをマージ、FK(grant_programs/foundation_focus_areas)をrepoint後、重複行を削除。
   Usage: python3 scripts/dedup_abbrev.py [--apply]  (default dry-run)"""
import sqlite3, re, unicodedata, sys, json
from collections import defaultdict
DB = "corporate_research_grants.sqlite"
APPLY = "--apply" in sys.argv

def norm2(s):
    if not s: return ""
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r'[（(](公財|公社|一財|一社|特非|社福|独|国|地独|福|学|宗|財|社|一|特|医|地財|学財)[）)]', '', s)
    s = re.sub(r'(公益|一般|特定非営利活動|認定特定非営利活動)?(財団|社団)?法人', '', s)
    s = s.replace('ー','').replace('・','')
    return re.sub(r'[\s　,，.。()（）「」『』]', '', s).strip().lower()

c = sqlite3.connect(DB); c.row_factory = sqlite3.Row
g = defaultdict(list)
for r in c.execute("SELECT id,name,source_dataset FROM organizations"):
    g[norm2(r["name"])].append(r["id"])
groups = [(k, v) for k, v in g.items() if len(v) > 1 and k]

def richness(oid):
    progs = c.execute("SELECT COUNT(*) FROM grant_programs WHERE organization_id=?", (oid,)).fetchone()[0]
    focus = c.execute("SELECT COUNT(*) FROM foundation_focus_areas WHERE organization_id=?", (oid,)).fetchone()[0]
    o = c.execute("SELECT established_year,total_assets,annual_grant_amount,jfc_rank,source_dataset FROM organizations WHERE id=?", (oid,)).fetchone()
    fin = sum(1 for x in (o["established_year"], o["total_assets"], o["annual_grant_amount"], o["jfc_rank"]) if x is not None)
    curated = 3 if o["source_dataset"] is None else 0
    return progs * 10 + focus * 3 + fin * 2 + curated

MERGE_COLS = ["koeki_verified", "koeki_matched_name", "admin_agency", "prefecture", "municipality",
              "established_year", "total_assets", "annual_grant_amount", "annual_grant_year", "jfc_rank",
              "url", "description", "name_en", "founder_name", "established_year", "primary_field",
              "primary_field_method", "research_relevance", "contact_address", "koeki_id"]
merged = 0; deleted = 0; log = []
for k, ids in groups:
    ids.sort(key=richness, reverse=True)
    keep = ids[0]; drops = ids[1:]
    keeprow = c.execute("SELECT * FROM organizations WHERE id=?", (keep,)).fetchone()
    keepd = dict(keeprow)
    for d in drops:
        drow = dict(c.execute("SELECT * FROM organizations WHERE id=?", (d,)).fetchone())
        fills = {}
        for col in MERGE_COLS:
            if (keepd.get(col) in (None, "")) and drow.get(col) not in (None, ""):
                fills[col] = drow[col]; keepd[col] = drow[col]
        log.append({"keep": keeprow["name"], "drop": drow["name"], "filled": list(fills.keys())})
        if APPLY:
            if fills:
                c.execute("UPDATE organizations SET %s WHERE id=?" % ",".join("%s=?" % x for x in fills),
                          (*fills.values(), keep))
            c.execute("UPDATE grant_programs SET organization_id=? WHERE organization_id=?", (keep, d))
            for fr in c.execute("SELECT category_id FROM foundation_focus_areas WHERE organization_id=?", (d,)).fetchall():
                exists = c.execute("SELECT 1 FROM foundation_focus_areas WHERE organization_id=? AND category_id=?", (keep, fr["category_id"])).fetchone()
                if exists:
                    c.execute("DELETE FROM foundation_focus_areas WHERE organization_id=? AND category_id=?", (d, fr["category_id"]))
                else:
                    c.execute("UPDATE foundation_focus_areas SET organization_id=? WHERE organization_id=? AND category_id=?", (keep, d, fr["category_id"]))
            c.execute("UPDATE cross_db_mapping SET cfg_id=? WHERE cfg_id=?", (keep, d))
            c.execute("DELETE FROM organizations WHERE id=?", (d,))
        merged += 1; deleted += 1
if APPLY: c.commit()
json.dump(log, open("research_results/dedup_abbrev_log.json", "w"), ensure_ascii=False, indent=1)
print(("APPLIED" if APPLY else "DRY-RUN"), "groups=%d merged=%d deleted=%d" % (len(groups), merged, deleted))
print("total organizations now:", c.execute("SELECT COUNT(*) FROM organizations").fetchone()[0])
for x in log[:8]: print("  keep:", x["keep"][:24], "| drop:", x["drop"][:24], "| filled:", x["filled"])
