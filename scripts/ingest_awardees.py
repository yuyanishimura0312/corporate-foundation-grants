#!/usr/bin/env python3
"""Phase 5 ingest — fable検証後の codex採択者を grant_results へ投入 + RID連携。
   財団ごとに program/call を用意し awardees を grant_results に挿入。source_url必須・provenance記録。
   その後 RID(31.1万)へ名寄せし rid_base_id / rid_field を付与。Usage: [--apply]"""
import sqlite3, json, sys, re, unicodedata, uuid
CFG = "corporate_research_grants.sqlite"
RID = "/Users/nishimura+/projects/research/researcher-intelligence-db/data/rid.db"
STAGING = "research_results/codex_awardees_staging.json"
APPLY = "--apply" in sys.argv
NOW = "2026-07-15"
SKIP_AWARDEE_FOUNDATIONS = ("生命保険協会",)  # fable: 非研究助成(保育施設整備)・出典404
def nn(s): return re.sub(r'[\s　・]', '', unicodedata.normalize("NFKC", s or ""))
def base_inst(a):
    a = nn(a); m = re.match(r'^(.+?大学)', a) or re.match(r'^(.+?(研究所|機構|センター|大学校|病院))', a)
    return m.group(1) if m else a[:12]

c = sqlite3.connect(CFG); c.row_factory = sqlite3.Row
for t, col, typ in [("grant_results", "rid_base_id", "INTEGER"), ("grant_results", "rid_field", "TEXT"),
                    ("grant_results", "source_dataset", "TEXT")]:
    if col not in [r[1] for r in c.execute("PRAGMA table_info(%s)" % t)]:
        c.execute("ALTER TABLE %s ADD COLUMN %s %s" % (t, col, typ))

staging = json.load(open(STAGING)) if __import__("os").path.exists(STAGING) else {}
plan = {"foundations": 0, "awardees_inserted": 0, "skipped_no_source": 0, "skipped_no_name": 0, "programs_created": 0, "calls_created": 0}
for fname, rec in staging.items():
    cx = rec.get("codex")
    if not cx or not cx.get("awardees"): continue
    src = cx.get("source_url"); fy = cx.get("fiscal_year")
    if not src: plan["skipped_no_source"] += len(cx.get("awardees", [])); continue
    if any(x in fname for x in SKIP_AWARDEE_FOUNDATIONS): continue
    org = c.execute("SELECT id FROM organizations WHERE name=?", (fname,)).fetchone()
    if not org: continue
    oid = org["id"]
    if not APPLY:
        plan["foundations"] += 1; plan["awardees_inserted"] += len([a for a in cx["awardees"] if a.get("awardee_name")]); continue
    # get/create program
    pr = c.execute("SELECT id FROM grant_programs WHERE organization_id=? AND source_url=? LIMIT 1", (oid, src)).fetchone()
    if pr: pid = pr["id"]
    else:
        pid = str(uuid.uuid4())
        c.execute("INSERT INTO grant_programs (id,organization_id,name,category,source_url,metadata,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)",
                  (pid, oid, "研究助成", cx.get("awardees", [{}])[0].get("field_hint"), src,
                   json.dumps({"source": "codex-awardees", "collected_at": NOW}, ensure_ascii=False), NOW, NOW))
        plan["programs_created"] += 1
    # get/create call
    cl = c.execute("SELECT id FROM grant_calls WHERE program_id=? AND fiscal_year IS ? LIMIT 1", (pid, fy)).fetchone()
    if cl: cid = cl["id"]
    else:
        cid = str(uuid.uuid4())
        c.execute("INSERT INTO grant_calls (id,program_id,fiscal_year,title,status,source_url,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)",
                  (cid, pid, fy, "%s年度 研究助成" % (fy or ""), "closed", src, NOW, NOW))
        plan["calls_created"] += 1
    c.execute("DELETE FROM grant_results WHERE call_id=? AND source_dataset='codex_awardees_2026'", (cid,))
    for a in cx["awardees"]:
        name = (a.get("awardee_name") or "").strip()
        if not name: plan["skipped_no_name"] += 1; continue
        c.execute("""INSERT INTO grant_results (id,call_id,fiscal_year,awardee_name,awardee_affiliation,awardee_position,
                     project_title,award_amount,keywords,source_url,source_dataset,metadata,created_at,updated_at)
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                  (str(uuid.uuid4()), cid, fy or 0, name, a.get("affiliation"), a.get("position"),
                   a.get("project_title") or "(不明)", None, a.get("field_hint"), src, "codex_awardees_2026",
                   json.dumps({"confidence": cx.get("confidence"), "amount_hint_unverified": a.get("amount_jpy")}, ensure_ascii=False), NOW, NOW))
        plan["awardees_inserted"] += 1
    plan["foundations"] += 1
if APPLY: c.commit()

# RID linkage for codex-collected awardees
if APPLY:
    rid = sqlite3.connect(RID)
    from collections import defaultdict
    ridmap = defaultdict(list)
    for bid, nm, inst in rid.execute("SELECT base_researcher_id,name_ja,institute_name FROM rid_identity WHERE name_ja IS NOT NULL"):
        ridmap[nn(nm)].append((bid, inst or ""))
    field = {}
    for bid, fj in rid.execute("SELECT r.base_researcher_id,f.agd_field_ja FROM rid_agd_field f JOIN rid_identity r ON r.rid=f.rid"):
        field.setdefault(bid, fj)
    linked = 0
    for r in c.execute("SELECT id,awardee_name,awardee_affiliation FROM grant_results WHERE source_dataset='codex_awardees_2026' AND rid_base_id IS NULL").fetchall():
        cands = ridmap.get(nn(r["awardee_name"]), []); bid = None
        if len(cands) == 1: bid = cands[0][0]
        elif len(cands) > 1:
            af = base_inst(r["awardee_affiliation"]); m = [x for x in cands if af and (af in nn(x[1]) or nn(x[1])[:6] in af)]
            if len(m) == 1: bid = m[0][0]
        if bid:
            c.execute("UPDATE grant_results SET rid_base_id=?, rid_field=? WHERE id=?", (bid, field.get(bid), r["id"]))
            linked += 1
    c.commit(); plan["rid_linked"] = linked
print(("APPLIED" if APPLY else "DRY-RUN"), json.dumps(plan, ensure_ascii=False, indent=1))
