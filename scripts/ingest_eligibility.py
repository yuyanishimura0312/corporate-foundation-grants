#!/usr/bin/env python3
"""応募要項拡張投入 — fable検証後の codex応募資格を eligibility_criteria へ。
   財団ごとにgrant_call(なければ最新)にひも付け。名前照合ゲート・出典必須。type別に展開。Usage: [--apply]"""
import sqlite3, json, sys, os, re, unicodedata, uuid
CFG = "corporate_research_grants.sqlite"
STAGING = "research_results/codex_eligibility_staging.json"
APPLY = "--apply" in sys.argv
NOW = "2026-07-16"
def nm(s):
    s = re.sub(r'(公益|一般|認定)?(財団|社団|特定非営利活動)?法人|株式会社|旧[：:].*|（.*?）|\(.*?\)', '', unicodedata.normalize("NFKC", s or ""))
    return re.sub(r'[\s　・,，.。「」]', '', s).strip().lower()
# eligibility項目 → criterion_type
FIELD_TYPE = {"age_limit": "age", "career_stage": "career_stage", "nationality": "nationality",
              "field_scope": "field", "gender_scope": "gender", "other_requirements": "other"}
c = sqlite3.connect(CFG); c.row_factory = sqlite3.Row
staging = json.load(open(STAGING)) if os.path.exists(STAGING) else {}
plan = {"foundations": 0, "criteria_added": 0, "misattr": 0, "no_call": 0, "source_dataset": "codex_eligibility_2026"}
# provenance列(冪等)
if "source_dataset" not in [r[1] for r in c.execute("PRAGMA table_info(eligibility_criteria)")]:
    if APPLY: c.execute("ALTER TABLE eligibility_criteria ADD COLUMN source_dataset TEXT")
if "source_url" not in [r[1] for r in c.execute("PRAGMA table_info(eligibility_criteria)")]:
    if APPLY: c.execute("ALTER TABLE eligibility_criteria ADD COLUMN source_url TEXT")
for fname, rec in staging.items():
    cx = rec.get("codex")
    if not cx or not cx.get("eligibility"): continue
    el = cx["eligibility"]
    if not any(el.values()): continue
    if nm(cx.get("foundation_name")) and nm(fname) and nm(cx["foundation_name"]) != nm(fname) \
       and nm(cx["foundation_name"]) not in nm(fname) and nm(fname) not in nm(cx["foundation_name"]):
        plan["misattr"] += 1; continue
    src = cx.get("source_url")
    org = c.execute("SELECT id FROM organizations WHERE name=?", (fname,)).fetchone()
    if not org: continue
    oid = org["id"]
    # 対象call: 該当財団の研究助成callの最新(なければ新規)
    call = c.execute("""SELECT gc.id FROM grant_calls gc JOIN grant_programs p ON gc.program_id=p.id
        WHERE p.organization_id=? ORDER BY gc.fiscal_year DESC NULLS LAST LIMIT 1""", (oid,)).fetchone()
    if call: cid = call["id"]
    else:
        plan["no_call"] += 1
        if not APPLY: continue
        pr = c.execute("SELECT id FROM grant_programs WHERE organization_id=? LIMIT 1", (oid,)).fetchone()
        if not pr:
            pid = str(uuid.uuid4()); c.execute("INSERT INTO grant_programs (id,organization_id,name,created_at,updated_at) VALUES (?,?,?,?,?)", (pid, oid, "研究助成", NOW, NOW))
        else: pid = pr["id"]
        cid = str(uuid.uuid4()); c.execute("INSERT INTO grant_calls (id,program_id,fiscal_year,title,status,source_url,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)",
                                           (cid, pid, cx.get("fiscal_year"), "研究助成", "unknown", src, NOW, NOW))
    if APPLY:
        # 既存codex分をこのcallから削除(冪等)
        c.execute("DELETE FROM eligibility_criteria WHERE call_id=? AND source_dataset='codex_eligibility_2026'", (cid,))
    plan["foundations"] += 1
    # スカラ項目
    for fld, typ in FIELD_TYPE.items():
        v = el.get(fld)
        if v and isinstance(v, str) and v.strip():
            if APPLY:
                c.execute("INSERT INTO eligibility_criteria (id,call_id,criterion_type,description,is_required,source_url,source_dataset,created_at) VALUES (?,?,?,?,?,?,?,?)",
                          (str(uuid.uuid4()), cid, typ, v.strip()[:400], 1, src, "codex_eligibility_2026", NOW))
            plan["criteria_added"] += 1
    # 配列項目(position/affiliation_type)
    for fld, typ in (("position", "position"), ("affiliation_type", "affiliation_type")):
        arr = el.get(fld)
        if arr and isinstance(arr, list) and arr:
            desc = ",".join(str(x) for x in arr if x)[:400]
            if desc and APPLY:
                c.execute("INSERT INTO eligibility_criteria (id,call_id,criterion_type,description,is_required,source_url,source_dataset,created_at) VALUES (?,?,?,?,?,?,?,?)",
                          (str(uuid.uuid4()), cid, typ, desc, 1, src, "codex_eligibility_2026", NOW))
            if desc: plan["criteria_added"] += 1
if APPLY: c.commit()
print(("APPLIED" if APPLY else "DRY-RUN"), json.dumps(plan, ensure_ascii=False))
