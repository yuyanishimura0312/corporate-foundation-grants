#!/usr/bin/env python3
"""Phase 2 ingest — fable検証後の codex役員を foundation_officers へ投入。
   企業役員兼任(is_corporate_exec/corporate_name)が④企業関係の素になる。source_url必須。Usage: [--apply]"""
import sqlite3, json, sys, uuid, os
CFG = "corporate_research_grants.sqlite"
STAGING = "research_results/codex_officers_staging.json"
APPLY = "--apply" in sys.argv
NOW = "2026-07-15"
VALID_ROLE = {"reviewer", "director", "councilor", "chair", "auditor", "other"}
SKIP_FOUNDATIONS = {"トヨタ財団", "公益財団法人トヨタ財団"}  # fable: 改選で出典stale→再収集後に投入
# fable: 顧問/相談役/特別顧問/元/前は「企業役員兼任」でない→exec_flagから除外
NON_EXEC_TITLE = ("顧問", "相談役", "特別顧問", "元", "前")
c = sqlite3.connect(CFG); c.row_factory = sqlite3.Row
staging = json.load(open(STAGING)) if os.path.exists(STAGING) else {}
plan = {"foundations": 0, "officers_inserted": 0, "corporate_execs": 0, "skipped_no_source": 0}
for fname, rec in staging.items():
    if fname in SKIP_FOUNDATIONS: continue
    cx = rec.get("codex")
    if not cx or not cx.get("officers"): continue
    src = cx.get("source_url")
    if not src: plan["skipped_no_source"] += len(cx.get("officers", [])); continue
    org = c.execute("SELECT id FROM organizations WHERE name=?", (fname,)).fetchone()
    if not org: continue
    oid = org["id"]
    if not APPLY:
        plan["foundations"] += 1; plan["officers_inserted"] += len([o for o in cx["officers"] if o.get("person_name")])
        plan["corporate_execs"] += sum(1 for o in cx["officers"] if o.get("is_corporate_exec")); continue
    # replace prior codex officers for this org+source (idempotent)
    c.execute("DELETE FROM foundation_officers WHERE organization_id=? AND source_dataset='codex_officers_2026'", (oid,))
    for o in cx["officers"]:
        nm = (o.get("person_name") or "").strip()
        if not nm: continue
        role = o.get("role") if o.get("role") in VALID_ROLE else "other"
        tt = (o.get("title") or "") + (o.get("role_ja") or "")
        ce = 1 if (o.get("is_corporate_exec") and o.get("corporate_name") and not any(k in tt for k in NON_EXEC_TITLE)) else 0
        c.execute("""INSERT INTO foundation_officers
            (id,organization_id,role,role_ja,person_name,affiliation,title,is_corporate_exec,corporate_name,
             fiscal_year,source_url,source_dataset,metadata,created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (str(uuid.uuid4()), oid, role, o.get("role_ja"), nm, o.get("affiliation"), o.get("title"),
             ce, o.get("corporate_name"), cx.get("fiscal_year"), src, "codex_officers_2026",
             json.dumps({"confidence": cx.get("confidence")}, ensure_ascii=False), NOW))
        plan["officers_inserted"] += 1
        if ce: plan["corporate_execs"] += 1
    plan["foundations"] += 1
if APPLY: c.commit()
print(("APPLIED" if APPLY else "DRY-RUN"), json.dumps(plan, ensure_ascii=False, indent=1))
