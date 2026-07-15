#!/usr/bin/env python3
"""金額DB拡張投入 — fable検証後の codex program金額を grant_programs/grant_calls へ。
   program単位の amount_per_award / num_awards / total_budget を格納(source_url必須・provenance)。Usage: [--apply]"""
import sqlite3, json, sys, os, re
CFG = "corporate_research_grants.sqlite"
STAGING = "research_results/codex_amounts_staging.json"
APPLY = "--apply" in sys.argv
NOW = "2026-07-16"
def sane(v): return isinstance(v, int) and 0 < v < 100_000_000_000
c = sqlite3.connect(CFG); c.row_factory = sqlite3.Row
# program金額用の列(冪等)
for col, typ in [("amount_per_award", "INTEGER"), ("num_awards_per_year", "INTEGER"), ("program_total_budget", "INTEGER"), ("amount_source_url", "TEXT")]:
    if col not in [r[1] for r in c.execute("PRAGMA table_info(grant_programs)")]:
        if APPLY: c.execute("ALTER TABLE grant_programs ADD COLUMN %s %s" % (col, typ))
staging = json.load(open(STAGING)) if os.path.exists(STAGING) else {}
plan = {"foundations": 0, "programs_updated": 0, "amt_set": 0, "skipped_no_source": 0, "skipped_sane": 0}
amt_records = []  # for distribution
for fname, rec in staging.items():
    cx = rec.get("codex")
    if not cx or not cx.get("programs"): continue
    src = cx.get("source_url")
    org = c.execute("SELECT id FROM organizations WHERE name=?", (fname,)).fetchone()
    if not org: continue
    oid = org["id"]; plan["foundations"] += 1
    for pg in cx["programs"]:
        amt = pg.get("amount_per_award_jpy") or pg.get("amount_max_jpy") or pg.get("amount_min_jpy")
        if amt is not None and not sane(amt): plan["skipped_sane"] += 1; amt = None
        if not src and amt: plan["skipped_no_source"] += 1; continue
        if amt: amt_records.append({"foundation": fname, "program": pg.get("program_name"), "amount": amt,
                                    "num": pg.get("num_awards_per_year"), "total": pg.get("total_budget_jpy"),
                                    "field": pg.get("target_field"), "source": src})
        if APPLY and amt:
            # 該当財団のcodex program(研究助成)にamountを付与(なければ新規program)
            pr = c.execute("SELECT id FROM grant_programs WHERE organization_id=? AND (name LIKE '%研究助成%' OR name LIKE '%助成%') AND amount_per_award IS NULL LIMIT 1", (oid,)).fetchone()
            pid = pr["id"] if pr else None
            if not pid:
                import uuid; pid = str(uuid.uuid4())
                c.execute("INSERT INTO grant_programs (id,organization_id,name,source_url,created_at,updated_at) VALUES (?,?,?,?,?,?)",
                          (pid, oid, pg.get("program_name") or "研究助成", src, NOW, NOW))
            c.execute("UPDATE grant_programs SET amount_per_award=?, num_awards_per_year=?, program_total_budget=?, amount_source_url=?, updated_at=? WHERE id=?",
                      (amt, pg.get("num_awards_per_year"), pg.get("total_budget_jpy"), src, NOW, pid))
            plan["programs_updated"] += 1; plan["amt_set"] += 1
if APPLY: c.commit()
json.dump(amt_records, open("research_results/program_amounts.json", "w"), ensure_ascii=False, indent=1)
print(("APPLIED" if APPLY else "DRY-RUN"), json.dumps(plan, ensure_ascii=False))
print("program金額レコード:", len(amt_records))
