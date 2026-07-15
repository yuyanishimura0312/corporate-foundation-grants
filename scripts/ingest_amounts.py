#!/usr/bin/env python3
"""金額DB拡張投入 — fable検証後の codex program金額を grant_programs/grant_calls へ。
   program単位の amount_per_award / num_awards / total_budget を格納(source_url必須・provenance)。Usage: [--apply]"""
import sqlite3, json, sys, os, re, unicodedata
CFG = "corporate_research_grants.sqlite"
STAGING = "research_results/codex_amounts_staging.json"
APPLY = "--apply" in sys.argv
NOW = "2026-07-16"
def sane(v): return isinstance(v, int) and 0 < v < 100_000_000_000
def nm(s):  # 名前照合(誤帰属ゲート・fable)
    s = re.sub(r'(公益|一般|認定)?(財団|社団|特定非営利活動)?法人|株式会社|旧[：:].*|（.*?）|\(.*?\)', '', unicodedata.normalize("NFKC", s or ""))
    return re.sub(r'[\s　・,，.。「」]', '', s).strip().lower()
UNIT_FLAG = ("日額", "月額", "貸与", "／月", "/月", "／日", "/日", "月あたり", "日あたり")  # 1件あたりでない
c = sqlite3.connect(CFG); c.row_factory = sqlite3.Row
# program金額用の列(冪等)
for col, typ in [("amount_per_award", "INTEGER"), ("num_awards_per_year", "INTEGER"), ("program_total_budget", "INTEGER"), ("amount_source_url", "TEXT")]:
    if col not in [r[1] for r in c.execute("PRAGMA table_info(grant_programs)")]:
        if APPLY: c.execute("ALTER TABLE grant_programs ADD COLUMN %s %s" % (col, typ))
staging = json.load(open(STAGING)) if os.path.exists(STAGING) else {}
plan = {"foundations": 0, "programs_updated": 0, "amt_set": 0, "skipped_no_source": 0, "skipped_sane": 0,
        "skipped_misattribution": 0, "skipped_stale": 0, "unit_flagged": 0}
amt_records = []  # for distribution
for fname, rec in staging.items():
    cx = rec.get("codex")
    if not cx or not cx.get("programs"): continue
    # 誤帰属ゲート(fable): codex.foundation_name がキー財団と一致しなければ全スキップ
    if nm(cx.get("foundation_name")) and nm(fname) and nm(cx["foundation_name"]) != nm(fname) \
       and nm(cx["foundation_name"]) not in nm(fname) and nm(fname) not in nm(cx["foundation_name"]):
        plan["skipped_misattribution"] += len(cx["programs"]); continue
    src = cx.get("source_url")
    org = c.execute("SELECT id FROM organizations WHERE name=?", (fname,)).fetchone()
    if not org: continue
    oid = org["id"]; plan["foundations"] += 1
    for pg in cx["programs"]:
        # 鮮度ゲート(fable): 2023年より前は隔離
        fy = pg.get("fiscal_year")
        if fy and fy < 2023: plan["skipped_stale"] += 1; continue
        # 単位ゲート(fable): 日額/月額/貸与 は per_award でない
        pt = (pg.get("program_name") or "") + (cx.get("notes") or "")
        is_unit = any(u in pt for u in UNIT_FLAG)
        amt = pg.get("amount_per_award_jpy") or pg.get("amount_max_jpy") or pg.get("amount_min_jpy")
        if amt is not None and not sane(amt): plan["skipped_sane"] += 1; amt = None
        if is_unit and amt: plan["unit_flagged"] += 1; continue  # 日額/月額は分布から除外
        if not src and amt: plan["skipped_no_source"] += 1; continue
        if amt: amt_records.append({"foundation": fname, "program": pg.get("program_name"), "amount": amt,
                                    "num": pg.get("num_awards_per_year"), "total": pg.get("total_budget_jpy"),
                                    "field": pg.get("target_field"), "source": src,
                                    "amount_type": "max" if not pg.get("amount_per_award_jpy") else "per_award"})
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
