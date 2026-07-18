#!/usr/bin/env python3
"""fable監査③: total_assets を持つが source_url 欠落の13著名財団に、出典URLを targeted codex で補填。
   codex収集値が既存DB値と±5%一致 → total_assets_source_url を metadata に付与(値は不変)。
   乖離 → 値は変えず discrepancy を flag(生成と検証の分離)。additive・捏造しない。
   実行: python3 scripts/backfill_13_asset_sources.py [--apply]"""
import sqlite3, json, subprocess, os, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
DB="corporate_research_grants.sqlite"
SCHEMA=os.path.expanduser("~/projects/apps/corporate-foundation-grants/collectors/codex/schema_financials.json")
CODEX=os.path.expanduser("~/.local/bin/codex-run")
APPLY="--apply" in sys.argv
c=sqlite3.connect(DB); c.row_factory=sqlite3.Row
targets=[dict(r) for r in c.execute(
  "SELECT id,name,total_assets FROM organizations WHERE total_assets IS NOT NULL AND metadata NOT LIKE '%total_assets_source_url%' AND duplicate_of IS NULL")]
print("対象:",len(targets),"件")

PROMPT="""日本の公益財団の総資産(貸借対照表の資産合計。正味財産ではない)を最新の事業報告書/決算から確認。捏造厳禁・不明はnull。
対象財団: {name}
web検索とサイト閲覧で total_assets_jpy(円・整数) と実在アクセス可能な financial_source_url を schemaのJSONで返す。established_year等の他項目もわかれば。確認不能はnull。"""

def run_one(t):
    try:
        p=subprocess.run([CODEX,"--label","cfg13-"+t["id"][:8],"--profile","research","--schema",SCHEMA,
                          PROMPT.format(name=t["name"])],capture_output=True,text=True,timeout=360)
        env=json.loads(p.stdout.strip().splitlines()[-1])
        if env.get("status")=="ok" and env.get("result_file") and os.path.exists(env["result_file"]):
            return t, json.load(open(env["result_file"]))
    except Exception as e:
        return t, {"error":str(e)}
    return t, {}

results=[]
with ThreadPoolExecutor(max_workers=6) as ex:
    for f in as_completed([ex.submit(run_one,t) for t in targets]):
        results.append(f.result())

attached=flagged=0
for t, cx in results:
    ta=cx.get("total_assets_jpy"); src=cx.get("financial_source_url")
    db=t["total_assets"]
    if ta and src and abs(ta-db)/db<=0.05:
        row=c.execute("SELECT metadata FROM organizations WHERE id=?",(t["id"],)).fetchone()
        try: m=json.loads(row[0] or "{}")
        except: m={}
        m.setdefault("financials_provenance",{})
        m["financials_provenance"]["total_assets_source_url"]=src
        m["financials_provenance"]["backfilled_2026-07-18"]=True
        print(f"  ✓ 出典付与: {t['name'][:24]} DB={db:,} codex={ta:,} src={src[:50]}")
        if APPLY: c.execute("UPDATE organizations SET metadata=? WHERE id=?",(json.dumps(m,ensure_ascii=False),t["id"]))
        attached+=1
    else:
        row=c.execute("SELECT metadata FROM organizations WHERE id=?",(t["id"],)).fetchone()
        try: m=json.loads(row[0] or "{}")
        except: m={}
        m.setdefault("financials_provenance",{})
        m["financials_provenance"]["source_unverified"]=True
        m["financials_provenance"]["codex_check"]={"total_assets":ta,"src":src,"db_value":db}
        print(f"  ⚠ 未確認/乖離 flag: {t['name'][:24]} DB={db:,} codex={ta}")
        if APPLY: c.execute("UPDATE organizations SET metadata=? WHERE id=?",(json.dumps(m,ensure_ascii=False),t["id"]))
        flagged+=1
if APPLY: c.commit(); print(f"\n[applied] 出典付与 {attached} / 未確認flag {flagged}")
else: print(f"\n[dry-run] 出典付与予定 {attached} / flag予定 {flagged}")
c.close()
