#!/usr/bin/env python3
"""Phase 3 — codex-orchestrated financial collection (Claude conducts, codex collects, verify later).
   Runs codex-run per foundation (web-enabled) to gather established_year/total_assets/annual_grant
   with source_url. Writes to STAGING json only (never straight to DB). Resumable. Concurrency-capped.
   Usage: python3 collect_financials.py --limit 40 --concurrency 6
"""
import sqlite3, json, subprocess, argparse, os, sys
from concurrent.futures import ThreadPoolExecutor, as_completed

DB = "corporate_research_grants.sqlite"
HERE = os.path.dirname(os.path.abspath(__file__))
SCHEMA = os.path.join(HERE, "schema_financials.json")
STAGING = "research_results/codex_financials_staging.json"
CODEX = os.path.expanduser("~/.local/bin/codex-run")

def load_staging():
    if os.path.exists(STAGING):
        return json.load(open(STAGING))
    return {}

def select(limit):
    c = sqlite3.connect(DB); c.row_factory = sqlite3.Row
    rows = c.execute("""
      SELECT o.id,o.name,o.url,o.jfc_rank FROM organizations o
      WHERE o.url IS NOT NULL AND o.url LIKE 'http%'
        AND (o.jfc_rank IS NOT NULL OR o.id IN (SELECT organization_id FROM grant_programs))
        AND (o.established_year IS NULL OR o.total_assets IS NULL)
      ORDER BY o.jfc_rank IS NULL, o.jfc_rank
      LIMIT ?""", (limit,)).fetchall()
    return rows

PROMPT_TMPL = """あなたは日本の公益財団の財務データ収集担当です。捏造厳禁・確認できた値のみ・不明はnull。

対象財団: {name}
公式サイト: {url}

web検索とサイト閲覧で収集しschemaのJSONで返す:
- established_year: 設立年(西暦)を公式の沿革/概要で確認
- total_assets_jpy: 総資産または正味財産(円)を最新の事業報告書/決算から
- annual_grant_amount_jpy: 直近年度の年間助成金支出総額(円)を助成実績/事業報告から
各値に実在しアクセス可能なsource_urlを付す。確認不能はnull+notesに理由。数値は円単位の整数。推測禁止。"""

def run_one(row):
    prompt = PROMPT_TMPL.format(name=row["name"], url=row["url"])
    label = "cfgfin-" + row["id"][:8]
    try:
        p = subprocess.run([CODEX, "--label", label, "--profile", "research",
                            "--schema", SCHEMA, prompt],
                           capture_output=True, text=True, timeout=360)
        env = json.loads(p.stdout.strip().splitlines()[-1])
        if env.get("status") == "ok" and env.get("result_file") and os.path.exists(env["result_file"]):
            data = json.load(open(env["result_file"]))
            return row["id"], {"name": row["name"], "url": row["url"], "jfc_rank": row["jfc_rank"],
                               "codex": data, "elapsed_s": env.get("elapsed_s")}
        return row["id"], {"name": row["name"], "error": env.get("status"), "envelope": env}
    except Exception as e:
        return row["id"], {"name": row["name"], "error": str(e)}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=40)
    ap.add_argument("--concurrency", type=int, default=6)
    a = ap.parse_args()
    staging = load_staging()
    rows = [r for r in select(a.limit) if r["id"] not in staging]
    print("to collect: %d (already staged: %d)" % (len(rows), len(staging)), flush=True)
    done = 0
    with ThreadPoolExecutor(max_workers=a.concurrency) as ex:
        futs = {ex.submit(run_one, r): r for r in rows}
        for f in as_completed(futs):
            oid, res = f.result()
            staging[oid] = res
            done += 1
            json.dump(staging, open(STAGING, "w"), ensure_ascii=False, indent=1)  # checkpoint each
            tag = "OK" if "codex" in res else "ERR"
            cx = res.get("codex", {})
            print("[%d/%d] %s %s | est=%s assets=%s grant=%s conf=%s" % (
                done, len(rows), tag, res["name"][:24],
                cx.get("established_year"), cx.get("total_assets_jpy"),
                cx.get("annual_grant_amount_jpy"), cx.get("confidence")), flush=True)
    ok = sum(1 for v in staging.values() if "codex" in v)
    print("staged total: %d (ok=%d)" % (len(staging), ok))

if __name__ == "__main__":
    main()
