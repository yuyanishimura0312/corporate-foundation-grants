#!/usr/bin/env python3
"""Tier B 財務収集 — url を持たない財団(koeki研究財団high 等)を名称+所在地でweb検索。
   既存 collect_financials.py の url依存 select を「名称検索」に置換した姉妹版。
   staging / schema / ingest は共有(冪等)。捏造厳禁・source_url必須・確認不能はnull。
   Usage: python3 collectors/codex/collect_financials_byname.py --limit 200 --concurrency 8
          [--relevance high|all] [--min-assets-null]  (既定=koeki_research high・未取得のみ)"""
import sqlite3, json, subprocess, argparse, os, sys
from concurrent.futures import ThreadPoolExecutor, as_completed

HERE = os.path.dirname(os.path.abspath(__file__))
DB = "corporate_research_grants.sqlite"
SCHEMA = os.path.join(HERE, "schema_financials.json")
STAGING = "research_results/codex_financials_staging.json"
CODEX = os.path.expanduser("~/.local/bin/codex-run")

def load_staging():
    try: return json.load(open(STAGING))
    except Exception: return {}

def select(limit, relevance):
    c = sqlite3.connect(DB); c.row_factory = sqlite3.Row
    rel = "" if relevance == "all" else " AND o.research_relevance='high'"
    rows = c.execute(f"""
      SELECT o.id, o.name, o.prefecture, o.municipality, o.admin_agency
      FROM organizations o
      WHERE (o.url IS NULL OR o.url NOT LIKE 'http%')
        AND o.source_dataset='koeki_research_2026'{rel}
        AND (o.established_year IS NULL OR o.total_assets IS NULL)
      ORDER BY o.name
      LIMIT ?""", (limit,)).fetchall()
    return rows

PROMPT_TMPL = """あなたは日本の公益財団の財務データ収集担当です。捏造厳禁・確認できた値のみ・不明はnull。

対象財団: {name}
所在地: {pref}{muni}（所管: {agency}）
※公式サイトURLは未登録。まず正式名称+所在地でweb検索し公式サイト/公益法人informationを特定してから収集する。同名の別財団に注意(所在地・所管で照合)。

web検索とサイト閲覧で収集しschemaのJSONで返す:
- established_year: 設立年(西暦)を公式の沿革/概要で確認
- total_assets_jpy: 総資産(貸借対照表の資産合計。正味財産ではない)を最新の事業報告書/決算から。正味財産しか無ければnull+notesに明記
- annual_grant_amount_jpy: 直近年度の年間助成金支出総額(円)を助成実績/事業報告から
- grant_fields: この財団が助成する領域を11区分から列挙(公式の助成対象・目的から。複数可・優先順)
- grant_scope_text: 公式の助成対象分野の記述(要約)
各値に実在しアクセス可能なsource_urlを付す。確認不能はnull(配列は空)+notesに理由。数値は円単位の整数。推測禁止。
公式サイトが見つからない/非公開なら全項目null+notesに"no public source"。別財団と誤認するくらいならnull。"""

def run_one(row):
    prompt = PROMPT_TMPL.format(name=row["name"], pref=row["prefecture"] or "",
                                muni=row["municipality"] or "", agency=row["admin_agency"] or "不明")
    label = "cfgfinB-" + row["id"][:8]
    try:
        p = subprocess.run([CODEX, "--label", label, "--profile", "research",
                            "--schema", SCHEMA, prompt],
                           capture_output=True, text=True, timeout=360)
        env = json.loads(p.stdout.strip().splitlines()[-1])
        if env.get("status") == "ok" and env.get("result_file") and os.path.exists(env["result_file"]):
            data = json.load(open(env["result_file"]))
            return row["id"], {"name": row["name"], "url": None, "jfc_rank": None,
                               "codex": data, "elapsed_s": env.get("elapsed_s"), "tier": "B"}
        return row["id"], {"name": row["name"], "error": env.get("status"), "envelope": env}
    except Exception as e:
        return row["id"], {"name": row["name"], "error": str(e)}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--relevance", default="high", choices=["high", "all"])
    a = ap.parse_args()
    staging = load_staging()
    rows = [r for r in select(a.limit, a.relevance) if r["id"] not in staging]
    print("to collect(TierB): %d (already staged: %d)" % (len(rows), len(staging)), flush=True)
    done = 0
    with ThreadPoolExecutor(max_workers=a.concurrency) as ex:
        futs = {ex.submit(run_one, r): r for r in rows}
        for f in as_completed(futs):
            oid, res = f.result()
            staging[oid] = res
            done += 1
            json.dump(staging, open(STAGING, "w"), ensure_ascii=False, indent=1)
            cx = res.get("codex", {})
            print("[%d/%d] %s %s | est=%s assets=%s grant=%s conf=%s" % (
                done, len(rows), "OK" if "codex" in res else "ERR", res["name"][:22],
                cx.get("established_year"), cx.get("total_assets_jpy"),
                cx.get("annual_grant_amount_jpy"), cx.get("confidence")), flush=True)
    ok = sum(1 for v in staging.values() if "codex" in v)
    print("staged total: %d (ok=%d)" % (len(staging), ok))

if __name__ == "__main__":
    main()
