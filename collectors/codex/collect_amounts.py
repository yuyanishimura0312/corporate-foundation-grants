#!/usr/bin/env python3
"""金額DB拡張 — codexで研究助成プログラムの金額情報(1件あたり額・件数・総額)を募集要項から収集。
   個別採択額は非公開が多いため program 単位の確実な金額を取る。出典必須・捏造禁止。staging json。resumable。
   Usage: python3 collect_amounts.py --auto N [--concurrency 40]"""
import sqlite3, json, subprocess, argparse, os
from concurrent.futures import ThreadPoolExecutor, as_completed
DB = "corporate_research_grants.sqlite"
HERE = os.path.dirname(os.path.abspath(__file__))
SCHEMA = os.path.join(HERE, "schema_amounts.json")
STAGING = "research_results/codex_amounts_staging.json"
CODEX = os.path.expanduser("~/.local/bin/codex-run")
PROMPT = """あなたは日本の研究助成財団の「助成金額」データ収集担当です。捏造厳禁・公式の募集要項/助成概要のみ・不明はnull。

対象財団: {name}
参考URL(誤りの可能性あり): {url}

手順:
1. 財団名「{name}」で公式サイトを特定(参考URLがこの財団か確認・違えば正しい公式を検索)。
2. 公式の「募集要項/研究助成概要/助成金額」ページを探す。
3. programs 配列に研究助成プログラムごとの金額情報を収集:
   - program_name / amount_per_award_jpy(1件あたり額・円) / amount_min_jpy・amount_max_jpy(幅がある場合) / num_awards_per_year(採択件数) / total_budget_jpy(総額) / fiscal_year / target_field
公式に明記された金額のみ。source_url に金額の出典URL。数値は円単位の整数。推測禁止。見つからなければ programs 空・notesに理由。"""

def run_one(name, url):
    label = "cfgamt-" + "".join(ch for ch in name if ch.isalnum())[:10]
    try:
        p = subprocess.run([CODEX, "--label", label, "--profile", "research", "--schema", SCHEMA,
                            PROMPT.format(name=name, url=url or "(なし)")], capture_output=True, text=True, timeout=420)
        env = json.loads(p.stdout.strip().splitlines()[-1])
    except Exception as e:
        return name, {"error": str(e)}
    if env.get("status") == "ok" and env.get("result_file") and os.path.exists(env["result_file"]):
        return name, {"codex": json.load(open(env["result_file"])), "url_hint": url}
    return name, {"error": env.get("status")}

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--auto", type=int, default=100); ap.add_argument("--concurrency", type=int, default=40)
    a = ap.parse_args()
    c = sqlite3.connect(DB); c.row_factory = sqlite3.Row
    # 採択者(研究助成)を持つが年間助成額/個別額が薄い財団を優先
    rows = c.execute("""SELECT DISTINCT o.name,o.url FROM organizations o
        JOIN grant_programs p ON p.organization_id=o.id JOIN grant_calls gc ON gc.program_id=p.id JOIN grant_results r ON r.call_id=gc.id
        WHERE o.url LIKE 'http%' AND r.grant_type='research_individual'
          AND o.id NOT IN (SELECT organization_id FROM grant_programs WHERE amount_per_award>0)
        ORDER BY o.total_assets DESC NULLS LAST LIMIT ?""", (a.auto,)).fetchall()
    staging = json.load(open(STAGING)) if os.path.exists(STAGING) else {}
    targets = [(r["name"], r["url"]) for r in rows if r["name"] not in staging]
    print("to collect:", len(targets), flush=True)
    with ThreadPoolExecutor(max_workers=a.concurrency) as ex:
        futs = {ex.submit(run_one, n, u): n for n, u in targets}; done = 0
        for f in as_completed(futs):
            nm, res = f.result(); staging[nm] = res; done += 1
            json.dump(staging, open(STAGING, "w"), ensure_ascii=False, indent=1)
            pg = res.get("codex", {}).get("programs", [])
            print("[%d/%d] %s %s | programs=%d" % (done, len(targets), "OK" if "codex" in res else "ERR", nm[:24], len(pg)), flush=True)

if __name__ == "__main__":
    main()
