#!/usr/bin/env python3
"""応募要項拡張 — codexで研究助成の応募資格(対象者像)を募集要項から収集。
   年齢/キャリア/職位/所属種別/国籍/分野/性別/その他。出典必須・捏造禁止。staging json。resumable。
   Usage: python3 collect_eligibility.py --auto N [--concurrency 40]"""
import sqlite3, json, subprocess, argparse, os
from concurrent.futures import ThreadPoolExecutor, as_completed
DB = "corporate_research_grants.sqlite"
HERE = os.path.dirname(os.path.abspath(__file__))
SCHEMA = os.path.join(HERE, "schema_eligibility.json")
STAGING = "research_results/codex_eligibility_staging.json"
CODEX = os.path.expanduser("~/.local/bin/codex-run")
PROMPT = """あなたは日本の研究助成財団の「応募資格(対象者像)」データ収集担当です。捏造厳禁・公式の募集要項のみ・不明はnull。

対象財団: {name}
参考URL(誤りの可能性あり): {url}

手順:
1. 財団名「{name}」で公式サイトを特定(参考URLがこの財団か確認・違えば正しい公式を検索)。
2. 公式の「募集要項/応募資格/応募要領」ページを探す。
3. eligibility に研究助成の応募資格を収集:
   - age_limit(年齢制限) / career_stage(キャリア段階) / position(対象職位・配列) / affiliation_type(対象所属種別・配列) / nationality(国籍要件) / field_scope(対象分野) / gender_scope(性別要件) / other_requirements(その他主要要件)
公式に明記された要件のみ。source_url に応募資格の出典URL。推測禁止。見つからなければ各項目null/空・notesに理由。"""

def run_one(name, url):
    label = "cfgelig-" + "".join(ch for ch in name if ch.isalnum())[:9]
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
    rows = c.execute("""SELECT DISTINCT o.name,o.url FROM organizations o
        JOIN grant_programs p ON p.organization_id=o.id JOIN grant_calls gc ON gc.program_id=p.id JOIN grant_results r ON r.call_id=gc.id
        WHERE o.url LIKE 'http%' AND r.grant_type='research_individual'
        ORDER BY o.total_assets DESC NULLS LAST LIMIT ?""", (a.auto,)).fetchall()
    staging = json.load(open(STAGING)) if os.path.exists(STAGING) else {}
    targets = [(r["name"], r["url"]) for r in rows if r["name"] not in staging]
    print("to collect:", len(targets), flush=True)
    with ThreadPoolExecutor(max_workers=a.concurrency) as ex:
        futs = {ex.submit(run_one, n, u): n for n, u in targets}; done = 0
        for f in as_completed(futs):
            nm, res = f.result(); staging[nm] = res; done += 1
            json.dump(staging, open(STAGING, "w"), ensure_ascii=False, indent=1)
            el = res.get("codex", {}).get("eligibility", {})
            has = sum(1 for v in el.values() if v) if el else 0
            print("[%d/%d] %s %s | fields=%d" % (done, len(targets), "OK" if "codex" in res else "ERR", nm[:22], has), flush=True)

if __name__ == "__main__":
    main()
