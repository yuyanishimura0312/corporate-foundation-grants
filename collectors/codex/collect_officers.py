#!/usr/bin/env python3
"""Phase 2 — codex役員(理事/評議員/監事/審査員)収集。公式の役員名簿・選考委員から。
   企業役員兼任(is_corporate_exec)も判定=④企業関係の素。出典必須・捏造禁止。staging json。resumable。
   Usage: python3 collect_officers.py --names "内藤記念科学振興財団,..." [--concurrency 4]"""
import sqlite3, json, subprocess, argparse, os
from concurrent.futures import ThreadPoolExecutor, as_completed
DB = "corporate_research_grants.sqlite"
HERE = os.path.dirname(os.path.abspath(__file__))
SCHEMA = os.path.join(HERE, "schema_officers.json")
STAGING = "research_results/codex_officers_staging.json"
CODEX = os.path.expanduser("~/.local/bin/codex-run")
PROMPT = """あなたは日本の研究助成財団の「役員(理事・評議員・監事)および審査員(選考委員)」データ収集担当です。捏造厳禁・公式情報のみ・不明はnull。

対象財団: {name}
参考URL(誤りの可能性あり): {url}

手順:
1. 財団名「{name}」で公式サイトを特定(参考URLがこの財団か確認・違えば正しい公式を検索)。
2. 公式の「役員一覧/組織/役員名簿」ページ、および助成の「選考委員/審査委員」ページを探す。
3. officers 配列に収集: person_name(氏名)/role(理事director・評議員councilor・監事auditor・理事長chair・審査員reviewer)/role_ja(原文役職)/affiliation(所属)/title(肩書)。
4. 各人が企業の役員(社長/会長/取締役/顧問等)を兼任していれば is_corporate_exec=true・corporate_name に企業名。
確認できた人のみ。source_url に役員一覧の実在URL。見つからなければ officers 空・notesに理由。推測禁止。"""

def run_one(name, url):
    label = "cfgof-" + "".join(ch for ch in name if ch.isalnum())[:10]
    p = subprocess.run([CODEX, "--label", label, "--profile", "research", "--schema", SCHEMA,
                        PROMPT.format(name=name, url=url or "(なし)")], capture_output=True, text=True, timeout=420)
    try: env = json.loads(p.stdout.strip().splitlines()[-1])
    except Exception: return name, {"error": "parse"}
    if env.get("status") == "ok" and env.get("result_file") and os.path.exists(env["result_file"]):
        return name, {"codex": json.load(open(env["result_file"])), "url_hint": url}
    return name, {"error": env.get("status")}

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--names", default=""); ap.add_argument("--auto", type=int, default=0); ap.add_argument("--concurrency", type=int, default=4)
    a = ap.parse_args()
    c = sqlite3.connect(DB); c.row_factory = sqlite3.Row
    targets = []
    if a.names:
        for nm in a.names.split(","):
            r = c.execute("SELECT name,url FROM organizations WHERE name LIKE ? LIMIT 1", ("%" + nm.strip() + "%",)).fetchone()
            if r: targets.append((r["name"], r["url"]))
    if a.auto:
        for r in c.execute("SELECT name,url FROM organizations WHERE url LIKE 'http%' AND foundation_subtype='corporate' AND id NOT IN (SELECT organization_id FROM foundation_officers) ORDER BY total_assets DESC NULLS LAST LIMIT ?", (a.auto,)).fetchall():
            targets.append((r["name"], r["url"]))
    staging = json.load(open(STAGING)) if os.path.exists(STAGING) else {}
    targets = [t for t in targets if t[0] not in staging]
    print("to collect:", len(targets), flush=True)
    with ThreadPoolExecutor(max_workers=a.concurrency) as ex:
        futs = {ex.submit(run_one, n, u): n for n, u in targets}; done = 0
        for f in as_completed(futs):
            nm, res = f.result(); staging[nm] = res; done += 1
            json.dump(staging, open(STAGING, "w"), ensure_ascii=False, indent=1)
            of = res.get("codex", {}).get("officers", []); ce = sum(1 for o in of if o.get("is_corporate_exec"))
            print("[%d/%d] %s %s | officers=%d 企業役員=%d" % (done, len(targets), "OK" if "codex" in res else "ERR", nm[:22], len(of), ce), flush=True)

if __name__ == "__main__":
    main()
