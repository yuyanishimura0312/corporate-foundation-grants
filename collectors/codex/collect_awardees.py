#!/usr/bin/env python3
"""Phase 5 — codex採択者収集。財団の公式採択結果一覧から採択者(氏名/所属/職位/テーマ/分野/金額)を
   2026-10-19: the per-foundation loop now runs through tri-batch: canary, ramp,
   breaker read only, machine-wide 48-slot cap, and resume.
   Replaced the thread pool with one batch call; prompts and staging writes stay here.
   直近年度で収集。web有効・出典URL必須・捏造禁止。staging json のみ(直接DB書込しない)。resumable。
   Usage: python3 collect_awardees.py --names "内藤記念科学振興財団,三島海雲記念財団" [--concurrency 4]
          または --auto N (research_relevance=high・URL有・採択者未収集 から N 財団)"""
import sqlite3, json, subprocess, argparse, os, sys, hashlib
from pathlib import Path

sys.path.insert(0, str(Path.home() / "tools" / "tri-run"))
from tri_batch_adapter import run as tri_batch_run

DB = "corporate_research_grants.sqlite"
HERE = os.path.dirname(os.path.abspath(__file__))
SCHEMA = os.path.join(HERE, "schema_awardees.json")
STAGING = "research_results/codex_awardees_staging.json"
CODEX = os.path.expanduser("~/.local/bin/codex-run")

PROMPT = """あなたは日本の研究助成財団の「採択者(助成先)」データ収集担当です。捏造厳禁・公式の採択結果に載っている人だけ・不明はnull。

対象財団: {name}
参考URL(誤っている可能性あり): {url}

手順:
1. まず財団名「{name}」で公式サイトを特定(参考URLがこの財団のものか確認。違えば正しい公式サイトを検索)。
2. 公式の「採択者一覧/助成先/研究助成 採択課題/贈呈式」等のページを探す。
3. 直近で判明する1年度分の採択者を awardees 配列に収集:
   - awardee_name(氏名)/affiliation(所属)/position(職位)/project_title(研究課題名)/field_hint(分野)/amount_jpy(個別金額があれば)
4. source_url に採択者一覧の実在URL、fiscal_year にその年度(西暦)。
確認できた採択者のみ。人数が多ければ最新年度の全員(上限なし・全員)。推測・水増し禁止。見つからなければ awardees は空・notesに理由。"""

def run_one(name, url):
    label = "cfgaw-" + "".join(ch for ch in name if ch.isalnum())[:10]
    p = subprocess.run([CODEX, "--label", label, "--profile", "research", "--schema", SCHEMA,
                        PROMPT.format(name=name, url=url or "(なし)")],
                       capture_output=True, text=True, timeout=420)
    try:
        env = json.loads(p.stdout.strip().splitlines()[-1])
    except Exception:
        return name, {"error": "envelope parse"}
    if env.get("status") == "ok" and env.get("result_file") and os.path.exists(env["result_file"]):
        return name, {"codex": json.load(open(env["result_file"])), "url_hint": url, "elapsed_s": env.get("elapsed_s")}
    return name, {"error": env.get("status"), "envelope": env}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--names", default="")
    ap.add_argument("--auto", type=int, default=0)
    ap.add_argument("--concurrency", type=int, default=4)
    a = ap.parse_args()
    c = sqlite3.connect(DB); c.row_factory = sqlite3.Row
    targets = []
    if a.names:
        for nm in a.names.split(","):
            nm = nm.strip()
            r = c.execute("SELECT name,url FROM organizations WHERE name LIKE ? LIMIT 1", ("%" + nm + "%",)).fetchone()
            if r: targets.append((r["name"], r["url"]))
    if a.auto:
        for r in c.execute("""SELECT name,url FROM organizations WHERE url LIKE 'http%' AND research_relevance IN ('high','medium')
            AND id NOT IN (SELECT p.organization_id FROM grant_programs p JOIN grant_calls gc ON gc.program_id=p.id JOIN grant_results r ON r.call_id=gc.id)
            ORDER BY total_assets DESC NULLS LAST LIMIT ?""", (a.auto,)).fetchall():
            targets.append((r["name"], r["url"]))
    staging = json.load(open(STAGING)) if os.path.exists(STAGING) else {}
    targets = [t for t in targets if t[0] not in staging]
    print("to collect:", len(targets), flush=True)
    rows, target_by_id, occurrences = [], {}, {}
    for nm, url in targets:
        key = hashlib.sha256(json.dumps([nm, url], ensure_ascii=False).encode()).hexdigest()
        occurrences[key] = occurrences.get(key, 0) + 1
        iid = key + "-" + str(occurrences[key])
        rows.append({"id": iid, "prompt": PROMPT.format(name=nm, url=url or "(なし)")})
        target_by_id[iid] = (nm, url)
    batch_out = Path.home() / ".tri-run" / "mig-corporate-foundation-grants-collect-awardees" / "batch"
    results = tri_batch_run(
        "mig-corporate-foundation-grants-collect-awardees", rows, schema=SCHEMA,
        out=batch_out, profile="collect", model="gpt-6-astra", effort="low",
        concurrency=min(a.concurrency, 48), timeout=420, source_scope="public_web",
    ) if rows else []
    done = 0
    for result in results:
        nm, url = target_by_id[result["id"]]
        state_file = batch_out / "items" / (result["id"] + ".json")
        env = json.loads(state_file.read_text()) if state_file.exists() else {
            "status": result["status"], "run_dir": result["run_dir"], "note": result["note"]}
        if result["status"] == "ok":
            res = {"codex": result["value"], "url_hint": url, "elapsed_s": env.get("elapsed_s")}
        else:
            res = {"error": result["status"], "envelope": env}
        staging[nm] = res; done += 1
        json.dump(staging, open(STAGING, "w"), ensure_ascii=False, indent=1)
        naw = len(res.get("codex", {}).get("awardees", [])) if "codex" in res else 0
        print("[%d/%d] %s %s | awardees=%d fy=%s" % (done, len(targets), "OK" if "codex" in res else "ERR",
              nm[:22], naw, res.get("codex", {}).get("fiscal_year")), flush=True)

if __name__ == "__main__":
    main()
