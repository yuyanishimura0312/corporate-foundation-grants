#!/usr/bin/env python3
"""採択者 詳細補填 収集(Phase5)。既存 grant_results の空欄(金額/職位/研究概要/期間)を、
   出典URL(採択結果ページ)単位で codex に全採択者を再抽出させて確保する。捏造禁止・確認できた値のみ。
   既存レコードは enrich_awardee_details.py が source_url+氏名で照合し additive 充填(新規作成しない)。
   Usage: python3 collectors/codex/collect_awardee_details.py --limit 120 --concurrency 8"""
import sqlite3, json, subprocess, argparse, os
from concurrent.futures import ThreadPoolExecutor, as_completed
HERE=os.path.dirname(os.path.abspath(__file__))
DB="corporate_research_grants.sqlite"
SCHEMA=os.path.join(HERE,"schema_awardee_details.json")
STAGING="research_results/codex_awardee_details_staging.json"
CODEX=os.path.expanduser("~/.local/bin/codex-run")

def load_staging():
    try: return json.load(open(STAGING))
    except Exception: return {}

def select(limit):
    c=sqlite3.connect(DB); c.row_factory=sqlite3.Row
    # 詳細に空欄がある research_individual 採択者を含む出典URLを、採択者数の多い順に
    rows=c.execute("""
      SELECT gr.source_url, COUNT(*) n,
             MAX(o.name) fdn, MAX(gr.fiscal_year) fy
      FROM grant_results gr
      JOIN grant_calls gc ON gc.id=gr.call_id
      JOIN grant_programs gp ON gp.id=gc.program_id
      JOIN organizations o ON o.id=gp.organization_id
      WHERE gr.grant_type='research_individual' AND gr.source_url LIKE 'http%'
        AND (gr.award_amount IS NULL OR gr.awardee_position IS NULL OR gr.awardee_position=''
             OR gr.project_abstract IS NULL OR gr.project_abstract='')
      GROUP BY gr.source_url
      HAVING SUM(gr.award_amount IS NULL) >= 1
      ORDER BY SUM(gr.award_amount IS NULL) DESC, n DESC
      LIMIT ?""",(limit,)).fetchall()
    return rows

PROMPT="""日本の公益財団の採択結果ページから、掲載された採択者(研究者)の詳細を全員分そのまま抽出する。捏造厳禁・ページに無い値はnull・推測禁止。

財団: {fdn}
採択結果ページ(このURLを閲覧): {url}
年度の目安: {fy}

このページに載っている採択者を全員、schemaのJSON配列で返す。各採択者について:
- awardee_name(氏名)/affiliation(所属)/position(職位)/project_title(研究課題名)
- project_abstract(研究概要。ページに要旨があれば・無ければnull)
- amount_jpy(その人個人への助成額・円。ページに個別金額の記載があれば・無ければnull。全員一律額なら各人にその額)
- period_start/period_end(助成期間。記載あれば)
URLが開けない/一覧が無い場合は awardees を空配列にし source_url にそのURLを入れる。氏名が読めない行は含めない。"""

def run_one(row):
    prompt=PROMPT.format(fdn=row["fdn"], url=row["source_url"], fy=row["fy"] or "")
    label="cfgdet-"+str(abs(hash(row["source_url"])))[:8]
    try:
        p=subprocess.run([CODEX,"--label",label,"--profile","research","--schema",SCHEMA,prompt],
                         capture_output=True,text=True,timeout=420)
        env=json.loads(p.stdout.strip().splitlines()[-1])
        if env.get("status")=="ok" and env.get("result_file") and os.path.exists(env["result_file"]):
            data=json.load(open(env["result_file"]))
            return row["source_url"], {"fdn":row["fdn"],"n_existing":row["n"],"codex":data,"elapsed_s":env.get("elapsed_s")}
        return row["source_url"], {"fdn":row["fdn"],"error":env.get("status"),"envelope":env}
    except Exception as e:
        return row["source_url"], {"fdn":row["fdn"],"error":str(e)}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--limit",type=int,default=120)
    ap.add_argument("--concurrency",type=int,default=8)
    a=ap.parse_args()
    staging=load_staging()
    rows=[r for r in select(a.limit) if r["source_url"] not in staging]
    print("to collect(pages): %d (already staged: %d)"%(len(rows),len(staging)),flush=True)
    done=0
    with ThreadPoolExecutor(max_workers=a.concurrency) as ex:
        futs={ex.submit(run_one,r):r for r in rows}
        for f in as_completed(futs):
            url,res=f.result(); staging[url]=res; done+=1
            json.dump(staging,open(STAGING,"w"),ensure_ascii=False,indent=1)
            na=len(res.get("codex",{}).get("awardees",[])) if "codex" in res else 0
            print("[%d/%d] %s %s | 抽出%d人 (既存%s)"%(done,len(rows),"OK" if "codex" in res else "ERR",
                  (res.get("fdn") or "")[:20],na,res.get("n_existing","?")),flush=True)
    print("staged pages: %d"%len(staging))

if __name__=="__main__":
    main()
