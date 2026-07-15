#!/usr/bin/env python3
"""金額投入v2 — fable指摘の是正版。専用テーブル grant_amounts に格納(行マッチ問題を回避)。
   名前照合ゲート・古データ除外・単位除外・総額型隔離(amount_type)・重複排除。汚染したgrant_programs付与はrevert。"""
import sqlite3, json, os, re, unicodedata, uuid
CFG = "corporate_research_grants.sqlite"
STAGING = "research_results/codex_amounts_staging.json"
NOW = "2026-07-16"
def sane(v): return isinstance(v, int) and 0 < v < 100_000_000_000
def nm(s):
    s = re.sub(r'(公益|一般|認定)?(財団|社団|特定非営利活動)?法人|株式会社|旧[：:].*|（.*?）|\(.*?\)', '', unicodedata.normalize("NFKC", s or ""))
    return re.sub(r'[\s　・,，.。「」]', '', s).strip().lower()
UNIT_FLAG = ("日額", "月額", "貸与", "／月", "/月", "／日", "/日", "月あたり", "日あたり")
JUNK = ("応募要領", "お知らせ", "構成", "実施のお知らせ", "募集要項について", ">>", "一覧")
c = sqlite3.connect(CFG)
# revert 汚染付与
c.execute("UPDATE grant_programs SET amount_per_award=NULL, num_awards_per_year=NULL, program_total_budget=NULL, amount_source_url=NULL WHERE amount_per_award IS NOT NULL")
# clean table
c.execute("DROP TABLE IF EXISTS grant_amounts")
c.execute("""CREATE TABLE grant_amounts (
  id TEXT PRIMARY KEY, organization_id TEXT REFERENCES organizations(id),
  foundation_name TEXT, program_name TEXT, amount_per_award INTEGER, amount_type TEXT,
  num_awards_per_year INTEGER, total_budget INTEGER, fiscal_year INTEGER, target_field TEXT,
  source_url TEXT, source_dataset TEXT DEFAULT 'codex_amounts_2026', created_at TEXT)""")
staging = json.load(open(STAGING))
plan = {"foundations": 0, "amounts": 0, "misattr": 0, "stale": 0, "unit": 0, "junk": 0, "total_isolated": 0, "dup": 0}
seen = set()
for fname, rec in staging.items():
    cx = rec.get("codex")
    if not cx or not cx.get("programs"): continue
    if nm(cx.get("foundation_name")) and nm(fname) and nm(cx["foundation_name"]) != nm(fname) \
       and nm(cx["foundation_name"]) not in nm(fname) and nm(fname) not in nm(cx["foundation_name"]):
        plan["misattr"] += len(cx["programs"]); continue
    src = cx.get("source_url")
    org = c.execute("SELECT id FROM organizations WHERE name=?", (fname,)).fetchone()
    if not org or not src: continue
    oid = org[0]; plan["foundations"] += 1
    for pg in cx["programs"]:
        pname = pg.get("program_name") or ""
        fy = pg.get("fiscal_year")
        if fy and fy < 2023: plan["stale"] += 1; continue
        if any(j in pname for j in JUNK): plan["junk"] += 1; continue
        if any(u in (pname + (cx.get("notes") or "")) for u in UNIT_FLAG): plan["unit"] += 1; continue
        per = pg.get("amount_per_award_jpy"); mx = pg.get("amount_max_jpy"); tot = pg.get("total_budget_jpy")
        num = pg.get("num_awards_per_year")
        amt = per or mx
        if amt is not None and not sane(amt): continue
        if not amt: continue
        # 総額型隔離: per無し & (num>1 or 名前に総額/年間) & 額>=5000万 → totalへ
        atype = "per_award"
        if not per and amt >= 50_000_000 and (num and num > 1 or "総額" in pname or "総額" in (cx.get("notes") or "")):
            tot = amt; amt = None; atype = "total_only"; plan["total_isolated"] += 1
        key = (oid, nm(pname), amt)
        if key in seen: plan["dup"] += 1; continue
        seen.add(key)
        c.execute("""INSERT INTO grant_amounts (id,organization_id,foundation_name,program_name,amount_per_award,amount_type,num_awards_per_year,total_budget,fiscal_year,target_field,source_url,created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (str(uuid.uuid4()), oid, fname, pname, amt, atype, num, tot, fy, pg.get("target_field"), src, NOW))
        if amt: plan["amounts"] += 1
c.commit()
print(json.dumps(plan, ensure_ascii=False))
n = c.execute("SELECT COUNT(*) FROM grant_amounts WHERE amount_per_award>0").fetchone()[0]
print("grant_amounts per_award:", n)
