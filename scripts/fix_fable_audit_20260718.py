#!/usr/bin/env python3
"""fable独立監査(2026-07-18)の major欠陥2点を additive 是正。既存列・値は不変。
 ① 別名重複(同一total_assetsの6組)→ duplicate_of 列で非正本→正本を指す(集計は duplicate_of IS NULL で二重計上回避)。
    正本 = grant_programs 最多(tie: officers→source→curated)。
 ② annual_grant_year 汚染(2024codex/2024est等)→ annual_grant_fy(int) + annual_grant_is_estimate(0/1) を新列に正規化。
 実行: python3 scripts/fix_fable_audit_20260718.py [--apply]"""
import sqlite3, re, sys
DB="corporate_research_grants.sqlite"
APPLY="--apply" in sys.argv
c=sqlite3.connect(DB); c.row_factory=sqlite3.Row

# --- 列追加(冪等) ---
def addcol(col, decl):
    cols=[r[1] for r in c.execute("PRAGMA table_info(organizations)")]
    if col not in cols:
        if APPLY: c.execute(f"ALTER TABLE organizations ADD COLUMN {col} {decl}")
        print(f"  + column {col} {decl}")
addcol("duplicate_of","TEXT")
addcol("annual_grant_fy","INTEGER")
addcol("annual_grant_is_estimate","INTEGER")

# --- ① 別名重複 ---
print("\n① 別名重複 → duplicate_of:")
groups=c.execute("SELECT total_assets FROM organizations WHERE total_assets IS NOT NULL GROUP BY total_assets HAVING COUNT(*)>1").fetchall()
def score(oid):
    p=c.execute("SELECT COUNT(*) FROM grant_programs WHERE organization_id=?",(oid,)).fetchone()[0]
    o=c.execute("SELECT COUNT(*) FROM foundation_officers WHERE organization_id=?",(oid,)).fetchone()[0]
    s=1 if c.execute("SELECT metadata LIKE '%total_assets_source_url%' FROM organizations WHERE id=?",(oid,)).fetchone()[0] else 0
    cur=1 if c.execute("SELECT source_dataset IS NULL FROM organizations WHERE id=?",(oid,)).fetchone()[0] else 0
    return (p,o,s,cur)
dup_n=0
for g in groups:
    recs=c.execute("SELECT id,name FROM organizations WHERE total_assets=?",(g[0],)).fetchall()
    canon=max(recs,key=lambda r:score(r["id"]))
    for r in recs:
        if r["id"]!=canon["id"]:
            print(f"   dup: {r['name'][:26]:26s} → canon: {canon['name'][:24]}")
            if APPLY: c.execute("UPDATE organizations SET duplicate_of=? WHERE id=?",(canon["id"],r["id"]))
            dup_n+=1
print(f"   非正本マーク: {dup_n}件")

# --- ② 年度正規化 ---
print("\n② annual_grant_year 正規化:")
rows=c.execute("SELECT id, annual_grant_year FROM organizations WHERE annual_grant_amount IS NOT NULL").fetchall()
norm_n=est_n=0
for r in rows:
    y=(r["annual_grant_year"] or "").strip()
    m=re.match(r"(\d{4})", y)
    fy=int(m.group(1)) if m else None
    is_est=1 if "est" in y.lower() else 0
    if APPLY: c.execute("UPDATE organizations SET annual_grant_fy=?, annual_grant_is_estimate=? WHERE id=?",(fy,is_est,r["id"]))
    if fy: norm_n+=1
    if is_est: est_n+=1
print(f"   年度→fy(int)正規化: {norm_n}件 / 推定フラグ立て: {est_n}件 / 全対象 {len(rows)}")

if APPLY:
    c.commit(); print("\n[applied]")
else:
    print("\n[dry-run] --apply で適用")
c.close()
