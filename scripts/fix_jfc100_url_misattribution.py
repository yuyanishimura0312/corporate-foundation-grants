#!/usr/bin/env python3
"""jfc100バッチ 行ずれ由来のurl混入 是正。
   ページtitle実フェッチ照合で「別財団を指すurl」8件を検出(2026-07-17)。
   誤情報の除去=NULL化(正URLは捏造しない)。additive-integrity: 検証済み誤値のみ除去・正しい5件は不変。
   実行: python3 scripts/fix_jfc100_url_misattribution.py [--apply]"""
import sqlite3, sys, json, datetime
CFG="corporate_research_grants.sqlite"
APPLY="--apply" in sys.argv

# title実フェッチで別財団と確認できた誤帰属(財団名, 実際に指していた別財団)
WRONG=[
 ("公益財団法人三菱みらい育成財団","三菱財団"),
 ("公益財団法人内藤記念科学振興財団","アステラス病態代謝研究会"),
 ("公益財団法人持田記念医学薬学振興財団","テルモ生命科学振興財団"),
 ("公益財団法人小笠原敏晶記念財団","野村グループ基金"),
 ("公益財団法人船井情報科学振興財団","丸紅基金"),
 ("公益財団法人キヤノン財団","野村財団"),
 ("公益財団法人飯島藤十郎記念食品科学振興財団","国土緑化推進機構"),
 ("公益財団法人池谷科学技術振興財団","電気通信普及財団"),
]
c=sqlite3.connect(CFG); c.row_factory=sqlite3.Row
ts=datetime.datetime.now().isoformat(timespec="seconds")
n=0; skipped=[]
for name,wrong_target in WRONG:
    row=c.execute("SELECT id,url,metadata FROM organizations WHERE id LIKE 'jfc100_%' AND name=?",(name,)).fetchone()
    if not row: skipped.append(name); continue
    prov=json.dumps({"url_fix":{"reason":"jfc100 row-shift misattribution","was":row["url"],"pointed_to":wrong_target,"verified":"page-title fetch","ts":ts}},ensure_ascii=False)
    print(f"  {name}\n    誤url={row['url']} (実際は{wrong_target})")
    if APPLY:
        c.execute("UPDATE organizations SET url=NULL, metadata=? WHERE id=?",(prov,row["id"])); n+=1
if skipped: print("  未検出(スキップ):",skipped)
if APPLY:
    c.commit(); print(f"\n[applied] url誤帰属を除去: {n} 件")
    left=c.execute("SELECT COUNT(*) FROM organizations WHERE id LIKE 'jfc100_%' AND url IS NOT NULL AND url<>''").fetchone()[0]
    print(f"jfc100 残url(検証済み正しい5件想定): {left}")
else:
    print(f"\n[dry-run] 誤帰属 {len(WRONG)} 件を --apply で NULL化")
c.close()
