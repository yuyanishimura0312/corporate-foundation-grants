#!/usr/bin/env python3
"""Phase 3b — apply fable final-audit corrections (deterministic, by foundation name).
   NULL out fable-flagged unreliable financial values + preserve reason in metadata review.
   Non-destructive elsewhere. Idempotent."""
import sqlite3, json
DB = "corporate_research_grants.sqlite"
NOW = "2026-07-15"
# (name_substring, field, reason) — from fable final audit 2026-07-15
CORR = [
    ("臨床研究奨励基金", "annual_grant_amount", "codex値15.2Mが出典に不在(実額12.7M・fable)"),
    ("三井住友海上福祉財団", "annual_grant_amount", "codex値が出典に不在=丸め誤り(実額61,153,868・fable)"),
    ("むつ小川原", "annual_grant_amount", "出典未検証・資産除外との一貫性(fable)"),
    ("日本国際教育支援協会", "annual_grant_amount", "JEES出典不信・資産除外との一貫性(fable)"),
    ("立石科学技術振興財団", "total_assets", "サイト403で検証不能+正味財産取り違えの疑い(fable)"),
    ("ちゅうでん教育振興財団", "total_assets", "スキャンPDF照合不能(fable)"),
]
c = sqlite3.connect(DB); c.row_factory = sqlite3.Row
done = []
for sub, field, reason in CORR:
    for r in c.execute("SELECT id,name,metadata,%s AS val FROM organizations WHERE name LIKE ? AND %s IS NOT NULL AND metadata LIKE '%%financials_provenance%%'" % (field, field), ("%" + sub + "%",)).fetchall():
        try: meta = json.loads(r["metadata"]) if r["metadata"] else {}
        except Exception: meta = {}
        meta.setdefault("financials_review", []).append({"field": field, "removed_value": r["val"], "reason": reason, "at": NOW})
        sets = {field: None, "metadata": json.dumps(meta, ensure_ascii=False), "updated_at": NOW}
        if field == "annual_grant_amount":
            sets["annual_grant_year"] = None
        cols = ",".join("%s=?" % k for k in sets)
        c.execute("UPDATE organizations SET %s WHERE id=?" % cols, (*sets.values(), r["id"]))
        done.append((r["name"], field, r["val"]))
c.commit()
print("corrected (value NULLed + review preserved):")
for n, f, v in done: print("  -", n[:26], "|", f, "|", v)
print("counts now: est %d / total_assets %d / annual_grant %d" % (
    c.execute("SELECT COUNT(*) FROM organizations WHERE established_year IS NOT NULL").fetchone()[0],
    c.execute("SELECT COUNT(*) FROM organizations WHERE total_assets IS NOT NULL").fetchone()[0],
    c.execute("SELECT COUNT(*) FROM organizations WHERE annual_grant_amount IS NOT NULL").fetchone()[0]))
