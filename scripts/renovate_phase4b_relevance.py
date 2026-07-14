#!/usr/bin/env python3
"""Phase 4b — deterministic research-relevance tagging (addresses fable's scope-dilution finding).
   Transparent keyword heuristic over name+description. Lets the research-grant-strategy use case
   filter genuine research/scholarship foundations from the broad public-interest landscape.
   Additive column research_relevance in {high, medium, low}. Non-destructive."""
import sqlite3, json
DB = "corporate_research_grants.sqlite"
c = sqlite3.connect(DB)
if "research_relevance" not in [r[1] for r in c.execute("PRAGMA table_info(organizations)")]:
    c.execute("ALTER TABLE organizations ADD COLUMN research_relevance TEXT")

HIGH = ["研究", "学術", "科学", "奨学", "学会", "医学", "工学", "理学", "化学", "物理", "生命", "ゲノム",
        "がん", "医療研究", "技術振興", "科学技術", "学振", "アカデ", "博士", "教授"]
MED = ["振興", "技術", "教育", "文化財", "芸術", "医療", "環境", "農", "生物多様", "エネルギー", "宇宙", "海洋", "情報"]

def rel(name, desc):
    t = (name or "") + " " + (desc or "")
    if any(k in t for k in HIGH): return "high"
    if any(k in t for k in MED): return "medium"
    return "low"

cnt = {"high": 0, "medium": 0, "low": 0}
for oid, name, desc in c.execute("SELECT id,name,description FROM organizations").fetchall():
    r = rel(name, desc)
    c.execute("UPDATE organizations SET research_relevance=? WHERE id=?", (r, oid)); cnt[r] += 1
c.commit()
print("research_relevance overall:", json.dumps(cnt, ensure_ascii=False))
print("among NEW provisional (koeki_research_2026):")
d = {r[0]: r[1] for r in c.execute("SELECT research_relevance,COUNT(*) FROM organizations WHERE source_dataset='koeki_research_2026' GROUP BY research_relevance")}
print("  ", json.dumps(d, ensure_ascii=False))
print("among ORIGINAL curated (source_dataset IS NULL):")
d2 = {r[0]: r[1] for r in c.execute("SELECT research_relevance,COUNT(*) FROM organizations WHERE source_dataset IS NULL GROUP BY research_relevance")}
print("  ", json.dumps(d2, ensure_ascii=False))
