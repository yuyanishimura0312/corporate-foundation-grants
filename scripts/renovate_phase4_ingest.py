#!/usr/bin/env python3
"""Phase 4 — additive ingestion of authoritative koeki research foundations not yet in DB.
   Source: 内閣府 公益法人information (data/koeki_research_foundations.json, category 学術・科学技術).
   Deterministic. No fabrication: only name/address/admin/purpose from authoritative registry.
   New rows are clearly flagged (source_dataset / needs_program_verification) as provisional
   (program/grant-call/awardee data not yet attached — that is Phase 3/5)."""
import sqlite3, json, re, unicodedata, uuid
DB = "corporate_research_grants.sqlite"
DATASET = "koeki_research_2026"
NOW = "2026-07-15"

def nk(s): return unicodedata.normalize("NFKC", s) if s else s
def norm(s):
    if not s: return ""
    s = nk(s)
    s = re.sub(r'(公益|一般|特定非営利活動|認定特定非営利活動)?(財団|社団)?法人', '', s)
    s = re.sub(r'[\s　・,，.。()（）「」『』]', '', s)
    return s.strip().lower()
PREF = "北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
def split_addr(a):
    a = nk(a or ""); m = re.match(r'^(' + PREF + r')', a)
    if not m: return None, None
    pref = m.group(1); rest = a[len(pref):]
    mm = re.match(r'^(.+?[市区町村])', rest); muni = mm.group(1) if mm else None
    if muni:
        m2 = re.match(r'^(' + PREF + r')(.+[市区町村])$', muni)
        if m2: muni = m2.group(2)
    return pref, muni
def legal_of(name):
    n = nk(name)
    for p in ("公益財団法人", "公益社団法人", "一般財団法人", "一般社団法人"):
        if n.startswith(p): return p
    return "その他"

c = sqlite3.connect(DB); c.row_factory = sqlite3.Row
# idempotent columns
cols = [r[1] for r in c.execute("PRAGMA table_info(organizations)")]
for col, typ in [("source_dataset", "TEXT"), ("needs_program_verification", "INTEGER"), ("ingested_at", "TEXT")]:
    if col not in cols:
        c.execute("ALTER TABLE organizations ADD COLUMN %s %s" % (col, typ))

dbkeys = set(norm(r[0]) for r in c.execute("SELECT name FROM organizations"))
res = json.load(open("data/koeki_research_foundations.json"))
# dedup source by normalized name, prefer record with an address
seen = {}
for r in res:
    k = norm(r["name"])
    if not k: continue
    if k not in seen or (not seen[k].get("address") and r.get("address")):
        seen[k] = r

added = 0; skipped_dup = 0; no_pref = 0
for k, r in seen.items():
    if k in dbkeys:
        skipped_dup += 1; continue
    pref, muni = split_addr(r.get("address"))
    if not pref:
        no_pref += 1; continue
    name = nk(r["name"])
    admin = nk(r.get("admin") or "").strip() or None
    purpose = nk(r.get("purpose") or "")
    desc = purpose if (purpose and not purpose.endswith("_kana") and len(purpose) > 6) else None
    oid = str(uuid.uuid4())
    c.execute("""INSERT INTO organizations
        (id,name,type,legal_form,foundation_subtype,prefecture,municipality,admin_agency,
         koeki_verified,koeki_matched_name,description,source_dataset,needs_program_verification,
         ingested_at,created_at,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (oid, name, "foundation", legal_of(name), "other", pref, muni, admin,
         1, r["name"], desc, DATASET, 1, NOW, NOW, NOW))
    dbkeys.add(k); added += 1

c.commit()
tot = c.execute("SELECT COUNT(*) FROM organizations").fetchone()[0]
print(json.dumps({"added": added, "skipped_already_in_db": skipped_dup, "skipped_no_prefecture": no_pref,
                  "organizations_total_now": tot}, ensure_ascii=False, indent=1))
print("new rows prefecture top:")
for row in c.execute("SELECT prefecture,COUNT(*) n FROM organizations WHERE source_dataset=? GROUP BY prefecture ORDER BY n DESC LIMIT 8", (DATASET,)):
    print("   ", row[0], row[1])
print("sample new rows:")
for row in c.execute("SELECT name,prefecture,municipality,admin_agency,legal_form FROM organizations WHERE source_dataset=? LIMIT 5", (DATASET,)):
    print("   ", tuple(row))
