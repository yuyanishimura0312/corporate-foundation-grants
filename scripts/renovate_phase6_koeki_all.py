#!/usr/bin/env python3
"""Phase 6 網羅性向上 — additive ingestion of ALL remaining koeki 公益財団法人 (koeki_all) not yet in DB.
   Completes the national 公益財団 registry coverage. Source: 内閣府 公益法人information (data/koeki_all_foundations.json).
   Distinct source_dataset='koeki_all_2026' so the research subset (koeki_research_2026) stays distinguishable.
   Deterministic. Provisional (needs_program_verification=1). No fabrication."""
import sqlite3, json, re, unicodedata, uuid
DB = "corporate_research_grants.sqlite"
DATASET = "koeki_all_2026"
NOW = "2026-07-15"

def nk(s): return unicodedata.normalize("NFKC", s) if s else s
def norm(s):
    if not s: return ""
    s = nk(s)
    s = re.sub(r'[（(](公財|公社|一財|一社|特非|社福|独|国|地独|福|学|宗)[）)]', '', s)
    s = re.sub(r'(公益|一般|特定非営利活動|認定特定非営利活動)?(財団|社団)?法人', '', s)
    return re.sub(r'[\s　・,，.。()（）「」『』]', '', s).strip().lower()
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
dbkeys = set(norm(r[0]) for r in c.execute("SELECT name FROM organizations"))
allf = json.load(open("data/koeki_all_foundations.json"))
seen = {}
for r in allf:
    k = norm(r["name"])
    if not k: continue
    if k not in seen or (not seen[k].get("address") and r.get("address")): seen[k] = r
added = skipped = nopref = 0
for k, r in seen.items():
    if k in dbkeys: skipped += 1; continue
    pref, muni = split_addr(r.get("address"))
    if not pref: nopref += 1; continue
    name = nk(r["name"]); admin = nk(r.get("admin") or "").strip() or None
    purpose = nk(r.get("purpose") or "")
    desc = purpose if (purpose and not purpose.endswith("_kana") and len(purpose) > 6) else None
    c.execute("""INSERT INTO organizations
        (id,name,type,legal_form,foundation_subtype,prefecture,municipality,admin_agency,
         koeki_verified,koeki_matched_name,description,source_dataset,needs_program_verification,ingested_at,created_at,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (str(uuid.uuid4()), name, "foundation", legal_of(name), "other", pref, muni, admin,
         1, r["name"], desc, DATASET, 1, NOW, NOW, NOW))
    dbkeys.add(k); added += 1
c.commit()
print(json.dumps({"added": added, "skipped_in_db": skipped, "skipped_no_pref": nopref,
                  "organizations_total_now": c.execute("SELECT COUNT(*) FROM organizations").fetchone()[0]}, ensure_ascii=False, indent=1))
