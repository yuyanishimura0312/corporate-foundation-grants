#!/usr/bin/env python3
"""CFG renovation Phase 1 — deterministic, authoritative-source grounded, no fabrication.
   (a) koeki grounding: municipality / prefecture-fill / admin_agency / koeki_verified
   (b) quality fixes: fullwidth-digit normalization, non-http url, suspicious names
   (c) freshness: normalize dates, derive fiscal_year (年度), close past-deadline calls
   Conflicts are LOGGED, never auto-overwritten. Idempotent.
"""
import sqlite3, json, re, unicodedata, datetime
DB = "corporate_research_grants.sqlite"
TODAY = "2026-07-15"
c = sqlite3.connect(DB); c.row_factory = sqlite3.Row
log = {"phase": "1", "date": TODAY, "changes": {}}

def has_col(t, col):
    return col in [r[1] for r in c.execute("PRAGMA table_info(%s)" % t)]

# --- add provenance columns (idempotent) ---
for ddl in [("organizations", "admin_agency", "TEXT"),
            ("organizations", "koeki_verified", "INTEGER"),
            ("organizations", "koeki_matched_name", "TEXT")]:
    if not has_col(ddl[0], ddl[1]):
        c.execute("ALTER TABLE %s ADD COLUMN %s %s" % ddl)

def norm(s):
    if not s: return ""
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r'(公益|一般|特定非営利活動|認定特定非営利活動)?(財団|社団)?法人', '', s)
    s = re.sub(r'[\s　・,，.。()（）「」『』]', '', s)
    return s.strip().lower()

PREF = "北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
def split_addr(a):
    a = unicodedata.normalize("NFKC", a or "")
    m = re.match(r'^(' + PREF + r')', a)
    if not m: return None, None
    pref = m.group(1); rest = a[len(pref):]
    mm = re.match(r'^(.+?[市区町村])', rest)
    muni = mm.group(1) if mm else None
    if muni:  # strip accidental leading prefecture repeat
        m2 = re.match(r'^(' + PREF + r')(.+[市区町村])$', muni)
        if m2: muni = m2.group(2)
    return pref, muni

# --- (a) koeki grounding ---
koeki = json.load(open("data/koeki_all_foundations.json"))
kmap = {}
for r in koeki:
    k = norm(r["name"])
    if k and k not in kmap: kmap[k] = r

muni_filled = pref_filled = admin_set = verified = 0
conflicts = []
for o in c.execute("SELECT id,name,prefecture,municipality FROM organizations").fetchall():
    r = kmap.get(norm(o["name"]))
    if not r: continue
    verified += 1
    pref, muni = split_addr(r["address"])
    admin = unicodedata.normalize("NFKC", r["admin"] or "").strip() or None
    c.execute("UPDATE organizations SET koeki_verified=1, koeki_matched_name=?, admin_agency=? WHERE id=?",
              (r["name"], admin, o["id"]))
    if admin: admin_set += 1
    if muni and not (o["municipality"] or "").strip():
        c.execute("UPDATE organizations SET municipality=? WHERE id=?", (muni, o["id"])); muni_filled += 1
    if pref:
        if not (o["prefecture"] or "").strip():
            c.execute("UPDATE organizations SET prefecture=? WHERE id=?", (pref, o["id"])); pref_filled += 1
        elif pref != o["prefecture"]:
            conflicts.append({"id": o["id"], "name": o["name"], "db_pref": o["prefecture"], "koeki_pref": pref, "koeki_addr": r["address"]})
log["changes"]["koeki_verified"] = verified
log["changes"]["municipality_filled"] = muni_filled
log["changes"]["prefecture_filled"] = pref_filled
log["changes"]["admin_agency_set"] = admin_set
log["changes"]["prefecture_conflicts_logged"] = len(conflicts)

# --- (b) quality fixes ---
# non-http url
url_fixed = 0
for o in c.execute("SELECT id,url FROM organizations WHERE url IS NOT NULL AND url!='' AND url NOT LIKE 'http%'").fetchall():
    u = o["url"].strip()
    if re.match(r'^www\.|^[a-z0-9.-]+\.(jp|com|org|net|go\.jp|or\.jp|ac\.jp)', u, re.I):
        c.execute("UPDATE organizations SET url=? WHERE id=?", ("https://" + u, o["id"])); url_fixed += 1
log["changes"]["url_prefixed_https"] = url_fixed
# suspicious names (report only)
susp = [dict(id=r["id"], name=r["name"]) for r in c.execute("SELECT id,name FROM organizations WHERE name LIKE '%test%' OR name LIKE '%サンプル%' OR name LIKE '%unknown%' OR name LIKE '%未定%' OR LENGTH(name)<3").fetchall()]
log["changes"]["suspicious_names"] = susp

# --- (c) freshness: normalize fullwidth digits in date fields, derive FY, close past ---
def nfkc(s): return unicodedata.normalize("NFKC", s) if s else s
date_norm = 0
for row in c.execute("SELECT id,application_start,application_deadline FROM grant_calls").fetchall():
    upd = {}
    for f in ("application_start", "application_deadline"):
        v = row[f]
        if v and v != nfkc(v):
            upd[f] = nfkc(v)
    if upd:
        sets = ",".join("%s=?" % k for k in upd)
        c.execute("UPDATE grant_calls SET %s WHERE id=?" % sets, (*upd.values(), row["id"])); date_norm += 1
log["changes"]["dates_fullwidth_normalized"] = date_norm

# derive fiscal_year (年度: FY=year if month>=4 else year-1) from deadline where missing
fy_derived = 0
for row in c.execute("SELECT id,application_deadline FROM grant_calls WHERE fiscal_year IS NULL AND application_deadline IS NOT NULL AND application_deadline!=''").fetchall():
    m = re.match(r'^(\d{4})-(\d{2})', nfkc(row["application_deadline"]))
    if m:
        y, mo = int(m.group(1)), int(m.group(2))
        fy = y if mo >= 4 else y - 1
        c.execute("UPDATE grant_calls SET fiscal_year=? WHERE id=?", (fy, row["id"])); fy_derived += 1
log["changes"]["fiscal_year_derived_from_deadline"] = fy_derived

# close calls whose deadline is in the past and status not already closed
closed = 0
for row in c.execute("SELECT id,application_deadline,status FROM grant_calls WHERE application_deadline IS NOT NULL AND application_deadline!=''").fetchall():
    d = nfkc(row["application_deadline"])
    if re.match(r'^\d{4}-\d{2}-\d{2}$', d) and d < TODAY and (row["status"] or "") not in ("closed",):
        c.execute("UPDATE grant_calls SET status='closed' WHERE id=?", (row["id"],)); closed += 1
log["changes"]["calls_closed_past_deadline"] = closed

c.commit()
json.dump({"conflicts": conflicts}, open("research_results/phase1_prefecture_conflicts.json", "w"), ensure_ascii=False, indent=1)
json.dump(log, open("research_results/phase1_changes.json", "w"), ensure_ascii=False, indent=1)
print(json.dumps(log["changes"], ensure_ascii=False, indent=1))
print("\nconflicts (logged, NOT applied):")
for x in conflicts[:12]: print("  ", x["name"], ":", x["db_pref"], "vs koeki", x["koeki_pref"], "|", x["koeki_addr"][:30])
