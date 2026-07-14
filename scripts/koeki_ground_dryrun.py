#!/usr/bin/env python3
"""Dry-run: match DB organizations to authoritative koeki registry by normalized name."""
import sqlite3, json, re, unicodedata
DB = "corporate_research_grants.sqlite"

def norm(s):
    if not s: return ""
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r'(公益|一般|特定非営利活動|認定特定非営利活動)?(財団|社団)?法人', '', s)
    s = re.sub(r'[\s　・,，.。()（）「」『』]', '', s)
    return s.strip().lower()

koeki = json.load(open("data/koeki_all_foundations.json"))
kmap = {}
for r in koeki:
    k = norm(r["name"])
    if k and k not in kmap:
        kmap[k] = r

c = sqlite3.connect(DB); c.row_factory = sqlite3.Row
orgs = c.execute("SELECT id,name,legal_form,foundation_subtype,prefecture,municipality,contact_address FROM organizations").fetchall()

PREF = "北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
def split_addr(a):
    a = unicodedata.normalize("NFKC", a or "")
    m = re.match(r'^(' + PREF + r')', a)
    pref = m.group(1) if m else None
    muni = None
    if pref:
        rest = a[len(pref):]
        mm = re.match(r'^(.+?[市区町村])', rest)
        muni = mm.group(1) if mm else None
    return pref, muni

matched = 0; pref_fillable = 0; muni_fillable = 0; pref_conflict = 0
public = 0
samples = []
for o in orgs:
    is_public = (o["legal_form"] in ("公益財団法人", "公益社団法人", "一般財団法人", "一般社団法人"))
    if is_public: public += 1
    k = norm(o["name"])
    r = kmap.get(k)
    if not r:
        continue
    matched += 1
    pref, muni = split_addr(r["address"])
    if pref and not o["prefecture"]:
        pref_fillable += 1
    if pref and o["prefecture"] and pref != o["prefecture"]:
        pref_conflict += 1
    if muni and not o["municipality"]:
        muni_fillable += 1
    if len(samples) < 6:
        samples.append((o["name"], r["admin"], pref, muni))

print("orgs total: %d ; public-ish (公益/一般 財団社団): %d" % (len(orgs), public))
print("koeki registry records: %d ; unique normalized keys: %d" % (len(koeki), len(kmap)))
print("MATCHED orgs -> koeki: %d (%.1f%% of all, %.1f%% of public-ish)" % (matched, 100*matched/len(orgs), 100*matched/public))
print("  prefecture fillable (empty->authoritative): %d" % pref_fillable)
print("  prefecture CONFLICT (db != koeki): %d" % pref_conflict)
print("  municipality fillable (empty->authoritative): %d" % muni_fillable)
print("samples (name | admin所管 | pref | muni):")
for s in samples: print("   ", s)

# reverse: research foundations in koeki NOT in our db
kres = json.load(open("data/koeki_research_foundations.json"))
dbkeys = set(norm(o["name"]) for o in orgs)
missing = [r for r in kres if norm(r["name"]) and norm(r["name"]) not in dbkeys]
print("\nkoeki research-foundations: %d ; NOT in our DB (candidate new): %d" % (len(kres), len(missing)))
for r in missing[:8]: print("    +", r["name"], "|", r.get("admin"))
