#!/usr/bin/env python3
"""Phase 1c — fill municipality from our own contact_address where still NULL and
   consistent with prefecture. Deterministic, uses existing DB field only."""
import sqlite3, re, unicodedata
DB = "corporate_research_grants.sqlite"
PREF = "北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
def parse(a):
    a = unicodedata.normalize("NFKC", a or "")
    m = re.match(r'^(' + PREF + r')', a)
    if not m: return None, None
    pref = m.group(1); rest = a[len(pref):]
    mm = re.match(r'^(.+?[市区町村])', rest)
    muni = mm.group(1) if mm else None
    if muni:
        m2 = re.match(r'^(' + PREF + r')(.+[市区町村])$', muni)
        if m2: muni = m2.group(2)
    return pref, muni
c = sqlite3.connect(DB)
filled = 0
for oid, pref, caddr in c.execute("SELECT id,prefecture,contact_address FROM organizations WHERE (municipality IS NULL OR municipality='') AND contact_address IS NOT NULL AND contact_address!=''").fetchall():
    cp, cm = parse(caddr)
    # only fill when contact_address prefecture agrees with row prefecture (safety)
    if cm and cp and (not pref or cp == pref):
        c.execute("UPDATE organizations SET municipality=? WHERE id=?", (cm, oid)); filled += 1
c.commit()
print("municipality filled from contact_address:", filled)
print("municipality total now:", c.execute("SELECT COUNT(*) FROM organizations WHERE municipality IS NOT NULL AND municipality!=''").fetchone()[0])
