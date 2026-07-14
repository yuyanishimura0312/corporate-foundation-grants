#!/usr/bin/env python3
"""CFG renovation Phase 1b — correct koeki grounding per fable's independent review.
   Fixes: (1) same-name multi-entity disambiguation (was first-wins -> contaminated 奥村奨学会);
          (2) intra-row inconsistency: adopt koeki as authoritative for prefecture on confident
              matches so prefecture/municipality/admin are consistent (old value logged, not lost).
   Recomputes grounding deterministically from the PRE-renovation backup ground-truth. Idempotent.
"""
import sqlite3, json, re, unicodedata, glob
DB = "corporate_research_grants.sqlite"
BACKUP = sorted(glob.glob("backups/*.pre-renovation.sqlite"))[-1]

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
    if muni:
        m2 = re.match(r'^(' + PREF + r')(.+[市区町村])$', muni)
        if m2: muni = m2.group(2)
    return pref, muni
def pref_of(addr):
    return split_addr(addr)[0]

# --- original ground truth from backup ---
b = sqlite3.connect(BACKUP)
orig = {r[0]: {"pref": (r[1] or "").strip(), "muni": (r[2] or "").strip(), "caddr": (r[3] or "")}
        for r in b.execute("SELECT id,prefecture,municipality,contact_address FROM organizations")}
b.close()

# --- koeki groups: name_norm -> list of unique-address records ---
koeki = json.load(open("data/koeki_all_foundations.json"))
groups = {}
for r in koeki:
    k = norm(r["name"])
    if not k: continue
    groups.setdefault(k, [])
    if not any(x["address"] == r["address"] for x in groups[k]):
        groups[k].append(r)

def choose(org_id, recs):
    """Return (record, method) or (None, reason). Disambiguate multi-entity by address/prefecture."""
    if len(recs) == 1:
        return recs[0], "unique"
    # multiple distinct addresses -> ambiguous, disambiguate
    o = orig[org_id]
    cand_by_caddr = [r for r in recs if o["caddr"] and pref_of(r["address"]) and pref_of(r["address"]) in unicodedata.normalize("NFKC", o["caddr"])]
    # stronger: contact_address startswith koeki address prefecture+muni
    if o["caddr"]:
        cad = unicodedata.normalize("NFKC", o["caddr"])
        exact = [r for r in recs if unicodedata.normalize("NFKC", r["address"])[:8] and cad.startswith(unicodedata.normalize("NFKC", r["address"])[:8])]
        if len(exact) == 1:
            return exact[0], "disambig_by_contact_address"
    if len(cand_by_caddr) == 1:
        return cand_by_caddr[0], "disambig_by_caddr_pref"
    cand_by_pref = [r for r in recs if o["pref"] and pref_of(r["address"]) == o["pref"]]
    if len(cand_by_pref) == 1:
        return cand_by_pref[0], "disambig_by_db_pref"
    return None, "ambiguous_unresolved"

c = sqlite3.connect(DB); c.row_factory = sqlite3.Row
stats = {"verified": 0, "muni_filled": 0, "pref_filled": 0, "admin_set": 0,
         "pref_superseded_authoritative": 0, "ambiguous_unresolved": 0, "disambiguated": 0,
         "reverted_bad_ground": 0}
superseded = []; ambiguous = []
for o in c.execute("SELECT id,name FROM organizations").fetchall():
    oid = o["id"]; og = orig[oid]
    recs = groups.get(norm(o["name"]))
    # default = restore original (this reverts any wrong Phase-1 fill)
    new_pref = og["pref"] or None
    new_muni = og["muni"] or None
    verified = None; matched = None; admin = None
    if recs:
        rec, method = choose(oid, recs)
        if rec is None:
            stats["ambiguous_unresolved"] += 1
            ambiguous.append({"id": oid, "name": o["name"], "n_addr": len(recs)})
            if not (og["muni"]): new_muni = None  # ensure any phase1 wrong fill removed
        else:
            if method.startswith("disambig"): stats["disambiguated"] += 1
            kp, km = split_addr(rec["address"])
            admin = unicodedata.normalize("NFKC", rec["admin"] or "").strip() or None
            verified = 1; matched = rec["name"]
            if admin: stats["admin_set"] += 1
            stats["verified"] += 1
            # municipality
            if og["muni"]:
                new_muni = og["muni"]
            elif km:
                new_muni = km; stats["muni_filled"] += 1
            # prefecture (koeki authoritative on confident match)
            if not og["pref"]:
                if kp: new_pref = kp; stats["pref_filled"] += 1
            elif kp and kp != og["pref"]:
                superseded.append({"id": oid, "name": o["name"], "old": og["pref"], "new": kp, "koeki_addr": rec["address"]})
                new_pref = kp; stats["pref_superseded_authoritative"] += 1
    # detect revert of a bad phase-1 fill (orig empty muni but current had value while now None/other)
    c.execute("UPDATE organizations SET prefecture=?, municipality=?, koeki_verified=?, koeki_matched_name=?, admin_agency=? WHERE id=?",
              (new_pref, new_muni, verified, matched, admin, oid))
c.commit()
json.dump(superseded, open("research_results/phase1b_prefecture_superseded.json", "w"), ensure_ascii=False, indent=1)
json.dump(ambiguous, open("research_results/phase1b_ambiguous_unresolved.json", "w"), ensure_ascii=False, indent=1)
json.dump(stats, open("research_results/phase1b_stats.json", "w"), ensure_ascii=False, indent=1)
print(json.dumps(stats, ensure_ascii=False, indent=1))
print("\nprefecture superseded (koeki authoritative), sample:")
for s in superseded[:12]: print("  ", s["name"], ":", s["old"], "->", s["new"], "|", s["koeki_addr"][:26])
print("\nambiguous unresolved (grounding skipped):")
for a in ambiguous[:12]: print("  ", a["name"], "(", a["n_addr"], "addrs)")
# spot check 奥村
row = c.execute("SELECT name,prefecture,municipality,admin_agency,koeki_matched_name,koeki_verified FROM organizations WHERE name LIKE '%奥村奨学会%'").fetchone()
print("\n奥村奨学会 after:", dict(row) if row else None)
