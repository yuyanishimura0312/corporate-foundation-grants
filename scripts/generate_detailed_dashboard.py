#!/usr/bin/env python3
"""CFG 詳細ダッシュボード生成 — 全層(団体/財務/採択者×研究者DB/役員/審査員/企業関係/応募要項/金額)を
   赤白CI textbook style で統合。実データ全数集計・自己完結HTML。"""
import sqlite3, re, unicodedata, json
from collections import Counter
CFG = "corporate_research_grants.sqlite"
c = sqlite3.connect(CFG); c.row_factory = sqlite3.Row
def q(s, *a): return c.execute(s, a).fetchall()
def one(s, *a): return c.execute(s, a).fetchone()[0]
NAME11 = {"natural_science":"自然科学","life_science":"生命科学・医学","engineering":"工学・技術","humanities_social":"人文社会科学","arts_culture":"芸術・文化","education":"教育・人材育成","welfare":"福祉・健康","environment":"環境","international":"国際交流・協力","regional":"地域","interdisciplinary":"学際・融合"}
SUBTYPE = {"corporate":"企業財団","academic":"学術系","individual":"個人記念","ngo":"市民活動","intl":"国際機関","govt":"政府系","group":"企業グループ","other":"その他"}
fmap = json.load(open("research_results/grant_field_map.json"))

def bars(data, namef=lambda x:x, unit="", maxn=None):
    items = data[:maxn] if maxn else data
    mx = max((n for _, n in items), default=1)
    return "".join('<div class="bar"><span class="bl">%s</span><span class="bt"><span class="bf" style="width:%d%%"></span></span><span class="bn">%s%s</span></div>' % (namef(k), int(100*n/mx), "{:,}".format(n), unit) for k, n in items)

# ---- collect all sections ----
tot = one("SELECT COUNT(*) FROM organizations")
koeki = one("SELECT COUNT(*) FROM organizations WHERE koeki_verified=1")
subtype = [(SUBTYPE.get(r[0],r[0]), r[1]) for r in q("SELECT foundation_subtype,COUNT(*) FROM organizations GROUP BY foundation_subtype ORDER BY 2 DESC")]
legal = q("SELECT legal_form,COUNT(*) FROM organizations WHERE legal_form!='' GROUP BY legal_form ORDER BY 2 DESC LIMIT 7")
admin = [("国（内閣府）", one("SELECT COUNT(*) FROM organizations WHERE admin_agency='内閣府'")),
         ("都道府県管轄", one("SELECT COUNT(*) FROM organizations WHERE admin_agency IS NOT NULL AND admin_agency!='内閣府'"))]
pref = q("SELECT prefecture,COUNT(*) FROM organizations WHERE prefecture!='' GROUP BY prefecture ORDER BY 2 DESC")
relv = q("SELECT research_relevance,COUNT(*) FROM organizations WHERE research_relevance IS NOT NULL GROUP BY research_relevance ORDER BY 2 DESC")

n_award = one("SELECT COUNT(*) FROM grant_results WHERE grant_type='research_individual'")
n_rid = one("SELECT COUNT(*) FROM grant_results WHERE rid_base_id IS NOT NULL")
aw_found = q("SELECT o.name,COUNT(*) FROM grant_results r JOIN grant_calls gc ON r.call_id=gc.id JOIN grant_programs p ON gc.program_id=p.id JOIN organizations o ON p.organization_id=o.id WHERE r.grant_type='research_individual' GROUP BY o.id ORDER BY 2 DESC LIMIT 15")
field55 = q("SELECT rid_field,COUNT(*) FROM grant_results WHERE rid_field IS NOT NULL GROUP BY rid_field ORDER BY 2 DESC LIMIT 15")

n_off = one("SELECT COUNT(*) FROM foundation_officers")
role = q("SELECT role,COUNT(*) FROM foundation_officers WHERE role IS NOT NULL GROUP BY role ORDER BY 2 DESC")
ROLE = {"reviewer":"審査員/選考委員","director":"理事","councilor":"評議員","chair":"理事長/会長","auditor":"監事","other":"その他"}
n_exec = one("SELECT COUNT(*) FROM foundation_officers WHERE is_corporate_exec=1")
corp = q("SELECT corporate_name,COUNT(*) FROM foundation_officers WHERE is_corporate_exec=1 AND corporate_name!='' GROUP BY corporate_name ORDER BY 2 DESC LIMIT 12")
off_found = q("SELECT o.name,COUNT(*) FROM foundation_officers fo JOIN organizations o ON o.id=fo.organization_id GROUP BY o.id ORDER BY 2 DESC LIMIT 10")

elig = {}
for t in ("age","career_stage","nationality","position","affiliation_type","gender"):
    elig[t] = [(r[0][:30], r[1]) for r in q("SELECT description,COUNT(*) FROM eligibility_criteria WHERE criterion_type=? AND description!='' GROUP BY description ORDER BY 2 DESC LIMIT 6", t)]
n_elig = one("SELECT COUNT(*) FROM eligibility_criteria")

namt = one("SELECT COUNT(*) FROM grant_amounts WHERE amount_per_award>0")
amt_med = one("SELECT amount_per_award FROM grant_amounts WHERE amount_per_award>0 ORDER BY amount_per_award LIMIT 1 OFFSET (SELECT COUNT(*)/2 FROM grant_amounts WHERE amount_per_award>0)")

def kv(items): return "".join('<div class="kv"><span class="kk">%s</span><span class="kvv">%s</span></div>' % (k, v) for k, v in items)
