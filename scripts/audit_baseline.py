#!/usr/bin/env python3
"""CFG baseline audit — deterministic, no web. 4 axes: gaps/quality/coverage/freshness."""
import sqlite3, json, re
from collections import Counter
DB = "corporate_research_grants.sqlite"
c = sqlite3.connect(DB); c.row_factory = sqlite3.Row
def q(s): return c.execute(s).fetchall()
def one(s): return c.execute(s).fetchone()[0]
out = []
def P(x): out.append(str(x)); print(x)

N = one("SELECT COUNT(*) FROM organizations")
cols = [r[1] for r in q("PRAGMA table_info(organizations)")]
P("=" * 60); P("AXIS A -- gaps (organizations n=%d)" % N)
for col in ["url", "prefecture", "municipality", "legal_form", "foundation_subtype",
            "established_year", "annual_grant_amount", "total_assets", "koeki_id",
            "founder_name", "contact_address", "description", "name_en"]:
    if col in cols:
        f = one("SELECT COUNT(*) FROM organizations WHERE %s IS NOT NULL AND %s!=''" % (col, col))
        P("  %-24s %5d/%d  %5.1f%%" % (col, f, N, 100 * f / N))

P("\n" + "=" * 60); P("AXIS B -- quality/integrity")
P("  orphan programs: %d" % one("SELECT COUNT(*) FROM grant_programs p LEFT JOIN organizations o ON p.organization_id=o.id WHERE o.id IS NULL"))
P("  orphan calls: %d" % one("SELECT COUNT(*) FROM grant_calls c LEFT JOIN grant_programs p ON c.program_id=p.id WHERE p.id IS NULL"))
P("  orphan results: %d" % one("SELECT COUNT(*) FROM grant_results r LEFT JOIN grant_calls c ON r.call_id=c.id WHERE c.id IS NULL"))
P("  orphan focus(org): %d" % one("SELECT COUNT(*) FROM foundation_focus_areas f LEFT JOIN organizations o ON f.organization_id=o.id WHERE o.id IS NULL"))
P("  orphan focus(cat): %d" % one("SELECT COUNT(*) FROM foundation_focus_areas f LEFT JOIN foundation_categories fc ON f.category_id=fc.id WHERE fc.id IS NULL"))
dn = q("SELECT name,COUNT(*) n FROM organizations GROUP BY name HAVING n>1 ORDER BY n DESC")
P("  exact dup names: %d groups / %d extra rows" % (len(dn), sum(r['n'] - 1 for r in dn)))
for r in dn[:8]: P("      %dx  %s" % (r['n'], r['name']))
def norm(s):
    s = re.sub(r'(公益|一般)?(財団|社団)法人|特定非営利活動法人|株式会社', '', s or '')
    return re.sub(r'[\s　]+', '', s)
nm = Counter(norm(r['name']) for r in q("SELECT id,name FROM organizations"))
ndup = [(k, v) for k, v in nm.items() if v > 1 and k]
P("  normalized-name collisions: %d keys / %d extra rows" % (len(ndup), sum(v - 1 for _, v in ndup)))
for k, v in sorted(ndup, key=lambda x: -x[1])[:8]: P("      %dx  %s" % (v, k))
P("  legal_form NULL: %d ; subtype NULL: %d" % (one("SELECT COUNT(*) FROM organizations WHERE legal_form IS NULL OR legal_form=''"), one("SELECT COUNT(*) FROM organizations WHERE foundation_subtype IS NULL OR foundation_subtype=''")))
P("  url not http*: %d" % one("SELECT COUNT(*) FROM organizations WHERE url IS NOT NULL AND url!='' AND url NOT LIKE 'http%'"))
P("  annual_amount<=0: %d ; >1e12: %d" % (one("SELECT COUNT(*) FROM organizations WHERE annual_grant_amount<=0"), one("SELECT COUNT(*) FROM organizations WHERE annual_grant_amount>1000000000000")))
jd = q("SELECT jfc_rank,COUNT(*) n FROM organizations WHERE jfc_rank IS NOT NULL GROUP BY jfc_rank HAVING n>1")
P("  duplicate jfc_rank values: %d" % len(jd))
P("  suspicious/placeholder names: %d" % one("SELECT COUNT(*) FROM organizations WHERE name LIKE '%test%' OR name LIKE '%サンプル%' OR name LIKE '%unknown%' OR name LIKE '%未定%' OR LENGTH(name)<3"))

P("\n" + "=" * 60); P("AXIS C -- coverage")
P("  orgs w/ >=1 program: %d/%d" % (one("SELECT COUNT(DISTINCT organization_id) FROM grant_programs"), N))
P("  orgs w/ focus tag: %d/%d" % (one("SELECT COUNT(DISTINCT organization_id) FROM foundation_focus_areas"), N))
P("  programs w/ >=1 call: %d/%d" % (one("SELECT COUNT(DISTINCT program_id) FROM grant_calls"), one("SELECT COUNT(*) FROM grant_programs")))
P("  calls w/ >=1 result: %d/%d" % (one("SELECT COUNT(DISTINCT call_id) FROM grant_results"), one("SELECT COUNT(*) FROM grant_calls")))
P("  foundations w/ awardees:")
for r in q("SELECT o.name, COUNT(*) n FROM grant_results r JOIN grant_calls gc ON r.call_id=gc.id JOIN grant_programs p ON gc.program_id=p.id JOIN organizations o ON p.organization_id=o.id GROUP BY o.id ORDER BY n DESC")[:20]:
    P("      %5d  %s" % (r['n'], r['name']))

P("\n" + "=" * 60); P("AXIS D -- freshness")
P("  calls fiscal_year:")
for r in q("SELECT fiscal_year,COUNT(*) n FROM grant_calls GROUP BY fiscal_year ORDER BY fiscal_year DESC"):
    P("      FY%s: %d" % (r['fiscal_year'], r['n']))
P("  calls status:")
for r in q("SELECT status,COUNT(*) n FROM grant_calls GROUP BY status ORDER BY n DESC"):
    P("      %-12s %d" % (r['status'], r['n']))
P("  calls w/ deadline: %d ; latest: %s" % (one("SELECT COUNT(*) FROM grant_calls WHERE application_deadline IS NOT NULL AND application_deadline!=''"), one("SELECT MAX(application_deadline) FROM grant_calls")))
P("  results fiscal_year range: %s .. %s" % (one("SELECT MIN(fiscal_year) FROM grant_results"), one("SELECT MAX(fiscal_year) FROM grant_results")))
json.dump({"lines": out}, open("research_results/audit_baseline_latest.json", "w"), ensure_ascii=False, indent=1)
