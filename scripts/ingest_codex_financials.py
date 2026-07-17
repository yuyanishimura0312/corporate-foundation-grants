#!/usr/bin/env python3
"""Phase 3 ingest — apply fable-VERIFIED codex financial data into organizations.
   Run ONLY after fable verification of research_results/codex_financials_staging.json.
   Sanity gates + provenance (source_url in metadata). Idempotent. No fabrication:
   only ingest values that (a) are non-null, (b) pass range sanity, (c) carry a source_url.
   Usage: python3 scripts/ingest_codex_financials.py [--apply]   (dry-run without --apply)
"""
import sqlite3, json, sys, re
DB = "corporate_research_grants.sqlite"
STAGING = "research_results/codex_financials_staging.json"
APPLY = "--apply" in sys.argv
NOW = "2026-07-15"

def sane_year(y): return isinstance(y, int) and 1800 <= y <= 2026
def sane_money(v): return isinstance(v, int) and 0 < v < 100_000_000_000_000  # <100兆 guard

# --- fable-derived gates (Phase 3 verification 2026-07-15) ---
BROKEN_URL = re.compile(r'%[0-9A-Fa-f]%')  # malformed percent-encoding (e.g. 台湾交流 est url 400)
def net_asset_skip(n): return ("正味財産" in (n or "")) and ("資産合計" not in (n or ""))  # 純資産 mislabeled as total
def guarantee_skip(n): return any(k in (n or "") for k in ("債務保証", "機関保証", "保証見返"))  # JEES 4.8兆 distortion
EXCLUDE_ASSET_NAME = ("むつ小川原", "日本国際教育支援協会")  # source unverifiable / JEES 4.8兆=債務保証見返込み (fable)
IMPLAUSIBLE_ASSETS = 1_000_000_000_000  # >1兆 for a grant foundation = likely guarantee/consolidated → review
# non-foundation entity: company / national agency の親実体資産が混入 (2026-07-18 検証: 大塚商会/日本新薬/AMED/JSPS/NICT/芸術文化振興会)
NON_FOUNDATION = re.compile(r'株式会社|（株）|\(株\)|国立研究開発法人|独立行政法人|（独|日本学術振興会|日本医療研究開発機構')
NON_FOUNDATION_ASSET_MIN = 100_000_000_000  # >1000億 かつ非財団名 = 親実体の資産混入

staging = json.load(open(STAGING))
c = sqlite3.connect(DB); c.row_factory = sqlite3.Row
plan = {"est_year": 0, "total_assets": 0, "annual_grant": 0, "rows_touched": 0,
        "skipped_no_source": 0, "skipped_sanity": 0, "skipped_broken_url": 0, "assets_flagged_review": 0}
review = []
for oid, rec in staging.items():
    cx = rec.get("codex")
    if not cx: continue
    row = c.execute("SELECT name,established_year,total_assets,annual_grant_amount,metadata FROM organizations WHERE id=?", (oid,)).fetchone()
    if not row: continue
    name = row["name"]; notes = cx.get("notes", "")
    # guard: never re-ingest a field previously NULLed for review (fable corrections persist)
    reviewed = set()
    try:
        _m = json.loads(row["metadata"]) if row["metadata"] else {}
        for rv in _m.get("financials_review", []):
            f = rv.get("field")
            reviewed.add("annual_grant_amount" if f in ("annual_grant", "annual_grant_amount") else f)
    except Exception:
        pass
    sets = {}; prov = {}
    ey = cx.get("established_year"); esrc = cx.get("established_source_url")
    if ey is not None and row["established_year"] is None and "established_year" not in reviewed:
        if not esrc:
            plan["skipped_no_source"] += 1
        elif BROKEN_URL.search(esrc):
            plan["skipped_broken_url"] += 1; review.append({"name": name, "field": "established_year", "reason": "broken source url", "url": esrc})
        elif sane_year(ey):
            sets["established_year"] = ey; prov["established_source_url"] = esrc; plan["est_year"] += 1
        else:
            plan["skipped_sanity"] += 1
    ta = cx.get("total_assets_jpy"); fsrc = cx.get("financial_source_url")
    if ta is not None and row["total_assets"] is None and "total_assets" not in reviewed:
        reason = None
        if not fsrc: reason = "no source"
        elif not sane_money(ta): reason = "sanity"
        elif net_asset_skip(notes): reason = "net_assets_not_total (正味財産)"
        elif guarantee_skip(notes): reason = "guarantee_liability_inflated (債務保証見返)"
        elif any(x in name for x in EXCLUDE_ASSET_NAME): reason = "source_unverified_or_guarantee_inflated"
        elif NON_FOUNDATION.search(name) and ta > NON_FOUNDATION_ASSET_MIN: reason = "non_foundation_entity_assets (company/national agency)"
        elif ta > IMPLAUSIBLE_ASSETS: reason = "implausibly_large_>1兆 (likely guarantee/consolidated)"
        if reason:
            plan["assets_flagged_review"] += 1
            review.append({"name": name, "field": "total_assets", "value": ta, "reason": reason})
        else:
            sets["total_assets"] = ta; prov["total_assets_source_url"] = fsrc
            prov["total_assets_fy"] = cx.get("total_assets_fiscal_year"); plan["total_assets"] += 1
    ga = cx.get("annual_grant_amount_jpy")
    if ga is not None and row["annual_grant_amount"] is None and "annual_grant_amount" not in reviewed:
        # apply exclusion gates on grant path too (fable: source-unverified/guarantee foundations)
        if (not fsrc) or (not sane_money(ga)) or guarantee_skip(notes) \
           or any(x in name for x in EXCLUDE_ASSET_NAME) or cx.get("confidence") == "low":
            plan.setdefault("grant_flagged_review", 0)
            if ga is not None and (guarantee_skip(notes) or any(x in name for x in EXCLUDE_ASSET_NAME) or cx.get("confidence") == "low"):
                plan["grant_flagged_review"] += 1
                review.append({"name": name, "field": "annual_grant", "value": ga, "reason": "source_unverified/guarantee/low_conf"})
            elif not fsrc:
                plan["skipped_no_source"] += 1
            else:
                plan["skipped_sanity"] += 1
            ga = None  # skip ingest
        if ga is not None:
            sets["annual_grant_amount"] = ga
            sets["annual_grant_year"] = str(cx.get("annual_grant_fiscal_year") or "") + "codex"
            prov["annual_grant_source_url"] = fsrc; prov["annual_grant_fy"] = cx.get("annual_grant_fiscal_year")
            plan["annual_grant"] += 1
        # else: already counted in the gate block above
    # codex grant_fields -> foundation_focus_areas (source='llm_classified', codex=LLM) + primary_field
    VALID_F = {"natural_science", "life_science", "engineering", "humanities_social", "arts_culture",
               "education", "welfare", "environment", "international", "regional", "interdisciplinary"}
    gf = [f for f in (cx.get("grant_fields") or []) if f in VALID_F][:5]  # cap top-5 (primary + 4)
    if gf and APPLY:
        # resolve dual-primary: demote any pre-existing primary before setting codex primary (fable #1)
        c.execute("UPDATE foundation_focus_areas SET is_primary=0 WHERE organization_id=? AND source!='llm_classified'", (oid,))
        ev = ((cx.get("grant_scope_text") or "")[:260])
        if cx.get("grant_field_source_url"):  # traceability (fable #3)
            ev = (ev + " | src:" + cx["grant_field_source_url"])[:400]
        for i, fld in enumerate(gf):
            c.execute("""INSERT OR IGNORE INTO foundation_focus_areas
                (organization_id,category_id,weight,is_primary,evidence,source)
                VALUES (?,?,?,?,?, 'llm_classified')""",
                (oid, fld, 1.0 if i == 0 else 0.4, 1 if i == 0 else 0, ev))
        c.execute("UPDATE organizations SET primary_field=?, primary_field_method='codex' WHERE id=?", (gf[0], oid))
        plan.setdefault("grant_fields_written", 0); plan["grant_fields_written"] += 1
    if sets:
        meta = {}
        try: meta = json.loads(row["metadata"]) if row["metadata"] else {}
        except Exception: meta = {"_raw_metadata": row["metadata"]}
        meta.setdefault("financials_provenance", {})
        meta["financials_provenance"].update({**prov, "source": "codex-web", "confidence": cx.get("confidence"), "collected_at": NOW})
        sets["metadata"] = json.dumps(meta, ensure_ascii=False)
        sets["updated_at"] = NOW
        plan["rows_touched"] += 1
        if APPLY:
            cols = ",".join("%s=?" % k for k in sets)
            c.execute("UPDATE organizations SET %s WHERE id=?" % cols, (*sets.values(), oid))
if APPLY:
    c.commit()
json.dump(review, open("research_results/phase3_ingest_review_excluded.json", "w"), ensure_ascii=False, indent=1)
print(("APPLIED" if APPLY else "DRY-RUN"), json.dumps(plan, ensure_ascii=False, indent=1))
print("flagged-for-review (NOT ingested, preserved):")
for r in review: print("  -", r["name"][:26], "|", r["field"], "|", r["reason"])
