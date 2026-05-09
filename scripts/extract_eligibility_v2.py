#!/usr/bin/env python3
"""Sprint 2.1: 拡張版応募資格抽出 — 46% → 75%カバー目標

データソース統合:
1. grant_calls.summary / target_area / target_industries / keywords
2. grant_programs.name / description / purpose / metadata (society_awards等)
3. organizations.description / metadata.umin.grant_sample (target_researcher / target_content)
4. cache/umin/{kikan_cd}.html — 「対象研究者」「対象内容」テーブルセル

抽出軸:
- age: 年齢制限（○歳以下/未満/まで、若手、ポスドク経過○年以内、博士号取得後○年）
- position: 職位（教授〜学生、特任、客員、名誉、研究員等）
- nationality: 国籍（日本国籍/不問/外国籍可/永住権/留学生）
- affiliation_type: 所属種別（大学/院/研究機関/企業/NPO/独立研究者）
- field: 分野（11カテゴリ＋59細分類自動マッピング）
- career_stage: undergraduate / phd_candidate / postdoc / pi_early / pi_mid / senior / unrestricted

career_stage は grant_programs.metadata に書き込み。
busy_timeout=300秒で並行処理に対応。
"""
from __future__ import annotations

import json
import re
import sqlite3
import sys
import uuid
from pathlib import Path

DB = Path("/Users/nishimura+/projects/apps/corporate-foundation-grants/corporate_research_grants.sqlite")
UMIN_CACHE = Path("/Users/nishimura+/projects/apps/corporate-foundation-grants/cache/umin")

BUSY_TIMEOUT_MS = 300_000  # 300秒


# ---------- HTML/Text helpers ----------
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def strip_html(s: str) -> str:
    if not s:
        return ""
    s = TAG_RE.sub(" ", s)
    s = s.replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    return WS_RE.sub(" ", s).strip()


def extract_umin_cell(html: str, header_jp: str) -> str | None:
    """Extract <td> following <th>HEADER</th> from cached UMIN HTML."""
    if not html:
        return None
    # Tolerate whitespace between th and td, and varied attributes
    pat = re.compile(
        rf"<th[^>]*>\s*{re.escape(header_jp)}\s*</th>\s*<td[^>]*>(.*?)</td>",
        re.DOTALL,
    )
    m = pat.search(html)
    if not m:
        return None
    return strip_html(m.group(1))


# ---------- Extractors ----------
def extract_age(text: str) -> str | None:
    parts = []
    for m in re.finditer(r"(\d{2})\s*歳\s*(以下|未満|まで|程度)", text):
        parts.append(f"{m.group(1)}歳{m.group(2)}")
    for m in re.finditer(r"満\s*(\d{2})\s*歳", text):
        parts.append(f"満{m.group(1)}歳")
    # ポスドク経過○年以内 / 博士号取得後○年以内 / 学位取得後○年
    for m in re.finditer(r"(?:博士号取得後|学位取得後|博士取得後|ポストドクター|ポスドク[^\s\d]{0,8})\s*(\d{1,2})\s*年(以内|未満)", text):
        parts.append(f"博士号取得後{m.group(1)}年{m.group(2)}")
    if not parts:
        if "若手" in text:
            parts.append("若手研究者")
        if "新進" in text:
            parts.append("新進研究者")
        if "中堅" in text:
            parts.append("中堅研究者")
    seen = set()
    out = []
    for p in parts:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return ",".join(out) if out else None


def extract_position(text: str) -> str | None:
    positions = []
    keywords = [
        "教授", "准教授", "講師", "助教", "助手",
        "ポストドクター", "ポスドク", "PI", "プリンシパル",
        "博士課程", "博士後期", "博士前期", "修士課程", "修士",
        "学部生", "大学院生", "院生",
        "研究員", "主任研究員", "上席研究員",
        "特任", "客員", "名誉教授", "招聘",
        "補助員", "技術職員", "URA",
        "医師", "歯科医師", "看護師",
    ]
    for kw in keywords:
        if kw in text:
            positions.append(kw)
    seen = set()
    out = []
    for p in positions:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return ",".join(out) if out else None


def extract_nationality(text: str) -> str | None:
    flags = []
    if "日本国籍" in text or "日本国民" in text:
        flags.append("日本国籍")
    if "国籍不問" in text or "国籍を問わない" in text or "国籍は問わない" in text:
        flags.append("国籍不問")
    if "在日外国人" in text or "外国籍" in text or ("外国人" in text and "歓迎" in text):
        flags.append("外国籍可")
    if "永住権" in text or "永住者" in text or "永住を許可" in text:
        flags.append("永住権保持者可")
    if "留学生" in text:
        flags.append("留学生対応あり")
    seen = set()
    out = []
    for f in flags:
        if f not in seen:
            seen.add(f)
            out.append(f)
    return ",".join(out) if out else None


def extract_affiliation(text: str) -> str | None:
    types = []
    # Order matters: longer match first to avoid double-counting
    mapping = [
        ("大学院", "大学院"),
        ("学部", "学部"),
        ("国立研究開発法人", "国立研究開発法人"),
        ("独立行政法人", "独立行政法人"),
        ("公立研究機関", "公立研究機関"),
        ("国公立", "国公立"),
        ("私立大学", "私立大学"),
        ("研究機関", "研究機関"),
        ("高等専門学校", "高等専門学校"),
        ("民間企業", "民間企業"),
        ("企業研究者", "企業研究者"),
        ("企業", "企業"),
        ("医療機関", "医療機関"),
        ("病院", "病院"),
        ("NPO", "NPO"),
        ("NGO", "NGO"),
        ("非営利", "非営利団体"),
        ("独立研究者", "独立研究者"),
        ("大学", "大学"),
    ]
    found = set()
    for kw, label in mapping:
        if kw in text and label not in found:
            found.add(label)
            types.append(label)
    return ",".join(types) if types else None


# Field keyword → (top11_category, subcategory_code)
FIELD_MAP: list[tuple[str, str, str]] = [
    # life sciences
    ("生命科学", "life_sciences", "ls_basic"),
    ("基礎医学", "life_sciences", "ls_basic"),
    ("医学", "life_sciences", "ls_med"),
    ("臨床医学", "life_sciences", "ls_med"),
    ("薬学", "life_sciences", "ls_pharm"),
    ("がん", "life_sciences", "ls_cancer"),
    ("癌", "life_sciences", "ls_cancer"),
    ("免疫", "life_sciences", "ls_immune"),
    ("ゲノム", "life_sciences", "ls_genome"),
    ("脳", "life_sciences", "ls_neuro"),
    ("神経", "life_sciences", "ls_neuro"),
    ("再生医療", "life_sciences", "ls_regen"),
    # natural sciences
    ("物理", "natural_sciences", "ns_phys"),
    ("化学", "natural_sciences", "ns_chem"),
    ("数学", "natural_sciences", "ns_math"),
    ("生物学", "natural_sciences", "ns_bio"),
    ("地球科学", "natural_sciences", "ns_geo"),
    ("地学", "natural_sciences", "ns_geo"),
    ("天文", "natural_sciences", "ns_astro"),
    ("宇宙", "natural_sciences", "ns_astro"),
    # engineering
    ("機械", "engineering", "eng_mech"),
    ("電気", "engineering", "eng_elec"),
    ("電子", "engineering", "eng_elec"),
    ("情報", "engineering", "eng_info"),
    ("コンピュータ", "engineering", "eng_info"),
    ("人工知能", "engineering", "eng_info"),
    ("AI", "engineering", "eng_info"),
    ("土木", "engineering", "eng_civil"),
    ("建築", "engineering", "eng_civil"),
    ("材料", "engineering", "eng_mater"),
    ("化学工学", "engineering", "eng_chem"),
    ("エネルギー", "engineering", "eng_energy"),
    ("航空", "engineering", "eng_aero"),
    # human/social
    ("社会学", "humanities_social", "hs_socio"),
    ("経済", "humanities_social", "hs_econ"),
    ("経営", "humanities_social", "hs_econ"),
    ("政治", "humanities_social", "hs_polit"),
    ("法律", "humanities_social", "hs_polit"),
    ("法学", "humanities_social", "hs_polit"),
    ("歴史", "humanities_social", "hs_hist"),
    ("哲学", "humanities_social", "hs_phil"),
    ("言語", "humanities_social", "hs_lang"),
    ("心理", "humanities_social", "hs_psych"),
    # education
    ("学校教育", "education", "ed_school"),
    ("初等教育", "education", "ed_school"),
    ("中等教育", "education", "ed_school"),
    ("高等教育", "education", "ed_higher"),
    ("奨学", "education", "ed_scholarship"),
    # welfare
    ("高齢者", "welfare", "wf_aging"),
    ("介護", "welfare", "wf_aging"),
    ("社会福祉", "welfare", "wf_social"),
    ("健康", "welfare", "wf_health"),
    ("保健", "welfare", "wf_health"),
    # international
    ("国際交流", "international", "intl_exchange"),
    ("国際協力", "international", "intl_exchange"),
    ("国際開発", "international", "intl_dev"),
    ("国際研究", "international", "intl_research"),
    # interdisciplinary
    ("学際", "interdisciplinary", "inter_emerging"),
    ("分野横断", "interdisciplinary", "inter_emerging"),
    # generic
    ("全分野", "all_fields", None),
    ("あらゆる分野", "all_fields", None),
]


def extract_field(text: str) -> tuple[str | None, list[str]]:
    """Return (top-level fields, subcategory codes)."""
    fields = set()
    subs = []
    for kw, top, sub in FIELD_MAP:
        if kw in text:
            fields.add(top)
            if sub and sub not in subs:
                subs.append(sub)
    return ",".join(sorted(fields)) if fields else None, subs


# career_stage rules
def infer_career_stage(text: str, age_value: str | None, position_value: str | None) -> str | None:
    t = text
    # Most specific first
    if "学部生" in t:
        return "undergraduate"
    if "博士課程" in t or "博士後期" in t or "博士前期" in t or "大学院生" in t or "院生" in t:
        return "phd_candidate"
    if "修士" in t and "学生" in t:
        return "phd_candidate"
    if "ポスドク" in t or "ポストドクター" in t or "博士号取得後" in t or "学位取得後" in t:
        return "postdoc"
    if age_value:
        # parse smallest age threshold
        ages = [int(m) for m in re.findall(r"(\d{2})", age_value)]
        if ages:
            a = min(ages)
            if a <= 35:
                return "pi_early"
            if a <= 45:
                return "pi_mid"
            if a <= 55:
                return "senior"
    if "若手" in t or "新進" in t or "early career" in t.lower():
        return "pi_early"
    if "中堅" in t:
        return "pi_mid"
    if "名誉教授" in t:
        return "senior"
    if "教授" in t and "准教授" not in t and "助教" not in t:
        return "senior"
    if position_value and ("准教授" in position_value or "講師" in position_value or "助教" in position_value):
        return "pi_early"
    # Scholarship / 奨学 → undergraduate-leaning
    if "奨学" in t or "スカラシップ" in t:
        if "学部" in t or "高校" in t or "高等学校" in t:
            return "undergraduate"
        return "phd_candidate"
    # Generic "研究者" with no age constraint → unrestricted (broad)
    if ("国籍不問" in t or "年齢制限なし" in t or "年齢を問わない" in t
            or "あらゆる" in t or "制限なし" in t or "問いません" in t):
        return "unrestricted"
    if "研究者" in t and "若手" not in t and "中堅" not in t and not age_value:
        return "unrestricted"
    return None


# ---------- Main ----------
def main():
    if not DB.exists():
        print(f"DB not found: {DB}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(DB, timeout=300)
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA journal_mode = WAL")
    cur = conn.cursor()

    # Build umin kikan_cd → cache file map (only ones present)
    umin_files = {}
    if UMIN_CACHE.is_dir():
        for p in UMIN_CACHE.glob("*.html"):
            umin_files[p.stem] = p

    cur.execute(
        """
        SELECT c.id AS call_id,
               c.summary, c.target_area, c.target_industries, c.keywords,
               p.id AS prog_id, p.name, p.description, p.purpose, p.metadata,
               o.description AS o_desc, o.metadata AS o_meta
          FROM grant_calls c
          JOIN grant_programs p ON c.program_id = p.id
          JOIN organizations o ON p.organization_id = o.id
        """
    )
    rows = cur.fetchall()
    print(f"Total calls: {len(rows)}")

    inserted = 0
    field_subcat_added = 0
    career_stage_set = 0
    calls_now_covered = set()

    # Stage seen criteria per call to avoid duplicates
    cur.execute("SELECT call_id, criterion_type FROM eligibility_criteria")
    existing = {(r[0], r[1]) for r in cur.fetchall()}
    for cid, _ in existing:
        calls_now_covered.add(cid)

    extractors = [
        ("age", extract_age),
        ("position", extract_position),
        ("nationality", extract_nationality),
        ("affiliation_type", extract_affiliation),
    ]

    # Pre-load all program metadata that we may update for subcategory enrichment
    # (for field subcategory addition we update grant_programs.subcategories)
    cur.execute("SELECT id, subcategories, metadata FROM grant_programs")
    prog_state: dict[str, tuple[str | None, str | None]] = {
        r[0]: (r[1], r[2]) for r in cur.fetchall()
    }

    for (cid, summary, t_area, t_ind, kw,
         prog_id, p_name, p_desc, p_purpose, p_meta,
         o_desc, o_meta) in rows:
        # Compose a unified text blob
        text_parts = [summary, t_area, t_ind, kw, p_name, p_desc, p_purpose, o_desc]

        # Pull umin grant_sample target_researcher / target_content from org metadata
        umin_kikan_cd = None
        if o_meta:
            try:
                om = json.loads(o_meta)
                gs = (om.get("umin") or {}).get("grant_sample") or {}
                for k in ("target_researcher", "target_content"):
                    v = gs.get(k)
                    if v:
                        text_parts.append(v)
                umin_kikan_cd = (om.get("umin") or {}).get("kikan_cd")
            except (ValueError, TypeError):
                pass

        # Pull program metadata target field hints
        if p_meta:
            try:
                pm = json.loads(p_meta)
                tgt = pm.get("target")
                if tgt:
                    text_parts.append(tgt)
                fld = pm.get("field")
                if fld:
                    text_parts.append(fld)
                sf = pm.get("subfield")
                if sf:
                    text_parts.append(sf)
            except (ValueError, TypeError):
                pm = {}
        else:
            pm = {}

        # Pull umin HTML cache (fallback for richer text)
        if umin_kikan_cd and umin_kikan_cd in umin_files:
            try:
                html = umin_files[umin_kikan_cd].read_text(encoding="utf-8", errors="ignore")
                for header in ("対象研究者", "対象内容", "応募要件", "応募資格", "申請資格"):
                    cell = extract_umin_cell(html, header)
                    if cell:
                        text_parts.append(cell)
            except OSError:
                pass

        text = " ".join([s for s in text_parts if s])
        if not text:
            continue

        had_any = False
        for crit_type, fn in extractors:
            value = fn(text)
            if not value:
                continue
            if (cid, crit_type) in existing:
                continue
            cur.execute(
                """INSERT INTO eligibility_criteria
                   (id, call_id, criterion_type, description, is_required, created_at)
                   VALUES (?, ?, ?, ?, 1, datetime('now','localtime'))""",
                (str(uuid.uuid4()), cid, crit_type, value),
            )
            existing.add((cid, crit_type))
            inserted += 1
            had_any = True

        # Field + subcategories: combined extractor
        field_top, sub_codes = extract_field(text)
        if field_top and (cid, "field") not in existing:
            cur.execute(
                """INSERT INTO eligibility_criteria
                   (id, call_id, criterion_type, description, is_required, created_at)
                   VALUES (?, ?, ?, ?, 1, datetime('now','localtime'))""",
                (str(uuid.uuid4()), cid, "field", field_top),
            )
            existing.add((cid, "field"))
            inserted += 1
            had_any = True

        # Add subcategory codes to grant_programs.subcategories if missing
        if sub_codes and prog_id in prog_state:
            cur_subs, cur_meta = prog_state[prog_id]
            current = set()
            if cur_subs and cur_subs not in ("[]",):
                for s in cur_subs.split(","):
                    s = s.strip()
                    if s:
                        current.add(s)
            new_sub = [c for c in sub_codes if c not in current]
            if new_sub:
                merged = sorted(current | set(sub_codes))
                cur.execute(
                    "UPDATE grant_programs SET subcategories = ?, updated_at = datetime('now','localtime') WHERE id = ?",
                    (",".join(merged), prog_id),
                )
                prog_state[prog_id] = (",".join(merged), cur_meta)
                field_subcat_added += 1

        # career_stage in grant_programs.metadata
        # (use the same combined text)
        # Pull current age / position values from existing or freshly computed
        age_v = extract_age(text)
        pos_v = extract_position(text)
        cs = infer_career_stage(text, age_v, pos_v)
        if cs:
            cur_meta_str = prog_state[prog_id][1] if prog_id in prog_state else None
            try:
                meta_obj = json.loads(cur_meta_str) if cur_meta_str else {}
            except (ValueError, TypeError):
                meta_obj = {}
            if meta_obj.get("career_stage") != cs:
                meta_obj["career_stage"] = cs
                new_meta_str = json.dumps(meta_obj, ensure_ascii=False)
                cur.execute(
                    "UPDATE grant_programs SET metadata = ?, updated_at = datetime('now','localtime') WHERE id = ?",
                    (new_meta_str, prog_id),
                )
                prog_state[prog_id] = (prog_state[prog_id][0], new_meta_str)
                career_stage_set += 1

        if had_any:
            calls_now_covered.add(cid)

    conn.commit()

    # Final coverage
    cur.execute("SELECT COUNT(*) FROM grant_calls")
    total_calls = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT call_id) FROM eligibility_criteria")
    cov = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM eligibility_criteria")
    crit_total = cur.fetchone()[0]
    cur.execute("SELECT criterion_type, COUNT(*) FROM eligibility_criteria GROUP BY criterion_type ORDER BY 2 DESC")
    by_type = cur.fetchall()
    cur.execute("SELECT COUNT(*) FROM grant_programs")
    total_prog = cur.fetchone()[0]
    cur.execute(
        """SELECT COUNT(*) FROM grant_programs
           WHERE subcategories IS NOT NULL AND subcategories != '' AND subcategories != '[]'"""
    )
    subcat_cov = cur.fetchone()[0]
    cur.execute(
        """SELECT COUNT(*) FROM grant_programs
           WHERE metadata IS NOT NULL AND metadata LIKE '%"career_stage"%'"""
    )
    cs_total = cur.fetchone()[0]

    print("\n=== Results ===")
    print(f"New criteria inserted: {inserted}")
    print(f"Programs subcategory enriched: {field_subcat_added}")
    print(f"Programs career_stage set/updated this run: {career_stage_set}")
    print(f"Eligibility coverage: {cov}/{total_calls} ({cov/total_calls*100:.1f}%)")
    print(f"Total criteria records: {crit_total}")
    print(f"Subcategory coverage: {subcat_cov}/{total_prog} ({subcat_cov/total_prog*100:.1f}%)")
    print(f"Career_stage rows total: {cs_total}/{total_prog} ({cs_total/total_prog*100:.1f}%)")
    print("\nCriterion type distribution:")
    for t, c in by_type:
        print(f"  {t}: {c}")

    conn.close()


if __name__ == "__main__":
    main()
