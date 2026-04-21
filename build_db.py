#!/usr/bin/env python3
"""
Corporate Foundation Research Grants Database Builder

Extracts corporate foundation research grants from the main Grant DB
and builds a focused analysis database.
"""

import sqlite3
import json
import re
from pathlib import Path

SOURCE_DB = Path.home() / "projects/apps/grant-db/grant_db.sqlite"
TARGET_DB = Path(__file__).parent / "corporate_research_grants.sqlite"

# --- Corporate foundation identification ---
# Keywords that indicate a corporate origin in foundation names
CORPORATE_KEYWORDS = [
    # Automotive
    "トヨタ", "マツダ", "日産", "ホンダ", "スズキ", "日野自動車", "ヤマハ",
    # Electronics / Tech
    "ソニー", "パナソニック", "日立", "東芝", "NEC", "富士通", "キヤノン",
    "村田", "ローム", "ホソカワ", "電気通信", "カインズデジタル",
    # Pharma / Medical
    "武田", "アステラス", "小野薬品", "テルモ", "加藤記念バイオ",
    "先進医薬", "長瀬科学", "金原", "臨床研究奨励",
    # Finance / Insurance
    "三菱", "住友", "野村", "大和証券", "みずほ", "りそな", "日本生命",
    "ニッセイ", "太陽生命", "大同生命", "SOMPO", "ＳＯＭＰＯ", "損保ジャパン",
    "セコム", "全国銀行", "日本証券", "明治安田",
    # Trading / Heavy Industry
    "丸紅", "三井", "住友生命", "鉄鋼環境",
    # Food / Consumer
    "ニッポンハム", "サントリー", "アサヒ", "ロッテ", "味の素",
    "花王", "小林製薬", "大塚商会", "浦上食品",
    # Transport / Infrastructure
    "JKA", "ＪＫＡ", "ＪＲ西日本", "NEXCO", "ＮＥＸＣＯ", "ＳＧＨ",
    "車両競技", "日工組",
    # Construction / Materials
    "旭硝子", "AGC", "鹿島", "建設物価", "市村清新技術",
    "東洋アルミ", "軽金属",
    # Other corporate
    "稲盛", "PwC", "デロイト", "Konno", "COSMO", "エフピコ",
    "ちゅうでん", "中部電力", "電通", "日本郵便", "ゆうちょ",
    "牧誠", "萩原学術", "齋藤茂昭", "SMBCグループ",
    "日本デザイン振興", "COSMOエコ", "ベネッセ",
    # Explicitly corporate-origin research foundations
    "大川情報通信", "三島海雲", "安藤スポーツ", "杉浦",
    "戸部眞紀", "tetote", "正力", "野口研究所",
    "高原環境", "福武", "木口", "洲崎", "芳心会",
    "白珪社", "愛恵", "庭野", "北野生涯",
    "日本フィランソロピック",  # corporate-linked grant intermediary
]

# Explicit exclusions (government, NPO umbrella, non-corporate)
EXCLUDE_NAMES = [
    "アーツカウンシル東京",
    "中央共同募金会",
    "京都市市民活動総合センター",
    "国立研究開発法人",
    "独立行政法人",
    "（独法）",
    "（独）",
    "日本財団",  # government-linked
    "笹川平和財団",  # government-linked
    "CANPAN",
    "助成財団センター",  # meta-org
    "公益推進協会",  # intermediary
    "パブリックリソース財団",  # intermediary
    "（社福）中央共同募金会",
    "高松青年会議所",
    "ETIC.",
    "お金をまわそう基金",
    "（一社）日本メイスン財団",
    "（公社）企業メセナ協議会",
    "（公財）セゾン文化財団",
    "生協総合研究所",
    "母子健康協会",
    "ラン・フォー・ピース",
    "福祉医療機構",
    "自然保護助成基金",
    "北海道NPOファンド",
    "大阪NPOセンター",
    "日本NPOセンター",
    "神戸市社会福祉協議会",
]


def is_corporate_foundation(name: str) -> bool:
    """Determine if an organization name indicates a corporate foundation."""
    # Check exclusions first
    for excl in EXCLUDE_NAMES:
        if excl in name:
            return False
    # Check corporate keywords
    for kw in CORPORATE_KEYWORDS:
        if kw in name:
            return True
    return False


def build_database():
    src = sqlite3.connect(str(SOURCE_DB))
    src.row_factory = sqlite3.Row

    # Step 1: Identify corporate foundation org IDs
    orgs = src.execute(
        "SELECT id, name FROM organizations WHERE type = 'foundation'"
    ).fetchall()

    corporate_org_ids = set()
    corporate_org_names = {}
    for org in orgs:
        if is_corporate_foundation(org["name"]):
            corporate_org_ids.add(org["id"])
            corporate_org_names[org["id"]] = org["name"]

    print(f"Identified {len(corporate_org_ids)} corporate foundations")

    # Step 2: Find research-related programs from these orgs
    placeholders = ",".join(["?"] * len(corporate_org_ids))
    programs = src.execute(
        f"""
        SELECT * FROM grant_programs
        WHERE organization_id IN ({placeholders})
          AND (category = 'research'
               OR name LIKE '%研究%'
               OR name LIKE '%学術%'
               OR name LIKE '%科学%'
               OR name LIKE '%調査%')
        """,
        list(corporate_org_ids),
    ).fetchall()

    program_ids = set(p["id"] for p in programs)
    # Also get org IDs that actually have research programs
    active_org_ids = set(p["organization_id"] for p in programs)

    print(f"Found {len(programs)} research-related programs from {len(active_org_ids)} foundations")

    # Step 3: Get all grant calls for these programs
    if not program_ids:
        print("No programs found. Exiting.")
        return

    p_placeholders = ",".join(["?"] * len(program_ids))
    calls = src.execute(
        f"SELECT * FROM grant_calls WHERE program_id IN ({p_placeholders})",
        list(program_ids),
    ).fetchall()
    call_ids = set(c["id"] for c in calls)
    print(f"Found {len(calls)} grant calls")

    # Step 4: Get related data
    c_placeholders = ",".join(["?"] * len(call_ids)) if call_ids else "''"

    eligibility = []
    documents = []
    doc_sections = []
    eval_criteria = []
    budget_cats = []
    source_pdfs = []

    if call_ids:
        cid_list = list(call_ids)
        eligibility = src.execute(
            f"SELECT * FROM eligibility_criteria WHERE call_id IN ({c_placeholders})",
            cid_list,
        ).fetchall()

        documents = src.execute(
            f"SELECT * FROM required_documents WHERE call_id IN ({c_placeholders})",
            cid_list,
        ).fetchall()

        eval_criteria = src.execute(
            f"SELECT * FROM evaluation_criteria WHERE call_id IN ({c_placeholders})",
            cid_list,
        ).fetchall()

        budget_cats = src.execute(
            f"SELECT * FROM budget_categories WHERE call_id IN ({c_placeholders})",
            cid_list,
        ).fetchall()

        source_pdfs = src.execute(
            f"SELECT * FROM source_pdfs WHERE call_id IN ({c_placeholders})",
            cid_list,
        ).fetchall()

        # Document sections need document IDs
        doc_ids = [d["id"] for d in documents]
        if doc_ids:
            d_placeholders = ",".join(["?"] * len(doc_ids))
            doc_sections = src.execute(
                f"SELECT * FROM document_sections WHERE document_id IN ({d_placeholders})",
                doc_ids,
            ).fetchall()

    print(f"Related data: {len(eligibility)} eligibility, {len(documents)} docs, "
          f"{len(doc_sections)} sections, {len(eval_criteria)} eval criteria, "
          f"{len(budget_cats)} budget categories, {len(source_pdfs)} PDFs")

    # Step 5: Get organizations (only active ones)
    o_placeholders = ",".join(["?"] * len(active_org_ids))
    org_rows = src.execute(
        f"SELECT * FROM organizations WHERE id IN ({o_placeholders})",
        list(active_org_ids),
    ).fetchall()

    src.close()

    # Step 6: Build target database
    if TARGET_DB.exists():
        TARGET_DB.unlink()

    tgt = sqlite3.connect(str(TARGET_DB))
    tgt.executescript("""
        -- Core tables
        CREATE TABLE organizations (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            name_en TEXT,
            type TEXT NOT NULL DEFAULT 'foundation',
            corporate_parent TEXT,  -- inferred parent company
            prefecture TEXT,
            municipality TEXT,
            url TEXT,
            description TEXT,
            contact_phone TEXT,
            contact_email TEXT,
            contact_address TEXT,
            metadata TEXT,
            created_at TEXT,
            updated_at TEXT
        );

        CREATE TABLE grant_programs (
            id TEXT PRIMARY KEY,
            organization_id TEXT NOT NULL REFERENCES organizations(id),
            name TEXT NOT NULL,
            description TEXT,
            purpose TEXT,
            category TEXT,
            subcategories TEXT,
            total_budget INTEGER,
            is_recurring INTEGER DEFAULT 0,
            source_url TEXT,
            metadata TEXT,
            created_at TEXT,
            updated_at TEXT
        );

        CREATE TABLE grant_calls (
            id TEXT PRIMARY KEY,
            program_id TEXT NOT NULL REFERENCES grant_programs(id),
            fiscal_year INTEGER,
            round_number INTEGER DEFAULT 1,
            title TEXT NOT NULL,
            status TEXT DEFAULT 'unknown',
            application_start TEXT,
            application_deadline TEXT,
            review_period_start TEXT,
            review_period_end TEXT,
            grant_period_start TEXT,
            grant_period_end TEXT,
            grant_amount_min INTEGER,
            grant_amount_max INTEGER,
            grant_rate REAL,
            grant_rate_description TEXT,
            expected_awards INTEGER,
            actual_awards INTEGER,
            source_url TEXT,
            guideline_url TEXT,
            jgrants_id TEXT,
            jgrants_acceptance_id TEXT,
            summary TEXT,
            target_area TEXT,
            target_industries TEXT,
            keywords TEXT,
            metadata TEXT,
            created_at TEXT,
            updated_at TEXT
        );

        CREATE TABLE eligibility_criteria (
            id TEXT PRIMARY KEY,
            call_id TEXT NOT NULL REFERENCES grant_calls(id),
            criterion_type TEXT,
            description TEXT,
            is_required INTEGER DEFAULT 1,
            metadata TEXT,
            created_at TEXT
        );

        CREATE TABLE required_documents (
            id TEXT PRIMARY KEY,
            call_id TEXT NOT NULL REFERENCES grant_calls(id),
            name TEXT,
            description TEXT,
            format TEXT,
            page_limit INTEGER,
            is_required INTEGER DEFAULT 1,
            template_url TEXT,
            metadata TEXT,
            created_at TEXT
        );

        CREATE TABLE document_sections (
            id TEXT PRIMARY KEY,
            document_id TEXT NOT NULL REFERENCES required_documents(id),
            section_number INTEGER,
            title TEXT,
            description TEXT,
            char_limit INTEGER,
            evaluation_weight REAL,
            writing_tips TEXT,
            metadata TEXT,
            created_at TEXT
        );

        CREATE TABLE evaluation_criteria (
            id TEXT PRIMARY KEY,
            call_id TEXT NOT NULL REFERENCES grant_calls(id),
            name TEXT,
            description TEXT,
            weight REAL,
            max_score INTEGER,
            metadata TEXT,
            created_at TEXT
        );

        CREATE TABLE budget_categories (
            id TEXT PRIMARY KEY,
            call_id TEXT NOT NULL REFERENCES grant_calls(id),
            category_name TEXT,
            description TEXT,
            is_eligible INTEGER DEFAULT 1,
            max_amount INTEGER,
            max_rate REAL,
            notes TEXT,
            metadata TEXT,
            created_at TEXT
        );

        CREATE TABLE source_pdfs (
            id TEXT PRIMARY KEY,
            call_id TEXT NOT NULL REFERENCES grant_calls(id),
            filename TEXT,
            pdf_type TEXT,
            url TEXT,
            local_path TEXT,
            page_count INTEGER,
            metadata TEXT,
            created_at TEXT
        );

        -- Analysis views
        CREATE VIEW v_grants_overview AS
        SELECT
            o.name AS foundation_name,
            o.corporate_parent,
            gp.name AS program_name,
            gp.category,
            gc.fiscal_year,
            gc.title AS call_title,
            gc.grant_amount_min,
            gc.grant_amount_max,
            gc.expected_awards,
            gc.actual_awards,
            gc.application_deadline,
            gc.status,
            gc.keywords
        FROM organizations o
        JOIN grant_programs gp ON gp.organization_id = o.id
        JOIN grant_calls gc ON gc.program_id = gp.id
        ORDER BY gc.application_deadline DESC;

        CREATE VIEW v_foundation_summary AS
        SELECT
            o.name AS foundation_name,
            o.corporate_parent,
            COUNT(DISTINCT gp.id) AS program_count,
            COUNT(gc.id) AS call_count,
            MAX(gc.grant_amount_max) AS max_grant_amount,
            MIN(gc.grant_amount_min) AS min_grant_amount,
            GROUP_CONCAT(DISTINCT gp.category) AS categories,
            MAX(gc.application_deadline) AS latest_deadline
        FROM organizations o
        JOIN grant_programs gp ON gp.organization_id = o.id
        LEFT JOIN grant_calls gc ON gc.program_id = gp.id
        GROUP BY o.id
        ORDER BY call_count DESC;

        CREATE VIEW v_amount_ranking AS
        SELECT
            o.name AS foundation_name,
            gp.name AS program_name,
            gc.grant_amount_max,
            gc.grant_amount_min,
            gc.fiscal_year,
            gc.application_deadline,
            gc.status
        FROM organizations o
        JOIN grant_programs gp ON gp.organization_id = o.id
        JOIN grant_calls gc ON gc.program_id = gp.id
        WHERE gc.grant_amount_max IS NOT NULL
        ORDER BY gc.grant_amount_max DESC;

        -- Indexes
        CREATE INDEX idx_program_org ON grant_programs(organization_id);
        CREATE INDEX idx_program_cat ON grant_programs(category);
        CREATE INDEX idx_call_program ON grant_calls(program_id);
        CREATE INDEX idx_call_deadline ON grant_calls(application_deadline);
        CREATE INDEX idx_call_status ON grant_calls(status);
        CREATE INDEX idx_call_fy ON grant_calls(fiscal_year);
        CREATE INDEX idx_elig_call ON eligibility_criteria(call_id);
        CREATE INDEX idx_doc_call ON required_documents(call_id);
        CREATE INDEX idx_docsec_doc ON document_sections(document_id);
        CREATE INDEX idx_eval_call ON evaluation_criteria(call_id);
        CREATE INDEX idx_budget_call ON budget_categories(call_id);
        CREATE INDEX idx_pdf_call ON source_pdfs(call_id);
    """)

    # Helper: dict from Row
    def row_to_dict(row):
        return dict(row)

    # Infer corporate parent from foundation name
    PARENT_MAP = {
        "トヨタ": "トヨタ自動車", "マツダ": "マツダ", "日産": "日産自動車",
        "ソニー": "ソニーグループ", "パナソニック": "パナソニック",
        "日立": "日立製作所", "東芝": "東芝", "村田": "村田製作所",
        "ローム": "ローム", "武田": "武田薬品工業", "アステラス": "アステラス製薬",
        "小野薬品": "小野薬品工業", "テルモ": "テルモ",
        "三菱": "三菱グループ", "住友": "住友グループ", "野村": "野村グループ",
        "大和証券": "大和証券グループ", "みずほ": "みずほFG",
        "りそな": "りそなグループ", "日本生命": "日本生命保険",
        "ニッセイ": "日本生命保険", "太陽生命": "太陽生命保険",
        "大同生命": "大同生命保険", "SOMPO": "SOMPOホールディングス",
        "ＳＯＭＰＯ": "SOMPOホールディングス", "損保ジャパン": "SOMPOホールディングス",
        "セコム": "セコム", "明治安田": "明治安田生命",
        "丸紅": "丸紅", "ニッポンハム": "日本ハム",
        "サントリー": "サントリーHD", "アサヒ": "アサヒグループHD",
        "ロッテ": "ロッテ", "花王": "花王", "小林製薬": "小林製薬",
        "大塚商会": "大塚商会", "旭硝子": "AGC", "AGC": "AGC",
        "稲盛": "京セラ（稲盛和夫）", "PwC": "PwC",
        "デロイト": "デロイトトーマツ", "SMBC": "SMBCグループ",
        "ベネッセ": "ベネッセHD", "電通": "電通グループ",
        "日本郵便": "日本郵政グループ", "ゆうちょ": "日本郵政グループ",
        "JKA": "JKA（競輪・オートレース）", "ＪＫＡ": "JKA（競輪・オートレース）",
        "ＪＲ西日本": "JR西日本", "NEXCO": "NEXCO東日本",
        "ＮＥＸＣＯ": "NEXCO東日本", "ＳＧＨ": "SGホールディングス",
        "エフピコ": "エフピコ", "ホソカワ": "ホソカワミクロン",
    }

    def infer_parent(name):
        for kw, parent in PARENT_MAP.items():
            if kw in name:
                return parent
        return None

    # Insert organizations
    for org in org_rows:
        d = row_to_dict(org)
        d["corporate_parent"] = infer_parent(d["name"])
        tgt.execute(
            """INSERT INTO organizations
               (id, name, name_en, type, corporate_parent, prefecture, municipality,
                url, description, contact_phone, contact_email, contact_address,
                metadata, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (d["id"], d["name"], d.get("name_en"), d["type"], d["corporate_parent"],
             d.get("prefecture"), d.get("municipality"), d.get("url"),
             d.get("description"), d.get("contact_phone"), d.get("contact_email"),
             d.get("contact_address"), d.get("metadata"),
             d.get("created_at"), d.get("updated_at")),
        )

    # Insert programs
    for prog in programs:
        d = row_to_dict(prog)
        tgt.execute(
            """INSERT INTO grant_programs
               (id, organization_id, name, description, purpose, category,
                subcategories, total_budget, is_recurring, source_url,
                metadata, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (d["id"], d["organization_id"], d["name"], d.get("description"),
             d.get("purpose"), d.get("category"), d.get("subcategories"),
             d.get("total_budget"), d.get("is_recurring"), d.get("source_url"),
             d.get("metadata"), d.get("created_at"), d.get("updated_at")),
        )

    # Insert calls
    for call in calls:
        d = row_to_dict(call)
        tgt.execute(
            """INSERT INTO grant_calls
               (id, program_id, fiscal_year, round_number, title, status,
                application_start, application_deadline,
                review_period_start, review_period_end,
                grant_period_start, grant_period_end,
                grant_amount_min, grant_amount_max,
                grant_rate, grant_rate_description,
                expected_awards, actual_awards,
                source_url, guideline_url,
                jgrants_id, jgrants_acceptance_id,
                summary, target_area, target_industries, keywords,
                metadata, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (d["id"], d["program_id"], d.get("fiscal_year"), d.get("round_number"),
             d["title"], d.get("status"),
             d.get("application_start"), d.get("application_deadline"),
             d.get("review_period_start"), d.get("review_period_end"),
             d.get("grant_period_start"), d.get("grant_period_end"),
             d.get("grant_amount_min"), d.get("grant_amount_max"),
             d.get("grant_rate"), d.get("grant_rate_description"),
             d.get("expected_awards"), d.get("actual_awards"),
             d.get("source_url"), d.get("guideline_url"),
             d.get("jgrants_id"), d.get("jgrants_acceptance_id"),
             d.get("summary"), d.get("target_area"), d.get("target_industries"),
             d.get("keywords"), d.get("metadata"),
             d.get("created_at"), d.get("updated_at")),
        )

    # Insert related tables
    def insert_rows(table, rows, columns):
        if not rows:
            return
        placeholders = ",".join(["?"] * len(columns))
        cols = ",".join(columns)
        for row in rows:
            d = row_to_dict(row)
            vals = [d.get(c) for c in columns]
            tgt.execute(f"INSERT INTO {table} ({cols}) VALUES ({placeholders})", vals)

    elig_cols = ["id", "call_id", "criterion_type", "description", "is_required", "metadata", "created_at"]
    insert_rows("eligibility_criteria", eligibility, elig_cols)

    doc_cols = ["id", "call_id", "name", "description", "format", "page_limit",
                "is_required", "template_url", "metadata", "created_at"]
    insert_rows("required_documents", documents, doc_cols)

    sec_cols = ["id", "document_id", "section_number", "title", "description",
                "char_limit", "evaluation_weight", "writing_tips", "metadata", "created_at"]
    insert_rows("document_sections", doc_sections, sec_cols)

    eval_cols = ["id", "call_id", "name", "description", "weight", "max_score", "metadata", "created_at"]
    insert_rows("evaluation_criteria", eval_criteria, eval_cols)

    budget_cols = ["id", "call_id", "category_name", "description", "is_eligible",
                   "max_amount", "max_rate", "notes", "metadata", "created_at"]
    insert_rows("budget_categories", budget_cats, budget_cols)

    pdf_cols = ["id", "call_id", "filename", "pdf_type", "url", "local_path",
                "page_count", "metadata", "created_at"]
    insert_rows("source_pdfs", source_pdfs, pdf_cols)

    tgt.commit()

    # Print summary
    print("\n=== Database Built ===")
    for table in ["organizations", "grant_programs", "grant_calls",
                   "eligibility_criteria", "required_documents", "document_sections",
                   "evaluation_criteria", "budget_categories", "source_pdfs"]:
        count = tgt.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count} rows")

    db_size = TARGET_DB.stat().st_size
    print(f"\nDatabase size: {db_size / 1024:.0f} KB")
    print(f"Location: {TARGET_DB}")

    tgt.close()


if __name__ == "__main__":
    build_database()
