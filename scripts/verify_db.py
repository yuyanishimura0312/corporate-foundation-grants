"""Quality verification framework for the Corporate Foundation Grants (CFG) DB.

Usage:
    python3 scripts/verify_db.py

Outputs:
    VERIFICATION_REPORT.md  (in project root)

Verifies six categories: integrity, duplicates, data quality, awardee quality,
coverage, cross-DB checks. Each item resolves to PASS / WARN / FAIL with details.
"""
from __future__ import annotations

import os
import re
import sqlite3
import sys
import time
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "corporate_research_grants.sqlite"
GRANT_DB_PATH = Path.home() / "projects/apps/grant-db/grant_db.sqlite"
REPORT_PATH = PROJECT_ROOT / "VERIFICATION_REPORT.md"

# ---------------------------------------------------------------------------
# Expected values (baselines)
# ---------------------------------------------------------------------------
EXPECTED_MIN_ROWS = {
    "organizations": 1900,
    "grant_programs": 250,
    "grant_calls": 280,
    "grant_results": 2000,
}
JFC_TOP100_EXPECTED_RATE = 0.65  # 65% baseline
EXPECTED_PREF_COUNT_MIN = 40  # 47都道府県カバレッジ目標

# ---------------------------------------------------------------------------
# Result holder
# ---------------------------------------------------------------------------
class Result:
    __slots__ = ("section", "name", "status", "detail")

    def __init__(self, section: str, name: str, status: str, detail: str = ""):
        assert status in ("PASS", "WARN", "FAIL", "INFO")
        self.section = section
        self.name = name
        self.status = status
        self.detail = detail


RESULTS: list[Result] = []


def add(section: str, name: str, status: str, detail: str = "") -> None:
    RESULTS.append(Result(section, name, status, detail))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_NORM_RE_PARENS = re.compile(r"[（\(].*?[）\)]")
_NORM_RE_PUNCT = re.compile(r"[\s・\-_,，、。\.／/]+")
LEGAL_FORM_TOKENS = (
    "公益財団法人", "一般財団法人", "公益社団法人", "一般社団法人",
    "特定非営利活動法人", "NPO法人", "株式会社", "有限会社", "学校法人",
    "独立行政法人", "国立大学法人", "社会福祉法人", "宗教法人",
)


def normalize_name(name: str | None) -> str:
    """Aggressive normalization for duplicate detection."""
    if not name:
        return ""
    s = unicodedata.normalize("NFKC", name)
    # strip parenthetical annotations e.g. (旧:...)
    s = _NORM_RE_PARENS.sub("", s)
    # strip legal form tokens
    for tok in LEGAL_FORM_TOKENS:
        s = s.replace(tok, "")
    # collapse punctuation/whitespace
    s = _NORM_RE_PUNCT.sub("", s)
    return s.strip().lower()


def normalize_url(url: str | None) -> str:
    if not url:
        return ""
    try:
        p = urlparse(url.strip().lower())
    except Exception:
        return url.strip().lower()
    host = (p.netloc or p.path).lstrip("www.")
    return host.split("/")[0]


PHONE_RE = re.compile(r"^[0-9０-９\-－()()\s+]+$")
EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")
PREFECTURES = {
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
}
ALLOWED_SUBTYPES = {"corporate", "individual", "group", "academic", "govt", "intl", "ngo", "other"}


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Section 1: DB integrity
# ---------------------------------------------------------------------------
def check_integrity(conn: sqlite3.Connection) -> None:
    section = "1. DB整合性"
    cur = conn.cursor()

    # PRAGMA integrity_check
    rows = cur.execute("PRAGMA integrity_check").fetchall()
    msg = ", ".join(r[0] for r in rows)
    if len(rows) == 1 and rows[0][0] == "ok":
        add(section, "PRAGMA integrity_check", "PASS", "ok")
    else:
        add(section, "PRAGMA integrity_check", "FAIL", msg[:500])

    # PRAGMA foreign_key_check
    fk_rows = cur.execute("PRAGMA foreign_key_check").fetchall()
    if not fk_rows:
        add(section, "PRAGMA foreign_key_check", "PASS", "違反なし")
    else:
        sample = "; ".join(f"{r[0]}.{r[3]}={r[1]}" for r in fk_rows[:5])
        add(section, "PRAGMA foreign_key_check", "FAIL",
            f"{len(fk_rows)}件の外部キー違反: {sample}")

    # Row counts
    for tbl, expected in EXPECTED_MIN_ROWS.items():
        n = cur.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        if n >= expected:
            add(section, f"row_count {tbl}", "PASS", f"{n}件 (期待 ≥{expected})")
        elif n >= expected * 0.9:
            add(section, f"row_count {tbl}", "WARN",
                f"{n}件 (期待 ≥{expected}、90%以上)")
        else:
            add(section, f"row_count {tbl}", "FAIL",
                f"{n}件 (期待 ≥{expected})")

    # Required column NULL rates
    null_checks = [
        ("organizations", "name", 0.0),
        ("organizations", "type", 0.0),
        ("organizations", "foundation_subtype", 0.05),
        ("organizations", "url", 0.50),
        ("organizations", "prefecture", 0.40),
        ("grant_results", "awardee_name", 0.01),
        ("grant_results", "project_title", 0.05),
        ("grant_results", "fiscal_year", 0.0),
    ]
    for tbl, col, threshold in null_checks:
        total = cur.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        if total == 0:
            add(section, f"NULL率 {tbl}.{col}", "WARN", "テーブルが空")
            continue
        n_null = cur.execute(
            f"SELECT COUNT(*) FROM {tbl} WHERE {col} IS NULL OR {col} = ''"
        ).fetchone()[0]
        rate = n_null / total
        detail = f"{n_null}/{total} = {rate:.1%} (許容 ≤{threshold:.0%})"
        if rate <= threshold:
            add(section, f"NULL率 {tbl}.{col}", "PASS", detail)
        elif rate <= threshold + 0.10:
            add(section, f"NULL率 {tbl}.{col}", "WARN", detail)
        else:
            add(section, f"NULL率 {tbl}.{col}", "FAIL", detail)


# ---------------------------------------------------------------------------
# Section 2: Duplicate detection
# ---------------------------------------------------------------------------
def check_duplicates(conn: sqlite3.Connection) -> None:
    section = "2. 重複検出"
    cur = conn.cursor()

    rows = cur.execute("SELECT id, name, url, contact_address FROM organizations").fetchall()

    name_groups: dict[str, list[sqlite3.Row]] = defaultdict(list)
    url_groups: dict[str, list[sqlite3.Row]] = defaultdict(list)
    addr_groups: dict[str, list[sqlite3.Row]] = defaultdict(list)

    for r in rows:
        nn = normalize_name(r["name"])
        if nn:
            name_groups[nn].append(r)
        nu = normalize_url(r["url"])
        if nu:
            url_groups[nu].append(r)
        if r["contact_address"]:
            addr_groups[r["contact_address"].strip()].append(r)

    # Same normalized name, multiple ids
    name_dups = [(k, v) for k, v in name_groups.items() if len(v) > 1]
    if not name_dups:
        add(section, "正規化名重複", "PASS", "重複なし")
    else:
        sample = "; ".join(f"{v[0]['name']}×{len(v)}" for _, v in name_dups[:5])
        status = "WARN" if len(name_dups) <= 5 else "FAIL"
        add(section, "正規化名重複", status,
            f"{len(name_dups)}グループ: {sample}")

    # Same URL host but different normalized name -> potential
    suspicious_url = []
    for u, v in url_groups.items():
        if len(v) > 1:
            distinct_names = {normalize_name(r["name"]) for r in v}
            if len(distinct_names) > 1:
                suspicious_url.append((u, v))
    if not suspicious_url:
        add(section, "URL一致・名前差異", "PASS", "重複なし")
    else:
        sample = "; ".join(
            f"{u}: {','.join(r['name'] for r in v[:2])}"
            for u, v in suspicious_url[:5]
        )
        status = "WARN" if len(suspicious_url) <= 10 else "FAIL"
        add(section, "URL一致・名前差異", status,
            f"{len(suspicious_url)}件: {sample}")

    # Address exact match -> 複数団体は要警戒
    addr_dups = [(k, v) for k, v in addr_groups.items() if len(v) > 1]
    if not addr_dups:
        add(section, "住所完全一致", "PASS", "重複なし")
    else:
        sample = "; ".join(
            f"{k[:30]}…×{len(v)}" for k, v in addr_dups[:5]
        )
        status = "INFO" if len(addr_dups) <= 5 else "WARN"
        add(section, "住所完全一致", status,
            f"{len(addr_dups)}住所: {sample} (※同一ビル内財団など正規ケースもある)")


# ---------------------------------------------------------------------------
# Section 3: Data quality
# ---------------------------------------------------------------------------
def check_data_quality(conn: sqlite3.Connection, sample_url_check: bool = True) -> None:
    section = "3. データ品質"
    cur = conn.cursor()

    # URL liveness (HEAD, 100 sample)
    if sample_url_check:
        try:
            import urllib.request
            urls = [
                r["url"] for r in cur.execute(
                    "SELECT url FROM organizations WHERE url IS NOT NULL AND url != '' "
                    "ORDER BY RANDOM() LIMIT 100"
                ).fetchall()
            ]
            ok = bad = err = 0
            for u in urls:
                try:
                    req = urllib.request.Request(u, method="HEAD",
                                                 headers={"User-Agent": "CFG-DBVerify/1.0"})
                    with urllib.request.urlopen(req, timeout=5) as resp:
                        if 200 <= resp.status < 400:
                            ok += 1
                        else:
                            bad += 1
                except Exception:
                    err += 1
            total = len(urls)
            if total == 0:
                add(section, "URL有効性 (HEAD 100件)", "WARN", "サンプル取得不可")
            else:
                err_rate = (bad + err) / total
                detail = f"OK={ok}, 4xx/5xx={bad}, 接続エラー={err}, 失敗率={err_rate:.1%}"
                if err_rate <= 0.10:
                    add(section, "URL有効性 (HEAD 100件)", "PASS", detail)
                elif err_rate <= 0.25:
                    add(section, "URL有効性 (HEAD 100件)", "WARN", detail)
                else:
                    add(section, "URL有効性 (HEAD 100件)", "FAIL", detail)
        except Exception as e:
            add(section, "URL有効性 (HEAD 100件)", "WARN", f"検査スキップ: {e}")

    # Phone format
    phones = cur.execute(
        "SELECT contact_phone FROM organizations WHERE contact_phone IS NOT NULL AND contact_phone != ''"
    ).fetchall()
    bad_phone = sum(1 for r in phones if not PHONE_RE.match(r["contact_phone"].strip()))
    detail = f"{bad_phone}/{len(phones)} 不正形式"
    if not phones:
        add(section, "電話番号形式", "WARN", "データなし")
    elif bad_phone / max(1, len(phones)) <= 0.05:
        add(section, "電話番号形式", "PASS", detail)
    else:
        add(section, "電話番号形式", "WARN", detail)

    # Email format
    emails = cur.execute(
        "SELECT contact_email FROM organizations WHERE contact_email IS NOT NULL AND contact_email != ''"
    ).fetchall()
    bad_email = sum(1 for r in emails if not EMAIL_RE.match(r["contact_email"].strip()))
    detail = f"{bad_email}/{len(emails)} 不正形式"
    if not emails:
        add(section, "メール形式", "WARN", "データなし")
    elif bad_email / max(1, len(emails)) <= 0.05:
        add(section, "メール形式", "PASS", detail)
    else:
        add(section, "メール形式", "WARN", detail)

    # Grant amount anomalies (organizations.annual_grant_amount + grant_results.award_amount)
    n_neg_org = cur.execute(
        "SELECT COUNT(*) FROM organizations WHERE annual_grant_amount IS NOT NULL AND annual_grant_amount < 0"
    ).fetchone()[0]
    n_huge_org = cur.execute(
        "SELECT COUNT(*) FROM organizations WHERE annual_grant_amount IS NOT NULL AND annual_grant_amount > 100000000000"
    ).fetchone()[0]  # > 100億円
    detail_org = f"負値={n_neg_org}, 100億円超={n_huge_org}"
    if n_neg_org == 0 and n_huge_org == 0:
        add(section, "annual_grant_amount 異常値", "PASS", detail_org)
    elif n_neg_org > 0:
        add(section, "annual_grant_amount 異常値", "FAIL", detail_org)
    else:
        add(section, "annual_grant_amount 異常値", "WARN", detail_org)

    n_neg_award = cur.execute(
        "SELECT COUNT(*) FROM grant_results WHERE award_amount IS NOT NULL AND award_amount < 0"
    ).fetchone()[0]
    n_huge_award = cur.execute(
        "SELECT COUNT(*) FROM grant_results WHERE award_amount IS NOT NULL AND award_amount > 1000000000"
    ).fetchone()[0]  # > 10億円
    detail_award = f"負値={n_neg_award}, 10億円超={n_huge_award}"
    if n_neg_award == 0 and n_huge_award == 0:
        add(section, "award_amount 異常値", "PASS", detail_award)
    elif n_neg_award > 0:
        add(section, "award_amount 異常値", "FAIL", detail_award)
    else:
        add(section, "award_amount 異常値", "WARN", detail_award)

    # Prefecture validity
    prefs = cur.execute(
        "SELECT prefecture FROM organizations WHERE prefecture IS NOT NULL AND prefecture != ''"
    ).fetchall()
    bad_pref = [r["prefecture"] for r in prefs if r["prefecture"].strip() not in PREFECTURES]
    detail = f"{len(bad_pref)}/{len(prefs)} 不正値"
    if not bad_pref:
        add(section, "都道府県名有効性", "PASS", detail)
    elif len(bad_pref) / max(1, len(prefs)) <= 0.02:
        add(section, "都道府県名有効性", "WARN",
            detail + " 例: " + ", ".join(sorted(set(bad_pref))[:5]))
    else:
        add(section, "都道府県名有効性", "FAIL",
            detail + " 例: " + ", ".join(sorted(set(bad_pref))[:5]))

    # foundation_subtype CHECK制約整合性 (実値が許可セットに含まれるか)
    subtypes = cur.execute(
        "SELECT DISTINCT foundation_subtype FROM organizations WHERE foundation_subtype IS NOT NULL"
    ).fetchall()
    actual = {r["foundation_subtype"] for r in subtypes}
    invalid = actual - ALLOWED_SUBTYPES
    if not invalid:
        add(section, "foundation_subtype 整合性", "PASS",
            f"値={sorted(actual)}")
    else:
        add(section, "foundation_subtype 整合性", "FAIL",
            f"許可外の値: {sorted(invalid)}")


# ---------------------------------------------------------------------------
# Section 4: Awardee data quality
# ---------------------------------------------------------------------------
def check_awardee_quality(conn: sqlite3.Connection) -> None:
    section = "4. 採択者データ品質"
    cur = conn.cursor()

    total = cur.execute("SELECT COUNT(*) FROM grant_results").fetchone()[0]
    if total == 0:
        add(section, "前提", "WARN", "grant_results が空")
        return

    n_blank = cur.execute(
        "SELECT COUNT(*) FROM grant_results WHERE awardee_name IS NULL OR TRIM(awardee_name) = ''"
    ).fetchone()[0]
    rate = n_blank / total
    detail = f"{n_blank}/{total} = {rate:.2%}"
    if rate == 0:
        add(section, "awardee_name 空白率", "PASS", detail)
    elif rate <= 0.01:
        add(section, "awardee_name 空白率", "WARN", detail)
    else:
        add(section, "awardee_name 空白率", "FAIL", detail)

    # Affiliation 表記揺れ: same normalized form, multiple raw strings
    affs = cur.execute(
        "SELECT awardee_affiliation FROM grant_results "
        "WHERE awardee_affiliation IS NOT NULL AND awardee_affiliation != ''"
    ).fetchall()
    aff_map: dict[str, set[str]] = defaultdict(set)
    for r in affs:
        raw = r["awardee_affiliation"].strip()
        nn = normalize_name(raw)
        if nn:
            aff_map[nn].add(raw)
    variant_groups = [(k, v) for k, v in aff_map.items() if len(v) > 1]
    n_variants = sum(len(v) - 1 for _, v in variant_groups)
    if not variant_groups:
        add(section, "awardee_affiliation 表記揺れ", "PASS", "揺れ検出なし")
    elif len(variant_groups) <= 30:
        sample = "; ".join(
            f"[{','.join(list(v)[:2])}]" for _, v in variant_groups[:3]
        )
        add(section, "awardee_affiliation 表記揺れ", "WARN",
            f"{len(variant_groups)}グループ・追加揺れ {n_variants}件 例: {sample}")
    else:
        add(section, "awardee_affiliation 表記揺れ", "WARN",
            f"{len(variant_groups)}グループ・追加揺れ {n_variants}件 (要正規化)")

    # 同一研究者の複数財団重複 (期待 ≈ 69)
    multi = cur.execute(
        """
        SELECT awardee_name, COUNT(DISTINCT gp.organization_id) AS n_orgs
        FROM grant_results gr
        JOIN grant_calls gc ON gr.call_id = gc.id
        JOIN grant_programs gp ON gc.program_id = gp.id
        WHERE awardee_name IS NOT NULL AND awardee_name != ''
        GROUP BY awardee_name
        HAVING n_orgs > 1
        """
    ).fetchall()
    n_multi = len(multi)
    detail = f"{n_multi}名 (参考値 69)"
    if 30 <= n_multi <= 200:
        add(section, "複数財団からの受領者", "PASS", detail)
    elif n_multi == 0:
        add(section, "複数財団からの受領者", "WARN", "0件 (収集不足の可能性)")
    else:
        add(section, "複数財団からの受領者", "WARN", detail + " (要確認)")

    # fiscal_year 異常値
    fy_min, fy_max = cur.execute(
        "SELECT MIN(fiscal_year), MAX(fiscal_year) FROM grant_results"
    ).fetchone()
    n_old = cur.execute(
        "SELECT COUNT(*) FROM grant_results WHERE fiscal_year < 1990"
    ).fetchone()[0]
    n_future = cur.execute(
        "SELECT COUNT(*) FROM grant_results WHERE fiscal_year > 2030"
    ).fetchone()[0]
    detail = f"範囲={fy_min}〜{fy_max}, 1990未満={n_old}, 2030超={n_future}"
    if n_old == 0 and n_future == 0:
        add(section, "fiscal_year 異常値", "PASS", detail)
    else:
        add(section, "fiscal_year 異常値", "WARN", detail)


# ---------------------------------------------------------------------------
# Section 5: Coverage indicators
# ---------------------------------------------------------------------------
def check_coverage(conn: sqlite3.Connection) -> None:
    section = "5. カバレッジ指標"
    cur = conn.cursor()

    # JFC top100 match
    matched = cur.execute(
        "SELECT COUNT(*) FROM organizations WHERE jfc_rank IS NOT NULL AND jfc_rank <= 100"
    ).fetchone()[0]
    rate = matched / 100
    detail = f"{matched}/100 = {rate:.0%} (基準 ≥{JFC_TOP100_EXPECTED_RATE:.0%})"
    if rate >= 0.80:
        add(section, "JFC top100 マッチ率", "PASS", detail)
    elif rate >= JFC_TOP100_EXPECTED_RATE:
        add(section, "JFC top100 マッチ率", "WARN", detail)
    else:
        add(section, "JFC top100 マッチ率", "FAIL", detail)

    # Legal form distribution
    forms = cur.execute(
        "SELECT legal_form, COUNT(*) c FROM organizations GROUP BY legal_form ORDER BY c DESC"
    ).fetchall()
    form_str = ", ".join(f"{r['legal_form'] or '(NULL)'}={r['c']}" for r in forms)
    add(section, "法人形態別構成", "INFO", form_str)

    # 47都道府県 coverage
    pref_count = cur.execute(
        "SELECT COUNT(DISTINCT prefecture) FROM organizations "
        "WHERE prefecture IS NOT NULL AND prefecture != ''"
    ).fetchone()[0]
    detail = f"{pref_count}/47都道府県"
    if pref_count >= 47:
        add(section, "47都道府県カバレッジ", "PASS", detail)
    elif pref_count >= EXPECTED_PREF_COUNT_MIN:
        add(section, "47都道府県カバレッジ", "WARN", detail)
    else:
        add(section, "47都道府県カバレッジ", "FAIL", detail)

    # Subtype distribution balance
    subs = cur.execute(
        "SELECT foundation_subtype, COUNT(*) c FROM organizations "
        "GROUP BY foundation_subtype ORDER BY c DESC"
    ).fetchall()
    total = sum(r["c"] for r in subs) or 1
    sub_str = ", ".join(
        f"{r['foundation_subtype'] or '(NULL)'}={r['c']}({r['c']/total:.0%})"
        for r in subs
    )
    # Bias check: corporate should dominate, but no single non-corporate type exceeds 50%
    top = subs[0] if subs else None
    if top and top["foundation_subtype"] == "corporate" and top["c"] / total >= 0.30:
        add(section, "設立者形態別分布", "PASS", sub_str)
    else:
        add(section, "設立者形態別分布", "WARN", sub_str)


# ---------------------------------------------------------------------------
# Section 6: Cross checks
# ---------------------------------------------------------------------------
def check_cross_db(conn: sqlite3.Connection) -> None:
    section = "6. クロスチェック"
    cur = conn.cursor()

    # Grant DB overlap
    if not GRANT_DB_PATH.exists():
        add(section, "Grant DB 重複検出", "WARN", f"Grant DB 不在: {GRANT_DB_PATH}")
    else:
        cfg_names = {
            normalize_name(r["name"])
            for r in cur.execute("SELECT name FROM organizations").fetchall()
            if r["name"]
        }
        cfg_names.discard("")
        try:
            with _connect(GRANT_DB_PATH) as gconn:
                grant_names = {
                    normalize_name(r["name"])
                    for r in gconn.execute("SELECT name FROM organizations").fetchall()
                    if r["name"]
                }
                grant_names.discard("")
        except Exception as e:
            add(section, "Grant DB 重複検出", "WARN", f"読み込み失敗: {e}")
            grant_names = set()

        if grant_names:
            overlap = cfg_names & grant_names
            rate = len(overlap) / max(1, len(cfg_names))
            detail = (f"CFG={len(cfg_names)}, Grant={len(grant_names)}, "
                      f"重複={len(overlap)}件 ({rate:.1%})")
            # Overlap is informational; high overlap is expected for some orgs
            if rate <= 0.50:
                add(section, "Grant DB 重複検出", "INFO", detail)
            else:
                add(section, "Grant DB 重複検出", "WARN",
                    detail + " (重複多数・統合検討)")

    # 親・子プロジェクト整合性
    # parent_companies referenced by organizations.parent_company_id
    bad_parent = cur.execute(
        """
        SELECT COUNT(*) FROM organizations
        WHERE parent_company_id IS NOT NULL
          AND parent_company_id NOT IN (SELECT id FROM parent_companies)
        """
    ).fetchone()[0]
    n_with_parent = cur.execute(
        "SELECT COUNT(*) FROM organizations WHERE parent_company_id IS NOT NULL"
    ).fetchone()[0]
    detail = f"親company未参照={bad_parent}件, parent_company_id付与={n_with_parent}件"
    if bad_parent == 0:
        add(section, "親-子プロジェクト整合性", "PASS", detail)
    else:
        add(section, "親-子プロジェクト整合性", "FAIL", detail)

    # program -> org / call -> program / result -> call の孤立行
    orphan_calls = cur.execute(
        """
        SELECT COUNT(*) FROM grant_calls gc
        WHERE NOT EXISTS (SELECT 1 FROM grant_programs gp WHERE gp.id = gc.program_id)
        """
    ).fetchone()[0]
    orphan_programs = cur.execute(
        """
        SELECT COUNT(*) FROM grant_programs gp
        WHERE NOT EXISTS (SELECT 1 FROM organizations o WHERE o.id = gp.organization_id)
        """
    ).fetchone()[0]
    orphan_results = cur.execute(
        """
        SELECT COUNT(*) FROM grant_results gr
        WHERE NOT EXISTS (SELECT 1 FROM grant_calls gc WHERE gc.id = gr.call_id)
        """
    ).fetchone()[0]
    detail = (f"orphan programs={orphan_programs}, "
              f"orphan calls={orphan_calls}, "
              f"orphan results={orphan_results}")
    if orphan_calls == 0 and orphan_results == 0 and orphan_programs == 0:
        add(section, "親子テーブル孤立行", "PASS", detail)
    else:
        add(section, "親子テーブル孤立行", "FAIL", detail)


# ---------------------------------------------------------------------------
# Reporter
# ---------------------------------------------------------------------------
def write_report() -> tuple[int, int, int]:
    by_section: dict[str, list[Result]] = defaultdict(list)
    for r in RESULTS:
        by_section[r.section].append(r)

    n_pass = sum(1 for r in RESULTS if r.status == "PASS")
    n_warn = sum(1 for r in RESULTS if r.status == "WARN")
    n_fail = sum(1 for r in RESULTS if r.status == "FAIL")
    n_info = sum(1 for r in RESULTS if r.status == "INFO")

    overall = "PASS" if n_fail == 0 and n_warn == 0 else ("WARN" if n_fail == 0 else "FAIL")

    lines = []
    lines.append("# CFG-DB 品質検証レポート")
    lines.append("")
    lines.append(f"- 生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"- DB: `{DB_PATH}`")
    lines.append(f"- 総合判定: **{overall}**")
    lines.append(f"- 内訳: PASS={n_pass} / WARN={n_warn} / FAIL={n_fail} / INFO={n_info}")
    lines.append("")
    lines.append("## サマリ")
    lines.append("")
    lines.append("| セクション | PASS | WARN | FAIL | INFO |")
    lines.append("|---|---:|---:|---:|---:|")
    for sec in sorted(by_section.keys()):
        rs = by_section[sec]
        lines.append(
            f"| {sec} | "
            f"{sum(1 for r in rs if r.status=='PASS')} | "
            f"{sum(1 for r in rs if r.status=='WARN')} | "
            f"{sum(1 for r in rs if r.status=='FAIL')} | "
            f"{sum(1 for r in rs if r.status=='INFO')} |"
        )
    lines.append("")

    for sec in sorted(by_section.keys()):
        lines.append(f"## {sec}")
        lines.append("")
        lines.append("| 判定 | 検査項目 | 詳細 |")
        lines.append("|---|---|---|")
        for r in by_section[sec]:
            detail = (r.detail or "").replace("|", "\\|").replace("\n", " ")
            lines.append(f"| **{r.status}** | {r.name} | {detail} |")
        lines.append("")

    # Recommended actions
    lines.append("## 推奨改善アクション")
    lines.append("")
    fails = [r for r in RESULTS if r.status == "FAIL"]
    warns = [r for r in RESULTS if r.status == "WARN"]
    if fails:
        lines.append("### FAIL (即時対応)")
        for r in fails:
            lines.append(f"- [{r.section}] {r.name}: {r.detail}")
        lines.append("")
    if warns:
        lines.append("### WARN (次回スプリントで対応)")
        for r in warns:
            lines.append(f"- [{r.section}] {r.name}: {r.detail}")
        lines.append("")
    if not fails and not warns:
        lines.append("- 即時対応事項なし。次回検証時はサンプリングサイズ拡大を推奨。")
        lines.append("")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    return n_pass, n_warn, n_fail


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    if not DB_PATH.exists():
        print(f"[ERROR] DB not found: {DB_PATH}", file=sys.stderr)
        return 2

    no_net = "--no-network" in sys.argv

    started = time.time()
    print(f"[verify_db] using {DB_PATH}")
    with _connect(DB_PATH) as conn:
        check_integrity(conn)
        check_duplicates(conn)
        check_data_quality(conn, sample_url_check=not no_net)
        check_awardee_quality(conn)
        check_coverage(conn)
        check_cross_db(conn)

    n_pass, n_warn, n_fail = write_report()
    elapsed = time.time() - started
    print(f"[verify_db] PASS={n_pass} WARN={n_warn} FAIL={n_fail}  ({elapsed:.1f}s)")
    print(f"[verify_db] report: {REPORT_PATH}")
    return 0 if n_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
