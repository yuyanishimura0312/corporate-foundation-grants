"""CFG ↔ Grant DB 横断クエリヘルパー

両DBに同時接続し、cross_db_mappingで紐付けたデータを統合検索する。
"""
from __future__ import annotations
import sqlite3
from pathlib import Path

CFG_DB = Path("/Users/nishimura+/projects/apps/corporate-foundation-grants/corporate_research_grants.sqlite")
GRANT_DB = Path("/Users/nishimura+/projects/apps/grant-db/grant_db.sqlite")


def connect_both() -> tuple[sqlite3.Connection, sqlite3.Connection]:
    """両DBに同時接続を返す"""
    cfg = sqlite3.connect(CFG_DB, timeout=60)
    cfg.execute("PRAGMA busy_timeout = 60000")
    cfg.row_factory = sqlite3.Row
    gdb = sqlite3.connect(GRANT_DB, timeout=60)
    gdb.row_factory = sqlite3.Row
    return cfg, gdb


def get_grant_calls_for_cfg(cfg_id: str) -> list[dict]:
    """CFG財団のidに対応するGrant DB側のgrant_callsを取得"""
    cfg, gdb = connect_both()
    try:
        # Get mapped Grant DB org IDs
        mapped = cfg.execute(
            "SELECT grant_db_id FROM cross_db_mapping WHERE cfg_id = ?",
            (cfg_id,)
        ).fetchall()
        if not mapped:
            return []
        gdb_ids = [r["grant_db_id"] for r in mapped]
        placeholders = ",".join(["?"] * len(gdb_ids))
        # Query Grant DB
        rows = gdb.execute(
            f"""SELECT c.id, c.title, c.fiscal_year, c.application_deadline,
                       c.grant_amount_min, c.grant_amount_max, c.status,
                       c.summary, c.source_url, c.guideline_url,
                       p.name AS program_name, p.category, o.name AS org_name
                FROM grant_calls c
                JOIN grant_programs p ON p.id = c.program_id
                JOIN organizations o ON o.id = p.organization_id
                WHERE p.organization_id IN ({placeholders})
                ORDER BY c.application_deadline DESC""",
            gdb_ids,
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        cfg.close()
        gdb.close()


def search_unified(query: str, limit: int = 50) -> dict:
    """CFG + Grant DB をキーワード検索"""
    cfg, gdb = connect_both()
    try:
        result = {"cfg_foundations": [], "cfg_programs": [], "cfg_awardees": [],
                  "gdb_calls": [], "gdb_programs": []}
        like = f"%{query}%"

        # CFG: foundations
        rows = cfg.execute(
            """SELECT id, name, foundation_subtype, prefecture, url
               FROM organizations
               WHERE name LIKE ? OR description LIKE ?
               LIMIT ?""",
            (like, like, limit),
        ).fetchall()
        result["cfg_foundations"] = [dict(r) for r in rows]

        # CFG: programs
        rows = cfg.execute(
            """SELECT p.id, p.name, p.category, o.name AS foundation
               FROM grant_programs p
               JOIN organizations o ON o.id = p.organization_id
               WHERE p.name LIKE ? OR p.description LIKE ? OR p.purpose LIKE ?
               LIMIT ?""",
            (like, like, like, limit),
        ).fetchall()
        result["cfg_programs"] = [dict(r) for r in rows]

        # CFG: awardees
        rows = cfg.execute(
            """SELECT awardee_name, awardee_affiliation, project_title, fiscal_year, award_amount
               FROM grant_results
               WHERE project_title LIKE ? OR awardee_affiliation LIKE ?
               LIMIT ?""",
            (like, like, limit),
        ).fetchall()
        result["cfg_awardees"] = [dict(r) for r in rows]

        # Grant DB: calls
        rows = gdb.execute(
            """SELECT c.id, c.title, c.fiscal_year, c.application_deadline,
                      c.status, p.name AS program_name, o.name AS org_name
               FROM grant_calls c
               JOIN grant_programs p ON p.id = c.program_id
               JOIN organizations o ON o.id = p.organization_id
               WHERE c.title LIKE ? OR c.summary LIKE ? OR c.keywords LIKE ?
               LIMIT ?""",
            (like, like, like, limit),
        ).fetchall()
        result["gdb_calls"] = [dict(r) for r in rows]

        # Grant DB: programs
        rows = gdb.execute(
            """SELECT p.id, p.name, p.category, o.name AS org_name
               FROM grant_programs p
               JOIN organizations o ON o.id = p.organization_id
               WHERE p.name LIKE ? OR p.description LIKE ?
               LIMIT ?""",
            (like, like, limit),
        ).fetchall()
        result["gdb_programs"] = [dict(r) for r in rows]

        return result
    finally:
        cfg.close()
        gdb.close()


def coverage_overlap_stats() -> dict:
    """両DBのカバレッジ重複統計"""
    cfg, gdb = connect_both()
    try:
        stats = {}
        stats["cfg_total"] = cfg.execute("SELECT COUNT(*) FROM organizations").fetchone()[0]
        stats["gdb_total"] = gdb.execute("SELECT COUNT(*) FROM organizations").fetchone()[0]
        stats["mapped"] = cfg.execute("SELECT COUNT(*) FROM cross_db_mapping").fetchone()[0]
        stats["cfg_only"] = stats["cfg_total"] - cfg.execute(
            "SELECT COUNT(DISTINCT cfg_id) FROM cross_db_mapping"
        ).fetchone()[0]
        stats["gdb_only"] = stats["gdb_total"] - cfg.execute(
            "SELECT COUNT(DISTINCT grant_db_id) FROM cross_db_mapping"
        ).fetchone()[0]
        stats["cfg_calls"] = cfg.execute("SELECT COUNT(*) FROM grant_calls").fetchone()[0]
        stats["gdb_calls"] = gdb.execute("SELECT COUNT(*) FROM grant_calls").fetchone()[0]
        stats["cfg_programs"] = cfg.execute("SELECT COUNT(*) FROM grant_programs").fetchone()[0]
        stats["gdb_programs"] = gdb.execute("SELECT COUNT(*) FROM grant_programs").fetchone()[0]
        return stats
    finally:
        cfg.close()
        gdb.close()


def upcoming_calls_for_field(field_keyword: str, limit: int = 30) -> list[dict]:
    """特定領域の応募受付中・予定の公募（CFG+Grant DB統合）"""
    cfg, gdb = connect_both()
    try:
        like = f"%{field_keyword}%"
        # CFG side
        cfg_rows = cfg.execute(
            """SELECT 'CFG' AS source, c.title, c.application_deadline, c.grant_amount_max,
                      o.name AS foundation, c.source_url
               FROM grant_calls c
               JOIN grant_programs p ON p.id = c.program_id
               JOIN organizations o ON o.id = p.organization_id
               WHERE (c.application_deadline >= date('now') OR c.status = 'open')
                 AND (c.title LIKE ? OR c.keywords LIKE ? OR p.name LIKE ?)
               LIMIT ?""",
            (like, like, like, limit),
        ).fetchall()
        # Grant DB side
        gdb_rows = gdb.execute(
            """SELECT 'GDB' AS source, c.title, c.application_deadline, c.grant_amount_max,
                      o.name AS foundation, c.source_url
               FROM grant_calls c
               JOIN grant_programs p ON p.id = c.program_id
               JOIN organizations o ON o.id = p.organization_id
               WHERE (c.application_deadline >= date('now') OR c.status = 'open')
                 AND (c.title LIKE ? OR c.keywords LIKE ? OR p.name LIKE ?)
               LIMIT ?""",
            (like, like, like, limit),
        ).fetchall()
        combined = [dict(r) for r in cfg_rows] + [dict(r) for r in gdb_rows]
        # Sort by deadline
        combined.sort(key=lambda x: x.get("application_deadline") or "9999-12-31")
        return combined[:limit]
    finally:
        cfg.close()
        gdb.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "stats":
        s = coverage_overlap_stats()
        print(f"=== CFG ↔ Grant DB カバレッジ統計 ===")
        print(f"CFG total: {s['cfg_total']:,} / Grant DB total: {s['gdb_total']:,}")
        print(f"Mapped: {s['mapped']:,}")
        print(f"CFG only: {s['cfg_only']:,}")
        print(f"GDB only: {s['gdb_only']:,}")
        print(f"\nGrant calls: CFG {s['cfg_calls']:,} vs GDB {s['gdb_calls']:,}")
        print(f"Programs: CFG {s['cfg_programs']:,} vs GDB {s['gdb_programs']:,}")
    elif len(sys.argv) > 1 and sys.argv[1] == "search":
        query = sys.argv[2] if len(sys.argv) > 2 else "がん"
        r = search_unified(query, limit=20)
        print(f"=== Unified search: {query} ===")
        print(f"CFG foundations: {len(r['cfg_foundations'])}")
        print(f"CFG programs: {len(r['cfg_programs'])}")
        print(f"CFG awardees: {len(r['cfg_awardees'])}")
        print(f"GDB calls: {len(r['gdb_calls'])}")
        print(f"GDB programs: {len(r['gdb_programs'])}")
    else:
        print("Usage: cross_db.py stats | search <keyword>")
