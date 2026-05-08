#!/usr/bin/env python3
"""S1.1: actual_awards / expected_awards をgrant_resultsから逆算"""
from __future__ import annotations
import sqlite3
from collections import defaultdict
from pathlib import Path

DB = Path("/Users/nishimura+/projects/apps/corporate-foundation-grants/corporate_research_grants.sqlite")


def main():
    conn = sqlite3.connect(DB, timeout=60)
    conn.execute("PRAGMA busy_timeout = 60000")
    cur = conn.cursor()

    # 1. actual_awards: grant_results count by call_id
    cur.execute("""
        SELECT call_id, COUNT(*) FROM grant_results
        GROUP BY call_id
    """)
    actual_by_call = dict(cur.fetchall())
    print(f"call_ids with awardees: {len(actual_by_call)}")

    actual_updated = 0
    for call_id, cnt in actual_by_call.items():
        cur.execute(
            "UPDATE grant_calls SET actual_awards=?, updated_at=datetime('now','localtime') WHERE id=?",
            (cnt, call_id),
        )
        if cur.rowcount > 0:
            actual_updated += 1
    print(f"  actual_awards updated: {actual_updated}")

    # 2. expected_awards: per program, take avg or median of past actual_awards
    # First, aggregate awardees by (program_id, fiscal_year)
    cur.execute("""
        SELECT p.id AS program_id, c.fiscal_year, COUNT(r.id) AS n
        FROM grant_programs p
        JOIN grant_calls c ON c.program_id = p.id
        LEFT JOIN grant_results r ON r.call_id = c.id
        GROUP BY p.id, c.fiscal_year
    """)
    program_year_counts = defaultdict(list)
    for pid, year, n in cur.fetchall():
        if n > 0:
            program_year_counts[pid].append(n)

    # Compute median per program
    expected_updated = 0
    for pid, counts in program_year_counts.items():
        counts.sort()
        median = counts[len(counts) // 2]
        # Update all grant_calls for this program where expected_awards is NULL
        cur.execute("""
            UPDATE grant_calls SET expected_awards=?, updated_at=datetime('now','localtime')
            WHERE program_id=? AND expected_awards IS NULL
        """, (median, pid))
        expected_updated += cur.rowcount

    print(f"  expected_awards updated (median backfill): {expected_updated}")

    conn.commit()

    cur.execute("SELECT COUNT(*) FROM grant_calls WHERE actual_awards IS NOT NULL")
    a = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM grant_calls WHERE expected_awards IS NOT NULL")
    e = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM grant_calls")
    t = cur.fetchone()[0]
    print(f"\nFinal: actual_awards {a}/{t} ({a/t*100:.1f}%)")
    print(f"Final: expected_awards {e}/{t} ({e/t*100:.1f}%)")
    conn.close()


if __name__ == "__main__":
    main()
