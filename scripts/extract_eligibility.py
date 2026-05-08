#!/usr/bin/env python3
"""S1.4: 応募資格条件をdescription/summary/purposeから抽出してeligibility_criteriaへ"""
from __future__ import annotations
import re
import sqlite3
import uuid
from pathlib import Path

DB = Path("/Users/nishimura+/projects/apps/corporate-foundation-grants/corporate_research_grants.sqlite")


def extract_age(text: str) -> str | None:
    m = re.search(r"(\d{2})\s*歳\s*(?:以下|未満|まで)", text)
    if m:
        return f"{m.group(1)}歳以下"
    if "若手" in text:
        return "若手研究者（年齢制限あり）"
    return None


def extract_position(text: str) -> str | None:
    positions = []
    for kw in ["教授", "准教授", "講師", "助教", "ポストドクター", "ポスドク",
              "博士課程", "博士後期", "博士前期", "修士", "学部生",
              "研究員", "PI", "プリンシパル", "助手"]:
        if kw in text:
            positions.append(kw)
    return ",".join(positions) if positions else None


def extract_nationality(text: str) -> str | None:
    if "日本国籍" in text or "日本国民" in text:
        return "日本国籍"
    if "国籍不問" in text or "国籍を問わない" in text:
        return "国籍不問"
    if "在日外国人" in text or "外国籍" in text:
        return "外国籍可"
    return None


def extract_affiliation(text: str) -> str | None:
    types = []
    for kw in ["大学", "大学院", "研究機関", "国公立", "私立",
              "企業研究者", "民間企業", "NPO", "NGO",
              "独立行政法人", "国立研究開発法人"]:
        if kw in text:
            types.append(kw)
    return ",".join(types) if types else None


def extract_field(text: str) -> str | None:
    fields = []
    for kw in ["自然科学", "生命科学", "医学", "工学", "情報",
              "人文", "社会科学", "教育", "芸術", "国際",
              "全分野", "あらゆる分野", "学際"]:
        if kw in text:
            fields.append(kw)
    return ",".join(fields) if fields else None


def main():
    conn = sqlite3.connect(DB, timeout=60)
    conn.execute("PRAGMA busy_timeout = 60000")
    cur = conn.cursor()

    cur.execute("""SELECT c.id, c.summary, p.name, p.description, p.purpose
                   FROM grant_calls c
                   JOIN grant_programs p ON c.program_id = p.id""")
    calls = cur.fetchall()
    print(f"Total calls: {len(calls)}")

    extractors = [
        ("age", extract_age),
        ("position", extract_position),
        ("nationality", extract_nationality),
        ("affiliation_type", extract_affiliation),
        ("field", extract_field),
    ]

    inserted_total = 0
    calls_with_any = 0

    for cid, summary, name, desc, purpose in calls:
        text = " ".join(filter(None, [summary, name, desc, purpose]))
        if not text:
            continue
        had_any = False
        for crit_type, extractor in extractors:
            value = extractor(text)
            if value:
                # Check if already exists
                cur.execute("""SELECT id FROM eligibility_criteria
                               WHERE call_id = ? AND criterion_type = ?""",
                            (cid, crit_type))
                if cur.fetchone():
                    continue
                cur.execute("""INSERT INTO eligibility_criteria
                    (id, call_id, criterion_type, description, is_required, created_at)
                    VALUES (?, ?, ?, ?, 1, datetime('now','localtime'))""",
                    (str(uuid.uuid4()), cid, crit_type, value))
                inserted_total += 1
                had_any = True
        if had_any:
            calls_with_any += 1

    conn.commit()

    cur.execute("SELECT COUNT(DISTINCT call_id) FROM eligibility_criteria")
    cwc = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM eligibility_criteria")
    total_c = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM grant_calls")
    t = cur.fetchone()[0]
    print(f"\nNew criteria inserted: {inserted_total}")
    print(f"Calls covered (with any criterion): {cwc}/{t} ({cwc/t*100:.1f}%)")
    print(f"Total criteria records: {total_c}")
    conn.close()


if __name__ == "__main__":
    main()
