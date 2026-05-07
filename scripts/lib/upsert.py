"""Upsert helpers for ``grant_results``.

A parsed record is shaped like::

    {
        "fiscal_year": 2024,
        "awardee_name": "井上 大地",
        "awardee_affiliation": "東京大学",
        "awardee_position": "教授",
        "project_title": "...",
        "award_amount": 5000000,         # JPY, optional
        "program_name": "自然科学研究助成",
        "source_url": "https://...",
        "field_category_id": "natural_science",  # optional
        "metadata": {"...": "..."},                # optional
    }

`call_id` is resolved by:
1. Look up ``grant_programs`` row by (organization_id, name LIKE program_name)
   – if absent, create a minimal program row.
2. Look up ``grant_calls`` by (program_id, fiscal_year) – if absent, create a
   minimal call row.

The function is idempotent on (call_id, awardee_name, project_title) –
re-running does not duplicate records.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from datetime import datetime
from typing import Iterable

LOG = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _new_id(prefix: str = "") -> str:
    return f"{prefix}{uuid.uuid4().hex[:12]}"


def _ensure_program(
    conn: sqlite3.Connection,
    organization_id: str,
    program_name: str,
    source_url: str | None,
) -> str:
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM grant_programs WHERE organization_id=? AND name=? LIMIT 1",
        (organization_id, program_name),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    pid = _new_id("prog_")
    cur.execute(
        """
        INSERT INTO grant_programs (
            id, organization_id, name, category, source_url, is_recurring,
            created_at, updated_at
        ) VALUES (?, ?, ?, 'research', ?, 1, ?, ?)
        """,
        (pid, organization_id, program_name, source_url, _now(), _now()),
    )
    return pid


def _ensure_call(
    conn: sqlite3.Connection,
    program_id: str,
    fiscal_year: int,
    program_name: str,
    source_url: str | None,
) -> str:
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM grant_calls WHERE program_id=? AND fiscal_year=? LIMIT 1",
        (program_id, fiscal_year),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cid = _new_id("call_")
    cur.execute(
        """
        INSERT INTO grant_calls (
            id, program_id, fiscal_year, title, status, source_url,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, 'closed', ?, ?, ?)
        """,
        (
            cid,
            program_id,
            fiscal_year,
            f"{program_name} {fiscal_year}年度",
            source_url,
            _now(),
            _now(),
        ),
    )
    return cid


def _result_exists(
    conn: sqlite3.Connection, call_id: str, awardee_name: str, project_title: str
) -> str | None:
    cur = conn.cursor()
    cur.execute(
        """SELECT id FROM grant_results
           WHERE call_id=? AND awardee_name=? AND project_title=? LIMIT 1""",
        (call_id, awardee_name, project_title),
    )
    row = cur.fetchone()
    return row[0] if row else None


def upsert_results(
    conn: sqlite3.Connection,
    organization_id: str,
    records: Iterable[dict],
) -> tuple[int, int]:
    """Insert or update grant_results records. Returns (inserted, updated)."""
    inserted = 0
    updated = 0
    for rec in records:
        program_name = rec.get("program_name") or "研究助成"
        source_url = rec.get("source_url")
        fy = rec.get("fiscal_year")
        if fy is None:
            LOG.warning("skip: missing fiscal_year for %s", rec)
            continue
        program_id = _ensure_program(conn, organization_id, program_name, source_url)
        call_id = _ensure_call(conn, program_id, int(fy), program_name, source_url)
        awardee = (rec.get("awardee_name") or "").strip()
        title = (rec.get("project_title") or "").strip()
        if not awardee or not title:
            LOG.warning("skip: missing name/title in %s", rec)
            continue

        existing = _result_exists(conn, call_id, awardee, title)
        meta = rec.get("metadata") or {}
        meta_json = json.dumps(meta, ensure_ascii=False) if meta else None

        if existing:
            conn.execute(
                """UPDATE grant_results SET
                       awardee_affiliation=COALESCE(?, awardee_affiliation),
                       awardee_position=COALESCE(?, awardee_position),
                       award_amount=COALESCE(?, award_amount),
                       field_category_id=COALESCE(?, field_category_id),
                       source_url=COALESCE(?, source_url),
                       metadata=COALESCE(?, metadata),
                       updated_at=?
                   WHERE id=?""",
                (
                    rec.get("awardee_affiliation"),
                    rec.get("awardee_position"),
                    rec.get("award_amount"),
                    rec.get("field_category_id"),
                    source_url,
                    meta_json,
                    _now(),
                    existing,
                ),
            )
            updated += 1
        else:
            rid = _new_id("res_")
            conn.execute(
                """INSERT INTO grant_results (
                       id, call_id, fiscal_year, awardee_name,
                       awardee_affiliation, awardee_position,
                       project_title, award_amount,
                       field_category_id, source_url, metadata,
                       created_at, updated_at
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    rid,
                    call_id,
                    int(fy),
                    awardee,
                    rec.get("awardee_affiliation"),
                    rec.get("awardee_position"),
                    title,
                    rec.get("award_amount"),
                    rec.get("field_category_id"),
                    source_url,
                    meta_json,
                    _now(),
                    _now(),
                ),
            )
            inserted += 1
    conn.commit()
    return inserted, updated


def resolve_organization_id(
    conn: sqlite3.Connection, name_patterns: list[str]
) -> str | None:
    """Find an organizations.id for the first matching name LIKE pattern.

    Patterns are tried in order. ``name_patterns`` should not include ``%``;
    they are added automatically.
    """
    cur = conn.cursor()
    for pat in name_patterns:
        cur.execute(
            "SELECT id, name FROM organizations WHERE name LIKE ? "
            "ORDER BY CASE WHEN name LIKE ? THEN 0 ELSE 1 END, length(name) LIMIT 1",
            (f"%{pat}%", f"公益財団法人{pat}"),
        )
        row = cur.fetchone()
        if row:
            return row[0]
    return None
