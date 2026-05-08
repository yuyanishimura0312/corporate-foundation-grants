#!/usr/bin/env python3
"""
collect_prefectural.py
======================

47都道府県+政令指定都市の地域研究振興財団・産業支援機関を収集してCFG DBに統合する。

対象:
  - 各都道府県の科学技術振興センター・産業振興公社・産業支援機構
  - 政令指定都市の同種団体（札幌・仙台・東京・横浜・川崎・千葉・さいたま・新潟
    ・静岡・浜松・名古屋・京都・大阪・神戸・岡山・広島・福岡・北九州・熊本）
  - 県・市の研究助成財団

データソース:
  data/prefectural_organizations.json — 公益法人info / 各都道府県公式サイト
  「外郭団体」「所管団体」リストを基にした手動編集リスト

DB統合:
  - type='foundation', subtype='govt'（外郭団体・自治体出資）
  - 重複検出: 既存ロジック（normalize_name）流用
  - metadata.source='prefectural_collection'
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "corporate_research_grants.sqlite"
SRC = ROOT / "data" / "prefectural_organizations.json"


def normalize_name(name: str) -> str:
    """法人格・スペース・全角を除去した正規化名（既存 import_koeki と同方式）"""
    if not name:
        return ""
    s = name
    s = re.sub(r"^(公益|一般|特定非営利活動|特例)?(財団法人|社団法人)?\s*", "", s)
    s = s.replace("　", "").replace(" ", "")
    return s.lower()


def detect_legal_form(name: str) -> str:
    if "公益財団法人" in name:
        return "公益財団法人"
    if "公益社団法人" in name:
        return "公益社団法人"
    if "一般財団法人" in name:
        return "一般財団法人"
    if "一般社団法人" in name:
        return "一般社団法人"
    if "特定非営利活動法人" in name:
        return "特定非営利活動法人"
    return "その他"


def detect_subtype(name: str, description: str = "") -> str:
    """都道府県外郭団体は基本 'govt'、大学・学会系は 'academic'。"""
    n = name + " " + (description or "")
    if any(k in n for k in ["大学", "学会", "学院", "学術振興会"]):
        return "academic"
    if any(k in n for k in ["国際", "アジア", "ユネスコ", "UNESCO"]):
        # 自治体外郭の国際交流系も govt に寄せるが「アジア研究所」等は intl 寄り
        if "都市研究" in n or "産業振興" in n or "国際協力" in n:
            return "govt"
        return "intl"
    # それ以外（産業振興公社・科学技術振興財団・支援センター等）はgovt
    return "govt"


def main():
    parser = argparse.ArgumentParser(
        description="47都道府県+政令指定都市の振興財団・支援機関をCFG DBに統合"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="DB変更を行わず、新規・既存・更新の件数のみ表示")
    parser.add_argument("--db", type=Path, default=DB, help="SQLite DB path")
    parser.add_argument("--src", type=Path, default=SRC, help="JSON source path")
    args = parser.parse_args()

    if not args.src.exists():
        raise SystemExit(f"source JSON not found: {args.src}")

    with open(args.src, encoding="utf-8") as f:
        data = json.load(f)

    orgs = data.get("organizations", [])
    print(f"Source list: {len(orgs)} organizations")

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("SELECT id, name, foundation_subtype, prefecture FROM organizations")
    rows = cur.fetchall()
    existing = {normalize_name(r["name"]): dict(r) for r in rows}
    print(f"Existing organizations in DB: {len(existing)}")

    new_count = 0
    update_count = 0
    skip_count = 0
    pref_set = set()
    new_pref_set = set()

    for r in orgs:
        name = (r.get("name") or "").strip()
        if not name:
            continue
        norm = normalize_name(name)
        prefecture = r.get("prefecture")
        municipality = r.get("municipality")
        url = r.get("url")
        description = r.get("description") or ""
        name_alt = r.get("name_alt")

        legal_form = detect_legal_form(name)
        subtype = detect_subtype(name, description)
        pref_set.add(prefecture)

        metadata = json.dumps({
            "source": "prefectural_collection",
            "name_alt": name_alt,
            "compiled_date": data.get("_meta", {}).get("compiled_date"),
        }, ensure_ascii=False)

        if norm in existing:
            existing_id = existing[norm]["id"]
            if not args.dry_run:
                # 既存レコードの欠損フィールドを補完
                cur.execute(
                    """
                    UPDATE organizations
                    SET prefecture       = COALESCE(prefecture, ?),
                        municipality     = COALESCE(municipality, ?),
                        url              = COALESCE(NULLIF(url, ''), ?),
                        description      = COALESCE(NULLIF(description, ''), ?),
                        legal_form       = COALESCE(legal_form, ?),
                        foundation_subtype = COALESCE(foundation_subtype, ?),
                        updated_at       = datetime('now', 'localtime')
                    WHERE id = ?
                    """,
                    (prefecture, municipality, url, description,
                     legal_form, subtype, existing_id),
                )
            update_count += 1
            continue

        # 新規挿入
        new_id = f"pref_{uuid.uuid4().hex[:12]}"
        new_pref_set.add(prefecture)
        if not args.dry_run:
            cur.execute(
                """
                INSERT INTO organizations
                (id, name, type, foundation_subtype, legal_form, prefecture,
                 municipality, url, description, metadata, country_code,
                 created_at, updated_at)
                VALUES (?, ?, 'foundation', ?, ?, ?, ?, ?, ?, ?, 'JP',
                        datetime('now','localtime'), datetime('now','localtime'))
                """,
                (new_id, name, subtype, legal_form, prefecture,
                 municipality, url, description, metadata),
            )
            existing[norm] = {"id": new_id, "name": name,
                              "foundation_subtype": subtype, "prefecture": prefecture}
        new_count += 1

    if not args.dry_run:
        conn.commit()

    cur.execute("SELECT COUNT(*) FROM organizations")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM organizations WHERE foundation_subtype='govt'")
    govt_total = cur.fetchone()[0]

    print()
    print("=" * 60)
    print(f"  Mode:           {'DRY-RUN' if args.dry_run else 'COMMITTED'}")
    print(f"  Source orgs:    {len(orgs)}")
    print(f"  New inserts:    {new_count}")
    print(f"  Updates:        {update_count}")
    print(f"  Skipped:        {skip_count}")
    print(f"  Prefectures covered: {len(pref_set)} / 47")
    print(f"  Pref list:      {sorted(pref_set)}")
    print(f"  Total orgs:     {total}")
    print(f"  govt subtype:   {govt_total}")
    print("=" * 60)

    conn.close()


if __name__ == "__main__":
    main()
