#!/usr/bin/env python3
"""
Import koeki_research_foundations.json into organizations table.

Strategy:
- Filter score >= 3 (1,073 candidates)
- Dedupe against existing organizations by normalized name
- Auto-classify foundation_subtype using heuristics
- Assign legal_form from name prefix
- Extract prefecture from address
- Mark imported records with metadata.source='koeki_info'
"""
from __future__ import annotations

import json
import re
import sqlite3
import uuid
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "corporate_research_grants.sqlite"
KOEKI = ROOT / "data" / "koeki_research_foundations.json"

PREFECTURES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]


def normalize_name(name: str) -> str:
    """法人格・スペース・全角を除去した正規化名"""
    if not name:
        return ""
    s = name
    s = re.sub(r"^(公益|一般|特定非営利活動|特例)?(財団法人|社団法人)?\s*", "", s)
    s = s.replace("　", "").replace(" ", "")
    return s.lower()


def detect_legal_form(name: str) -> Optional[str]:
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


def detect_subtype(name: str, admin: str) -> str:
    n = name + " " + (admin or "")
    if any(k in n for k in ["国際", "世界", "アジア", "ユネスコ", "UNESCO"]):
        return "intl"
    if any(k in n for k in ["大学", "学会", "学術振興会", "学院"]):
        return "academic"
    if any(k in n for k in ["記念", "奨学", "篤志", "賞"]):
        return "individual"
    if any(k in n for k in ["市民", "ボランティア", "市民基金"]):
        return "ngo"
    if any(k in n for k in ["振興", "技術", "科学", "産業"]):
        return "corporate"
    return "other"


def detect_prefecture(address: str) -> Optional[str]:
    if not address:
        return None
    for pref in PREFECTURES:
        if address.startswith(pref):
            return pref
    return None


def main():
    with open(KOEKI) as f:
        data = json.load(f)

    candidates = [r for r in data if (r.get("research_score") or 0) >= 3]
    print(f"Total koeki: {len(data)}")
    print(f"Score >= 3: {len(candidates)}")

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("SELECT id, name FROM organizations")
    existing = {normalize_name(r["name"]): r["id"] for r in cur.fetchall()}
    print(f"Existing organizations: {len(existing)}")

    new_count = 0
    skip_count = 0
    update_count = 0

    for r in candidates:
        name = (r.get("name") or "").strip()
        if not name:
            continue
        norm = normalize_name(name)
        admin = r.get("admin") or ""
        address = r.get("address") or ""
        score = r.get("research_score")
        purpose = r.get("purpose") or ""

        legal_form = detect_legal_form(name)
        subtype = detect_subtype(name, admin)
        prefecture = detect_prefecture(address)

        if norm in existing:
            # Update existing record with koeki info if missing
            cur.execute(
                """
                UPDATE organizations
                SET prefecture = COALESCE(prefecture, ?),
                    description = COALESCE(NULLIF(description,''), ?),
                    legal_form = COALESCE(legal_form, ?),
                    foundation_subtype = COALESCE(foundation_subtype, ?),
                    contact_address = COALESCE(contact_address, ?),
                    updated_at = datetime('now','localtime')
                WHERE id = ?
                """,
                (prefecture, purpose, legal_form, subtype, address, existing[norm]),
            )
            update_count += 1
            continue

        new_id = str(uuid.uuid4())
        metadata = json.dumps({
            "source": "koeki_info",
            "research_score": score,
            "admin": admin,
        }, ensure_ascii=False)

        cur.execute(
            """
            INSERT INTO organizations
            (id, name, type, foundation_subtype, legal_form, prefecture,
             description, contact_address, metadata, country_code,
             created_at, updated_at)
            VALUES (?, ?, 'foundation', ?, ?, ?, ?, ?, ?, 'JP',
                    datetime('now','localtime'), datetime('now','localtime'))
            """,
            (new_id, name, subtype, legal_form, prefecture, purpose, address, metadata),
        )
        new_count += 1
        existing[norm] = new_id

    conn.commit()
    cur.execute("SELECT COUNT(*) FROM organizations")
    total = cur.fetchone()[0]
    print(f"\nImport complete:")
    print(f"  New: {new_count}")
    print(f"  Updated: {update_count}")
    print(f"  Skipped: {skip_count}")
    print(f"  Total organizations now: {total}")
    conn.close()


if __name__ == "__main__":
    main()
