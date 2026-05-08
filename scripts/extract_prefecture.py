#!/usr/bin/env python3
"""Extract prefecture from contact_address for organizations missing prefecture."""
from __future__ import annotations
import sqlite3
from pathlib import Path

DB = Path("/Users/nishimura+/projects/apps/corporate-foundation-grants/corporate_research_grants.sqlite")

PREFECTURES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

# Common alternate forms (city name without prefecture)
CITY_TO_PREF = {
    "札幌": "北海道", "仙台": "宮城県", "新宿": "東京都", "渋谷": "東京都",
    "千代田": "東京都", "中央区": "東京都",  # ambiguous, default Tokyo
    "横浜": "神奈川県", "川崎": "神奈川県", "千葉市": "千葉県",
    "さいたま": "埼玉県", "名古屋": "愛知県", "京都市": "京都府",
    "大阪市": "大阪府", "神戸": "兵庫県", "福岡市": "福岡県",
    "北九州": "福岡県", "広島市": "広島県", "岡山市": "岡山県",
    "新潟市": "新潟県", "金沢": "石川県", "静岡市": "静岡県",
    "浜松": "静岡県",
}


def extract(addr: str) -> str | None:
    if not addr:
        return None
    for pref in PREFECTURES:
        if pref in addr:
            return pref
    # City fallback
    for city, pref in CITY_TO_PREF.items():
        if city in addr:
            return pref
    return None


def main():
    conn = sqlite3.connect(DB, timeout=60)
    conn.execute("PRAGMA busy_timeout = 60000")
    cur = conn.cursor()

    cur.execute("""SELECT id, name, contact_address FROM organizations
                   WHERE (prefecture IS NULL OR prefecture = '')
                     AND contact_address IS NOT NULL AND contact_address != ''""")
    rows = cur.fetchall()
    print(f"Targets: {len(rows)} (with address but no prefecture)")

    updated = 0
    no_match = 0
    for rid, name, addr in rows:
        pref = extract(addr)
        if pref:
            cur.execute(
                "UPDATE organizations SET prefecture = ?, updated_at=datetime('now','localtime') WHERE id = ?",
                (pref, rid),
            )
            updated += 1
        else:
            no_match += 1

    conn.commit()

    # Also try extracting from name (e.g., "東京都中小企業振興公社")
    cur.execute("""SELECT id, name FROM organizations
                   WHERE prefecture IS NULL OR prefecture = ''""")
    name_rows = cur.fetchall()
    name_updated = 0
    for rid, name in name_rows:
        pref = extract(name)
        if pref:
            cur.execute(
                "UPDATE organizations SET prefecture = ?, updated_at=datetime('now','localtime') WHERE id = ?",
                (pref, rid),
            )
            name_updated += 1
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM organizations WHERE prefecture IS NOT NULL AND prefecture != ''")
    total_with_pref = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM organizations")
    total = cur.fetchone()[0]
    print(f"\nFrom address: updated={updated}, no_match={no_match}")
    print(f"From name: updated={name_updated}")
    print(f"Total with prefecture: {total_with_pref}/{total} ({total_with_pref/total*100:.1f}%)")
    conn.close()


if __name__ == "__main__":
    main()
