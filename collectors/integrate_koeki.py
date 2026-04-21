#!/usr/bin/env python3
"""
Integrate newly discovered corporate research foundations from koeki-info.go.jp
into the corporate_research_grants database.
"""
import sqlite3, json, uuid
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "corporate_research_grants.sqlite"
KOEKI_DATA = Path(__file__).parent.parent / "data" / "koeki_research_foundations.json"

# Existing DB orgs for dedup
def get_existing_names(db):
    rows = db.execute("SELECT name FROM organizations").fetchall()
    names = set()
    for r in rows:
        n = r[0].replace("公益財団法人", "").replace("（公財）", "").replace("一般財団法人", "").strip()
        names.add(n)
        # Also add shorter variants
        if len(n) > 4:
            names.add(n[:6])
    return names


def is_duplicate(name, existing_names):
    clean = name.replace("公益財団法人", "").strip()
    if clean in existing_names:
        return True
    if len(clean) > 4 and clean[:6] in existing_names:
        return True
    # Check reverse
    for ex in existing_names:
        if len(ex) > 4 and (ex in clean or clean in ex):
            return True
    return False


PARENT_MAP = {
    "コニカミノルタ": "コニカミノルタ", "三井": "三井グループ",
    "中谷": "中谷医工計測技術振興", "大塚": "大塚グループ",
    "みずほ": "みずほFG", "カシオ": "カシオ計算機",
    "住友電工": "住友電気工業", "村田": "村田製作所",
    "アサヒ": "アサヒグループHD", "加藤": None,
    "岩谷": "岩谷産業", "日立": "日立製作所",
    "旭化成": "旭化成", "東レ": "東レ",
    "豊田": "トヨタグループ", "ソニー": "ソニーグループ",
    "ローム": "ローム", "古河": "古河電気工業",
    "島津": "島津製作所", "NEC": "日本電気",
    "オムロン": "オムロン", "ダイキン": "ダイキン工業",
    "ファナック": "ファナック", "ブリヂストン": "ブリヂストン",
    "デンソー": "デンソー", "TOTO": "TOTO",
    "LIXIL": "LIXIL", "資生堂": "資生堂",
    "富士フイルム": "富士フイルムHD", "日本ペイント": "日本ペイントHD",
    "帝人": "帝人", "信越": "信越化学工業",
    "三菱ケミカル": "三菱ケミカルグループ", "三菱重工": "三菱重工業",
    "川崎重工": "川崎重工業", "日本製鉄": "日本製鉄",
    "JFE": "JFEホールディングス", "神戸製鋼": "神戸製鋼所",
    "住友": "住友グループ", "三菱": "三菱グループ",
    "ENEOS": "ENEOSホールディングス", "NTT": "日本電信電話",
    "KDDI": "KDDI", "ソフトバンク": "ソフトバンクグループ",
    "楽天": "楽天グループ", "リクルート": "リクルートHD",
    "ニコン": "ニコン", "オリンパス": "オリンパス",
    "ヤマハ": "ヤマハ", "キーエンス": "キーエンス",
    "野村": "野村グループ", "大和": "大和証券グループ",
}


def infer_parent(name):
    for kw, parent in PARENT_MAP.items():
        if kw in name:
            return parent
    return None


def main():
    with open(KOEKI_DATA) as f:
        koeki = json.load(f)

    corporate = [f for f in koeki if f.get("is_corporate")]

    db = sqlite3.connect(str(DB_PATH))
    existing_names = get_existing_names(db)

    new_foundations = []
    for f in corporate:
        if not is_duplicate(f["name"], existing_names):
            new_foundations.append(f)

    print(f"Corporate research foundations from koeki-info: {len(corporate)}")
    print(f"After dedup: {len(new_foundations)} new foundations to add")

    added = 0
    for f in new_foundations:
        org_id = f"koeki_{uuid.uuid4().hex[:12]}"
        name = f["name"]
        parent = infer_parent(name)
        admin = f.get("admin", "")
        address = f.get("address", "")
        purpose = f.get("purpose", "")

        # Determine prefecture from address
        prefecture = None
        if address:
            for pref in ["北海道","青森","岩手","宮城","秋田","山形","福島",
                         "茨城","栃木","群馬","埼玉","千葉","東京","神奈川",
                         "新潟","富山","石川","福井","山梨","長野","岐阜",
                         "静岡","愛知","三重","滋賀","京都","大阪","兵庫",
                         "奈良","和歌山","鳥取","島根","岡山","広島","山口",
                         "徳島","香川","愛媛","高知","福岡","佐賀","長崎",
                         "熊本","大分","宮崎","鹿島","沖縄"]:
                if pref in address:
                    prefecture = pref + ("都" if pref=="東京" else "府" if pref in ["京都","大阪"] else "道" if pref=="北海道" else "県")
                    break

        db.execute("""
            INSERT INTO organizations (id, name, type, corporate_parent, prefecture, description, contact_address, metadata, created_at, updated_at)
            VALUES (?, ?, 'foundation', ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
        """, (
            org_id, name, parent, prefecture, purpose, address,
            json.dumps({"source": "koeki-info.go.jp", "admin_agency": admin, "research_score": f.get("research_score", 0)}, ensure_ascii=False)
        ))
        added += 1

    db.commit()

    # Verify
    total = db.execute("SELECT COUNT(*) FROM organizations").fetchone()[0]
    print(f"\nDatabase updated: {total} total organizations ({added} added)")

    # Show summary
    print(f"\n--- Newly added foundations ---")
    for f in new_foundations[:15]:
        parent = infer_parent(f["name"])
        print(f"  {f['name']} ({parent or '親企業不明'}) [{f.get('admin','')}]")

    db.close()


if __name__ == "__main__":
    main()
