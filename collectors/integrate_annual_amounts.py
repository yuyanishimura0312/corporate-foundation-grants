#!/usr/bin/env python3
"""
Integrate annual grant amounts from 助成財団センター Top 100 ranking (2022年度)
into the corporate_research_grants database.
Source: https://www.jfc.or.jp/bunseki-top/rank_grant/rank_grant2022/
"""
import sqlite3, json, re
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "corporate_research_grants.sqlite"
OUTPUT_FILE = Path(__file__).parent.parent / "data" / "jfc_top100_amounts.json"

# 2022年度 年間助成額上位100財団 (百万円)
TOP100_DATA = [
    (1, "日本財団", 65619),
    (2, "ジャパン・プラットフォーム", 8660),
    (3, "ＪＫＡ", 6238),
    (4, "日本教育公務員弘済会", 3619),
    (5, "武田科学振興財団", 2708),
    (6, "大阪府育英会", 2703),
    (7, "上原記念生命科学財団", 1463),
    (8, "日本国際教育支援協会", 1241),
    (9, "ロータリー米山記念奨学会", 1222),
    (10, "福岡県共同募金会", 1066),
    (11, "東京都共同募金会", 989),
    (12, "稲盛財団", 987),
    (13, "秋田県育英会", 939),
    (14, "中央共同募金会", 846),
    (15, "北海道さけ・ます増殖事業協会", 824),
    (16, "資本市場振興財団", 788),
    (17, "三菱みらい育成財団", 744),
    (18, "業務スーパージャパンドリーム財団", 741),
    (19, "小野奨学会", 731),
    (20, "トヨタ・モビリティ基金", 716),
    (21, "中谷財団", 712),
    (22, "神戸やまぶき財団", 702),
    (23, "笹川平和財団", 699),
    (24, "日本台湾交流協会", 650),
    (25, "むつ小川原地域・産業振興財団", 640),
    (26, "三菱財団", 608),
    (27, "交通遺児育英会", 598),
    (28, "大阪府共同募金会", 590),
    (29, "内藤記念科学振興財団", 583),
    (30, "村田学術振興・教育財団", 551),
    (31, "セコム科学技術振興財団", 547),
    (32, "中央競馬馬主社会福祉財団", 534),
    (33, "全国こども食堂支援センター・むすびえ", 524),
    (34, "上田記念財団", 523),
    (35, "高橋産業経済研究財団", 521),
    (36, "飯塚毅育英会", 518),
    (37, "岩手県市町村振興協会", 514),
    (38, "旭硝子財団", 499),
    (39, "鹿児島県育英財団", 488),
    (40, "中村積善会", 481),
    (41, "静岡県共同募金会", 472),
    (42, "石橋財団", 470),
    (43, "市村清新技術財団", 462),
    (44, "持田記念医学薬学振興財団", 446),
    (45, "電通育英会", 427),
    (46, "発酵研究所", 418),
    (47, "住友財団", 409),
    (48, "清水基金", 382),
    (49, "新潟県共同募金会", 382),
    (50, "小林財団", 375),
    (51, "中外創薬科学財団", 373),
    (52, "ヒロセ財団", 373),
    (53, "テルモ生命科学振興財団", 371),
    (54, "トヨタ財団", 367),
    (55, "朝鮮奨学会", 362),
    (56, "喫煙科学研究財団", 362),
    (57, "わかやま産業振興財団", 354),
    (58, "博報堂教育財団", 352),
    (59, "パブリックリソース財団", 349),
    (60, "髙山国際教育財団", 336),
    (61, "小笠原敏晶記念財団", 326),
    (62, "とくしま産業振興機構", 325),
    (63, "ローム ミュージック ファンデーション", 314),
    (64, "船井情報科学振興財団", 307),
    (65, "似鳥国際奨学財団", 300),
    (66, "山口県共同募金会", 300),
    (67, "キヤノン財団", 290),
    (68, "宮城県共同募金会", 289),
    (69, "岡山県共同募金会", 285),
    (70, "企業メセナ協議会", 278),
    (71, "天田財団", 273),
    (72, "平和中島財団", 272),
    (73, "化学及血清療法研究所", 268),
    (74, "鉄道弘済会", 265),
    (75, "古岡奨学会", 260),
    (76, "島根県育英会", 241),
    (77, "日本生命財団", 229),
    (78, "飯島藤十郎記念食品科学振興財団", 225),
    (79, "小田急財団", 223),
    (80, "岡田甲子男記念奨学財団", 217),
    (81, "中島記念国際交流財団", 216),
    (82, "Ｇ－７奨学財団", 214),
    (83, "日本科学協会", 209),
    (84, "島根県市町村振興協会", 206),
    (85, "篠原欣子記念財団", 205),
    (86, "日揮・実吉奨学会", 204),
    (87, "池谷科学技術振興財団", 203),
    (88, "本庄国際奨学財団", 202),
    (89, "大塚敏美育英奨学財団", 198),
    (90, "三菱UFJ信託奨学財団", 194),
    (91, "スズキ財団", 194),
    (92, "栃木県育英会", 194),
    (93, "電気通信普及財団", 192),
    (94, "国土緑化推進機構", 190),
    (95, "車両競技公益資金記念財団", 188),
    (96, "岩谷直治記念財団", 188),
    (97, "立石科学技術振興財団", 184),
    (98, "海外子女教育振興財団", 179),
    (99, "公益推進協会", 177),
    (100, "野村財団", 177),
]


def match_foundation(top100_name, db_orgs):
    """Fuzzy match a Top 100 name to our DB organizations."""
    # Clean the name
    clean = top100_name.replace("公益財団法人", "").replace("一般財団法人", "")
    clean = clean.replace("社会福祉法人", "").replace("特定非営利活動法人", "")
    clean = clean.replace("公益社団法人", "").strip()

    for org_id, org_name in db_orgs:
        org_clean = org_name.replace("公益財団法人", "").replace("（公財）", "")
        org_clean = org_clean.replace("一般財団法人", "").strip()

        if clean == org_clean:
            return org_id, org_name
        if clean in org_clean or org_clean in clean:
            return org_id, org_name
        # Partial match (first 4+ chars)
        if len(clean) > 4 and len(org_clean) > 4:
            if clean[:5] in org_clean or org_clean[:5] in clean:
                return org_id, org_name

    return None, None


def main():
    db = sqlite3.connect(str(DB_PATH))

    # Add annual_grant_amount column if not exists
    try:
        db.execute("ALTER TABLE organizations ADD COLUMN annual_grant_amount INTEGER")
        db.execute("ALTER TABLE organizations ADD COLUMN annual_grant_year TEXT")
        db.execute("ALTER TABLE organizations ADD COLUMN jfc_rank INTEGER")
        print("Added annual_grant_amount columns")
    except:
        pass  # Columns already exist

    # Get all orgs
    db_orgs = db.execute("SELECT id, name FROM organizations").fetchall()

    matched = []
    unmatched_corporate = []
    total_amount_matched = 0

    for rank, name, amount_million in TOP100_DATA:
        amount_yen = amount_million * 1_000_000
        org_id, org_name = match_foundation(name, db_orgs)

        if org_id:
            db.execute("""
                UPDATE organizations
                SET annual_grant_amount = ?, annual_grant_year = '2022', jfc_rank = ?
                WHERE id = ?
            """, (amount_yen, rank, org_id))
            matched.append((rank, name, org_name, amount_yen))
            total_amount_matched += amount_yen
        else:
            # Check if this is a corporate foundation we should add
            unmatched_corporate.append((rank, name, amount_yen))

    db.commit()

    # Save full data
    output = {
        "source": "助成財団センター 年間助成額上位100財団 2022年度",
        "url": "https://www.jfc.or.jp/bunseki-top/rank_grant/rank_grant2022/",
        "matched": [
            {"rank": r, "top100_name": n, "db_name": dn, "amount": a}
            for r, n, dn, a in matched
        ],
        "unmatched": [
            {"rank": r, "name": n, "amount": a}
            for r, n, a in unmatched_corporate
        ],
    }
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    # Summary
    print(f"\n=== JFC Top 100 Integration ===")
    print(f"Top 100 total: {sum(a for _,_,a in TOP100_DATA) * 1_000_000 / 100_000_000:.1f}億円")
    print(f"Matched to our DB: {len(matched)} foundations")
    print(f"Matched amount: {total_amount_matched / 100_000_000:.1f}億円")

    print(f"\n--- Matched foundations ---")
    for rank, name, db_name, amount in sorted(matched, key=lambda x: -x[3]):
        print(f"  #{rank:3d} {name}: {amount/100_000_000:.1f}億円 (→ {db_name})")

    # Calculate new coverage
    print(f"\n--- Coverage update ---")
    # Our total: program-level amounts + annual amounts
    prog_total = db.execute("""
        SELECT SUM(grant_amount_max) FROM grant_calls
        WHERE grant_amount_max > 0 AND grant_amount_max < 10000000000
    """).fetchone()[0] or 0

    orgs_with_annual = db.execute("""
        SELECT COUNT(*), SUM(annual_grant_amount) FROM organizations
        WHERE annual_grant_amount IS NOT NULL
    """).fetchone()

    print(f"  Program-level amounts: {prog_total/100_000_000:.1f}億円 (per-call max)")
    print(f"  Annual amounts (JFC): {orgs_with_annual[1]/100_000_000:.1f}億円 ({orgs_with_annual[0]} foundations)")
    print(f"  Combined: ~{orgs_with_annual[1]/100_000_000:.1f}億円 annual (more reliable)")
    print(f"  vs Market estimate: 200-300億円")
    print(f"  New coverage: {orgs_with_annual[1]/200_0000_0000*100:.0f}〜{orgs_with_annual[1]/300_0000_0000*100:.0f}%")

    db.close()


if __name__ == "__main__":
    main()
