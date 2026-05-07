#!/usr/bin/env python3
"""
Refine foundation_subtype classification for organizations classified as 'other'.

Strategy:
  Use multiple signal sources in priority order:
    1. metadata.admin (主務官庁) — strongest signal for govt/academic
    2. description (purpose) — pattern matching
    3. name pattern — extended keyword set
    4. parent company existence
"""
from __future__ import annotations
import json, sqlite3
from pathlib import Path

DB = Path("/Users/nishimura+/projects/apps/corporate-foundation-grants/corporate_research_grants.sqlite")


def classify(name: str, description: str, admin: str, parent: str) -> str:
    """Return refined subtype or 'other' if still unknown."""
    n = name or ""
    d = description or ""
    a = admin or ""
    text = f"{n} {d} {a}"

    # 1. Admin signal (主務官庁) is strongest
    if a:
        if any(k in a for k in ["文部科学省", "文科省", "MEXT"]):
            if any(k in n for k in ["奨学", "育英", "学費"]):
                return "academic"
            return "academic"  # 文科省所管は基本学術系
        if any(k in a for k in ["内閣府"]):
            # 内閣府は多種、追加判断
            pass
        if any(k in a for k in ["農林水産省", "経済産業省", "厚生労働省", "国土交通省",
                                  "環境省", "総務省", "外務省", "財務省", "防衛省"]):
            return "govt"

    # 2. Parent company exists
    if parent and parent.strip():
        return "corporate"

    # 3. Name + description patterns (extended)
    # Individual foundation - 創業者・経営者・名士の名前
    individual_keywords = [
        "記念", "奨学", "篤志", "賞", "育英", "翁",
        "夫人", "博士", "先生",
    ]
    if any(k in n for k in individual_keywords):
        return "individual"

    # Academic - 学術系
    academic_keywords = [
        "大学", "学会", "学術振興会", "学院", "学園",
        "教育振興", "学術", "研究振興", "研究所",
        "工学院", "理科振興", "学事", "学園",
    ]
    if any(k in text for k in academic_keywords):
        return "academic"

    # International
    intl_keywords = [
        "国際", "世界", "アジア", "ユネスコ", "UNESCO",
        "アメリカ", "欧州", "韓国", "中国", "日米",
        "日中", "日韓", "国際交流",
    ]
    if any(k in text for k in intl_keywords):
        return "intl"

    # NGO/NPO
    ngo_keywords = [
        "市民", "ボランティア", "市民基金", "コミュニティ",
        "NPO", "非営利",
    ]
    if any(k in text for k in ngo_keywords):
        return "ngo"

    # Industry/corporate (no parent but industry-related)
    corp_keywords = [
        "振興", "技術", "科学", "産業", "工業", "商工",
        "建築", "土木", "鉄道", "海事", "農業", "水産",
        "畜産", "森林", "資源", "金融", "保険", "信託",
        "証券", "銀行", "公庫", "金庫", "電気", "電力",
        "ガス", "通信", "放送", "石油", "鉱業", "製造",
        "化学", "薬品", "繊維", "紙", "印刷", "食品",
        "酒造", "醸造", "観光", "交通", "運輸", "物流",
        "建設", "不動産", "情報", "コンピュータ", "電子",
    ]
    if any(k in text for k in corp_keywords):
        return "corporate"

    # 福祉系
    if any(k in text for k in ["福祉", "厚生", "健康", "医療", "病院", "看護"]):
        if any(k in text for k in ["市民", "コミュニティ"]):
            return "ngo"
        return "corporate"  # 多くは企業系厚生事業団

    # 文化・芸術
    if any(k in text for k in ["文化", "芸術", "音楽", "美術", "演劇", "美術館", "博物館"]):
        return "individual"  # 多くは個人/家族財団

    # 環境
    if any(k in text for k in ["環境", "自然", "緑化", "森林", "河川", "海洋"]):
        return "corporate"

    # 国際スポーツ・スポーツ
    if any(k in text for k in ["スポーツ", "オリンピック", "競技"]):
        return "ngo"

    return "other"


def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("""
        SELECT id, name, description, metadata, corporate_parent, foundation_subtype
        FROM organizations
        WHERE foundation_subtype = 'other' OR foundation_subtype IS NULL
    """)
    rows = cur.fetchall()
    print(f"Other/null subtype: {len(rows)}")

    reclass_counts = {"corporate": 0, "individual": 0, "academic": 0, "intl": 0,
                      "ngo": 0, "govt": 0, "group": 0, "other": 0}

    for rid, name, desc, meta_str, parent, current in rows:
        admin = ""
        if meta_str:
            try:
                meta = json.loads(meta_str)
                admin = meta.get("admin") or ""
            except Exception:
                pass

        new_sub = classify(name, desc, admin, parent)
        reclass_counts[new_sub] = reclass_counts.get(new_sub, 0) + 1
        if new_sub != "other":
            cur.execute(
                "UPDATE organizations SET foundation_subtype=?, updated_at=datetime('now','localtime') WHERE id=?",
                (new_sub, rid),
            )

    conn.commit()

    # Final distribution
    cur.execute("SELECT foundation_subtype, COUNT(*) FROM organizations GROUP BY foundation_subtype ORDER BY 2 DESC")
    print("\nReclassification breakdown (from 'other'):")
    for k, v in reclass_counts.items():
        print(f"  -> {k}: {v}")

    print("\nFinal subtype distribution:")
    for k, v in cur.fetchall():
        print(f"  {k}: {v}")

    conn.close()


if __name__ == "__main__":
    main()
