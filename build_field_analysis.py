#!/usr/bin/env python3
"""
Cross-reference corporate foundation grants with Academic Knowledge DB.
Maps each grant to Level 1 fields and Level 2 subfields using keyword matching.
"""
import sqlite3, json, re
from pathlib import Path

GRANT_DB = Path.home() / "projects/research/corporate-foundation-grants/corporate_research_grants.sqlite"
ACAD_DB = Path.home() / "projects/research/academic-knowledge-db/academic.db"
OUT = Path(__file__).parent / "field_analysis.json"


def q(db, sql):
    return [dict(r) for r in db.execute(sql).fetchall()]


# Level 2 subfield → keywords mapping (manually curated from survey_frame)
# Each entry: (domain, L1 parent, L2 subfield name, search keywords)
SUBFIELD_KEYWORDS = [
    # === Natural Science (自然科学) ===
    # Biology
    ("natural_discovery", "生物学", "分子生物学・ゲノミクス", ["分子","ゲノム","遺伝子","DNA","RNA","遺伝","バイオ","生命"]),
    ("natural_discovery", "生物学", "細胞生物学", ["細胞","幹細胞","iPS"]),
    ("natural_discovery", "生物学", "免疫学", ["免疫","感染","ウイルス","ワクチン","感染症"]),
    ("natural_discovery", "生物学", "神経科学", ["神経","脳","ニューロ"]),
    ("natural_discovery", "生物学", "進化生物学", ["進化生物","系統分類","進化論"]),
    # Ecology
    ("natural_discovery", "生態学", "生態系生態学", ["生態系","生態"]),
    ("natural_discovery", "生態学", "保全生態学", ["保全","生物多様性","絶滅"]),
    ("natural_discovery", "生態学", "進化生態学", ["進化生態"]),
    # Chemistry
    ("natural_discovery", "化学", "化学（一般）", ["化学","触媒","合成","有機","無機","高分子"]),
    # Physics
    ("natural_discovery", "物理学", "物理学（一般）", ["物理","量子","素粒子","光学","レーザー"]),
    ("natural_discovery", "物理学", "凝縮系物理学", ["材料","結晶","半導体","超伝導"]),
    # Earth Science
    ("natural_discovery", "地球科学", "気候科学", ["気候","温暖化","大気","気象"]),
    ("natural_discovery", "地球科学", "地質学・構造地質学", ["地震","火山","地質","地球"]),
    # Astronomy
    ("natural_discovery", "天文学・宇宙物理学", "天文学（一般）", ["天文","宇宙","銀河","天体"]),
    # Mathematics
    ("natural_discovery", "数学", "数学（一般）", ["数学","統計","データサイエンス","アルゴリズム"]),
    # Medicine (not in academic DB but major grant category)
    ("natural_discovery", "医学", "臨床医学", ["臨床","患者","治療","診断","医療","病","疾患","症"]),
    ("natural_discovery", "医学", "基礎医学", ["医学","病態","病理","解剖"]),
    ("natural_discovery", "医学", "薬学", ["薬","創薬","薬理","製薬"]),
    ("natural_discovery", "医学", "公衆衛生", ["公衆衛生","健康","予防","疫学","保健"]),
    ("natural_discovery", "医学", "がん研究", ["がん","腫瘍","癌","オンコロジー"]),
    # Food/Agriculture (applied natural science)
    ("natural_discovery", "農学・食品科学", "食品科学", ["食品","食","栄養","食文化","食育"]),
    ("natural_discovery", "農学・食品科学", "農学", ["農","農業","植物","作物","園芸"]),
    # Environmental science
    ("natural_discovery", "環境科学", "環境科学（一般）", ["環境","汚染","生態","リサイクル","廃棄物","海洋","水質"]),
    ("natural_discovery", "環境科学", "エネルギー", ["エネルギー","再生可能","太陽光","風力","蓄電"]),

    # === Social Science (社会科学) ===
    # Psychology
    ("social_theory", "心理学", "臨床・異常心理学", ["臨床心理","カウンセリング","精神","メンタル"]),
    ("social_theory", "心理学", "発達心理学", ["発達","児童心理","青年"]),
    ("social_theory", "心理学", "社会心理学", ["社会心理"]),
    ("social_theory", "心理学", "認知心理学", ["認知","知覚","記憶","注意"]),
    ("social_theory", "心理学", "健康心理学", ["健康心理","ウェルビーイング","well-being"]),
    # Sociology
    ("social_theory", "社会学", "社会構造・階層論", ["格差","貧困","階層","不平等","社会構造"]),
    ("social_theory", "社会学", "文化社会学", ["文化社会","サブカルチャー"]),
    ("social_theory", "社会学", "組織社会学", ["組織","ガバナンス","経営","マネジメント"]),
    ("social_theory", "社会学", "ネットワーク社会学", ["ネットワーク","ソーシャルキャピタル"]),
    ("social_theory", "社会学", "科学・技術社会学 (STS)", ["STS","科学技術社会"]),
    # Economics
    ("social_theory", "経済学", "行動経済学", ["行動経済"]),
    ("social_theory", "経済学", "開発経済学", ["開発","途上国","ODA","国際協力"]),
    ("social_theory", "経済学", "制度経済学", ["制度","規制","市場"]),
    # Political Science
    ("social_theory", "政治学", "政治学（一般）", ["政治","政策","行政","自治体","法制度","規制","立法","条例","地方自治","公共政策"]),
    # Education
    ("social_theory", "教育学", "教育学（一般）", ["教育","学習","授業","カリキュラム","学校","教員","教師"]),
    ("social_theory", "教育学", "青少年教育", ["青少年","子ども","こども","児童","育成","次世代"]),
    ("social_theory", "教育学", "生涯学習", ["生涯学習","リカレント","成人教育"]),
    # Welfare / Social Work
    ("social_theory", "社会福祉学", "高齢者福祉", ["高齢","介護","老人","シニア","認知症"]),
    ("social_theory", "社会福祉学", "障害者福祉", ["障害","障がい","バリアフリー","インクルーシブ"]),
    ("social_theory", "社会福祉学", "児童福祉", ["児童福祉","虐待","養護","里親"]),
    ("social_theory", "社会福祉学", "地域福祉", ["福祉","社会福祉","共生","ボランティア","NPO","市民"]),
    # International / Area Studies
    ("social_theory", "国際関係", "国際関係（一般）", ["国際","外国","海外","留学","グローバル"]),
    ("social_theory", "国際関係", "平和・紛争研究", ["平和","紛争","難民","人権"]),
    ("social_theory", "国際関係", "国際開発", ["開発援助","JICA","アジア","アフリカ","途上"]),
    # Regional Studies
    ("social_theory", "地域研究", "地域研究（一般）", ["地域","まちづくり","地方","コミュニティ","復興","防災","減災"]),
    # Communication
    ("social_theory", "コミュニケーション論", "コミュニケーション（一般）", ["メディア","ジャーナリズム","広報","情報発信"]),
    # Sports
    ("social_theory", "スポーツ科学", "スポーツ科学（一般）", ["スポーツ","運動","体育","アスリート","オリンピック","パラリンピック","自転車","モーターサイクル"]),
    # Gender
    ("social_theory", "ジェンダー研究", "ジェンダー研究（一般）", ["ジェンダー","女性","男女","LGBTQ"]),
    # SDGs / Sustainability
    ("social_theory", "持続可能性研究", "持続可能性（一般）", ["SDGs","持続可能","サステナビリティ","ESG"]),

    # === Engineering (工学) ===
    ("engineering_method", "情報工学・コンピュータ科学", "機械学習・AI", ["AI","人工知能","機械学習","深層学習","ディープラーニング"]),
    ("engineering_method", "情報工学・コンピュータ科学", "情報工学（一般）", ["情報","デジタル","IT","ICT","コンピュータ","データ","DX"]),
    ("engineering_method", "情報工学・コンピュータ科学", "セキュリティ・暗号理論", ["セキュリティ","暗号","サイバー"]),
    ("engineering_method", "情報工学・コンピュータ科学", "ネットワーク・分散システム", ["ネットワーク","通信","IoT","5G","電気通信"]),
    ("engineering_method", "機械工学", "機械工学（一般）", ["機械","ロボット","自動車","モビリティ","製造","ものづくり"]),
    ("engineering_method", "建築・土木工学", "建築・土木（一般）", ["建設","建築","土木","インフラ","道路","橋","交通"]),
    ("engineering_method", "電気・電子工学", "電気・電子（一般）", ["電気","電子","半導体","回路","電力"]),
    ("engineering_method", "化学工学", "化学工学（一般）", ["化学工学","プロセス","粉体","セラミック"]),
    ("engineering_method", "環境・エネルギー工学", "環境工学（一般）", ["環境技術","省エネ","脱炭素","カーボン"]),
    ("engineering_method", "バイオエンジニアリング", "バイオエンジニアリング（一般）", ["バイオテクノロジー","遺伝子工学","バイオ医薬"]),
    ("engineering_method", "システム工学・制御工学", "ロボティクス・自律システム", ["ロボティクス","自動運転","自律"]),
    ("engineering_method", "材料工学", "材料工学（一般）", ["材料","合金","軽金属","鉄鋼","金属","ナノ"]),
    ("engineering_method", "安全工学", "安全工学（一般）", ["安全工学","防犯","サイバーセキュリティ","事故防止","安全技術"]),
    # Innovation (applied)
    ("engineering_method", "イノベーション", "技術革新（一般）", ["イノベーション","技術開発","新技術","産業技術","技術移転","技術革新","ベンチャー"]),

    # === Humanities (人文学) ===
    ("humanities_concept", "文化人類学", "文化人類学（一般）", ["人類学","民族","エスノグラフィ","文化人類"]),
    ("humanities_concept", "哲学", "哲学（一般）", ["哲学","倫理","思想","形而上"]),
    ("humanities_concept", "歴史学", "歴史学（一般）", ["歴史","史","考古","文献","アーカイブ","遺跡"]),
    ("humanities_concept", "言語学", "言語学（一般）", ["言語","言語学","翻訳","通訳"]),
    ("humanities_concept", "文学・文芸批評", "文学（一般）", ["文学","文芸","小説","詩"]),
    ("humanities_concept", "宗教学・宗教哲学", "宗教学（一般）", ["宗教","仏教","キリスト","イスラム","神道"]),
    ("humanities_concept", "美学・芸術哲学", "美学（一般）", ["美学","美術","芸術"]),
    ("humanities_concept", "文化研究", "文化研究（一般）", ["文化活動","伝統文化","民俗","文化遺産","文化財","異文化","多文化","文化交流","文化振興"]),

    # === Arts (芸術) ===
    ("arts_question", "音楽", "音楽（一般）", ["音楽","演奏","作曲","オーケストラ","コンサート"]),
    ("arts_question", "美術・視覚芸術", "美術（一般）", ["美術","絵画","彫刻","造形"]),
    ("arts_question", "デザイン", "デザイン（一般）", ["デザイン","クリエイティブ"]),
    ("arts_question", "演劇・パフォーマンス", "演劇（一般）", ["演劇","劇","パフォーマンス","舞台"]),
    ("arts_question", "映像・映画", "映像（一般）", ["映像","映画","アニメーション","ゲーム"]),
    ("arts_question", "メディアアート・デジタル表現", "メディアアート（一般）", ["メディアアート","インスタレーション"]),
]

DOMAIN_NAMES = {
    "natural_discovery": "自然科学",
    "social_theory": "社会科学",
    "engineering_method": "工学",
    "humanities_concept": "人文学",
    "arts_question": "芸術",
}


def classify_grant(grant):
    """Classify a grant into domains, L1 fields, and L2 subfields."""
    text = " ".join(filter(None, [
        grant.get("foundation", ""), grant.get("program", ""),
        grant.get("title", ""), grant.get("prog_desc", ""),
        grant.get("purpose", ""), grant.get("summary", ""),
        grant.get("keywords", ""), grant.get("subcategories", ""),
    ]))

    matches = []  # (domain, l1_field, l2_subfield, score)
    for domain, l1, l2, keywords in SUBFIELD_KEYWORDS:
        score = sum(1 for kw in keywords if kw in text)
        if score > 0:
            matches.append((domain, l1, l2, score))

    if not matches:
        return {
            "domains": ["unclassified"],
            "domain_names": ["未分類"],
            "l1_fields": [],
            "l2_subfields": [],
            "matches": [],
        }

    domains = {}
    l1_fields = {}
    l2_subfields = {}
    for domain, l1, l2, score in matches:
        domains[domain] = domains.get(domain, 0) + score
        l1_fields[l1] = l1_fields.get(l1, 0) + score
        l2_subfields[l2] = l2_subfields.get(l2, 0) + score

    return {
        "domains": list(domains.keys()),
        "domain_names": [DOMAIN_NAMES.get(d, d) for d in domains],
        "l1_fields": sorted(l1_fields.keys(), key=lambda k: -l1_fields[k]),
        "l2_subfields": sorted(l2_subfields.keys(), key=lambda k: -l2_subfields[k]),
        "matches": sorted(matches, key=lambda m: -m[3]),
    }


def build():
    grant_db = sqlite3.connect(str(GRANT_DB))
    grant_db.row_factory = sqlite3.Row

    # === Layer 1: Program-level classification (258 programs) ===
    grants = q(grant_db, """
        SELECT o.name as foundation, o.corporate_parent,
            gp.name as program, gp.category, gp.description as prog_desc,
            gp.purpose, gp.subcategories,
            gc.title, gc.summary, gc.keywords,
            gc.grant_amount_max, gc.grant_amount_min,
            gc.application_deadline, gc.status, gc.fiscal_year
        FROM organizations o
        JOIN grant_programs gp ON gp.organization_id = o.id
        JOIN grant_calls gc ON gc.program_id = gp.id
    """)

    classified = []
    for g in grants:
        c = classify_grant(g)
        classified.append({**g, **c})

    # === Layer 2: Foundation-level classification (all 189) ===
    # Classify foundations that have no programs using their description
    all_orgs = q(grant_db, """
        SELECT o.name as foundation, o.corporate_parent,
            o.description as prog_desc, o.annual_grant_amount,
            o.jfc_rank, o.annual_grant_year,
            (SELECT COUNT(*) FROM grant_programs gp WHERE gp.organization_id = o.id) as program_count
        FROM organizations o
    """)

    classified_orgs = []
    for org in all_orgs:
        # Build text for classification from org description
        fake_grant = {
            "foundation": org["foundation"],
            "program": "",
            "title": "",
            "prog_desc": org.get("prog_desc") or "",
            "purpose": "",
            "summary": "",
            "keywords": "",
            "subcategories": "",
        }
        c = classify_grant(fake_grant)
        classified_orgs.append({**org, **c})

    # === Aggregations using ANNUAL AMOUNTS (primary) ===
    def aggregate_stats(items, amount_key="annual_grant_amount"):
        domain_stats = {}
        l1_stats = {}
        l2_stats = {}

        for g in items:
            amt = g.get(amount_key) or 0
            if amt < 0 or amt > 100_000_000_000:
                amt = 0

            for d in g["domains"]:
                if d not in domain_stats:
                    domain_stats[d] = {"count": 0, "total_amount": 0, "amount_count": 0}
                domain_stats[d]["count"] += 1
                if amt > 0:
                    domain_stats[d]["total_amount"] += amt
                    domain_stats[d]["amount_count"] += 1

            for f in g.get("l1_fields", []):
                if f not in l1_stats:
                    dom = "unknown"
                    for m in g.get("matches", []):
                        if m[1] == f:
                            dom = m[0]
                            break
                    l1_stats[f] = {"count": 0, "total_amount": 0, "amount_count": 0, "domain": dom}
                l1_stats[f]["count"] += 1
                if amt > 0:
                    l1_stats[f]["total_amount"] += amt
                    l1_stats[f]["amount_count"] += 1

            for sf in g.get("l2_subfields", []):
                if sf not in l2_stats:
                    dom = "unknown"
                    l1 = "unknown"
                    for m in g.get("matches", []):
                        if m[2] == sf:
                            dom = m[0]
                            l1 = m[1]
                            break
                    l2_stats[sf] = {"count": 0, "total_amount": 0, "amount_count": 0, "domain": dom, "l1": l1}
                l2_stats[sf]["count"] += 1
                if amt > 0:
                    l2_stats[sf]["total_amount"] += amt
                    l2_stats[sf]["amount_count"] += 1

        return domain_stats, l1_stats, l2_stats

    # Foundation-level stats (annual amounts)
    org_domain_stats, org_l1_stats, org_l2_stats = aggregate_stats(classified_orgs, "annual_grant_amount")

    # Program-level stats (per-call amounts, for program count)
    prog_domain_stats, prog_l1_stats, prog_l2_stats = aggregate_stats(classified, "grant_amount_max")

    # === Build output ===
    domain_distribution = []
    for d in ["natural_discovery", "social_theory", "engineering_method", "humanities_concept", "arts_question", "unclassified"]:
        os = org_domain_stats.get(d, {"count": 0, "total_amount": 0, "amount_count": 0})
        ps = prog_domain_stats.get(d, {"count": 0, "total_amount": 0, "amount_count": 0})
        domain_distribution.append({
            "domain": d,
            "domain_name": DOMAIN_NAMES.get(d, "未分類"),
            "count": os["count"],  # foundation count
            "program_count": ps["count"],  # program count
            "total_amount": os["total_amount"],  # annual amount
            "avg_amount": int(os["total_amount"] / os["amount_count"]) if os["amount_count"] else 0,
        })

    l1_distribution = sorted([
        {
            "name": f,
            "domain": DOMAIN_NAMES.get(s["domain"], s["domain"]),
            "count": s["count"],
            "program_count": prog_l1_stats.get(f, {}).get("count", 0),
            "total_amount": s["total_amount"],
            "avg_amount": int(s["total_amount"] / s["amount_count"]) if s["amount_count"] else 0,
        }
        for f, s in org_l1_stats.items()
    ], key=lambda x: -x["total_amount"] if x["total_amount"] > 0 else -x["count"])

    l2_distribution = sorted([
        {
            "name": sf,
            "l1_field": s["l1"],
            "domain": DOMAIN_NAMES.get(s["domain"], s["domain"]),
            "count": s["count"],
            "program_count": prog_l2_stats.get(sf, {}).get("count", 0),
            "total_amount": s["total_amount"],
            "avg_amount": int(s["total_amount"] / s["amount_count"]) if s["amount_count"] else 0,
        }
        for sf, s in org_l2_stats.items()
    ], key=lambda x: -x["total_amount"] if x["total_amount"] > 0 else -x["count"])

    # Foundation-level detail (all 189)
    grants_with_fields = []
    # First: programs
    for g in classified:
        grants_with_fields.append({
            "foundation": g["foundation"],
            "corporate_parent": g.get("corporate_parent"),
            "program": g["program"],
            "domains": g["domain_names"],
            "l1_fields": g["l1_fields"],
            "l2_subfields": g["l2_subfields"],
            "amount": g.get("grant_amount_max"),
            "deadline": g.get("application_deadline"),
            "status": g.get("status"),
            "type": "program",
        })
    # Then: foundations without programs
    seen_foundations = set(g["foundation"] for g in classified)
    for org in classified_orgs:
        if org["foundation"] not in seen_foundations and org["domains"] != ["unclassified"]:
            grants_with_fields.append({
                "foundation": org["foundation"],
                "corporate_parent": org.get("corporate_parent"),
                "program": "(事業目的から推定)",
                "domains": org["domain_names"],
                "l1_fields": org["l1_fields"],
                "l2_subfields": org["l2_subfields"],
                "amount": org.get("annual_grant_amount"),
                "deadline": None,
                "status": "active" if org.get("annual_grant_amount") else "unknown",
                "type": "foundation",
            })

    # Hierarchy tree
    tree = {}
    for sf, s in org_l2_stats.items():
        dom = DOMAIN_NAMES.get(s["domain"], s["domain"])
        l1 = s["l1"]
        if dom not in tree:
            tree[dom] = {}
        if l1 not in tree[dom]:
            tree[dom][l1] = []
        tree[dom][l1].append({"name": sf, "count": s["count"], "amount": s["total_amount"]})

    hierarchy = []
    for dom, l1s in tree.items():
        children = []
        for l1, l2s in l1s.items():
            children.append({
                "name": l1,
                "children": sorted(l2s, key=lambda x: -x["amount"] if x["amount"] > 0 else -x["count"]),
                "count": sum(x["count"] for x in l2s),
                "amount": sum(x["amount"] for x in l2s),
            })
        children.sort(key=lambda x: -x["amount"] if x["amount"] > 0 else -x["count"])
        hierarchy.append({
            "name": dom,
            "children": children,
            "count": sum(c["count"] for c in children),
            "amount": sum(c["amount"] for c in children),
        })
    hierarchy.sort(key=lambda x: -x["amount"] if x["amount"] > 0 else -x["count"])

    result = {
        "domain_distribution": domain_distribution,
        "l1_distribution": l1_distribution,
        "l2_distribution": l2_distribution,
        "hierarchy": hierarchy,
        "grants_with_fields": grants_with_fields,
    }

    with open(str(OUT), "w") as f:
        json.dump(result, f, ensure_ascii=False)

    total_annual = sum(d["total_amount"] for d in domain_distribution)
    classified_count = sum(1 for o in classified_orgs if o["domains"] != ["unclassified"])
    unclassified_count = sum(1 for o in classified_orgs if o["domains"] == ["unclassified"])

    print(f"=== Field Analysis Results (v2: Annual Amount Based) ===")
    print(f"Total foundations: {len(classified_orgs)}")
    print(f"  Classified: {classified_count}")
    print(f"  Unclassified: {unclassified_count}")
    print(f"Programs: {len(classified)}")
    print(f"Domains: {len(domain_distribution)}")
    print(f"L1 fields: {len(l1_distribution)}")
    print(f"L2 subfields: {len(l2_distribution)}")
    print(f"Total annual amount mapped: {total_annual/100000000:.1f}億円")
    print(f"\n--- Domain Distribution (Annual Amount) ---")
    for d in domain_distribution:
        print(f"  {d['domain_name']}: {d['count']} foundations, {d['program_count']} programs, {d['total_amount']/100000000:.1f}億円")
    print(f"\n--- L1 Top 15 (by Annual Amount) ---")
    for f in l1_distribution[:15]:
        print(f"  [{f['domain']}] {f['name']}: {f['count']} foundations, {f['total_amount']/100000000:.1f}億円")
    print(f"\n--- L2 Top 20 (by Annual Amount) ---")
    for sf in l2_distribution[:20]:
        print(f"  [{sf['domain']}] {sf['l1_field']} > {sf['name']}: {sf['count']} foundations, {sf['total_amount']/100000000:.1f}億円")

    print(f"\nOutput: {OUT} ({OUT.stat().st_size / 1024:.0f} KB)")
    grant_db.close()


if __name__ == "__main__":
    build()
