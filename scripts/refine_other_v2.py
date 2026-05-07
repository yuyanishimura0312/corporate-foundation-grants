#!/usr/bin/env python3
"""
Refine foundation_subtype for organizations classified as 'other' (v2).

This is an enhanced version of refine_other_classification.py with:
  - Expanded keyword set per subtype (corporate / individual / group / academic
    / govt / intl / ngo).
  - Confidence scoring (high / medium / low). Only high+medium are written;
    low remains 'other'.
  - Reason tracking — every decision is recorded back to organizations.metadata
    under `subtype_refine` so the audit trail is preserved.
  - Multi-signal scoring rather than first-match-wins so the best-supported
    subtype is chosen when several patterns hit.

Inputs per organization:
  name, description, metadata.admin / admin_agency, contact_address,
  metadata.research_score, corporate_parent.

Run:
  python3 scripts/refine_other_v2.py             # apply changes
  python3 scripts/refine_other_v2.py --dry-run   # preview only
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any, Optional

DB = Path(
    "/Users/nishimura+/projects/apps/corporate-foundation-grants/"
    "corporate_research_grants.sqlite"
)

# Central ministries -> govt
CENTRAL_MINISTRIES = [
    "農林水産省", "経済産業省", "厚生労働省", "国土交通省",
    "環境省", "総務省", "外務省", "財務省", "防衛省",
    "法務省", "警察庁", "金融庁", "デジタル庁", "国税庁",
    "気象庁", "海上保安庁", "観光庁", "中小企業庁", "特許庁",
    "資源エネルギー庁",
]

# Prefectures (admin) — local government, mostly academic/ngo when education/scholarship
PREFECTURE_ADMINS = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

# Group conglomerate markers (parent companies / holdings spanning many firms)
GROUP_MARKERS = [
    "グループ", "ホールディングス", "Holdings", "HD", "ＨＤ",
    "三井", "三菱", "住友", "SMBC", "MUFG", "みずほ",
    "野村", "大和", "JR", "NTT",
]

# Single-company brand names — strong corporate signal
COMPANY_BRANDS = [
    "カインズ", "コーセー", "りそな", "スプリックス", "コスモ", "ラッシュ",
    "九電", "アサヒ", "キリン", "サントリー", "ヤマト", "日通", "日本郵船",
    "JFE", "新日鉄", "JT", "JR東日本", "JR西日本", "JR西日本あんしん",
    "ENEOS", "出光",
    "リコー", "キヤノン", "富士通", "NEC", "シャープ", "オムロン", "京セラ",
    "村田製作所", "島津製作所", "ホンダ", "トヨタ", "日産", "マツダ",
    "スバル", "いすゞ", "コマツ", "クボタ", "ヤンマー",
    "資生堂", "花王", "ユニ・チャーム", "ライオン",
    "明治", "森永", "雪印", "日清", "味の素", "キッコーマン",
    "セブン", "イオン", "ローソン", "ファミリーマート",
    "東京海上", "損保ジャパン", "MS&AD", "第一生命", "明治安田", "日本生命",
    "東京ガス", "大阪ガス", "東邦ガス", "東京電力", "関西電力", "中部電力",
    "九州電力", "北海道電力", "東北電力", "中国電力", "四国電力",
    "JT", "リクルート", "サイバーエージェント", "DeNA", "GMO",
    "ソフトバンク", "KDDI", "楽天", "LINE", "メルカリ",
    "パナソニック", "ソニー", "東芝", "日立",
    "tetote", "Konno", "サウンドハウス",
    # additional foundations seen in DB
    "ノエビア", "フランスベッド", "ベネッセ", "キユーピー", "マツダ",
    "ローム", "博報", "福武", "前川", "森村", "ノーマライゼーション住宅",
    "JKA", "日母", "庭野", "日工組", "ユニベール", "セイノー",
    "コニカミノルタ", "デンソー", "ボーイング", "テル・コーポレーション",
    "TOTO", "東急", "タカラレーベン", "フェリシモ", "ジョンソン",
    "エドワーズライフサイエンス", "ノバルティス", "ザ・ボディショップ",
    "アムウェイ", "パタゴニア", "ソフトバンク", "Yahoo", "西友", "ウォルマート",
    "ホンダ", "ニッセイ", "日本生命", "毎日新聞", "読売", "日経",
    "日本郵便", "日興", "三菱商事", "三菱", "野村", "大塚商会",
    "サキナ", "フヨウサキナ", "田辺三菱", "ラン・フォー・ピース",
    "サウンドハウス", "リタワークス", "Ideal Leaders", "コングラント",
    "Ideal", "テル", "CIPA",
    # personal/family-named that may be corporate-tied (used as soft brand)
    # nothing extra here; these stay 'individual' via the personal pattern.
]

# Corporate / industry keywords (not group level, single-company / industry assoc)
CORP_KEYWORDS = [
    "技術", "産業", "工業", "商工", "建築", "土木", "鉄道", "海事",
    "農業", "水産", "畜産", "森林", "資源", "金融", "保険", "信託",
    "証券", "銀行", "公庫", "金庫", "電気", "電力", "ガス", "通信",
    "放送", "石油", "鉱業", "製造", "化学", "薬品", "繊維", "紙",
    "印刷", "食品", "酒造", "醸造", "観光", "交通", "運輸", "物流",
    "建設", "不動産", "情報", "コンピュータ", "電子", "デジタル",
    "コスメ", "化粧品", "機械", "自動車",
]
CORP_DESC_HINTS = [
    "産業の発展", "技術の振興", "業界の発展", "業の振興",
    "技術開発", "事業の振興", "産業振興",
]

# Individual / memorial / scholarship-named foundation
INDIVIDUAL_NAME_HINTS = [
    "記念", "翁", "夫人", "博士", "先生", "翁顕彰", "顕彰",
]
INDIVIDUAL_DESC_HINTS = [
    "創業者", "故人", "遺志", "遺贈", "顕彰",
]
# Scholarship-only foundations are usually individual/family but sometimes
# corporate. We use it as a soft signal only.
SCHOLARSHIP_HINTS = ["奨学", "育英", "学資", "学費"]

# Academic / research / education foundations
ACADEMIC_NAME_HINTS = [
    "大学", "学会", "学術", "学院", "学園", "学事", "学校",
    "工学院", "理科振興", "学術振興", "研究振興", "学資",
    "教育", "教育振興", "教育研究",
]
ACADEMIC_DESC_HINTS = [
    "学校教育", "教育研究", "学術研究", "研究助成", "学術振興",
    "大学教育", "大学院", "教職員",
]

# International / cross-border
INTL_KEYWORDS = [
    "国際", "世界", "アジア", "ユネスコ", "UNESCO", "アメリカ",
    "欧州", "韓国", "中国", "日米", "日中", "日韓", "国際交流",
    "海外", "留学", "グローバル", "オセアニア", "アフリカ",
]

# NGO / civic / community
NGO_KEYWORDS = [
    "市民", "ボランティア", "コミュニティ", "NPO", "非営利",
    "地域", "まちづくり", "社会課題", "市民活動", "社会的",
    "支援", "福祉", "子ども", "こども", "障害",
]
NGO_DESC_HINTS = [
    "市民活動", "地域社会", "社会課題", "社会的に困難",
    "コミュニティ", "ひとり親", "母子", "子育て",
]

# Government-style descriptions even if admin is 内閣府
GOVT_DESC_HINTS = [
    "国の施策", "政府の施策", "公共政策", "国土", "防災",
    "公共事業", "警察", "消防", "国土保全",
]


def _meta_field(meta: dict[str, Any], *keys: str) -> str:
    for k in keys:
        v = meta.get(k)
        if v:
            return str(v)
    return ""


def _has_any(text: str, words: list[str]) -> Optional[str]:
    for w in words:
        if w and w in text:
            return w
    return None


def classify(
    name: str,
    description: str,
    admin: str,
    parent: str,
    address: str,
) -> tuple[str, str, str]:
    """
    Return (subtype, confidence, reason).
    subtype is 'other' when confidence is 'low'.
    """
    n = name or ""
    d = description or ""
    a = admin or ""
    p = parent or ""
    addr = address or ""
    text = f"{n} {d} {addr}"

    scores: Counter[str] = Counter()
    reasons: dict[str, list[str]] = {}

    def add(subtype: str, weight: int, reason: str) -> None:
        scores[subtype] += weight
        reasons.setdefault(subtype, []).append(reason)

    # ---------- Strong signals ----------

    # 0. CANPAN listing — civic/NPO directory => ngo
    if d and "CANPAN" in d:
        add("ngo", 3, "CANPAN-listed (civic directory)")

    # 1. parent company => corporate (very strong)
    if p.strip():
        add("corporate", 5, f"parent_company={p.strip()}")

    # 2. Admin (主務官庁) signals
    if a:
        if any(k in a for k in CENTRAL_MINISTRIES):
            add("govt", 4, f"admin={a} (central ministry)")
        if "文部科学省" in a or "文科省" in a:
            add("academic", 4, f"admin={a} (MEXT)")
        if any(k in a for k in PREFECTURE_ADMINS):
            # Prefecture-administered: education/scholarship leans academic;
            # culture/welfare leans ngo. Provide modest weight.
            if any(k in (n + d) for k in ["教育", "奨学", "学術", "学校"]):
                add("academic", 3, f"admin={a} (prefecture) + education")
            elif any(k in (n + d) for k in ["福祉", "市民", "コミュニティ", "地域"]):
                add("ngo", 2, f"admin={a} (prefecture) + welfare/community")
        if "内閣府" in a:
            # 内閣府 is broad — only a weak hint towards govt; rely on text.
            pass

    # 3. Group conglomerate markers in name
    for marker in GROUP_MARKERS:
        if marker in n:
            add("group", 4, f"name has group marker '{marker}'")
            break

    # 3b. Brand-named foundations -> corporate (strong)
    for brand in COMPANY_BRANDS:
        if brand and brand in n:
            add("corporate", 4, f"name has brand '{brand}'")
            break

    # 3c. Direct company suffix => corporate
    if any(k in n for k in ["株式会社", "（株）", "(株)", "Inc.", "Co.,",
                              "有限会社", "合同会社", "合資会社"]):
        add("corporate", 5, "company-suffix in name")

    # 3d. Well-known general NGO names
    NGO_NAMES = [
        "日本財団", "共同募金", "赤い羽根", "セーブ・ザ・チルドレン",
        "セーブザチルドレン", "ETIC", "CAMPFIRE", "JapanGiving",
        "Yahoo!基金", "公益推進協会", "公益法人協会",
        "あしたの日本を創る協会", "メセナ協議会",
        "NPO", "NGO", "ネットワーク", "ボランティア",
        "ファザーリングジャパン", "ホームスタート", "サービスグラント",
        "プロボネット", "フィランソロピー", "フィランソロピック",
        "ソーシャル", "プラチナ・ギルド", "ジャパン・カインドネス",
        "REEP", "Soil", "lush", "LUSH", "ラッシュジャパン",
        "中央共同募金会", "全日本社会貢献団体機構", "日本フィランソロピー",
    ]
    for kw in NGO_NAMES:
        if kw in n:
            add("ngo", 3, f"name '{kw}' (well-known NGO)")
            break

    # 3e. International / cross-border named orgs
    INTL_NAMES = [
        "日本台湾交流協会", "日米", "日中", "日韓", "国際交流",
        "アフリカ", "アジア", "オセアニア", "欧州", "ササカワ",
        "スカンジナビア", "国際協力",
        "ドナルド・マクドナルド・ハウス",
        "パイロットインターナショナル",
    ]
    for kw in INTL_NAMES:
        if kw in n:
            add("intl", 3, f"name '{kw}' (intl)")
            break

    # 3f. Government / quasi-public bodies
    GOVT_NAMES = [
        "中小企業庁", "国交省", "市役所", "区役所", "都市開発推進機構",
        "区画整理促進機構", "民間都市開発", "砂防フロンティア", "国土計画",
        "都市農山漁村活性化", "防災教育",
    ]
    for kw in GOVT_NAMES:
        if kw in n:
            add("govt", 3, f"name '{kw}' (govt-affiliated)")
            break

    # 3g. Local / community fund => ngo
    LOCAL_FUND_HINTS = [
        "市民基金", "地域基金", "地域づくり基金", "市民活動", "地域づくり",
        "まちづくり", "まちの", "コミュニティファンド", "復興支援",
        "創造基金", "未来基金",
    ]
    for kw in LOCAL_FUND_HINTS:
        if kw in n:
            add("ngo", 3, f"name '{kw}' (local fund)")
            break

    # ---------- Name-based signals ----------

    if hit := _has_any(n, INDIVIDUAL_NAME_HINTS):
        add("individual", 3, f"name hint '{hit}'")

    # Individual-style name pattern: 漢字2-4文字の人名 + 財団 / 育成会 / 奨学会
    # e.g. 重田教育財団, 寺下援護会, 米盛誠心育成会, 鈴木万平糖尿病財団
    if re.search(
        r"[一-龥]{2,5}(教育|奨学|育英|育成|援護|顕彰|医学|学術)?(財団|基金|育成会|奨学会|援護会)",
        n,
    ):
        # Avoid matching pure org names like "日本リウマチ財団" — require non-common prefix.
        common_prefixes = ("日本", "全国", "国際", "東京", "大阪", "京都", "公益")
        head = n.replace("公益財団法人", "").replace("一般財団法人", "").strip()[:3]
        if not any(head.startswith(c) for c in common_prefixes):
            add("individual", 2, f"personal-name pattern in '{head}…'")

    if hit := _has_any(n, ACADEMIC_NAME_HINTS):
        add("academic", 2, f"name hint '{hit}'")

    if hit := _has_any(n, CORP_KEYWORDS):
        add("corporate", 2, f"name hint '{hit}'")

    if hit := _has_any(n, INTL_KEYWORDS):
        add("intl", 2, f"name hint '{hit}'")

    if hit := _has_any(n, NGO_KEYWORDS):
        add("ngo", 1, f"name hint '{hit}'")

    # ---------- Description-based signals ----------

    if hit := _has_any(d, ACADEMIC_DESC_HINTS):
        add("academic", 3, f"desc hint '{hit}'")

    if hit := _has_any(d, CORP_DESC_HINTS):
        add("corporate", 2, f"desc hint '{hit}'")

    if hit := _has_any(d, INDIVIDUAL_DESC_HINTS):
        add("individual", 2, f"desc hint '{hit}'")

    if hit := _has_any(d, INTL_KEYWORDS):
        add("intl", 2, f"desc hint '{hit}'")

    if hit := _has_any(d, NGO_DESC_HINTS):
        add("ngo", 3, f"desc hint '{hit}'")

    if hit := _has_any(d, GOVT_DESC_HINTS):
        add("govt", 2, f"desc hint '{hit}'")

    # Scholarship-only soft signal — boosts individual unless a stronger
    # corporate / group pattern already exists.
    if hit := _has_any(text, SCHOLARSHIP_HINTS):
        if not scores.get("corporate") and not scores.get("group"):
            add("individual", 2, f"scholarship hint '{hit}'")

    # Generic association/center patterns
    if "協会" in n and not scores.get("corporate"):
        # Many "協会" names are industry associations or NGOs.
        if any(k in n for k in CORP_KEYWORDS):
            add("corporate", 2, "name '協会' + industry term")
        else:
            add("ngo", 3, "name '協会' (industry/civic association)")
    if "センター" in n and any(k in d for k in ["教育", "学術", "研究"]):
        add("academic", 2, "name 'センター' + research/education")
    if "基金" in n and not scores:
        add("ngo", 3, "name '基金' (community fund)")
    # Medical / disease-named foundations (リウマチ, 糖尿病, がん, ALS, etc.)
    DISEASE_KW = [
        "リウマチ", "糖尿病", "がん", "癌", "ALS", "白血病", "心臓",
        "腎臓", "肝臓", "難病", "精神", "認知症", "アルツハイマー",
        "エイズ", "ＡＬＳ", "ドラベ症候群", "IDDM",
    ]
    for kw in DISEASE_KW:
        if kw in n:
            add("academic", 3, f"medical/disease '{kw}' (research foundation)")
            break
    # Description-based medical research signal
    if any(k in d for k in ["医学研究", "医学の発展", "薬学", "病理",
                              "医薬品", "創薬", "予防医学"]):
        add("academic", 2, "medical research description")
    # Therapy / professional society
    PROF_KW = [
        "理学療法士", "看護師", "薬剤師", "弁護士", "司法書士",
        "栄養士", "管理栄養士", "技術士", "医師会",
    ]
    for kw in PROF_KW:
        if kw in n:
            add("ngo", 3, f"professional society '{kw}'")
            break
    # General "会" / 同盟 / 連合 / フォーラム etc — NGO bias
    if not scores and any(k in n for k in ["同盟", "連合", "フォーラム", "委員会", "協議会"]):
        add("ngo", 2, "name has association suffix")
    # 'XX財団' alone, no other signal → likely individual/family foundation
    if not scores and "財団" in n and len(n) <= 12:
        add("individual", 2, "short '財団' name (likely family/individual)")

    # Cooperative / mutual aid / labor => ngo
    if any(k in n for k in ["生活協同組合", "ろうきん", "労働金庫", "共済",
                              "互助", "JC", "青年会議所", "ライオンズ", "ロータリー"]):
        add("ngo", 3, "cooperative/mutual aid")

    # Recovery / addiction / shelter
    if any(k in n for k in ["DARC", "ダルク", "リハビリ", "更生", "シェルター"]):
        add("ngo", 3, "recovery/shelter")

    # Arts council / cultural-fund non-corporate
    if any(k in n for k in ["アーツカウンシル", "メセナ", "ギャラリー",
                              "アートカウンシル"]):
        add("ngo", 3, "arts/culture council")

    # Crowdfunding / donation platforms => ngo
    if any(k in n for k in ["FAAVO", "CAMPFIRE", "JapanGiving", "motion gallery",
                              "コングラント", "クラウドファンディング", "Yahoo!基金",
                              "JapanGiving"]):
        add("ngo", 3, "crowdfunding/donation platform")

    # Trust / 'トラスト'
    if any(k in n for k in ["トラスト", "Trust"]):
        add("ngo", 2, "trust-style civic org")

    # Single-name catch-alls (no signal at all yet) — assume civic NGO if Japanese-only short
    # 'CANPAN調べ', 'CANPAN掲載団体' are placeholder rows from import
    if n in {"CANPAN調べ", "CANPAN掲載団体"}:
        # placeholder, leave as 'other' but mark explicitly
        add("ngo", 1, "CANPAN placeholder")

    # 研究/総研 => academic
    if any(k in n for k in ["総研", "研究所", "総合研究", "総合研所"]):
        add("academic", 3, "research institute")

    # source=grant_db_import in metadata == CANPAN FIELDS NPO
    # (description may already trigger CANPAN check above; this is a name-side fallback
    #  for very short org names that read as community/social orgs)
    SOCIAL_NAME_HINTS = [
        "応援団", "ファンド", "つながり", "応援", "ジオパーク",
        "推進会議", "推進協議会", "ワーキング", "コーチング",
        "応援", "支え合い", "おもちゃ", "図書館", "学習支援",
        "プラットフォーム", "ネット",
    ]
    for kw in SOCIAL_NAME_HINTS:
        if kw in n:
            add("ngo", 2, f"social org name '{kw}'")
            break

    # ---------- Decide ----------
    if not scores:
        return "other", "low", "no signal matched"

    # Pick the top class
    best, best_score = scores.most_common(1)[0]
    runner_up_score = 0
    if len(scores) > 1:
        runner_up_score = scores.most_common(2)[1][1]

    margin = best_score - runner_up_score

    if best_score >= 5 and margin >= 2:
        confidence = "high"
    elif best_score >= 4:
        confidence = "high"
    elif best_score >= 3:
        confidence = "medium"
    elif best_score >= 2 and margin >= 1:
        confidence = "medium"
    elif best_score >= 2:
        # tied — still better than nothing if 'reasons' contain 2+ signals
        if len(reasons.get(best, [])) >= 2:
            confidence = "medium"
        else:
            confidence = "low"
    else:
        confidence = "low"

    reason = f"score={best_score}; " + "; ".join(reasons[best])
    if confidence == "low":
        return "other", "low", reason
    return best, confidence, reason


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="do not write")
    ap.add_argument("--export-json", default=None,
                    help="If set, also dump decisions to this JSON file")
    args = ap.parse_args()

    # Use a generous busy_timeout to wait through any concurrent writers
    # (e.g. discover_urls.py running in parallel).
    conn = sqlite3.connect(DB, timeout=300.0)
    cur = conn.cursor()
    cur.execute("PRAGMA busy_timeout = 300000")

    cur.execute(
        """
        SELECT id, name, description, metadata, corporate_parent, contact_address
        FROM organizations
        WHERE foundation_subtype = 'other'
        """
    )
    rows = cur.fetchall()
    print(f"Targets (foundation_subtype='other'): {len(rows)}")

    moves: Counter[str] = Counter()
    confidence_counts: Counter[str] = Counter()
    examples: dict[str, list[tuple[str, str]]] = {}
    decisions: list[dict[str, Any]] = []

    for rid, name, desc, meta_str, parent, address in rows:
        admin = ""
        meta: dict[str, Any] = {}
        if meta_str:
            try:
                meta = json.loads(meta_str)
                admin = _meta_field(meta, "admin", "admin_agency")
            except Exception:
                meta = {}

        new_sub, conf, reason = classify(
            name or "", desc or "", admin, parent or "", address or ""
        )
        moves[new_sub] += 1
        confidence_counts[conf] += 1
        examples.setdefault(new_sub, []).append((name or "(no name)", reason))

        # Always record the audit trail
        meta["subtype_refine"] = {
            "version": "v2",
            "result": new_sub,
            "confidence": conf,
            "reason": reason,
        }
        decisions.append({
            "id": rid,
            "name": name,
            "result": new_sub,
            "confidence": conf,
            "reason": reason,
            "metadata": meta,
        })

        if args.dry_run:
            continue

        if new_sub != "other":
            cur.execute(
                """
                UPDATE organizations
                SET foundation_subtype = ?,
                    metadata = ?,
                    updated_at = datetime('now','localtime')
                WHERE id = ?
                """,
                (new_sub, json.dumps(meta, ensure_ascii=False), rid),
            )
        else:
            # still record reason (helps next round of refinement)
            cur.execute(
                """
                UPDATE organizations
                SET metadata = ?,
                    updated_at = datetime('now','localtime')
                WHERE id = ?
                """,
                (json.dumps(meta, ensure_ascii=False), rid),
            )

    if not args.dry_run:
        conn.commit()

    if args.export_json:
        with open(args.export_json, "w", encoding="utf-8") as fh:
            json.dump(decisions, fh, ensure_ascii=False, indent=2)
        print(f"\nExported {len(decisions)} decisions to {args.export_json}")

    print("\nReclassification result (from 'other'):")
    for k, v in moves.most_common():
        print(f"  -> {k:10s}: {v}")

    print("\nConfidence breakdown:")
    for k, v in confidence_counts.most_common():
        print(f"  {k}: {v}")

    print("\nExamples per subtype (max 3):")
    for sub, items in examples.items():
        print(f"\n[{sub}]")
        for nm, rs in items[:3]:
            print(f"  - {nm}: {rs}")

    cur.execute(
        """
        SELECT foundation_subtype, COUNT(*)
        FROM organizations
        GROUP BY foundation_subtype
        ORDER BY 2 DESC
        """
    )
    print("\nFinal distribution:")
    for k, v in cur.fetchall():
        print(f"  {k}: {v}")

    conn.close()


if __name__ == "__main__":
    main()
