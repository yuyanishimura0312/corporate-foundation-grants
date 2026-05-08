#!/usr/bin/env python3
"""Backfill prefecture for organizations missing prefecture using multiple methods.

Methods (applied in order, cheapest/most-reliable first):
  1. Manual mapping dictionary (200+ known foundations)
  2. Name-pattern extraction (prefecture/city name in org name; university->pref)
  3. Codex Phase 4-7 extracted snippets (loose-norm match)
  4. Wikipedia API lookup (infobox 本部所在地/所在地/headquarters)
  5. Official website scraping (top page + about/contact links)
  6. koeki-info.go.jp by name search (fallback for koeki_id missing)

Rate limits: Wikipedia 1 req/sec, koeki-info 1 req/3sec, websites 1 req/2sec.
Cache: cache/prefecture_lookup/<source>/<key>.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import sys
import time
import unicodedata
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path("/Users/nishimura+/projects/apps/corporate-foundation-grants")
DB = ROOT / "corporate_research_grants.sqlite"
CACHE_DIR = ROOT / "cache" / "prefecture_lookup"
CODEX_FILES = [
    ROOT / "research_results" / "codex_phase4_extracted.json",
    ROOT / "research_results" / "codex_phase5_extracted.json",
    ROOT / "research_results" / "codex_phase6_extracted.json",
]

PREFECTURES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

# Pref short forms for name detection (without 都/府/県 suffix)
PREF_SHORT = {p[:-1] if p.endswith(("都", "府", "県")) else p[:-1]: p for p in PREFECTURES}
PREF_SHORT["北海"] = "北海道"
# 県 = 県名と同じ短縮形は誤マッチを生むため一部除外（"奈良"は奈良県だが奈良県大学等で衝突しない、保持）

CITY_TO_PREF = {
    # 北海道
    "札幌": "北海道", "旭川": "北海道", "函館": "北海道", "釧路": "北海道", "帯広": "北海道",
    # 東北
    "仙台": "宮城県", "盛岡": "岩手県", "秋田市": "秋田県", "山形市": "山形県",
    "福島市": "福島県", "郡山": "福島県", "いわき": "福島県", "青森市": "青森県", "八戸": "青森県",
    # 関東
    "新宿": "東京都", "渋谷": "東京都", "千代田": "東京都", "中央区": "東京都",
    "港区": "東京都", "目黒": "東京都", "品川": "東京都", "世田谷": "東京都",
    "豊島": "東京都", "杉並": "東京都", "練馬": "東京都", "板橋": "東京都",
    "足立": "東京都", "墨田": "東京都", "江東": "東京都", "台東": "東京都",
    "文京": "東京都", "荒川": "東京都", "葛飾": "東京都", "江戸川": "東京都",
    "中野区": "東京都", "大田区": "東京都", "八王子": "東京都", "立川": "東京都",
    "三鷹": "東京都", "武蔵野": "東京都", "府中市": "東京都",
    "横浜": "神奈川県", "川崎": "神奈川県", "鎌倉": "神奈川県", "藤沢": "神奈川県",
    "相模原": "神奈川県", "横須賀": "神奈川県", "厚木": "神奈川県", "湘南": "神奈川県",
    "千葉市": "千葉県", "船橋": "千葉県", "市川": "千葉県", "柏": "千葉県", "松戸": "千葉県",
    "さいたま": "埼玉県", "川口": "埼玉県", "所沢": "埼玉県", "浦和": "埼玉県", "大宮": "埼玉県",
    "宇都宮": "栃木県", "前橋": "群馬県", "高崎": "群馬県", "水戸": "茨城県", "つくば": "茨城県",
    # 中部
    "名古屋": "愛知県", "豊田": "愛知県", "岡崎": "愛知県", "豊橋": "愛知県",
    "京都市": "京都府", "宇治": "京都府",
    "大阪市": "大阪府", "堺": "大阪府", "東大阪": "大阪府", "豊中": "大阪府", "吹田": "大阪府",
    "神戸": "兵庫県", "姫路": "兵庫県", "西宮": "兵庫県", "尼崎": "兵庫県",
    "新潟市": "新潟県", "長岡": "新潟県",
    "金沢": "石川県", "富山市": "富山県", "高岡": "富山県", "福井市": "福井県",
    "甲府": "山梨県", "長野市": "長野県", "松本": "長野県",
    "岐阜市": "岐阜県", "静岡市": "静岡県", "浜松": "静岡県", "沼津": "静岡県",
    "津市": "三重県", "四日市": "三重県", "鈴鹿": "三重県",
    "大津": "滋賀県",
    # 中国・四国
    "広島市": "広島県", "福山": "広島県", "岡山市": "岡山県", "倉敷": "岡山県",
    "鳥取市": "鳥取県", "松江": "島根県", "山口市": "山口県", "下関": "山口県",
    "徳島市": "徳島県", "高松": "香川県", "松山": "愛媛県", "高知市": "高知県",
    # 九州
    "福岡市": "福岡県", "北九州": "福岡県", "久留米": "福岡県",
    "佐賀市": "佐賀県", "長崎市": "長崎県", "熊本市": "熊本県",
    "大分市": "大分県", "宮崎市": "宮崎県", "鹿児島市": "鹿児島県",
    "那覇": "沖縄県", "沖縄市": "沖縄県",
    "和歌山市": "和歌山県", "奈良市": "奈良県",
}

# University -> prefecture (well-known national/major universities)
UNIVERSITY_TO_PREF = {
    "東京大学": "東京都", "京都大学": "京都府", "大阪大学": "大阪府",
    "東北大学": "宮城県", "九州大学": "福岡県", "北海道大学": "北海道",
    "名古屋大学": "愛知県", "東京工業大学": "東京都", "東京医科歯科": "東京都",
    "一橋大学": "東京都", "早稲田大学": "東京都", "慶應義塾": "東京都",
    "東京理科大学": "東京都", "上智大学": "東京都", "明治大学": "東京都",
    "中央大学": "東京都", "法政大学": "東京都", "立教大学": "東京都",
    "青山学院": "東京都", "学習院": "東京都", "順天堂": "東京都",
    "日本大学": "東京都", "東京農工": "東京都", "電気通信大学": "東京都",
    "横浜国立": "神奈川県", "横浜市立": "神奈川県", "千葉大学": "千葉県",
    "筑波大学": "茨城県", "埼玉大学": "埼玉県",
    "金沢大学": "石川県", "新潟大学": "新潟県", "信州大学": "長野県",
    "山梨大学": "山梨県", "富山大学": "富山県", "福井大学": "福井県",
    "岐阜大学": "岐阜県", "静岡大学": "静岡県", "三重大学": "三重県",
    "名古屋工業": "愛知県", "豊橋技術科学": "愛知県",
    "神戸大学": "兵庫県", "大阪市立": "大阪府", "大阪府立": "大阪府",
    "立命館": "京都府", "同志社": "京都府", "京都工芸繊維": "京都府",
    "奈良女子": "奈良県", "和歌山大学": "和歌山県", "滋賀大学": "滋賀県",
    "岡山大学": "岡山県", "広島大学": "広島県", "山口大学": "山口県",
    "島根大学": "島根県", "鳥取大学": "鳥取県",
    "徳島大学": "徳島県", "香川大学": "香川県", "愛媛大学": "愛媛県",
    "高知大学": "高知県",
    "九州工業": "福岡県", "佐賀大学": "佐賀県", "長崎大学": "長崎県",
    "熊本大学": "熊本県", "大分大学": "大分県", "宮崎大学": "宮崎県",
    "鹿児島大学": "鹿児島県", "琉球大学": "沖縄県",
    "弘前大学": "青森県", "岩手大学": "岩手県", "山形大学": "山形県",
    "福島大学": "福島県", "茨城大学": "茨城県", "宇都宮大学": "栃木県",
    "群馬大学": "群馬県", "秋田大学": "秋田県",
}

# Manual mapping for known organizations (keyed by core name substring)
MANUAL_MAPPING: dict[str, str] = {
    # 主要企業財団 - 東京
    "ロッテ財団": "東京都",
    "齋藤茂昭記念": "東京都",
    "デロイトトーマツ": "東京都",
    "カインズデジタル": "埼玉県",
    "日工組社会安全": "東京都",
    "鉄道弘済会": "東京都",
    "Konno": "東京都",
    "ちゅうでん教育振興": "愛知県",
    "業務スーパージャパン": "兵庫県",
    "天田財団": "神奈川県",
    "JFE21世紀": "東京都",
    "KDDI財団": "東京都",
    "MSD生命科学": "東京都",
    "旭硝子財団": "東京都",
    "三菱財団": "東京都",
    "武田科学振興": "大阪府",
    "サントリー生命科学": "大阪府",
    "野村財団": "東京都",
    "キヤノン財団": "東京都",
    "三井住友海上": "東京都",
    "セコム科学技術": "東京都",
    "松下幸之助": "大阪府",
    "万有生命科学": "東京都",
    "PwC財団": "東京都",
    "助成財団センター": "東京都",
    "社会貢献支援財団": "東京都",
    "中谷財団": "東京都",
    "中谷医工計測": "東京都",
    "稲盛財団": "京都府",
    "京セラみらい": "京都府",
    "豊田理化学": "愛知県",
    "豊田財団": "愛知県",
    "トヨタ財団": "東京都",
    "矢崎科学技術": "東京都",
    "日立財団": "東京都",
    "村田学術": "京都府",
    "オムロン財団": "京都府",
    "立石科学技術": "京都府",
    "島津科学技術": "京都府",
    "島津製作所": "京都府",
    "本田財団": "東京都",
    "ホンダ財団": "東京都",
    "ホンダ・ファウンデーション": "東京都",
    "三菱電機SDジャパン": "東京都",
    "三菱重工": "東京都",
    "三菱UFJ": "東京都",
    "三菱UFJ国際": "東京都",
    "三井住友": "東京都",
    "みずほ": "東京都",
    "野村": "東京都",
    "大和証券": "東京都",
    "大和日英基金": "東京都",
    "セブン-イレブン": "東京都",
    "イオン環境": "千葉県",
    "イオン1%": "千葉県",
    "セブンイレブン": "東京都",
    "ローソン": "東京都",
    "セコム": "東京都",
    "アサヒビール": "東京都",
    "サッポロビール": "東京都",
    "キリンビール": "東京都",
    "キリン・福祉": "東京都",
    "キリン記念": "東京都",
    "サントリー文化": "大阪府",
    "サントリーホールディングス": "大阪府",
    "コカ・コーラ": "東京都",
    "資生堂": "東京都",
    "花王": "東京都",
    "ライオン": "東京都",
    "東芝国際": "東京都",
    "東芝": "東京都",
    "NEC C&C": "東京都",
    "NEC": "東京都",
    "富士通": "東京都",
    "富士フイルム": "東京都",
    "電通育英": "東京都",
    "博報堂教育": "東京都",
    "博報堂財団": "東京都",
    "公文教育": "大阪府",
    "ベネッセ": "岡山県",
    "学研": "東京都",
    "三菱UFJ信託": "東京都",
    "JR東日本": "東京都",
    "JR西日本": "大阪府",
    "JR東海": "愛知県",
    "JR九州": "福岡県",
    "ANA": "東京都",
    "JAL": "東京都",
    "東京ガス": "東京都",
    "東京電力": "東京都",
    "関西電力": "大阪府",
    "中部電力": "愛知県",
    "東北電力": "宮城県",
    "中国電力": "広島県",
    "九州電力": "福岡県",
    "九電みらい": "福岡県",
    "四国電力": "香川県",
    "北海道電力": "北海道",
    "北陸電力": "富山県",
    "東京海上": "東京都",
    "損保ジャパン": "東京都",
    "あいおいニッセイ": "東京都",
    "明治安田": "東京都",
    "日本生命": "大阪府",
    "第一生命": "東京都",
    "住友生命": "大阪府",
    "プルデンシャル": "東京都",
    # 大学・学会系
    "日本理学療法士": "東京都",
    "日本看護協会": "東京都",
    "日本医師会": "東京都",
    "日本歯科医師": "東京都",
    "日本薬剤師": "東京都",
    "日本学術振興会": "東京都",
    "科学技術振興機構": "東京都",
    "JST": "東京都",
    "理化学研究所": "埼玉県",
    "産業技術総合研究所": "茨城県",
    "国立がん研究": "東京都",
    "国立感染症": "東京都",
    "国立環境研究所": "茨城県",
    "東京大学": "東京都",
    "京都大学": "京都府",
    "大阪大学": "大阪府",
    "東北大学": "宮城県",
    "九州大学": "福岡県",
    "北海道大学": "北海道",
    "名古屋大学": "愛知県",
    "筑波大学": "茨城県",
    "東京工業": "東京都",
    "早稲田": "東京都",
    "慶應": "東京都",
    "上智": "東京都",
    "立命館": "京都府",
    "同志社": "京都府",
    "明治": "東京都",
    "中央大学": "東京都",
    "法政": "東京都",
    "立教": "東京都",
    "青山学院": "東京都",
    "学習院": "東京都",
    # 政府・公益系
    "国交省": "東京都",
    "民間都市開発": "東京都",
    "都市農山漁村活性化": "東京都",
    "中央競馬馬主": "東京都",
    "日本メイスン": "東京都",
    "九州地域づくり": "福岡県",
    "大竹財団": "東京都",
    "ライフスポーツ": "東京都",
    "イーパーツ": "東京都",
    "アジア農村交流": "東京都",
    "朝日新聞厚生": "東京都",
    "朝日新聞文化": "東京都",
    "毎日新聞": "東京都",
    "読売新聞": "東京都",
    "日本経済新聞": "東京都",
    "産経新聞": "東京都",
    "NHK": "東京都",
    # NGO/NPO
    "Kids Code Club": "福岡県",
    "SMBCグループ": "東京都",
    "サウンドハウス": "千葉県",
    "お金をまわそう基金": "東京都",
    "アーツカウンシル東京": "東京都",
    "ふじのくに未来": "静岡県",
    "アスクネット": "愛知県",
    "ジャパン・カインドネス": "東京都",
    "グッドネーバーズ": "東京都",
    "ホームスタート": "東京都",
    "全国こども食堂": "東京都",
    "むすびえ": "東京都",
    "チャリティーサンタ": "東京都",
    "瀬戸内オリーブ": "香川県",
    "パタゴニア": "神奈川県",
    "みんなのコード": "東京都",
    "つなぐいのち": "東京都",
    "OVA": "東京都",
    "産直ドミノ": "東京都",
    "ドミノ・ピザ": "東京都",
    "関西NGO": "大阪府",
    "ジャパン・フィランソロピック": "東京都",
    "コングラント": "東京都",
    "日本ユネスコ": "東京都",
    "ユースキャリア": "愛知県",
    "室戸ジオパーク": "高知県",
    "ラッシュジャパン": "神奈川県",
    "全日本冠婚葬祭": "東京都",
    "NGO福岡": "福岡県",
    "Soil": "東京都",
    "MUFG": "東京都",
    "リタワークス": "大阪府",
    "Social-Ship": "大阪府",
    "ソーシャル・インベストメント": "東京都",
    "葉田財団": "東京都",
    "Reon": "岐阜県",
    "ペガサス財団": "東京都",
    "日本フラッグフットボール": "東京都",
    "みらいRITA": "東京都",
    "タチバナ財団": "東京都",
    "tetote教育": "東京都",
    "エフピコ": "広島県",
    "東洋アルミ軽金属": "大阪府",
    "白珪社": "東京都",
    "芳心会": "東京都",
    "松の花基金": "東京都",
    "樫の芽会": "東京都",
    "日本ウェルビーイング": "東京都",
    "環境再生保全機構": "神奈川県",
    "ERCA": "神奈川県",
    "地球環境基金": "神奈川県",
    "パルシステム千葉": "千葉県",
    "パルシステム東京": "東京都",
    "パルシステム神奈川": "神奈川県",
    "パルシステム埼玉": "埼玉県",
    "生活協同組合連合会": "東京都",
    "生活クラブ": "東京都",
    # その他
    "助成財団": "東京都",
    "笹川": "東京都",
    "笹川平和": "東京都",
    "日本財団": "東京都",
    "経団連": "東京都",
    "日本商工会議所": "東京都",
    "東京商工会議所": "東京都",
    "大阪商工会議所": "大阪府",
    "名古屋商工会議所": "愛知県",
    "京都商工会議所": "京都府",
    "ナミねぇ": "東京都",
    "プロップ・ステーション": "兵庫県",
    "ユニベール": "東京都",
    "JKA": "東京都",
    "オートレース": "東京都",
    "競輪": "東京都",
    "BOAT RACE": "東京都",
    "中央競馬": "東京都",
}


def now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def normalize_name(name: str) -> str:
    """Loose-norm: lowercase, strip legal suffixes, remove spaces and punctuation."""
    if not name:
        return ""
    s = unicodedata.normalize("NFKC", name)
    # Remove legal forms
    for tok in [
        "公益財団法人", "一般財団法人", "公益社団法人", "一般社団法人",
        "特定非営利活動法人", "特活", "ＮＰＯ法人", "NPO法人",
        "（公財）", "（一財）", "（公社）", "（一社）", "（特）", "(NPO)",
        "(公財)", "(一財)", "(公社)", "(一社)",
        "株式会社", "有限会社", "（株）", "（有）", "(株)", "(有)",
        "独立行政法人", "国立研究開発法人", "国立大学法人", "公立大学法人",
        "学校法人", "宗教法人", "社会福祉法人",
        "財団法人", "社団法人",
    ]:
        s = s.replace(tok, "")
    s = re.sub(r"[\s　・･]+", "", s)
    s = re.sub(r"[（）()【】「」『』、。．,.\-_/]", "", s)
    return s.lower()


def extract_pref_from_text(text: str) -> str | None:
    """Extract prefecture name from arbitrary Japanese text."""
    if not text:
        return None
    # Full prefecture name first (most specific)
    for pref in PREFECTURES:
        if pref in text:
            return pref
    # University-based detection
    for uni, pref in UNIVERSITY_TO_PREF.items():
        if uni in text:
            return pref
    # City-based fallback
    for city, pref in CITY_TO_PREF.items():
        if city in text:
            return pref
    # Short pref form (e.g., "東京" without 都, "大阪" without 府)
    for short, pref in PREF_SHORT.items():
        if short and short in text and len(short) >= 2:
            return pref
    return None


# ------------------------------------------------------------------ Method 1
def method_manual(name: str) -> str | None:
    """Lookup against MANUAL_MAPPING by substring match against normalized name."""
    if not name:
        return None
    norm = normalize_name(name)
    for key, pref in MANUAL_MAPPING.items():
        if normalize_name(key) in norm:
            return pref
    return None


# ------------------------------------------------------------------ Method 2
def method_name_pattern(name: str) -> str | None:
    """Pattern-extract prefecture from organization name itself."""
    return extract_pref_from_text(name)


# ------------------------------------------------------------------ Method 3
_codex_cache: dict | None = None


def _load_codex() -> dict:
    global _codex_cache
    if _codex_cache is not None:
        return _codex_cache
    out: dict[str, str] = {}  # norm name -> joined snippet text
    for fp in CODEX_FILES:
        if not fp.exists():
            continue
        try:
            d = json.loads(fp.read_text())
        except Exception:
            continue
        for k, v in d.get("foundations", {}).items():
            ctx = " ".join(v.get("context_snippets", []) + [v.get("name", ""), v.get("name_core", "") or ""])
            for nm in [k, v.get("name", ""), v.get("name_core", "") or ""]:
                if nm:
                    nn = normalize_name(nm)
                    if nn:
                        out[nn] = (out.get(nn, "") + " " + ctx).strip()
    _codex_cache = out
    return out


def method_codex(name: str) -> str | None:
    cache = _load_codex()
    nn = normalize_name(name)
    if not nn:
        return None
    # Direct match
    ctx = cache.get(nn)
    if ctx:
        pref = extract_pref_from_text(ctx)
        if pref:
            return pref
    # Substring match (loose)
    for key, ctx in cache.items():
        if not key:
            continue
        if (len(nn) >= 4 and nn in key) or (len(key) >= 4 and key in nn):
            pref = extract_pref_from_text(ctx)
            if pref:
                return pref
    return None


# ------------------------------------------------------------------ Method 4: Wikipedia
def _http_get(url: str, timeout: int = 15) -> str | None:
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "miratuku-foundation-db/1.0 (https://github.com/yuyanishimura0312)",
                "Accept-Language": "ja,en;q=0.7",
            },
        )
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = r.read()
            # Try utf-8 then fallback
            try:
                return data.decode("utf-8")
            except UnicodeDecodeError:
                return data.decode("utf-8", errors="ignore")
    except Exception:
        return None


def _cache_path(source: str, key: str) -> Path:
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    p = CACHE_DIR / source
    p.mkdir(parents=True, exist_ok=True)
    return p / f"{h}.json"


_last_wiki_call = 0.0


def method_wikipedia(name: str) -> str | None:
    """Query Japanese Wikipedia for the org and parse infobox for 所在地."""
    global _last_wiki_call
    if not name:
        return None
    # Strip legal forms for query
    q = name
    for tok in ["公益財団法人", "一般財団法人", "公益社団法人", "一般社団法人",
                "特定非営利活動法人", "（公財）", "（一財）", "（公社）", "（一社）",
                "(公財)", "(一財)", "(公社)", "(一社)",
                "財団法人", "社団法人"]:
        q = q.replace(tok, "")
    q = q.strip()
    if len(q) < 2:
        return None

    cache_file = _cache_path("wikipedia", q)
    if cache_file.exists():
        try:
            cached = json.loads(cache_file.read_text())
            return cached.get("prefecture")
        except Exception:
            pass

    # Rate limit 1 req/sec
    elapsed = time.time() - _last_wiki_call
    if elapsed < 1.0:
        time.sleep(1.0 - elapsed)
    _last_wiki_call = time.time()

    # Search via OpenSearch
    api = (
        "https://ja.wikipedia.org/w/api.php?action=opensearch&format=json&limit=3&search="
        + urllib.parse.quote(q)
    )
    body = _http_get(api)
    pref = None
    if body:
        try:
            arr = json.loads(body)
            titles = arr[1] if len(arr) > 1 else []
            urls = arr[3] if len(arr) > 3 else []
            for title, page_url in zip(titles, urls):
                # Fetch summary via REST
                elapsed = time.time() - _last_wiki_call
                if elapsed < 1.0:
                    time.sleep(1.0 - elapsed)
                _last_wiki_call = time.time()
                # Use parse API to get plain text wikitext (faster than HTML parsing)
                parse = (
                    "https://ja.wikipedia.org/w/api.php?action=parse&format=json"
                    "&prop=wikitext&redirects=1&page=" + urllib.parse.quote(title)
                )
                pbody = _http_get(parse)
                if not pbody:
                    continue
                try:
                    pj = json.loads(pbody)
                    wikitext = pj.get("parse", {}).get("wikitext", {}).get("*", "")
                except Exception:
                    wikitext = ""
                if not wikitext:
                    continue
                # Look for infobox 所在地 / 本部所在地 / 本部 / 本店 lines
                m = re.search(
                    r"\|\s*(?:本部所在地|所在地|本店|本社所在地|headquarters|location)\s*=\s*([^\n|]+)",
                    wikitext, re.IGNORECASE,
                )
                if m:
                    pref = extract_pref_from_text(m.group(1))
                if not pref:
                    # Fallback: scan whole wikitext (first 4000 chars to limit noise)
                    pref = extract_pref_from_text(wikitext[:4000])
                if pref:
                    break
        except Exception:
            pass

    cache_file.write_text(json.dumps({"query": q, "prefecture": pref}, ensure_ascii=False))
    return pref


# ------------------------------------------------------------------ Method 5: Web scraping
_last_web_call = 0.0


def method_website(url: str) -> str | None:
    global _last_web_call
    if not url:
        return None
    # Clean URL: handle "http://https://..." malformed entries
    if url.startswith("http://https://"):
        url = url[len("http://"):]
    if url.startswith("http://http://"):
        url = url[len("http://"):]
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    cache_file = _cache_path("website", url)
    if cache_file.exists():
        try:
            cached = json.loads(cache_file.read_text())
            return cached.get("prefecture")
        except Exception:
            pass

    # Rate limit 1 req/2sec
    elapsed = time.time() - _last_web_call
    if elapsed < 2.0:
        time.sleep(2.0 - elapsed)
    _last_web_call = time.time()

    body = _http_get(url, timeout=12)
    pref = None
    if body:
        # Strip HTML tags crudely, search for 所在地 anchor first
        text = re.sub(r"<script.*?</script>", " ", body, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<style.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<[^>]+>", " ", text)
        text = unicodedata.normalize("NFKC", text)

        # Locate "所在地" / "住所" markers and prefer surrounding text
        for marker in ["本部所在地", "所在地", "住所", "本社", "事務局"]:
            idx = text.find(marker)
            if idx >= 0:
                window = text[idx: idx + 200]
                p = extract_pref_from_text(window)
                if p:
                    pref = p
                    break
        if not pref:
            # Fallback: full-text scan (first 20000 chars)
            pref = extract_pref_from_text(text[:20000])

        # If nothing on top page, try /about, /company, /contact links (single follow)
        if not pref:
            base_match = re.match(r"(https?://[^/]+)", url)
            if base_match:
                base = base_match.group(1)
                for path in ["/about", "/company", "/contact", "/about/", "/access", "/profile"]:
                    elapsed = time.time() - _last_web_call
                    if elapsed < 2.0:
                        time.sleep(2.0 - elapsed)
                    _last_web_call = time.time()
                    sub = _http_get(base + path, timeout=10)
                    if not sub:
                        continue
                    t2 = re.sub(r"<script.*?</script>", " ", sub, flags=re.DOTALL | re.IGNORECASE)
                    t2 = re.sub(r"<style.*?</style>", " ", t2, flags=re.DOTALL | re.IGNORECASE)
                    t2 = re.sub(r"<[^>]+>", " ", t2)
                    t2 = unicodedata.normalize("NFKC", t2)
                    for marker in ["本部所在地", "所在地", "住所"]:
                        idx = t2.find(marker)
                        if idx >= 0:
                            window = t2[idx: idx + 200]
                            p = extract_pref_from_text(window)
                            if p:
                                pref = p
                                break
                    if not pref:
                        pref = extract_pref_from_text(t2[:15000])
                    if pref:
                        break

    cache_file.write_text(json.dumps({"url": url, "prefecture": pref}, ensure_ascii=False))
    return pref


# ------------------------------------------------------------------ Method 6: koeki-info search
_last_koeki_call = 0.0


def method_koeki(name: str) -> str | None:
    """Search koeki-info.go.jp by name; parse address from result page."""
    global _last_koeki_call
    if not name:
        return None
    q = name
    for tok in ["公益財団法人", "一般財団法人", "公益社団法人", "一般社団法人",
                "（公財）", "（一財）", "(公財)", "(一財)",
                "財団法人", "社団法人"]:
        q = q.replace(tok, "")
    q = q.strip()
    if len(q) < 2:
        return None

    cache_file = _cache_path("koeki", q)
    if cache_file.exists():
        try:
            cached = json.loads(cache_file.read_text())
            return cached.get("prefecture")
        except Exception:
            pass

    elapsed = time.time() - _last_koeki_call
    if elapsed < 3.0:
        time.sleep(3.0 - elapsed)
    _last_koeki_call = time.time()

    # search URL
    url = "https://www.koeki-info.go.jp/regularization/?keyword=" + urllib.parse.quote(q)
    body = _http_get(url, timeout=15)
    pref = None
    if body:
        text = re.sub(r"<script.*?</script>", " ", body, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<style.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<[^>]+>", " ", text)
        text = unicodedata.normalize("NFKC", text)
        pref = extract_pref_from_text(text[:30000])

    cache_file.write_text(json.dumps({"query": q, "prefecture": pref}, ensure_ascii=False))
    return pref


# ------------------------------------------------------------------ Driver
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="Limit organizations processed (0=all)")
    ap.add_argument("--methods", default="manual,name,codex,wiki,web",
                    help="Comma-separated methods to run")
    ap.add_argument("--dry-run", action="store_true", help="Do not write to DB")
    args = ap.parse_args()

    methods_to_run = set(m.strip() for m in args.methods.split(","))

    conn = sqlite3.connect(DB, timeout=60)
    conn.execute("PRAGMA busy_timeout = 60000")
    cur = conn.cursor()

    cur.execute("""
        SELECT id, name, url, koeki_id
        FROM organizations
        WHERE (prefecture IS NULL OR prefecture = '')
          AND (country_code = 'JP' OR country_code IS NULL)
    """)
    rows = cur.fetchall()
    if args.limit:
        rows = rows[: args.limit]
    print(f"[{now()}] Targets: {len(rows)} organizations")

    counts = {m: 0 for m in ["manual", "name", "codex", "wiki", "web", "koeki"]}
    updates: list[tuple[str, str, str]] = []  # (id, prefecture, method)
    failures: list[tuple[str, str]] = []  # (id, name)

    for idx, (rid, name, url, koeki_id) in enumerate(rows, 1):
        pref = None
        method_used = None

        if "manual" in methods_to_run and not pref:
            pref = method_manual(name)
            if pref:
                method_used = "manual"

        if "name" in methods_to_run and not pref:
            pref = method_name_pattern(name)
            if pref:
                method_used = "name"

        if "codex" in methods_to_run and not pref:
            pref = method_codex(name)
            if pref:
                method_used = "codex"

        if "wiki" in methods_to_run and not pref:
            pref = method_wikipedia(name)
            if pref:
                method_used = "wiki"

        if "web" in methods_to_run and not pref and url:
            pref = method_website(url)
            if pref:
                method_used = "web"

        if "koeki" in methods_to_run and not pref:
            pref = method_koeki(name)
            if pref:
                method_used = "koeki"

        if pref:
            counts[method_used] += 1
            updates.append((rid, pref, method_used))
        else:
            failures.append((rid, name))

        if idx % 25 == 0:
            print(f"  [{idx}/{len(rows)}] resolved={sum(counts.values())} "
                  f"manual={counts['manual']} name={counts['name']} "
                  f"codex={counts['codex']} wiki={counts['wiki']} "
                  f"web={counts['web']} koeki={counts['koeki']}")

    # Apply updates
    if not args.dry_run and updates:
        for rid, pref, _m in updates:
            cur.execute(
                "UPDATE organizations SET prefecture = ?, "
                "updated_at=datetime('now','localtime') WHERE id = ?",
                (pref, rid),
            )
        conn.commit()

    # Final stats
    cur.execute("SELECT COUNT(*) FROM organizations")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM organizations WHERE prefecture IS NOT NULL AND prefecture != ''")
    have = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM organizations WHERE (prefecture IS NULL OR prefecture='') AND (country_code='JP' OR country_code IS NULL)")
    remaining_jp = cur.fetchone()[0]

    print(f"\n[{now()}] === Backfill complete ===")
    print(f"  Manual mapping     : {counts['manual']}")
    print(f"  Name pattern       : {counts['name']}")
    print(f"  Codex extraction   : {counts['codex']}")
    print(f"  Wikipedia          : {counts['wiki']}")
    print(f"  Website scrape     : {counts['web']}")
    print(f"  Koeki-info search  : {counts['koeki']}")
    print(f"  Total resolved     : {sum(counts.values())} / {len(rows)}")
    print(f"  DB coverage        : {have}/{total} ({have/total*100:.1f}%)")
    print(f"  Remaining JP unknown: {remaining_jp}")

    # Sample of failures
    if failures:
        print(f"\n  Sample of unresolved ({min(20, len(failures))}/{len(failures)}):")
        for rid, name in failures[:20]:
            print(f"    - {name}")

    # Save report
    report = CACHE_DIR / "backfill_report.json"
    report.write_text(json.dumps({
        "timestamp": now(),
        "targets": len(rows),
        "counts": counts,
        "total_resolved": sum(counts.values()),
        "db_coverage_pct": round(have / total * 100, 2) if total else 0,
        "remaining_jp_unknown": remaining_jp,
        "unresolved_samples": [n for _, n in failures[:50]],
    }, ensure_ascii=False, indent=2))
    print(f"  Report saved to    : {report}")

    conn.close()


if __name__ == "__main__":
    main()
