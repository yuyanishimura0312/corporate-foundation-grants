#!/usr/bin/env python3
"""S1.2: project_titleからキーワード抽出してgrant_calls.keywordsへ格納

Strategy: noun-phrase extraction without external NLP. Uses simple Japanese
patterns (kanji-katakana sequences). For each call, aggregate top-5 keywords
from awardee project_titles.
"""
from __future__ import annotations
import re
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

DB = Path("/Users/nishimura+/projects/apps/corporate-foundation-grants/corporate_research_grants.sqlite")

# Stopwords for Japanese title noise
STOP = {
    "研究", "解析", "開発", "応用", "活用", "調査", "検討", "考察", "分析",
    "における", "及び", "並びに", "について", "に関する", "のための",
    "における", "向け", "新規", "新たな", "高度", "次世代", "革新",
    "創出", "創成", "実現", "確立", "推進", "向上", "ための", "もの",
    "こと", "ベース", "プロジェクト", "化", "性", "型", "学", "法",
}

# Domain markers — high-signal keywords (extracted as priority)
DOMAIN_MARKERS = [
    # 生命医学
    "がん", "癌", "免疫", "細胞", "遺伝", "ゲノム", "DNA", "RNA", "タンパク",
    "脳", "神経", "認知", "免疫", "感染", "ウイルス", "細菌", "抗体", "抗原",
    "幹細胞", "iPS", "再生医療", "創薬", "抗がん剤", "ワクチン",
    "代謝", "糖尿病", "高血圧", "心血管", "循環器", "呼吸器", "消化器",
    # 化学
    "触媒", "高分子", "ポリマー", "結晶", "ナノ", "界面", "表面",
    # 物理
    "量子", "光", "レーザー", "プラズマ", "素粒子", "宇宙", "天文",
    # 工学
    "半導体", "電池", "太陽電池", "燃料電池", "ロボット", "AI",
    "機械学習", "深層学習", "アルゴリズム", "ネットワーク", "通信",
    # 環境
    "気候変動", "温暖化", "CO2", "再生可能", "サステナ", "生物多様性",
    # 社会
    "教育", "格差", "貧困", "高齢", "少子", "ジェンダー",
]


def extract_kanji_phrases(text: str, min_len: int = 2, max_len: int = 8) -> list[str]:
    """Extract kanji+katakana noun phrases from Japanese text."""
    if not text:
        return []
    # Match sequences of kanji/katakana/hiragana mix (likely noun phrases)
    pattern = r"[一-鿿々〆ヵヶ]{" + str(min_len) + r"," + str(max_len) + r"}"
    phrases = re.findall(pattern, text)
    # Also extract katakana-only
    kana_pattern = r"[ァ-ヶー]{3,15}"
    phrases.extend(re.findall(kana_pattern, text))
    return phrases


def extract_keywords(titles: list[str], top_n: int = 8) -> list[str]:
    """Aggregate keywords from a list of titles."""
    counter = Counter()
    domain_hits = Counter()

    for t in titles:
        # Domain markers (high signal)
        for marker in DOMAIN_MARKERS:
            if marker in t:
                domain_hits[marker] += 1
        # Generic noun phrases
        for p in extract_kanji_phrases(t, 2, 6):
            if p in STOP or len(p) < 2:
                continue
            counter[p] += 1

    # Combine: prefer domain markers, then frequent phrases
    keywords = []
    for k, _ in domain_hits.most_common(top_n // 2):
        keywords.append(k)
    for k, _ in counter.most_common():
        if k in keywords:
            continue
        keywords.append(k)
        if len(keywords) >= top_n:
            break
    return keywords[:top_n]


def main():
    conn = sqlite3.connect(DB, timeout=60)
    conn.execute("PRAGMA busy_timeout = 60000")
    cur = conn.cursor()

    # Group awardee titles by call_id
    cur.execute("""SELECT r.call_id, r.project_title FROM grant_results r
                   WHERE r.project_title IS NOT NULL AND r.project_title != ''""")
    titles_by_call = defaultdict(list)
    for cid, title in cur.fetchall():
        titles_by_call[cid].append(title)

    print(f"Calls with awardee titles: {len(titles_by_call)}")

    # Update grant_calls.keywords
    updated_calls = 0
    for cid, titles in titles_by_call.items():
        kws = extract_keywords(titles, top_n=8)
        if kws:
            kw_str = ",".join(kws)
            cur.execute(
                "UPDATE grant_calls SET keywords=?, updated_at=datetime('now','localtime') WHERE id=? AND (keywords IS NULL OR keywords='')",
                (kw_str, cid),
            )
            if cur.rowcount > 0:
                updated_calls += 1

    conn.commit()
    print(f"grant_calls.keywords backfilled: {updated_calls}")

    # Also extract from program description for calls without awardees
    cur.execute("""SELECT c.id, p.name, p.description, p.purpose
                   FROM grant_calls c
                   JOIN grant_programs p ON c.program_id = p.id
                   WHERE (c.keywords IS NULL OR c.keywords = '')""")
    no_kw_calls = cur.fetchall()
    print(f"Calls still without keywords: {len(no_kw_calls)}")

    program_extracted = 0
    for cid, name, desc, purpose in no_kw_calls:
        text = " ".join(filter(None, [name, desc, purpose]))
        if not text:
            continue
        kws = extract_keywords([text], top_n=6)
        if kws:
            cur.execute(
                "UPDATE grant_calls SET keywords=?, updated_at=datetime('now','localtime') WHERE id=?",
                (",".join(kws), cid),
            )
            program_extracted += 1
    conn.commit()
    print(f"From program text: +{program_extracted}")

    cur.execute("SELECT COUNT(*) FROM grant_calls WHERE keywords IS NOT NULL AND keywords != ''")
    final = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM grant_calls")
    t = cur.fetchone()[0]
    print(f"\nFinal: keywords {final}/{t} ({final/t*100:.1f}%)")
    conn.close()


if __name__ == "__main__":
    main()
