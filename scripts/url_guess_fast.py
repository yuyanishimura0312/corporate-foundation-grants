#!/usr/bin/env python3
"""Fast URL discovery via heuristic guess only (no DDG search).

Try common URL patterns based on foundation name's romaji core. Verify by
HTTP HEAD/GET and title/h1 keyword match. Much faster than full search.
"""
from __future__ import annotations
import argparse
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

DB = Path("/Users/nishimura+/projects/apps/corporate-foundation-grants/corporate_research_grants.sqlite")
USER_AGENT = "Mozilla/5.0 (compatible; CFG-DB/1.0)"
TIMEOUT = 5

KANA_TO_LATIN = {
    "ロームミュージック": "rohm-music-fdn",
    "ローム": "rohm",
    "トヨタ": "toyota", "ホンダ": "honda", "ニッサン": "nissan",
    "ソニー": "sony", "パナソニック": "panasonic", "三菱": "mitsubishi",
    "三井": "mitsui", "住友": "sumitomo", "サントリー": "suntory",
    "キヤノン": "canon", "キャノン": "canon", "リコー": "ricoh",
    "富士通": "fujitsu", "富士フイルム": "fujifilm", "東芝": "toshiba",
    "日立": "hitachi", "村田": "murata", "京セラ": "kyocera",
    "オムロン": "omron", "デンソー": "denso", "コニカミノルタ": "konica-minolta",
    "オリンパス": "olympus", "ニコン": "nikon", "アサヒ": "asahi",
    "キリン": "kirin", "サッポロ": "sapporo", "明治": "meiji",
    "森永": "morinaga", "ヤクルト": "yakult", "資生堂": "shiseido",
    "花王": "kao", "ライオン": "lion", "セコム": "secom",
    "リクルート": "recruit", "ベネッセ": "benesse", "野村": "nomura",
    "大和": "daiwa", "みずほ": "mizuho", "ソンポ": "sompo",
    "ロッテ": "lotte", "りそな": "resona", "稲盛": "inamori",
    "笹川": "sasakawa", "鹿島": "kajima", "武田": "takeda",
    "上原": "uehara", "テルモ": "terumo", "中谷": "nakatani",
    "島津": "shimadzu", "天田": "amada", "持田": "mochida",
    "市村": "ichimura", "ポーラ": "pola", "シャープ": "sharp",
    "東洋": "toyo", "JT": "jt", "JR": "jr",
    "アステラス": "astellas", "塩野義": "shionogi", "第一三共": "daiichisankyo",
    "エーザイ": "eisai", "中外": "chugai", "大塚": "otsuka",
    "ファイザー": "pfizer", "メルク": "merck", "GSK": "gsk",
    "ヤンマー": "yanmar", "クボタ": "kubota", "コマツ": "komatsu",
    "ブリヂストン": "bridgestone",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def latin_cores(name: str) -> list[str]:
    """Generate romaji core candidates from foundation name."""
    n = re.sub(r"^(公益|一般|特定非営利活動|認定特定非営利活動)?(財団法人|社団法人)\s*", "", name).strip()
    n = re.sub(r"^[（(].+?[)）]\s*", "", n).strip()  # strip leading abbreviation prefix
    n = re.sub(r"[（(].*?[)）]\s*$", "", n).strip()  # strip trailing parenthetical
    n = re.sub(r"(財団|奨学会|記念会|振興会|事業団|基金|協会|会|学会|研究所|研究会|機構|センター)$", "", n).strip()

    cores = []
    # Match longest kana keys first
    for jp, en in sorted(KANA_TO_LATIN.items(), key=lambda x: -len(x[0])):
        if jp in name or jp in n:
            cores.append(en)

    # Extract direct ASCII from full-width-converted name
    han = n.translate(str.maketrans(
        "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ",
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
    ))
    ascii_part = re.sub(r"[^A-Za-z0-9]", "", han)
    if 3 <= len(ascii_part) <= 25 and ascii_part.lower() not in [c.lower() for c in cores]:
        cores.append(ascii_part.lower())

    return cores[:4]  # try top 4 candidates


def candidate_urls(cores: list[str]) -> list[str]:
    urls = []
    for core in cores:
        for tmpl in (
            "https://www.{c}-foundation.or.jp",
            "https://{c}-foundation.or.jp",
            "https://www.{c}foundation.or.jp",
            "https://www.{c}-zaidan.or.jp",
            "https://{c}-zaidan.or.jp",
            "https://www.{c}.or.jp",
            "https://{c}.or.jp",
            "https://www.{c}.org",
            "https://{c}.org",
            "https://www.{c}.jp",
        ):
            urls.append(tmpl.format(c=core))
    return list(dict.fromkeys(urls))


def verify(url: str, name: str, name_kw: list[str]) -> bool:
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT,
                         allow_redirects=True)
        if r.status_code != 200:
            return False
        soup = BeautifulSoup(r.text, "html.parser")
        title = (soup.title.string if soup.title else "") or ""
        h1 = soup.h1.get_text() if soup.h1 else ""
        body_text = (title + " " + h1)[:1500]
        for kw in name_kw:
            if kw and kw in body_text:
                return True
        return False
    except Exception:
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=200)
    args = ap.parse_args()

    conn = sqlite3.connect(DB, timeout=120)
    conn.execute("PRAGMA busy_timeout = 120000")
    cur = conn.cursor()

    cur.execute("""SELECT id, name FROM organizations
                   WHERE (url IS NULL OR url = '')
                     AND legal_form IN ('公益財団法人','一般財団法人','公益社団法人','一般社団法人')
                   ORDER BY annual_grant_amount DESC NULLS LAST, jfc_rank ASC NULLS LAST, name
                   LIMIT ?""", (args.limit,))
    targets = cur.fetchall()
    print(f"Targets: {len(targets)}")

    found = 0
    not_found = 0
    started = time.time()

    for i, (rid, name) in enumerate(targets, 1):
        cores = latin_cores(name)
        if not cores:
            not_found += 1
            continue

        # Foundation name keywords for verification
        name_kw_full = name
        # Extract核 (without legal prefix)
        kw = re.sub(r"^(公益|一般)?(財団法人|社団法人)\s*", "", name).strip()
        kw = re.sub(r"(財団|基金|振興会|奨学会|事業団|研究会|学会|協会|機構|センター)$", "", kw).strip()
        keywords = [name_kw_full, kw]
        if len(kw) > 3:
            keywords.append(kw[:5])  # partial

        urls = candidate_urls(cores)
        result_url = None
        for url in urls[:8]:  # try up to 8 candidates per org
            if verify(url, name, keywords):
                result_url = url
                break
            time.sleep(0.3)

        if result_url:
            cur.execute("UPDATE organizations SET url = ?, updated_at=datetime('now','localtime') WHERE id = ?",
                        (result_url, rid))
            found += 1
            elapsed = time.time() - started
            rate = found / max(1, found + not_found) * 100
            print(f"[{i}/{len(targets)}] ({elapsed:.0f}s, found={found}, rate={rate:.1f}%) {name}")
            print(f"    -> {result_url}")
            if i % 10 == 0:
                conn.commit()
        else:
            not_found += 1

    conn.commit()
    elapsed = time.time() - started
    print(f"\nDone: {found}/{len(targets)} found ({found/max(1,len(targets))*100:.1f}%)")
    print(f"Elapsed: {elapsed:.0f}s ({elapsed/max(1,len(targets)):.1f}s/org)")
    conn.close()


if __name__ == "__main__":
    main()
