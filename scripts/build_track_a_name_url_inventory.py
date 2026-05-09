#!/usr/bin/env python3
"""Build Track A inventory: formal name, official URL, koeki URL, fiscal year.

This is an evidence inventory, not the final amount extractor. It ranks
JFC-top81-excluded, koeki-registered medium foundations and deduplicates by
normalized foundation name.
"""
from __future__ import annotations

import csv
import json
import re
import sqlite3
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "corporate_research_grants.sqlite"
KOEKI_ALL = ROOT / "data" / "koeki_all_foundations.json"
KOEKI_RESEARCH = ROOT / "data" / "koeki_research_foundations.json"
OUT = ROOT / "research_results" / "trackA_300_name_url_koeki_inventory_2026-05-09.csv"
SUMMARY = ROOT / "research_results" / "trackA_300_name_url_koeki_inventory_summary_2026-05-09.md"

LEGAL_PREFIXES = (
    "公益財団法人",
    "一般財団法人",
    "公益社団法人",
    "一般社団法人",
    "特定非営利活動法人",
    "認定特定非営利活動法人",
    "公益財団",
    "一般財団",
    "（公財）",
    "(公財)",
    "（一財）",
    "(一財)",
)

KNOWN_DISCLOSURE_YEARS = {
    "武田科学振興財団": "2024",
    "三菱財団": "2024",
    "上原記念生命科学財団": "2024",
    "内藤記念科学振興財団": "2024",
    "トヨタ財団": "2024",
    "旭硝子財団": "2024",
    "セコム科学技術振興財団": "2024",
    "中谷医工計測技術振興財団": "2024",
    "中谷財団": "2024",
    "持田記念医学薬学振興財団": "2024",
    "市村清新技術財団": "2024",
    "キヤノン財団": "2024",
    "立石科学技術振興財団": "2024",
    "船井情報科学振興財団": "2024",
    "スズキ財団": "2024",
    "岩谷直治記念財団": "2024",
    "小笠原敏晶記念財団": "2024",
    "電気通信普及財団": "2024",
    "本庄国際奨学財団": "2024",
    "第一三共生命科学研究振興財団": "2024",
    "国際科学振興財団": "2024",
    "ひと・健康・未来研究財団": "2024",
}


def fw_to_hw(s: str) -> str:
    return s.translate(
        str.maketrans(
            "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ０１２３４５６７８９",
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
        )
    )


def norm(name: str) -> str:
    n = fw_to_hw(name or "").strip()
    for _ in range(3):
        changed = False
        for p in LEGAL_PREFIXES:
            if n.startswith(p):
                n = n[len(p) :].strip()
                changed = True
                break
        if not changed:
            break
    n = re.sub(r"[（(].*?[)）]\s*$", "", n)
    n = n.replace("　", "").replace(" ", "").replace("・", "")
    n = n.replace("（", "").replace("）", "").replace("(", "").replace(")", "")
    return n.lower()


def core_name(name: str) -> str:
    n = name or ""
    for p in LEGAL_PREFIXES:
        n = n.replace(p, "")
    return n.replace("　", " ").strip()


def koeki_search_url(name: str) -> str:
    return "https://www.koeki-info.go.jp/corporations/corporation-search?keyword=" + quote(name)


def source_rank(row: dict) -> int:
    meta = row.get("metadata") or ""
    score = 0
    if row.get("url"):
        score += 50
    if row.get("annual_grant_amount"):
        score += 40
    if row.get("total_assets"):
        score += 20
    if row.get("jfc_rank"):
        score += 20
    if "koeki" in meta.lower():
        score += 10
    return score


def fiscal_year(row: dict) -> str:
    name = row["name"]
    for key, year in KNOWN_DISCLOSURE_YEARS.items():
        if key in name:
            return year
    y = row.get("annual_grant_year")
    if y:
        return str(y)
    if row.get("jfc_rank"):
        return "2022"
    return ""


def main() -> None:
    koeki_rows = json.loads(KOEKI_ALL.read_text(encoding="utf-8"))
    koeki_by_norm = {norm(r["name"]): r for r in koeki_rows}
    research_rows = json.loads(KOEKI_RESEARCH.read_text(encoding="utf-8"))
    research_score_by_norm = {norm(r["name"]): r.get("research_score") or 0 for r in research_rows}

    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    db_rows = [
        dict(r)
        for r in con.execute(
            """
            SELECT id, name, legal_form, foundation_subtype, prefecture, url,
                   annual_grant_amount, annual_grant_year, total_assets, jfc_rank,
                   metadata
            FROM organizations
            WHERE name LIKE '%財団%'
              AND COALESCE(jfc_rank, 9999) > 81
              AND foundation_subtype IN ('corporate','academic','individual','group','govt','intl','other')
            """
        )
    ]

    db_grouped: dict[str, list[dict]] = {}
    for r in db_rows:
        k = norm(r["name"])
        if not k:
            continue
        if "/" in r["name"] or "／" in r["name"]:
            continue
        db_grouped.setdefault(k, []).append(r)

    selected = []
    for k, koeki in koeki_by_norm.items():
        if "財団" not in koeki["name"]:
            continue
        group = db_grouped.get(k, [])
        if group:
            group.sort(key=source_rank, reverse=True)
            r = group[0]
        else:
            r = {
                "name": koeki["name"],
                "url": "",
                "annual_grant_amount": None,
                "annual_grant_year": "",
                "total_assets": None,
                "jfc_rank": None,
                "foundation_subtype": "",
            }
        jfc_rank = r.get("jfc_rank")
        if jfc_rank and int(jfc_rank) <= 81:
            continue
        amount = r.get("annual_grant_amount") or 0
        research_score = research_score_by_norm.get(k, 0)
        if not amount and not jfc_rank and research_score <= 0 and not r.get("url"):
            continue
        selected.append(
            {
                "rank_in_inventory": 0,
                "canonical_key": k,
                "formal_name": koeki["name"] if koeki else r["name"],
                "db_name": r["name"],
                "official_url": r.get("url") or "",
                "koeki_info_url": koeki_search_url(koeki["name"] if koeki else r["name"]),
                "koeki_registered": "yes",
                "admin_agency": koeki.get("admin", ""),
                "prefecture_or_address": koeki.get("address", r.get("prefecture") or ""),
                "financial_document_year": fiscal_year(r),
                "annual_grant_amount_yen_existing": amount if amount else "",
                "total_assets_yen_existing": r.get("total_assets") or "",
                "jfc_rank_2022": r.get("jfc_rank") or "",
                "foundation_subtype": r.get("foundation_subtype") or "",
                "koeki_research_score": research_score,
                "duplicate_rows_collapsed": max(1, len(group)),
                "verification_status": "official_url_and_koeki_name_matched" if r.get("url") else "koeki_name_matched_official_url_needed",
            }
        )

    selected.sort(
        key=lambda r: (
            -(int(r["annual_grant_amount_yen_existing"]) if r["annual_grant_amount_yen_existing"] else 0),
            int(r["jfc_rank_2022"]) if r["jfc_rank_2022"] else 9999,
            -int(r["koeki_research_score"]),
            r["formal_name"],
        )
    )
    selected = selected[:300]
    for i, r in enumerate(selected, 1):
        r["rank_in_inventory"] = i

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(selected[0]))
        w.writeheader()
        w.writerows(selected)

    matched = sum(1 for r in selected if r["koeki_registered"] == "yes")
    urls = sum(1 for r in selected if r["official_url"])
    years = sum(1 for r in selected if r["financial_document_year"])
    collapsed = sum(int(r["duplicate_rows_collapsed"]) - 1 for r in selected)
    SUMMARY.write_text(
        "\n".join(
            [
                "# Track A 300団体 正式名称・URL・公益法人info照合サマリ",
                "",
                f"- 出力CSV: `{OUT}`",
                f"- 対象: JFC 2022 top81を除く、公益法人info登録済みの財団系組織から300件を抽出",
                f"- 公益法人info登録名一致: {matched}/300",
                f"- 公式URLあり: {urls}/300",
                f"- 財務書類年度または既存金額年度あり: {years}/300",
                f"- 名寄せで畳み込んだ重複行: {collapsed}行",
                "",
                "## 判定メモ",
                "",
                "- `koeki_info_url` は公益法人infoの法人名検索URL。詳細ページIDを未取得の法人も再検索可能にした。",
                "- `financial_document_year=2024` は公式情報公開ページで2024年度事業報告・決算書類が確認できる主要財団、または既存抽出対象レジストリで2024年度を対象にしている法人。",
                "- `2024est` は既存DBの推定値であり、公益法人info財務PDFからの再抽出が必要。",
                "- `verification_status=koeki_name_matched_official_url_needed` は公益法人info登録名は確認済みだが、公式URLの別途照合が必要。",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    print(OUT)
    print(SUMMARY)
    print({"rows": len(selected), "koeki_matched": matched, "official_urls": urls, "years": years, "collapsed": collapsed})


if __name__ == "__main__":
    main()
