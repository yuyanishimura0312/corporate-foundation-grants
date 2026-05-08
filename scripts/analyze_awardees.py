#!/usr/bin/env python3
"""Awardees network analysis — affiliation concentration, foundation matrix, year trends."""
from __future__ import annotations
import json
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from scripts.lib.affiliation_normalize import normalize_affiliation

DB = ROOT / "corporate_research_grants.sqlite"
OUT_JSON = ROOT / "data" / "awardees_network_analysis.json"
OUT_REPORT = ROOT / "research_results" / "awardees_network_report.md"


def hhi(counts: list[int]) -> float:
    """Herfindahl-Hirschman Index normalized to 0-10000."""
    total = sum(counts)
    if total == 0:
        return 0.0
    shares = [c / total for c in counts]
    return sum(s * s * 10000 for s in shares)


def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute(
        """SELECT r.awardee_name, r.awardee_affiliation, r.fiscal_year,
                  r.award_amount, o.name AS foundation
             FROM grant_results r
             JOIN grant_calls c ON c.id = r.call_id
             JOIN grant_programs p ON p.id = c.program_id
             JOIN organizations o ON o.id = p.organization_id"""
    )
    rows = cur.fetchall()
    conn.close()

    print(f"Total awardees: {len(rows)}")

    # Normalize affiliations
    normalized = []
    for name, aff, year, amount, foundation in rows:
        canonical = normalize_affiliation(aff or "")
        normalized.append({
            "name": (name or "").strip(),
            "affiliation_raw": aff or "",
            "affiliation": canonical,
            "year": year,
            "amount": amount,
            "foundation": foundation,
        })

    # Affiliation distribution
    aff_counts = Counter(r["affiliation"] for r in normalized if r["affiliation"])
    top_affiliations = aff_counts.most_common(50)

    # Affiliation x foundation matrix
    matrix = defaultdict(lambda: defaultdict(int))
    for r in normalized:
        if r["affiliation"] and r["foundation"]:
            matrix[r["affiliation"]][r["foundation"]] += 1

    # By year
    year_counts = Counter(r["year"] for r in normalized if r["year"])

    # Multi-foundation awardees (same name across multiple foundations)
    name_foundation = defaultdict(set)
    for r in normalized:
        if r["name"]:
            name_foundation[r["name"]].add(r["foundation"])
    multi_fund = {n: list(f) for n, f in name_foundation.items() if len(f) > 1}

    # Concentration (HHI) on affiliations
    concentration = hhi(list(aff_counts.values()))

    # Foundation totals
    foundation_totals = Counter(r["foundation"] for r in normalized)

    result = {
        "total_awardees": len(rows),
        "unique_affiliations": len(aff_counts),
        "unique_awardees": len(set(r["name"] for r in normalized if r["name"])),
        "concentration_hhi": round(concentration, 1),
        "top_affiliations_50": [{"affiliation": a, "count": c} for a, c in top_affiliations],
        "by_year": dict(sorted(year_counts.items())),
        "by_foundation": dict(foundation_totals),
        "multi_foundation_awardees_count": len(multi_fund),
        "multi_foundation_awardees_sample": dict(list(multi_fund.items())[:30]),
        "affiliation_x_foundation_top20": {
            aff: dict(matrix[aff])
            for aff, _ in top_affiliations[:20]
        },
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    # Generate markdown report
    with open(OUT_REPORT, "w", encoding="utf-8") as f:
        f.write("# 採択者ネットワーク分析レポート\n\n")
        f.write(f"対象: {len(rows):,}件採択者 / {result['unique_awardees']:,}人 / {result['unique_affiliations']:,}機関\n\n")
        f.write(f"**集中度（HHI）**: {result['concentration_hhi']:.1f}\n")
        f.write("（HHI: 1500未満=低集中、1500-2500=中、2500超=高集中）\n\n")

        f.write("## 上位機関（採択者数 Top 20）\n\n")
        f.write("| 順位 | 機関 | 採択者数 | 割合 |\n|---|---|---|---|\n")
        for i, (a, c) in enumerate(top_affiliations[:20], 1):
            pct = c / len(rows) * 100
            f.write(f"| {i} | {a} | {c} | {pct:.1f}% |\n")

        f.write("\n## 機関×財団マトリクス（上位10機関）\n\n")
        foundations_list = sorted(foundation_totals.keys())
        f.write("| 機関 | " + " | ".join(foundations_list) + " | 合計 |\n")
        f.write("|" + "---|" * (len(foundations_list) + 2) + "\n")
        for aff, _ in top_affiliations[:10]:
            row = matrix[aff]
            cells = [str(row.get(fnd, 0)) for fnd in foundations_list]
            total_cell = sum(row.values())
            f.write(f"| {aff} | " + " | ".join(cells) + f" | {total_cell} |\n")

        f.write(f"\n## 複数財団重複採択者\n\n")
        f.write(f"複数財団から採択された研究者: **{len(multi_fund)}人**\n\n")
        f.write("### 重複採択者サンプル（上位30）\n\n")
        for n, fs in list(multi_fund.items())[:30]:
            f.write(f"- **{n}**: {', '.join(fs)}\n")

        f.write(f"\n## 年度別 採択件数\n\n")
        f.write("| 年度 | 件数 |\n|---|---|\n")
        for y in sorted(year_counts.keys()):
            f.write(f"| {y} | {year_counts[y]} |\n")

        f.write(f"\n## 財団別 採択件数\n\n")
        for fnd, c in foundation_totals.most_common():
            f.write(f"- {fnd}: {c}件\n")

    print(f"\nResults:")
    print(f"  Concentration HHI: {concentration:.1f}")
    print(f"  Top affiliation: {top_affiliations[0][0]} ({top_affiliations[0][1]}件)")
    print(f"  Multi-foundation awardees: {len(multi_fund)}")
    print(f"  Saved: {OUT_JSON}")
    print(f"  Saved: {OUT_REPORT}")


if __name__ == "__main__":
    main()
