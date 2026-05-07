#!/usr/bin/env python3
"""Extract foundation data from Codex 40 Phase 4 output text files.

Each team*.txt file contains a Codex agent's research report with
foundation names, URLs, addresses, and other structured data embedded
in markdown tables and prose.

This script extracts:
  - Foundation names (and likely URLs)
  - Annual grant amounts (parsed from text)
  - Awardee mentions
  - JFC rank data

Output: research_results/codex_phase4_extracted.json
        research_results/codex_phase4_summary.md
"""
from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = Path("/tmp/codex-team-20260508-082225")
OUT_JSON = ROOT / "research_results" / "codex_phase4_extracted.json"
OUT_MD = ROOT / "research_results" / "codex_phase4_summary.md"


# Match Japanese foundation names with optional 公益/一般 prefix
# Captures: legal_form, name_core
NAME_RE = re.compile(
    r"((?:公益|一般)?(?:財団法人|社団法人))\s*([一-鿿々ヵヶ゠-ヿ぀-ゟ　-〿A-Za-z0-9・]{2,40}?(?:財団|基金|振興会|奨学会|事業団|研究会|学会|協会|機構|センター|会|学院))"
)
# Match URLs
URL_RE = re.compile(r"https?://[\w./\-?=&%#~]+", re.IGNORECASE)
# Match yen amounts e.g. 1.2億円 / 5,000万円 / 100百万円 / 12億7,000万円
YEN_RE = re.compile(r"((?:[0-9０-９,，][0-9０-９,，.]*\s*(?:億|千万|百万|万)?\s*円|億\s*[0-9]+))")


def parse_yen(s: str) -> int | None:
    """Parse a Japanese yen amount string into an integer (yen)."""
    s = s.replace("，", ",").replace(",", "").strip()
    s = s.replace("０", "0").replace("１", "1").replace("２", "2").replace("３", "3")
    s = s.replace("４", "4").replace("５", "5").replace("６", "6").replace("７", "7")
    s = s.replace("８", "8").replace("９", "9")
    m = re.search(r"([0-9.]+)\s*億\s*([0-9.]+)?\s*万?\s*円", s)
    if m:
        oku = float(m.group(1)) * 100_000_000
        man = float(m.group(2)) * 10_000 if m.group(2) else 0
        return int(oku + man)
    m = re.search(r"([0-9.]+)\s*億\s*円", s)
    if m:
        return int(float(m.group(1)) * 100_000_000)
    m = re.search(r"([0-9.]+)\s*千万\s*円", s)
    if m:
        return int(float(m.group(1)) * 10_000_000)
    m = re.search(r"([0-9.]+)\s*百万\s*円", s)
    if m:
        return int(float(m.group(1)) * 1_000_000)
    m = re.search(r"([0-9.]+)\s*万\s*円", s)
    if m:
        return int(float(m.group(1)) * 10_000)
    return None


def normalize_name(name: str) -> str:
    s = re.sub(r"^(公益|一般)(財団法人|社団法人)\s*", "", name)
    s = s.replace("　", "").replace(" ", "")
    return s.strip()


def extract_foundations(text: str) -> dict:
    """Extract foundation entries from a Codex report text."""
    result: dict[str, dict] = {}

    # Pattern 1: lines with foundation name + URL (markdown table or list)
    for match in NAME_RE.finditer(text):
        legal = match.group(1)
        core = match.group(2)
        full_name = f"{legal}{core}".strip()
        norm = normalize_name(full_name)
        if not norm or len(norm) < 2:
            continue
        if norm not in result:
            result[norm] = {
                "name": full_name,
                "name_core": core,
                "legal_form": legal,
                "urls": [],
                "amounts": [],
                "context_snippets": [],
            }
        # Search 200 chars around for URL/amount/context
        start = max(0, match.start() - 100)
        end = min(len(text), match.end() + 300)
        ctx = text[start:end]
        for url in URL_RE.findall(ctx):
            if "wikipedia" not in url.lower() and url not in result[norm]["urls"]:
                result[norm]["urls"].append(url.rstrip(".,)"))
        for amt_match in YEN_RE.findall(ctx):
            yen = parse_yen(amt_match)
            if yen and yen > 1_000_000:  # >1M yen, likely a grant amount
                if yen not in result[norm]["amounts"]:
                    result[norm]["amounts"].append(yen)
        if len(result[norm]["context_snippets"]) < 2:
            snippet = ctx.replace("\n", " ")[:200]
            result[norm]["context_snippets"].append(snippet)

    return result


def main():
    if not RESULTS_DIR.exists():
        print(f"No Codex results: {RESULTS_DIR}")
        return

    files = sorted(RESULTS_DIR.glob("team*.txt"))
    print(f"Processing {len(files)} Codex output files")

    aggregated: dict[str, dict] = {}
    by_team: dict[str, int] = {}

    for f in files:
        text = f.read_text(encoding="utf-8", errors="replace")
        extracted = extract_foundations(text)
        by_team[f.name] = len(extracted)
        for norm, data in extracted.items():
            if norm not in aggregated:
                aggregated[norm] = {
                    **data,
                    "sources": [f.name],
                }
            else:
                # Merge URLs/amounts
                for u in data["urls"]:
                    if u not in aggregated[norm]["urls"]:
                        aggregated[norm]["urls"].append(u)
                for a in data["amounts"]:
                    if a not in aggregated[norm]["amounts"]:
                        aggregated[norm]["amounts"].append(a)
                if f.name not in aggregated[norm]["sources"]:
                    aggregated[norm]["sources"].append(f.name)

    print(f"Total unique foundations extracted: {len(aggregated)}")
    print(f"Foundations with at least 1 URL: {sum(1 for v in aggregated.values() if v['urls'])}")
    print(f"Foundations with amount: {sum(1 for v in aggregated.values() if v['amounts'])}")

    # Save JSON
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump({
            "extracted_count": len(aggregated),
            "by_team": by_team,
            "foundations": {k: v for k, v in sorted(aggregated.items())},
        }, f, ensure_ascii=False, indent=2)

    # Save markdown summary (top 100 by signal richness)
    scored = sorted(
        aggregated.items(),
        key=lambda kv: (len(kv[1]["urls"]) + len(kv[1]["amounts"]) * 2 + len(kv[1]["sources"])),
        reverse=True,
    )
    with open(OUT_MD, "w", encoding="utf-8") as f:
        f.write("# Codex Phase 4 — 抽出財団リスト（信号順 上位100）\n\n")
        f.write(f"対象: {len(files)}ファイル / 抽出ユニーク財団: {len(aggregated)}\n\n")
        f.write("| 財団名 | URL候補 | 助成額候補 | 出現Team数 |\n")
        f.write("|---|---|---|---|\n")
        for norm, d in scored[:100]:
            urls = "<br>".join(d["urls"][:3]) if d["urls"] else "—"
            amounts = ", ".join(f"{a:,}" for a in d["amounts"][:3]) if d["amounts"] else "—"
            f.write(f"| {d['name']} | {urls} | {amounts} | {len(d['sources'])} |\n")
        f.write(f"\n## Team別抽出件数\n\n")
        for team, count in sorted(by_team.items()):
            f.write(f"- {team}: {count}\n")

    print(f"\nSaved: {OUT_JSON}")
    print(f"Saved: {OUT_MD}")


if __name__ == "__main__":
    main()
