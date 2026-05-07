#!/usr/bin/env python3
"""
Generate textbook-style dashboard for Corporate Foundation Grants DB.

Output: ~/projects/apps/miratuku-news-v2/dashboards/cfg.html
Template source: miratuku-news-v2/dashboards/_template-akashiro.html (赤白CI #CC1400)

Sections:
  1. 概観 (overview stats)
  2. 財団分類 (taxonomy: subtype/legal_form)
  3. 助成規模ティア (size tiers)
  4. 親企業ネットワーク
  5. 地域分布
  6. 主要研究助成プログラム
  7. 公募カレンダー
  8. データソース・カバレッジ
  9. 使い方
"""
from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "corporate_research_grants.sqlite"
TEMPLATE = Path("/Users/nishimura+/projects/apps/miratuku-news-v2/dashboards/_template-akashiro.html")
OUTPUT = Path("/Users/nishimura+/projects/apps/miratuku-news-v2/dashboards/cfg.html")


SUBTYPE_LABELS = {
    "corporate": "企業財団",
    "individual": "個人財団",
    "group": "グループ企業財団",
    "academic": "学術系財団",
    "govt": "政府系・独法",
    "intl": "国際機関系",
    "ngo": "NPO/NGO系",
    "other": "その他・分類未確定",
}


def fetch_stats(conn: sqlite3.Connection) -> dict:
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row

    stats = {}
    stats["total_orgs"] = cur.execute("SELECT COUNT(*) FROM organizations").fetchone()[0]
    stats["total_programs"] = cur.execute("SELECT COUNT(*) FROM grant_programs").fetchone()[0]
    stats["total_calls"] = cur.execute("SELECT COUNT(*) FROM grant_calls").fetchone()[0]

    # Subtype distribution
    cur.execute("""
        SELECT COALESCE(foundation_subtype,'other') AS k, COUNT(*) AS c
        FROM organizations GROUP BY k ORDER BY c DESC
    """)
    stats["subtype"] = [(r[0], r[1]) for r in cur.fetchall()]

    # Legal form
    cur.execute("""
        SELECT COALESCE(legal_form,'未設定') AS k, COUNT(*) AS c
        FROM organizations GROUP BY k ORDER BY c DESC
    """)
    stats["legal_form"] = [(r[0], r[1]) for r in cur.fetchall()]

    # Prefecture top 15
    cur.execute("""
        SELECT COALESCE(prefecture,'未設定') AS k, COUNT(*) AS c
        FROM organizations GROUP BY k ORDER BY c DESC LIMIT 15
    """)
    stats["prefecture"] = [(r[0], r[1]) for r in cur.fetchall()]

    # Top parent companies
    cur.execute("""
        SELECT corporate_parent AS k, COUNT(*) AS c
        FROM organizations
        WHERE corporate_parent IS NOT NULL AND corporate_parent != ''
        GROUP BY k ORDER BY c DESC LIMIT 25
    """)
    stats["parents"] = [(r[0], r[1]) for r in cur.fetchall()]

    # Annual grant amount tiers
    cur.execute("SELECT annual_grant_amount FROM organizations WHERE annual_grant_amount IS NOT NULL")
    amounts = [r[0] for r in cur.fetchall()]
    tiers = Counter()
    for a in amounts:
        if a is None:
            continue
        if a < 50_000_000:
            tiers["T1: <5000万円"] += 1
        elif a < 100_000_000:
            tiers["T2: 5000万-1億円"] += 1
        elif a < 500_000_000:
            tiers["T3: 1-5億円"] += 1
        elif a < 1_000_000_000:
            tiers["T4: 5-10億円"] += 1
        elif a < 5_000_000_000:
            tiers["T5: 10-50億円"] += 1
        else:
            tiers["T6: 50億円以上"] += 1
    stats["tiers"] = sorted(tiers.items(), key=lambda x: x[0])
    stats["amount_known"] = len(amounts)

    # Top foundations by amount
    cur.execute("""
        SELECT name, corporate_parent, annual_grant_amount, prefecture, jfc_rank
        FROM organizations
        WHERE annual_grant_amount IS NOT NULL
        ORDER BY annual_grant_amount DESC LIMIT 30
    """)
    stats["top_funded"] = [dict(zip(["name", "parent", "amount", "pref", "jfc_rank"], r)) for r in cur.fetchall()]

    # Programs sample
    cur.execute("""
        SELECT p.name, o.name, p.category, p.total_budget
        FROM grant_programs p
        JOIN organizations o ON p.organization_id = o.id
        ORDER BY p.total_budget DESC NULLS LAST LIMIT 30
    """)
    rows = cur.fetchall()
    stats["top_programs"] = [
        dict(zip(["program", "foundation", "category", "budget"], r)) for r in rows
    ]

    # Open calls
    cur.execute("""
        SELECT c.title, o.name, c.application_deadline, c.grant_amount_max
        FROM grant_calls c
        JOIN grant_programs p ON c.program_id = p.id
        JOIN organizations o ON p.organization_id = o.id
        WHERE c.application_deadline >= date('now') OR c.status = 'open'
        ORDER BY c.application_deadline LIMIT 30
    """)
    stats["open_calls"] = [
        dict(zip(["title", "foundation", "deadline", "amount_max"], r)) for r in cur.fetchall()
    ]

    # Data source coverage
    cur.execute("""
        SELECT
            SUM(CASE WHEN metadata LIKE '%koeki_info%' THEN 1 ELSE 0 END) AS from_koeki,
            SUM(CASE WHEN metadata IS NULL OR metadata = '' OR metadata NOT LIKE '%koeki_info%' THEN 1 ELSE 0 END) AS other
        FROM organizations
    """)
    r = cur.fetchone()
    stats["source_koeki"] = r[0] or 0
    stats["source_other"] = r[1] or 0

    # NULL ratios
    cur.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN url IS NULL OR url = '' THEN 1 ELSE 0 END) AS null_url,
            SUM(CASE WHEN prefecture IS NULL OR prefecture = '' THEN 1 ELSE 0 END) AS null_pref,
            SUM(CASE WHEN annual_grant_amount IS NULL THEN 1 ELSE 0 END) AS null_amount,
            SUM(CASE WHEN foundation_subtype IS NULL OR foundation_subtype = '' THEN 1 ELSE 0 END) AS null_subtype
        FROM organizations
    """)
    r = cur.fetchone()
    stats["null_url"] = r[1]
    stats["null_pref"] = r[2]
    stats["null_amount"] = r[3]
    stats["null_subtype"] = r[4]

    return stats


def yen_format(n: int) -> str:
    if n is None:
        return "—"
    if n >= 100_000_000:
        return f"{n/100_000_000:.1f}億円"
    if n >= 10_000_000:
        return f"{n/10_000_000:.1f}千万円"
    if n >= 10_000:
        return f"{n/10_000:.0f}万円"
    return f"{n:,}円"


def bar(value: int, max_value: int, width: int = 200) -> str:
    if max_value == 0:
        return ""
    w = int(value / max_value * width)
    return f'<span class="bar" style="width:{w}px"></span>'


def render_chapter(num: int, slug: str, label: str, title: str, body: str) -> str:
    return f"""<section class="chapter-section" id="ch{num}">
<div class="chapter-number-label">CHAPTER {num:02d} — {label}</div>
<h2 class="chapter-title">{title}</h2>
{body}
</section>"""


def main():
    conn = sqlite3.connect(DB)
    s = fetch_stats(conn)
    conn.close()

    template = TEMPLATE.read_text(encoding="utf-8")

    # Replace static placeholders
    template = template.replace("{{TITLE}}", "Corporate Foundation Grants DB")
    template = template.replace("{{SECTION_LABEL}}", "CFG")
    template = template.replace(
        "{{SUBTITLE}}",
        f"日本の研究助成財団 {s['total_orgs']:,}団体・"
        f"研究助成プログラム {s['total_programs']:,}件・"
        f"公募 {s['total_calls']:,}件を構造化したデータベース。"
        f"JFC助成財団センター・内閣府公益法人info由来の登録情報を統合し、"
        f"分類タクソノミー（法人形態×設立者形態）と助成額ティアで意思決定を支援する。",
    )
    template = template.replace(
        "{{TAGS}}",
        '<span class="db-tag primary">CFG</span>'
        '<span class="db-tag">研究助成</span>'
        '<span class="db-tag">財団</span>'
        '<span class="db-tag">JFC</span>'
        '<span class="db-tag">公益法人info</span>',
    )
    template = template.replace(
        "{{META}}",
        f"<span>更新日: {datetime.now().strftime('%Y-%m-%d')}</span>"
        f"<span>団体数: {s['total_orgs']:,}</span>"
        f"<span>カバー率推定: {s['total_orgs'] * 100 / 1000:.0f}%（目標 1,000団体）</span>",
    )

    # TOC
    chapters = [
        ("ch1", "概観", "1. データベース全体像"),
        ("ch2", "財団分類", "2. 設立者形態×法人形態タクソノミー"),
        ("ch3", "助成規模", "3. 助成額ティア分布"),
        ("ch4", "親企業", "4. 親企業ネットワーク"),
        ("ch5", "地域分布", "5. 都道府県別分布"),
        ("ch6", "プログラム", "6. 主要研究助成プログラム"),
        ("ch7", "公募カレンダー", "7. 公募カレンダー"),
        ("ch8", "データソース", "8. データソースとカバレッジ"),
        ("ch9", "使い方", "9. 使い方とアクセス"),
    ]
    toc = "\n".join(
        f'<li><a href="#{cid}"><span class="toc-num">{i:02d}</span>{title}</a></li>'
        for i, (cid, _, title) in enumerate(chapters, 1)
    )
    template = template.replace("{{TOC_LIST}}", toc)

    # ----- CHAPTER 1: 概観 -----
    ch1 = f"""<p class="lead">本データベースは、日本の研究助成を行う財団を網羅的に収録する構造化データベースである。当初189団体・258プログラムから出発し、助成財団センター（JFC）と内閣府公益法人informationの登録情報を統合することで {s['total_orgs']:,}団体規模に拡張した。</p>

<div class="stats-row">
<div class="stat-box"><div class="stat-num">{s['total_orgs']:,}</div><div class="stat-label">財団・団体</div></div>
<div class="stat-box"><div class="stat-num">{s['total_programs']:,}</div><div class="stat-label">研究助成プログラム</div></div>
<div class="stat-box"><div class="stat-num">{s['total_calls']:,}</div><div class="stat-label">公募</div></div>
<div class="stat-box"><div class="stat-num">{s['amount_known']:,}</div><div class="stat-label">助成額判明</div></div>
</div>

<p>このうち、設立者形態は企業財団・個人財団・学術系・国際機関系・NPO系などに細分化されており、利用者は「自社の研究テーマに親和性のある財団」を分類軸から効率的に絞り込むことができる。法人形態（公益財団法人・一般財団法人・公益社団法人ほか）と設立者形態を組み合わせることで、研究助成エコシステム全体の構造を俯瞰する出発点として機能する。</p>

<div class="callout">
<div class="callout-title">DESIGN PRINCIPLE</div>
本DBは「集約サイトに依存せず、各元サイトから直接収集する」方針を採る。JFCの助成・奨学金情報navi（2,539団体・8,091プログラム）と内閣府公益法人info（約3,000-4,000公益法人）を一次情報源とし、研究助成カテゴリで実質的に活動している団体を選別している。
</div>"""

    # ----- CHAPTER 2: 財団分類 -----
    max_sub = max(c for _, c in s["subtype"]) if s["subtype"] else 1
    subtype_rows = "\n".join(
        f"<tr><td>{SUBTYPE_LABELS.get(k, k)}</td><td>{c:,}</td>"
        f"<td>{c*100/s['total_orgs']:.1f}%</td>"
        f"<td>{bar(c, max_sub)}</td></tr>"
        for k, c in s["subtype"]
    )
    legal_rows = "\n".join(
        f"<tr><td>{k}</td><td>{c:,}</td>"
        f"<td>{c*100/s['total_orgs']:.1f}%</td></tr>"
        for k, c in s["legal_form"]
    )
    ch2 = f"""<p>財団は「設立者形態」と「法人形態」の二軸で分類される。設立者形態は資金源の構造を、法人形態は税制・公益認定の枠組みを示す。両者は独立した分類軸であり、たとえば「公益財団法人 × 個人財団」や「一般財団法人 × 企業財団」といった組み合わせが存在する。</p>

<h3>設立者形態（foundation_subtype）</h3>
<table>
<thead><tr><th>分類</th><th>団体数</th><th>割合</th><th>分布</th></tr></thead>
<tbody>{subtype_rows}</tbody>
</table>

<h3>法人形態（legal_form）</h3>
<table>
<thead><tr><th>法人形態</th><th>団体数</th><th>割合</th></tr></thead>
<tbody>{legal_rows}</tbody>
</table>

<div class="callout">
<div class="callout-title">分類精度の現状</div>
名称ベースの自動推測ロジックで分類しているため、現時点では <em>{SUBTYPE_LABELS["other"]}</em> に分類された団体が存在する。今後の精緻化フェーズで、各団体の公式情報（設立趣旨書・寄付者情報・公益認定情報）から手動で分類を確定する予定である。
</div>"""

    # ----- CHAPTER 3: 助成規模 -----
    if s["tiers"]:
        max_tier = max(c for _, c in s["tiers"])
        tier_rows = "\n".join(
            f"<tr><td>{k}</td><td>{c:,}</td>"
            f"<td>{c*100/s['amount_known']:.1f}%</td>"
            f"<td>{bar(c, max_tier)}</td></tr>"
            for k, c in s["tiers"]
        )
    else:
        tier_rows = "<tr><td colspan='4'>—</td></tr>"

    if s["top_funded"]:
        top_rows = "\n".join(
            f"<tr><td>{r['name']}</td>"
            f"<td>{r['parent'] or '—'}</td>"
            f"<td>{r['pref'] or '—'}</td>"
            f"<td>{yen_format(r['amount'])}</td></tr>"
            for r in s["top_funded"][:20]
        )
    else:
        top_rows = "<tr><td colspan='4'>—</td></tr>"

    ch3 = f"""<p>JFCの2019年調査（985団体）では、年間助成額50億円超のT6に属する財団がわずか28団体（3%）であるにもかかわらず、累積資産シェアの約61%を占めるという二極化構造が確認されている。本DBで助成額が判明している {s['amount_known']:,}団体について、JFCのティア区分に従って分布を集計した。</p>

<h3>助成額ティア分布（{s['amount_known']:,}団体）</h3>
<table>
<thead><tr><th>ティア</th><th>団体数</th><th>割合</th><th>分布</th></tr></thead>
<tbody>{tier_rows}</tbody>
</table>

<h3>助成額上位20団体</h3>
<table>
<thead><tr><th>財団名</th><th>親企業／設立者</th><th>所在地</th><th>年間助成額</th></tr></thead>
<tbody>{top_rows}</tbody>
</table>"""

    # ----- CHAPTER 4: 親企業ネットワーク -----
    if s["parents"]:
        max_p = max(c for _, c in s["parents"])
        parent_rows = "\n".join(
            f"<tr><td>{k}</td><td>{c}</td><td>{bar(c, max_p, 150)}</td></tr>"
            for k, c in s["parents"][:25]
        )
    else:
        parent_rows = "<tr><td colspan='3'>—</td></tr>"
    ch4 = f"""<p>企業財団の多くは複数の財団を傘下に持つ。同一企業グループが「自然科学」「人文社会」「文化芸術」「教育」など複数領域にまたがる財団を運営することも珍しくない。親企業（corporate_parent）別に集計することで、企業グループの研究助成戦略の輪郭が浮かび上がる。</p>

<h3>親企業別 財団数（上位25）</h3>
<table>
<thead><tr><th>親企業／グループ</th><th>財団数</th><th>分布</th></tr></thead>
<tbody>{parent_rows}</tbody>
</table>"""

    # ----- CHAPTER 5: 地域分布 -----
    max_pref = max(c for _, c in s["prefecture"]) if s["prefecture"] else 1
    pref_rows = "\n".join(
        f"<tr><td>{k}</td><td>{c:,}</td>"
        f"<td>{c*100/s['total_orgs']:.1f}%</td>"
        f"<td>{bar(c, max_pref)}</td></tr>"
        for k, c in s["prefecture"]
    )
    ch5 = f"""<p>研究助成財団は東京都に強く偏在する傾向がある。これは大企業本社・有力大学・公益法人主務官庁が東京に集積していることと整合する。一方で、地方独自の研究助成財団も一定数存在し、地域研究や地場産業に特化した助成プログラムを運営している。</p>

<h3>都道府県別分布（上位15）</h3>
<table>
<thead><tr><th>都道府県</th><th>団体数</th><th>割合</th><th>分布</th></tr></thead>
<tbody>{pref_rows}</tbody>
</table>"""

    # ----- CHAPTER 6: プログラム -----
    if s["top_programs"]:
        prog_rows = "\n".join(
            f"<tr><td>{r['program'] or '—'}</td>"
            f"<td>{r['foundation'] or '—'}</td>"
            f"<td>{r['category'] or '—'}</td>"
            f"<td>{yen_format(r['budget']) if r['budget'] else '—'}</td></tr>"
            for r in s["top_programs"][:25]
        )
    else:
        prog_rows = "<tr><td colspan='4'>—</td></tr>"
    ch6 = f"""<p>本DBには {s['total_programs']:,}件の研究助成プログラムが収録されている。プログラムは「公募テーマ」と「カテゴリ」で分類され、過去複数回開催されているシリーズは <code>is_recurring=1</code> として年次サイクルが追跡可能である。</p>

<h3>主要研究助成プログラム（予算額順 上位25）</h3>
<table>
<thead><tr><th>プログラム名</th><th>運営財団</th><th>カテゴリ</th><th>総予算</th></tr></thead>
<tbody>{prog_rows}</tbody>
</table>"""

    # ----- CHAPTER 7: カレンダー -----
    if s["open_calls"]:
        call_rows = "\n".join(
            f"<tr><td>{r['deadline'] or '—'}</td>"
            f"<td>{r['title'] or '—'}</td>"
            f"<td>{r['foundation'] or '—'}</td>"
            f"<td>{yen_format(r['amount_max']) if r['amount_max'] else '—'}</td></tr>"
            for r in s["open_calls"][:25]
        )
    else:
        call_rows = "<tr><td colspan='4'>受付中の公募データなし（過去公募の蓄積を強化中）</td></tr>"
    ch7 = f"""<p>研究助成は年度サイクルで運用されることが多く、応募締切は年4-6月に集中する。本DBでは応募締切（application_deadline）と現在ステータス（status: open/upcoming/closed）を保持し、研究者・研究機関のグラントオフィスが応募計画を立てる際の参照基盤として機能する。</p>

<h3>応募受付中・予定の公募（締切順）</h3>
<table>
<thead><tr><th>締切</th><th>公募名</th><th>運営財団</th><th>助成額上限</th></tr></thead>
<tbody>{call_rows}</tbody>
</table>"""

    # ----- CHAPTER 8: ソース -----
    ch8 = f"""<p>本DBは複数の一次情報源を統合して構築されている。各団体の <code>metadata.source</code> フィールドにデータソースが記録され、来歴の追跡が可能である。</p>

<h3>データソース別カバレッジ</h3>
<table>
<thead><tr><th>ソース</th><th>団体数</th><th>備考</th></tr></thead>
<tbody>
<tr><td>内閣府公益法人info（koeki_info）</td><td>{s['source_koeki']:,}</td>
<td>研究関連スコア3以上の公益法人を抽出</td></tr>
<tr><td>JFC助成財団センター + その他</td><td>{s['source_other']:,}</td>
<td>研究助成プログラム情報を含む団体</td></tr>
</tbody>
</table>

<h3>欠損率（improvement opportunity）</h3>
<table>
<thead><tr><th>項目</th><th>欠損数</th><th>欠損率</th></tr></thead>
<tbody>
<tr><td>公式URL</td><td>{s['null_url']:,}</td>
<td>{s['null_url']*100/s['total_orgs']:.1f}%</td></tr>
<tr><td>所在都道府県</td><td>{s['null_pref']:,}</td>
<td>{s['null_pref']*100/s['total_orgs']:.1f}%</td></tr>
<tr><td>年間助成額</td><td>{s['null_amount']:,}</td>
<td>{s['null_amount']*100/s['total_orgs']:.1f}%</td></tr>
<tr><td>設立者形態</td><td>{s['null_subtype']:,}</td>
<td>{s['null_subtype']*100/s['total_orgs']:.1f}%</td></tr>
</tbody>
</table>

<div class="callout">
<div class="callout-title">次フェーズ計画</div>
今後、各団体の公式URLからの情報抽出（年次報告PDF・募集要項PDF・採択者リスト）を進め、欠損率の改善を図る。特に年間助成額の判明率向上は、利用者の意思決定支援に直結する重要課題である。
</div>"""

    # ----- CHAPTER 9: 使い方 -----
    ch9 = """<p>本DBは下記の用途で利用される想定で設計されている。</p>

<h3>1. 助成金検索（個別研究者・グラントオフィス）</h3>
<p>研究テーマと自身の所属（大学・企業・NPO）を入力すると、応募可能な助成プログラムが提示される。締切順・助成額順・分野マッチ度順でソート可能である。</p>

<h3>2. 研究助成エコシステム分析（政策担当者・調査機関）</h3>
<p>分類タクソノミー（設立者形態×法人形態×助成額ティア）の集計から、日本の研究助成エコシステムの構造的特徴と課題（東京偏在・大型財団への集中・若手研究者向け制度の手薄さ等）を抽出できる。</p>

<h3>3. 産学連携・寄付戦略（企業）</h3>
<p>同業他社の研究助成戦略（領域カバレッジ・採択分野傾向）を比較し、自社の研究助成プログラム設計の参考とする。</p>

<h3>アクセス</h3>
<ul>
<li>ソースリポジトリ: <code>~/projects/apps/corporate-foundation-grants/</code></li>
<li>SQLite DB: <code>corporate_research_grants.sqlite</code></li>
<li>関連プロジェクト: <a href="https://grant-db.vercel.app">Grant DB</a>（4,552件の助成金・補助金DB、AIマッチング搭載）</li>
<li>データソース: <a href="https://www.jfc.or.jp/">助成財団センター</a> / <a href="https://www.koeki-info.go.jp/">内閣府公益法人info</a></li>
</ul>"""

    chapters_html = "\n\n".join([
        render_chapter(1, "ch1", "OVERVIEW", "1. データベース全体像", ch1),
        render_chapter(2, "ch2", "TAXONOMY", "2. 設立者形態 × 法人形態タクソノミー", ch2),
        render_chapter(3, "ch3", "SCALE", "3. 助成額ティア分布", ch3),
        render_chapter(4, "ch4", "NETWORK", "4. 親企業ネットワーク", ch4),
        render_chapter(5, "ch5", "GEOGRAPHY", "5. 都道府県別分布", ch5),
        render_chapter(6, "ch6", "PROGRAMS", "6. 主要研究助成プログラム", ch6),
        render_chapter(7, "ch7", "CALENDAR", "7. 公募カレンダー", ch7),
        render_chapter(8, "ch8", "PROVENANCE", "8. データソースとカバレッジ", ch8),
        render_chapter(9, "ch9", "USAGE", "9. 使い方とアクセス", ch9),
    ])
    template = template.replace("{{CHAPTERS}}", chapters_html)

    # Footer
    template = template.replace(
        "{{FOOTER}}",
        f"Corporate Foundation Grants DB / 更新: {datetime.now().strftime('%Y-%m-%d')}<br>"
        "© ミラツク（NPO法人ミラツク）<br>"
        '<a href="../databases.html">← Databases一覧へ戻る</a>',
    )

    OUTPUT.write_text(template, encoding="utf-8")
    print(f"Generated: {OUTPUT}")
    print(f"  Size: {OUTPUT.stat().st_size:,} bytes")


if __name__ == "__main__":
    main()
