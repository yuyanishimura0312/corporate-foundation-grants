#!/usr/bin/env python3
"""grant_field_map.json → 研究助成領域MAP HTML(赤白CI textbook style・SVGバー)。A-E統合レポート。"""
import json
d = json.load(open("research_results/grant_field_map.json"))
NAME = {"natural_science":"自然科学","life_science":"生命科学・医学","engineering":"工学・技術","humanities_social":"人文社会科学","arts_culture":"芸術・文化","education":"教育・人材育成","welfare":"福祉・健康","environment":"環境","international":"国際交流・協力","regional":"地域","interdisciplinary":"学際・融合"}
def ja(k): return NAME.get(k, k)

def barrows(data, tot=None, unit="件", namef=ja, maxn=None):
    items = data[:maxn] if maxn else data
    mx = max((n for _, n in items), default=1)
    out = []
    for k, n in items:
        w = int(100 * n / mx)
        out.append('<div class="bar"><span class="bl">%s</span><span class="bt"><span class="bf" style="width:%d%%"></span></span><span class="bn">%s</span></div>' % (namef(k), w, "{:,}".format(n)))
    return "".join(out)

# B comparison dual bars
def compbars(comp):
    mx = max(max(x["foundation_pct"], x["kaken_pct"]) for x in comp) or 1
    out = []
    for x in comp:
        if x["foundation_pct"] == 0 and x["kaken_pct"] == 0: continue
        fw = int(100 * x["foundation_pct"] / mx); kw = int(100 * x["kaken_pct"] / mx)
        dcls = "hi" if x["diff"] > 3 else ("lo" if x["diff"] < -3 else "")
        out.append('<div class="cmp"><span class="cl">%s</span><div class="cb"><div class="cf" style="width:%d%%"></div><span class="cv">財団 %.1f%%</span></div><div class="cb"><div class="ck" style="width:%d%%"></div><span class="cv">科研費 %.1f%%</span></div><span class="cd %s">%+.1f</span></div>' % (x["field"], fw, x["foundation_pct"], kw, x["kaken_pct"], dcls, x["diff"]))
    return "".join(out)

# C prefecture — full 47 as bars
def prefbars(data):
    mx = max((n for _, n in data), default=1)
    out = []
    for k, n in data:
        out.append('<div class="pbar"><span class="pl">%s</span><span class="pt"><span class="pf" style="width:%d%%"></span></span><span class="pn">%d</span></div>' % (k, int(100 * n / mx), n))
    return "".join(out)

fm = d["foundation_map"]
html = """<!DOCTYPE html><html lang="ja"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>研究助成 領域MAP — 財団 × 科研費 × 大学分布</title><link rel="icon" href="https://esse-sense.com/favicon.ico">
<link href="https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@300;400;500;700;900&family=Noto+Serif+JP:wght@700&family=Fira+Code&display=swap" rel="stylesheet">
<style>
:root{--bg:#FFF;--ink:#121212;--soft:#555;--mute:#8A7868;--accent:#CC1400;--accent2:#0E4F6B;--rule:#E4E0D8;--surf:#FAF8F4}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--ink);font-family:"Noto Sans JP",sans-serif;line-height:1.8;font-feature-settings:"palt"}
.wrap{max-width:900px;margin:0 auto;padding:40px 28px}
.cover{border-bottom:3px solid var(--ink);padding-bottom:22px;margin-bottom:32px}
.badge{display:inline-block;font-size:11px;font-weight:700;letter-spacing:.24em;color:var(--accent);border:1px solid var(--accent);padding:4px 12px;margin-bottom:16px}
h1{font-size:27px;font-weight:900;line-height:1.4}
.sub{font-size:14px;color:var(--soft);margin-top:8px}
.meta{font-size:12px;color:var(--mute);margin-top:14px;font-family:"Fira Code"}
h2{font-size:20px;font-weight:900;margin:38px 0 4px;padding-top:14px;border-top:1px solid var(--rule)}
.cap{font-size:12px;color:var(--mute);letter-spacing:.1em;font-family:"Fira Code"}
.rule{width:48px;height:3px;background:var(--accent);margin:8px 0 16px}
p.lead{font-size:14px;background:var(--surf);border-left:3px solid var(--accent);padding:12px 16px;margin:14px 0}
.bar{display:grid;grid-template-columns:130px 1fr 64px;align-items:center;gap:10px;margin:3px 0;font-size:12.5px}
.bl{text-align:right;color:var(--soft)}.bt{background:var(--surf);height:16px;border-radius:2px;overflow:hidden}
.bf{display:block;height:100%%;background:var(--accent)}.bn{font-family:"Fira Code";font-size:11px;text-align:right}
.cmp{display:grid;grid-template-columns:120px 1fr 54px;gap:8px;align-items:center;margin:8px 0;font-size:12px}
.cl{text-align:right;font-weight:500}.cb{position:relative;height:15px;background:var(--surf);border-radius:2px;margin:1px 0}
.cf{height:100%%;background:var(--accent)}.ck{height:100%%;background:var(--accent2)}
.cv{position:absolute;right:4px;top:-1px;font-size:9.5px;font-family:"Fira Code";color:var(--ink)}
.cd{font-family:"Fira Code";font-size:11px;text-align:right;color:var(--mute)}.cd.hi{color:var(--accent);font-weight:700}.cd.lo{color:var(--accent2);font-weight:700}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:8px 28px}
.pbar{display:grid;grid-template-columns:56px 1fr 40px;align-items:center;gap:6px;font-size:11px;margin:1.5px 0}
.pl{text-align:right;color:var(--soft)}.pt{background:var(--surf);height:12px;border-radius:2px;overflow:hidden}.pf{display:block;height:100%%;background:var(--accent)}.pn{font-family:"Fira Code";font-size:10px;text-align:right}
table{width:100%%;border-collapse:collapse;font-size:12px;margin:12px 0}
th{background:var(--ink);color:#fff;padding:7px 9px;text-align:left;font-weight:500}
td{padding:6px 9px;border-bottom:1px solid var(--rule)}td.n{font-family:"Fira Code";text-align:right}
.foot{margin-top:40px;padding-top:14px;border-top:2px solid var(--accent);font-size:11px;color:var(--mute);display:flex;justify-content:space-between}
.legend{font-size:11px;color:var(--mute)}.legend b.f{color:var(--accent)}.legend b.k{color:var(--accent2)}
@media(max-width:680px){.grid2{grid-template-columns:1fr}.bar{grid-template-columns:100px 1fr 54px}}
</style></head><body><div class="wrap">
<div class="cover"><span class="badge">RESEARCH GRANT FIELD MAP</span>
<h1>研究助成 領域MAP<br>財団 × 科研費 × 大学分布 × 応募要項 × 金額</h1>
<p class="sub">助成金プログラム・採択研究者・採択テーマの3階層を学術分野（AGD 55分野／11大分類）で整理し、科研費23.5万人と照合</p>
<div class="meta">研究者DB 31.1万人版接続 ／ 財団研究助成 %d件 ／ 科研費 %d,000人 ／ 生成 2026-07-16 ／ NPO法人ミラツク・esse-sense</div></div>

<h2>A. 財団による研究助成の領域MAP</h2><div class="cap">3階層（プログラム／研究者／テーマ）</div><div class="rule"></div>
<p class="lead">研究助成 %d件を3階層で領域整理。<b>助成先研究者の学問分野（研究者DB接続）</b>と<b>採択テーマの分類</b>がともに生命科学・医学へ最も厚く、自然科学が続く。プログラム階層（財団の掲げる領域）と研究者階層のズレも可視化される。</p>
<h3 style="font-size:14px;margin:16px 0 6px">① プログラム階層（財団が掲げる助成領域・11分類）</h3>%s
<h3 style="font-size:14px;margin:18px 0 6px">② 研究者階層（採択研究者の学問分野・11大分類・RID接続）</h3>%s
<h3 style="font-size:14px;margin:18px 0 6px">③ テーマ階層（採択課題名の分類）</h3>%s
<h3 style="font-size:14px;margin:18px 0 6px">研究者階層 詳細（AGD 55分野・上位20）</h3>%s

<h2>B. 財団 × 科研費 領域MAP 照合</h2><div class="cap">11大分類・正規化%%・研究者DB接続分</div><div class="rule"></div>
<p class="lead">同一の学問分野タクソノミ（研究者DB由来）で財団助成と科研費を照合。<b class="legend"><b class="f">■財団</b> <b class="k">■科研費</b></b>。<b>生命科学・医学（+10pt）と自然科学（+8.5pt）は財団が科研費より厚く、人文社会科学は財団が薄い（−11pt）</b>——財団助成は理系・生命系に偏る構造が定量化された。</p>
%s

<h2>C. 所属大学の全国分布・大学別分布</h2><div class="cap">財団助成 vs 科研費・47都道府県</div><div class="rule"></div>
<p class="lead">助成獲得時の所属機関を大学単位・都道府県単位で集計（都道府県は主要大学マップで解決した分。中小機関は今後拡充）。<b>財団助成・科研費とも東京・京都・大阪へ集中</b>し、旧帝大＋主要私大に厚い。地方大学の獲得実態も47都道府県で把握できる。</p>
<h3 style="font-size:14px;margin:16px 0 6px">大学別 上位20（財団助成）</h3><table><tr><th>大学</th><th>財団助成</th><th>科研費(参考)</th></tr>%s</table>
<div class="grid2"><div><h3 style="font-size:13px;margin:14px 0 6px">財団助成 都道府県分布（47）</h3>%s</div>
<div><h3 style="font-size:13px;margin:14px 0 6px">科研費 都道府県分布（47・参考）</h3>%s</div></div>

<h2>D. 応募要項のパターン形成</h2><div class="cap">対象者像・応募資格の類型</div><div class="rule"></div>
<p class="lead">応募資格を軸別に集計（562→2,717件・483財団へ大幅拡張）。<b>若手研究者（40歳以下等）targeting が主流</b>で、日本国籍中心、職位は多様。財団助成の「誰を対象とするか」の型が読める。</p>
%s

<h2>E. 金額分布</h2><div class="cap">個別採択額・財団年間助成額</div><div class="rule"></div>
<p class="lead">codexで<b>募集要項から1件あたり助成額を802件収集</b>（中央値150万円）——<b>100-200万円が最頻</b>で、財団研究助成の標準的な規模が定量化された。加えて個別採択額（検証済み武田/三菱）・財団年間助成額（5000万〜10億円超）も併載。</p>
<h3 style="font-size:13px;margin:8px 0 6px">★ 1件あたり助成額（募集要項ベース・%d件・中央値 %s万円）</h3>%s
<div class="grid2">
<div><h3 style="font-size:13px;margin:14px 0 6px">個別採択額（検証済み・武田/三菱財団）</h3>%s</div>
<div><h3 style="font-size:13px;margin:14px 0 6px">個別採択額（codex推定・参考）</h3>%s</div></div>
<h3 style="font-size:13px;margin:16px 0 6px">財団 年間助成額規模</h3>%s

<div class="foot"><span>NPO法人ミラツク ／ esse-sense</span><span>研究助成 領域MAP ／ 2026.07.16 ／ 全数実測・研究者DB 31.1万人接続</span></div>
</div></body></html>""" % (
    fm["n_records"], sum(n for _,n in d["kaken_field11"])//1000, fm["n_records"],
    barrows(fm["L1_program11"]), barrows(fm["L2_researcher11"]), barrows(fm["L3_theme11"]),
    barrows(fm["L2_researcher55"], namef=lambda x:x, maxn=20),
    compbars(d["comparison_11"]),
    "".join("<tr><td>%s</td><td class='n'>%s</td><td class='n'>%s</td></tr>" % (u, "{:,}".format(n), "{:,}".format(dict(d["kaken_univ_top"]).get(u, 0))) for u, n in d["foundation_univ_top"][:20]),
    prefbars(d["foundation_pref"]), prefbars(d["kaken_pref"]),
    "".join('<h3 style="font-size:13px;margin:12px 0 4px;color:#CC1400">%s</h3>%s' % (
        {"age":"年齢","career_stage":"キャリア段階","nationality":"国籍","position":"職位","affiliation_type":"所属種別","field":"分野","gender":"性別要件"}.get(t,t),
        barrows([(k, n) for k, n in d["eligibility_patterns"][t][:6]], namef=lambda x:x)) for t in ("age","career_stage","nationality","position","affiliation_type","gender") if d["eligibility_patterns"].get(t)),
    d["amount_per_award_program_stats"]["n"], "{:,}".format(d["amount_per_award_program_stats"]["median"]//10000),
    barrows(d["amount_per_award_program_dist"], namef=lambda x:x),
    barrows(d["amount_award_dist"], namef=lambda x:x), barrows(d.get("amount_hint_dist",[]), namef=lambda x:x),
    barrows(d["amount_foundation_dist"], namef=lambda x:x),
)
open("report/grant_field_map.html", "w").write(html)
print("saved report/grant_field_map.html (%d KB)" % (len(html)//1024))
