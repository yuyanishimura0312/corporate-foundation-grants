#!/usr/bin/env python3
"""学術分野 詳細解析ダッシュボード — 赤白CI textbook style。token置換で組む(literal %保護)。
   AGD55分野 × 代表性比(vs科研費) × 財団ランドスケープ × 専門化HHI × サブテーマ。fable検証前提のhonest版。"""
import json
d = json.load(open("research_results/field_deep.json"))
L = d["landscape"]

def esc(s): return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def hbars(pairs, denom, unit="件", pct=True, maxn=None):
    items = pairs[:maxn] if maxn else pairs
    mx = max((n for _, n in items), default=1)
    o = []
    for k, n in items:
        w = int(100 * n / mx)
        tail = "{:,}".format(n) + unit + ((" (%.1f%%)" % (100*n/denom)) if pct and denom else "")
        o.append('<div class="bar"><span class="bl">' + esc(k) + '</span><span class="bt"><span class="bf" style="width:' + str(w) + '%"></span></span><span class="bn">' + tail + '</span></div>')
    return "".join(o)

def repbars(items, over=True):
    # ratio を 0-3 スケールで棒に。基準1.0を境に色替え
    o = []
    for x in items:
        r = x["ratio"]; w = min(100, int(100 * r / 3.0))
        cls = "bf over" if r >= 1.0 else "bf under"
        o.append('<div class="bar"><span class="bl">' + esc(x["field"]) + '</span><span class="bt"><span class="' + cls + '" style="width:' + str(w) + '%"></span></span>'
                 '<span class="bn">×' + ("%.2f" % r) + '  <span class="dim">財団' + ("%.1f" % x["cfg_share"]) + '%／科研費' + ("%.1f" % x["kaken_share"]) + '%</span></span></div>')
    return "".join(o)

# 分野分布(AGD55, TOP20)
field_bars = hbars(d["field_dist_agd55"][:20], d["n_matched"])
over_bars = repbars(d["over_rep"][:10], over=True)
under_bars = repbars(d["under_rep"][:10], over=False)
top_fnd = hbars([(f["name"].replace("公益財団法人","").replace("一般財団法人",""), f["n"]) for f in L["top_foundations"]], d["n_all"])

# 専門化テーブル(HHI降順)
spec_sorted = sorted(d["specialization"], key=lambda z: -z["hhi"])
spec_rows = "".join(
    "<tr><td>" + esc(x["field"]) + "</td><td class='n'>" + str(x["total"]) + "</td><td class='n'>" + str(x["n_foundations"]) + "</td>"
    "<td class='n'>" + ("%.2f" % x["hhi"]) + "</td><td>" + esc(x["top"][0]["name"].replace("公益財団法人","")) + " <b>" + ("%.0f" % x["top"][0]["share"]) + "%</b></td></tr>"
    for x in spec_sorted[:12])

# サブテーマ(3分野)
sub_blocks = []
for label, sd in d["subtheme"].items():
    bars = hbars(sd["dist"], sd["base"])
    sub_blocks.append('<h3>' + esc(label) + '（n=' + "{:,}".format(sd["base"]) + '／未分類 ' + str(sd["unclassified"]) + '）</h3>' + bars)
sub_html = "".join(sub_blocks)

CH = [("ch1","01 OVERVIEW","1. 解析の全体像"),("ch2","02 FIELDS","2. 学問分野分布（AGD 44分野）"),
      ("ch3","03 REPRESENTATION","3. 代表性比 — 科研費母集団との照合"),("ch4","04 LANDSCAPE","4. 財団ランドスケープ（寡占構造）"),
      ("ch5","05 SPECIALIZATION","5. 分野専門化（集中度HHI）"),("ch6","06 SUBTHEME","6. 生命医学系サブテーマ分解"),
      ("ch7","07 LIMITS","7. 解析の限界")]
toc = "".join('<li><a href="#' + c + '"><span class="tn">' + code.split()[0] + '</span>' + t + '</a></li>' for c, code, t in CH)

CSS = """
:root{--bg:#FFF;--card:#FFF;--ink:#121212;--soft:#555;--mute:#8A7868;--accent:#CC1400;--accent2:#0E4F6B;--rule:#E4E0D8;--surf:#FAF8F4;--over:#CC1400;--under:#0E4F6B}
[data-theme=dark]{--bg:#141210;--card:#1C1A17;--ink:#E8E2D8;--soft:#B8AE9E;--mute:#8A8070;--accent:#FF5A47;--rule:#33302A;--surf:#1F1C18;--over:#FF5A47;--under:#5AA8D0}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--ink);font-family:"Noto Sans JP",sans-serif;line-height:1.85;font-feature-settings:"palt"}
.top{position:sticky;top:0;z-index:50;background:var(--bg);border-bottom:3px solid var(--ink);padding:12px 24px;display:flex;justify-content:space-between;align-items:center}
.brand{font-weight:900;font-size:15px}.brand span{color:var(--accent)}
.tbtn{font-size:12px;padding:5px 12px;border:1px solid var(--rule);background:var(--card);color:var(--ink);cursor:pointer;border-radius:3px}
.layout{display:grid;grid-template-columns:250px 1fr;max-width:1160px;margin:0 auto}
.toc{position:sticky;top:60px;height:calc(100vh - 60px);overflow-y:auto;padding:24px 16px;border-right:1px solid var(--rule)}
.toc-label{font-size:11px;letter-spacing:.2em;color:var(--mute);margin-bottom:12px;font-family:"Fira Code"}
.toc ol{list-style:none}.toc li a{display:block;padding:6px 8px;font-size:12.5px;color:var(--soft);text-decoration:none;border-radius:3px}
.toc li a:hover{background:var(--surf);color:var(--accent)}.tn{font-family:"Fira Code";font-size:10px;color:var(--accent);margin-right:8px}
main{padding:32px 40px;max-width:800px}
.hero{border-bottom:2px solid var(--ink);padding-bottom:20px;margin-bottom:28px}
.tags{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:12px}.tag{font-size:10px;padding:3px 9px;background:var(--surf);color:var(--soft);border-radius:2px}.tag.p{background:var(--accent);color:#fff}
h1{font-size:25px;font-weight:900;line-height:1.4}
.hsub{font-size:13.5px;color:var(--soft);margin-top:8px}
.hmeta{font-size:11.5px;color:var(--mute);margin-top:12px;font-family:"Fira Code";display:flex;gap:16px;flex-wrap:wrap}
section{margin:40px 0;scroll-margin-top:70px}
.chlabel{font-family:"Fira Code";font-size:11px;color:var(--accent);letter-spacing:.15em}
h2{font-size:20px;font-weight:900;margin:2px 0 4px}
.rule{width:48px;height:3px;background:var(--accent);margin:8px 0 16px}
p.lead{font-size:14px;background:var(--surf);border-left:3px solid var(--accent);padding:12px 16px;margin:14px 0;line-height:1.9}
p.body{font-size:13.5px;margin:12px 0;text-align:justify}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:16px 0}
.stat{background:var(--card);border:1px solid var(--rule);border-top:3px solid var(--accent);padding:14px;text-align:center;border-radius:4px}
.snum{font-size:24px;font-weight:900;color:var(--accent);font-family:"Fira Code"}.slabel{font-size:11px;color:var(--soft);margin-top:4px}
.bar{display:grid;grid-template-columns:110px 1fr 210px;align-items:center;gap:10px;margin:3px 0;font-size:12px}
.bl{text-align:right;color:var(--soft);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.bt{background:var(--surf);height:14px;border-radius:2px;overflow:hidden}.bf{display:block;height:100%;background:var(--accent)}
.bf.over{background:var(--over)}.bf.under{background:var(--under)}
.bn{font-family:"Fira Code";font-size:10.5px;text-align:right}.bn .dim{color:var(--mute);font-size:9.5px}
table{width:100%;border-collapse:collapse;font-size:12px;margin:10px 0}
th{background:var(--ink);color:var(--bg);padding:7px 9px;text-align:left;font-weight:500}
td{padding:6px 9px;border-bottom:1px solid var(--rule)}td.n{font-family:"Fira Code";text-align:right}
h3{font-size:13px;font-weight:700;margin:18px 0 6px;color:var(--accent)}
.callout{background:var(--ink);color:#F4F1EC;padding:16px 20px;border-radius:5px;margin:16px 0;font-size:13px;line-height:1.85}
.callout b{color:#FF8A78}
.caveat{background:#FFF8F0;border:1px solid #E8D0B0;border-left:3px solid var(--accent2);padding:12px 16px;font-size:12.5px;margin:14px 0;color:var(--soft)}
[data-theme=dark] .caveat{background:#20201A;border-color:#4A4436}
.legend{font-size:11px;color:var(--mute);margin:6px 0 2px;font-family:"Fira Code"}
.legend .o{color:var(--over)}.legend .u{color:var(--under)}
.foot{margin:40px 0 20px;padding-top:16px;border-top:2px solid var(--accent);font-size:11px;color:var(--mute);display:flex;justify-content:space-between}
@media(max-width:860px){.layout{grid-template-columns:1fr}.toc{display:none}.stats{grid-template-columns:repeat(2,1fr)}main{padding:24px 20px}.bar{grid-template-columns:80px 1fr 150px}}
"""

TPL = """<!DOCTYPE html><html lang="ja" data-theme="light"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>CFG 学術分野 詳細解析 — 企業財団 研究助成</title><link rel="icon" href="https://esse-sense.com/favicon.ico">
<link href="https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@300;400;500;700;900&family=Fira+Code&display=swap" rel="stylesheet">
<style>__CSS__</style></head><body>
<div class="top"><div class="brand">MIRATUKU / <span>CFG</span> 学術分野 詳細解析</div>
<div><a href="cfg-strategy.html" style="font-size:12px;color:var(--accent);margin-right:14px;text-decoration:none">集中支援戦略 &rarr;</a>
<button class="tbtn" onclick="var h=document.documentElement;h.dataset.theme=h.dataset.theme=='dark'?'light':'dark'">DARK/LIGHT</button></div></div>
<div class="layout"><nav class="toc"><div class="toc-label">CONTENTS</div><ol>__TOC__</ol></nav><main>
<div class="hero"><div class="tags"><span class="tag p">CFG</span><span class="tag">学術分野</span><span class="tag">AGD 44分野</span><span class="tag">科研費照合</span><span class="tag">研究者DB接続</span></div>
<h1>企業財団 研究助成<br>学術分野 詳細解析</h1>
<p class="hsub">15区分より深く、AGD 44分野の粒度で「何が助成され、科研費母集団と比べてどの分野が過剰／過少か、どの財団が寡占するか、生命医学のどのサブテーマに集中するか」を全数実測する。</p>
<div class="hmeta"><span>更新: 2026-07-16</span><span>採択件数: __N_ALL__件</span><span>分野接地: __N_M__件／研究者__NDIST__名</span><span>科研費母集団: __KTOT__名</span></div></div>

<section id="ch1"><div class="chlabel">CHAPTER 01 — OVERVIEW</div><h2>1. 解析の全体像</h2><div class="rule"></div>
<p class="lead">企業財団（corporate/group）の研究助成<b>__N_ALL__件</b>の採択のうち<b>__N_M__件（__MRATE__%）</b>を研究者DB経由でAGD学問分野に接地した（延べ__N_M__件＝distinct研究者<b>__NDIST__名</b>・うち<b>__MULTI__名</b>が複数回受賞）。これを科研費母集団<b>__KTOT__名</b>の分野構成と、両側とも研究者数（distinct）基準で照合する。出現分野は<b>__NFIELD__分野</b>。15区分の粗い分野をAGD原分野の粒度へ細分し、「代表性比・財団寡占・サブテーマ」の3軸を加えた。</p>
<div class="stats"><div class="stat"><div class="snum">__N_ALL__</div><div class="slabel">企業財団 採択件数</div></div>
<div class="stat"><div class="snum">__NFIELD__</div><div class="slabel">出現AGD分野</div></div>
<div class="stat"><div class="snum">×1.9</div><div class="slabel">化学の過剰代表(最大)</div></div>
<div class="stat"><div class="snum">26.5%</div><div class="slabel">武田1財団の占有</div></div></div>
<p class="body">3つの発見が浮かぶ。第一に、企業財団は<b>化学・薬学・生化学・生物学を科研費母集団の約1.7〜1.9倍</b>の比率で助成し、人文社会系を<b>0.2〜0.4倍</b>に絞る（ただし後者は接続バイアスを含む・第3章／第7章参照）。第二に、この領域は<b>武田科学振興財団という単一財団が全体の4分の1</b>を占める強い寡占構造を持つ。第三に、金額で分野を比較することは<b>できない</b>—金額が判明する記録の8割超を武田1財団が占め、その半分が固定額（1,000万円）だからである。</p></section>

<section id="ch2"><div class="chlabel">CHAPTER 02 — FIELDS</div><h2>2. 学問分野分布（AGD 44分野）</h2><div class="rule"></div>
<p class="body">RID接続済み__N_M__件を、科研費と同じAGD分野体系で分類した実分布（採択件数ベース・上位20）。西洋医学・生物学の2分野で全体のほぼ半分を占め、化学・生化学が続く。生命科学と化学への強い偏りが、粗い15区分よりも鮮明に見える。</p>
__FIELD_BARS__</section>

<section id="ch3"><div class="chlabel">CHAPTER 03 — REPRESENTATION</div><h2>3. 代表性比 — 科研費母集団との照合</h2><div class="rule"></div>
<p class="lead">本解析の中核。<b>代表性比＝（財団助成での分野シェア）÷（科研費母集団での分野シェア）</b>。両側とも<b>研究者数（distinct）基準</b>に揃えて算出する。1.0が母集団並み、1.0超は企業財団が母集団以上に厚く助成する分野、1.0未満は手薄な分野を意味する。</p>
<div class="legend"><span class="o">■</span> 過剰代表（比&ge;1.0） ／ <span class="u">■</span> 過少代表（比&lt;1.0）　棒はratioを0〜3で正規化</div>
<h3>過剰代表 — 母集団以上に厚く助成される分野</h3>
__OVER_BARS__
<h3>過少代表 — 母集団に対し手薄な分野</h3>
__UNDER_BARS__
<div class="callout"><b>企業財団は「化学・薬学・生化学」に母集団の約1.7〜1.9倍を投じ、「教育学・数学・歴史学・社会学」を母集団の3〜6分の1に絞る。</b>これは企業財団のミッション（科学技術・医薬・産業応用）が構造的に生み出す偏りである。ただし過少代表側は次のcaveatの通り接続バイアスを強く含むため、STEMの過剰代表（頑健）と対称には読めない。</div>
<div class="caveat">※ <b>過少代表側は測定バイアスを強く含み、「助成不在」の証拠にはならない</b>。財団の性格別に研究者DB接続率を実測すると、<b>科学・医学系財団 __STEM_RATE__% に対し、文化・芸術・社会教育系財団はわずか __CULT_RATE__%</b>。人文・芸術分野の助成者（ヨネックススポーツ／エネルギア文化／芸術文化振興会 等）の受賞者は科研費(KAKEN)ベースの研究者DBにほとんど載らず、教育学・歴史学・社会学の過少代表比の相当部分は<b>接続漏れ</b>である。一方、化学・生化学・薬学など接続の厚いSTEM分野の過剰代表は頑健。読み手は「STEMへの厚い配分」を主結論とし、人文社会の薄さは本DBだけでは断定しないこと。</div></section>

<section id="ch4"><div class="chlabel">CHAPTER 04 — LANDSCAPE</div><h2>4. 財団ランドスケープ（寡占構造）</h2><div class="rule"></div>
<p class="body">分野の偏りは「どの財団が助成しているか」と不可分である。企業財団の研究助成は、単一財団への強い集中を示す。採択件数上位8財団を示す（比率は全採択__N_ALL__件に対する割合）。</p>
__TOP_FND__
<div class="callout"><b>武田科学振興財団の1財団だけで、企業財団研究助成の__TAKEDA__%（__TAKEDA_N__件）を占める。</b>2位の三菱財団（4.6%）以下を大きく引き離す。「企業財団の生命科学助成」を語ることは、相当程度「武田を語る」ことに等しい。集中支援戦略を設計する際、この寡占は前提条件になる。</div>
<div class="caveat">※ <b>金額による分野比較は行わない。</b>金額が判明する記録の__AMT_TAKEDA__%を武田1財団が占め（武田の助成額は一律ではなく1,000万・500万・300万・200万・3,000万等が混在。うち1,000万固定が全金額記録の__AMT10M__%）、分野ごとの中央値はすべて武田の助成設計に引きずられる。「生命科学は1件1,000万円」は分野の性質ではなく特定財団の設計であり、分野比較の根拠にならない。</div></section>

<section id="ch5"><div class="chlabel">CHAPTER 05 — SPECIALIZATION</div><h2>5. 分野専門化（集中度HHI）</h2><div class="rule"></div>
<p class="body">各分野を「何財団が助成し、筆頭財団がどれだけ占めるか」で見る。HHI（ハーフィンダール指数・1に近いほど1財団集中）が高い分野ほど、少数の財団に依存している。生命科学系はいずれも武田が筆頭で、HHI 0.22〜0.28と集中が強い。</p>
<table><tr><th>分野</th><th>採択数</th><th>財団数</th><th>HHI</th><th>筆頭財団（占有率）</th></tr>__SPEC_ROWS__</table>
<p class="body">経済学はHHI 0.20と、全国銀行学術研究振興財団（39%）に依存する。分野の助成が特定財団に強く紐づく構造は、その財団の方針変更が分野全体の資金供給を左右しうる<b>脆弱性</b>でもある。</p></section>

<section id="ch6"><div class="chlabel">CHAPTER 06 — SUBTHEME</div><h2>6. 生命医学系サブテーマ分解</h2><div class="rule"></div>
<p class="body">最大分野である西洋医学・生物学・化学を、課題名からサブテーマへ分解する（キーワード分類・未分類は各分野に残る）。分野内のどの主題に助成が集まるかを可視化する。</p>
__SUB_HTML__
<p class="body">西洋医学では<b>がん・腫瘍（20%）</b>が突出し、脳神経・免疫・循環器が続く。企業財団の医学助成は疾患領域の主要ホットスポットに沿っており、希少疾患・予防・公衆衛生など社会的重要度が高くても産業応用の遠い領域は相対的に薄い可能性がある（未分類の精査は今後の課題）。</p></section>

<section id="ch7"><div class="chlabel">CHAPTER 07 — LIMITS</div><h2>7. 解析の限界</h2><div class="rule"></div>
<div class="caveat">
・<b>件数と人数の区別</b>：採択__N_ALL__件・接地__N_M__件は「件数」（延べ）。distinct研究者は__NDIST__名で、__MULTI__名が複数回受賞。分野分布グラフは件数ベース、代表性比は研究者数（distinct）ベースで算出しており、混同しない。<br>
・<b>人文社会の過少代表は接続バイアスを強く含む</b>：科学系財団の接続率__STEM_RATE__%に対し文化・芸術系は__CULT_RATE__%。教育学・歴史学・社会学の過少代表は「助成不在」でなく接続漏れが主因の可能性が高く、本DBだけで断定しない。STEMの過剰代表は頑健。<br>
・<b>金額の分野比較は不可</b>：金額充填記録の__AMT_TAKEDA__%を武田1財団が占める（額は一律でなく1,000万固定が記録の__AMT10M__%）。分野別の金額差は算出しない。<br>
・<b>AGD分類は自動接地</b>：研究者DBのAGD分野を第一権威とするが、分野境界（例：生化学↔生物学）はゆらぎを含む。序列は方向性として信頼できる。<br>
・<b>サブテーマは課題名キーワード</b>：抄録全文でなく課題名で分類するため未分類が残る。分野内の主題序列（がん→脳神経→免疫）は頑健だが、精緻な比率には幅がある。<br>
・企業財団（corporate/group）に限定し、大学・政府・国際財団は含まない。
</div></section>

<div class="foot"><span>NPO法人ミラツク ／ esse-sense</span><span>CFG 学術分野 詳細解析 ／ 2026.07.16 ／ 全数実測・科研費照合・研究者DB接続</span></div>
</main></div></body></html>"""

html = (TPL.replace("__CSS__", CSS).replace("__TOC__", toc)
        .replace("__FIELD_BARS__", field_bars).replace("__OVER_BARS__", over_bars).replace("__UNDER_BARS__", under_bars)
        .replace("__TOP_FND__", top_fnd).replace("__SPEC_ROWS__", spec_rows).replace("__SUB_HTML__", sub_html)
        .replace("__N_ALL__", "{:,}".format(d["n_all"])).replace("__N_M__", "{:,}".format(d["n_matched"]))
        .replace("__MRATE__", "%.0f" % d["match_rate"]).replace("__KTOT__", "{:,}".format(d["kaken_total"]))
        .replace("__NFIELD__", str(d["agd_fields_in_cfg"]))
        .replace("__NDIST__", "{:,}".format(d["distinct_researchers"])).replace("__MULTI__", "{:,}".format(d["multi_winners"]))
        .replace("__STEM_RATE__", "%.0f" % d["match_by_kind"]["stem"]["rate"])
        .replace("__CULT_RATE__", "%.0f" % d["match_by_kind"]["culture"]["rate"])
        .replace("__AMT10M__", "%.0f" % L["amt10m_share"])
        .replace("__TAKEDA_N__", "{:,}".format(L["takeda_n"])).replace("__TAKEDA__", "%.1f" % L["takeda_share"])
        .replace("__AMT_TAKEDA__", "%.0f" % L["amt_takeda_share"]))
open("report/cfg-field-deep.html", "w").write(html)
import re
left = re.findall(r'__[A-Z_]+__', html)
print("saved report/cfg-field-deep.html (%d KB) | 未展開token %s" % (len(html)//1024, left))
