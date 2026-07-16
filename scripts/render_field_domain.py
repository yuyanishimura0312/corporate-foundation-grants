#!/usr/bin/env python3
"""研究領域 深掘りダッシュボード — 赤白CI textbook。token置換(literal %保護)。
   分野(discipline)の一段下=実質的な研究テーマ領域。先端テーマ prevalence・分野横断性・
   分野別研究領域プロファイル・財団エコロジー・脆弱分野。金額は不使用(武田寡占アーティファクト)。"""
import json
d = json.load(open("research_results/field_domain.json"))

def esc(s): return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def hbars(pairs, denom, unit="件", maxn=None, color=None):
    items = pairs[:maxn] if maxn else pairs
    mx = max((n for _, n in items), default=1)
    o = []
    for k, n in items:
        w = int(100 * n / mx)
        cls = "bf " + color if color else "bf"
        tail = "{:,}".format(n) + unit + ((" (%.1f%%)" % (100*n/denom)) if denom else "")
        o.append('<div class="bar"><span class="bl">' + esc(k) + '</span><span class="bt"><span class="' + cls + '" style="width:' + str(w) + '%"></span></span><span class="bn">' + tail + '</span></div>')
    return "".join(o)

# ch2 テーマ prevalence(件・%)
theme_bars = hbars([(t["theme"], t["n"]) for t in d["themes"][:16]], d["n"])
# ch3 分野横断性(跨り分野数降順)
cc = sorted(d["themes"], key=lambda z: -z["n_fields"])
cc_bars = "".join(
    '<div class="bar"><span class="bl">' + esc(x["theme"]) + '</span><span class="bt"><span class="bf cc" style="width:' + str(int(100*x["n_fields"]/24)) + '%"></span></span>'
    '<span class="bn">' + str(x["n_fields"]) + '分野に跨る <span class="dim">(' + "{:,}".format(x["n"]) + '件)</span></span></div>'
    for x in cc[:12])

# ch4 分野別プロファイル カード
prof_cards = []
for p in d["profiles"]:
    themes = "".join('<span class="chip">' + esc(t["t"]) + ' <b>' + str(t["n"]) + '</b></span>' for t in p["top_themes"][:4])
    hhi_cls = "hot" if p["hhi"] >= 0.15 else "cool"
    prof_cards.append(
        '<div class="pcard"><div class="pc-head"><span class="pc-field">' + esc(p["field"]) + '</span><span class="pc-n">' + "{:,}".format(p["n"]) + '件</span></div>'
        '<div class="pc-meta"><span>財団 <b>' + str(p["n_foundations"]) + '</b></span>'
        '<span class="' + hhi_cls + '">集中HHI <b>' + ("%.2f" % p["hhi"]) + '</b></span>'
        '<span>筆頭 ' + esc(p["top_foundation"][:12]) + ' <b>' + ("%.0f" % p["top_foundation_share"]) + '%</b></span>'
        '<span>非旧帝 <b>' + (("%.0f%%" % p["nonkyutei_pct"]) if p["nonkyutei_pct"] is not None else "—") + '</b></span>'
        '<span>若手 <b>' + (("%.0f%%" % p["young_pct"]) if p["young_pct"] is not None else "—") + '</b></span></div>'
        '<div class="pc-themes">' + themes + '</div></div>')
prof_html = "".join(prof_cards)

# ch5 財団エコロジー(HHI 上位分野・分散↔寡占)
eco = sorted(d["profiles"], key=lambda z: -z["hhi"])
eco_rows = "".join(
    "<tr><td>" + esc(p["field"]) + "</td><td class='n'>" + str(p["n"]) + "</td><td class='n'>" + str(p["n_foundations"]) + "</td>"
    "<td class='n'>" + ("%.2f" % p["hhi"]) + "</td><td>" + ("寡占" if p["hhi"] >= 0.15 else "分散") + " / " + esc(p["top_foundation"][:14]) + " " + ("%.0f" % p["top_foundation_share"]) + "%</td></tr>"
    for p in eco)

# ch6 脆弱分野
vuln_rows = "".join(
    "<tr><td>" + esc(x["field"]) + "</td><td class='n'>" + str(x["n"]) + "</td><td class='n'>" + str(x["n_foundations"]) + "</td>"
    "<td>" + esc(x["top_foundation"][:16]) + " <b>" + ("%.0f" % x["top_share"]) + "%</b></td></tr>"
    for x in d["vuln_fewfund"][:10])

CH = [("ch1","01 OVERVIEW","1. 研究領域という切り口"),("ch2","02 THEMES","2. 先端研究テーマの実勢"),
      ("ch3","03 CROSSCUT","3. テーマの分野横断性"),("ch4","04 PROFILES","4. 分野別 研究領域プロファイル"),
      ("ch5","05 ECOLOGY","5. 財団エコロジー（分散↔寡占）"),("ch6","06 VULNERABILITY","6. 少数財団依存の脆弱分野"),
      ("ch7","07 LIMITS","7. 解析の限界")]
toc = "".join('<li><a href="#' + c + '"><span class="tn">' + code.split()[0] + '</span>' + t + '</a></li>' for c, code, t in CH)

CSS = """
:root{--bg:#FFF;--card:#FFF;--ink:#121212;--soft:#555;--mute:#8A7868;--accent:#CC1400;--accent2:#0E4F6B;--rule:#E4E0D8;--surf:#FAF8F4;--cc:#0E4F6B;--hot:#CC1400;--cool:#0E4F6B}
[data-theme=dark]{--bg:#141210;--card:#1C1A17;--ink:#E8E2D8;--soft:#B8AE9E;--mute:#8A8070;--accent:#FF5A47;--rule:#33302A;--surf:#1F1C18;--cc:#5AA8D0;--hot:#FF5A47;--cool:#5AA8D0}
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
main{padding:32px 40px;max-width:810px}
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
.snum{font-size:22px;font-weight:900;color:var(--accent);font-family:"Fira Code"}.slabel{font-size:11px;color:var(--soft);margin-top:4px}
.bar{display:grid;grid-template-columns:180px 1fr 150px;align-items:center;gap:10px;margin:3px 0;font-size:12px}
.bl{text-align:right;color:var(--soft);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.bt{background:var(--surf);height:14px;border-radius:2px;overflow:hidden}.bf{display:block;height:100%;background:var(--accent)}
.bf.cc{background:var(--cc)}
.bn{font-family:"Fira Code";font-size:10.5px;text-align:right}.bn .dim{color:var(--mute);font-size:9.5px}
.pcard{border:1px solid var(--rule);border-left:3px solid var(--accent);border-radius:5px;padding:12px 14px;margin:10px 0;background:var(--card)}
.pc-head{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:6px}
.pc-field{font-size:15px;font-weight:900}.pc-n{font-family:"Fira Code";font-size:12px;color:var(--accent)}
.pc-meta{display:flex;gap:14px;flex-wrap:wrap;font-size:11.5px;color:var(--soft);margin-bottom:8px}
.pc-meta b{color:var(--ink);font-family:"Fira Code"}.pc-meta .hot b{color:var(--hot)}.pc-meta .cool b{color:var(--cool)}
.pc-themes{display:flex;gap:6px;flex-wrap:wrap}
.chip{font-size:11px;background:var(--surf);padding:3px 9px;border-radius:12px;color:var(--soft)}.chip b{color:var(--accent);font-family:"Fira Code"}
table{width:100%;border-collapse:collapse;font-size:12px;margin:10px 0}
th{background:var(--ink);color:var(--bg);padding:7px 9px;text-align:left;font-weight:500}
td{padding:6px 9px;border-bottom:1px solid var(--rule)}td.n{font-family:"Fira Code";text-align:right}
.callout{background:var(--ink);color:#F4F1EC;padding:16px 20px;border-radius:5px;margin:16px 0;font-size:13px;line-height:1.85}.callout b{color:#FF8A78}
.caveat{background:#FFF8F0;border:1px solid #E8D0B0;border-left:3px solid var(--accent2);padding:12px 16px;font-size:12.5px;margin:14px 0;color:var(--soft)}
[data-theme=dark] .caveat{background:#20201A;border-color:#4A4436}
.foot{margin:40px 0 20px;padding-top:16px;border-top:2px solid var(--accent);font-size:11px;color:var(--mute);display:flex;justify-content:space-between}
@media(max-width:860px){.layout{grid-template-columns:1fr}.toc{display:none}.stats{grid-template-columns:repeat(2,1fr)}main{padding:24px 20px}.bar{grid-template-columns:120px 1fr 110px}}
"""

TPL = """<!DOCTYPE html><html lang="ja" data-theme="light"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>CFG 研究領域 深掘り解析 — 企業財団 研究助成</title><link rel="icon" href="https://esse-sense.com/favicon.ico">
<link href="https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@300;400;500;700;900&family=Fira+Code&display=swap" rel="stylesheet">
<style>__CSS__</style></head><body>
<div class="top"><div class="brand">MIRATUKU / <span>CFG</span> 研究領域 深掘り解析</div>
<div><a href="cfg-field-deep.html" style="font-size:12px;color:var(--accent);margin-right:14px;text-decoration:none">学問分野 詳細解析 &rarr;</a>
<button class="tbtn" onclick="var h=document.documentElement;h.dataset.theme=h.dataset.theme=='dark'?'light':'dark'">DARK/LIGHT</button></div></div>
<div class="layout"><nav class="toc"><div class="toc-label">CONTENTS</div><ol>__TOC__</ol></nav><main>
<div class="hero"><div class="tags"><span class="tag p">CFG</span><span class="tag">研究領域</span><span class="tag">先端テーマ20</span><span class="tag">分野横断</span><span class="tag">財団エコロジー</span></div>
<h1>企業財団 研究助成<br>研究領域 深掘り解析</h1>
<p class="hsub">学問分野（discipline）の一段下、実際の研究課題名から立ち上がる「研究テーマ領域」で解析する。何のテーマに助成が流れ、それが分野の壁をどう越え、どの財団に支えられ、どこが脆弱かを全数実測する。</p>
<div class="hmeta"><span>更新: 2026-07-17</span><span>研究助成: __N__件</span><span>先端テーマ該当: __ANY__%</span><span>テーマ辞書: 20領域</span></div></div>

<section id="ch1"><div class="chlabel">CHAPTER 01 — OVERVIEW</div><h2>1. 研究領域という切り口</h2><div class="rule"></div>
<p class="lead">前稿（学問分野 詳細解析）はAGD 44分野という<b>学問の区分</b>で助成を見た。本稿はその一段下、課題名から抽出した<b>実質的な研究テーマ領域</b>（創薬・免疫・脳神経・ゲノム・AI・材料…）で見る。テーマは学問分野の壁を越えて広がるため、「どの分野に属すか」では見えない助成の実勢が浮かぶ。__N__件の課題名を20の先端テーマ辞書で分類し、__ANY__%が少なくとも1つのテーマに該当した。</p>
<div class="stats"><div class="stat"><div class="snum">694</div><div class="slabel">創薬・治療・診断(最多11%)</div></div>
<div class="stat"><div class="snum">23</div><div class="slabel">AIが跨る分野数(最多横断)</div></div>
<div class="stat"><div class="snum">22</div><div class="slabel">地域・福祉が跨る分野数</div></div>
<div class="stat"><div class="snum">11</div><div class="slabel">法学を支える財団数(最少)</div></div></div>
<p class="body">3つの発見。第一に、テーマで見ても<b>創薬・免疫・脳神経・がん・ゲノムという臨床/生命科学が上位</b>を独占する。第二に、テーマは<b>分野を強く横断する</b>—「AI・機械学習」は絶対量こそ3%だが<b>23分野</b>に浸透して最も横断的で、「地域・福祉」も<b>22分野</b>に跨り医学と社会科学の境界に立つ。第三に、テーマごとに<b>支える財団の厚みが極端に違う</b>—生命科学は武田寡占、化学・物理は分散、人文系は少数財団依存で脆弱である。</p></section>

<section id="ch2"><div class="chlabel">CHAPTER 02 — THEMES</div><h2>2. 先端研究テーマの実勢</h2><div class="rule"></div>
<p class="body">課題名に現れる研究テーマの出現率（多重該当を許容・全__N__件に対する%）。学問分野ラベルではなく「実際に何を研究する助成か」を示す。臨床・生命科学のテーマが上位を占め、材料・AI・エネルギー・環境が中位に続く。</p>
__THEME_BARS__
<div class="caveat">※ テーマは課題名（平均32字）のキーワードで判定するため多重該当があり、合計は100%を超える。「創薬・治療・診断」「免疫・感染・炎症」等は広めのテーマで、細かな主題までは分解しない。抄録全文は本DBに無く、未該当47%は装置・国際会議・奨学金等の非課題型を含む。</div></section>

<section id="ch3"><div class="chlabel">CHAPTER 03 — CROSSCUT</div><h2>3. テーマの分野横断性</h2><div class="rule"></div>
<p class="lead">同じ研究テーマが、いくつのAGD学問分野に跨って現れるか。数が大きいほど「特定分野の専有物でなく、分野を越えて研究される道具・課題」であることを意味する。</p>
__CC_BARS__
<div class="callout"><b>「AI・機械学習」は絶対量3%ながら23分野に浸透し、分野を問わない汎用ツールとして最も横断的に助成に現れる。「地域・福祉」は22分野に跨り、その主分野は西洋医学（20）と社会学（18）がほぼ拮抗する—高齢化・介護というテーマが医学と社会科学の境界に立つことを映す。</b>一方「がん・腫瘍」（12分野）や「再生・幹細胞」（10分野）は特定の生命科学分野に集中する、専有度の高いテーマである。研究領域の設計では、<b>横断テーマ（AI・データ・社会課題）</b>と<b>専有テーマ（がん・再生）</b>を分けて考える必要がある。</div></section>

<section id="ch4"><div class="chlabel">CHAPTER 04 — PROFILES</div><h2>4. 分野別 研究領域プロファイル</h2><div class="rule"></div>
<p class="body">上位10のAGD分野それぞれを、①分野内で厚い研究テーマ ②財団エコロジー（財団数・集中HHI・筆頭財団） ③機関ティア（非旧帝大比率） ④キャリア（若手比率）で立体的に描く。同じ「生命科学」でも、西洋医学は創薬・免疫・がん、生物学は免疫・ゲノム、と厚いテーマが異なる。</p>
__PROF_CARDS__
<p class="body">化学は分野内の財団集中HHIが<b>0.05</b>と低く（多数の財団が薄く広く助成）、テーマは材料・ナノに集中する。対して西洋医学・生物学・生化学・生理学は武田筆頭でHHI 0.22〜0.28と高い。<b>研究領域ごとに「担い手の生態系」が根本的に異なる</b>ことが、プロファイルから読める。</p></section>

<section id="ch5"><div class="chlabel">CHAPTER 05 — ECOLOGY</div><h2>5. 財団エコロジー（分散↔寡占）</h2><div class="rule"></div>
<p class="body">分野を「何財団が支え、筆頭がどれだけ占めるか」で並べる。HHI（1に近いほど1財団集中）が高い分野は少数財団への依存が強い。</p>
<table><tr><th>分野</th><th>件数</th><th>財団数</th><th>HHI</th><th>構造 / 筆頭財団</th></tr>__ECO_ROWS__</table>
<div class="callout"><b>生命科学系（西洋医学・生物学・生化学・生理学）はいずれも武田科学振興財団が筆頭でHHI 0.22〜0.28の寡占。化学・物理・計算機科学・機械工学はHHI 0.05前後の分散型で、多数の財団が薄く支える。</b>寡占型の分野は筆頭財団の方針変更が分野全体の資金供給を左右する脆弱性を、分散型の分野は調整役不在で戦略的集中がしにくい特性を、それぞれ持つ。</div></section>

<section id="ch6"><div class="chlabel">CHAPTER 06 — VULNERABILITY</div><h2>6. 少数財団依存の脆弱分野</h2><div class="rule"></div>
<p class="body">助成件数が一定以上（n≥20）ありながら、支える財団数が少ない分野。単一財団が抜けると分野全体の供給が細る構造的リスクを抱える。</p>
<table><tr><th>分野</th><th>件数</th><th>支える財団数</th><th>筆頭財団（占有率）</th></tr>__VULN_ROWS__</table>
<div class="callout"><b>法学（11財団）・歴史学（12財団）・数学（13財団）・生態学（14財団）は、企業財団の中でごく少数にしか支えられていない。特に歴史学は三菱財団1財団で62%を占め、この1財団が撤退すれば分野助成の過半が消える。</b>基礎理学（数学・生態学）と人文社会（法学・歴史学・経済学）は、企業財団エコシステムの中で構造的に脆い。集中支援戦略と併せ、これらの「担い手の薄い領域」をどう支えるかが問われる。</div></section>

<section id="ch7"><div class="chlabel">CHAPTER 07 — LIMITS</div><h2>7. 解析の限界</h2><div class="rule"></div>
<div class="caveat">
・<b>テーマは課題名キーワードで判定</b>：抄録全文は本DBに無く、平均32字の課題名で多重分類する。未該当__UNMATCH__%は非課題型（装置・国際会議・奨学金・活動助成）を含む。テーマ辞書は20領域の設計判断であり、粒度・境界には幅がある。<br>
・<b>独立検証で偽陽性を是正済</b>：「地域・福祉」の"障害"（＝腎/神経/睡眠障害の病態）・"子ども"（＝基礎神経科学）、「宇宙」の"月"（＝ヶ月/半月板）などの偽友を除外した。それでも「地域・福祉」は高齢化・介護研究が医学と社会福祉の両文脈に跨るため、主分野で西洋医学と社会学が拮抗する（テーマ本来の境界性であり誤りではない）。<br>
・<b>分野横断性は接地済みのみ</b>：跨り分野数はrid_field接地済み課題で数える。人文社会系は研究者DB（KAKEN基盤）接続率が低く（科学系78% vs 文化芸術系12%）、テーマの分野内訳はSTEM寄りに偏る。<br>
・<b>金額は解析しない</b>：金額判明記録の8割超を武田1財団が占めるため、テーマ別・分野別の金額比較は行わない（前稿参照）。<br>
・<b>財団エコロジー・脆弱性は件数ベース</b>：延べ採択件数で数える（distinct研究者ではない）。脆弱分野の件数閾値はn≥20。<br>
・企業財団（corporate/group）に限定し、大学・政府・国際財団は含まない。
</div></section>

<div class="foot"><span>NPO法人ミラツク ／ esse-sense</span><span>CFG 研究領域 深掘り解析 ／ 2026.07.17 ／ 全数実測・課題名接地・研究者DB接続</span></div>
</main></div></body></html>"""

html = (TPL.replace("__CSS__", CSS).replace("__TOC__", toc)
        .replace("__THEME_BARS__", theme_bars).replace("__CC_BARS__", cc_bars)
        .replace("__PROF_CARDS__", prof_html).replace("__ECO_ROWS__", eco_rows).replace("__VULN_ROWS__", vuln_rows)
        .replace("__N__", "{:,}".format(d["n"])).replace("__ANY__", "%.0f" % d["any_theme_pct"])
        .replace("__UNMATCH__", "%.0f" % (100 - d["any_theme_pct"])))
open("report/cfg-field-domain.html", "w").write(html)
import re
left = re.findall(r'__[A-Z_]+__', html)
print("saved report/cfg-field-domain.html (%d KB) | 未展開token %s" % (len(html)//1024, left))
