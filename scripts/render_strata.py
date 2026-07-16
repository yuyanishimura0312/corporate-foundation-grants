#!/usr/bin/env python3
"""研究層 詳細解析 + 本来助成すべき層 ダッシュボード(Part1-4)。赤白CI textbook・token置換。
   Part1 詳細分野(分野×テーマ微細セル+学際) / Part2 採択研究者連関(h/論文/共著/機関) /
   Part3 ×31.1万(被覆率+選抜性) / Part4 需要vs供給+産業創出プロセス仮説→本来助成すべき層。"""
import json
p123 = json.load(open("research_results/strata_part123.json"))
dem = json.load(open("research_results/strata_demand.json"))
S = p123["summary"]

def esc(s): return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

# Part1 微細セル bars
mx = max(x["n"] for x in p123["fine_cells"][:14])
cell_bars = "".join(
    '<div class="bar"><span class="bl">' + esc(x["field"]) + ' × ' + esc(x["theme"]) + '</span>'
    '<span class="bt"><span class="bf" style="width:' + str(int(100*x["n"]/mx)) + '%"></span></span>'
    '<span class="bn">' + str(x["n"]) + '件</span></div>' for x in p123["fine_cells"][:14])
sec_chips = "".join('<span class="chip">' + esc(x["primary"]) + '→' + esc(x["secondary"]) + ' <b>' + str(x["n"]) + '</b></span>' for x in p123["secondary_pairs"][:8])

# Part2 プロファイル テーブル
p2_rows = "".join(
    "<tr><td>" + esc(p["field"]) + "</td><td class='n'>" + str(p["n_award"]) + "</td>"
    "<td class='n'>" + (str(p["funded_h_med"]) if p["funded_h_med"] is not None else "—") + "</td>"
    "<td class='n'>" + (str(p["avg_works"]) if p["avg_works"] else "—") + "</td>"
    "<td class='n'>" + (str(p["avg_coauthor"]) if p["avg_coauthor"] else "—") + "</td>"
    "<td class='n'>" + (("%.0f%%" % p["young_pct"]) if p["young_pct"] is not None else "—") + "</td>"
    "<td>" + "・".join(esc(i["u"]) + str(i["n"]) for i in p["top_insts"][:2]) + "</td></tr>"
    for p in p123["profiles"])

# Part3 被覆率+選抜性 テーブル
p3_rows = "".join(
    "<tr><td>" + esc(p["field"]) + "</td><td class='n'>" + str(p["n_researchers"]) + "</td>"
    "<td class='n'>" + "{:,}".format(p["pool_311k_hontai"]) + "</td>"
    "<td class='n'>" + (("%.2f%%" % p["coverage_pct"]) if p["coverage_pct"] is not None else "—") + "</td>"
    "<td class='n'>" + (str(p["funded_h_med"]) if p["funded_h_med"] is not None else "—") + " / " + (str(p["pop_h_med"]) if p["pop_h_med"] is not None else "—") + "</td>"
    "<td class='n'>" + (("+%.0f" % p["h_uplift"]) if p["h_uplift"] is not None else "—") + "</td></tr>"
    for p in p123["profiles"])

# Part4 需要 vs 供給 テーブル(demand降順)
def rcls(r):
    if r is None: return "unreach"
    if r < 0.8: return "under"
    if r > 1.3: return "over"
    return ""
p4_rows = "".join(
    "<tr class='" + rcls(m["cfg_ratio"]) + "'><td>" + esc(m["field"]) + "</td>"
    "<td class='n'>" + str(m["policy"]) + "</td><td class='n'>" + str(m["industry"]) + "</td><td class='n'>" + str(m["social"]) + "</td>"
    "<td class='n'><b>" + str(m["demand"]) + "</b></td>"
    "<td class='n'>" + (("%.2f" % m["cfg_ratio"]) if m["cfg_ratio"] else "未到達") + "</td></tr>"
    for m in dem["by_demand"][:15])
under_chips = "".join('<span class="chip u">' + esc(m["field"]) + ' <b>' + str(m["demand"]) + '</b> <span class="dim">' + (("比%.2f" % m["cfg_ratio"]) if m["cfg_ratio"] else "未到達") + '</span></span>' for m in dem["underserved"][:12])

CH = [("ch1","01 OVERVIEW","1. 4つの問いと全体像"),("ch2","02 FINEGRAIN","2. 詳細分野（分野×テーマ微細セル）"),
      ("ch3","03 RESEARCHERS","3. 採択研究者との連関"),("ch4","04 POPULATION","4. 31.1万人母集団との関係"),
      ("ch5","05 DEMAND","5. 需要と供給のミスマッチ"),("ch6","06 PROCESS","6. 産業創出プロセス仮説"),
      ("ch7","07 STRATA","7. 本来助成されると良い研究層"),("ch8","08 LIMITS","8. 解析の限界")]
toc = "".join('<li><a href="#' + c + '"><span class="tn">' + code.split()[0] + '</span>' + t + '</a></li>' for c, code, t in CH)

CSS = """
:root{--bg:#FFF;--card:#FFF;--ink:#121212;--soft:#555;--mute:#8A7868;--accent:#CC1400;--accent2:#0E4F6B;--rule:#E4E0D8;--surf:#FAF8F4;--under:#CC1400;--over:#3F6B2E;--unreach:#0E4F6B}
[data-theme=dark]{--bg:#141210;--card:#1C1A17;--ink:#E8E2D8;--soft:#B8AE9E;--mute:#8A8070;--accent:#FF5A47;--rule:#33302A;--surf:#1F1C18;--under:#FF5A47;--over:#7DB86A;--unreach:#5AA8D0}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--ink);font-family:"Noto Sans JP",sans-serif;line-height:1.85;font-feature-settings:"palt"}
.top{position:sticky;top:0;z-index:50;background:var(--bg);border-bottom:3px solid var(--ink);padding:12px 24px;display:flex;justify-content:space-between;align-items:center}
.brand{font-weight:900;font-size:15px}.brand span{color:var(--accent)}
.tbtn{font-size:12px;padding:5px 12px;border:1px solid var(--rule);background:var(--card);color:var(--ink);cursor:pointer;border-radius:3px}
.layout{display:grid;grid-template-columns:250px 1fr;max-width:1160px;margin:0 auto}
.toc{position:sticky;top:60px;height:calc(100vh - 60px);overflow-y:auto;padding:24px 16px;border-right:1px solid var(--rule)}
.toc-label{font-size:11px;letter-spacing:.2em;color:var(--mute);margin-bottom:12px;font-family:"Fira Code"}
.toc ol{list-style:none}.toc li a{display:block;padding:6px 8px;font-size:12px;color:var(--soft);text-decoration:none;border-radius:3px}
.toc li a:hover{background:var(--surf);color:var(--accent)}.tn{font-family:"Fira Code";font-size:10px;color:var(--accent);margin-right:8px}
main{padding:32px 40px;max-width:820px}
.hero{border-bottom:2px solid var(--ink);padding-bottom:20px;margin-bottom:28px}
.tags{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:12px}.tag{font-size:10px;padding:3px 9px;background:var(--surf);color:var(--soft);border-radius:2px}.tag.p{background:var(--accent);color:#fff}
h1{font-size:24px;font-weight:900;line-height:1.4}
.hsub{font-size:13.5px;color:var(--soft);margin-top:8px}
.hmeta{font-size:11.5px;color:var(--mute);margin-top:12px;font-family:"Fira Code";display:flex;gap:16px;flex-wrap:wrap}
section{margin:40px 0;scroll-margin-top:70px}
.chlabel{font-family:"Fira Code";font-size:11px;color:var(--accent);letter-spacing:.15em}
h2{font-size:20px;font-weight:900;margin:2px 0 4px}
.rule{width:48px;height:3px;background:var(--accent);margin:8px 0 16px}
p.lead{font-size:14px;background:var(--surf);border-left:3px solid var(--accent);padding:12px 16px;margin:14px 0;line-height:1.9}
p.body{font-size:13.5px;margin:12px 0;text-align:justify}
h3{font-size:13px;font-weight:700;margin:18px 0 6px;color:var(--accent)}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:16px 0}
.stat{background:var(--card);border:1px solid var(--rule);border-top:3px solid var(--accent);padding:14px;text-align:center;border-radius:4px}
.snum{font-size:21px;font-weight:900;color:var(--accent);font-family:"Fira Code"}.slabel{font-size:10.5px;color:var(--soft);margin-top:4px}
.bar{display:grid;grid-template-columns:200px 1fr 60px;align-items:center;gap:10px;margin:3px 0;font-size:12px}
.bl{text-align:right;color:var(--soft);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.bt{background:var(--surf);height:14px;border-radius:2px;overflow:hidden}.bf{display:block;height:100%;background:var(--accent)}
.bn{font-family:"Fira Code";font-size:10.5px;text-align:right}
table{width:100%;border-collapse:collapse;font-size:11.5px;margin:10px 0}
th{background:var(--ink);color:var(--bg);padding:6px 8px;text-align:left;font-weight:500}
td{padding:5px 8px;border-bottom:1px solid var(--rule)}td.n{font-family:"Fira Code";text-align:right}
tr.under td:first-child{border-left:3px solid var(--under);font-weight:700}
tr.unreach td:first-child{border-left:3px solid var(--unreach);font-weight:700}
tr.over td:first-child{border-left:3px solid var(--over)}
.chip{font-size:11px;background:var(--surf);padding:3px 9px;border-radius:12px;color:var(--soft);display:inline-block;margin:2px}.chip b{color:var(--accent);font-family:"Fira Code"}
.chip.u{border:1px solid var(--under)}.chip .dim{color:var(--mute);font-size:9.5px}
.callout{background:var(--ink);color:#F4F1EC;padding:16px 20px;border-radius:5px;margin:16px 0;font-size:13px;line-height:1.85}.callout b{color:#FF8A78}
.caveat{background:#FFF8F0;border:1px solid #E8D0B0;border-left:3px solid var(--accent2);padding:12px 16px;font-size:12.5px;margin:14px 0;color:var(--soft)}
[data-theme=dark] .caveat{background:#20201A;border-color:#4A4436}
.rec{display:flex;gap:14px;align-items:flex-start;margin:14px 0;padding:14px 16px;background:var(--surf);border-radius:5px}
.rnum{font-family:"Fira Code";font-size:26px;font-weight:900;color:var(--accent);line-height:1}.rb b{color:var(--accent)}
.legend{font-size:11px;color:var(--mute);margin:6px 0;font-family:"Fira Code"}.legend .u{color:var(--under)}.legend .o{color:var(--over)}.legend .n{color:var(--unreach)}
.foot{margin:40px 0 20px;padding-top:16px;border-top:2px solid var(--accent);font-size:11px;color:var(--mute);display:flex;justify-content:space-between}
@media(max-width:860px){.layout{grid-template-columns:1fr}.toc{display:none}.stats{grid-template-columns:repeat(2,1fr)}main{padding:24px 20px}.bar{grid-template-columns:130px 1fr 50px}}
"""

TPL = """<!DOCTYPE html><html lang="ja" data-theme="light"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>CFG 研究層 詳細解析と本来助成すべき層 — 企業財団 研究助成</title><link rel="icon" href="https://esse-sense.com/favicon.ico">
<link href="https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@300;400;500;700;900&family=Fira+Code&display=swap" rel="stylesheet">
<style>__CSS__</style></head><body>
<div class="top"><div class="brand">MIRATUKU / <span>CFG</span> 研究層 詳細解析 + 本来助成すべき層</div>
<div><a href="cfg-field-domain.html" style="font-size:12px;color:var(--accent);margin-right:14px;text-decoration:none">研究領域 深掘り &rarr;</a>
<button class="tbtn" onclick="var h=document.documentElement;h.dataset.theme=h.dataset.theme=='dark'?'light':'dark'">DARK/LIGHT</button></div></div>
<div class="layout"><nav class="toc"><div class="toc-label">CONTENTS</div><ol>__TOC__</ol></nav><main>
<div class="hero"><div class="tags"><span class="tag p">CFG</span><span class="tag">詳細分野</span><span class="tag">研究者31.1万</span><span class="tag">テーマ連携</span><span class="tag">産業創出プロセス</span></div>
<h1>企業財団 研究助成<br>研究層の詳細解析と「本来助成されると良い層」</h1>
<p class="hsub">分野×テーマの微細セルまで解像度を上げ、採択研究者を研究者DB31.1万人の属性・母集団と連関させ、産業・社会・施策テーマとの連携と産業創出プロセス仮説から「本来助成されると良い研究層」を導く。</p>
<div class="hmeta"><span>更新: 2026-07-17</span><span>採択: __NAWARD__件</span><span>接地研究者: __NRES__名</span><span>需要: NGF施策200+GVC産業/社会</span><span>プロセス: ICFM/VCPM</span></div></div>

<section id="ch1"><div class="chlabel">CHAPTER 01 — OVERVIEW</div><h2>1. 4つの問いと全体像</h2><div class="rule"></div>
<p class="lead">本稿は4つの問いに答える。<b>(1) より詳細な分野で</b>—分野×研究テーマの微細セルまで。<b>(2) 採択研究者との連関</b>—採択者は研究者DBでどんな属性か。<b>(3) 31.1万人母集団との関係</b>—母集団のどの層が、どれだけ拾われているか。<b>(4) 本来助成されると良い層</b>—産業・社会・施策テーマとの連携と産業創出プロセス仮説から、供給が薄い高需要層を特定する。</p>
<div class="stats"><div class="stat"><div class="snum">__FH__ / __PH__</div><div class="slabel">採択者h指数 / 母集団h指数(中央値)</div></div>
<div class="stat"><div class="snum">0.5-2.6%</div><div class="slabel">各分野母集団の被覆率</div></div>
<div class="stat"><div class="snum">189</div><div class="slabel">計算機科学のテーマ連携数(最大)</div></div>
<div class="stat"><div class="snum">0.34</div><div class="slabel">数学のCFG代表性比(最小)</div></div></div>
<p class="body">全体像はこうだ。企業財団は各分野の母集団の<b>ごく一部（0.5〜2.6%）の、しかもh指数が母集団中央値の2倍を超える上位層</b>を拾う。一方、産業・社会・施策テーマを最も広く下支えする<b>横断的分野（情報・電気・統計・数学・経済・社会科学）は構造的に手薄</b>で、企業財団が厚く投じる生命科学・化学とはミッションが噛み合っても、社会全体の研究需要とは噛み合っていない。産業創出プロセス仮説（統合点が律速）は、この横断的分野の重要性を裏づける。</p></section>

<section id="ch2"><div class="chlabel">CHAPTER 02 — FINEGRAIN</div><h2>2. 詳細分野（分野×テーマ微細セル）</h2><div class="rule"></div>
<p class="body">学問分野（西洋医学・生物学…）と研究テーマ（がん・免疫・ゲノム…）を掛け合わせ、実際の研究領域を微細セルまで分解する。「西洋医学×創薬」「西洋医学×がん」「生物学×免疫」が突出し、企業財団助成の中核が<b>創薬・がん・免疫・ゲノムという臨床/生命科学の具体テーマ</b>にあることが、分野ラベルより鮮明になる。</p>
__CELL_BARS__
<h3>採択者の学際性（主専門→副専門）</h3>
<div>__SEC_CHIPS__</div>
<div class="caveat">※ 学際性データは薄い（研究者DBの副専門分野が__NSEC__名分のみ登録）。西洋医学↔生物学の相互越境が支配的だが、母数が小さく傾向の提示に留める。</div></section>

<section id="ch3"><div class="chlabel">CHAPTER 03 — RESEARCHERS</div><h2>3. 採択研究者との連関</h2><div class="rule"></div>
<p class="body">採択者を研究者DB31.1万人に接続し、分野別に属性を描く。h指数（被引用の多さ）・論文数・共著者数・若手率・主要機関。企業財団が「どんな研究者を選ぶか」の実像である。</p>
<table><tr><th>分野</th><th>採択</th><th>h指数中央</th><th>平均論文</th><th>平均共著</th><th>若手率</th><th>主要機関</th></tr>__P2_ROWS__</table>
<div class="callout"><b>採択者のh指数中央値は12.0で、母集団全体の5.0の2.4倍。企業財団は各分野の「すでに実績のある上位研究者」を強く選抜している。</b>ただし分野差があり、社会学は採択者h指数2.0＝母集団と同水準（人文社会は被引用文化が異なりh指数が機能しにくい）、機械工学も選抜性が弱い。生命科学系ほど「エリート選抜」が強く働く。</div></section>

<section id="ch4"><div class="chlabel">CHAPTER 04 — POPULATION</div><h2>4. 31.1万人母集団との関係</h2><div class="rule"></div>
<p class="lead">採択研究者を、各分野の全国母集団（科研費収録の本体研究者）と対照する。<b>被覆率</b>＝母集団の何%が企業財団に拾われたか。<b>選抜性(h上乗せ)</b>＝拾われた研究者が母集団平均よりどれだけ高h指数か。</p>
<table><tr><th>分野</th><th>採択研究者</th><th>母集団(本体)</th><th>被覆率</th><th>h採択/母集団</th><th>選抜上乗せ</th></tr>__P3_ROWS__</table>
<div class="callout"><b>企業財団が拾うのは母集団の0.5〜2.6%というごく薄い層で、その多くが母集団より数ポイント高いh指数を持つ。</b>農学（+8）・生理学（+6）・生化学/薬学/電気工学（+5）では選抜が非常に強く、企業財団は「実績上位の少数」に集中する。逆に社会学・機械工学は被覆率も選抜上乗せも低く、企業財団の関与がそもそも薄い。<b>母集団の裾野（若手・中堅・地方）は構造的に届いていない。</b></div>
<div class="caveat">※ 被覆率は接地済み（RIDマッチ64%）で数えるため<b>下限値</b>。未マッチの採択者を含めれば実被覆はやや高い。h指数は人文社会で機能しにくい（被引用文化差）。母集団は本体（科研費収録）で、サブDB研究者は属性が希薄なため本章は本体で算出。</div></section>

<section id="ch5"><div class="chlabel">CHAPTER 05 — DEMAND</div><h2>5. 需要と供給のミスマッチ</h2><div class="rule"></div>
<p class="lead">「本来助成されると良い層」を導く核。<b>需要</b>＝各学問分野が下支えする 施策テーマ（NGF 200・政策）＋産業テーマ＋社会テーマ（GVC）の数。<b>供給</b>＝企業財団の分野別代表性比（1=母集団並・&lt;1=手薄）。需要が高いのに供給が薄い分野が、社会が必要とするのに企業財団が届いていない層である。</p>
<div class="legend"><span class="u">■</span> 供給薄(比&lt;0.8) ／ <span class="n">■</span> 企業財団ほぼ未到達 ／ <span class="o">■</span> 供給厚(比&gt;1.3・生命科学)</div>
<table><tr><th>学問分野</th><th>政策</th><th>産業</th><th>社会</th><th>テーマ計</th><th>CFG代表性比</th></tr>__P4_ROWS__</table>
<div class="callout"><b>需要と供給が反転している。産業・社会・施策テーマを最も広く下支えする分野（計算機科学189・電気工学179・経営学121・経済学108・統計学93・社会学98・数学55）を、企業財団はいずれも母集団以下（比0.34〜0.69）でしか助成せず、経営学・地理学に至ってはほぼ未到達。</b>逆に企業財団が厚く投じる化学・薬学・生化学・生物学（比1.7〜1.9）は、テーマ連携数がむしろ中位。企業財団のミッション（生命科学・化学）は妥当だが、それが社会・産業・政策の研究需要の広がりとは噛み合っていない。</div>
<div class="caveat">※ 「需要」は NGF（政策注目）・GVC（VC投資が見る市場）という2つのレンズであり、社会的価値そのものではない。「未到達（測定不能）」はRIDマッチ済みの企業財団助成に当該分野がほぼ現れないことを指し、真の助成不在と接続バイアスの両方を含む。ただし<b>需要側（NGF/GVC）はRIDマッチに非依存</b>で、需要の高さは頑健。</div></section>

<section id="ch6"><div class="chlabel">CHAPTER 06 — PROCESS</div><h2>6. 産業創出プロセス仮説</h2><div class="rule"></div>
<p class="body">なぜ横断的分野が重要か。産業創出の実証モデル（ICFM）と価値創造経路モデル（VCPM）は、新産業の成否を分けるのは<b>核となる科学の量ではなく「統合点（integration）」と「基盤の完全性（infra）」</b>だと示す。ICFMの産業創出100ケースで統合点を備えたのは<b>22ケースのみ</b>、VCPMの経路readinessは基盤完全性F=0.85に対し統合点の質Q=0.55が律速していた。</p>
<div class="callout"><b>VCPMの次世代電池ケースが象徴的だ。核となる無機化学(KSI 0.85)・材料科学(0.82)・電気化学(0.67)は充足しているのに、産業化の律速は「化学工学＝量産プロセス(KSI 0.12・不十分)」「計算材料科学(0.22・不十分)」「界面科学(0.44・部分的)」——すなわち統合・実装・データの分野にある。</b>企業財団は充足済みの核科学（無機化学・材料）を厚く助成する一方、実際のボトルネックである統合・工学・計算の横断分野は手薄——第5章の需要供給ミスマッチと、産業創出プロセスの律速が、同じ場所を指している。</div>
<div class="caveat">※ ICFM/VCPMのプロセス信号はアンカーテーマ（次世代電池・モビリティ等）中心で全分野を覆わない。本章は「統合点・基盤が律速」という定性的仮説の例証であり、全分野の定量割当ではない。</div></section>

<section id="ch7"><div class="chlabel">CHAPTER 07 — STRATA</div><h2>7. 本来助成されると良い研究層</h2><div class="rule"></div>
<p class="lead">第2〜6章を統合する。「本来助成されると良い層」＝<b>需要が高く（テーマ連携・産業創出の律速）／供給が薄く（企業財団の代表性比&lt;1・被覆率低）／裾野が届いていない（非旧帝大・地方・若手）</b>研究層。企業財団のミッションを踏まえ、実装可能性で3層に分ける。</p>

<div class="rec"><div class="rnum">1</div><div class="rb"><b>横断的enabling分野の若手</b>（計算機科学・統計学・数学・電気工学・機械工学）<br>
テーマ連携が最大級（計算機189・電気179・統計93）かつ産業創出の律速（統合・計算・実装）でありながら、企業財団の代表性比は0.34〜0.91と手薄。<b>科学技術ミッションの内側で企業財団が今すぐ動ける最有力層</b>。とくに非旧帝大・地方の若手（被覆率0.9〜1.2%）を厚くする余地が大きい。</div></div>

<div class="rec"><div class="rnum">2</div><div class="rb"><b>基礎科学の統合・界面領域</b>（化学工学・計算材料・界面科学など、核科学と応用の"あいだ"）<br>
企業財団が既に厚い生命科学・化学の<b>隣接する統合層</b>。VCPMが示す律速点（KSI 0.12〜0.44）であり、既存ミッションの延長線上で「核科学の次のボトルネック」に配分を移せる。</div></div>

<div class="rec"><div class="rnum">3</div><div class="rb"><b>社会・政策テーマを支える社会科学（経済・経営・社会・地理・心理）</b><br>
施策テーマ・社会テーマの大宗を支える（社会学98・経済108・経営121）が、企業財団のミッション外でほぼ未到達。<b>これは企業財団単独でなく、公的資金・大学・分野横断財団が担うべき構造的空白</b>として明示する（NPO中立の観点から、企業財団に無理に負わせず、担い手の分担を提言する）。</div></div>

<div class="callout"><b>要するに、企業財団が最大のインパクトを出せるのは「核科学をさらに厚くする」ことではなく、既に充足した核科学の隣で律速している統合・計算・工学の横断分野、とくにその非旧帝大・地方の若手へ配分を移すことである。</b>社会科学・人文の構造的空白は、企業財団の限界を正直に認め、他の担い手との分担で埋める。</div></section>

<section id="ch8"><div class="chlabel">CHAPTER 08 — LIMITS</div><h2>8. 解析の限界</h2><div class="rule"></div>
<div class="caveat">
・<b>需要は2レンズの代理</b>：NGF（政策注目）とGVC（VC投資が見る市場）であり、社会的価値そのものではない。政策・市場が拾わない基礎研究の価値を過小評価しうる。<br>
・<b>供給側にRIDマッチバイアス</b>：CFG代表性比・被覆率は接地済み（マッチ64%・人文社会は接続率低）で算出。人社の「未到達」は真の不在と接続漏れの混在。ただし需要側（NGF/GVC）はマッチ非依存で頑健。<br>
・<b>h指数の分野差</b>：人文社会は被引用文化が異なりh指数が機能しにくい。選抜性の分野間比較は生命/自然科学内で読む。<br>
・<b>被覆率は下限</b>：未マッチ採択者を除くため実被覆はやや高い。本体（科研費収録）で算出、サブDB研究者は属性希薄で除外。<br>
・<b>産業創出プロセス信号は限定被覆</b>：ICFM/VCPMはアンカーテーマ中心。全分野の定量割当でなく定性的例証。<br>
・<b>金額は不使用</b>：武田1財団が金額記録の8割超を占めるアーティファクトのため。<br>
・企業財団（corporate/group）に限定。ミッション上、社会科学助成を企業財団に求めることの妥当性は分担論として別途要検討。
</div></section>

<div class="foot"><span>NPO法人ミラツク ／ esse-sense</span><span>CFG 研究層 詳細解析 + 本来助成すべき層 ／ 2026.07.17 ／ 全数実測・研究者DB31.1万接続・テーマ連携・産業創出プロセス</span></div>
</main></div></body></html>"""

html = (TPL.replace("__CSS__", CSS).replace("__TOC__", toc)
        .replace("__CELL_BARS__", cell_bars).replace("__SEC_CHIPS__", sec_chips)
        .replace("__P2_ROWS__", p2_rows).replace("__P3_ROWS__", p3_rows).replace("__P4_ROWS__", p4_rows)
        .replace("__NAWARD__", "{:,}".format(S["n_award"])).replace("__NRES__", "{:,}".format(S["n_researchers_hontai"]))
        .replace("__FH__", str(S["funded_h_med"])).replace("__PH__", str(S["pop_h_med"]))
        .replace("__NSEC__", str(S["n_with_secondary"])))
open("report/cfg-strata.html", "w").write(html)
import re
left = re.findall(r'__[A-Z_]+__', html)
print("saved report/cfg-strata.html (%d KB) | 未展開token %s" % (len(html)//1024, left))
