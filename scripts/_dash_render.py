# -*- coding: utf-8 -*-
# CFG 詳細ダッシュボード HTML生成部 (generate_detailed_dashboard.py から exec される)
CHAPTERS = [
  ("ch1", "01 OVERVIEW", "1. データベース全体像"), ("ch2", "02 TAXONOMY", "2. 設立者形態・法人形態・所管庁"),
  ("ch3", "03 GEOGRAPHY", "3. 都道府県分布（47）"), ("ch4", "04 AWARDEES", "4. 助成先研究者（採択者）"),
  ("ch5", "05 FIELD", "5. 学問分野分布（研究者DB接続）"), ("ch6", "06 OFFICERS", "6. 役員・審査員構成"),
  ("ch7", "07 CORPORATE", "7. 企業との関係（役員兼任）"), ("ch8", "08 ELIGIBILITY", "8. 応募要項・対象者像"),
  ("ch9", "09 AMOUNT", "9. 助成金額分布"), ("ch10", "10 REPORTS", "10. 分析レポート・データソース"),
]
toc = "".join('<li><a href="#%s"><span class="tn">%s</span>%s</a></li>' % (cid, code.split()[0], title) for cid, code, title in CHAPTERS)

def prefbars(data):
    mx = max((n for _, n in data), default=1)
    return "".join('<div class="pb"><span class="pl">%s</span><span class="pt"><span class="pf" style="width:%d%%"></span></span><span class="pn">%d</span></div>' % (k, int(100 * n / mx), n) for k, n in data)

CSS = """
:root{--bg:#FFF;--card:#FFF;--ink:#121212;--soft:#555;--mute:#8A7868;--accent:#CC1400;--accent2:#0E4F6B;--rule:#E4E0D8;--surf:#FAF8F4}
[data-theme=dark]{--bg:#141210;--card:#1C1A17;--ink:#E8E2D8;--soft:#B8AE9E;--mute:#8A8070;--accent:#FF5A47;--rule:#33302A;--surf:#1F1C18}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--ink);font-family:"Noto Sans JP",sans-serif;line-height:1.85;font-feature-settings:"palt"}
.top{position:sticky;top:0;z-index:50;background:var(--bg);border-bottom:3px solid var(--ink);padding:12px 24px;display:flex;justify-content:space-between;align-items:center}
.brand{font-weight:900;font-size:15px}.brand span{color:var(--accent)}
.tbtn{font-size:12px;padding:5px 12px;border:1px solid var(--rule);background:var(--card);color:var(--ink);cursor:pointer;border-radius:3px}
.layout{display:grid;grid-template-columns:240px 1fr;max-width:1180px;margin:0 auto}
.toc{position:sticky;top:60px;height:calc(100vh - 60px);overflow-y:auto;padding:24px 16px;border-right:1px solid var(--rule)}
.toc-label{font-size:11px;letter-spacing:.2em;color:var(--mute);margin-bottom:12px;font-family:"Fira Code"}
.toc ol{list-style:none}.toc li a{display:block;padding:6px 8px;font-size:12.5px;color:var(--soft);text-decoration:none;border-radius:3px}
.toc li a:hover{background:var(--surf);color:var(--accent)}.tn{font-family:"Fira Code";font-size:10px;color:var(--accent);margin-right:8px}
main{padding:32px 40px;max-width:820px}
.hero{border-bottom:2px solid var(--ink);padding-bottom:20px;margin-bottom:28px}
.tags{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:12px}.tag{font-size:10px;padding:3px 9px;background:var(--surf);color:var(--soft);border-radius:2px}.tag.p{background:var(--accent);color:#fff}
h1{font-size:26px;font-weight:900;line-height:1.4}
.hsub{font-size:13.5px;color:var(--soft);margin-top:8px}
.hmeta{font-size:11.5px;color:var(--mute);margin-top:12px;font-family:"Fira Code";display:flex;gap:16px;flex-wrap:wrap}
section{margin:40px 0;scroll-margin-top:70px}
.chlabel{font-family:"Fira Code";font-size:11px;color:var(--accent);letter-spacing:.15em}
h2{font-size:21px;font-weight:900;margin:2px 0 4px}
.rule{width:48px;height:3px;background:var(--accent);margin:8px 0 16px}
p.lead{font-size:14px;background:var(--surf);border-left:3px solid var(--accent);padding:12px 16px;margin:14px 0;line-height:1.9}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:16px 0}
.stat{background:var(--card);border:1px solid var(--rule);border-top:3px solid var(--accent);padding:14px;text-align:center;border-radius:4px}
.snum{font-size:26px;font-weight:900;color:var(--accent);font-family:"Fira Code"}.slabel{font-size:11px;color:var(--soft);margin-top:4px}
.bar{display:grid;grid-template-columns:150px 1fr 68px;align-items:center;gap:10px;margin:3px 0;font-size:12.5px}
.bl{text-align:right;color:var(--soft);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.bt{background:var(--surf);height:15px;border-radius:2px;overflow:hidden}.bf{display:block;height:100%;background:var(--accent)}.bn{font-family:"Fira Code";font-size:11px;text-align:right}
.pb{display:grid;grid-template-columns:56px 1fr 40px;align-items:center;gap:6px;font-size:11px;margin:1.5px 0}
.pl{text-align:right;color:var(--soft)}.pt{background:var(--surf);height:11px;border-radius:2px;overflow:hidden}.pf{display:block;height:100%;background:var(--accent)}.pn{font-family:"Fira Code";font-size:10px;text-align:right}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:6px 28px}
table{width:100%;border-collapse:collapse;font-size:12px;margin:10px 0}
th{background:var(--ink);color:var(--bg);padding:7px 9px;text-align:left;font-weight:500}
td{padding:6px 9px;border-bottom:1px solid var(--rule)}td.n{font-family:"Fira Code";text-align:right}
h3{font-size:13.5px;font-weight:700;margin:16px 0 6px;color:var(--accent)}
.card{background:var(--card);border:1px solid var(--rule);border-radius:5px;padding:16px 18px;margin:14px 0}
.cta{display:inline-block;padding:10px 18px;background:var(--accent);color:#fff;text-decoration:none;border-radius:4px;font-size:13px;font-weight:700;margin:4px 8px 4px 0}
.cta.o{background:var(--card);color:var(--accent);border:1px solid var(--accent)}
.kv{display:grid;grid-template-columns:230px 1fr;gap:4px 14px;font-size:12px;padding:5px 0;border-bottom:1px solid var(--rule)}.kk{color:var(--soft)}
.foot{margin:40px 0 20px;padding-top:16px;border-top:2px solid var(--accent);font-size:11px;color:var(--mute);display:flex;justify-content:space-between}
@media(max-width:860px){.layout{grid-template-columns:1fr}.toc{display:none}.stats{grid-template-columns:repeat(2,1fr)}.grid2{grid-template-columns:1fr}main{padding:24px 20px}.kv{grid-template-columns:1fr}}
"""

S = []
S.append("<!DOCTYPE html><html lang='ja' data-theme='light'><head><meta charset='UTF-8'><meta name='viewport' content='width=device-width,initial-scale=1'>")
S.append("<title>CFG 研究助成財団DB — 詳細ダッシュボード</title><link rel='icon' href='https://esse-sense.com/favicon.ico'>")
S.append("<link href='https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@300;400;500;700;900&family=Fira+Code&display=swap' rel='stylesheet'>")
S.append("<style>" + CSS + "</style></head><body>")
S.append("<div class='top'><div class='brand'>MIRATUKU / <span>CFG</span> 研究助成財団DB</div>")
S.append("<div><a href='cfg-field-map.html' style='font-size:12px;color:var(--accent);margin-right:14px;text-decoration:none'>領域MAP &rarr;</a>")
S.append("<button class='tbtn' onclick=\"var h=document.documentElement;h.dataset.theme=h.dataset.theme=='dark'?'light':'dark'\">DARK/LIGHT</button></div></div>")
S.append("<div class='layout'><nav class='toc'><div class='toc-label'>CONTENTS</div><ol>" + toc + "</ol></nav><main>")
S.append("<div class='hero'><div class='tags'><span class='tag p'>CFG</span><span class='tag'>研究助成</span><span class='tag'>財団</span><span class='tag'>研究者DB連携</span><span class='tag'>公益法人info</span></div>")
S.append("<h1>Corporate Foundation Grants DB<br>詳細ダッシュボード</h1>")
S.append("<p class='hsub'>日本の研究助成財団を内閣府 公益法人informationに権威接地し、助成先研究者・研究テーマ・審査員・理事評議員・企業関係・応募要項・助成金額まで多次元に把握。</p>")
S.append("<div class='hmeta'><span>更新: 2026-07-16</span><span>団体: %s</span><span>採択者: %s</span><span>役員: %s</span><span>応募資格: %s</span><span>研究者DB 31.1万人接続</span></div></div>" % ("{:,}".format(tot), "{:,}".format(n_award), "{:,}".format(n_off), "{:,}".format(n_elig)))
# ch1
S.append("<section id='ch1'><div class='chlabel'>CHAPTER 01 — OVERVIEW</div><h2>1. データベース全体像</h2><div class='rule'></div>")
S.append("<p class='lead'>当初189団体から出発し、内閣府 公益法人information・JFCの登録情報を統合して<b>%s団体</b>へ拡張。うち<b>%s団体</b>を公式レジストリで権威接地した。研究助成の現状を「団体×領域×金額×研究者×学問分野×大学分布×対象者像×役員×企業関係」で多次元に把握する。</p>" % ("{:,}".format(tot), "{:,}".format(koeki)))
S.append("<div class='stats'><div class='stat'><div class='snum'>%s</div><div class='slabel'>財団・団体</div></div><div class='stat'><div class='snum'>%s</div><div class='slabel'>助成先研究者</div></div><div class='stat'><div class='snum'>%s</div><div class='slabel'>役員・審査員</div></div><div class='stat'><div class='snum'>%s</div><div class='slabel'>応募資格項目</div></div></div>" % ("{:,}".format(tot), "{:,}".format(n_award), "{:,}".format(n_off), "{:,}".format(n_elig)))
S.append("<h3>研究関連度（研究助成戦略の絞り込み軸）</h3>" + bars([(("研究助成候補（high）" if r[0] == "high" else "周辺（medium）" if r[0] == "medium" else "低（low）"), r[1]) for r in relv]) + "</section>")
# ch2
S.append("<section id='ch2'><div class='chlabel'>CHAPTER 02 — TAXONOMY</div><h2>2. 設立者形態・法人形態・所管庁</h2><div class='rule'></div>")
S.append("<p class='lead'>財団を設立者形態・法人形態・所管庁で分類。<b>企業財団843・学術系512・個人記念285</b>。所管庁は<b>国（内閣府）と都道府県</b>に二分され、動向分析の新次元となる。</p>")
S.append("<div class='grid2'><div><h3>設立者形態</h3>" + bars(subtype) + "</div><div><h3>所管庁</h3>" + bars(admin) + "</div></div><h3>法人形態</h3>" + bars([(r[0], r[1]) for r in legal]) + "</section>")
# ch3
S.append("<section id='ch3'><div class='chlabel'>CHAPTER 03 — GEOGRAPHY</div><h2>3. 都道府県分布（47）</h2><div class='rule'></div>")
S.append("<p class='lead'>財団所在地の全国分布。<b>東京・大阪・京都</b>に集中しつつ、47都道府県すべてに研究助成財団が存在する。</p>" + prefbars([(r[0], r[1]) for r in pref]) + "</section>")
# ch4
S.append("<section id='ch4'><div class='chlabel'>CHAPTER 04 — AWARDEES</div><h2>4. 助成先研究者（採択者）</h2><div class='rule'></div>")
S.append("<p class='lead'>研究助成の採択者<b>%s名</b>を収録し、<b>%s名を研究者DB 31.1万人版に接続</b>して学問分野を付与。財団別の採択実績が把握できる。</p>" % ("{:,}".format(n_award), "{:,}".format(n_rid)))
S.append("<h3>採択者数 上位財団</h3><table><tr><th>財団</th><th>採択者数</th></tr>" + "".join("<tr><td>%s</td><td class='n'>%s</td></tr>" % (r[0][:26], "{:,}".format(r[1])) for r in aw_found) + "</table></section>")
# ch5
S.append("<section id='ch5'><div class='chlabel'>CHAPTER 05 — FIELD</div><h2>5. 学問分野分布（研究者DB接続）</h2><div class='rule'></div>")
S.append("<p class='lead'>採択研究者の学問分野を研究者DB由来のAGD 55分野で集計。<b>西洋医学・生物学・生化学</b>が厚く、財団助成は生命・自然科学系に偏る。科研費との詳細照合は領域MAPを参照。</p>")
S.append("<h3>採択研究者 学問分野 上位15（AGD 55分野）</h3>" + bars([(r[0], r[1]) for r in field55]))
S.append("<div class='card'><b>&#9654; 領域MAP（A-E統合分析）</b>で財団&times;科研費照合・3階層領域・大学分布を詳細に見る<br><a class='cta' href='cfg-field-map.html'>研究助成 領域MAP</a></div></section>")
# ch6
S.append("<section id='ch6'><div class='chlabel'>CHAPTER 06 — OFFICERS</div><h2>6. 役員・審査員構成</h2><div class='rule'></div>")
S.append("<p class='lead'>財団の理事・評議員・監事・審査員（選考委員）を<b>%s名</b>収録（うち審査員%s名）。財団のガバナンスと選考体制が把握できる。</p>" % ("{:,}".format(n_off), "{:,}".format(one("SELECT COUNT(*) FROM foundation_officers WHERE role='reviewer'"))))
S.append("<div class='grid2'><div><h3>役職別構成</h3>" + bars([(ROLE.get(r[0], r[0]), r[1]) for r in role]) + "</div><div><h3>役員数 上位財団</h3>" + bars([(r[0][:20], r[1]) for r in off_found]) + "</div></div></section>")
# ch7
S.append("<section id='ch7'><div class='chlabel'>CHAPTER 07 — CORPORATE</div><h2>7. 企業との関係（役員兼任）</h2><div class='rule'></div>")
S.append("<p class='lead'>財団役員のうち<b>現職の会社役員（社長・会長・取締役等）を兼任する%s名</b>を厳格判定（顧問・相談役・名誉職は除外）。財団と企業の人的つながりが可視化される。</p>" % "{:,}".format(n_exec))
S.append("<h3>役員兼任先 企業 上位12</h3>" + bars([(r[0][:22], r[1]) for r in corp]) + "</section>")
# ch8
S.append("<section id='ch8'><div class='chlabel'>CHAPTER 08 — ELIGIBILITY</div><h2>8. 応募要項・対象者像</h2><div class='rule'></div>")
S.append("<p class='lead'>応募資格<b>%s件</b>を8軸（年齢・キャリア段階・職位・所属種別・国籍・分野・性別・その他）で構造化。<b>若手（40歳以下等）targeting・日本国籍中心</b>——「どんな人が対象か」が把握できる。</p>" % "{:,}".format(n_elig))
S.append("<div class='grid2'><div><h3>年齢制限</h3>" + bars(elig["age"]) + "<h3>キャリア段階</h3>" + bars(elig["career_stage"]) + "<h3>国籍要件</h3>" + bars(elig["nationality"]) + "</div><div><h3>対象職位</h3>" + bars(elig["position"]) + "<h3>対象所属種別</h3>" + bars(elig["affiliation_type"]) + "<h3>性別要件</h3>" + bars(elig["gender"] or [("要件明記なしが大半", 0)]) + "</div></div></section>")
# ch9
S.append("<section id='ch9'><div class='chlabel'>CHAPTER 09 — AMOUNT</div><h2>9. 助成金額分布</h2><div class='rule'></div>")
S.append("<p class='lead'>募集要項から<b>1件あたり助成額を%s件収集</b>（中央値<b>%s万円</b>）。<b>100-200万円が最頻</b>で、財団研究助成の標準規模が定量化された。</p>" % ("{:,}".format(namt), "{:,}".format(amt_med // 10000)))
S.append("<h3>1件あたり助成額分布</h3>" + bars(fmap["amount_per_award_program_dist"]) + "<h3>財団 年間助成額規模</h3>" + bars(fmap["amount_foundation_dist"]) + "</section>")
# ch10
S.append("<section id='ch10'><div class='chlabel'>CHAPTER 10 — REPORTS</div><h2>10. 分析レポート・データソース</h2><div class='rule'></div>")
S.append("<p class='lead'>本DBの多次元分析は専用レポートで詳細に提供。全データは内閣府 公益法人information・各財団公式サイト・研究者DBに接地し、生成と検証を分離（独立検証）した。捏造ゼロ・出典URL必須。</p>")
S.append("<a class='cta' href='cfg-field-map.html'>研究助成 領域MAP（A-E統合）</a><a class='cta o' href='cfg-progress-report.html'>精緻化 進捗報告書</a>")
S.append("<h3>データソースと接地</h3>" + kv([("財団基礎・住所・所管庁", "内閣府 公益法人information（権威レジストリ）"), ("採択者・役員・審査員・金額・応募要項", "各財団公式サイト（codex収集・出典URL必須）"), ("学問分野・研究者連携", "研究者DB 31.1万人版（本体∪サブ・AGD 55分野）"), ("科研費照合", "KAKEN 23.5万人（rid_agd_field）"), ("品質保証", "生成と検証の分離（fable独立検証）・捏造ゼロ")]) + "</section>")
S.append("<div class='foot'><span>NPO法人ミラツク ／ esse-sense</span><span>CFG 詳細ダッシュボード ／ 2026.07.16 ／ 全数実測・研究者DB 31.1万人接続</span></div>")
S.append("</main></div></body></html>")
open("report/cfg-dashboard-detailed.html", "w").write("".join(S))
print("saved report/cfg-dashboard-detailed.html (%d KB)" % (len("".join(S)) // 1024))
