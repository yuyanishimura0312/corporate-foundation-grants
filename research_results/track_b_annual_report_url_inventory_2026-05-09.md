# トラックB: JFC上位300財団 年次報告書PDF所在URL収集 調査メモ

調査日: 2026-05-09  
対象: JFC上位100公開リスト、JFC 101-300推定候補、年間助成1億円超の研究助成・企業系財団  
目的: 年間助成額、採択件数、総資産、基本財産を抽出するための公式PDF所在URLを収集する。

## 基準資料

- 助成財団センター「年間助成額上位100財団（2022年度）」: https://www.jfc.or.jp/bunseki-top/rank_grant/rank_grant2022/
- 助成財団センター「年間助成額上位100財団」年度一覧: https://www.jfc.or.jp/bunseki-top/rank_grant/
- 助成財団センター「助成団体要覧2023（電子書籍）」: https://www.jfc.or.jp/handbook2023/

## 既存DB状況

- `organizations`: 2,086団体
- `jfc_rank` 付与済み: 81団体
- `annual_grant_amount` 付与済み: 135団体
- `url` 付与済み: 1,142団体
- 既存抽出済みPDF対象: 三菱財団、稲盛財団、旭硝子財団、上原記念生命科学財団、中谷財団、テルモ生命科学振興財団など

## 公式PDF/情報公開URL 優先収集リスト

| 財団・団体 | JFC順位/根拠 | 公式所在URL | 最新確認資料 | 抽出対象 |
|---|---:|---|---|---|
| 日本財団 | JFC 2022: 1位、65,619百万円 | https://www.nippon-foundation.or.jp/who/disclosure/annual_reports | 2024年度アニュアルレポート: https://www.nippon-foundation.or.jp/wp-content/uploads/2025/06/who_dis_ann_34.pdf | 助成実績、件数、財務諸表 |
| 日本財団 | 同上 | https://www.nippon-foundation.or.jp/who/disclosure/financials | 2024年度事業報告: https://www.nippon-foundation.or.jp/wp-content/uploads/2025/05/2024_jigyohoukoku.pdf | 事業費、資産、財産目録 |
| JKA | JFC 2022: 3位、6,238百万円 | https://www.keirin-autorace.or.jp/hojo/ | 補助事業ページ | 補助金総額、採択事業 |
| 武田科学振興財団 | JFC 2022: 5位、2,708百万円 | https://www.takeda-sci.or.jp/about/archive.php | 2024年度事業報告書 | 助成額、562件、資産 |
| 上原記念生命科学財団 | JFC 2022: 7位、1,463百万円 | https://www.ueharazaidan.or.jp/about/disclosure.html | 2024年度事業報告書 | 335件、助成金総額、資産 |
| 稲盛財団 | JFC 2022: 12位、987百万円 | https://www.inamori-f.or.jp/about/reports | 2024年度事業報告、決算報告書 | 研究助成50件、事業費、資産 |
| 中谷財団 | JFC 2022: 21位、712百万円 | https://www.nakatani-foundation.jp/about/annual_report/ | 年報、事業報告 | 事業費、長期大型研究、資産 |
| 三菱財団 | JFC 2022: 26位、608百万円 | https://www.mitsubishi-zaidan.jp/annual-report/index.html | 2024年度年次報告書 | 助成額、件数、資産 |
| 村田学術振興・教育財団 | JFC 2022: 30位、551百万円 | https://corporate.murata.com/ja-jp/group/zaidan | 2024年度研究助成贈呈式リリース | 217件、5億6,774万円 |
| セコム科学技術振興財団 | JFC 2022: 31位、547百万円 | https://www.secomzaidan.jp/ | 一般研究助成・情報公開探索対象 | 大型研究助成額、件数 |
| 旭硝子財団 | JFC 2022: 38位、499百万円 | https://www.af-info.or.jp/research/index.html | 2024年度新規採択、情報公開PDF | 国内117件、海外39件、資産 |
| 住友財団 | JFC 2022: 47位、409百万円 | https://www.sumitomo.or.jp/ | 活動報告・助成実績探索対象 | 助成額、件数、資産 |
| テルモ生命科学振興財団 | JFC 2022: 53位、371百万円 | https://www.terumozaidan.or.jp/disclosure/ | 2024年度事業報告書、財務諸表 | 事業費、資産 |
| トヨタ財団 | JFC 2022: 54位、367百万円 | https://www.toyotafound.or.jp/service/foundation_publications/ | 2024年度年次報告書 | 助成額、プログラム別件数 |
| トヨタ財団 | 同上 | https://www.toyotafound.or.jp/about/disclosure.html | 2024年度事業計画・収支予算 | 予算・財務補完 |
| 花王芸術・科学財団 | 中型/企業系 | https://www.kao-foundation.or.jp/about/statements/ | 2024年度事業報告書 | 助成額、基本財産48億円 |
| サントリー生命科学財団 | 中型/企業系 | https://www.sunbor.or.jp/about/info/ | 令和6年度事業報告書、決算諸表 | 研究奨励助成、財務 |
| サントリー文化財団 | 中型/企業系 | https://www.suntory.co.jp/news/article/14640.html | 2024年度研究助成決定 | 30件、3,000万円 |
| アステラス病態代謝研究会 | 中型/企業系 | https://www.astellas-foundation.or.jp/about/report.html | 2024年度事業報告書、財務諸表 | 助成額、資産 |
| 鹿島学術振興財団 | 中型/企業系 | https://www.kajima-f.or.jp/reports/ | 2024年度事業報告書、助成実績 | 93件、2億1,585万円 |
| 鹿島学術振興財団 | 同上 | https://www.kajima-f.or.jp/profile/disclosure/ | 決算報告、財産目録 | 総資産、基本財産 |
| 島津科学技術振興財団 | 中型/企業系 | https://www.shimadzu.co.jp/aboutus/ssf/information.html | 2024年度事業報告書、財務諸表 | 助成額、資産 |
| コニカミノルタ科学技術振興財団 | 中型/企業系 | https://www.konicaminoltastf.or.jp/ | 画像科学奨励賞、大学研究助成 | 助成額、件数 |
| 丸紅基金 | JFC DB候補/企業系福祉 | https://www.marubeni.com/jp/news/2024/release/00054.html | 2024年度社会福祉助成決定 | 148件、2億9,749万円 |
| 双日国際交流財団 | 企業系 | https://sojitz-zaidan.or.jp/foundation/information/index.html | 2024年度事業報告書、決算報告書 | 助成額、資産 |
| 伊藤忠記念財団 | 企業系 | https://www.itc-zaidan.or.jp/pdf/about/2024_annualReport.pdf | 2024年度年次報告書 | 助成額、件数、財務 |
| 伊藤記念財団 | 食肉研究助成 | https://www.itokinen-zaidan.or.jp/foundation/outline/accounting/ | 令和6年度事業報告、財務資料 | 研究助成額、資産 |
| JR東日本文化創造財団 | JR系 | https://www.jreast-ci.or.jp/ | 財団公式サイト、JR東日本レポート導線 | 文化事業・助成有無 |
| JR東海文化財団 | JR系 | https://www.jrtf.or.jp/2024/10/2024101-jr-jr.html | 名称変更告知 | 財団情報公開探索 |
| 三井住友信託銀行 公益信託 | 信託系補完 | https://www.smtb.jp/personal/entrustment/public/example | 代表的公益信託・募集案内 | 公益信託別助成額 |

## 2024年度の抽出済み・抽出可能な金額例

| 財団 | 年間助成・事業費等 | 件数 | 根拠 |
|---|---:|---:|---|
| 日本財団 | ボートレース売上金を活用した助成事業 593億6,059万5,358円 | 1,020団体・1,225件 | 2024年度アニュアルレポート |
| 武田科学振興財団 | 23億1,400万円 | 562件 | 2024年度研究助成発表、2024年度事業報告 |
| 三菱財団 | 19億6,928万9,467円 | PDF抽出対象 | 既存 `annual_grant_extraction_results.json` |
| 上原記念生命科学財団 | 13億5,075万円、うち各種助成12億9,075万円 | 335件 | 2024年度受賞者・助成金決定リリース |
| 中谷財団 | 13億8,962万1,480円 | PDF抽出対象 | 既存 `annual_grant_extraction_results.json` |
| 旭硝子財団 | 6億5,170万1,921円 | 国内117件、海外39件 | 既存抽出、2024年度新規採択 |
| テルモ生命科学振興財団 | 5億5,470万1,537円 | PDF抽出対象 | 既存抽出 |
| 村田学術振興・教育財団 | 5億6,774万円 | 217件 | 村田製作所2024年度贈呈式リリース |
| 鹿島学術振興財団 | 2億1,585万円 | 93件 | 鹿島建設2024年度助成金贈呈式リリース |
| 花王芸術・科学財団 | 3,200万円 | 22件 | 花王2024年度助成先リリース |
| サントリー文化財団 | 3,000万円 | 30件 | 2024年度研究助成決定リリース |
| 丸紅基金 | 2億9,749万円 | 148件 | 2024年度社会福祉助成決定リリース |

## 実装上の注意

1. 「年次報告書」と「事業報告書」は財団ごとに用語が違う。検索キーは `年次報告書`, `年報`, `事業報告書`, `情報公開`, `ディスクロージャー`, `決算報告`, `財産目録` を併用する。
2. PDFリンクは年度更新でURLが変わるため、直リンクだけでなく親ページURLもDBに保存する。
3. 年間助成額は `助成金支出`, `助成事業費`, `事業費`, `研究助成事業`, `補助金`, `給付奨学金` など勘定科目が異なる。抽出時は勘定科目名とページ番号を保存する。
4. JFC 101-300位の正式名簿はJFC電子書籍版『助成団体要覧2023』が一次資料。公開Webのみでは「推定順位」として扱う。
5. 企業系財団は親会社ニュースリリースに採択件数・助成総額だけが出る場合がある。資産・基本財産は財団公式の情報公開PDFで補完する。

