# Phase8 Track A: URL欠損944団体のWikipedia記事有無分類 調査メモ

調査日: 2026-05-09  
対象DB: `corporate_research_grants.sqlite` / `organizations`

## 1. 母集団

`organizations.url IS NULL OR url = ''` でURL欠損は944団体。

| 法人格 | 全団体数 | URL欠損 |
|---|---:|---:|
| 公益財団法人 | 1,283 | 805 |
| その他 | 515 | 113 |
| 一般社団法人 | 134 | 12 |
| 一般財団法人 | 63 | 9 |
| 公益社団法人 | 62 | 2 |
| 特定非営利活動法人 | 13 | 2 |
| 株式会社 | 16 | 1 |

## 2. 記事存在・URL抽出確認済み

| DB名 | Wikipedia記事 | 公式URL | 主要プログラム/事業 | 所在地 | 設立年 | 親企業/設立者 | 判定 |
|---|---|---|---|---|---:|---|---|
| 公益財団法人石橋財団 | https://ja.wikipedia.org/wiki/石橋財団 | https://www.ishibashi-foundation.or.jp/ | 美術館運営、美術・教育助成 | 東京都中央区 | 1956 | 石橋正二郎 / ブリヂストン | 記事存在 |
| 社団法人国土緑化推進機構 | https://ja.wikipedia.org/wiki/国土緑化推進機構 | https://www.green.or.jp/ | 国土緑化、森林・緑化運動 | 東京都千代田区平河町 | 2011 | 森林愛護連盟等を起源 | 記事存在 |
| 企業メセナ協議会 | https://ja.wikipedia.org/wiki/企業メセナ協議会 | https://www.mecenat.or.jp/ | 芸術文化支援、メセナ活動支援 | 東京都港区芝 | 1990 | 企業メセナ中間支援 | 記事存在 |
| KDDI財団 | https://ja.wikipedia.org/wiki/KDDI財団 | https://www.kddi-foundation.or.jp/ | ICT普及、国際協力、調査研究等 | 東京都千代田区飯田橋 | 1974 | KDDI系、KEC/ICFを起源 | 記事存在 |
| 電気通信普及財団 | https://ja.wikipedia.org/wiki/電気通信普及財団 | http://www.taf.or.jp/ | 研究調査助成、海外研究援助、財団賞 | 東京都港区西新橋 | 1984 | NTT出捐 | 記事存在 |

補足: 三菱財団は英語版 Wikipedia 記事が検索で確認でき、外部リンクに `http://www.mitsubishi-zaidan.or.jp/` が掲載されている。ただしTrack Aの主対象を日本語版 `ja.wikipedia.org` とするなら、英語版は「補助ソース」として扱うのが妥当。

## 3. 検索未検出・記事不存在候補

以下はURL欠損上位からのスポット確認で、`site:ja.wikipedia.org/wiki/ "<団体名>"` では記事が検出されなかった候補。Wikipedia経由ではなく公式検索・JFC・公益法人information等へ回す。

| DB名 | 年間助成額/JFC順位 | 判定 |
|---|---:|---|
| 公益財団法人上原記念生命科学財団 | 1,450,619,759 / 7 | 記事未検出 |
| 公益財団法人トヨタ・モビリティ基金 | 716,000,000 / 20 | 記事未検出 |
| 村田学術振興・教育財団 | 551,000,000 / 30 | 記事未検出 |
| セコム科学技術振興財団 | 547,000,000 / 31 | 記事未検出 |
| 公益財団法人博報堂教育財団 | 352,000,000 / 58 | 記事未検出 |
| 公益財団法人天田財団 | 273,000,000 / 71 | 記事未検出 |
| 公益財団法人島津科学技術振興財団 | 254,999,999 / - | 記事未検出 |
| 公益財団法人本庄国際奨学財団 | 202,000,000 / 88 | 記事未検出 |

## 4. 全944件分類の判定ルール

推奨ステータス:

- `wiki_article_exists`: 日本語版Wikipediaに独立記事があり、財団/団体本文またはinfoboxに一致する。
- `wiki_article_exists_en_only`: 英語版等には記事があるが、日本語版には未検出。
- `wiki_article_redirect_or_related`: 親企業・美術館・賞など関連記事のみ。公式URL抽出は可能だが財団記事ではない。
- `wiki_article_not_found`: OpenSearch/Google site検索で完全一致・略称一致とも未検出。
- `needs_manual_review`: 同名団体、法人名変更、リダイレクト候補があり機械判定不可。

照合キー:

1. 法人格を除いた正規化名: `公益財団法人石橋財団` -> `石橋財団`
2. 旧法人名・略称: `社団法人国土緑化推進機構` -> `国土緑化推進機構`
3. 親企業/設立者名との混同排除: `石橋財団` と `アーティゾン美術館`、`島津科学技術振興財団` と `島津製作所` など。

URL採用優先順位:

1. Infoboxの `ウェブサイト`
2. 外部リンク節の公式サイト
3. Wikidata P856 official website
4. Wikipedia本文内の公式URL

## 5. 参考文献・技術資料

- MediaWiki, "API:Opensearch", Wikimedia Foundation. Wikipedia記事名検索に利用。URL: https://www.mediawiki.org/wiki/API:Opensearch
- MediaWiki, "API:Extlinks", Wikimedia Foundation. 記事内外部リンク抽出に利用。URL: https://www.mediawiki.org/wiki/API:Extlinks
- Denny Vrandečić and Markus Krötzsch, "Wikidata: A Free Collaborative Knowledge Base", Communications of the ACM, 57(10), 2014. 機関: Google Inc.; TU Dresden. URL: https://research.google/pubs/wikidata-a-free-collaborative-knowledge-base/
- Jens Lehmann, Robert Isele, Max Jakob, Anja Jentzsch, Dimitris Kontokostas, Pablo N. Mendes, Sebastian Hellmann, Mohamed Morsey, Patrick van Kleef, Sören Auer, Christian Bizer, "DBpedia – A large-scale, multilingual knowledge base extracted from Wikipedia", Semantic Web, 6(2), 2015. 機関: University of Leipzig AKSW, HPI, University of Mannheim, Wright State University, Fraunhofer IAIS等. URL: https://journals.sagepub.com/doi/10.3233/SW-140134
- Fei Wu and Daniel S. Weld, "Autonomously Semantifying Wikipedia", CIKM 2007. 機関: University of Washington. URL: https://doi.org/10.1145/1321440.1321449

## 6. 実行上の制約

この実行環境のシェルからは `ja.wikipedia.org` のDNS解決ができなかったため、944件のMediaWiki API一括実行は未完了。Web検索・直接閲覧で確認できた候補を上表に記録した。全件分類は、ネットワーク到達可能な環境でMediaWiki OpenSearch + Extlinks + Wikidata P856照合を実行する。
