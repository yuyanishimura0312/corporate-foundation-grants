# CFG-DB 品質検証レポート

- 生成日時: 2026-05-08 16:23:35
- DB: `/Users/nishimura+/projects/apps/corporate-foundation-grants/corporate_research_grants.sqlite`
- 総合判定: **FAIL**
- 内訳: PASS=27 / WARN=2 / FAIL=3 / INFO=2

## サマリ

| セクション | PASS | WARN | FAIL | INFO |
|---|---:|---:|---:|---:|
| 1. DB整合性 | 14 | 0 | 0 | 0 |
| 2. 重複検出 | 0 | 1 | 2 | 0 |
| 3. データ品質 | 6 | 0 | 0 | 0 |
| 4. 採択者データ品質 | 3 | 1 | 0 | 0 |
| 5. カバレッジ指標 | 2 | 0 | 1 | 1 |
| 6. クロスチェック | 2 | 0 | 0 | 1 |

## 1. DB整合性

| 判定 | 検査項目 | 詳細 |
|---|---|---|
| **PASS** | PRAGMA integrity_check | ok |
| **PASS** | PRAGMA foreign_key_check | 違反なし |
| **PASS** | row_count organizations | 2095件 (期待 ≥1900) |
| **PASS** | row_count grant_programs | 540件 (期待 ≥250) |
| **PASS** | row_count grant_calls | 295件 (期待 ≥280) |
| **PASS** | row_count grant_results | 2122件 (期待 ≥2000) |
| **PASS** | NULL率 organizations.name | 0/2095 = 0.0% (許容 ≤0%) |
| **PASS** | NULL率 organizations.type | 0/2095 = 0.0% (許容 ≤0%) |
| **PASS** | NULL率 organizations.foundation_subtype | 0/2095 = 0.0% (許容 ≤5%) |
| **PASS** | NULL率 organizations.url | 842/2095 = 40.2% (許容 ≤50%) |
| **PASS** | NULL率 organizations.prefecture | 599/2095 = 28.6% (許容 ≤40%) |
| **PASS** | NULL率 grant_results.awardee_name | 0/2122 = 0.0% (許容 ≤1%) |
| **PASS** | NULL率 grant_results.project_title | 0/2122 = 0.0% (許容 ≤5%) |
| **PASS** | NULL率 grant_results.fiscal_year | 0/2122 = 0.0% (許容 ≤0%) |

## 2. 重複検出

| 判定 | 検査項目 | 詳細 |
|---|---|---|
| **FAIL** | 正規化名重複 | 13グループ: 日本郵便株式会社×2; 独立行政法人環境再生保全機構 地球環境基金×2; 独立行政法人国際交流基金×2; 日本NPOセンター×2; 大阪NPOセンター×2 |
| **FAIL** | URL一致・名前差異 | 65件: af-info.or.jp: 公益財団法人旭硝子財団,村田学術振興・教育財団; toyotafound.or.jp: セコム科学技術振興財団,トヨタ財団; fields.canpan.info: マツダ財団,ニッポンハム食の未来財団; mitsubishi-zaidan.jp: 公益財団法人三菱財団,公益財団法人中谷財団; nakatani-foundation.jp: テルモ生命科学振興財団,公益財団法人島津科学技術振興財団 |
| **WARN** | 住所完全一致 | 8住所: 東京都港区芝２丁目４番３号…×2; 大阪府大阪市北区大深町5番54号グラングリーン大阪南館パーク…×3; 東京都中央区日本橋３丁目１４番１０号…×2; 愛知県名古屋市中村区名駅南２丁目１３番４号…×2; 東京都文京区弥生２丁目４番１６号学会センタービル内…×2 (※同一ビル内財団など正規ケースもある) |

## 3. データ品質

| 判定 | 検査項目 | 詳細 |
|---|---|---|
| **PASS** | 電話番号形式 | 13/689 不正形式 |
| **PASS** | メール形式 | 15/656 不正形式 |
| **PASS** | annual_grant_amount 異常値 | 負値=0, 100億円超=0 |
| **PASS** | award_amount 異常値 | 負値=0, 10億円超=0 |
| **PASS** | 都道府県名有効性 | 0/1496 不正値 |
| **PASS** | foundation_subtype 整合性 | 値=['academic', 'corporate', 'govt', 'group', 'individual', 'intl', 'ngo', 'other'] |

## 4. 採択者データ品質

| 判定 | 検査項目 | 詳細 |
|---|---|---|
| **PASS** | awardee_name 空白率 | 0/2122 = 0.00% |
| **WARN** | awardee_affiliation 表記揺れ | 42グループ・追加揺れ 45件 (要正規化) |
| **PASS** | 複数財団からの受領者 | 69名 (参考値 69) |
| **PASS** | fiscal_year 異常値 | 範囲=2022〜2026, 1990未満=0, 2030超=0 |

## 5. カバレッジ指標

| 判定 | 検査項目 | 詳細 |
|---|---|---|
| **FAIL** | JFC top100 マッチ率 | 63/100 = 63% (基準 ≥65%) |
| **INFO** | 法人形態別構成 | 公益財団法人=1283, その他=523, 一般社団法人=134, 一般財団法人=63, 公益社団法人=62, 株式会社=17, 特定非営利活動法人=13 |
| **PASS** | 47都道府県カバレッジ | 47/47都道府県 |
| **PASS** | 設立者形態別分布 | corporate=839(40%), academic=509(24%), individual=257(12%), ngo=197(9%), intl=155(7%), govt=72(3%), other=57(3%), group=9(0%) |

## 6. クロスチェック

| 判定 | 検査項目 | 詳細 |
|---|---|---|
| **INFO** | Grant DB 重複検出 | CFG=2082, Grant=1126, 重複=563件 (27.0%) |
| **PASS** | 親-子プロジェクト整合性 | 親company未参照=0件, parent_company_id付与=105件 |
| **PASS** | 親子テーブル孤立行 | orphan programs=0, orphan calls=0, orphan results=0 |

## 推奨改善アクション

### FAIL (即時対応)
- [2. 重複検出] 正規化名重複: 13グループ: 日本郵便株式会社×2; 独立行政法人環境再生保全機構 地球環境基金×2; 独立行政法人国際交流基金×2; 日本NPOセンター×2; 大阪NPOセンター×2
- [2. 重複検出] URL一致・名前差異: 65件: af-info.or.jp: 公益財団法人旭硝子財団,村田学術振興・教育財団; toyotafound.or.jp: セコム科学技術振興財団,トヨタ財団; fields.canpan.info: マツダ財団,ニッポンハム食の未来財団; mitsubishi-zaidan.jp: 公益財団法人三菱財団,公益財団法人中谷財団; nakatani-foundation.jp: テルモ生命科学振興財団,公益財団法人島津科学技術振興財団
- [5. カバレッジ指標] JFC top100 マッチ率: 63/100 = 63% (基準 ≥65%)

### WARN (次回スプリントで対応)
- [2. 重複検出] 住所完全一致: 8住所: 東京都港区芝２丁目４番３号…×2; 大阪府大阪市北区大深町5番54号グラングリーン大阪南館パーク…×3; 東京都中央区日本橋３丁目１４番１０号…×2; 愛知県名古屋市中村区名駅南２丁目１３番４号…×2; 東京都文京区弥生２丁目４番１６号学会センタービル内…×2 (※同一ビル内財団など正規ケースもある)
- [4. 採択者データ品質] awardee_affiliation 表記揺れ: 42グループ・追加揺れ 45件 (要正規化)
