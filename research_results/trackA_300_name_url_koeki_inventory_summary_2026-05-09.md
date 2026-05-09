# Track A 300団体 正式名称・URL・公益法人info照合サマリ

- 出力CSV: `/Users/nishimura+/projects/apps/corporate-foundation-grants/research_results/trackA_300_name_url_koeki_inventory_2026-05-09.csv`
- 対象: JFC 2022 top81を除く、公益法人info登録済みの財団系組織から300件を抽出
- 公益法人info登録名一致: 300/300
- 公式URLあり: 116/300
- 財務書類年度または既存金額年度あり: 47/300
- 名寄せで畳み込んだ重複行: 0行

## 判定メモ

- `koeki_info_url` は公益法人infoの法人名検索URL。詳細ページIDを未取得の法人も再検索可能にした。
- `financial_document_year=2024` は公式情報公開ページで2024年度事業報告・決算書類が確認できる主要財団、または既存抽出対象レジストリで2024年度を対象にしている法人。
- `2024est` は既存DBの推定値であり、公益法人info財務PDFからの再抽出が必要。
- `verification_status=koeki_name_matched_official_url_needed` は公益法人info登録名は確認済みだが、公式URLの別途照合が必要。
