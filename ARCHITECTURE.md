# Corporate Foundation Grants DB — System Architecture

**版**: v1.0
**策定日**: 2026-05-08
**設計者**: System Architect (Claude)
**対象**: 研究助成財団DBの大規模拡張（189団体/258プログラム → 500-1000団体規模）
**準拠ルール**: `~/.claude/rules/db-design-system.md`（textbook.html構造 + 赤白CI #CC1400）

---

## 1. 設計目的とスコープ

### 1.1 現状（baseline）
- DB: `corporate_research_grants.sqlite`（SQLite、約1MB）
- 規模: 189団体 / 258プログラム / 親プロジェクト Grant DB（3,669件）からのキーワード抽出派生
- 収集系: `collectors/` に7スクリプト（JFC/koeki/annual_reports）
- 公開: ローカル `index.html` のみ。miratuku-news-v2 統合ダッシュボード未登録

### 1.2 目的
1. **規模**: 財団 500-1,000団体に拡張（国内主要財団＋国際財団）
2. **構造化**: 財団種別タクソノミーを導入し、財団間比較・親企業横断分析を可能にする
3. **歴史性**: 過去公募履歴・採択結果（awardees）を時系列で蓄積
4. **国際化**: 米欧アジアの主要研究助成財団を含める
5. **公開**: `~/projects/apps/miratuku-news-v2/dashboards/corporate-foundation-grants.html` として公開（赤白CI準拠）+ `databases.html` インデックスへの登録

### 1.3 非目的（Out of Scope）
- 申請書類の自動生成（Grant DB / KAKENHI Writer 側で扱う）
- リアルタイムAPI公開（バッチDB公開のみ）
- 個別研究者プロファイル管理

---

## 2. アーキテクチャ概要

### 2.1 4層モデル

```
[Layer 1: Sources]      [Layer 2: Collectors]    [Layer 3: Pipeline]      [Layer 4: Publication]
─────────────────       ──────────────────       ──────────────────       ──────────────────────
JFC（助成財団C）       collectors/jfc/          1. fetch (HTTP cache)    miratuku-news-v2/
koeki info net    →    collectors/koeki/    →   2. parse (raw_*)     →   dashboards/
foundation HP/PDF       collectors/national/     3. normalize             corporate-foundation-
EDINET/有報             collectors/intl/         4. dedupe (fingerprint)  grants.html (+report)
NSF/NIH/Wellcome        collectors/awardees/     5. quality gate          databases.html (登録)
US 990 (ProPublica)     collectors/annual_       6. upsert
                          reports/               7. relate (FK建設)
```

各層の責務を分離し、Layer 2の追加（新ソース）が Layer 3/4 に波及しないことを保証する。

### 2.2 主要原則

| 原則 | 内容 |
|---|---|
| **Source-of-truth保持** | 生データ（HTML/PDF/JSON）は `data/raw/{source}/{yyyy-mm-dd}/` に必ず残す。再正規化を可能にする |
| **冪等性** | 全collectorは何度走っても同じ結果（`run_id` + content hash でskip） |
| **分離された段階** | raw → staging → main の3段階。stagingは検証されてからmainへmerge |
| **Fingerprintによる重複検出** | foundation: name normalized + corporate_parent + url の正準化ハッシュ |
| **国際/国内の同居** | `country_code`（ISO 3166）+ `currency_code` で多通貨対応 |
| **タクソノミーの第一級** | 財団種別を別テーブルに分離し、複数タグ付与可能（many-to-many） |

---

## 3. ディレクトリ構成案

```
~/projects/apps/corporate-foundation-grants/
├── ARCHITECTURE.md                    ← 本書
├── README.md
├── PROJECT_PLAN.md                    ← 新規（フェーズ別タスク）
├── STATUS.md                          ← 新規（最終更新・スナップショット）
├── corporate_research_grants.sqlite   ← main DB
├── corporate_research_grants.staging.sqlite  ← staging DB（QC前）
│
├── schema/
│   ├── 001_core.sql                  ← organizations / programs / calls
│   ├── 002_taxonomy.sql              ← foundation_types / foundation_tags
│   ├── 003_awardees.sql              ← awardees / awarded_grants
│   ├── 004_intl.sql                  ← intl-extension（country/currency）
│   ├── 005_provenance.sql            ← source_runs / source_records
│   └── views.sql                     ← v_grants_overview ほか
│
├── collectors/
│   ├── _common/
│   │   ├── http_client.py            ← retry/cache/rate-limit/UA
│   │   ├── fingerprint.py            ← name正規化＋hash
│   │   ├── pdf_extract.py            ← pdfminer wrapper
│   │   └── upsert.py                 ← staging→main upsert共通化
│   │
│   ├── national/
│   │   └── foundations/
│   │       ├── jfc/                  ← 助成財団センター（既存scrape_jfc_v2.py移管）
│   │       ├── koeki/                ← 公益情報net（既存collect_koeki_info移管）
│   │       ├── canpan/               ← 新規（CANPAN財団情報）
│   │       ├── meti/                 ← 新規（経産省・関連財団）
│   │       └── annual_reports/       ← 各財団HP/年次報告（既存scrape_annual_reports移管）
│   │
│   ├── intl/
│   │   └── foundations/
│   │       ├── propublica_990/       ← 米国 IRS 990 (ProPublica Nonprofit Explorer)
│   │       ├── nsf/                  ← NSF Awards API
│   │       ├── nih/                  ← NIH RePORTER API
│   │       ├── wellcome/             ← Wellcome Trust grants
│   │       ├── eu_cordis/            ← Horizon Europe / CORDIS
│   │       └── _registry.yaml        ← 各国主要財団リスト
│   │
│   └── awardees/
│       ├── from_annual_reports/      ← 採択者抽出（年次報告書PDFから）
│       ├── from_foundation_pages/    ← 財団HP「過去採択者」セクション
│       └── kaken_link/               ← KAKEN(科研費)との照合
│
├── pipeline/
│   ├── 01_fetch.py                   ← collector dispatcher
│   ├── 02_parse.py                   ← raw → staging
│   ├── 03_normalize.py               ← name/amount/date正規化
│   ├── 04_dedupe.py                  ← fingerprint突合
│   ├── 05_quality_gate.py            ← QC（後述§5）
│   ├── 06_upsert.py                  ← staging → main
│   ├── 07_relate.py                  ← parent-company FK建設
│   └── run_all.py                    ← オーケストレータ
│
├── taxonomy/
│   ├── foundation_types.yaml         ← 財団種別定義（後述§4.2）
│   ├── parent_company_map.yaml       ← 財団→親企業マッピング
│   ├── research_field_map.yaml       ← 助成分野（NDC/JST分類との対応）
│   └── country_currency.yaml
│
├── data/
│   ├── raw/                          ← {source}/{yyyy-mm-dd}/* (gitignore)
│   ├── staging/                      ← 中間JSON
│   └── snapshots/                    ← 月次CSV/JSON dump
│
├── reports/
│   ├── coverage_report.py            ← 定期カバレッジ計測
│   └── quality_report.py
│
├── dashboard/
│   ├── generate_dashboard.py         ← 赤白CIテンプレート使用
│   ├── generate_report.py            ← 公開用レポート（赤白CI）
│   ├── _template.html                ← _template-akashiro.htmlコピー
│   └── deploy.sh                     ← miratuku-news-v2/dashboards/ へのコピー
│
├── tests/
│   ├── test_fingerprint.py
│   ├── test_normalize.py
│   └── fixtures/
│
└── build_db.py                       ← 既存（互換維持・段階的にpipelineへ移行）
```

### 3.1 collectors階層の設計判断

- `national/foundations/` 配下に`jfc/`/`koeki/`/`canpan/`/`meti/`/`annual_reports/`を配置: 国内集約系インデックス（JFC, koeki）と個別財団HP（annual_reports）を兄弟として並置することで、後者からのデータが前者の不足を補完する関係を構造化する
- `intl/foundations/` は国別ではなく**ソース別**: 米国は ProPublica 990 で多数の財団を一括取得できるため、国軸より「ソースAPI軸」が実装上効率的
- `awardees/` は独立: 採択者データは collector 側ではなく結果側のため別ディレクトリに分離。財団HP/年次報告/KAKEN照合の3経路から集約

---

## 4. データモデル

### 4.1 コアスキーマ（既存 + 拡張）

```sql
-- 既存テーブルを拡張
organizations (
  id INTEGER PK,
  name TEXT,
  name_normalized TEXT,            -- 新規: 正規化名（重複検出キー）
  name_en TEXT,                    -- 新規: 英語名
  country_code TEXT DEFAULT 'JP',  -- 新規: ISO 3166
  corporate_parent TEXT,           -- 既存
  corporate_parent_id INTEGER,     -- 新規: 企業マスタへのFK（将来）
  founded_year INTEGER,            -- 新規
  endowment_jpy INTEGER,           -- 新規: 基本財産（円換算）
  url TEXT,
  url_en TEXT,                     -- 新規
  fingerprint TEXT UNIQUE,         -- 新規: 重複検出ハッシュ
  source_run_id INTEGER,           -- 新規: 最終更新run
  type TEXT                        -- 既存（foundation/government/...）
);

grant_programs (
  id INTEGER PK,
  organization_id INTEGER FK,
  name TEXT,
  name_en TEXT,                    -- 新規
  description TEXT,
  research_field TEXT,             -- 既存
  research_field_codes TEXT,       -- 新規: NDC/JST分類のJSON配列
  established_year INTEGER,        -- 新規
  status TEXT                      -- active/discontinued
);

grant_calls (
  id INTEGER PK,
  program_id INTEGER FK,
  fiscal_year INTEGER,             -- 新規: 公募年度
  amount_min INTEGER, amount_max INTEGER,
  currency_code TEXT DEFAULT 'JPY',-- 新規
  application_deadline DATE,
  status TEXT                      -- open/upcoming/closed/historical
);
```

### 4.2 タクソノミー（新規）

```sql
foundation_types (
  code TEXT PK,                    -- 例: corp_single / corp_group / industry / academic / public
  name_ja TEXT, name_en TEXT,
  description TEXT
);

foundation_tags (                  -- many-to-many
  organization_id INTEGER FK,
  type_code TEXT FK,
  confidence REAL,                 -- 0.0-1.0（自動分類の信頼度）
  source TEXT                      -- manual / heuristic / llm
);
```

`taxonomy/foundation_types.yaml` 初期定義案:

```yaml
- code: corp_single        # 単一企業設立財団（トヨタ財団等）
- code: corp_group         # 企業グループ財団（三菱財団等）
- code: industry           # 業界団体由来（日工組・JKA等）
- code: corp_alumni        # 創業者個人遺産系（稲盛財団・本田財団等）
- code: bank_insurance     # 金融・保険系
- code: family_office      # ファミリーオフィス系（米欧）
- code: pharma_industry    # 製薬業界系
- code: tech_industry      # IT・電機系
- code: academic_partnered # 大学連携型
- code: public_corporate   # 公的＋企業共同（NEDO等）
- code: international      # 国際財団（Wellcome / Gates 等）
```

### 4.3 採択結果（新規）

```sql
awardees (
  id INTEGER PK,
  call_id INTEGER FK,
  recipient_name TEXT,
  recipient_affiliation TEXT,
  recipient_kaken_id TEXT,         -- KAKEN照合結果
  project_title TEXT,
  amount INTEGER, currency_code TEXT,
  awarded_date DATE,
  source_doc_id INTEGER FK         -- どの年次報告書から抽出したか
);
```

### 4.4 来歴（provenance）

```sql
source_runs (
  id INTEGER PK, run_at TIMESTAMP,
  collector TEXT, source_url TEXT,
  records_fetched INTEGER, records_upserted INTEGER,
  status TEXT  -- success / partial / failed
);

source_records (
  id INTEGER PK, run_id INTEGER FK,
  raw_path TEXT,                   -- data/raw/... への相対パス
  content_hash TEXT,
  target_table TEXT, target_id INTEGER
);
```

---

## 5. 品質ゲート（取り込み前検証）

`pipeline/05_quality_gate.py` で以下を実行。Stagingからmainへのupsertは**全項目クリア時のみ**。

### 5.1 構造的チェック（HARD失敗）
1. **必須フィールド充足**: organizationは `name`+`country_code`、callは `program_id`+`fiscal_year`
2. **型整合**: 金額・年度・日付が型に合致
3. **FK整合**: `program_id` が `grant_programs` に存在
4. **fingerprint生成可能**: 名前正規化が空でない

### 5.2 セマンティックチェック（HARD失敗）
5. **金額レンジ**: amount > 0、`amount_max >= amount_min`、上限10億円（外れ値検知）
6. **日付妥当性**: `application_deadline` が `fiscal_year` ±2年以内
7. **重複検出**: fingerprint突合で既存orgとの重複が0.9以上ならmerge候補としてフラグ
8. **言語**: `name`が日本語ORアルファベットのみ（mojibake検出）

### 5.3 カバレッジチェック（SOFT警告）
9. **空欄率**: `description`欠損 < 30%、`url`欠損 < 20%
10. **所属関係**: `country_code='JP'`の財団は `corporate_parent` または `type` 必須

### 5.4 履歴整合性（SOFT警告）
11. **削除検出**: 前回run時に存在し今回欠損 → 廃止候補としてフラグ（即削除しない）
12. **金額激変**: 前年比 ±50% 超 → 注記

### 5.5 出力
- `quality_gate_report_{run_id}.json`: 各チェックのpass/fail件数
- HARD失敗レコードはstagingに留め、mainへ反映しない
- 月次サマリーを`reports/quality_report.py`が集計

---

## 6. 公開ダッシュボード設計

### 6.1 公開先と命名
- **メインダッシュボード**: `~/projects/apps/miratuku-news-v2/dashboards/corporate-foundation-grants.html`
- **解析レポート**: `~/projects/apps/miratuku-news-v2/dashboards/corporate-foundation-grants-report.html`
- **DB ID**: `CFG`（Corporate Foundation Grants）

### 6.2 構造（textbook.html準拠 + 赤白CI #CC1400）

`~/projects/apps/miratuku-news-v2/dashboards/_template-akashiro.html` をベースに、以下の章構成で生成:

```
1. 概観（KPI: 財団数 / プログラム数 / 公募数 / 累計助成額）
2. 財団タクソノミー（11種別の分布・代表例）
3. 親企業ネットワーク（業界×財団数マトリクス）
4. 助成プログラム一覧（フィルタ: 国/種別/分野/金額帯）
5. 公募カレンダー（年度別 timeline）
6. 採択者分析（受給機関ランキング・分野推移）
7. 国際比較（JP / US / UK / EU / Asia）
8. データソース来歴（source_runs サマリー）
9. クエリと使い方（SQLite直接アクセス例）
```

### 6.3 必須要素（db-design-system.md準拠）
- top-bar 48px固定、border-top 3px solid #121212、ブランド「miratuku-news / CFG」、ダーク切替
- toc-sidebar 240px（章番号付き、ホバーで赤色）
- main max-width 760px、`text-indent: 1em` 段落スタイル
- Noto Sans JP（UI）+ Noto Serif JP（本文）
- favicon: `https://esse-sense.com/favicon.ico`
- ダークモード対応（`[data-theme="dark"]`）
- モバイル `<1000px`: サイドバー上部展開
- 印刷用 `@media print`: サイドバー非表示

### 6.4 NG事項（再掲）
- 絵文字・アイコンフォント未使用
- 青/緑/紫を主役色にしない（赤白CI #CC1400 のみ）
- card内背景色多用しない（白基調維持）

### 6.5 databases.html への登録

`~/projects/apps/miratuku-news-v2/data/db-registry.json` に追記:

```json
{
  "id": "CFG",
  "name": "Corporate Foundation Grants DB",
  "nameJa": "企業財団研究助成DB",
  "stat": "{N}団体 / {M}プログラム / {K}公募",
  "description": "国内外の企業財団による研究助成を集約したDB。財団種別タクソノミー11カテゴリ・親企業マッピング・過去採択者・国際財団を統合し、研究資金獲得戦略の意思決定を支援する。",
  "repo": "corporate-foundation-grants",
  "dbId": "corporate_research_grants",
  "tables": 14,
  "rows": 0,
  "storage": "SQLite",
  "update": "月次",
  "dashboard": "dashboards/corporate-foundation-grants.html",
  "agent": null
}
```

配置先 layer は **`structural`**（基軸ではないが構造理解レイヤー）または、新たに **「事業推進・実務支援」レイヤー** が望ましい（Grant DB / KAKENHI Writer と同列）。`databases.html` は `db-registry.json` を動的レンダリングしているため、JSONへの追記のみで反映される。

---

## 7. 段階的実装ロードマップ

各フェーズは「即実行可能タスク → 完了基準」を持つ。フェーズ間の依存関係を明示。

### Phase 0: 設計凍結とリポジトリ準備（1-2日）

| # | タスク | 完了基準 |
|---|---|---|
| 0.1 | ARCHITECTURE.md（本書）レビュー・確定 | 西村承認 |
| 0.2 | `PROJECT_PLAN.md` / `STATUS.md` 作成 | ファイル存在 |
| 0.3 | ディレクトリ骨格（schema/, pipeline/, taxonomy/, dashboard/, tests/）作成 | `tree -L 2` で確認 |
| 0.4 | `_template-akashiro.html` を `dashboard/_template.html` にコピー | ファイル存在 |
| 0.5 | `.gitignore` に `data/raw/`, `*.staging.sqlite`, `*.sqlite-shm`, `*.sqlite-wal` 追加 | コミット |

### Phase 1: 既存資産のリファクタ（3-5日）

| # | タスク | 完了基準 |
|---|---|---|
| 1.1 | 既存 `collectors/scrape_jfc_v2.py` → `collectors/national/foundations/jfc/` 移管 | importパス変更・既存出力と一致 |
| 1.2 | 同 `scrape_koeki.py` → `collectors/national/foundations/koeki/` | 同上 |
| 1.3 | 同 `scrape_annual_reports.py` → `collectors/national/foundations/annual_reports/` | 同上 |
| 1.4 | `collectors/_common/` に http_client / fingerprint / pdf_extract を実装 | 単体テスト合格 |
| 1.5 | `schema/001_core.sql` 〜 `005_provenance.sql` 作成、ALTER TABLEで既存DBに段階適用 | スキーマ移行成功 |
| 1.6 | `pipeline/06_upsert.py`（staging→main）実装 | 既存189団体を再構築できる |

### Phase 2: タクソノミー導入（2-3日）

| # | タスク | 完了基準 |
|---|---|---|
| 2.1 | `taxonomy/foundation_types.yaml` 11種別の定義確定 | yaml lint pass |
| 2.2 | `foundation_types` / `foundation_tags` テーブル作成 + 初期投入 | 11行insert |
| 2.3 | 既存189団体への自動タグ付け（既存`CORPORATE_KEYWORDS`を流用） | カバー率 > 95% |
| 2.4 | 残5%を手動タグ付け | 100%タグ付与 |
| 2.5 | `taxonomy/parent_company_map.yaml` 整備（既存`corporate_parent`から抽出） | 親企業ユニーク数集計 |

### Phase 3: 国内拡張（5-7日）

| # | タスク | 完了基準 |
|---|---|---|
| 3.1 | JFCからの全件再スクレイプ（189 → 目標400） | 実件数記録 |
| 3.2 | CANPAN collector 新規実装 | 50団体以上取得 |
| 3.3 | METI / 各省関連財団リスト整備 | 30団体以上 |
| 3.4 | 各財団HP `annual_reports` 巡回拡張（応募要項PDF/採択結果） | 100報告書以上 |
| 3.5 | Quality Gate を初回フル実行・調整 | HARD失敗 < 5% |
| 3.6 | **国内500団体達成** | DB件数確認 |

### Phase 4: 採択結果収集（5-7日）

| # | タスク | 完了基準 |
|---|---|---|
| 4.1 | `awardees` テーブル + ETLスクリプト | スキーマ完成 |
| 4.2 | 年次報告書PDFからの採択者抽出（pdfminer + LLM補助） | 主要50財団で抽出成功 |
| 4.3 | KAKEN ID 照合（部分一致 + LLM判定） | 30%以上の名寄せ |
| 4.4 | 過去5年分の採択結果蓄積 | レコード数 >5,000 |

### Phase 5: 国際拡張（7-10日）

| # | タスク | 完了基準 |
|---|---|---|
| 5.1 | スキーマの`country_code` / `currency_code`本格運用 | 既存にJP一括付与 |
| 5.2 | ProPublica 990 collector（米国財団） | 100団体以上 |
| 5.3 | NSF Awards / NIH RePORTER collector | プログラム情報取得 |
| 5.4 | Wellcome / EU CORDIS / Gates Foundation等の主要国際財団 | 50団体以上 |
| 5.5 | **総計800-1,000団体達成** | DB件数確認 |

### Phase 6: ダッシュボード公開（3-5日）

| # | タスク | 完了基準 |
|---|---|---|
| 6.1 | `dashboard/generate_dashboard.py` 実装（赤白CI準拠） | HTML生成成功 |
| 6.2 | `dashboard/generate_report.py` 実装 | レポートHTML生成 |
| 6.3 | 9章構成での描画（§6.2参照）+ ダーク/印刷/モバイル対応確認 | 全要素チェック合格 |
| 6.4 | `db-registry.json` への CFG エントリ追記 | databases.htmlで描画 |
| 6.5 | `dashboard/deploy.sh` で `miratuku-news-v2/dashboards/` へコピー | 公開URL確認 |
| 6.6 | miratuku-news-v2 のリポジトリで commit + push | GitHub Pages反映 |

### Phase 7: 運用化（2-3日）

| # | タスク | 完了基準 |
|---|---|---|
| 7.1 | `pipeline/run_all.py` を月次cronまたは手動実行に整備 | dry-run成功 |
| 7.2 | カバレッジレポート (`reports/coverage_report.py`) 自動化 | 月次出力 |
| 7.3 | Notionアプリ一覧への登録 | ページ作成 |
| 7.4 | README / STATUS.md の最終更新 | 完了 |

### 7.x 即実行可能タスク（優先順）

最初の10タスク（Phase 0-1先頭）を以下の順で実行すれば、設計を「動く骨格」にできる:

1. `ARCHITECTURE.md` 確定 ← 本タスク
2. `PROJECT_PLAN.md` / `STATUS.md` テンプレ作成
3. 新ディレクトリ骨格作成（`mkdir -p schema pipeline taxonomy dashboard tests collectors/_common collectors/national/foundations/{jfc,koeki,canpan,meti,annual_reports} collectors/intl/foundations collectors/awardees`）
4. `dashboard/_template.html` を `_template-akashiro.html` から複写
5. `taxonomy/foundation_types.yaml` 11種別の定義 commit
6. `schema/001_core.sql` 〜 `005_provenance.sql` 作成
7. 既存collector の `national/foundations/` への移動（git mv で履歴保持）
8. `collectors/_common/{http_client,fingerprint,upsert}.py` を最小実装
9. `pipeline/06_upsert.py` が既存189団体を再投入できるところまで動作確認
10. `db-registry.json` に CFG エントリを「準備中」ステータスで先行登録

---

## 8. 主要な設計判断と却下した代案

| 判断 | 採用 | 却下 | 理由 |
|---|---|---|---|
| DB分離 | corporate-foundation-grants単独 | grant-dbへ吸収 | 親プロジェクトは「全公募」を扱い、本プロジェクトは「企業財団に絞った構造化」が目的。スキーマ要件が異なる |
| 重複検出キー | name_normalized + parent + url のhash | 名前完全一致のみ | 表記ゆれ（カナ/漢字/略称）多数。fingerprintで吸収 |
| 国際財団のスキーマ | 同一テーブルに `country_code`/`currency_code` | 別テーブル | 比較分析を容易にするため共通スキーマ。currency_code で換算は外で実施 |
| タクソノミー | 別テーブル + many-to-many | 単一カラム | 1財団が複数種別に該当する（例: corp_single + pharma_industry）ため |
| パイプライン段階 | 7段階（fetch→relate） | 単一スクリプト | 段階分離で再実行性・テスト容易性を確保 |
| 公開先 | miratuku-news-v2/dashboards/ 統合 | 独立Vercel公開 | 全DB成果物の一元化方針（赤白CI/db-registry）に従う |
| 採択者の名寄せ | KAKEN優先＋LLM補助 | 人手のみ | 数万件規模で人手は不可。70%自動・残り手動が現実的 |

---

## 9. リスクと緩和策

| リスク | 影響 | 緩和策 |
|---|---|---|
| スクレイピングのrobots.txt違反 | 法的リスク | `_common/http_client.py` に robots尊重・rate-limit・User-Agent明示を実装 |
| 財団HPの構造変更で大量失敗 | 月次更新停止 | collector毎に「正常時件数」を記録し、50%以上の急減で警告 |
| PDFから採択者抽出の精度不足 | データ品質低下 | LLM補助 + 人手レビューサイクル。confidenceフィールドで分離 |
| 国際財団のscope drift | 大規模化しすぎ | 「研究助成のみ・年間予算1M USD以上」等の組入れ基準を明記 |
| 親プロジェクト Grant DB との二重管理 | 不整合 | source_run_id で「Grant DB由来」を明示。月次でdiffレポート |

---

## 10. 完成時の到達点

- 800-1,000財団・3,000+ プログラム・10,000+ 採択結果を構造化保有
- 11カテゴリのタクソノミーで横断分析可能
- 赤白CI準拠の公開ダッシュボード（textbook.html構造）
- `databases.html` のDB一覧に CFG として登録
- 月次cronで自動更新、品質ゲートで取り込み品質を保証
- KAKEN / Grant DB / KAKENHI Writer との連携基盤

---

**附録A**: 主要な命名規則（fingerprint等）は `collectors/_common/fingerprint.py` の docstring を一次ソースとする。
**附録B**: 赤白CIの完全仕様は `~/.claude/rules/db-design-system.md` を参照。
