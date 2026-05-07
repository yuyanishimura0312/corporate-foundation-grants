-- ============================================================================
-- Migration 002: Taxonomy Expansion
-- Target DB: corporate_research_grants.sqlite
-- Purpose:   財団分類の細分化、過去採択結果テーブル、分野タクソノミー正規化
-- Date:      2026-05-08
-- Author:    db-specialist (NPO法人ミラツク)
--
-- Strategy:
--   1) 既存organizationsを破壊せずに ALTER TABLE で列追加（後方互換）
--   2) 新規マスタ/履歴/分類テーブルを CREATE TABLE IF NOT EXISTS で追加
--   3) 名称ベースの推測ルールでバックフィル UPDATE を実行
--   4) 既存ビュー(v_grants_overview等)はカラム削除なしのため影響なし
--   5) 拡張ビュー(v_foundation_taxonomy / v_coverage_progress)を追加
--
-- Roll-forward only. ロールバックする場合は 003_revert_taxonomy.sql を別途用意。
-- ============================================================================

-- NOTE: foreign_keys を OFF にして実行する。
-- ALTER TABLE で REFERENCES を含む列を追加する都合上、参照先テーブルが
-- 同一トランザクション内で後から CREATE される箇所があるため、
-- マイグレーション中は FK 検査を無効化する。完了後に再有効化する。
PRAGMA foreign_keys = OFF;
BEGIN TRANSACTION;

-- ----------------------------------------------------------------------------
-- 0. メタテーブル: マイグレーション履歴
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schema_migrations (
    version       TEXT PRIMARY KEY,
    description   TEXT,
    applied_at    TEXT DEFAULT (datetime('now', 'localtime'))
);

INSERT OR IGNORE INTO schema_migrations(version, description)
VALUES ('002', 'Taxonomy expansion: subtype, legal_form, parents, categories, results');

-- ============================================================================
-- 1. organizations 拡張
-- ----------------------------------------------------------------------------
-- 既存列を保持したまま ALTER TABLE ADD COLUMN で追加。
-- SQLiteのALTER TABLEはCHECK制約を後付けできるが、CHECKは
-- アプリ側でも検証する前提で軽く付与する。
-- ============================================================================

-- 財団種別（粒度を細分化）。NULL許容、推測でバックフィルする。
ALTER TABLE organizations ADD COLUMN foundation_subtype TEXT
    CHECK (foundation_subtype IN (
        'corporate',   -- 企業財団（親会社あり）
        'individual',  -- 個人財団（創業者・篤志家名義）
        'group',       -- 企業グループ財団（複数親会社/グループ全体）
        'academic',    -- 大学・学会系財団
        'govt',        -- 政府系・公共系
        'intl',        -- 国際機関・海外財団
        'ngo',         -- NPO/NGO系
        'other'
    ));

-- 法人格（公益/一般×財団/社団）
ALTER TABLE organizations ADD COLUMN legal_form TEXT
    CHECK (legal_form IN (
        '公益財団法人', '一般財団法人',
        '公益社団法人', '一般社団法人',
        '特定非営利活動法人', '株式会社', 'その他'
    ));

-- 設立年・公益認定情報
ALTER TABLE organizations ADD COLUMN established_year INTEGER;
ALTER TABLE organizations ADD COLUMN koeki_id TEXT;          -- 公益法人ID（内閣府）
ALTER TABLE organizations ADD COLUMN koeki_certified_date TEXT;  -- 公益認定日 YYYY-MM-DD

-- 創設者・国籍
ALTER TABLE organizations ADD COLUMN founder_name TEXT;
ALTER TABLE organizations ADD COLUMN country_code TEXT DEFAULT 'JP';  -- ISO 3166-1 alpha-2

-- 親企業ID参照（既存のcorporate_parent TEXT列はレガシー名称として残す）
ALTER TABLE organizations ADD COLUMN parent_company_id TEXT
    REFERENCES parent_companies(id) ON UPDATE CASCADE ON DELETE SET NULL;

-- 財務指標
ALTER TABLE organizations ADD COLUMN total_assets INTEGER;       -- 総資産（円）
ALTER TABLE organizations ADD COLUMN annual_grant_amount_history TEXT;
    -- 過去5年JSON 例: [{"year":2024,"amount":120000000}, ...]

-- 検索高速化用インデックス
CREATE INDEX IF NOT EXISTS idx_org_subtype     ON organizations(foundation_subtype);
CREATE INDEX IF NOT EXISTS idx_org_legalform   ON organizations(legal_form);
CREATE INDEX IF NOT EXISTS idx_org_country     ON organizations(country_code);
CREATE INDEX IF NOT EXISTS idx_org_parent_id   ON organizations(parent_company_id);
CREATE INDEX IF NOT EXISTS idx_org_established ON organizations(established_year);

-- ============================================================================
-- 2. parent_companies: 親企業マスタ
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS parent_companies (
    id           TEXT PRIMARY KEY,                 -- UUID or slug
    name         TEXT NOT NULL UNIQUE,             -- 企業名（正式）
    name_short   TEXT,                             -- 略称（"トヨタ"等）
    name_en      TEXT,
    ticker       TEXT,                             -- 証券コード（4桁/Bloombergティッカー）
    exchange     TEXT,                             -- TSE/NYSE/NASDAQ等
    group_name   TEXT,                             -- 企業グループ名（"三菱グループ"等）
    industry     TEXT,                             -- 業種（GICS 1次分類目安）
    headquarters_country TEXT DEFAULT 'JP',
    headquarters_pref    TEXT,
    founded_year INTEGER,
    url          TEXT,
    description  TEXT,
    metadata     TEXT,                             -- JSON 自由記述
    created_at   TEXT DEFAULT (datetime('now', 'localtime')),
    updated_at   TEXT DEFAULT (datetime('now', 'localtime'))
);

CREATE INDEX IF NOT EXISTS idx_parent_group   ON parent_companies(group_name);
CREATE INDEX IF NOT EXISTS idx_parent_ticker  ON parent_companies(ticker);

-- ============================================================================
-- 3. foundation_categories: 階層分野タクソノミー
-- ----------------------------------------------------------------------------
-- レベル1=大分類（自然科学/生命科学/工学/人文社会/芸術文化/教育/福祉/環境/国際）
-- レベル2=中分類、レベル3=小分類
-- 自己参照ツリー構造。
-- ============================================================================
CREATE TABLE IF NOT EXISTS foundation_categories (
    id           TEXT PRIMARY KEY,                 -- slug 'natural_science.physics' 等
    parent_id    TEXT REFERENCES foundation_categories(id) ON DELETE CASCADE,
    level        INTEGER NOT NULL CHECK (level BETWEEN 1 AND 3),
    name_ja      TEXT NOT NULL,
    name_en      TEXT,
    description  TEXT,
    sort_order   INTEGER DEFAULT 0,
    created_at   TEXT DEFAULT (datetime('now', 'localtime'))
);

CREATE INDEX IF NOT EXISTS idx_fcat_parent ON foundation_categories(parent_id);
CREATE INDEX IF NOT EXISTS idx_fcat_level  ON foundation_categories(level);

-- 初期マスタ投入（レベル1のみ。レベル2,3はアプリ側で順次拡張）
INSERT OR IGNORE INTO foundation_categories (id, parent_id, level, name_ja, name_en, sort_order) VALUES
 ('natural_science',  NULL, 1, '自然科学',     'Natural Science',         10),
 ('life_science',     NULL, 1, '生命科学・医学', 'Life Science & Medicine', 20),
 ('engineering',      NULL, 1, '工学・技術',    'Engineering & Technology', 30),
 ('humanities_social',NULL, 1, '人文社会科学',  'Humanities & Social Sciences', 40),
 ('arts_culture',     NULL, 1, '芸術・文化',    'Arts & Culture',          50),
 ('education',        NULL, 1, '教育・人材育成', 'Education',              60),
 ('welfare',          NULL, 1, '福祉・健康',    'Welfare & Health',        70),
 ('environment',      NULL, 1, '環境・サステナビリティ', 'Environment',  80),
 ('international',    NULL, 1, '国際交流・国際協力', 'International',     90),
 ('regional',         NULL, 1, '地域・コミュニティ', 'Regional',         100),
 ('interdisciplinary',NULL, 1, '学際・融合',    'Interdisciplinary',      110);

-- ============================================================================
-- 4. grant_results: 過去採択結果（受賞者・金額履歴）
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS grant_results (
    id                   TEXT PRIMARY KEY,
    call_id              TEXT NOT NULL REFERENCES grant_calls(id) ON DELETE CASCADE,
    fiscal_year          INTEGER NOT NULL,
    awardee_name         TEXT NOT NULL,           -- 受賞者氏名/組織名
    awardee_kana         TEXT,                    -- ふりがな
    awardee_type         TEXT CHECK (awardee_type IN
                            ('individual','team','organization','university','company')),
    awardee_affiliation  TEXT,                    -- 所属（大学・研究機関）
    awardee_position     TEXT,                    -- 職位（教授・准教授・研究員等）
    awardee_department   TEXT,                    -- 学部・学科
    project_title        TEXT NOT NULL,           -- 研究課題名
    project_abstract     TEXT,                    -- 概要
    award_amount         INTEGER,                 -- 採択金額（円）
    award_period_start   TEXT,
    award_period_end     TEXT,
    field_category_id    TEXT REFERENCES foundation_categories(id),
    keywords             TEXT,                    -- カンマ区切り
    source_url           TEXT,                    -- 採択結果掲載URL
    metadata             TEXT,                    -- JSON
    created_at           TEXT DEFAULT (datetime('now', 'localtime')),
    updated_at           TEXT DEFAULT (datetime('now', 'localtime'))
);

CREATE INDEX IF NOT EXISTS idx_result_call    ON grant_results(call_id);
CREATE INDEX IF NOT EXISTS idx_result_fy      ON grant_results(fiscal_year);
CREATE INDEX IF NOT EXISTS idx_result_awardee ON grant_results(awardee_name);
CREATE INDEX IF NOT EXISTS idx_result_field   ON grant_results(field_category_id);

-- ============================================================================
-- 5. foundation_focus_areas: 財団×分野の重点度マッピング
-- ----------------------------------------------------------------------------
-- 1財団が複数分野にN:Mで紐づく。weightは0.0-1.0で重点度。
-- ============================================================================
CREATE TABLE IF NOT EXISTS foundation_focus_areas (
    organization_id  TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    category_id      TEXT NOT NULL REFERENCES foundation_categories(id) ON DELETE CASCADE,
    weight           REAL NOT NULL DEFAULT 1.0 CHECK (weight BETWEEN 0.0 AND 1.0),
    is_primary       INTEGER NOT NULL DEFAULT 0,    -- 1=主たる重点分野
    evidence         TEXT,                          -- 出典・推測根拠
    source           TEXT CHECK (source IN
                        ('manual','inferred_from_program','inferred_from_results','llm_classified')),
    created_at       TEXT DEFAULT (datetime('now', 'localtime')),
    PRIMARY KEY (organization_id, category_id)
);

CREATE INDEX IF NOT EXISTS idx_focus_org   ON foundation_focus_areas(organization_id);
CREATE INDEX IF NOT EXISTS idx_focus_cat   ON foundation_focus_areas(category_id);
CREATE INDEX IF NOT EXISTS idx_focus_prim  ON foundation_focus_areas(is_primary);

-- ============================================================================
-- 6. データバックフィル
-- ----------------------------------------------------------------------------
-- 6-1. legal_form を name から推測
--      既存189件中120件が「公益財団法人」「財団法人」を含む。
-- ============================================================================
UPDATE organizations
   SET legal_form = '公益財団法人'
 WHERE legal_form IS NULL
   AND (name LIKE '公益財団法人%' OR name LIKE '%公益財団法人%');

UPDATE organizations
   SET legal_form = '一般財団法人'
 WHERE legal_form IS NULL
   AND (name LIKE '一般財団法人%' OR name LIKE '%一般財団法人%');

UPDATE organizations
   SET legal_form = '公益社団法人'
 WHERE legal_form IS NULL
   AND name LIKE '%公益社団法人%';

UPDATE organizations
   SET legal_form = '一般社団法人'
 WHERE legal_form IS NULL
   AND name LIKE '%一般社団法人%';

-- 「○○財団」「○○基金」「○○振興会」で法人格未指定 → 推定で公益財団法人
-- （日本の研究助成財団は約75%が公益認定済みのため、デフォルトを公益財団法人に）
UPDATE organizations
   SET legal_form = '公益財団法人'
 WHERE legal_form IS NULL
   AND (name LIKE '%財団%' OR name LIKE '%基金%' OR name LIKE '%振興会%' OR name LIKE '%研究会%');

-- 残りは「その他」（株式会社・任意団体など）
UPDATE organizations
   SET legal_form = 'その他'
 WHERE legal_form IS NULL;

-- ----------------------------------------------------------------------------
-- 6-2. foundation_subtype 推測ロジック
--   (a) corporate_parent列が非NULL/非空 → 'corporate'
--   (b) name に「○○記念」「○○奨学」「個人名らしいトークン」を含む & 親企業なし → 'individual'
--   (c) name に「グループ」「銀行」等の包括語 → 'group'
--   (d) name に「学術」「大学」「学会」 → 'academic'
--   (e) name に「国際」「世界」 → 'intl'
--   (f) その他 → 'other'
-- ----------------------------------------------------------------------------
-- (a) 親企業あり → corporate
UPDATE organizations
   SET foundation_subtype = 'corporate'
 WHERE foundation_subtype IS NULL
   AND corporate_parent IS NOT NULL
   AND TRIM(corporate_parent) <> '';

-- (c) グループ系（複数親企業/業界横断） — 「全国銀行学術研究振興財団」「鉄鋼環境基金」等
UPDATE organizations
   SET foundation_subtype = 'group'
 WHERE foundation_subtype IS NULL
   AND (name LIKE '%全国%' OR name LIKE '%鉄鋼%' OR name LIKE '%日工組%' OR name LIKE '%軽金属%');

-- (d) 学術・学会系
UPDATE organizations
   SET foundation_subtype = 'academic'
 WHERE foundation_subtype IS NULL
   AND (name LIKE '%学術%' OR name LIKE '%学会%' OR name LIKE '%大学%'
        OR name LIKE '%研究所%' OR name LIKE '%研究会%' OR name LIKE '%臨床研究%');

-- (e) 国際系
UPDATE organizations
   SET foundation_subtype = 'intl'
 WHERE foundation_subtype IS NULL
   AND (name LIKE '%国際%' OR name LIKE '%世界%' OR name LIKE '%フィランソロピック%');

-- (b) 個人名財団推定: 「人名+記念」「人名+奨学」「人名+教育/科学/文化」パターン
--     (姓を含む典型語尾で推測。LIKEで個別人名を網羅できないため篤志家パターンを優先)
UPDATE organizations
   SET foundation_subtype = 'individual'
 WHERE foundation_subtype IS NULL
   AND (name LIKE '%記念%' OR name LIKE '%奨学%'
        OR name LIKE '%加藤%' OR name LIKE '%三島%' OR name LIKE '%竹中%'
        OR name LIKE '%戸部%' OR name LIKE '%萩原%' OR name LIKE '%牧誠%'
        OR name LIKE '%齋藤%' OR name LIKE '%芳心会%' OR name LIKE '%白珪社%'
        OR name LIKE '%北野%' OR name LIKE '%金原%' OR name LIKE '%長瀬%'
        OR name LIKE '%大川%' OR name LIKE '%市村%' OR name LIKE '%杉浦%'
        OR name LIKE '%池谷%' OR name LIKE '%小笠原%' OR name LIKE '%高橋%'
        OR name LIKE '%大林%' OR name LIKE '%岡三%');

-- (f) 残りは other
UPDATE organizations
   SET foundation_subtype = 'other'
 WHERE foundation_subtype IS NULL;

-- ----------------------------------------------------------------------------
-- 6-3. country_code: 既存データはすべて日本国内財団 → 'JP'
-- ----------------------------------------------------------------------------
UPDATE organizations
   SET country_code = 'JP'
 WHERE country_code IS NULL;

-- ----------------------------------------------------------------------------
-- 6-4. parent_companies マスタへの初期投入
--      既存organizations.corporate_parentから DISTINCT を取って投入。
--      idは小文字英数slug化が望ましいが、SQLiteで生成困難のため
--      lower(replace(...)) でやれる範囲。手動補正は別工程で実施。
-- ----------------------------------------------------------------------------
INSERT OR IGNORE INTO parent_companies (id, name, name_short, group_name)
SELECT
    LOWER(HEX(RANDOMBLOB(8))),       -- 簡易ID（後で正規slug化を推奨）
    TRIM(corporate_parent),
    TRIM(corporate_parent),
    CASE
        WHEN corporate_parent LIKE '%グループ%' THEN TRIM(corporate_parent)
        WHEN corporate_parent LIKE '%HD%'      THEN TRIM(corporate_parent)
        ELSE NULL
    END
  FROM organizations
 WHERE corporate_parent IS NOT NULL
   AND TRIM(corporate_parent) <> ''
 GROUP BY TRIM(corporate_parent);

-- organizations.parent_company_id の紐付け
UPDATE organizations
   SET parent_company_id = (
        SELECT pc.id FROM parent_companies pc
         WHERE pc.name = TRIM(organizations.corporate_parent)
         LIMIT 1)
 WHERE corporate_parent IS NOT NULL
   AND TRIM(corporate_parent) <> ''
   AND parent_company_id IS NULL;

-- ----------------------------------------------------------------------------
-- 6-5. foundation_focus_areas へ既存 grant_programs.category から推測投入
--      既存 category enum: research / education / culture / welfare / environment /
--                          international / social
--      ↓ レベル1分類IDへマップ
-- ----------------------------------------------------------------------------
INSERT OR IGNORE INTO foundation_focus_areas
    (organization_id, category_id, weight, is_primary, evidence, source)
SELECT
    gp.organization_id,
    CASE gp.category
        WHEN 'research'      THEN 'natural_science'      -- 既定マッピング（要レビュー）
        WHEN 'education'     THEN 'education'
        WHEN 'culture'       THEN 'arts_culture'
        WHEN 'welfare'       THEN 'welfare'
        WHEN 'environment'   THEN 'environment'
        WHEN 'international' THEN 'international'
        WHEN 'social'        THEN 'humanities_social'
    END AS category_id,
    1.0,
    1,
    'inferred from grant_programs.category=' || gp.category,
    'inferred_from_program'
  FROM grant_programs gp
 WHERE gp.category IS NOT NULL
   AND gp.category IN ('research','education','culture','welfare','environment','international','social');

-- ============================================================================
-- 7. 拡張ビュー
-- ----------------------------------------------------------------------------
-- 既存ビュー(v_grants_overview / v_foundation_summary / v_amount_ranking)は
-- カラム削除を行わないため影響を受けない（追加列はビュー定義に含まれないだけ）。
-- ============================================================================

-- 7-1. v_foundation_taxonomy: 財団分類×法人格×国別の集計
DROP VIEW IF EXISTS v_foundation_taxonomy;
CREATE VIEW v_foundation_taxonomy AS
SELECT
    COALESCE(foundation_subtype, 'unknown') AS subtype,
    COALESCE(legal_form,         'unknown') AS legal_form,
    COALESCE(country_code,       'unknown') AS country,
    COUNT(*) AS foundation_count,
    SUM(CASE WHEN annual_grant_amount IS NOT NULL THEN 1 ELSE 0 END) AS with_amount_count,
    SUM(annual_grant_amount) AS total_annual_grant_amount,
    AVG(annual_grant_amount) AS avg_annual_grant_amount,
    MIN(established_year)    AS earliest_established,
    MAX(established_year)    AS latest_established
  FROM organizations
 GROUP BY foundation_subtype, legal_form, country_code
 ORDER BY foundation_count DESC;

-- 7-2. v_coverage_progress: フィールド充足率の進捗ダッシュボード
DROP VIEW IF EXISTS v_coverage_progress;
CREATE VIEW v_coverage_progress AS
SELECT
    'organizations' AS table_name,
    'foundation_subtype' AS field_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN foundation_subtype IS NOT NULL THEN 1 ELSE 0 END) AS filled_rows,
    ROUND(100.0 * SUM(CASE WHEN foundation_subtype IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS coverage_pct
FROM organizations
UNION ALL SELECT 'organizations', 'legal_form',
    COUNT(*), SUM(CASE WHEN legal_form IS NOT NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN legal_form IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2)
FROM organizations
UNION ALL SELECT 'organizations', 'established_year',
    COUNT(*), SUM(CASE WHEN established_year IS NOT NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN established_year IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2)
FROM organizations
UNION ALL SELECT 'organizations', 'koeki_id',
    COUNT(*), SUM(CASE WHEN koeki_id IS NOT NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN koeki_id IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2)
FROM organizations
UNION ALL SELECT 'organizations', 'parent_company_id',
    COUNT(*), SUM(CASE WHEN parent_company_id IS NOT NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN parent_company_id IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2)
FROM organizations
UNION ALL SELECT 'organizations', 'total_assets',
    COUNT(*), SUM(CASE WHEN total_assets IS NOT NULL THEN 1 ELSE 0 END),
    ROUND(100.0 * SUM(CASE WHEN total_assets IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2)
FROM organizations
UNION ALL SELECT 'grant_results', 'all_rows',
    (SELECT COUNT(*) FROM grant_results),
    (SELECT COUNT(*) FROM grant_results),
    100.0
UNION ALL SELECT 'foundation_focus_areas', 'all_rows',
    (SELECT COUNT(*) FROM foundation_focus_areas),
    (SELECT COUNT(*) FROM foundation_focus_areas),
    100.0;

COMMIT;

-- マイグレーション後にFKを再有効化
PRAGMA foreign_keys = ON;

-- ============================================================================
-- 後方互換性メモ
-- ----------------------------------------------------------------------------
-- ・既存ビュー v_grants_overview / v_foundation_summary / v_amount_ranking は
--   organizations から削除する列がないため、定義変更なしで継続稼働する。
-- ・既存列 organizations.corporate_parent (TEXT) は parent_company_id への
--   移行期間中は両方を保持。アプリケーション側で段階的に parent_company_id
--   を主キー参照に切り替える。最終的な廃止は migration 003 以降で実施。
-- ・grant_programs.subcategories TEXT は当面残し、新規データは
--   foundation_focus_areas を介して関係表現に移行する。
-- ・新カラムはすべて NULL 許容のため、既存INSERT文は無修正で動作する。
-- ============================================================================
