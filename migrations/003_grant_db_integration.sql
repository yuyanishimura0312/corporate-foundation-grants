-- ============================================================================
-- Migration 003: CFG ↔ Grant DB 連携
-- 目的: 両DBのID対応表 + 横断ビュー
-- Date: 2026-05-09
-- ============================================================================

PRAGMA foreign_keys = OFF;
BEGIN TRANSACTION;

-- ----------------------------------------------------------------------------
-- 1. cross_db_mapping: CFG organization ↔ Grant DB organization
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cross_db_mapping (
    cfg_id           TEXT NOT NULL,                  -- CFG organizations.id
    grant_db_id      TEXT NOT NULL,                  -- Grant DB organizations.id
    match_method     TEXT NOT NULL,                  -- 'name_exact'|'name_normalized'|'url'|'manual'
    match_confidence REAL NOT NULL CHECK (match_confidence BETWEEN 0.0 AND 1.0),
    cfg_name         TEXT NOT NULL,
    grant_db_name    TEXT NOT NULL,
    notes            TEXT,
    created_at       TEXT DEFAULT (datetime('now', 'localtime')),
    PRIMARY KEY (cfg_id, grant_db_id),
    FOREIGN KEY (cfg_id) REFERENCES organizations(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_xref_cfg_id     ON cross_db_mapping(cfg_id);
CREATE INDEX IF NOT EXISTS idx_xref_gdb_id     ON cross_db_mapping(grant_db_id);
CREATE INDEX IF NOT EXISTS idx_xref_method     ON cross_db_mapping(match_method);

-- ----------------------------------------------------------------------------
-- 2. integration_metadata: 同期実行ログ
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS integration_metadata (
    id               TEXT PRIMARY KEY,
    operation        TEXT NOT NULL,                  -- 'full_sync'|'incremental'|'mapping_refresh'
    cfg_count        INTEGER,
    grant_db_count   INTEGER,
    matched_count    INTEGER,
    unmatched_cfg    INTEGER,
    unmatched_gdb    INTEGER,
    duration_ms      INTEGER,
    notes            TEXT,
    started_at       TEXT NOT NULL,
    completed_at     TEXT
);

INSERT OR IGNORE INTO schema_migrations(version, description)
VALUES ('003', 'Grant DB integration: cross_db_mapping + integration_metadata');

COMMIT;
PRAGMA foreign_keys = ON;
