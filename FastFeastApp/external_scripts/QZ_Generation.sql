-- ============================================================
-- FastFeast Quarantine Zone — DDL Script
-- Schema: QUARANTINE
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- RejectedRecords
-- Dead on arrival — failed schema/business rule validation
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS QUARANTINE.RejectedRecords (
    rejected_id         INT             NOT NULL AUTOINCREMENT,
    record_payload      VARIANT         NOT NULL,
    rejected_reason     VARCHAR         NOT NULL,
    batch_source        VARCHAR         NOT NULL,
    rejected_at         TIMESTAMP_NTZ   NOT NULL,

    CONSTRAINT pk_rejected PRIMARY KEY (rejected_id)
);

-- ────────────────────────────────────────────────────────────
-- OrphanRecords
-- Valid records with unresolvable foreign key references
-- Awaiting late-arriving dimension data
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS QUARANTINE.OrphanRecords (
    quarantine_id       INT             NOT NULL AUTOINCREMENT,
    record_payload      VARIANT         NOT NULL,
    source_table        VARCHAR         NOT NULL,
    source_file         VARCHAR         NOT NULL,
    orphaned_fk_column  VARCHAR         NOT NULL,
    orphaned_fk_value   VARCHAR         NOT NULL,
    quarantined_at      TIMESTAMP_NTZ   NOT NULL,
    resolved            BOOLEAN         NOT NULL DEFAULT FALSE,
    resolved_at         TIMESTAMP_NTZ,

    CONSTRAINT pk_orphan PRIMARY KEY (quarantine_id)
);