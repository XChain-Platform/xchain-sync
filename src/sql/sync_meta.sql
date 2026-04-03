-- Table used by xchain-indexer-sync for transparency log and sync metadata

DROP TABLE IF EXISTS sync_meta;
CREATE TABLE sync_meta (
    id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    block_index    BIGINT UNSIGNED NOT NULL,
    block_time     BIGINT UNSIGNED,
    ledger_hash    VARCHAR(64),
    actions_hash   VARCHAR(64),
    contract_hash  VARCHAR(64),
    logged_at      DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;

CREATE UNIQUE INDEX block_index ON sync_meta (block_index);
