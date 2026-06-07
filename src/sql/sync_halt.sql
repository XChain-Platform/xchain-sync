-- Table used by xchain-sync to durably record a divergence HALT.
--
-- When a client confirms a cross-source consensus-hash divergence (two honest
-- sources committed different ledger/actions/contract hashes for the same
-- block), it stops applying and records the halt here. On restart it re-reads
-- any uncleared halt and stays stopped — a halted validator must NEVER silently
-- resume onto a contested chain. An operator clears it explicitly after
-- investigating (cleared_at set), which lets the client resume.

DROP TABLE IF EXISTS sync_halt;
CREATE TABLE sync_halt (
    id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    db_type       VARCHAR(16) NOT NULL,
    block_index   BIGINT UNSIGNED NOT NULL,
    reason        VARCHAR(64) NOT NULL,
    mismatches    TEXT,
    sources       TEXT,
    detected_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
    cleared_at    DATETIME NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;

CREATE INDEX sync_halt_active ON sync_halt (db_type, cleared_at);
