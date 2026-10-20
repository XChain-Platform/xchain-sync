-- Copyright © 2025-2026 Dankest, LLC
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- migration-key: sync-state
CREATE TABLE IF NOT EXISTS sync_state (
    state_key   VARCHAR(128) NOT NULL PRIMARY KEY,
    state_value TEXT,
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;
