--********************************************************************
--
-- Copyright © 2025-2026 Dankest, LLC
-- Based on XChain Platform by Dankest, LLC - https://dankest.llc
--
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of XChain Platform. Licensed under the GNU Affero
-- General Public License v3.0 or later; see LICENSE.md. A commercial
-- license (without AGPL source-disclosure terms) is available -
-- contact legal@dankest.llc.
--
--********************************************************************

-- Table used by xchain-sync for transparency log and sync metadata

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
