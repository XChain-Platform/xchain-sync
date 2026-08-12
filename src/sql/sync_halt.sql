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

-- Durable record of a divergence HALT.
--
-- When a client confirms a cross-source consensus-hash divergence (two honest
-- sources committed different ledger/actions/contract hashes for the same
-- block), it stops applying and records the halt here. On restart it re-reads
-- any uncleared halt and stays stopped, because a halted validator must NEVER
-- silently resume onto a contested chain. Only an operator setting cleared_at,
-- after investigating, lets the client resume.

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
