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

-- Transparency-log reorg markers (server-side / indexer only): one row per
-- committed merkle_epoch invalidated by a chain reorg.
--
-- On a reorg the source prunes and re-derives sync_meta and merkle_epochs over
-- the canonical chain (see TransparencyLog.pruneFrom), and this table is the
-- append-only audit record of that change. new_root is backfilled when the epoch
-- re-commits, giving an auditable old->new trail for every root that changed.

CREATE TABLE merkle_reorgs (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    reorg_block  BIGINT NOT NULL,            -- canonical tip rewound to; blocks >= this were orphaned
    epoch        BIGINT NOT NULL,            -- the committed epoch invalidated
    start_block  BIGINT NOT NULL,
    end_block    BIGINT NOT NULL,
    old_root     VARCHAR(64) NOT NULL,       -- root committed before the reorg
    new_root     VARCHAR(64) DEFAULT NULL,   -- root after re-commit (NULL until the epoch re-crosses its boundary)
    detected_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;

CREATE INDEX merkle_reorgs_epoch ON merkle_reorgs (epoch);
