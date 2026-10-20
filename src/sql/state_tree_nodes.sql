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

CREATE TABLE state_tree_nodes (
    id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    node_hash   CHAR(64) NOT NULL,                        -- hex SHA-256, content address (the COW key)
    left_hash   CHAR(64) NOT NULL,                        -- the two child hashes (a child may be an EMPTY constant)
    right_hash  CHAR(64) NOT NULL,
    UNIQUE KEY uq_node_hash (node_hash),
    KEY idx_left_hash  (left_hash),                        -- no sync query reads it; kept identical to the indexer schema
    KEY idx_right_hash (right_hash)
    -- Content-addressed, copy-on-write SMT node store for the light-client state
    -- commitment (SPV spec §4.1/§4.3). Holds INTERNAL nodes only (depth 0..255);
    -- a value leaf (depth 256) is never its own row, it lives as a child hash of
    -- its depth-255 parent. Empty subtrees (the EMPTY[h] constants) are never
    -- stored. Append-only during forward processing (INSERT IGNORE; identical
    -- subtrees dedupe by hash). No retention pruner runs on this replica: rollback
    -- only drops the state_tree_roots pointers, reorg orphans stay, and the
    -- indexer's STATE_ROOT_RETENTION_BLOCKS / STATE_NODE_RECLAIM sweep never runs
    -- here. Between full-snapshot applies the table only grows; a full-snapshot
    -- apply replaces it with the source's rows (reportOrphanStats never deletes).
    -- Never written by hub_db_sync.
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;
