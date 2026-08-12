/********************************************************************
 *
 * Copyright © 2025-2026 Dankest, LLC
 * Based on XChain Platform by Dankest, LLC - https://dankest.llc
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This file is part of XChain Platform. Licensed under the GNU Affero
 * General Public License v3.0 or later; see LICENSE.md. A commercial
 * license (without AGPL source-disclosure terms) is available -
 * contact legal@dankest.llc.
 *
 ********************************************************************/

'use strict';

// Columns the DATABASE computes, which a replication INSERT must never name.
// The source ships block-scoped rows with SELECT *, so a generated column rides the
// wire like any other and ClientApplier builds its INSERT column list from the row's
// own keys. Naming a generated column is MariaDB errno 1906: a warning on a permissive
// server, a hard error under STRICT_TRANS_TABLES (the modern default), so a follower
// replicating a chain that writes contract state fails its block apply, rolls back,
// retries the same block and never advances.
//
// Frozen map rather than an information_schema lookup, deliberately: the applier's
// insert path is on the hot loop and its query sequence is pinned by unit tests, so a
// per-table schema probe would cost a round trip and change what those tests observe.
// Correctness is held by the drift guard in test/unit/generatedColumns.test.js, which
// derives this map from the indexer's own DDL and fails if a table gains or loses a
// generated column.
const GENERATED_COLUMNS = Object.freeze({
    // Binary-collation shadow of state_key, GENERATED ALWAYS AS (state_key) VIRTUAL.
    contract_state: Object.freeze(['state_key_bin'])
});

// Cached per table so the applier's per-batch call stays a map lookup.
const _sets = new Map();
function generatedColumns(table){
    if(!_sets.has(table))
        _sets.set(table, new Set(GENERATED_COLUMNS[table] || []));
    return _sets.get(table);
}

module.exports = { GENERATED_COLUMNS, generatedColumns };
