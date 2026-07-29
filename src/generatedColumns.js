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
//
// THE BUG THIS EXISTS TO STOP. The source ships block-scoped rows with SELECT *, so a
// generated column rides the wire like any other, and ClientApplier builds its INSERT
// column list from the row's own keys. Naming a generated column is MariaDB errno 1906:
// a WARNING on a permissive server, and a hard ERROR under STRICT_TRANS_TABLES, which
// is the default on every modern MariaDB. So a follower replicating a chain that writes
// contract state fails its block apply, rolls the transaction back, retries the same
// block and never advances.
//
// It is older and wider than the SPV work that found it: `state_key_bin` arrived with
// the state_key_collation flag-day (xchain-indexer ce3febe, 2026-07-10), and
// `contract_state` was already replicated `stream:block` before that. What the SPV
// sub-tree cross-twin suite did was be the first thing to run a follower over a block
// carrying contract state. No indexer-plus-follower venue exists, which is exactly the
// gap recorded in the spec's §7 step 1.
//
// A frozen map rather than an information_schema lookup, deliberately: the applier's
// insert path is on the hot loop and its query sequence is pinned by unit tests, so a
// per-table schema probe would both cost a round trip and change what those tests
// observe. Correctness is held by the drift guard in
// test/unit/generatedColumns.test.js, which derives this map from the indexer's own DDL
// and fails if a table gains or loses a generated column.
const GENERATED_COLUMNS = Object.freeze({
    // Binary-collation shadow of state_key, GENERATED ALWAYS AS (state_key) VIRTUAL.
    contract_state: Object.freeze(['state_key_bin'])
});

// The generated columns of `table` as a Set, empty for every table that has none.
// Cached per table so the applier's per-batch call is a map lookup.
const _sets = new Map();
function generatedColumns(table){
    if(!_sets.has(table))
        _sets.set(table, new Set(GENERATED_COLUMNS[table] || []));
    return _sets.get(table);
}

module.exports = { GENERATED_COLUMNS, generatedColumns };
